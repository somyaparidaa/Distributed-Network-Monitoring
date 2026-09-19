package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"distributed-network-monitor/services/monitoring/api"
	"distributed-network-monitor/services/monitoring/device"
	"distributed-network-monitor/services/monitoring/health"
	"distributed-network-monitor/services/monitoring/kafka"
	"distributed-network-monitor/services/monitoring/metrics"
	"distributed-network-monitor/services/monitoring/polling"
)

// Service coordinates monitoring operations and encapsulates application state.
type Service struct {
	registry     *device.Registry
	store        *polling.Store
	healthStore  *health.Store
	stateTracker *polling.StateTracker
	producer     kafka.Producer
	engine       *polling.Engine
	apiHandler   *api.Handler
	httpAddr     string
}

// NewService constructs a Service instance from the provided Config.
// When KAFKA_ENABLED=true, it initializes a production KafkaProducer connecting to the configured brokers.
// When KAFKA_ENABLED=false, no producer is created.
func NewService(cfg Config) (*Service, error) {
	return NewServiceWithProducer(cfg, nil)
}

// NewServiceWithProducer allows injecting a specific Producer (e.g. MemoryProducer for tests).
// If injectedProducer is nil and KafkaEnabled is true, a live KafkaProducer is created.
func NewServiceWithProducer(cfg Config, injectedProducer kafka.Producer) (*Service, error) {
	devices := cfg.ToMonitoredDevices()
	registry, err := device.NewRegistry(devices)
	if err != nil {
		return nil, fmt.Errorf("initialize device registry: %w", err)
	}

	store := polling.NewStore()
	healthStore := health.NewStore()
	evaluator := health.NewServiceEvaluator(store, healthStore)

	var producer kafka.Producer
	if cfg.KafkaEnabled {
		if injectedProducer != nil {
			producer = injectedProducer
		} else {
			kafkaCfg := kafka.Config{
				Enabled:        cfg.KafkaEnabled,
				Brokers:        cfg.KafkaBrokers,
				TelemetryTopic: cfg.TelemetryTopic,
				HealthTopic:    cfg.HealthTopic,
			}
			liveProducer, err := kafka.NewKafkaProducer(kafkaCfg, nil)
			if err != nil {
				return nil, fmt.Errorf("initialize kafka producer: %w", err)
			}
			producer = kafka.NewLoggingProducer(liveProducer, kafkaCfg)
		}
		publisher := kafka.NewEventPublisher(producer)
		evaluator.SetTransitionListener(publisher)
		metrics.KafkaAvailable.Set(1)
	} else {
		metrics.KafkaAvailable.Set(0)
	}

	stateTracker := polling.NewStateTracker()
	client := polling.NewClient(cfg.PollTimeout)

	engineConfig := polling.EngineConfig{
		PollInterval: cfg.PollInterval,
		Retry: polling.RetryConfig{
			MaxRetries:     cfg.MaxRetries,
			InitialBackoff: cfg.RetryBackoff,
		},
		FailureThreshold: cfg.FailureThreshold,
	}

	engine := polling.NewEngine(registry, store, stateTracker, client, engineConfig)
	engine.SetHealthEvaluator(evaluator)
	if producer != nil {
		engine.SetTelemetryPublisher(kafka.NewEventPublisher(producer))
	}

	apiHandler := api.NewHandlerWithDependencies(registry, store, healthStore, stateTracker, client, cfg.KafkaEnabled, cfg.KafkaBrokers)

	return &Service{
		registry:     registry,
		store:        store,
		healthStore:  healthStore,
		stateTracker: stateTracker,
		producer:     producer,
		engine:       engine,
		apiHandler:   apiHandler,
		httpAddr:     cfg.HTTPAddr,
	}, nil
}

// Producer returns the underlying Kafka Producer instance.
func (s *Service) Producer() kafka.Producer {
	return s.producer
}

// Registry returns the underlying device registry.
func (s *Service) Registry() *device.Registry {
	return s.registry
}

// Store returns the underlying in-memory telemetry store.
func (s *Service) Store() *polling.Store {
	return s.store
}

// HealthStore returns the underlying in-memory health assessment store.
func (s *Service) HealthStore() *health.Store {
	return s.healthStore
}

// StateTracker returns the underlying device state tracker.
func (s *Service) StateTracker() *polling.StateTracker {
	return s.stateTracker
}

// APIHandler returns the underlying HTTP API handler.
func (s *Service) APIHandler() *api.Handler {
	return s.apiHandler
}

// Run executes the monitoring service lifecycle until ctx is cancelled.
//
// Graceful shutdown order:
// 1. Stop accepting HTTP requests & gracefully shut down HTTP server
// 2. Stop polling workers
// 3. Close Kafka producer
func (s *Service) Run(ctx context.Context) error {
	log.Printf("monitoring service started; managing %d configured devices:", s.registry.Len())
	for _, d := range s.registry.List() {
		log.Printf("  - device [%s] target: %s", d.ID, d.MetricsURL)
	}

	// Create child context for polling workers
	pollCtx, cancelPoll := context.WithCancel(context.Background())
	defer cancelPoll()

	// Start concurrent polling workers
	s.engine.Start(pollCtx)

	// Start HTTP API server
	server := &http.Server{
		Addr:    s.httpAddr,
		Handler: s.apiHandler.Routes(),
	}

	serverErr := make(chan error, 1)
	go func() {
		log.Printf("monitoring state API listening on %s", s.httpAddr)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			serverErr <- err
		}
		close(serverErr)
	}()

	select {
	case err := <-serverErr:
		cancelPoll()
		s.engine.Wait()
		if s.producer != nil {
			_ = s.producer.Close()
		}
		return fmt.Errorf("monitoring api server error: %w", err)
	case <-ctx.Done():
		log.Println("monitoring service shutdown requested...")
	}

	// 1. Stop accepting HTTP requests & gracefully shut down HTTP server
	shutdownCtx, cancelHTTP := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancelHTTP()

	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Printf("monitoring api server shutdown error: %v", err)
	} else {
		log.Println("monitoring api server stopped gracefully")
	}
	<-serverErr

	// 2. Stop polling workers
	cancelPoll()
	s.engine.Wait()
	log.Println("all polling workers stopped cleanly")

	// 3. Close Kafka producer
	if s.producer != nil {
		if err := s.producer.Close(); err != nil {
			log.Printf("[KAFKA] error closing producer: %v", err)
		} else {
			log.Println("kafka producer closed cleanly")
		}
	}

	return nil
}

func main() {
	cfg := DefaultConfig()
	service, err := NewService(cfg)
	if err != nil {
		log.Fatalf("failed to initialize monitoring service: %v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	log.Println("starting distributed monitoring service...")
	if err := service.Run(ctx); err != nil {
		log.Fatalf("monitoring service error: %v", err)
	}
	log.Println("monitoring service process shutdown complete")
}
