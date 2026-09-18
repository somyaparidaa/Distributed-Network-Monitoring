package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"distributed-network-monitor/services/analysis/aggregation"
	"distributed-network-monitor/services/analysis/anomaly"
	"distributed-network-monitor/services/analysis/api"
	"distributed-network-monitor/services/analysis/consumer"
	"distributed-network-monitor/services/analysis/metrics"
	"distributed-network-monitor/services/analysis/model"
	"distributed-network-monitor/services/analysis/store"
)

// Pipeline represents the telemetry intelligence and analysis pipeline.
// In NETMON-3.4, Pipeline:
// 1. Projects latest telemetry and health events into DeviceStateRepository.
// 2. Feeds telemetry events into the in-memory AggregationEngine.
// 3. Persists time-windowed RollingMetrics into DeviceStateRepository.
// 4. Evaluates rolling metrics and health transitions using the deterministic AnomalyDetector.
// 5. Persists the resulting DeviceAnalysis summary into DeviceStateRepository under analysis:device:{id}:analysis.
type Pipeline struct {
	repo     store.DeviceStateRepository
	engine   *aggregation.Engine
	detector *anomaly.Detector
}

// NewPipeline constructs an event processing pipeline instance backed by storage, aggregation, and anomaly detection.
func NewPipeline(repo store.DeviceStateRepository, engine *aggregation.Engine, detector *anomaly.Detector) *Pipeline {
	if repo == nil {
		repo = store.NewMemoryRepository()
	}
	if engine == nil {
		engine = aggregation.NewEngine(nil)
	}
	if detector == nil {
		detector = anomaly.NewDetector("1m")
	}
	return &Pipeline{
		repo:     repo,
		engine:   engine,
		detector: detector,
	}
}

// Repository returns the underlying repository.
func (p *Pipeline) Repository() store.DeviceStateRepository {
	return p.repo
}

// Engine returns the underlying aggregation engine.
func (p *Pipeline) Engine() *aggregation.Engine {
	return p.engine
}

// Detector returns the underlying anomaly detector.
func (p *Pipeline) Detector() *anomaly.Detector {
	return p.detector
}

// HandleTelemetry projects decoded telemetry events into storage, computes rolling window statistics,
// evaluates anomaly rules, and persists device analysis.
// Redis/storage errors are logged as warnings and do NOT terminate consumption.
// HandleTelemetry projects decoded telemetry events into storage, computes rolling window statistics,
// evaluates anomaly rules, and persists device analysis.
// Redis/storage errors are logged as warnings and do NOT terminate consumption.
func (p *Pipeline) HandleTelemetry(ctx context.Context, event model.TelemetryEvent) error {
	start := time.Now()
	defer func() {
		metrics.ProcessingDurationSeconds.WithLabelValues("telemetry").Observe(time.Since(start).Seconds())
	}()

	log.Printf("[ANALYSIS] received telemetry event [%s] from device [%s] (CPU: %.1f%%, Latency: %dms, Loss: %.2f%%)",
		event.EventID, event.DeviceID, event.CPU, event.LatencyMS, event.PacketLoss)

	// 1. Persist latest point-in-time telemetry
	if err := p.repo.SaveLatestTelemetry(ctx, event.DeviceID, event); err != nil {
		metrics.StorageOperationsTotal.WithLabelValues("save_telemetry", "error").Inc()
		metrics.ProcessingErrorsTotal.WithLabelValues("telemetry", "save_telemetry").Inc()
		log.Printf("[ANALYSIS] warning: failed to project latest telemetry for device [%s] to repository: %v", event.DeviceID, err)
	} else {
		metrics.StorageOperationsTotal.WithLabelValues("save_telemetry", "success").Inc()
	}

	// 2. Compute in-memory rolling metrics across configured windows
	rollingMetrics := p.engine.AddSample(event)

	// 3. Persist latest derived aggregate per window in Redis/Repository
	for _, m := range rollingMetrics {
		if err := p.repo.SaveRollingMetrics(ctx, m); err != nil {
			metrics.StorageOperationsTotal.WithLabelValues("save_rolling", "error").Inc()
			metrics.ProcessingErrorsTotal.WithLabelValues("telemetry", "save_rolling").Inc()
			log.Printf("[ANALYSIS] warning: failed to save rolling metrics [%s] for device [%s] to repository: %v", m.Window, m.DeviceID, err)
		} else {
			metrics.StorageOperationsTotal.WithLabelValues("save_rolling", "success").Inc()
		}
	}

	// 4. Evaluate rolling metrics with AnomalyDetector
	analysis := p.detector.EvaluateMetrics(event.DeviceID, rollingMetrics)
	for _, a := range analysis.ActiveAnomalies {
		metrics.AnomaliesDetectedTotal.WithLabelValues(a.Signal, string(a.Severity)).Inc()
	}

	// 5. Persist latest DeviceAnalysis
	if err := p.repo.SaveDeviceAnalysis(ctx, analysis); err != nil {
		metrics.StorageOperationsTotal.WithLabelValues("save_analysis", "error").Inc()
		metrics.ProcessingErrorsTotal.WithLabelValues("telemetry", "save_analysis").Inc()
		log.Printf("[ANALYSIS] warning: failed to save device analysis for device [%s] to repository: %v", event.DeviceID, err)
	} else {
		metrics.StorageOperationsTotal.WithLabelValues("save_analysis", "success").Inc()
	}

	return nil
}

// HandleHealth projects decoded health state transition events into storage, evaluates health anomalies,
// and updates device analysis.
// Redis/storage errors are logged as warnings and do NOT terminate consumption.
func (p *Pipeline) HandleHealth(ctx context.Context, event model.HealthEvent) error {
	start := time.Now()
	defer func() {
		metrics.ProcessingDurationSeconds.WithLabelValues("health").Observe(time.Since(start).Seconds())
	}()

	log.Printf("[ANALYSIS] received health transition event [%s] for device [%s]: %s -> %s (score: %d)",
		event.EventID, event.DeviceID, event.PreviousStatus, event.CurrentStatus, event.Score)

	// 1. Persist latest health assessment
	if err := p.repo.SaveLatestHealth(ctx, event.DeviceID, event); err != nil {
		metrics.StorageOperationsTotal.WithLabelValues("save_health", "error").Inc()
		metrics.ProcessingErrorsTotal.WithLabelValues("health", "save_health").Inc()
		log.Printf("[ANALYSIS] warning: failed to project latest health for device [%s] to repository: %v", event.DeviceID, err)
	} else {
		metrics.StorageOperationsTotal.WithLabelValues("save_health", "success").Inc()
	}

	// 2. Evaluate health anomaly
	analysis := p.detector.EvaluateHealth(event)
	for _, a := range analysis.ActiveAnomalies {
		metrics.AnomaliesDetectedTotal.WithLabelValues(a.Signal, string(a.Severity)).Inc()
	}

	// 3. Persist updated DeviceAnalysis
	if err := p.repo.SaveDeviceAnalysis(ctx, analysis); err != nil {
		metrics.StorageOperationsTotal.WithLabelValues("save_analysis", "error").Inc()
		metrics.ProcessingErrorsTotal.WithLabelValues("health", "save_analysis").Inc()
		log.Printf("[ANALYSIS] warning: failed to save device analysis on health event for device [%s] to repository: %v", event.DeviceID, err)
	} else {
		metrics.StorageOperationsTotal.WithLabelValues("save_analysis", "success").Inc()
	}

	return nil
}

// Service coordinates the analysis service lifecycle, event consumption, and HTTP read API.
type Service struct {
	cfg        Config
	consumer   consumer.Consumer
	dispatcher *consumer.Dispatcher
	pipeline   consumer.Handler
	repo       store.DeviceStateRepository
	engine     *aggregation.Engine
	detector   *anomaly.Detector
	apiHandler *api.Handler
	httpAddr   string
	stopOnce   sync.Once
}

// NewService constructs a new Service from the provided configuration.
func NewService(cfg Config, handler consumer.Handler) (*Service, error) {
	return NewServiceWithDependencies(cfg, handler, nil, nil, nil, nil)
}

// NewServiceWithConsumer allows injecting a custom Consumer (e.g. MemoryConsumer in tests).
func NewServiceWithConsumer(cfg Config, handler consumer.Handler, injectedConsumer consumer.Consumer) (*Service, error) {
	return NewServiceWithDependencies(cfg, handler, injectedConsumer, nil, nil, nil)
}

// NewServiceWithDependencies allows injecting custom Consumer, DeviceStateRepository, Engine, and Detector implementations for testing.
func NewServiceWithDependencies(
	cfg Config,
	handler consumer.Handler,
	injectedConsumer consumer.Consumer,
	injectedRepo store.DeviceStateRepository,
	injectedEngine *aggregation.Engine,
	injectedDetector *anomaly.Detector,
) (*Service, error) {
	// Initialize repository
	var repo store.DeviceStateRepository
	if injectedRepo != nil {
		repo = injectedRepo
	} else if cfg.RedisEnabled {
		redisRepo, err := store.NewRedisRepository(store.RedisConfig{
			Addr:      cfg.RedisAddr,
			DB:        cfg.RedisDB,
			Password:  cfg.RedisPassword,
			KeyPrefix: cfg.RedisKeyPrefix,
		})
		if err != nil {
			return nil, fmt.Errorf("initialize redis repository: %w", err)
		}
		repo = redisRepo
	} else {
		repo = store.NewMemoryRepository()
	}

	// Initialize aggregation engine
	var engine *aggregation.Engine
	if injectedEngine != nil {
		engine = injectedEngine
	} else {
		engine = aggregation.NewEngine(cfg.AggregationWindows)
	}

	// Initialize anomaly detector
	var detector *anomaly.Detector
	if injectedDetector != nil {
		detector = injectedDetector
	} else {
		detector = anomaly.NewDetector("1m")
	}

	if handler == nil {
		handler = NewPipeline(repo, engine, detector)
	}

	dispatcher := consumer.NewDispatcher(handler, cfg.TelemetryTopic, cfg.HealthTopic)

	var c consumer.Consumer
	if cfg.KafkaEnabled {
		if injectedConsumer != nil {
			c = injectedConsumer
		} else {
			topics := []string{cfg.TelemetryTopic, cfg.HealthTopic}
			kcConfig := consumer.KafkaConsumerConfig{
				Brokers: cfg.KafkaBrokers,
				GroupID: cfg.ConsumerGroup,
				Topics:  topics,
			}
			c = consumer.NewKafkaConsumer(kcConfig, dispatcher, nil)
		}
	}

	apiHandler := api.NewHandler(repo, cfg.KafkaEnabled, cfg.RedisEnabled, cfg.AggregationWindows)

	return &Service{
		cfg:        cfg,
		consumer:   c,
		dispatcher: dispatcher,
		pipeline:   handler,
		repo:       repo,
		engine:     engine,
		detector:   detector,
		apiHandler: apiHandler,
		httpAddr:   cfg.HTTPAddr,
	}, nil
}

// Consumer returns the underlying Consumer instance.
func (s *Service) Consumer() consumer.Consumer {
	return s.consumer
}

// Dispatcher returns the underlying Dispatcher instance.
func (s *Service) Dispatcher() *consumer.Dispatcher {
	return s.dispatcher
}

// Pipeline returns the pipeline handler.
func (s *Service) Pipeline() consumer.Handler {
	return s.pipeline
}

// Repository returns the underlying repository instance.
func (s *Service) Repository() store.DeviceStateRepository {
	return s.repo
}

// Engine returns the underlying aggregation engine.
func (s *Service) Engine() *aggregation.Engine {
	return s.engine
}

// Detector returns the underlying anomaly detector.
func (s *Service) Detector() *anomaly.Detector {
	return s.detector
}

// APIHandler returns the HTTP API handler.
func (s *Service) APIHandler() *api.Handler {
	return s.apiHandler
}

// Run executes the analysis service lifecycle until ctx is cancelled.
//
// Lifecycle sequence:
//  1. Start HTTP Read API server on configured HTTPAddr.
//  2. Start Kafka consumer worker (if KafkaEnabled).
//  3. On context cancellation or server error, shut down in reverse order:
//     a. HTTP server stops accepting requests (Shutdown).
//     b. Kafka consumer worker stops and closes.
//     c. DeviceStateRepository closes.
func (s *Service) Run(ctx context.Context) error {
	log.Printf("analysis service started; http_addr=%s consumer_group=%s topics=[%s, %s] brokers=%v kafka_enabled=%v redis_enabled=%v redis_addr=%s",
		s.httpAddr, s.cfg.ConsumerGroup, s.cfg.TelemetryTopic, s.cfg.HealthTopic, s.cfg.KafkaBrokers, s.cfg.KafkaEnabled, s.cfg.RedisEnabled, s.cfg.RedisAddr)

	server := &http.Server{
		Addr:    s.httpAddr,
		Handler: s.apiHandler.Routes(),
	}

	serverErr := make(chan error, 1)
	go func() {
		log.Printf("analysis HTTP read API listening on %s", s.httpAddr)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErr <- err
		}
		close(serverErr)
	}()

	consumerCtx, cancelConsumer := context.WithCancel(context.Background())
	defer cancelConsumer()

	consumerErr := make(chan error, 1)
	if s.cfg.KafkaEnabled && s.consumer != nil {
		go func() {
			if err := s.consumer.Start(consumerCtx); err != nil && !errors.Is(err, context.Canceled) {
				consumerErr <- err
			}
			close(consumerErr)
		}()
	} else {
		log.Println("analysis service: kafka consumption is disabled, running HTTP API only")
	}

	var runErr error
	select {
	case err := <-serverErr:
		if err != nil {
			runErr = fmt.Errorf("analysis http server error: %w", err)
		}
	case err := <-consumerErr:
		if err != nil {
			runErr = fmt.Errorf("consumer runtime error: %w", err)
		}
	case <-ctx.Done():
		log.Println("analysis service shutdown requested...")
	}

	// 1. Gracefully shut down HTTP server
	shutdownCtx, cancelHTTP := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancelHTTP()

	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Printf("[ANALYSIS] http server shutdown error: %v", err)
	} else {
		log.Println("analysis http server stopped gracefully")
	}
	<-serverErr

	// 2. Stop and close consumer
	cancelConsumer()
	if s.cfg.KafkaEnabled && s.consumer != nil {
		if err := s.consumer.Close(); err != nil {
			log.Printf("[ANALYSIS] error closing consumer: %v", err)
		} else {
			log.Println("consumer resources closed cleanly")
		}
		<-consumerErr
		log.Println("consumer worker exited cleanly")
	}

	// 3. Close repository
	if s.repo != nil {
		if err := s.repo.Close(); err != nil {
			log.Printf("[ANALYSIS] error closing repository: %v", err)
		} else {
			log.Println("repository resources closed cleanly")
		}
	}

	return runErr
}

func main() {
	cfg := DefaultConfig()
	service, err := NewService(cfg, nil)
	if err != nil {
		log.Fatalf("failed to initialize analysis service: %v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	log.Println("starting distributed telemetry analysis service...")
	if err := service.Run(ctx); err != nil {
		log.Fatalf("analysis service error: %v", err)
	}
	log.Println("analysis service process shutdown complete")
}
