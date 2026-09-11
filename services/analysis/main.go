package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"distributed-network-monitor/services/analysis/consumer"
	"distributed-network-monitor/services/analysis/model"
)

// Pipeline represents the telemetry intelligence and analysis pipeline.
// For NETMON-3.1, this establishes the event processing boundary for later stories.
type Pipeline struct {
	mu              sync.Mutex
	telemetryEvents []model.TelemetryEvent
	healthEvents    []model.HealthEvent
}

// NewPipeline constructs an event processing pipeline instance.
func NewPipeline() *Pipeline {
	return &Pipeline{
		telemetryEvents: make([]model.TelemetryEvent, 0),
		healthEvents:    make([]model.HealthEvent, 0),
	}
}

// HandleTelemetry receives decoded telemetry events from the consumer.
func (p *Pipeline) HandleTelemetry(ctx context.Context, event model.TelemetryEvent) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.telemetryEvents = append(p.telemetryEvents, event)
	log.Printf("[ANALYSIS] received telemetry event [%s] from device [%s] (CPU: %.1f%%, Latency: %dms, Loss: %.2f%%)",
		event.EventID, event.DeviceID, event.CPU, event.LatencyMS, event.PacketLoss)
	return nil
}

// HandleHealth receives decoded health state transition events from the consumer.
func (p *Pipeline) HandleHealth(ctx context.Context, event model.HealthEvent) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.healthEvents = append(p.healthEvents, event)
	log.Printf("[ANALYSIS] received health transition event [%s] for device [%s]: %s -> %s (score: %d)",
		event.EventID, event.DeviceID, event.PreviousStatus, event.CurrentStatus, event.Score)
	return nil
}

// Service coordinates the analysis service lifecycle and manages event consumption.
type Service struct {
	cfg        Config
	consumer   consumer.Consumer
	dispatcher *consumer.Dispatcher
	pipeline   consumer.Handler
	stopOnce   sync.Once
}

// NewService constructs a new Service from the provided configuration.
// When KAFKA_ENABLED=true, it creates a real KafkaConsumer that connects to the configured brokers.
// When KAFKA_ENABLED=false, no consumer is started.
func NewService(cfg Config, handler consumer.Handler) (*Service, error) {
	return NewServiceWithConsumer(cfg, handler, nil)
}

// NewServiceWithConsumer allows injecting a custom Consumer (e.g., MemoryConsumer in tests).
// If injectedConsumer is nil and KafkaEnabled is true, a production KafkaConsumer is instantiated.
func NewServiceWithConsumer(cfg Config, handler consumer.Handler, injectedConsumer consumer.Consumer) (*Service, error) {
	if handler == nil {
		handler = NewPipeline()
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

	return &Service{
		cfg:        cfg,
		consumer:   c,
		dispatcher: dispatcher,
		pipeline:   handler,
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

// Run executes the analysis service lifecycle until ctx is cancelled.
//
// Graceful shutdown order:
// 1. Stop accepting new messages by cancelling consumer context
// 2. Close consumer resources cleanly
// 3. Return cleanly without leaking goroutines
func (s *Service) Run(ctx context.Context) error {
	log.Printf("analysis service started; consumer_group=%s topics=[%s, %s] brokers=%v kafka_enabled=%v",
		s.cfg.ConsumerGroup, s.cfg.TelemetryTopic, s.cfg.HealthTopic, s.cfg.KafkaBrokers, s.cfg.KafkaEnabled)

	if !s.cfg.KafkaEnabled || s.consumer == nil {
		log.Println("analysis service: kafka consumption is disabled, awaiting shutdown signal")
		<-ctx.Done()
		log.Println("analysis service shutdown requested")
		return nil
	}

	consumerCtx, cancelConsumer := context.WithCancel(context.Background())
	defer cancelConsumer()

	errCh := make(chan error, 1)
	go func() {
		if err := s.consumer.Start(consumerCtx); err != nil && err != context.Canceled {
			errCh <- err
		}
		close(errCh)
	}()

	select {
	case err := <-errCh:
		if err != nil {
			return fmt.Errorf("consumer runtime error: %w", err)
		}
		return nil
	case <-ctx.Done():
		log.Println("analysis service shutdown requested...")
	}

	// 1. Stop consumer loop
	cancelConsumer()

	// 2. Close consumer resources
	if err := s.consumer.Close(); err != nil {
		log.Printf("[ANALYSIS] error closing consumer: %v", err)
	} else {
		log.Println("consumer resources closed cleanly")
	}

	// Wait for consumer worker to finish
	<-errCh
	log.Println("consumer worker exited cleanly")

	return nil
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
