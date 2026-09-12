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
	"distributed-network-monitor/services/analysis/store"
)

// Pipeline represents the telemetry intelligence and analysis pipeline.
// In NETMON-3.2, Pipeline implements consumer.Handler and projects incoming
// TelemetryEvent and HealthEvent messages into the DeviceStateRepository (Redis or Memory).
type Pipeline struct {
	repo store.DeviceStateRepository
}

// NewPipeline constructs an event processing pipeline instance backed by a DeviceStateRepository.
func NewPipeline(repo store.DeviceStateRepository) *Pipeline {
	if repo == nil {
		repo = store.NewMemoryRepository()
	}
	return &Pipeline{
		repo: repo,
	}
}

// Repository returns the underlying repository.
func (p *Pipeline) Repository() store.DeviceStateRepository {
	return p.repo
}

// HandleTelemetry projects decoded telemetry events into storage.
// Redis/storage errors are logged as warnings and do NOT terminate consumption.
func (p *Pipeline) HandleTelemetry(ctx context.Context, event model.TelemetryEvent) error {
	log.Printf("[ANALYSIS] received telemetry event [%s] from device [%s] (CPU: %.1f%%, Latency: %dms, Loss: %.2f%%)",
		event.EventID, event.DeviceID, event.CPU, event.LatencyMS, event.PacketLoss)

	if err := p.repo.SaveLatestTelemetry(ctx, event.DeviceID, event); err != nil {
		log.Printf("[ANALYSIS] warning: failed to project latest telemetry for device [%s] to repository: %v", event.DeviceID, err)
		return nil
	}
	return nil
}

// HandleHealth projects decoded health state transition events into storage.
// Redis/storage errors are logged as warnings and do NOT terminate consumption.
func (p *Pipeline) HandleHealth(ctx context.Context, event model.HealthEvent) error {
	log.Printf("[ANALYSIS] received health transition event [%s] for device [%s]: %s -> %s (score: %d)",
		event.EventID, event.DeviceID, event.PreviousStatus, event.CurrentStatus, event.Score)

	if err := p.repo.SaveLatestHealth(ctx, event.DeviceID, event); err != nil {
		log.Printf("[ANALYSIS] warning: failed to project latest health for device [%s] to repository: %v", event.DeviceID, err)
		return nil
	}
	return nil
}

// Service coordinates the analysis service lifecycle and manages event consumption.
type Service struct {
	cfg        Config
	consumer   consumer.Consumer
	dispatcher *consumer.Dispatcher
	pipeline   consumer.Handler
	repo       store.DeviceStateRepository
	stopOnce   sync.Once
}

// NewService constructs a new Service from the provided configuration.
// When KAFKA_ENABLED=true, it creates a real KafkaConsumer that connects to the configured brokers.
// When REDIS_ENABLED=true, it initializes a real RedisRepository connecting to the configured Redis instance.
// When REDIS_ENABLED=false, it uses an in-memory repository.
func NewService(cfg Config, handler consumer.Handler) (*Service, error) {
	return NewServiceWithDependencies(cfg, handler, nil, nil)
}

// NewServiceWithConsumer allows injecting a custom Consumer (e.g. MemoryConsumer in tests).
func NewServiceWithConsumer(cfg Config, handler consumer.Handler, injectedConsumer consumer.Consumer) (*Service, error) {
	return NewServiceWithDependencies(cfg, handler, injectedConsumer, nil)
}

// NewServiceWithDependencies allows injecting custom Consumer and/or DeviceStateRepository implementations for testing.
func NewServiceWithDependencies(
	cfg Config,
	handler consumer.Handler,
	injectedConsumer consumer.Consumer,
	injectedRepo store.DeviceStateRepository,
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

	if handler == nil {
		handler = NewPipeline(repo)
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
		repo:       repo,
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

// Run executes the analysis service lifecycle until ctx is cancelled.
//
// Graceful shutdown order:
// 1. Stop consumer loop
// 2. Close consumer resources cleanly
// 3. Close storage repository cleanly
// 4. Return cleanly without leaking goroutines
func (s *Service) Run(ctx context.Context) error {
	log.Printf("analysis service started; consumer_group=%s topics=[%s, %s] brokers=%v kafka_enabled=%v redis_enabled=%v redis_addr=%s",
		s.cfg.ConsumerGroup, s.cfg.TelemetryTopic, s.cfg.HealthTopic, s.cfg.KafkaBrokers, s.cfg.KafkaEnabled, s.cfg.RedisEnabled, s.cfg.RedisAddr)

	if !s.cfg.KafkaEnabled || s.consumer == nil {
		log.Println("analysis service: kafka consumption is disabled, awaiting shutdown signal")
		<-ctx.Done()
		log.Println("analysis service shutdown requested")
		if s.repo != nil {
			_ = s.repo.Close()
		}
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
		cancelConsumer()
		if s.repo != nil {
			_ = s.repo.Close()
		}
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

	// 3. Close repository resources
	if s.repo != nil {
		if err := s.repo.Close(); err != nil {
			log.Printf("[ANALYSIS] error closing repository: %v", err)
		} else {
			log.Println("repository resources closed cleanly")
		}
	}

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
