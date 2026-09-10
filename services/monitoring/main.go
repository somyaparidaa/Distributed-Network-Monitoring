package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"distributed-network-monitor/services/monitoring/device"
	"distributed-network-monitor/services/monitoring/polling"
)

// Service coordinates monitoring operations and encapsulates application state.
type Service struct {
	registry     *device.Registry
	store        *polling.Store
	stateTracker *polling.StateTracker
	engine       *polling.Engine
}

// NewService constructs a Service instance from the provided Config.
func NewService(cfg Config) (*Service, error) {
	devices := cfg.ToMonitoredDevices()
	registry, err := device.NewRegistry(devices)
	if err != nil {
		return nil, fmt.Errorf("initialize device registry: %w", err)
	}

	store := polling.NewStore()
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

	return &Service{
		registry:     registry,
		store:        store,
		stateTracker: stateTracker,
		engine:       engine,
	}, nil
}

// Registry returns the underlying device registry.
func (s *Service) Registry() *device.Registry {
	return s.registry
}

// Store returns the underlying in-memory telemetry store.
func (s *Service) Store() *polling.Store {
	return s.store
}

// StateTracker returns the underlying device state tracker.
func (s *Service) StateTracker() *polling.StateTracker {
	return s.stateTracker
}

// Run executes the monitoring service lifecycle until ctx is cancelled.
func (s *Service) Run(ctx context.Context) error {
	log.Printf("monitoring service started; managing %d configured devices:", s.registry.Len())
	for _, d := range s.registry.List() {
		log.Printf("  - device [%s] target: %s", d.ID, d.MetricsURL)
	}

	// Start concurrent polling workers
	s.engine.Start(ctx)

	<-ctx.Done()
	log.Println("monitoring service shutdown requested; waiting for polling workers...")

	// Wait for workers to cleanly exit
	s.engine.Wait()
	log.Println("all polling workers stopped cleanly")

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
