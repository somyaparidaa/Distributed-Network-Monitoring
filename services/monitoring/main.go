package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"distributed-network-monitor/services/monitoring/device"
)

// Service coordinates monitoring operations and encapsulates application state.
type Service struct {
	registry *device.Registry
}

// NewService constructs a Service instance from the provided Config.
func NewService(cfg Config) (*Service, error) {
	devices := cfg.ToMonitoredDevices()
	registry, err := device.NewRegistry(devices)
	if err != nil {
		return nil, fmt.Errorf("initialize device registry: %w", err)
	}

	return &Service{
		registry: registry,
	}, nil
}

// Registry returns the underlying device registry.
func (s *Service) Registry() *device.Registry {
	return s.registry
}

// Run executes the monitoring service lifecycle until ctx is cancelled.
func (s *Service) Run(ctx context.Context) error {
	log.Printf("monitoring service started; managing %d configured devices:", s.registry.Len())
	for _, d := range s.registry.List() {
		log.Printf("  - device [%s] target: %s", d.ID, d.MetricsURL)
	}

	<-ctx.Done()
	log.Println("monitoring service shutdown requested; stopping...")
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
