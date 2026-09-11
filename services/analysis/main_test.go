package main

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"distributed-network-monitor/services/analysis/consumer"
	"distributed-network-monitor/services/analysis/model"
)

type testHandler struct {
	mu              sync.Mutex
	telemetryEvents []model.TelemetryEvent
	healthEvents    []model.HealthEvent
}

func (h *testHandler) HandleTelemetry(ctx context.Context, event model.TelemetryEvent) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.telemetryEvents = append(h.telemetryEvents, event)
	return nil
}

func (h *testHandler) HandleHealth(ctx context.Context, event model.HealthEvent) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.healthEvents = append(h.healthEvents, event)
	return nil
}

func TestConfigDefaultsAndOverrides(t *testing.T) {
	// 1. Verify defaults
	cfg := DefaultConfig()
	if !cfg.KafkaEnabled {
		t.Fatal("expected KafkaEnabled default true")
	}
	if len(cfg.KafkaBrokers) != 1 || cfg.KafkaBrokers[0] != "localhost:9092" {
		t.Fatalf("expected KafkaBrokers [localhost:9092], got %v", cfg.KafkaBrokers)
	}
	if cfg.ConsumerGroup != "analysis-service" {
		t.Fatalf("expected ConsumerGroup 'analysis-service', got %q", cfg.ConsumerGroup)
	}
	if cfg.TelemetryTopic != "network.telemetry" {
		t.Fatalf("expected TelemetryTopic 'network.telemetry', got %q", cfg.TelemetryTopic)
	}
	if cfg.HealthTopic != "network.health-events" {
		t.Fatalf("expected HealthTopic 'network.health-events', got %q", cfg.HealthTopic)
	}

	// 2. Test overrides
	t.Setenv("KAFKA_ENABLED", "false")
	t.Setenv("KAFKA_BROKERS", "k1:9092, k2:9092")
	t.Setenv("KAFKA_CONSUMER_GROUP", "custom-group")
	t.Setenv("KAFKA_TELEMETRY_TOPIC", "custom.telemetry")
	t.Setenv("KAFKA_HEALTH_TOPIC", "custom.health")

	overridden := DefaultConfig()
	if overridden.KafkaEnabled != false {
		t.Error("expected overridden KafkaEnabled false")
	}
	if len(overridden.KafkaBrokers) != 2 || overridden.KafkaBrokers[0] != "k1:9092" || overridden.KafkaBrokers[1] != "k2:9092" {
		t.Fatalf("unexpected overridden KafkaBrokers: %v", overridden.KafkaBrokers)
	}
	if overridden.ConsumerGroup != "custom-group" {
		t.Fatalf("expected ConsumerGroup 'custom-group', got %q", overridden.ConsumerGroup)
	}
	if overridden.TelemetryTopic != "custom.telemetry" || overridden.HealthTopic != "custom.health" {
		t.Fatalf("unexpected overridden topics: %q, %q", overridden.TelemetryTopic, overridden.HealthTopic)
	}

	// 3. Invalid boolean fallback
	t.Setenv("KAFKA_ENABLED", "not-a-bool")
	fallback := DefaultConfig()
	if !fallback.KafkaEnabled {
		t.Error("invalid KAFKA_ENABLED should fall back to true")
	}
}

func TestNewServiceCreatesRealKafkaConsumerWhenEnabled(t *testing.T) {
	cfg := DefaultConfig()
	cfg.KafkaEnabled = true
	cfg.KafkaBrokers = []string{"localhost:9092"}

	service, err := NewService(cfg, nil)
	if err != nil {
		t.Fatalf("failed to construct service: %v", err)
	}

	if service.Consumer() == nil {
		t.Fatal("expected non-nil consumer when KafkaEnabled=true")
	}

	// Verify that production service instantiated a real KafkaConsumer, not MemoryConsumer
	_, isKafkaConsumer := service.Consumer().(*consumer.KafkaConsumer)
	if !isKafkaConsumer {
		t.Fatalf("expected real KafkaConsumer instance in production mode, got %T", service.Consumer())
	}
}

func TestServiceLifecycleAndGracefulShutdown(t *testing.T) {
	for cycle := 0; cycle < 3; cycle++ {
		cfg := DefaultConfig()
		handler := &testHandler{}
		dispatcher := consumer.NewDispatcher(handler, cfg.TelemetryTopic, cfg.HealthTopic)
		memConsumer := consumer.NewMemoryConsumer(dispatcher, 10)

		service, err := NewServiceWithConsumer(cfg, handler, memConsumer)
		if err != nil {
			t.Fatalf("cycle %d: failed to create service: %v", cycle, err)
		}

		ctx, cancel := context.WithCancel(context.Background())
		errCh := make(chan error, 1)

		go func() {
			errCh <- service.Run(ctx)
		}()

		time.Sleep(25 * time.Millisecond)
		cancel()

		select {
		case err := <-errCh:
			if err != nil && !errors.Is(err, context.Canceled) {
				t.Fatalf("cycle %d: service.Run error: %v", cycle, err)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("cycle %d: service.Run did not stop within deadline", cycle)
		}
	}
}

func TestServiceLifecycleKafkaDisabled(t *testing.T) {
	cfg := DefaultConfig()
	cfg.KafkaEnabled = false

	service, err := NewService(cfg, nil)
	if err != nil {
		t.Fatalf("failed to create service: %v", err)
	}

	if service.Consumer() != nil {
		t.Fatalf("expected nil consumer when KafkaEnabled=false, got %v", service.Consumer())
	}

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)

	go func() {
		errCh <- service.Run(ctx)
	}()

	time.Sleep(20 * time.Millisecond)
	cancel()

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("expected nil error on shutdown when kafka disabled, got: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("service did not stop within deadline")
	}
}
