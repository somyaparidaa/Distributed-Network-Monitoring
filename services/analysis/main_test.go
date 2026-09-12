package main

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"distributed-network-monitor/services/analysis/consumer"
	"distributed-network-monitor/services/analysis/model"
	"distributed-network-monitor/services/analysis/store"
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
	if !cfg.RedisEnabled {
		t.Fatal("expected RedisEnabled default true")
	}
	if cfg.RedisAddr != "localhost:6379" {
		t.Fatalf("expected RedisAddr default localhost:6379, got %q", cfg.RedisAddr)
	}
	if cfg.RedisDB != 0 {
		t.Fatalf("expected RedisDB default 0, got %d", cfg.RedisDB)
	}
	if cfg.RedisKeyPrefix != "analysis" {
		t.Fatalf("expected RedisKeyPrefix default 'analysis', got %q", cfg.RedisKeyPrefix)
	}

	// 2. Test overrides
	t.Setenv("KAFKA_ENABLED", "false")
	t.Setenv("KAFKA_BROKERS", "k1:9092, k2:9092")
	t.Setenv("KAFKA_CONSUMER_GROUP", "custom-group")
	t.Setenv("KAFKA_TELEMETRY_TOPIC", "custom.telemetry")
	t.Setenv("KAFKA_HEALTH_TOPIC", "custom.health")
	t.Setenv("REDIS_ENABLED", "false")
	t.Setenv("REDIS_ADDR", "custom-redis:6380")
	t.Setenv("REDIS_DB", "2")
	t.Setenv("REDIS_PASSWORD", "secret")
	t.Setenv("REDIS_KEY_PREFIX", "custom_prefix")

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
	if overridden.RedisEnabled != false {
		t.Error("expected overridden RedisEnabled false")
	}
	if overridden.RedisAddr != "custom-redis:6380" {
		t.Fatalf("expected RedisAddr 'custom-redis:6380', got %q", overridden.RedisAddr)
	}
	if overridden.RedisDB != 2 {
		t.Fatalf("expected RedisDB 2, got %d", overridden.RedisDB)
	}
	if overridden.RedisPassword != "secret" {
		t.Fatalf("expected RedisPassword 'secret', got %q", overridden.RedisPassword)
	}
	if overridden.RedisKeyPrefix != "custom_prefix" {
		t.Fatalf("expected RedisKeyPrefix 'custom_prefix', got %q", overridden.RedisKeyPrefix)
	}

	// 3. Invalid fallback
	t.Setenv("KAFKA_ENABLED", "not-a-bool")
	t.Setenv("REDIS_ENABLED", "not-a-bool")
	fallback := DefaultConfig()
	if !fallback.KafkaEnabled {
		t.Error("invalid KAFKA_ENABLED should fall back to true")
	}
	if !fallback.RedisEnabled {
		t.Error("invalid REDIS_ENABLED should fall back to true")
	}
}

func TestNewServiceCreatesRealKafkaConsumerAndRedisWhenEnabled(t *testing.T) {
	cfg := DefaultConfig()
	cfg.KafkaEnabled = true
	cfg.RedisEnabled = true

	service, err := NewService(cfg, nil)
	if err != nil {
		t.Fatalf("failed to construct service: %v", err)
	}

	if service.Consumer() == nil {
		t.Fatal("expected non-nil consumer when KafkaEnabled=true")
	}

	_, isKafkaConsumer := service.Consumer().(*consumer.KafkaConsumer)
	if !isKafkaConsumer {
		t.Fatalf("expected real KafkaConsumer instance in production mode, got %T", service.Consumer())
	}

	_, isRedisRepo := service.Repository().(*store.RedisRepository)
	if !isRedisRepo {
		t.Fatalf("expected real RedisRepository in production mode, got %T", service.Repository())
	}
}

func TestServiceUsesMemoryRepositoryWhenRedisDisabled(t *testing.T) {
	cfg := DefaultConfig()
	cfg.RedisEnabled = false

	service, err := NewService(cfg, nil)
	if err != nil {
		t.Fatalf("failed to construct service: %v", err)
	}

	_, isMemoryRepo := service.Repository().(*store.MemoryRepository)
	if !isMemoryRepo {
		t.Fatalf("expected MemoryRepository when RedisEnabled=false, got %T", service.Repository())
	}
}

func TestPipelineProjectsTelemetryAndHealthEventsToRepository(t *testing.T) {
	repo := store.NewMemoryRepository()
	pipeline := NewPipeline(repo)
	ctx := context.Background()

	now := time.Now().UTC()
	telem := model.TelemetryEvent{
		EventID:      "evt-pipe-1",
		DeviceID:     "router-01",
		Timestamp:    now,
		CPU:          45.0,
		Memory:       55.0,
		LatencyMS:    30,
		PacketLoss:   0.0,
		InterfaceUp:  true,
		Connectivity: true,
	}

	if err := pipeline.HandleTelemetry(ctx, telem); err != nil {
		t.Fatalf("unexpected error from pipeline.HandleTelemetry: %v", err)
	}

	storedTelem, err := repo.GetLatestTelemetry(ctx, "router-01")
	if err != nil {
		t.Fatalf("failed to retrieve stored telemetry: %v", err)
	}
	if storedTelem.EventID != "evt-pipe-1" || storedTelem.CPU != 45.0 {
		t.Fatalf("stored telemetry mismatch: %+v", storedTelem)
	}

	health := model.HealthEvent{
		EventID:        "evt-pipe-h1",
		DeviceID:       "router-01",
		Timestamp:      now,
		PreviousStatus: "HEALTHY",
		CurrentStatus:  "WARNING",
		Score:          25,
		Reasons:        []string{"high latency"},
	}

	if err := pipeline.HandleHealth(ctx, health); err != nil {
		t.Fatalf("unexpected error from pipeline.HandleHealth: %v", err)
	}

	storedHealth, err := repo.GetLatestHealth(ctx, "router-01")
	if err != nil {
		t.Fatalf("failed to retrieve stored health: %v", err)
	}
	if storedHealth.EventID != "evt-pipe-h1" || storedHealth.CurrentStatus != "WARNING" {
		t.Fatalf("stored health mismatch: %+v", storedHealth)
	}

	devices, err := repo.ListDevices(ctx)
	if err != nil || len(devices) != 1 || devices[0] != "router-01" {
		t.Fatalf("expected device router-01 in repository, got %v", devices)
	}
}

func TestPipelineSurvivesRepositoryFailureWithoutCrashing(t *testing.T) {
	// A closed repo simulates a backend store failure
	repo := store.NewMemoryRepository()
	_ = repo.Close()

	pipeline := NewPipeline(repo)
	ctx := context.Background()

	// Should not return an error that terminates consumption
	err := pipeline.HandleTelemetry(ctx, model.TelemetryEvent{DeviceID: "router-01"})
	if err != nil {
		t.Fatalf("expected nil error (swallowed and logged) on repository failure, got: %v", err)
	}

	err = pipeline.HandleHealth(ctx, model.HealthEvent{DeviceID: "router-01"})
	if err != nil {
		t.Fatalf("expected nil error on repository failure, got: %v", err)
	}
}

func TestServiceLifecycleAndGracefulShutdown(t *testing.T) {
	for cycle := 0; cycle < 3; cycle++ {
		cfg := DefaultConfig()
		repo := store.NewMemoryRepository()
		pipeline := NewPipeline(repo)
		dispatcher := consumer.NewDispatcher(pipeline, cfg.TelemetryTopic, cfg.HealthTopic)
		memConsumer := consumer.NewMemoryConsumer(dispatcher, 10)

		service, err := NewServiceWithDependencies(cfg, pipeline, memConsumer, repo)
		if err != nil {
			t.Fatalf("cycle %d: failed to create service: %v", cycle, err)
		}

		ctx, cancel := context.WithCancel(context.Background())
		errCh := make(chan error, 1)

		go func() {
			errCh <- service.Run(ctx)
		}()

		// Enqueue a message to test active processing during lifecycle
		data, _ := json.Marshal(model.TelemetryEvent{
			EventID:      "evt-lifecycle-1",
			DeviceID:     "router-01",
			Timestamp:    time.Now().UTC(),
			CPU:          50.0,
			Connectivity: true,
		})
		_ = memConsumer.Enqueue(consumer.Message{Topic: cfg.TelemetryTopic, Value: data})

		// Allow processing before shutdown
		time.Sleep(30 * time.Millisecond)

		// Verify telemetry was saved prior to repository closure
		res, err := repo.GetLatestTelemetry(context.Background(), "router-01")
		if err != nil || res.EventID != "evt-lifecycle-1" {
			t.Fatalf("cycle %d: expected telemetry saved before shutdown, got %+v (err=%v)", cycle, res, err)
		}

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

	repo := store.NewMemoryRepository()
	service, err := NewServiceWithDependencies(cfg, nil, nil, repo)
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
