package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"distributed-network-monitor/services/analysis/aggregation"
	"distributed-network-monitor/services/analysis/anomaly"
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
	if cfg.HTTPAddr != ":8082" {
		t.Fatalf("expected HTTPAddr default ':8082', got %q", cfg.HTTPAddr)
	}
	if len(cfg.AggregationWindows) != 2 || cfg.AggregationWindows["1m"] != 1*time.Minute || cfg.AggregationWindows["5m"] != 5*time.Minute {
		t.Fatalf("expected AggregationWindows [1m, 5m], got %v", cfg.AggregationWindows)
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
	t.Setenv("ANALYSIS_HTTP_ADDR", ":9095")
	t.Setenv("AGGREGATION_WINDOWS", "30s,2m")

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
	if overridden.HTTPAddr != ":9095" {
		t.Fatalf("expected HTTPAddr ':9095', got %q", overridden.HTTPAddr)
	}
	if len(overridden.AggregationWindows) != 2 || overridden.AggregationWindows["30s"] != 30*time.Second || overridden.AggregationWindows["2m"] != 2*time.Minute {
		t.Fatalf("expected overridden AggregationWindows [30s, 2m], got %v", overridden.AggregationWindows)
	}

	// 3. Invalid fallback
	t.Setenv("KAFKA_ENABLED", "not-a-bool")
	t.Setenv("REDIS_ENABLED", "not-a-bool")
	t.Setenv("AGGREGATION_WINDOWS", "invalid,another_invalid")
	fallback := DefaultConfig()
	if !fallback.KafkaEnabled {
		t.Error("invalid KAFKA_ENABLED should fall back to true")
	}
	if !fallback.RedisEnabled {
		t.Error("invalid REDIS_ENABLED should fall back to true")
	}
	if len(fallback.AggregationWindows) != 2 || fallback.AggregationWindows["1m"] != 1*time.Minute {
		t.Error("invalid AGGREGATION_WINDOWS should fall back to default [1m, 5m]")
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

	if service.Engine() == nil {
		t.Fatal("expected non-nil AggregationEngine in Service")
	}

	if service.Detector() == nil {
		t.Fatal("expected non-nil AnomalyDetector in Service")
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

func TestPipelineProjectsTelemetryHealthRollingMetricsAndAnalysis(t *testing.T) {
	repo := store.NewMemoryRepository()
	engine := aggregation.NewEngine(map[string]time.Duration{"1m": 1 * time.Minute, "5m": 5 * time.Minute})
	detector := anomaly.NewDetector("1m")
	pipeline := NewPipeline(repo, engine, detector)
	ctx := context.Background()

	now := time.Now().UTC()

	// 1. Ingest sample 1 (normal)
	telem1 := model.TelemetryEvent{
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
	if err := pipeline.HandleTelemetry(ctx, telem1); err != nil {
		t.Fatalf("unexpected error from pipeline.HandleTelemetry: %v", err)
	}

	// 2. Ingest sample 2 with sustained critical latency (160ms) and high CPU (85%)
	telem2 := model.TelemetryEvent{
		EventID:      "evt-pipe-2",
		DeviceID:     "router-01",
		Timestamp:    now.Add(10 * time.Second),
		CPU:          85.0,
		Memory:       60.0,
		LatencyMS:    160,
		PacketLoss:   0.0,
		InterfaceUp:  true,
		Connectivity: true,
	}
	if err := pipeline.HandleTelemetry(ctx, telem2); err != nil {
		t.Fatalf("unexpected error from pipeline.HandleTelemetry: %v", err)
	}

	// Verify latest telemetry stored
	storedTelem, err := repo.GetLatestTelemetry(ctx, "router-01")
	if err != nil {
		t.Fatalf("failed to retrieve stored telemetry: %v", err)
	}
	if storedTelem.EventID != "evt-pipe-2" {
		t.Fatalf("stored telemetry mismatch: %+v", storedTelem)
	}

	// Verify rolling metrics stored
	m1m, err := repo.GetRollingMetrics(ctx, "router-01", "1m")
	if err != nil {
		t.Fatalf("failed to retrieve 1m rolling metrics: %v", err)
	}
	if m1m.SampleCount != 2 {
		t.Fatalf("expected 2 samples, got %d", m1m.SampleCount)
	}

	// Verify DeviceAnalysis generated and stored
	analysis, err := repo.GetDeviceAnalysis(ctx, "router-01")
	if err != nil {
		t.Fatalf("failed to retrieve device analysis: %v", err)
	}
	// Avg latency: (30 + 160) / 2 = 95.0ms -> WARNING (>= 50.0)
	if len(analysis.ActiveAnomalies) == 0 {
		t.Fatalf("expected active anomalies for high latency, got none")
	}
	foundLatency := false
	for _, a := range analysis.ActiveAnomalies {
		if a.Signal == "LATENCY" {
			foundLatency = true
			if a.Severity != model.SeverityWarning {
				t.Fatalf("expected Warning latency anomaly, got %v", a.Severity)
			}
		}
	}
	if !foundLatency {
		t.Fatalf("expected LATENCY anomaly, got %+v", analysis.ActiveAnomalies)
	}

	// 3. Verify health event evaluates health anomaly and saves analysis
	health := model.HealthEvent{
		EventID:        "evt-pipe-h1",
		DeviceID:       "router-01",
		Timestamp:      now.Add(15 * time.Second),
		PreviousStatus: "HEALTHY",
		CurrentStatus:  "DOWN",
		Score:          100,
		Reasons:        []string{"connection lost"},
	}

	if err := pipeline.HandleHealth(ctx, health); err != nil {
		t.Fatalf("unexpected error from pipeline.HandleHealth: %v", err)
	}

	storedHealth, err := repo.GetLatestHealth(ctx, "router-01")
	if err != nil {
		t.Fatalf("failed to retrieve stored health: %v", err)
	}
	if storedHealth.CurrentStatus != "DOWN" {
		t.Fatalf("stored health mismatch: %+v", storedHealth)
	}

	healthAnalysis, err := repo.GetDeviceAnalysis(ctx, "router-01")
	if err != nil {
		t.Fatalf("failed to retrieve device analysis: %v", err)
	}
	foundHealth := false
	for _, a := range healthAnalysis.ActiveAnomalies {
		if a.Signal == "HEALTH" && a.Severity == model.SeverityCritical {
			foundHealth = true
			break
		}
	}
	if !foundHealth {
		t.Fatalf("expected Critical HEALTH anomaly after DOWN event, got %+v", healthAnalysis.ActiveAnomalies)
	}
}

func TestPipelineSurvivesRepositoryFailureWithoutCrashing(t *testing.T) {
	// A closed repo simulates a backend store failure
	repo := store.NewMemoryRepository()
	_ = repo.Close()

	engine := aggregation.NewEngine(nil)
	detector := anomaly.NewDetector("1m")
	pipeline := NewPipeline(repo, engine, detector)
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
		cfg.HTTPAddr = "127.0.0.1:0"
		repo := store.NewMemoryRepository()
		engine := aggregation.NewEngine(cfg.AggregationWindows)
		detector := anomaly.NewDetector("1m")
		pipeline := NewPipeline(repo, engine, detector)
		dispatcher := consumer.NewDispatcher(pipeline, cfg.TelemetryTopic, cfg.HealthTopic)
		memConsumer := consumer.NewMemoryConsumer(dispatcher, 10)

		service, err := NewServiceWithDependencies(cfg, pipeline, memConsumer, repo, engine, detector)
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

		// Verify rolling metrics were saved
		m, err := repo.GetRollingMetrics(context.Background(), "router-01", "1m")
		if err != nil || m.SampleCount != 1 {
			t.Fatalf("cycle %d: expected rolling metrics saved before shutdown, got %+v (err=%v)", cycle, m, err)
		}

		// Verify analysis was saved
		a, err := repo.GetDeviceAnalysis(context.Background(), "router-01")
		if err != nil || a.DeviceID != "router-01" {
			t.Fatalf("cycle %d: expected analysis saved before shutdown, got %+v (err=%v)", cycle, a, err)
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
	cfg.HTTPAddr = "127.0.0.1:0"

	repo := store.NewMemoryRepository()
	service, err := NewServiceWithDependencies(cfg, nil, nil, repo, nil, nil)
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

func TestAnalysisAPIServerBindFailureReportsError(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen on dynamic port: %v", err)
	}
	defer ln.Close()

	cfg := DefaultConfig()
	cfg.KafkaEnabled = false
	cfg.HTTPAddr = ln.Addr().String()

	repo := store.NewMemoryRepository()
	service, err := NewServiceWithDependencies(cfg, nil, nil, repo, nil, nil)
	if err != nil {
		t.Fatalf("failed to create service: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- service.Run(ctx)
	}()

	select {
	case err := <-errCh:
		if err == nil {
			t.Fatal("expected service.Run to report error on bind collision, got nil")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("service.Run timed out instead of returning bind error")
	}
}

func TestEndToEndIntegratedAnalysisPipeline(t *testing.T) {
	cfg := DefaultConfig()
	cfg.HTTPAddr = "127.0.0.1:0"

	repo := store.NewMemoryRepository()
	engine := aggregation.NewEngine(cfg.AggregationWindows)
	detector := anomaly.NewDetector("1m")
	pipeline := NewPipeline(repo, engine, detector)
	dispatcher := consumer.NewDispatcher(pipeline, cfg.TelemetryTopic, cfg.HealthTopic)
	memConsumer := consumer.NewMemoryConsumer(dispatcher, 50)

	service, err := NewServiceWithDependencies(cfg, pipeline, memConsumer, repo, engine, detector)
	if err != nil {
		t.Fatalf("failed to construct service: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- service.Run(ctx)
	}()

	now := time.Now().UTC().Truncate(time.Millisecond)

	// Ingest sample 1: Normal telemetry
	s1, _ := json.Marshal(model.TelemetryEvent{
		EventID:      "e2e-t1",
		DeviceID:     "router-01",
		Timestamp:    now,
		CPU:          30.0,
		Memory:       40.0,
		LatencyMS:    20,
		PacketLoss:   0.0,
		InterfaceUp:  true,
		Connectivity: true,
	})
	_ = memConsumer.Enqueue(consumer.Message{Topic: cfg.TelemetryTopic, Value: s1})

	// Ingest sample 2: Degraded latency
	s2, _ := json.Marshal(model.TelemetryEvent{
		EventID:      "e2e-t2",
		DeviceID:     "router-01",
		Timestamp:    now.Add(5 * time.Second),
		CPU:          55.0,
		Memory:       50.0,
		LatencyMS:    80,
		PacketLoss:   0.5,
		InterfaceUp:  true,
		Connectivity: true,
	})
	_ = memConsumer.Enqueue(consumer.Message{Topic: cfg.TelemetryTopic, Value: s2})

	// Ingest sample 3: Sustained critical latency
	s3, _ := json.Marshal(model.TelemetryEvent{
		EventID:      "e2e-t3",
		DeviceID:     "router-01",
		Timestamp:    now.Add(10 * time.Second),
		CPU:          60.0,
		Memory:       55.0,
		LatencyMS:    160,
		PacketLoss:   1.0,
		InterfaceUp:  true,
		Connectivity: true,
	})
	_ = memConsumer.Enqueue(consumer.Message{Topic: cfg.TelemetryTopic, Value: s3})

	// Ingest health transition event
	h1, _ := json.Marshal(model.HealthEvent{
		EventID:        "e2e-h1",
		DeviceID:       "router-01",
		Timestamp:      now.Add(12 * time.Second),
		PreviousStatus: "HEALTHY",
		CurrentStatus:  "WARNING",
		Score:          25,
		Reasons:        []string{"elevated latency"},
	})
	_ = memConsumer.Enqueue(consumer.Message{Topic: cfg.HealthTopic, Value: h1})

	// Wait for event processing
	time.Sleep(100 * time.Millisecond)

	// Exercise HTTP read API routes against the service's APIHandler
	apiRoutes := service.APIHandler().Routes()

	// 1. GET /health
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	rec := httptest.NewRecorder()
	apiRoutes.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /health returned %d, want 200", rec.Code)
	}

	// 2. GET /devices
	req = httptest.NewRequest(http.MethodGet, "/devices", nil)
	rec = httptest.NewRecorder()
	apiRoutes.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /devices returned %d, want 200", rec.Code)
	}
	var devices []string
	_ = json.Unmarshal(rec.Body.Bytes(), &devices)
	if len(devices) != 1 || devices[0] != "router-01" {
		t.Fatalf("expected devices ['router-01'], got %v", devices)
	}

	// 3. GET /devices/router-01 (complete snapshot)
	req = httptest.NewRequest(http.MethodGet, "/devices/router-01", nil)
	rec = httptest.NewRecorder()
	apiRoutes.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /devices/router-01 returned %d, want 200", rec.Code)
	}
	var view struct {
		DeviceID        string               `json:"device_id"`
		LatestTelemetry model.TelemetryEvent `json:"latest_telemetry"`
		LatestHealth    model.HealthEvent    `json:"latest_health"`
		RollingMetrics  model.RollingMetrics `json:"rolling_metrics"`
		Analysis        model.DeviceAnalysis `json:"analysis"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &view); err != nil {
		t.Fatalf("failed to decode complete view: %v", err)
	}
	if view.DeviceID != "router-01" {
		t.Fatalf("expected device router-01, got %s", view.DeviceID)
	}
	if view.LatestTelemetry.EventID != "e2e-t3" {
		t.Fatalf("expected latest telemetry e2e-t3, got %s", view.LatestTelemetry.EventID)
	}
	if view.LatestHealth.CurrentStatus != "WARNING" {
		t.Fatalf("expected health status WARNING, got %s", view.LatestHealth.CurrentStatus)
	}

	// 4. GET /devices/router-01/telemetry?window=1m (verify properties, not brittle full object match)
	req = httptest.NewRequest(http.MethodGet, "/devices/router-01/telemetry?window=1m", nil)
	rec = httptest.NewRecorder()
	apiRoutes.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /devices/router-01/telemetry?window=1m returned %d, want 200", rec.Code)
	}
	var telemResp struct {
		Latest  model.TelemetryEvent  `json:"latest"`
		Rolling *model.RollingMetrics `json:"rolling"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &telemResp); err != nil {
		t.Fatalf("failed to decode rolling response: %v", err)
	}
	if telemResp.Rolling == nil {
		t.Fatal("expected rolling 1m metrics, got nil")
	}
	if telemResp.Rolling.SampleCount != 3 {
		t.Fatalf("expected 3 samples, got %d", telemResp.Rolling.SampleCount)
	}
	if telemResp.Rolling.MinLatencyMS != 20 || telemResp.Rolling.MaxLatencyMS != 160 {
		t.Fatalf("expected min 20 and max 160 latency, got min=%d max=%d",
			telemResp.Rolling.MinLatencyMS, telemResp.Rolling.MaxLatencyMS)
	}
	// Average latency: (20 + 80 + 160) / 3 = 86.67
	if telemResp.Rolling.AvgLatencyMS < 86.0 || telemResp.Rolling.AvgLatencyMS > 87.0 {
		t.Fatalf("expected avg latency ~86.67, got %.2f", telemResp.Rolling.AvgLatencyMS)
	}

	// 5. GET /devices/router-01/analysis (verify anomaly properties)
	req = httptest.NewRequest(http.MethodGet, "/devices/router-01/analysis", nil)
	rec = httptest.NewRecorder()
	apiRoutes.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /devices/router-01/analysis returned %d, want 200", rec.Code)
	}
	var analysisResp model.DeviceAnalysis
	if err := json.Unmarshal(rec.Body.Bytes(), &analysisResp); err != nil {
		t.Fatalf("failed to decode analysis: %v", err)
	}
	// Assert property: active anomaly exists for the device with WARNING severity
	if len(analysisResp.ActiveAnomalies) == 0 {
		t.Fatalf("expected at least 1 active anomaly in analysis, got 0")
	}
	hasExpectedSeverity := false
	for _, anom := range analysisResp.ActiveAnomalies {
		if anom.Severity == model.SeverityWarning {
			hasExpectedSeverity = true
			break
		}
	}
	if !hasExpectedSeverity {
		t.Fatalf("expected WARNING anomaly present in analysis: %+v", analysisResp.ActiveAnomalies)
	}

	// Graceful shutdown
	cancel()
	select {
	case err := <-errCh:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("service.Run error on shutdown: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("service did not stop within deadline")
	}
}

func TestRedisStorageFailureDoesNotTerminateConsumption(t *testing.T) {
	cfg := DefaultConfig()
	cfg.HTTPAddr = "127.0.0.1:0"

	// Closed repo simulates persistent Redis connection drops
	repo := store.NewMemoryRepository()
	_ = repo.Close()

	engine := aggregation.NewEngine(cfg.AggregationWindows)
	detector := anomaly.NewDetector("1m")
	pipeline := NewPipeline(repo, engine, detector)
	dispatcher := consumer.NewDispatcher(pipeline, cfg.TelemetryTopic, cfg.HealthTopic)
	memConsumer := consumer.NewMemoryConsumer(dispatcher, 20)

	service, err := NewServiceWithDependencies(cfg, pipeline, memConsumer, repo, engine, detector)
	if err != nil {
		t.Fatalf("failed to construct service: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- service.Run(ctx)
	}()

	// Ingest telemetry during storage failure
	payload, _ := json.Marshal(model.TelemetryEvent{
		EventID:      "evt-fail-1",
		DeviceID:     "router-01",
		Timestamp:    time.Now().UTC(),
		CPU:          50.0,
		Connectivity: true,
	})
	_ = memConsumer.Enqueue(consumer.Message{Topic: cfg.TelemetryTopic, Value: payload})

	time.Sleep(50 * time.Millisecond)

	// 1. Assert consumer is still running and alive (errCh has not emitted an error)
	select {
	case err := <-errCh:
		t.Fatalf("service.Run terminated prematurely on storage failure: %v", err)
	default:
		// Healthy: consumer didn't crash
	}

	// 2. Assert HTTP /health reports storage DISCONNECTED without crashing
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	rec := httptest.NewRecorder()
	service.APIHandler().Routes().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected /health to return 200, got %d", rec.Code)
	}
	var healthResp struct {
		Status       string            `json:"status"`
		Dependencies map[string]string `json:"dependencies"`
	}
	_ = json.Unmarshal(rec.Body.Bytes(), &healthResp)
	if healthResp.Status != "UP" {
		t.Fatalf("expected service status UP, got %s", healthResp.Status)
	}
	if healthResp.Dependencies["redis"] != "DISCONNECTED" {
		t.Fatalf("expected redis DISCONNECTED, got %s", healthResp.Dependencies["redis"])
	}

	// 3. Assert device endpoints return 503 Service Unavailable ("storage unavailable") rather than crashing or returning false 404s
	reqDev := httptest.NewRequest(http.MethodGet, "/devices/router-01", nil)
	recDev := httptest.NewRecorder()
	service.APIHandler().Routes().ServeHTTP(recDev, reqDev)
	if recDev.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 on storage failure, got %d", recDev.Code)
	}
	var errResp struct {
		Error string `json:"error"`
	}
	_ = json.Unmarshal(recDev.Body.Bytes(), &errResp)
	if errResp.Error != "storage unavailable" {
		t.Fatalf("expected error 'storage unavailable', got %q", errResp.Error)
	}
}

func TestMalformedKafkaMessagesSkippedWithoutBlocking(t *testing.T) {
	cfg := DefaultConfig()
	cfg.HTTPAddr = "127.0.0.1:0"

	repo := store.NewMemoryRepository()
	engine := aggregation.NewEngine(cfg.AggregationWindows)
	detector := anomaly.NewDetector("1m")
	pipeline := NewPipeline(repo, engine, detector)
	dispatcher := consumer.NewDispatcher(pipeline, cfg.TelemetryTopic, cfg.HealthTopic)
	memConsumer := consumer.NewMemoryConsumer(dispatcher, 20)

	service, err := NewServiceWithDependencies(cfg, pipeline, memConsumer, repo, engine, detector)
	if err != nil {
		t.Fatalf("failed to construct service: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		_ = service.Run(ctx)
	}()

	// 1. Poison pills: non-JSON, missing device ID, zero timestamp
	_ = memConsumer.Enqueue(consumer.Message{Topic: cfg.TelemetryTopic, Value: []byte("not valid json at all")})
	_ = memConsumer.Enqueue(consumer.Message{Topic: cfg.TelemetryTopic, Value: []byte(`{"event_id":"bad-1","device_id":""}`)})
	_ = memConsumer.Enqueue(consumer.Message{Topic: cfg.HealthTopic, Value: []byte(`{"event_id":"bad-2","device_id":"r1","current_status":"INVALID_STATUS"}`)})

	// 2. Subsequent valid event
	validPayload, _ := json.Marshal(model.TelemetryEvent{
		EventID:      "evt-valid-after-poison",
		DeviceID:     "router-resilient-01",
		Timestamp:    time.Now().UTC(),
		CPU:          42.0,
		Connectivity: true,
	})
	_ = memConsumer.Enqueue(consumer.Message{Topic: cfg.TelemetryTopic, Value: validPayload})

	time.Sleep(50 * time.Millisecond)

	// Assert that the valid event was processed successfully despite prior poison pills
	stored, err := repo.GetLatestTelemetry(context.Background(), "router-resilient-01")
	if err != nil {
		t.Fatalf("expected valid event to be processed after poison pills, got error: %v", err)
	}
	if stored.EventID != "evt-valid-after-poison" {
		t.Fatalf("expected event evt-valid-after-poison, got %s", stored.EventID)
	}
}

func TestConcurrentTelemetryStreamAndAPIQueries(t *testing.T) {
	cfg := DefaultConfig()
	cfg.HTTPAddr = "127.0.0.1:0"

	repo := store.NewMemoryRepository()
	engine := aggregation.NewEngine(cfg.AggregationWindows)
	detector := anomaly.NewDetector("1m")
	pipeline := NewPipeline(repo, engine, detector)
	dispatcher := consumer.NewDispatcher(pipeline, cfg.TelemetryTopic, cfg.HealthTopic)
	memConsumer := consumer.NewMemoryConsumer(dispatcher, 500)

	service, err := NewServiceWithDependencies(cfg, pipeline, memConsumer, repo, engine, detector)
	if err != nil {
		t.Fatalf("failed to construct service: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		_ = service.Run(ctx)
	}()

	apiRoutes := service.APIHandler().Routes()
	var wg sync.WaitGroup

	// Streamer goroutines: 3 workers streaming events for 5 devices
	for w := 0; w < 3; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for i := 0; i < 20; i++ {
				dev := fmt.Sprintf("router-%02d", (workerID+i)%5+1)
				data, _ := json.Marshal(model.TelemetryEvent{
					EventID:      fmt.Sprintf("e-%d-%d", workerID, i),
					DeviceID:     dev,
					Timestamp:    time.Now().UTC(),
					CPU:          float64(30 + i%50),
					LatencyMS:    int(20 + i%100),
					Connectivity: true,
				})
				_ = memConsumer.Enqueue(consumer.Message{Topic: cfg.TelemetryTopic, Value: data})
				time.Sleep(2 * time.Millisecond)
			}
		}(w)
	}

	// Reader goroutines: 3 workers querying various API routes concurrently
	for r := 0; r < 3; r++ {
		wg.Add(1)
		go func(readerID int) {
			defer wg.Done()
			for i := 0; i < 20; i++ {
				dev := fmt.Sprintf("router-%02d", (readerID+i)%5+1)
				paths := []string{
					"/health",
					"/devices",
					fmt.Sprintf("/devices/%s/telemetry", dev),
					fmt.Sprintf("/devices/%s/analysis", dev),
				}
				p := paths[i%len(paths)]
				req := httptest.NewRequest(http.MethodGet, p, nil)
				rec := httptest.NewRecorder()
				apiRoutes.ServeHTTP(rec, req)
				// Status should be either 200 or 404 (not yet ingested); never 500 or race panic
				if rec.Code != http.StatusOK && rec.Code != http.StatusNotFound {
					t.Errorf("unexpected status %d on path %s", rec.Code, p)
				}
				time.Sleep(2 * time.Millisecond)
			}
		}(r)
	}

	wg.Wait()
}
