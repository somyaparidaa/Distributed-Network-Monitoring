package main

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"distributed-network-monitor/services/monitoring/device"
	"distributed-network-monitor/services/monitoring/health"
	"distributed-network-monitor/services/monitoring/polling"
)

func TestDefaultConfigHasExpectedDevices(t *testing.T) {
	cfg := DefaultConfig()
	if len(cfg.Devices) != 3 {
		t.Fatalf("expected 3 default devices, got %d", len(cfg.Devices))
	}

	expected := map[string]string{
		"router-01": "http://localhost:8080/metrics/router-01",
		"router-02": "http://localhost:8080/metrics/router-02",
		"router-03": "http://localhost:8080/metrics/router-03",
	}

	for _, d := range cfg.Devices {
		expectedURL, exists := expected[d.ID]
		if !exists {
			t.Errorf("unexpected device ID %q", d.ID)
			continue
		}
		if d.MetricsURL != expectedURL {
			t.Errorf("device %s URL = %q, want %q", d.ID, d.MetricsURL, expectedURL)
		}
	}
}

func TestRegistryValidDevices(t *testing.T) {
	devices := []device.MonitoredDevice{
		{ID: "router-01", MetricsURL: "http://localhost:8080/metrics/router-01"},
		{ID: "router-02", MetricsURL: "http://localhost:8080/metrics/router-02"},
	}

	reg, err := device.NewRegistry(devices)
	if err != nil {
		t.Fatalf("unexpected error creating registry: %v", err)
	}

	if reg.Len() != 2 {
		t.Fatalf("registry length = %d, want 2", reg.Len())
	}

	d1, ok := reg.Get("router-01")
	if !ok || d1.ID != "router-01" || d1.MetricsURL != "http://localhost:8080/metrics/router-01" {
		t.Fatalf("unexpected device router-01: %+v", d1)
	}

	_, ok = reg.Get("router-99")
	if ok {
		t.Fatal("expected router-99 not to be present")
	}

	list := reg.List()
	if len(list) != 2 || list[0].ID != "router-01" || list[1].ID != "router-02" {
		t.Fatalf("unexpected ordered list: %+v", list)
	}
}

func TestRegistryValidationErrors(t *testing.T) {
	tests := []struct {
		name    string
		devices []device.MonitoredDevice
	}{
		{
			name: "empty device ID",
			devices: []device.MonitoredDevice{
				{ID: "", MetricsURL: "http://localhost:8080/metrics/router-01"},
			},
		},
		{
			name: "whitespace device ID",
			devices: []device.MonitoredDevice{
				{ID: "   ", MetricsURL: "http://localhost:8080/metrics/router-01"},
			},
		},
		{
			name: "empty URL",
			devices: []device.MonitoredDevice{
				{ID: "router-01", MetricsURL: ""},
			},
		},
		{
			name: "invalid URL without host",
			devices: []device.MonitoredDevice{
				{ID: "router-01", MetricsURL: "not-a-valid-url"},
			},
		},
		{
			name: "duplicate device ID",
			devices: []device.MonitoredDevice{
				{ID: "router-01", MetricsURL: "http://localhost:8080/metrics/router-01"},
				{ID: "router-01", MetricsURL: "http://localhost:8080/metrics/other"},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := device.NewRegistry(tc.devices)
			if err == nil {
				t.Fatalf("expected error for case %q, got nil", tc.name)
			}
		})
	}
}

func TestConfigParsingAndDefaults(t *testing.T) {
	// 1. Verify defaults
	cfg := DefaultConfig()
	if cfg.PollInterval != 2*time.Second {
		t.Fatalf("PollInterval default = %v, want 2s", cfg.PollInterval)
	}
	if cfg.PollTimeout != time.Second {
		t.Fatalf("PollTimeout default = %v, want 1s", cfg.PollTimeout)
	}
	if cfg.MaxRetries != 2 {
		t.Fatalf("MaxRetries default = %d, want 2", cfg.MaxRetries)
	}
	if cfg.RetryBackoff != 50*time.Millisecond {
		t.Fatalf("RetryBackoff default = %v, want 50ms", cfg.RetryBackoff)
	}
	if cfg.FailureThreshold != 3 {
		t.Fatalf("FailureThreshold default = %d, want 3", cfg.FailureThreshold)
	}
	if !cfg.KafkaEnabled {
		t.Fatal("KafkaEnabled default should be true")
	}
	if len(cfg.KafkaBrokers) != 1 || cfg.KafkaBrokers[0] != "localhost:9092" {
		t.Fatalf("KafkaBrokers default = %v, want [localhost:9092]", cfg.KafkaBrokers)
	}
	if cfg.TelemetryTopic != "network.telemetry" || cfg.HealthTopic != "network.health-events" {
		t.Fatalf("unexpected Kafka topic defaults: telemetry=%q health=%q", cfg.TelemetryTopic, cfg.HealthTopic)
	}
	if cfg.HTTPAddr != ":8081" {
		t.Fatalf("HTTPAddr default = %q, want :8081", cfg.HTTPAddr)
	}

	// 2. Test valid environment variable overrides
	t.Setenv("POLL_INTERVAL", "500ms")
	t.Setenv("POLL_TIMEOUT", "250ms")
	t.Setenv("MAX_RETRIES", "4")
	t.Setenv("RETRY_BACKOFF", "100ms")
	t.Setenv("FAILURE_THRESHOLD", "5")
	t.Setenv("KAFKA_ENABLED", "false")
	t.Setenv("KAFKA_BROKERS", "broker1:9092,broker2:9092")
	t.Setenv("KAFKA_TELEMETRY_TOPIC", "custom.telemetry")
	t.Setenv("KAFKA_HEALTH_TOPIC", "custom.health")
	t.Setenv("MONITORING_HTTP_ADDR", ":9999")

	overridden := DefaultConfig()
	if overridden.PollInterval != 500*time.Millisecond {
		t.Errorf("overridden PollInterval = %v, want 500ms", overridden.PollInterval)
	}
	if overridden.PollTimeout != 250*time.Millisecond {
		t.Errorf("overridden PollTimeout = %v, want 250ms", overridden.PollTimeout)
	}
	if overridden.MaxRetries != 4 {
		t.Errorf("overridden MaxRetries = %d, want 4", overridden.MaxRetries)
	}
	if overridden.RetryBackoff != 100*time.Millisecond {
		t.Errorf("overridden RetryBackoff = %v, want 100ms", overridden.RetryBackoff)
	}
	if overridden.FailureThreshold != 5 {
		t.Errorf("overridden FailureThreshold = %d, want 5", overridden.FailureThreshold)
	}
	if overridden.KafkaEnabled != false {
		t.Error("overridden KafkaEnabled should be false")
	}
	if len(overridden.KafkaBrokers) != 2 || overridden.KafkaBrokers[1] != "broker2:9092" {
		t.Errorf("overridden KafkaBrokers = %v", overridden.KafkaBrokers)
	}
	if overridden.TelemetryTopic != "custom.telemetry" || overridden.HealthTopic != "custom.health" {
		t.Errorf("overridden topics: telem=%q health=%q", overridden.TelemetryTopic, overridden.HealthTopic)
	}
	if overridden.HTTPAddr != ":9999" {
		t.Errorf("overridden HTTPAddr = %q, want :9999", overridden.HTTPAddr)
	}

	// 3. Test invalid environment overrides safely falling back to defaults
	t.Setenv("POLL_INTERVAL", "invalid-duration")
	t.Setenv("MAX_RETRIES", "not-a-number")
	t.Setenv("KAFKA_ENABLED", "not-a-bool")
	t.Setenv("FAILURE_THRESHOLD", "-10")

	fallback := DefaultConfig()
	if fallback.PollInterval != 2*time.Second {
		t.Errorf("invalid PollInterval did not fall back to default: %v", fallback.PollInterval)
	}
	if fallback.MaxRetries != 2 {
		t.Errorf("invalid MaxRetries did not fall back to default: %d", fallback.MaxRetries)
	}
	if fallback.KafkaEnabled != true {
		t.Errorf("invalid KafkaEnabled did not fall back to default: %v", fallback.KafkaEnabled)
	}
	if fallback.FailureThreshold != 3 {
		t.Errorf("invalid FailureThreshold did not fall back to default: %d", fallback.FailureThreshold)
	}
}

func TestAPIServerBindFailureReportsError(t *testing.T) {
	// Occupy a port first
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen on test port: %v", err)
	}
	defer ln.Close()

	cfg := DefaultConfig()
	cfg.HTTPAddr = ln.Addr().String() // Deliberately conflict with ln

	service, err := NewService(cfg)
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
			t.Fatal("expected service.Run to return error on port bind conflict, got nil")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("service.Run timed out instead of reporting bind error")
	}
}

func TestServiceLifecycleAndNoGoroutineLeaks(t *testing.T) {
	// Repeated start/stop cycles to verify goroutines do not accumulate
	for cycle := 0; cycle < 3; cycle++ {
		cfg := DefaultConfig()
		cfg.HTTPAddr = "127.0.0.1:0"
		cfg.KafkaEnabled = false // Also exercises Kafka-disabled lifecycle

		service, err := NewService(cfg)
		if err != nil {
			t.Fatalf("cycle %d: create service: %v", cycle, err)
		}

		ctx, cancel := context.WithCancel(context.Background())
		errCh := make(chan error, 1)

		go func() {
			errCh <- service.Run(ctx)
		}()

		time.Sleep(30 * time.Millisecond)
		cancel()

		select {
		case err := <-errCh:
			if err != nil {
				t.Fatalf("cycle %d: service.Run error: %v", cycle, err)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("cycle %d: service.Run did not stop within deadline", cycle)
		}
	}
}

func TestEndToEndIntegratedMonitoringPipeline(t *testing.T) {
	var pollCount atomic.Int64

	// Mock simulator device server
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		pollCount.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(polling.Telemetry{
			DeviceID:     "router-01",
			Condition:    "NORMAL",
			CPU:          40.0,
			Memory:       50.0,
			LatencyMS:    20,
			PacketLoss:   0.1,
			InterfaceUp:  true,
			Connectivity: true,
			Timestamp:    time.Now().UTC(),
		})
	}))
	defer ts.Close()

	cfg := Config{
		Devices: []DeviceConfig{
			{ID: "router-01", MetricsURL: ts.URL},
		},
		PollInterval:     25 * time.Millisecond,
		PollTimeout:      100 * time.Millisecond,
		MaxRetries:       1,
		RetryBackoff:     5 * time.Millisecond,
		FailureThreshold: 2,
		KafkaEnabled:     true,
		KafkaBrokers:     []string{"mock:9092"},
		TelemetryTopic:   "network.telemetry",
		HealthTopic:      "network.health-events",
		HTTPAddr:         "127.0.0.1:0",
	}

	service, err := NewService(cfg)
	if err != nil {
		t.Fatalf("NewService failed: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- service.Run(ctx)
	}()

	// Wait for polling cycles
	time.Sleep(100 * time.Millisecond)

	// Verify polling occurred
	if pollCount.Load() == 0 {
		t.Fatal("expected active polling to the mock server, got 0 polls")
	}

	// Verify in-memory telemetry store populated
	telem, ok := service.Store().Get("router-01")
	if !ok || telem.DeviceID != "router-01" {
		t.Fatalf("expected telemetry in store, got: %+v (ok=%v)", telem, ok)
	}

	// Verify health evaluation populated
	assessment, ok := service.HealthStore().Get("router-01")
	if !ok || assessment.Status != health.StatusHealthy {
		t.Fatalf("expected HEALTHY assessment, got: %+v (ok=%v)", assessment, ok)
	}

	// Query the API handler directly
	req := httptest.NewRequest(http.MethodGet, "/devices/router-01", nil)
	w := httptest.NewRecorder()
	service.APIHandler().Routes().ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("GET /devices/router-01 returned status %d, want 200", w.Code)
	}

	// Trigger graceful shutdown
	cancel()

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("service.Run returned error on shutdown: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("service.Run did not stop within deadline")
	}
}
