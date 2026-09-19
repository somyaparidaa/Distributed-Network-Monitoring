package api

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"distributed-network-monitor/services/monitoring/device"
	"distributed-network-monitor/services/monitoring/health"
	"distributed-network-monitor/services/monitoring/metrics"
	"distributed-network-monitor/services/monitoring/polling"
)

func setupTestAPI(t *testing.T) (*Handler, *device.Registry, *polling.Store, *health.Store, *polling.StateTracker) {
	t.Helper()
	reg, err := device.NewRegistry([]device.MonitoredDevice{
		{ID: "router-01", MetricsURL: "http://localhost:8080/metrics/router-01"},
		{ID: "router-02", MetricsURL: "http://localhost:8080/metrics/router-02"},
	})
	if err != nil {
		t.Fatalf("create test registry: %v", err)
	}

	store := polling.NewStore()
	healthStore := health.NewStore()
	tracker := polling.NewStateTracker()

	h := NewHandler(reg, store, healthStore, tracker)
	return h, reg, store, healthStore, tracker
}

func TestHealthEndpoint(t *testing.T) {
	h, _, _, _, _ := setupTestAPI(t)
	server := httptest.NewServer(h.Routes())
	defer server.Close()

	// GET /health
	resp, err := http.Get(server.URL + "/health")
	if err != nil {
		t.Fatalf("GET /health failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "application/json" {
		t.Fatalf("Content-Type = %q, want application/json", ct)
	}

	var sh ServiceHealth
	if err := json.NewDecoder(resp.Body).Decode(&sh); err != nil {
		t.Fatalf("decode ServiceHealth failed: %v", err)
	}
	if sh.Status != "UP" || sh.MonitoredDevices != 2 {
		t.Fatalf("unexpected ServiceHealth: %+v", sh)
	}

	// Non-GET method rejected with 405 and JSON error
	postResp, err := http.Post(server.URL+"/health", "application/json", nil)
	if err != nil {
		t.Fatalf("POST /health failed: %v", err)
	}
	defer postResp.Body.Close()

	if postResp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want 405", postResp.StatusCode)
	}
	if ct := postResp.Header.Get("Content-Type"); ct != "application/json" {
		t.Fatalf("Content-Type = %q, want application/json", ct)
	}
}

func TestMonitoringLiveAndReadinessEndpoints(t *testing.T) {
	h, reg, store, healthStore, tracker := setupTestAPI(t)
	server := httptest.NewServer(h.Routes())
	defer server.Close()

	// 1. GET /health/live returns 200 UP
	respLive, err := http.Get(server.URL + "/health/live")
	if err != nil {
		t.Fatalf("GET /health/live failed: %v", err)
	}
	defer respLive.Body.Close()
	if respLive.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", respLive.StatusCode)
	}
	var sh ServiceHealth
	if err := json.NewDecoder(respLive.Body).Decode(&sh); err != nil {
		t.Fatalf("decode live failed: %v", err)
	}
	if sh.Status != "UP" {
		t.Fatalf("live status = %s, want UP", sh.Status)
	}

	// 2. GET /health/ready with kafka disabled returns 200 READY
	respReady, err := http.Get(server.URL + "/health/ready")
	if err != nil {
		t.Fatalf("GET /health/ready failed: %v", err)
	}
	defer respReady.Body.Close()
	if respReady.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", respReady.StatusCode)
	}
	var rh ReadinessHealth
	if err := json.NewDecoder(respReady.Body).Decode(&rh); err != nil {
		t.Fatalf("decode ready failed: %v", err)
	}
	if rh.Status != "READY" || rh.Dependencies["kafka"] != "DISABLED" {
		t.Fatalf("unexpected ready response: %+v", rh)
	}

	// 3. GET /health/ready with Kafka enabled and unreachable broker returns 503 NOT_READY
	hWithKafka := NewHandlerWithDependencies(reg, store, healthStore, tracker, nil, true, []string{"127.0.0.1:54321"})
	serverKafka := httptest.NewServer(hWithKafka.Routes())
	defer serverKafka.Close()

	respDown, err := http.Get(serverKafka.URL + "/health/ready")
	if err != nil {
		t.Fatalf("GET /health/ready with bad kafka failed: %v", err)
	}
	defer respDown.Body.Close()
	if respDown.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", respDown.StatusCode)
	}
	var rhDown ReadinessHealth
	if err := json.NewDecoder(respDown.Body).Decode(&rhDown); err != nil {
		t.Fatalf("decode ready down failed: %v", err)
	}
	if rhDown.Status != "NOT_READY" || rhDown.Dependencies["kafka"] != "DISCONNECTED" {
		t.Fatalf("unexpected ready down response: %+v", rhDown)
	}

	// 4. Liveness still returns 200 UP even when Kafka is down
	respLiveEvenDown, err := http.Get(serverKafka.URL + "/health/live")
	if err != nil {
		t.Fatalf("GET /health/live failed: %v", err)
	}
	defer respLiveEvenDown.Body.Close()
	if respLiveEvenDown.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", respLiveEvenDown.StatusCode)
	}
}

func TestMonitoringMetricsEndpoint(t *testing.T) {
	h, _, _, _, _ := setupTestAPI(t)
	mux := h.Routes()

	// Simulate increments so the labeled vector metrics are initialized and rendered
	metrics.PollAttemptsTotal.WithLabelValues("success").Inc()
	metrics.HealthEvaluationsTotal.WithLabelValues("HEALTHY").Inc()

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 from /metrics, got %d", rec.Code)
	}
	body := rec.Body.String()
	if !strings.Contains(body, "monitoring_poll_attempts_total") {
		t.Errorf("expected body to contain 'monitoring_poll_attempts_total', got:\n%s", body)
	}
	if !strings.Contains(body, "monitoring_health_evaluations_total") {
		t.Errorf("expected body to contain 'monitoring_health_evaluations_total', got:\n%s", body)
	}
}

func TestDevicesEndpoints(t *testing.T) {
	h, _, store, healthStore, tracker := setupTestAPI(t)
	server := httptest.NewServer(h.Routes())
	defer server.Close()

	now := time.Now().UTC().Truncate(time.Millisecond)

	// Populate router-01 state: UP, HEALTHY, with telemetry
	store.Set("router-01", polling.Telemetry{
		DeviceID:     "router-01",
		CPU:          32.0,
		Memory:       45.0,
		LatencyMS:    20,
		PacketLoss:   0.1,
		InterfaceUp:  true,
		Connectivity: true,
		Timestamp:    now,
	})
	tracker.RecordSuccess("router-01")
	healthStore.Set("router-01", health.Assessment{
		DeviceID:    "router-01",
		Status:      health.StatusHealthy,
		Score:       0,
		EvaluatedAt: now,
	})

	// Populate router-02 state: DOWN with preserved telemetry
	store.Set("router-02", polling.Telemetry{
		DeviceID:     "router-02",
		CPU:          95.0,
		Memory:       80.0,
		LatencyMS:    220,
		PacketLoss:   8.0,
		InterfaceUp:  true,
		Connectivity: true,
		Timestamp:    now,
	})
	tracker.RecordFailure("router-02", errors.New("503"), 1)
	healthStore.Set("router-02", health.Assessment{
		DeviceID:    "router-02",
		Status:      health.StatusDown,
		Score:       100,
		Reasons:     []string{"transport unavailable"},
		EvaluatedAt: now,
	})

	// 1. GET /devices returns array of summaries
	t.Run("GET /devices", func(t *testing.T) {
		resp, err := http.Get(server.URL + "/devices")
		if err != nil {
			t.Fatalf("GET /devices failed: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			t.Fatalf("status = %d, want 200", resp.StatusCode)
		}
		if ct := resp.Header.Get("Content-Type"); ct != "application/json" {
			t.Fatalf("Content-Type = %q, want application/json", ct)
		}

		var summaries []DeviceSummary
		if err := json.NewDecoder(resp.Body).Decode(&summaries); err != nil {
			t.Fatalf("decode summaries: %v", err)
		}
		if len(summaries) != 2 {
			t.Fatalf("expected 2 summaries, got %d", len(summaries))
		}
		if summaries[0].DeviceID != "router-01" || summaries[0].Status != polling.StatusUp {
			t.Fatalf("unexpected summary for router-01: %+v", summaries[0])
		}
		if summaries[1].DeviceID != "router-02" || summaries[1].Status != polling.StatusDown {
			t.Fatalf("unexpected summary for router-02: %+v", summaries[1])
		}
	})

	// 2. GET /devices/router-01 returns single summary
	t.Run("GET /devices/router-01", func(t *testing.T) {
		resp, err := http.Get(server.URL + "/devices/router-01")
		if err != nil {
			t.Fatalf("GET failed: %v", err)
		}
		defer resp.Body.Close()

		var s DeviceSummary
		if err := json.NewDecoder(resp.Body).Decode(&s); err != nil {
			t.Fatalf("decode summary: %v", err)
		}
		if s.DeviceID != "router-01" || s.Status != polling.StatusUp || s.Health.Status != health.StatusHealthy {
			t.Fatalf("unexpected device summary: %+v", s)
		}
	})

	// 3. GET /devices/router-02 preserves last known state while DOWN
	t.Run("GET /devices/router-02 while DOWN returns preserved state", func(t *testing.T) {
		resp, err := http.Get(server.URL + "/devices/router-02")
		if err != nil {
			t.Fatalf("GET failed: %v", err)
		}
		defer resp.Body.Close()

		var s DeviceSummary
		if err := json.NewDecoder(resp.Body).Decode(&s); err != nil {
			t.Fatalf("decode summary: %v", err)
		}
		if s.Status != polling.StatusDown || s.Health.Status != health.StatusDown {
			t.Fatalf("expected router-02 status to be DOWN, got: %+v", s)
		}
		if s.LatestTelemetry == nil || s.LatestTelemetry.CPU != 95.0 {
			t.Fatalf("expected preserved latest telemetry on router-02, got: %+v", s.LatestTelemetry)
		}
	})

	// 4. GET /devices/router-02/metrics returns preserved telemetry while DOWN
	t.Run("GET /devices/router-02/metrics returns preserved telemetry", func(t *testing.T) {
		resp, err := http.Get(server.URL + "/devices/router-02/metrics")
		if err != nil {
			t.Fatalf("GET metrics failed: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			t.Fatalf("status = %d, want 200", resp.StatusCode)
		}

		var telem polling.Telemetry
		if err := json.NewDecoder(resp.Body).Decode(&telem); err != nil {
			t.Fatalf("decode telemetry: %v", err)
		}
		if telem.DeviceID != "router-02" || telem.CPU != 95.0 {
			t.Fatalf("unexpected telemetry: %+v", telem)
		}
	})

	// 5. GET /devices/router-01/health returns health assessment
	t.Run("GET /devices/router-01/health", func(t *testing.T) {
		resp, err := http.Get(server.URL + "/devices/router-01/health")
		if err != nil {
			t.Fatalf("GET health failed: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			t.Fatalf("status = %d, want 200", resp.StatusCode)
		}

		var a health.Assessment
		if err := json.NewDecoder(resp.Body).Decode(&a); err != nil {
			t.Fatalf("decode assessment: %v", err)
		}
		if a.DeviceID != "router-01" || a.Status != health.StatusHealthy {
			t.Fatalf("unexpected assessment: %+v", a)
		}
	})
}

func TestErrorEdgeCases(t *testing.T) {
	h, reg, _, _, _ := setupTestAPI(t)
	server := httptest.NewServer(h.Routes())
	defer server.Close()

	// Register router-03 without telemetry or health
	_ = reg

	tests := []struct {
		name           string
		method         string
		path           string
		expectedStatus int
		expectedMsg    string
	}{
		{
			name:           "unknown device 404",
			method:         http.MethodGet,
			path:           "/devices/router-99",
			expectedStatus: http.StatusNotFound,
			expectedMsg:    "device not found",
		},
		{
			name:           "unknown device metrics 404",
			method:         http.MethodGet,
			path:           "/devices/router-99/metrics",
			expectedStatus: http.StatusNotFound,
			expectedMsg:    "device not found",
		},
		{
			name:           "unknown device health 404",
			method:         http.MethodGet,
			path:           "/devices/router-99/health",
			expectedStatus: http.StatusNotFound,
			expectedMsg:    "device not found",
		},
		{
			name:           "registered device with no telemetry yet",
			method:         http.MethodGet,
			path:           "/devices/router-01/metrics",
			expectedStatus: http.StatusNotFound,
			expectedMsg:    "no telemetry available yet",
		},
		{
			name:           "registered device with no health yet",
			method:         http.MethodGet,
			path:           "/devices/router-01/health",
			expectedStatus: http.StatusNotFound,
			expectedMsg:    "no health assessment available yet",
		},
		{
			name:           "nested invalid path 404",
			method:         http.MethodGet,
			path:           "/devices/router-01/metrics/extra",
			expectedStatus: http.StatusNotFound,
			expectedMsg:    "endpoint not found",
		},
		{
			name:           "non-GET method 405",
			method:         http.MethodPost,
			path:           "/devices",
			expectedStatus: http.StatusMethodNotAllowed,
			expectedMsg:    "method not allowed",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req, _ := http.NewRequest(tc.method, server.URL+tc.path, nil)
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatalf("request failed: %v", err)
			}
			defer resp.Body.Close()

			if resp.StatusCode != tc.expectedStatus {
				t.Fatalf("status = %d, want %d", resp.StatusCode, tc.expectedStatus)
			}
			if ct := resp.Header.Get("Content-Type"); ct != "application/json" {
				t.Fatalf("Content-Type = %q, want application/json", ct)
			}

			var errResp errorResponse
			if err := json.NewDecoder(resp.Body).Decode(&errResp); err != nil {
				t.Fatalf("decode errorResponse: %v", err)
			}
			if errResp.Error != tc.expectedMsg {
				t.Fatalf("error = %q, want %q", errResp.Error, tc.expectedMsg)
			}
		})
	}
}

func TestServiceHealthRemainsUpWhenDevicesDown(t *testing.T) {
	h, _, _, healthStore, tracker := setupTestAPI(t)
	server := httptest.NewServer(h.Routes())
	defer server.Close()

	// Fail all devices
	tracker.RecordFailure("router-01", errors.New("err"), 1)
	tracker.RecordFailure("router-02", errors.New("err"), 1)
	healthStore.Set("router-01", health.Assessment{DeviceID: "router-01", Status: health.StatusDown})
	healthStore.Set("router-02", health.Assessment{DeviceID: "router-02", Status: health.StatusDown})

	resp, err := http.Get(server.URL + "/health")
	if err != nil {
		t.Fatalf("GET /health failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	var sh ServiceHealth
	_ = json.NewDecoder(resp.Body).Decode(&sh)
	if sh.Status != "UP" {
		t.Fatalf("service health should remain UP when fleet is down, got: %q", sh.Status)
	}
}
