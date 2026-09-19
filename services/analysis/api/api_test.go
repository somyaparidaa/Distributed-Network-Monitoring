package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"distributed-network-monitor/services/analysis/model"
	"distributed-network-monitor/services/analysis/store"
)

func setupTestHandler() (*Handler, *store.MemoryRepository) {
	repo := store.NewMemoryRepository()
	windows := map[string]time.Duration{
		"1m": 1 * time.Minute,
		"5m": 5 * time.Minute,
	}
	handler := NewHandler(repo, true, true, windows)
	return handler, repo
}

func TestHealthEndpoint(t *testing.T) {
	handler, repo := setupTestHandler()

	// 1. GET /health when repo is healthy
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	rec := httptest.NewRecorder()
	handler.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rec.Code)
	}

	var health ServiceHealth
	if err := json.Unmarshal(rec.Body.Bytes(), &health); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if health.Status != "UP" {
		t.Fatalf("expected status UP, got %s", health.Status)
	}
	if health.Dependencies["kafka"] != "ENABLED" {
		t.Fatalf("expected kafka ENABLED, got %s", health.Dependencies["kafka"])
	}
	if health.Dependencies["redis"] != "CONNECTED" {
		t.Fatalf("expected redis CONNECTED, got %s", health.Dependencies["redis"])
	}

	// 2. GET /health when repo is closed (simulate redis ping failure)
	_ = repo.Close()
	req2 := httptest.NewRequest(http.MethodGet, "/health", nil)
	rec2 := httptest.NewRecorder()
	handler.Routes().ServeHTTP(rec2, req2)

	if rec2.Code != http.StatusOK {
		t.Fatalf("expected status 200 even when redis is down, got %d", rec2.Code)
	}

	var healthDown ServiceHealth
	if err := json.Unmarshal(rec2.Body.Bytes(), &healthDown); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if healthDown.Dependencies["redis"] != "DISCONNECTED" {
		t.Fatalf("expected redis DISCONNECTED, got %s", healthDown.Dependencies["redis"])
	}

	// 3. Disabled flags
	handlerDisabled := NewHandler(repo, false, false, nil)
	req3 := httptest.NewRequest(http.MethodGet, "/health", nil)
	rec3 := httptest.NewRecorder()
	handlerDisabled.Routes().ServeHTTP(rec3, req3)

	var healthDisabled ServiceHealth
	_ = json.Unmarshal(rec3.Body.Bytes(), &healthDisabled)
	if healthDisabled.Dependencies["kafka"] != "DISABLED" || healthDisabled.Dependencies["redis"] != "DISABLED" {
		t.Fatalf("expected DISABLED dependencies, got %+v", healthDisabled.Dependencies)
	}

	// 4. Method Not Allowed on /health
	reqPost := httptest.NewRequest(http.MethodPost, "/health", nil)
	recPost := httptest.NewRecorder()
	handler.Routes().ServeHTTP(recPost, reqPost)

	if recPost.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405 Method Not Allowed, got %d", recPost.Code)
	}
	if recPost.Header().Get("Allow") != http.MethodGet {
		t.Fatalf("expected Allow: GET header, got %q", recPost.Header().Get("Allow"))
	}
}

func TestAnalysisLiveAndReadinessEndpoints(t *testing.T) {
	repo := store.NewMemoryRepository()
	windows := map[string]time.Duration{"1m": 1 * time.Minute}
	handler := NewHandler(repo, false, true, windows)

	// 1. Live probe returns 200 UP
	reqLive := httptest.NewRequest(http.MethodGet, "/health/live", nil)
	recLive := httptest.NewRecorder()
	handler.Routes().ServeHTTP(recLive, reqLive)
	if recLive.Code != http.StatusOK {
		t.Fatalf("expected 200 from /health/live, got %d", recLive.Code)
	}

	// 2. Ready probe with healthy redis returns 200 READY
	reqReady := httptest.NewRequest(http.MethodGet, "/health/ready", nil)
	recReady := httptest.NewRecorder()
	handler.Routes().ServeHTTP(recReady, reqReady)
	if recReady.Code != http.StatusOK {
		t.Fatalf("expected 200 from /health/ready, got %d", recReady.Code)
	}
	var shReady ServiceHealth
	if err := json.Unmarshal(recReady.Body.Bytes(), &shReady); err != nil {
		t.Fatalf("decode ready: %v", err)
	}
	if shReady.Status != "READY" || shReady.Dependencies["redis"] != "CONNECTED" {
		t.Fatalf("unexpected ready response: %+v", shReady)
	}

	// 3. When Redis fails, /health/ready returns 503 NOT_READY while /health/live remains 200 UP
	_ = repo.Close()
	recReadyDown := httptest.NewRecorder()
	handler.Routes().ServeHTTP(recReadyDown, reqReady)
	if recReadyDown.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 from /health/ready when redis down, got %d", recReadyDown.Code)
	}
	var shDown ServiceHealth
	if err := json.Unmarshal(recReadyDown.Body.Bytes(), &shDown); err != nil {
		t.Fatalf("decode ready down: %v", err)
	}
	if shDown.Status != "NOT_READY" || shDown.Dependencies["redis"] != "DISCONNECTED" {
		t.Fatalf("unexpected ready down: %+v", shDown)
	}

	recLiveStillUp := httptest.NewRecorder()
	handler.Routes().ServeHTTP(recLiveStillUp, reqLive)
	if recLiveStillUp.Code != http.StatusOK {
		t.Fatalf("expected 200 from /health/live even when redis down, got %d", recLiveStillUp.Code)
	}
}

func TestAnalysisMetricsEndpoint(t *testing.T) {
	handler, _ := setupTestHandler()

	// Trigger a health check so dependency gauges are set
	healthReq := httptest.NewRequest(http.MethodGet, "/health", nil)
	healthRec := httptest.NewRecorder()
	handler.Routes().ServeHTTP(healthRec, healthReq)

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()
	handler.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 from /metrics, got %d", rec.Code)
	}
	body := rec.Body.String()
	if !strings.Contains(body, "analysis_dependency_available") {
		t.Errorf("expected body to contain 'analysis_dependency_available', got:\n%s", body)
	}
}

func TestDevicesRootEndpoint(t *testing.T) {
	handler, repo := setupTestHandler()

	// 1. Empty list
	req := httptest.NewRequest(http.MethodGet, "/devices", nil)
	rec := httptest.NewRecorder()
	handler.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rec.Code)
	}

	var devices []string
	if err := json.Unmarshal(rec.Body.Bytes(), &devices); err != nil {
		t.Fatalf("failed to parse devices: %v", err)
	}
	if len(devices) != 0 {
		t.Fatalf("expected 0 devices, got %d", len(devices))
	}

	// 2. Add devices
	_ = repo.SaveLatestTelemetry(context.Background(), "router-01", model.TelemetryEvent{DeviceID: "router-01"})
	_ = repo.SaveLatestTelemetry(context.Background(), "router-02", model.TelemetryEvent{DeviceID: "router-02"})

	req = httptest.NewRequest(http.MethodGet, "/devices", nil)
	rec = httptest.NewRecorder()
	handler.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rec.Code)
	}
	_ = json.Unmarshal(rec.Body.Bytes(), &devices)
	if len(devices) != 2 {
		t.Fatalf("expected 2 devices, got %v", devices)
	}

	// 3. Method Not Allowed
	reqPost := httptest.NewRequest(http.MethodPost, "/devices", nil)
	recPost := httptest.NewRecorder()
	handler.Routes().ServeHTTP(recPost, reqPost)
	if recPost.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405 on POST /devices, got %d", recPost.Code)
	}

	// 4. Storage failure returns 503 Service Unavailable with {"error":"storage unavailable"}
	_ = repo.Close()
	reqDown := httptest.NewRequest(http.MethodGet, "/devices", nil)
	recDown := httptest.NewRecorder()
	handler.Routes().ServeHTTP(recDown, reqDown)
	if recDown.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 on closed repo, got %d", recDown.Code)
	}
	var errResp errorResponse
	_ = json.Unmarshal(recDown.Body.Bytes(), &errResp)
	if errResp.Error != "storage unavailable" {
		t.Fatalf("expected 'storage unavailable', got %q", errResp.Error)
	}
}

func TestDeviceCompleteSnapshot(t *testing.T) {
	handler, repo := setupTestHandler()
	ctx := context.Background()
	now := time.Now().UTC()

	// 1. Incomplete device returns 404
	req := httptest.NewRequest(http.MethodGet, "/devices/router-01", nil)
	rec := httptest.NewRecorder()
	handler.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404 for missing device, got %d", rec.Code)
	}

	// Add only telemetry -> still incomplete -> 404
	_ = repo.SaveLatestTelemetry(ctx, "router-01", model.TelemetryEvent{DeviceID: "router-01", EventID: "e1", CPU: 50.0})
	rec = httptest.NewRecorder()
	handler.Routes().ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404 when health is missing, got %d", rec.Code)
	}

	// Add health -> still missing 1m metrics -> 404
	_ = repo.SaveLatestHealth(ctx, "router-01", model.HealthEvent{DeviceID: "router-01", CurrentStatus: "HEALTHY"})
	rec = httptest.NewRecorder()
	handler.Routes().ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404 when 1m metrics missing, got %d", rec.Code)
	}

	// Add 1m metrics -> still missing analysis -> 404
	_ = repo.SaveRollingMetrics(ctx, model.RollingMetrics{DeviceID: "router-01", Window: "1m", SampleCount: 2})
	rec = httptest.NewRecorder()
	handler.Routes().ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404 when analysis missing, got %d", rec.Code)
	}

	// Add analysis -> complete -> 200 OK
	_ = repo.SaveDeviceAnalysis(ctx, model.DeviceAnalysis{
		DeviceID:     "router-01",
		CalculatedAt: now,
		ActiveAnomalies: []model.Anomaly{
			{Signal: "CPU", Severity: model.SeverityWarning},
		},
	})

	rec = httptest.NewRecorder()
	handler.Routes().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 for complete device snapshot, got %d", rec.Code)
	}

	var view DeviceView
	if err := json.Unmarshal(rec.Body.Bytes(), &view); err != nil {
		t.Fatalf("failed to decode DeviceView: %v", err)
	}
	if view.DeviceID != "router-01" {
		t.Fatalf("expected device_id router-01, got %s", view.DeviceID)
	}
	if view.LatestTelemetry.EventID != "e1" {
		t.Fatalf("expected telemetry e1, got %s", view.LatestTelemetry.EventID)
	}
	if view.LatestHealth.CurrentStatus != "HEALTHY" {
		t.Fatalf("expected health status HEALTHY, got %s", view.LatestHealth.CurrentStatus)
	}
	if view.RollingMetrics.SampleCount != 2 {
		t.Fatalf("expected sample count 2, got %d", view.RollingMetrics.SampleCount)
	}
	if len(view.Analysis.ActiveAnomalies) != 1 {
		t.Fatalf("expected 1 anomaly, got %d", len(view.Analysis.ActiveAnomalies))
	}
}

func TestDeviceTelemetryEndpoint(t *testing.T) {
	handler, repo := setupTestHandler()
	ctx := context.Background()

	// 1. Unknown device -> 404
	req := httptest.NewRequest(http.MethodGet, "/devices/router-01/telemetry", nil)
	rec := httptest.NewRecorder()
	handler.Routes().ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404 for unknown device telemetry, got %d", rec.Code)
	}

	// 2. Populate telemetry and aggregates
	_ = repo.SaveLatestTelemetry(ctx, "router-01", model.TelemetryEvent{DeviceID: "router-01", EventID: "telem-1", CPU: 40.0})
	_ = repo.SaveRollingMetrics(ctx, model.RollingMetrics{DeviceID: "router-01", Window: "1m", SampleCount: 5, AvgCPU: 42.0})
	_ = repo.SaveRollingMetrics(ctx, model.RollingMetrics{DeviceID: "router-01", Window: "5m", SampleCount: 20, AvgCPU: 45.0})

	// GET /devices/router-01/telemetry (no ?window parameter -> returns all configured aggregates)
	req = httptest.NewRequest(http.MethodGet, "/devices/router-01/telemetry", nil)
	rec = httptest.NewRecorder()
	handler.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	var allResp struct {
		Latest     model.TelemetryEvent             `json:"latest"`
		Aggregates map[string]*model.RollingMetrics `json:"aggregates"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &allResp); err != nil {
		t.Fatalf("failed to decode all aggregates response: %v", err)
	}
	if allResp.Latest.EventID != "telem-1" {
		t.Fatalf("expected latest event telem-1, got %s", allResp.Latest.EventID)
	}
	if len(allResp.Aggregates) != 2 || allResp.Aggregates["1m"] == nil || allResp.Aggregates["5m"] == nil {
		t.Fatalf("expected both 1m and 5m aggregates, got %+v", allResp.Aggregates)
	}

	// 3. GET /devices/router-01/telemetry?window=1m (returns only 1m)
	req = httptest.NewRequest(http.MethodGet, "/devices/router-01/telemetry?window=1m", nil)
	rec = httptest.NewRecorder()
	handler.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 for window=1m, got %d", rec.Code)
	}

	var w1mResp struct {
		Latest  model.TelemetryEvent  `json:"latest"`
		Rolling *model.RollingMetrics `json:"rolling"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &w1mResp); err != nil {
		t.Fatalf("failed to decode 1m response: %v", err)
	}
	if w1mResp.Rolling == nil || w1mResp.Rolling.Window != "1m" {
		t.Fatalf("expected rolling 1m metrics, got %+v", w1mResp.Rolling)
	}

	// 4. GET /devices/router-01/telemetry?window=10m (unconfigured window -> 400 Bad Request)
	req = httptest.NewRequest(http.MethodGet, "/devices/router-01/telemetry?window=10m", nil)
	rec = httptest.NewRecorder()
	handler.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 Bad Request for unconfigured window 10m, got %d", rec.Code)
	}
}

func TestDeviceHealthAndAnalysisEndpoints(t *testing.T) {
	handler, repo := setupTestHandler()
	ctx := context.Background()

	// 1. 404 on missing
	req := httptest.NewRequest(http.MethodGet, "/devices/router-01/health", nil)
	rec := httptest.NewRecorder()
	handler.Routes().ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404 for missing health, got %d", rec.Code)
	}

	req = httptest.NewRequest(http.MethodGet, "/devices/router-01/analysis", nil)
	rec = httptest.NewRecorder()
	handler.Routes().ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404 for missing analysis, got %d", rec.Code)
	}

	// 2. Populate and query
	_ = repo.SaveLatestHealth(ctx, "router-01", model.HealthEvent{DeviceID: "router-01", CurrentStatus: "DEGRADED", Score: 40})
	_ = repo.SaveDeviceAnalysis(ctx, model.DeviceAnalysis{DeviceID: "router-01", ActiveAnomalies: []model.Anomaly{{Signal: "MEMORY"}}})

	req = httptest.NewRequest(http.MethodGet, "/devices/router-01/health", nil)
	rec = httptest.NewRecorder()
	handler.Routes().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 for health, got %d", rec.Code)
	}
	var health model.HealthEvent
	_ = json.Unmarshal(rec.Body.Bytes(), &health)
	if health.CurrentStatus != "DEGRADED" {
		t.Fatalf("expected DEGRADED, got %s", health.CurrentStatus)
	}

	req = httptest.NewRequest(http.MethodGet, "/devices/router-01/analysis", nil)
	rec = httptest.NewRecorder()
	handler.Routes().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 for analysis, got %d", rec.Code)
	}
	var analysis model.DeviceAnalysis
	_ = json.Unmarshal(rec.Body.Bytes(), &analysis)
	if len(analysis.ActiveAnomalies) != 1 || analysis.ActiveAnomalies[0].Signal != "MEMORY" {
		t.Fatalf("expected MEMORY anomaly, got %+v", analysis.ActiveAnomalies)
	}

	// 3. Subtree 404 for invalid subpath
	req = httptest.NewRequest(http.MethodGet, "/devices/router-01/unknown-subpath", nil)
	rec = httptest.NewRecorder()
	handler.Routes().ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404 for unknown subpath, got %d", rec.Code)
	}

	// 4. Method not allowed on subpaths
	for _, sub := range []string{"", "telemetry", "health", "analysis"} {
		path := fmt.Sprintf("/devices/router-01/%s", sub)
		reqPost := httptest.NewRequest(http.MethodPost, path, nil)
		recPost := httptest.NewRecorder()
		handler.Routes().ServeHTTP(recPost, reqPost)
		if recPost.Code != http.StatusMethodNotAllowed {
			t.Fatalf("expected 405 on POST %s, got %d", path, recPost.Code)
		}
		if recPost.Header().Get("Allow") != http.MethodGet {
			t.Fatalf("expected Allow: GET header on %s, got %q", path, recPost.Header().Get("Allow"))
		}
	}
}
