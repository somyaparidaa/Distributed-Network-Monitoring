package main

import (
	"context"
	"encoding/json"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestNewSimulatorInitialState(t *testing.T) {
	simulator := NewSimulator("router-01", DefaultSimulationConfig())
	telemetry := simulator.currentTelemetry()

	if telemetry.DeviceID != "router-01" {
		t.Fatalf("device ID = %q, want router-01", telemetry.DeviceID)
	}
	if telemetry.Condition != ConditionNormal {
		t.Fatalf("condition = %q, want %q", telemetry.Condition, ConditionNormal)
	}
	if simulator.IsDown() {
		t.Fatal("new simulator is down")
	}
	if telemetry.CPU != normalCPU || telemetry.Memory != normalMemory || telemetry.LatencyMS != int(normalLatencyMS) || telemetry.PacketLoss != normalPacketLoss {
		t.Fatalf("unexpected initial telemetry: %+v", telemetry)
	}
	if !telemetry.InterfaceUp || !telemetry.Connectivity {
		t.Fatalf("initial device status is down: %+v", telemetry)
	}
	if telemetry.Timestamp.IsZero() {
		t.Fatal("initial timestamp is zero")
	}
}

func TestUpdateRefreshesStateWithinValidRanges(t *testing.T) {
	simulator := NewSimulator("router-01", DefaultSimulationConfig())
	before := simulator.currentTelemetry()
	time.Sleep(time.Millisecond)
	simulator.update()
	after := simulator.currentTelemetry()

	if !after.Timestamp.After(before.Timestamp) {
		t.Fatalf("timestamp = %v, want after %v", after.Timestamp, before.Timestamp)
	}
	assertValidTelemetry(t, after)
}

func TestSimulationLoopUpdatesState(t *testing.T) {
	simulator := NewSimulator("router-01", SimulationConfig{
		UpdateInterval:         10 * time.Millisecond,
		Seed:                   1,
		DegradationProbability: 0,
	})
	before := simulator.currentTelemetry()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go simulator.Run(ctx)
	time.Sleep(30 * time.Millisecond)

	if after := simulator.currentTelemetry(); !after.Timestamp.After(before.Timestamp) {
		t.Fatalf("timestamp = %v, want after %v", after.Timestamp, before.Timestamp)
	}
}

func TestMetricsHandlerReturnsCurrentState(t *testing.T) {
	simulator := NewSimulator("router-01", DefaultSimulationConfig())
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/metrics", nil)

	simulator.metricsHandler(w, r)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", w.Code, http.StatusOK)
	}
	if contentType := w.Header().Get("Content-Type"); contentType != "application/json" {
		t.Fatalf("Content-Type = %q, want application/json", contentType)
	}

	var response Telemetry
	if err := json.NewDecoder(w.Body).Decode(&response); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if response != simulator.currentTelemetry() {
		t.Fatalf("response = %+v, want current state %+v", response, simulator.currentTelemetry())
	}
}

func TestConditionBecomesDegradedAtThreshold(t *testing.T) {
	simulator := NewSimulator("router-01", SimulationConfig{
		UpdateInterval:         time.Second,
		Seed:                   1,
		DegradationProbability: 1,
	})
	simulator.degradation = degradedThreshold - 1

	simulator.update()
	telemetry := simulator.currentTelemetry()

	if telemetry.Condition != ConditionDegraded {
		t.Fatalf("condition = %q, want %q", telemetry.Condition, ConditionDegraded)
	}
	if !telemetry.InterfaceUp || !telemetry.Connectivity {
		t.Fatalf("degraded device should remain reachable: %+v", telemetry)
	}
}

func TestMetricsHandlerExposesCondition(t *testing.T) {
	simulator := NewSimulator("router-01", SimulationConfig{
		UpdateInterval:         time.Second,
		Seed:                   1,
		DegradationProbability: 1,
	})
	simulator.degradation = degradedThreshold
	simulator.update()

	w := httptest.NewRecorder()
	simulator.metricsHandler(w, httptest.NewRequest(http.MethodGet, "/metrics", nil))

	var response Telemetry
	if err := json.NewDecoder(w.Body).Decode(&response); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if response.Condition != ConditionDegraded {
		t.Fatalf("condition = %q, want %q", response.Condition, ConditionDegraded)
	}
}

func TestFleetCreatesIndependentDevices(t *testing.T) {
	fleet := newTestFleet(t)
	if fleet.Len() != 3 {
		t.Fatalf("fleet size = %d, want 3", fleet.Len())
	}

	seen := make(map[string]bool)
	for _, deviceID := range []string{"router-01", "router-02", "router-03"} {
		device, exists := fleet.Device(deviceID)
		if !exists {
			t.Fatalf("missing device %q", deviceID)
		}
		telemetry := device.currentTelemetry()
		if telemetry.DeviceID != deviceID {
			t.Errorf("device ID = %q, want %q", telemetry.DeviceID, deviceID)
		}
		if seen[telemetry.DeviceID] {
			t.Errorf("duplicate telemetry device ID %q", telemetry.DeviceID)
		}
		seen[telemetry.DeviceID] = true
	}
}

func TestUpdatingOneFleetDeviceDoesNotChangeAnother(t *testing.T) {
	fleet := newTestFleet(t)
	routerOne, _ := fleet.Device("router-01")
	routerTwo, _ := fleet.Device("router-02")
	before := routerTwo.currentTelemetry()

	routerOne.update()

	if after := routerTwo.currentTelemetry(); after != before {
		t.Fatalf("router-02 changed after router-01 update: before=%+v after=%+v", before, after)
	}
}

func TestFleetDevicesUseIndependentDeterministicSeeds(t *testing.T) {
	fleet := newTestFleet(t)
	routerOne, _ := fleet.Device("router-01")
	routerTwo, _ := fleet.Device("router-02")
	routerThree, _ := fleet.Device("router-03")

	if routerOne.config.Seed == routerTwo.config.Seed || routerOne.config.Seed == routerThree.config.Seed || routerTwo.config.Seed == routerThree.config.Seed {
		t.Fatal("fleet devices must use different seeds")
	}

	for range 10 {
		routerOne.update()
		routerTwo.update()
		routerThree.update()
	}

	first := routerOne.currentTelemetry()
	second := routerTwo.currentTelemetry()
	third := routerThree.currentTelemetry()
	if first.CPU == second.CPU && second.CPU == third.CPU && first.Memory == second.Memory && second.Memory == third.Memory {
		t.Fatal("independently seeded devices produced identical telemetry")
	}
	assertValidTelemetry(t, first)
	assertValidTelemetry(t, second)
	assertValidTelemetry(t, third)
}

func TestFleetMetricsHandlerReturnsRequestedDevice(t *testing.T) {
	fleet := newTestFleet(t)
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/metrics/router-02", nil)

	fleet.metricsHandler(w, r)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", w.Code, http.StatusOK)
	}
	var response Telemetry
	if err := json.NewDecoder(w.Body).Decode(&response); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if response.DeviceID != "router-02" {
		t.Fatalf("device ID = %q, want router-02", response.DeviceID)
	}
}

func TestSetDownStopsUpdatesAndRecoveryRestoresDevice(t *testing.T) {
	simulator := NewSimulator("router-01", DefaultSimulationConfig())
	simulator.SetDown(true)
	downTelemetry := simulator.currentTelemetry()

	if !simulator.IsDown() || downTelemetry.Condition != ConditionDown {
		t.Fatalf("device was not marked down: %+v", downTelemetry)
	}
	if downTelemetry.InterfaceUp || downTelemetry.Connectivity {
		t.Fatalf("down device remains operational: %+v", downTelemetry)
	}
	simulator.update()
	if afterUpdate := simulator.currentTelemetry(); afterUpdate != downTelemetry {
		t.Fatalf("down device telemetry changed: before=%+v after=%+v", downTelemetry, afterUpdate)
	}

	simulator.SetDown(false)
	recovered := simulator.currentTelemetry()
	if simulator.IsDown() || recovered.Condition != ConditionNormal {
		t.Fatalf("device did not recover to normal: %+v", recovered)
	}
	if !recovered.InterfaceUp || !recovered.Connectivity {
		t.Fatalf("recovered device remains unavailable: %+v", recovered)
	}
}

func TestRecoveryUsesExistingDegradationCondition(t *testing.T) {
	simulator := NewSimulator("router-01", DefaultSimulationConfig())
	simulator.degradation = degradedThreshold
	simulator.SetDown(true)
	simulator.SetDown(false)

	telemetry := simulator.currentTelemetry()
	if telemetry.Condition != ConditionDegraded {
		t.Fatalf("condition = %q, want %q", telemetry.Condition, ConditionDegraded)
	}
	if !telemetry.InterfaceUp || !telemetry.Connectivity {
		t.Fatalf("recovered degraded device is unavailable: %+v", telemetry)
	}
}

func TestDownDeviceMetricsReturnsServiceUnavailable(t *testing.T) {
	simulator := NewSimulator("router-01", DefaultSimulationConfig())
	simulator.SetDown(true)

	w := httptest.NewRecorder()
	simulator.metricsHandler(w, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d", w.Code, http.StatusServiceUnavailable)
	}

	simulator.SetDown(false)
	w = httptest.NewRecorder()
	simulator.metricsHandler(w, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", w.Code, http.StatusOK)
	}
}

func TestFleetControlEndpoints(t *testing.T) {
	fleet := newTestFleet(t)
	w := httptest.NewRecorder()
	fleet.controlHandler(w, httptest.NewRequest(http.MethodPost, "/control/router-02/down", nil))
	if w.Code != http.StatusNoContent {
		t.Fatalf("down status = %d, want %d", w.Code, http.StatusNoContent)
	}

	w = httptest.NewRecorder()
	fleet.metricsHandler(w, httptest.NewRequest(http.MethodGet, "/metrics/router-02", nil))
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("down metrics status = %d, want %d", w.Code, http.StatusServiceUnavailable)
	}

	w = httptest.NewRecorder()
	fleet.controlHandler(w, httptest.NewRequest(http.MethodPost, "/control/router-02/recover", nil))
	if w.Code != http.StatusNoContent {
		t.Fatalf("recover status = %d, want %d", w.Code, http.StatusNoContent)
	}

	w = httptest.NewRecorder()
	fleet.metricsHandler(w, httptest.NewRequest(http.MethodGet, "/metrics/router-02", nil))
	if w.Code != http.StatusOK {
		t.Fatalf("recovered metrics status = %d, want %d", w.Code, http.StatusOK)
	}
}

func TestControlEndpointsRejectUnknownDevicesAndNonPostMethods(t *testing.T) {
	fleet := newTestFleet(t)

	w := httptest.NewRecorder()
	fleet.controlHandler(w, httptest.NewRequest(http.MethodPost, "/control/router-99/down", nil))
	if w.Code != http.StatusNotFound {
		t.Fatalf("unknown device status = %d, want %d", w.Code, http.StatusNotFound)
	}

	w = httptest.NewRecorder()
	fleet.controlHandler(w, httptest.NewRequest(http.MethodGet, "/control/router-02/down", nil))
	if w.Code != http.StatusMethodNotAllowed {
		t.Fatalf("GET status = %d, want %d", w.Code, http.StatusMethodNotAllowed)
	}
}

func TestDownFleetDeviceDoesNotAffectOtherDevices(t *testing.T) {
	fleet := newTestFleet(t)
	routerOne, _ := fleet.Device("router-01")
	routerTwo, _ := fleet.Device("router-02")
	routerThree, _ := fleet.Device("router-03")
	beforeOne := routerOne.currentTelemetry()
	beforeThree := routerThree.currentTelemetry()

	routerTwo.SetDown(true)

	if routerOne.IsDown() || routerThree.IsDown() {
		t.Fatal("down state leaked to another fleet device")
	}
	if routerOne.currentTelemetry() != beforeOne || routerThree.currentTelemetry() != beforeThree {
		t.Fatal("telemetry changed for an unaffected fleet device")
	}
}

func TestConcurrentStateAccess(t *testing.T) {
	simulator := NewSimulator("router-01", DefaultSimulationConfig())
	var wg sync.WaitGroup

	for range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 100 {
				simulator.update()
			}
		}()
	}
	for range 10 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range 100 {
				simulator.SetDown(i%2 == 0)
			}
		}()
	}
	for range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 100 {
				_ = simulator.currentTelemetry()
			}
		}()
	}
	wg.Wait()

	assertValidTelemetry(t, simulator.currentTelemetry())
}

func TestMetricsChangeGraduallyDuringNormalOperation(t *testing.T) {
	simulator := NewSimulator("router-01", SimulationConfig{
		UpdateInterval:         time.Second,
		Seed:                   42,
		DegradationProbability: 0,
	})
	previous := simulator.currentTelemetry()

	for range 100 {
		simulator.update()
		current := simulator.currentTelemetry()

		if math.Abs(current.CPU-previous.CPU) > maximumCPUStep {
			t.Fatalf("CPU changed from %f to %f", previous.CPU, current.CPU)
		}
		if math.Abs(current.Memory-previous.Memory) > maximumMemoryStep {
			t.Fatalf("memory changed from %f to %f", previous.Memory, current.Memory)
		}
		if math.Abs(float64(current.LatencyMS-previous.LatencyMS)) > maximumLatencyStepMS+1 {
			t.Fatalf("latency changed from %d to %d", previous.LatencyMS, current.LatencyMS)
		}
		if math.Abs(current.PacketLoss-previous.PacketLoss) > maximumPacketLossStep {
			t.Fatalf("packet loss changed from %f to %f", previous.PacketLoss, current.PacketLoss)
		}
		assertValidTelemetry(t, current)
		previous = current
	}
}

func TestDegradationAffectsRelatedMetrics(t *testing.T) {
	simulator := NewSimulator("router-01", SimulationConfig{
		UpdateInterval:         time.Second,
		Seed:                   7,
		DegradationProbability: 1,
	})
	before := simulator.currentTelemetry()

	for range 5 {
		simulator.update()
	}
	after := simulator.currentTelemetry()

	if after.CPU <= before.CPU || after.Memory <= before.Memory || after.LatencyMS <= before.LatencyMS || after.PacketLoss <= before.PacketLoss {
		t.Fatalf("degradation did not raise related metrics: before=%+v after=%+v", before, after)
	}
	if after.Condition != ConditionDegraded {
		t.Fatalf("condition = %q, want %q", after.Condition, ConditionDegraded)
	}
}

func TestSeededSimulationIsDeterministic(t *testing.T) {
	config := SimulationConfig{UpdateInterval: time.Second, Seed: 99, DegradationProbability: 0.4}
	first := NewSimulator("router-01", config)
	second := NewSimulator("router-01", config)

	for range 20 {
		first.update()
		second.update()
		firstTelemetry := first.currentTelemetry()
		secondTelemetry := second.currentTelemetry()

		if firstTelemetry.CPU != secondTelemetry.CPU ||
			firstTelemetry.Memory != secondTelemetry.Memory ||
			firstTelemetry.LatencyMS != secondTelemetry.LatencyMS ||
			firstTelemetry.PacketLoss != secondTelemetry.PacketLoss ||
			firstTelemetry.Condition != secondTelemetry.Condition {
			t.Fatalf("seeded simulations diverged: first=%+v second=%+v", firstTelemetry, secondTelemetry)
		}
	}
}

func assertValidTelemetry(t *testing.T, telemetry Telemetry) {
	t.Helper()
	if telemetry.Timestamp.IsZero() {
		t.Error("telemetry timestamp is zero")
	}
	switch telemetry.Condition {
	case ConditionNormal, ConditionDegraded:
		if !telemetry.InterfaceUp || !telemetry.Connectivity {
			t.Errorf("operational device is unavailable: %+v", telemetry)
		}
	case ConditionDown:
		if telemetry.InterfaceUp || telemetry.Connectivity {
			t.Errorf("down device is operational: %+v", telemetry)
		}
	default:
		t.Errorf("unexpected condition %q", telemetry.Condition)
	}
	if telemetry.CPU < 0 || telemetry.CPU > maximumPercentage {
		t.Errorf("CPU = %f, outside 0-%f", telemetry.CPU, maximumPercentage)
	}
	if telemetry.Memory < 0 || telemetry.Memory > maximumPercentage {
		t.Errorf("memory = %f, outside 0-%f", telemetry.Memory, maximumPercentage)
	}
	if telemetry.PacketLoss < 0 || telemetry.PacketLoss > maximumPercentage {
		t.Errorf("packet loss = %f, outside 0-%f", telemetry.PacketLoss, maximumPercentage)
	}
	if telemetry.LatencyMS < 0 {
		t.Errorf("latency = %d, must be non-negative", telemetry.LatencyMS)
	}
}

func TestNewMuxRequiresPrimaryDevice(t *testing.T) {
	fleet, err := NewFleet([]DeviceConfig{
		{DeviceID: "router-02", Simulation: SimulationConfig{UpdateInterval: time.Second, Seed: 22}},
	})
	if err != nil {
		t.Fatalf("create fleet: %v", err)
	}

	if _, err := NewMux(fleet); err == nil {
		t.Fatal("expected error when primary device router-01 is missing from fleet, got nil")
	}
}

func TestHTTPHandlerEdgeCases(t *testing.T) {
	fleet := newTestFleet(t)
	handler, err := NewMux(fleet)
	if err != nil {
		t.Fatalf("NewMux failed: %v", err)
	}

	tests := []struct {
		name           string
		method         string
		path           string
		expectedStatus int
	}{
		{
			name:           "simulator health check returns 200",
			method:         http.MethodGet,
			path:           "/health",
			expectedStatus: http.StatusOK,
		},
		{
			name:           "prometheus metrics endpoint returns 200 on GET",
			method:         http.MethodGet,
			path:           "/metrics",
			expectedStatus: http.StatusOK,
		},
		{
			name:           "prometheus metrics endpoint rejects non-GET",
			method:         http.MethodPost,
			path:           "/metrics",
			expectedStatus: http.StatusMethodNotAllowed,
		},
		{
			name:           "metrics trailing slash empty ID",
			method:         http.MethodGet,
			path:           "/metrics/",
			expectedStatus: http.StatusNotFound,
		},
		{
			name:           "metrics nested path rejected",
			method:         http.MethodGet,
			path:           "/metrics/router-01/extra",
			expectedStatus: http.StatusNotFound,
		},
		{
			name:           "metrics unknown device",
			method:         http.MethodGet,
			path:           "/metrics/router-99",
			expectedStatus: http.StatusNotFound,
		},
		{
			name:           "control endpoint non-POST rejected",
			method:         http.MethodGet,
			path:           "/control/router-01/down",
			expectedStatus: http.StatusMethodNotAllowed,
		},
		{
			name:           "control unknown device",
			method:         http.MethodPost,
			path:           "/control/router-99/down",
			expectedStatus: http.StatusNotFound,
		},
		{
			name:           "control unknown action",
			method:         http.MethodPost,
			path:           "/control/router-01/restart",
			expectedStatus: http.StatusNotFound,
		},
		{
			name:           "control missing action",
			method:         http.MethodPost,
			path:           "/control/router-01",
			expectedStatus: http.StatusNotFound,
		},
		{
			name:           "control extra subpath rejected",
			method:         http.MethodPost,
			path:           "/control/router-01/down/extra",
			expectedStatus: http.StatusNotFound,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			w := httptest.NewRecorder()
			r := httptest.NewRequest(tc.method, tc.path, nil)
			handler.ServeHTTP(w, r)
			if w.Code != tc.expectedStatus {
				t.Fatalf("%s %s status = %d, want %d", tc.method, tc.path, w.Code, tc.expectedStatus)
			}
		})
	}
}

func TestRecoveringOneDeviceDoesNotAffectAnother(t *testing.T) {
	fleet := newTestFleet(t)
	r1, _ := fleet.Device("router-01")
	r2, _ := fleet.Device("router-02")

	r1.SetDown(true)
	r2.SetDown(true)

	if !r1.IsDown() || !r2.IsDown() {
		t.Fatal("both devices should be down")
	}

	// Recover r2 only
	r2.SetDown(false)

	if !r1.IsDown() {
		t.Fatal("recovering router-02 must not recover router-01")
	}
	if r2.IsDown() {
		t.Fatal("router-02 should be operational after recovery")
	}
}

func TestRunServerGracefulShutdown(t *testing.T) {
	fleet := newTestFleet(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	// Bind to an ephemeral port on loopback
	go func() {
		errCh <- Run(ctx, "127.0.0.1:0", fleet)
	}()

	// Allow server to start listening
	time.Sleep(50 * time.Millisecond)

	// Cancel context to initiate graceful shutdown
	cancel()

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("Run returned error on graceful shutdown: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Run timed out waiting for graceful shutdown")
	}
}

func TestPrometheusMetricsExposition(t *testing.T) {
	fleet := newTestFleet(t)
	handler, err := NewMux(fleet)
	if err != nil {
		t.Fatalf("NewMux failed: %v", err)
	}

	// Make a request to generate metrics
	req := httptest.NewRequest(http.MethodGet, "/metrics/router-01", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 from router-01 telemetry, got %d", rec.Code)
	}

	// Now scrape /metrics (Prometheus)
	metricsReq := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	metricsRec := httptest.NewRecorder()
	handler.ServeHTTP(metricsRec, metricsReq)

	if metricsRec.Code != http.StatusOK {
		t.Fatalf("expected 200 from /metrics, got %d", metricsRec.Code)
	}
	body := metricsRec.Body.String()
	if !strings.Contains(body, "simulator_devices_total") {
		t.Errorf("expected body to contain 'simulator_devices_total', got:\n%s", body)
	}
	if !strings.Contains(body, "simulator_http_requests_total") {
		t.Errorf("expected body to contain 'simulator_http_requests_total', got:\n%s", body)
	}
	if !strings.Contains(body, `endpoint="/metrics/{deviceID}"`) {
		t.Errorf("expected normalized endpoint label in metrics, got:\n%s", body)
	}
}

func TestRouterTelemetryRoutesPreserved(t *testing.T) {
	fleet := newTestFleet(t)
	handler, err := NewMux(fleet)
	if err != nil {
		t.Fatalf("NewMux failed: %v", err)
	}

	for _, id := range []string{"router-01", "router-02", "router-03"} {
		req := httptest.NewRequest(http.MethodGet, "/metrics/"+id, nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("device %s returned status %d, want 200", id, rec.Code)
		}
		if contentType := rec.Header().Get("Content-Type"); contentType != "application/json" {
			t.Fatalf("device %s returned Content-Type %q, want application/json", id, contentType)
		}
		var telem Telemetry
		if err := json.NewDecoder(rec.Body).Decode(&telem); err != nil {
			t.Fatalf("failed to decode telemetry JSON for %s: %v", id, err)
		}
		if telem.DeviceID != id {
			t.Fatalf("expected device_id %s, got %s", id, telem.DeviceID)
		}
	}
}

func TestSimulatorHealthAndReadinessEndpoints(t *testing.T) {
	fleet := newTestFleet(t)
	handler, err := NewMux(fleet)
	if err != nil {
		t.Fatalf("NewMux failed: %v", err)
	}

	// 1. GET /health (legacy)
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /health returned %d, want 200", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), `"status":"UP"`) {
		t.Fatalf("GET /health body = %s, want status UP", rec.Body.String())
	}

	// 2. GET /health/live
	reqLive := httptest.NewRequest(http.MethodGet, "/health/live", nil)
	recLive := httptest.NewRecorder()
	handler.ServeHTTP(recLive, reqLive)
	if recLive.Code != http.StatusOK {
		t.Fatalf("GET /health/live returned %d, want 200", recLive.Code)
	}
	if !strings.Contains(recLive.Body.String(), `"status":"UP"`) {
		t.Fatalf("GET /health/live body = %s, want status UP", recLive.Body.String())
	}

	// 3. GET /health/ready
	reqReady := httptest.NewRequest(http.MethodGet, "/health/ready", nil)
	recReady := httptest.NewRecorder()
	handler.ServeHTTP(recReady, reqReady)
	if recReady.Code != http.StatusOK {
		t.Fatalf("GET /health/ready returned %d, want 200", recReady.Code)
	}
	if !strings.Contains(recReady.Body.String(), `"status":"READY"`) {
		t.Fatalf("GET /health/ready body = %s, want status READY", recReady.Body.String())
	}

	// 4. Method not allowed
	reqPost := httptest.NewRequest(http.MethodPost, "/health/ready", nil)
	recPost := httptest.NewRecorder()
	handler.ServeHTTP(recPost, reqPost)
	if recPost.Code != http.StatusMethodNotAllowed {
		t.Fatalf("POST /health/ready returned %d, want 405", recPost.Code)
	}

	// 5. Individual simulated device failure does NOT cause simulator service readiness to fail
	r1, _ := fleet.Device("router-01")
	r1.SetDown(true)
	recReadyAfterDown := httptest.NewRecorder()
	handler.ServeHTTP(recReadyAfterDown, reqReady)
	if recReadyAfterDown.Code != http.StatusOK {
		t.Fatalf("GET /health/ready after router-01 failure returned %d, want 200", recReadyAfterDown.Code)
	}
}

func newTestFleet(t *testing.T) *Fleet {
	t.Helper()
	fleet, err := NewFleet([]DeviceConfig{
		{DeviceID: "router-01", Simulation: SimulationConfig{UpdateInterval: time.Second, Seed: 11, DegradationProbability: 0.1}},
		{DeviceID: "router-02", Simulation: SimulationConfig{UpdateInterval: time.Second, Seed: 22, DegradationProbability: 0.1}},
		{DeviceID: "router-03", Simulation: SimulationConfig{UpdateInterval: time.Second, Seed: 33, DegradationProbability: 0.1}},
	})
	if err != nil {
		t.Fatalf("create fleet: %v", err)
	}
	return fleet
}
