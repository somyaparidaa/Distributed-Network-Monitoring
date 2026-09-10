package polling

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"distributed-network-monitor/services/monitoring/device"
)

func TestStoreThreadSafety(t *testing.T) {
	store := NewStore()
	var wg sync.WaitGroup

	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			devID := fmt.Sprintf("router-%02d", idx%3+1)
			for j := 0; j < 50; j++ {
				store.Set(devID, Telemetry{
					DeviceID: devID,
					CPU:      float64(idx + j),
				})
			}
		}(i)
	}

	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			devID := fmt.Sprintf("router-%02d", idx%3+1)
			for j := 0; j < 50; j++ {
				_, _ = store.Get(devID)
				_ = store.All()
			}
		}(i)
	}

	wg.Wait()
}

func TestClientSuccess(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Millisecond)
	sample := Telemetry{
		DeviceID:     "router-01",
		Condition:    "NORMAL",
		CPU:          42.5,
		Memory:       60.1,
		LatencyMS:    25,
		PacketLoss:   0.05,
		InterfaceUp:  true,
		Connectivity: true,
		Timestamp:    now,
	}

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(sample)
	}))
	defer ts.Close()

	client := NewClient(time.Second)
	telemetry, err := client.Poll(context.Background(), ts.URL)
	if err != nil {
		t.Fatalf("expected successful poll, got error: %v", err)
	}

	if telemetry.DeviceID != sample.DeviceID || telemetry.CPU != sample.CPU || telemetry.Condition != sample.Condition {
		t.Fatalf("decoded telemetry mismatch: got %+v, want %+v", telemetry, sample)
	}
}

func TestClientErrors(t *testing.T) {
	tests := []struct {
		name       string
		handler    http.HandlerFunc
		timeout    time.Duration
		expectFail bool
	}{
		{
			name: "503 service unavailable",
			handler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusServiceUnavailable)
			},
			timeout: time.Second,
		},
		{
			name: "404 not found",
			handler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusNotFound)
			},
			timeout: time.Second,
		},
		{
			name: "malformed json response",
			handler: func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte("{invalid-json"))
			},
			timeout: time.Second,
		},
		{
			name: "request timeout exceeded",
			handler: func(w http.ResponseWriter, r *http.Request) {
				time.Sleep(100 * time.Millisecond)
				w.WriteHeader(http.StatusOK)
			},
			timeout: 20 * time.Millisecond,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ts := httptest.NewServer(tc.handler)
			defer ts.Close()

			client := NewClient(tc.timeout)
			_, err := client.Poll(context.Background(), ts.URL)
			if err == nil {
				t.Fatalf("expected error for case %q, got nil", tc.name)
			}
		})
	}
}

func TestEngineConcurrentPollingIsolation(t *testing.T) {
	var r1Polls, r2Polls, r3Polls atomic.Int64

	// Server simulates:
	// - router-01: normal fast response
	// - router-02: slow response (simulating latency / delay)
	// - router-03: failure response (503)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/metrics/router-01":
			r1Polls.Add(1)
			_ = json.NewEncoder(w).Encode(Telemetry{DeviceID: "router-01", CPU: 10})
		case "/metrics/router-02":
			r2Polls.Add(1)
			time.Sleep(50 * time.Millisecond) // Slow device
			_ = json.NewEncoder(w).Encode(Telemetry{DeviceID: "router-02", CPU: 20})
		case "/metrics/router-03":
			r3Polls.Add(1)
			w.WriteHeader(http.StatusServiceUnavailable) // Down device
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer ts.Close()

	registry, err := device.NewRegistry([]device.MonitoredDevice{
		{ID: "router-01", MetricsURL: ts.URL + "/metrics/router-01"},
		{ID: "router-02", MetricsURL: ts.URL + "/metrics/router-02"},
		{ID: "router-03", MetricsURL: ts.URL + "/metrics/router-03"},
	})
	if err != nil {
		t.Fatalf("create registry: %v", err)
	}

	store := NewStore()
	client := NewClient(200 * time.Millisecond)
	// Short interval to exercise multiple poll loops quickly
	interval := 30 * time.Millisecond
	engine := NewEngine(registry, store, client, interval)

	ctx, cancel := context.WithCancel(context.Background())
	engine.Start(ctx)

	// Let polling run for ~120ms
	time.Sleep(120 * time.Millisecond)
	cancel()
	engine.Wait()

	// router-01 should have been polled multiple times quickly
	if r1Polls.Load() < 2 {
		t.Fatalf("router-01 should have been polled at least 2 times, got %d", r1Polls.Load())
	}

	// router-02 was slow, but router-01 was NOT blocked by router-02
	if r2Polls.Load() == 0 {
		t.Fatalf("router-02 should have been polled, got 0")
	}

	// router-03 failed, but neither router-01 nor router-02 crashed or stopped
	if r3Polls.Load() < 2 {
		t.Fatalf("router-03 should have been polled at least 2 times, got %d", r3Polls.Load())
	}

	// Stored telemetry check
	t1, ok1 := store.Get("router-01")
	if !ok1 || t1.DeviceID != "router-01" {
		t.Fatalf("expected router-01 telemetry in store, got %+v (ok=%v)", t1, ok1)
	}

	t2, ok2 := store.Get("router-02")
	if !ok2 || t2.DeviceID != "router-02" {
		t.Fatalf("expected router-02 telemetry in store, got %+v (ok=%v)", t2, ok2)
	}

	// router-03 failed so it should not be stored
	_, ok3 := store.Get("router-03")
	if ok3 {
		t.Fatal("expected router-03 not to be stored due to 503 status")
	}
}

func TestStoreRetainsLatestSuccessfulTelemetryOnSubsequentFailure(t *testing.T) {
	var returnFailure atomic.Bool

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if returnFailure.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		_ = json.NewEncoder(w).Encode(Telemetry{
			DeviceID: "router-01",
			CPU:      55.5,
		})
	}))
	defer ts.Close()

	registry, _ := device.NewRegistry([]device.MonitoredDevice{
		{ID: "router-01", MetricsURL: ts.URL},
	})
	store := NewStore()
	client := NewClient(time.Second)
	engine := NewEngine(registry, store, client, 20*time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	engine.Start(ctx)

	// Wait for initial successful poll
	time.Sleep(30 * time.Millisecond)

	t1, ok := store.Get("router-01")
	if !ok || t1.CPU != 55.5 {
		t.Fatalf("expected initial telemetry to be stored, got: %+v (ok=%v)", t1, ok)
	}

	// Now simulate failure
	returnFailure.Store(true)
	time.Sleep(50 * time.Millisecond)

	// Verify the previous successful telemetry is still retained
	tAfterFail, ok := store.Get("router-01")
	if !ok || tAfterFail.CPU != 55.5 {
		t.Fatalf("expected previous telemetry to be retained across failure, got: %+v", tAfterFail)
	}

	cancel()
	engine.Wait()
}
