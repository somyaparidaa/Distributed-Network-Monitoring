package polling

import (
	"context"
	"encoding/json"
	"errors"
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

func TestClientErrorClassification(t *testing.T) {
	tests := []struct {
		name          string
		handler       http.HandlerFunc
		timeout       time.Duration
		expectedClass error
	}{
		{
			name: "503 classified as ErrDeviceUnavailable",
			handler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusServiceUnavailable)
			},
			timeout:       time.Second,
			expectedClass: ErrDeviceUnavailable,
		},
		{
			name: "request timeout classified as ErrDeviceUnreachable",
			handler: func(w http.ResponseWriter, r *http.Request) {
				time.Sleep(50 * time.Millisecond)
				w.WriteHeader(http.StatusOK)
			},
			timeout:       10 * time.Millisecond,
			expectedClass: ErrDeviceUnreachable,
		},
		{
			name: "404 not found is unclassified/non-transient",
			handler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusNotFound)
			},
			timeout:       time.Second,
			expectedClass: nil, // Should not match 503 or unreachable
		},
		{
			name: "malformed json is unclassified/non-transient",
			handler: func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte("{invalid-json"))
			},
			timeout:       time.Second,
			expectedClass: nil,
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

			if tc.expectedClass != nil {
				if !errors.Is(err, tc.expectedClass) {
					t.Fatalf("expected error to wrap %v, got %v", tc.expectedClass, err)
				}
				if !IsRetryable(err) {
					t.Fatalf("expected error %v to be classified as retryable", err)
				}
			} else {
				if IsRetryable(err) {
					t.Fatalf("expected error %v NOT to be classified as retryable", err)
				}
			}
		})
	}
}

func TestRetryPolicyTransientRecovery(t *testing.T) {
	var attempts atomic.Int64

	// Fails twice with 503, succeeds on 3rd attempt
	fn := func() (Telemetry, error) {
		att := attempts.Add(1)
		if att < 3 {
			return Telemetry{}, fmt.Errorf("%w: status 503", ErrDeviceUnavailable)
		}
		return Telemetry{DeviceID: "router-01", CPU: 20}, nil
	}

	cfg := RetryConfig{
		MaxRetries:     2,
		InitialBackoff: 5 * time.Millisecond,
	}

	telemetry, err := ExecuteWithRetry(context.Background(), cfg, fn)
	if err != nil {
		t.Fatalf("expected success after retries, got: %v", err)
	}

	if telemetry.DeviceID != "router-01" {
		t.Fatalf("unexpected telemetry: %+v", telemetry)
	}

	if attempts.Load() != 3 {
		t.Fatalf("expected 3 attempts, got %d", attempts.Load())
	}
}

func TestRetryPolicyNonRetryableDoesNotRetry(t *testing.T) {
	var attempts atomic.Int64

	// Fails with 404
	fn := func() (Telemetry, error) {
		attempts.Add(1)
		return Telemetry{}, fmt.Errorf("unexpected http status 404")
	}

	cfg := RetryConfig{
		MaxRetries:     3,
		InitialBackoff: 5 * time.Millisecond,
	}

	_, err := ExecuteWithRetry(context.Background(), cfg, fn)
	if err == nil {
		t.Fatal("expected error, got nil")
	}

	// Should stop on attempt 1 because 404 is not transient/retryable
	if attempts.Load() != 1 {
		t.Fatalf("expected exactly 1 attempt for non-retryable error, got %d", attempts.Load())
	}
}

func TestStateTrackerTransitionsToDownAndRecovers(t *testing.T) {
	tracker := NewStateTracker()
	devID := "router-01"

	// 1st failure (threshold = 3)
	state, transitioned := tracker.RecordFailure(devID, errors.New("err 1"), 3)
	if transitioned || state.Status != StatusUp || state.ConsecutiveFailures != 1 {
		t.Fatalf("unexpected state after 1st failure: %+v (transitioned=%v)", state, transitioned)
	}

	// 2nd failure
	state, transitioned = tracker.RecordFailure(devID, errors.New("err 2"), 3)
	if transitioned || state.Status != StatusUp || state.ConsecutiveFailures != 2 {
		t.Fatalf("unexpected state after 2nd failure: %+v (transitioned=%v)", state, transitioned)
	}

	// 3rd failure -> should transition to DOWN
	state, transitioned = tracker.RecordFailure(devID, errors.New("err 3"), 3)
	if !transitioned || state.Status != StatusDown || state.ConsecutiveFailures != 3 {
		t.Fatalf("expected transition to DOWN on 3rd failure: %+v (transitioned=%v)", state, transitioned)
	}

	// 4th failure -> stays DOWN, no transition, and counter capped at threshold (3)
	state, transitioned = tracker.RecordFailure(devID, errors.New("err 4"), 3)
	if transitioned || state.Status != StatusDown || state.ConsecutiveFailures != 3 {
		t.Fatalf("expected to remain DOWN with counter capped at 3: %+v (transitioned=%v)", state, transitioned)
	}

	// 5th failure -> stays DOWN, counter still 3
	state, transitioned = tracker.RecordFailure(devID, errors.New("err 5"), 3)
	if transitioned || state.Status != StatusDown || state.ConsecutiveFailures != 3 {
		t.Fatalf("expected counter to remain capped at 3: %+v (transitioned=%v)", state, transitioned)
	}

	// Recovery poll
	state, recovered := tracker.RecordSuccess(devID)
	if !recovered || state.Status != StatusUp || state.ConsecutiveFailures != 0 {
		t.Fatalf("expected recovery to UP: %+v (recovered=%v)", state, recovered)
	}

	// Subsequent success is NOT another recovery transition
	state, recovered = tracker.RecordSuccess(devID)
	if recovered || state.Status != StatusUp || state.ConsecutiveFailures != 0 {
		t.Fatalf("subsequent success should not report recovered: %+v", state)
	}
}

func TestConsecutiveFailuresCappedWhenDownRegression(t *testing.T) {
	tracker := NewStateTracker()
	devID := "router-02"
	threshold := 3

	// Fail 10 consecutive times
	for i := 1; i <= 10; i++ {
		state, transitioned := tracker.RecordFailure(devID, errors.New("conn refused"), threshold)
		if i < threshold {
			if state.Status != StatusUp || state.ConsecutiveFailures != i || transitioned {
				t.Fatalf("pre-threshold attempt %d: %+v", i, state)
			}
		} else if i == threshold {
			if state.Status != StatusDown || state.ConsecutiveFailures != threshold || !transitioned {
				t.Fatalf("threshold transition attempt %d: %+v", i, state)
			}
		} else {
			// i > threshold: must not keep incrementing beyond threshold
			if state.Status != StatusDown || state.ConsecutiveFailures != threshold || transitioned {
				t.Fatalf("post-threshold attempt %d: expected counter capped at %d, got %d",
					i, threshold, state.ConsecutiveFailures)
			}
		}
	}

	// Recover device
	state, recovered := tracker.RecordSuccess(devID)
	if !recovered || state.Status != StatusUp || state.ConsecutiveFailures != 0 {
		t.Fatalf("expected clean recovery: %+v", state)
	}
}

func TestEngineFailureTransitionAndFaultIsolation(t *testing.T) {
	var r1Polls, r2Polls atomic.Int64
	var r2Down atomic.Bool

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/metrics/router-01":
			r1Polls.Add(1)
			_ = json.NewEncoder(w).Encode(Telemetry{DeviceID: "router-01", CPU: 30})
		case "/metrics/router-02":
			r2Polls.Add(1)
			if r2Down.Load() {
				w.WriteHeader(http.StatusServiceUnavailable) // 503
				return
			}
			_ = json.NewEncoder(w).Encode(Telemetry{DeviceID: "router-02", CPU: 40})
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer ts.Close()

	registry, _ := device.NewRegistry([]device.MonitoredDevice{
		{ID: "router-01", MetricsURL: ts.URL + "/metrics/router-01"},
		{ID: "router-02", MetricsURL: ts.URL + "/metrics/router-02"},
	})

	store := NewStore()
	stateTracker := NewStateTracker()
	client := NewClient(100 * time.Millisecond)

	engineCfg := EngineConfig{
		PollInterval: 25 * time.Millisecond,
		Retry: RetryConfig{
			MaxRetries:     1,
			InitialBackoff: 2 * time.Millisecond,
		},
		FailureThreshold: 2,
	}

	engine := NewEngine(registry, store, stateTracker, client, engineCfg)
	ctx, cancel := context.WithCancel(context.Background())
	engine.Start(ctx)

	// Allow initial poll
	time.Sleep(35 * time.Millisecond)

	s1, _ := stateTracker.Get("router-01")
	s2, _ := stateTracker.Get("router-02")
	if s1.Status != StatusUp || s2.Status != StatusUp {
		t.Fatalf("expected both devices UP initially: r1=%v r2=%v", s1.Status, s2.Status)
	}

	// Now fail router-02
	r2Down.Store(true)

	// Wait for ~3 poll intervals so failure threshold (2) is exceeded
	time.Sleep(90 * time.Millisecond)

	s2After, _ := stateTracker.Get("router-02")
	if s2After.Status != StatusDown {
		t.Fatalf("expected router-02 to transition to DOWN, got: %+v", s2After)
	}

	// router-01 MUST remain UP and continue polling normally (fault isolation)
	s1After, _ := stateTracker.Get("router-01")
	if s1After.Status != StatusUp {
		t.Fatalf("router-01 should have remained UP: %+v", s1After)
	}
	if r1Polls.Load() < 3 {
		t.Fatalf("router-01 should have polled >= 3 times, got %d", r1Polls.Load())
	}

	// Now recover router-02
	r2Down.Store(false)
	time.Sleep(50 * time.Millisecond)

	s2Recovered, _ := stateTracker.Get("router-02")
	if s2Recovered.Status != StatusUp || s2Recovered.ConsecutiveFailures != 0 {
		t.Fatalf("expected router-02 to recover to UP: %+v", s2Recovered)
	}

	cancel()
	engine.Wait()
}
