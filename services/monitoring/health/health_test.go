package health

import (
	"testing"
	"time"

	"distributed-network-monitor/services/monitoring/polling"
)

func TestEvaluatePrecedenceDown(t *testing.T) {
	// Baseline normal telemetry
	healthyTelemetry := polling.Telemetry{
		DeviceID:     "router-01",
		CPU:          20.0,
		Memory:       30.0,
		LatencyMS:    15,
		PacketLoss:   0.0,
		InterfaceUp:  true,
		Connectivity: true,
		Timestamp:    time.Now().UTC(),
	}

	// 1. Transport is down -> DOWN
	t.Run("transport down produces DOWN", func(t *testing.T) {
		a := Evaluate("router-01", healthyTelemetry, true)
		if a.Status != StatusDown || a.Score != 100 {
			t.Fatalf("expected DOWN with score 100, got %+v", a)
		}
		if len(a.Reasons) == 0 || a.Reasons[0] != "transport unavailable" {
			t.Fatalf("expected transport reason, got: %v", a.Reasons)
		}
	})

	// 2. Interface is down -> DOWN even if transport is up and metrics are healthy
	t.Run("interface down produces DOWN", func(t *testing.T) {
		telem := healthyTelemetry
		telem.InterfaceUp = false
		a := Evaluate("router-01", telem, false)
		if a.Status != StatusDown || a.Score != 100 {
			t.Fatalf("expected DOWN with score 100, got %+v", a)
		}
	})

	// 3. Connectivity is lost -> DOWN even if transport is up and metrics are healthy
	t.Run("connectivity false produces DOWN", func(t *testing.T) {
		telem := healthyTelemetry
		telem.Connectivity = false
		a := Evaluate("router-01", telem, false)
		if a.Status != StatusDown || a.Score != 100 {
			t.Fatalf("expected DOWN with score 100, got %+v", a)
		}
	})

	// 4. DOWN takes precedence over severe metrics (e.g. 99% CPU)
	t.Run("down takes precedence over critical metrics", func(t *testing.T) {
		telem := healthyTelemetry
		telem.CPU = 99.0
		telem.LatencyMS = 500
		telem.InterfaceUp = false
		a := Evaluate("router-01", telem, false)
		if a.Status != StatusDown {
			t.Fatalf("expected DOWN precedence, got: %+v", a)
		}
	})
}

func TestEvaluateThresholdBoundaries(t *testing.T) {
	base := polling.Telemetry{
		DeviceID:     "router-01",
		CPU:          30.0,
		Memory:       40.0,
		LatencyMS:    20,
		PacketLoss:   0.1,
		InterfaceUp:  true,
		Connectivity: true,
	}

	tests := []struct {
		name           string
		modifier       func(t *polling.Telemetry)
		expectedStatus HealthStatus
		expectMinScore int
	}{
		// All normal
		{
			name:           "all metrics normal -> HEALTHY",
			modifier:       func(t *polling.Telemetry) {},
			expectedStatus: StatusHealthy,
			expectMinScore: 0,
		},

		// CPU boundaries (Warning: 70.0, Critical: 85.0)
		{
			name:           "CPU just below warning (69.9) -> HEALTHY",
			modifier:       func(t *polling.Telemetry) { t.CPU = 69.9 },
			expectedStatus: StatusHealthy,
			expectMinScore: 0,
		},
		{
			name:           "CPU equal warning (70.0) -> WARNING",
			modifier:       func(t *polling.Telemetry) { t.CPU = 70.0 },
			expectedStatus: StatusWarning,
			expectMinScore: 10,
		},
		{
			name:           "CPU just above warning (70.1) -> WARNING",
			modifier:       func(t *polling.Telemetry) { t.CPU = 70.1 },
			expectedStatus: StatusWarning,
			expectMinScore: 10,
		},
		{
			name:           "CPU just below critical (84.9) -> WARNING",
			modifier:       func(t *polling.Telemetry) { t.CPU = 84.9 },
			expectedStatus: StatusWarning,
			expectMinScore: 10,
		},
		{
			name:           "CPU equal critical (85.0) -> CRITICAL",
			modifier:       func(t *polling.Telemetry) { t.CPU = 85.0 },
			expectedStatus: StatusCritical,
			expectMinScore: 50,
		},
		{
			name:           "CPU just above critical (85.1) -> CRITICAL",
			modifier:       func(t *polling.Telemetry) { t.CPU = 85.1 },
			expectedStatus: StatusCritical,
			expectMinScore: 50,
		},

		// Memory boundaries (Warning: 75.0, Critical: 90.0)
		{
			name:           "Memory just below warning (74.9) -> HEALTHY",
			modifier:       func(t *polling.Telemetry) { t.Memory = 74.9 },
			expectedStatus: StatusHealthy,
			expectMinScore: 0,
		},
		{
			name:           "Memory equal warning (75.0) -> WARNING",
			modifier:       func(t *polling.Telemetry) { t.Memory = 75.0 },
			expectedStatus: StatusWarning,
			expectMinScore: 10,
		},
		{
			name:           "Memory equal critical (90.0) -> CRITICAL",
			modifier:       func(t *polling.Telemetry) { t.Memory = 90.0 },
			expectedStatus: StatusCritical,
			expectMinScore: 50,
		},

		// Latency boundaries (Warning: 50, Critical: 150)
		{
			name:           "Latency just below warning (49) -> HEALTHY",
			modifier:       func(t *polling.Telemetry) { t.LatencyMS = 49 },
			expectedStatus: StatusHealthy,
			expectMinScore: 0,
		},
		{
			name:           "Latency equal warning (50) -> WARNING",
			modifier:       func(t *polling.Telemetry) { t.LatencyMS = 50 },
			expectedStatus: StatusWarning,
			expectMinScore: 10,
		},
		{
			name:           "Latency equal critical (150) -> CRITICAL",
			modifier:       func(t *polling.Telemetry) { t.LatencyMS = 150 },
			expectedStatus: StatusCritical,
			expectMinScore: 50,
		},

		// Packet Loss boundaries (Warning: 1.0, Critical: 5.0)
		{
			name:           "PacketLoss just below warning (0.99) -> HEALTHY",
			modifier:       func(t *polling.Telemetry) { t.PacketLoss = 0.99 },
			expectedStatus: StatusHealthy,
			expectMinScore: 0,
		},
		{
			name:           "PacketLoss equal warning (1.0) -> WARNING",
			modifier:       func(t *polling.Telemetry) { t.PacketLoss = 1.0 },
			expectedStatus: StatusWarning,
			expectMinScore: 10,
		},
		{
			name:           "PacketLoss equal critical (5.0) -> CRITICAL",
			modifier:       func(t *polling.Telemetry) { t.PacketLoss = 5.0 },
			expectedStatus: StatusCritical,
			expectMinScore: 50,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			telem := base
			tc.modifier(&telem)
			a := Evaluate(telem.DeviceID, telem, false)
			if a.Status != tc.expectedStatus {
				t.Fatalf("status = %q, want %q (assessment: %+v)", a.Status, tc.expectedStatus, a)
			}
			if a.Score < tc.expectMinScore {
				t.Fatalf("score = %d, want >= %d", a.Score, tc.expectMinScore)
			}
			if tc.expectedStatus == StatusHealthy && len(a.Reasons) != 0 {
				t.Fatalf("expected no reasons for HEALTHY, got %v", a.Reasons)
			}
			if tc.expectedStatus != StatusHealthy && len(a.Reasons) == 0 {
				t.Fatalf("expected reasons for %s status", tc.expectedStatus)
			}
		})
	}
}

func TestEvaluateMultiSignalCombinations(t *testing.T) {
	// Combination of multiple warnings
	t.Run("multiple warnings -> WARNING with combined reasons and higher score", func(t *testing.T) {
		telem := polling.Telemetry{
			DeviceID:     "router-01",
			CPU:          72.0, // Warning
			Memory:       78.0, // Warning
			LatencyMS:    60,   // Warning
			PacketLoss:   0.2,  // Normal
			InterfaceUp:  true,
			Connectivity: true,
		}
		a := Evaluate(telem.DeviceID, telem, false)
		if a.Status != StatusWarning {
			t.Fatalf("expected WARNING, got %q", a.Status)
		}
		if len(a.Reasons) != 3 {
			t.Fatalf("expected 3 reasons, got %d: %v", len(a.Reasons), a.Reasons)
		}
		// 10 base + 3 * 5 = 25
		if a.Score != 25 {
			t.Fatalf("expected score 25, got %d", a.Score)
		}
	})

	// Combination of critical + warning
	t.Run("critical CPU + warning Latency -> CRITICAL", func(t *testing.T) {
		telem := polling.Telemetry{
			DeviceID:     "router-01",
			CPU:          92.0, // Critical
			Memory:       50.0, // Normal
			LatencyMS:    80,   // Warning
			PacketLoss:   0.1,  // Normal
			InterfaceUp:  true,
			Connectivity: true,
		}
		a := Evaluate(telem.DeviceID, telem, false)
		if a.Status != StatusCritical {
			t.Fatalf("expected CRITICAL, got %q", a.Status)
		}
		if len(a.Reasons) != 2 {
			t.Fatalf("expected 2 reasons, got %d: %v", len(a.Reasons), a.Reasons)
		}
		// 50 base + 1*10 + 1*5 = 65
		if a.Score != 65 {
			t.Fatalf("expected score 65, got %d", a.Score)
		}
	})
}

func TestHealthStoreThreadSafety(t *testing.T) {
	store := NewStore()
	done := make(chan struct{})

	go func() {
		for i := 0; i < 100; i++ {
			store.Set("router-01", Assessment{
				DeviceID: "router-01",
				Status:   StatusHealthy,
				Score:    i,
			})
		}
		close(done)
	}()

	for {
		select {
		case <-done:
			return
		default:
			_, _ = store.Get("router-01")
			_ = store.All()
		}
	}
}

func TestServiceEvaluatorIntegration(t *testing.T) {
	telemetryStore := polling.NewStore()
	healthStore := NewStore()
	evaluator := NewServiceEvaluator(telemetryStore, healthStore)

	// 1. Initial success poll -> assessment is HEALTHY
	now := time.Now().UTC()
	telem := polling.Telemetry{
		DeviceID:     "router-01",
		CPU:          35.0,
		Memory:       50.0,
		LatencyMS:    20,
		PacketLoss:   0.1,
		InterfaceUp:  true,
		Connectivity: true,
		Timestamp:    now,
	}
	telemetryStore.Set("router-01", telem)
	evaluator.RecordPollSuccess("router-01", telem)

	a1, ok := healthStore.Get("router-01")
	if !ok || a1.Status != StatusHealthy || a1.Score != 0 {
		t.Fatalf("expected HEALTHY assessment, got: %+v (ok=%v)", a1, ok)
	}

	// 2. Poll failure without transport-down (e.g. 1st failure under threshold) -> does not mark DOWN
	evaluator.RecordPollFailure("router-01", false)
	a2, _ := healthStore.Get("router-01")
	if a2.Status != StatusHealthy {
		t.Fatalf("expected status to remain HEALTHY before transport threshold: %+v", a2)
	}

	// 3. Poll failure with transport-down (consecutive >= threshold) -> marks DOWN
	evaluator.RecordPollFailure("router-01", true)
	a3, _ := healthStore.Get("router-01")
	if a3.Status != StatusDown || a3.Score != 100 {
		t.Fatalf("expected status to become DOWN: %+v", a3)
	}

	// Last successful telemetry must remain preserved in telemetryStore
	preservedTelem, ok := telemetryStore.Get("router-01")
	if !ok || preservedTelem.CPU != 35.0 || preservedTelem.Timestamp != now {
		t.Fatalf("last telemetry was mutated or lost: %+v", preservedTelem)
	}

	// 4. Recovery poll -> marks HEALTHY again
	evaluator.RecordPollSuccess("router-01", telem)
	a4, _ := healthStore.Get("router-01")
	if a4.Status != StatusHealthy || a4.Score != 0 {
		t.Fatalf("expected recovery to HEALTHY: %+v", a4)
	}
}
