package main

import (
	"context"
	"encoding/json"
	"math"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

func TestNewSimulatorInitialState(t *testing.T) {
	simulator := NewSimulator()
	telemetry := simulator.currentTelemetry()

	if telemetry.DeviceID != deviceID {
		t.Fatalf("device ID = %q, want %q", telemetry.DeviceID, deviceID)
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
	simulator := NewSimulator()
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
	simulator := NewSimulatorWithConfig(SimulationConfig{
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
	simulator := NewSimulator()
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

func TestConcurrentStateAccess(t *testing.T) {
	simulator := NewSimulator()
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
	simulator := NewSimulatorWithConfig(SimulationConfig{
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
	simulator := NewSimulatorWithConfig(SimulationConfig{
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
}

func TestSeededSimulationIsDeterministic(t *testing.T) {
	config := SimulationConfig{UpdateInterval: time.Second, Seed: 99, DegradationProbability: 0.4}
	first := NewSimulatorWithConfig(config)
	second := NewSimulatorWithConfig(config)

	for range 20 {
		first.update()
		second.update()
		firstTelemetry := first.currentTelemetry()
		secondTelemetry := second.currentTelemetry()

		if firstTelemetry.CPU != secondTelemetry.CPU ||
			firstTelemetry.Memory != secondTelemetry.Memory ||
			firstTelemetry.LatencyMS != secondTelemetry.LatencyMS ||
			firstTelemetry.PacketLoss != secondTelemetry.PacketLoss {
			t.Fatalf("seeded simulations diverged: first=%+v second=%+v", firstTelemetry, secondTelemetry)
		}
	}
}

func assertValidTelemetry(t *testing.T, telemetry Telemetry) {
	t.Helper()
	if telemetry.DeviceID != deviceID {
		t.Errorf("device ID = %q, want %q", telemetry.DeviceID, deviceID)
	}
	if telemetry.Timestamp.IsZero() {
		t.Error("telemetry timestamp is zero")
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
