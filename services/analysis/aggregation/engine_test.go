package aggregation

import (
	"math"
	"sync"
	"testing"
	"time"

	"distributed-network-monitor/services/analysis/model"
)

func TestEngineEmptyWindowReturnsNoData(t *testing.T) {
	engine := NewEngine(map[string]time.Duration{"1m": 1 * time.Minute})

	_, exists := engine.GetMetrics("router-01", "1m")
	if exists {
		t.Fatal("expected exists=false for empty window")
	}

	_, exists = engine.GetMetrics("unknown-device", "1m")
	if exists {
		t.Fatal("expected exists=false for unknown device")
	}

	_, exists = engine.GetMetrics("router-01", "unknown-window")
	if exists {
		t.Fatal("expected exists=false for unknown window")
	}
}

func TestEngineSingleSampleCalculation(t *testing.T) {
	engine := NewEngine(map[string]time.Duration{"1m": 1 * time.Minute})
	now := time.Now().UTC()

	event := model.TelemetryEvent{
		DeviceID:   "router-01",
		Timestamp:  now,
		CPU:        45.0,
		Memory:     60.0,
		LatencyMS:  30,
		PacketLoss: 0.5,
	}

	results := engine.AddSample(event)
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}

	m := results[0]
	if m.SampleCount != 1 {
		t.Fatalf("expected sample count 1, got %d", m.SampleCount)
	}
	if m.AvgCPU != 45.0 || m.MaxCPU != 45.0 {
		t.Fatalf("CPU stats mismatch: avg=%.1f max=%.1f", m.AvgCPU, m.MaxCPU)
	}
	if m.AvgMemory != 60.0 || m.MaxMemory != 60.0 {
		t.Fatalf("Memory stats mismatch: avg=%.1f max=%.1f", m.AvgMemory, m.MaxMemory)
	}
	if m.AvgLatencyMS != 30.0 || m.MinLatencyMS != 30 || m.MaxLatencyMS != 30 {
		t.Fatalf("Latency stats mismatch: avg=%.1f min=%d max=%d", m.AvgLatencyMS, m.MinLatencyMS, m.MaxLatencyMS)
	}
	if m.AvgPacketLoss != 0.5 || m.MaxPacketLoss != 0.5 {
		t.Fatalf("Packet loss mismatch: avg=%.2f max=%.2f", m.AvgPacketLoss, m.MaxPacketLoss)
	}
	if !m.FirstSampleTime.Equal(now) || !m.LatestSampleTime.Equal(now) {
		t.Fatalf("timestamps mismatch: first=%v latest=%v", m.FirstSampleTime, m.LatestSampleTime)
	}
}

func TestEngineDeterministicMultipleSamples(t *testing.T) {
	engine := NewEngine(map[string]time.Duration{"1m": 1 * time.Minute})
	baseTime := time.Date(2026, 9, 12, 10, 0, 0, 0, time.UTC)

	// Ingest 3 samples spaced 10 seconds apart
	samples := []struct {
		cpu     float64
		mem     float64
		latency int
		loss    float64
	}{
		{cpu: 20.0, mem: 40.0, latency: 10, loss: 0.0},
		{cpu: 40.0, mem: 50.0, latency: 20, loss: 1.0},
		{cpu: 60.0, mem: 60.0, latency: 30, loss: 2.0},
	}

	var latest model.RollingMetrics
	for i, s := range samples {
		event := model.TelemetryEvent{
			DeviceID:   "router-01",
			Timestamp:  baseTime.Add(time.Duration(i*10) * time.Second),
			CPU:        s.cpu,
			Memory:     s.mem,
			LatencyMS:  s.latency,
			PacketLoss: s.loss,
		}
		res := engine.AddSample(event)
		latest = res[0]
	}

	if latest.SampleCount != 3 {
		t.Fatalf("expected 3 samples, got %d", latest.SampleCount)
	}
	// Avg CPU: (20+40+60)/3 = 40.0, Max: 60.0
	if math.Abs(latest.AvgCPU-40.0) > 1e-6 || latest.MaxCPU != 60.0 {
		t.Fatalf("unexpected CPU stats: %+v", latest)
	}
	// Avg Mem: (40+50+60)/3 = 50.0, Max: 60.0
	if math.Abs(latest.AvgMemory-50.0) > 1e-6 || latest.MaxMemory != 60.0 {
		t.Fatalf("unexpected Mem stats: %+v", latest)
	}
	// Avg Latency: (10+20+30)/3 = 20.0, Min: 10, Max: 30
	if math.Abs(latest.AvgLatencyMS-20.0) > 1e-6 || latest.MinLatencyMS != 10 || latest.MaxLatencyMS != 30 {
		t.Fatalf("unexpected Latency stats: %+v", latest)
	}
	// Avg Loss: (0+1+2)/3 = 1.0, Max: 2.0
	if math.Abs(latest.AvgPacketLoss-1.0) > 1e-6 || latest.MaxPacketLoss != 2.0 {
		t.Fatalf("unexpected Loss stats: %+v", latest)
	}
}

func TestEngineExactBoundaryEviction(t *testing.T) {
	windowDur := 60 * time.Second
	engine := NewEngine(map[string]time.Duration{"1m": windowDur})
	t0 := time.Date(2026, 9, 12, 10, 0, 0, 0, time.UTC)

	// Sample 1 at t = 0s
	engine.AddSample(model.TelemetryEvent{
		DeviceID:  "router-01",
		Timestamp: t0,
		LatencyMS: 10,
	})

	// Sample 2 at t = 30s
	engine.AddSample(model.TelemetryEvent{
		DeviceID:  "router-01",
		Timestamp: t0.Add(30 * time.Second),
		LatencyMS: 20,
	})

	// Sample 3 at t = 60s
	// Cutoff for t=60s with 60s window is t0 (60s - 60s = 0s).
	// Requirement: Keep samples exactly on the boundary (t0 >= cutoff).
	res := engine.AddSample(model.TelemetryEvent{
		DeviceID:  "router-01",
		Timestamp: t0.Add(60 * time.Second),
		LatencyMS: 30,
	})
	if res[0].SampleCount != 3 {
		t.Fatalf("sample at exact boundary should be kept: expected 3 samples, got %d", res[0].SampleCount)
	}

	// Sample 4 at t = 61s
	// Cutoff is now t0 + 1s. Sample 1 (at t0) is strictly older than cutoff (< cutoff), so it must be evicted.
	res = engine.AddSample(model.TelemetryEvent{
		DeviceID:  "router-01",
		Timestamp: t0.Add(61 * time.Second),
		LatencyMS: 40,
	})
	if res[0].SampleCount != 3 {
		t.Fatalf("sample 1 should be evicted: expected 3 samples (samples 2, 3, 4), got %d", res[0].SampleCount)
	}
	if res[0].MinLatencyMS != 20 {
		t.Fatalf("expected min latency to be 20 (sample 2), got %d", res[0].MinLatencyMS)
	}
}

func TestEngineDeviceIsolation(t *testing.T) {
	engine := NewEngine(map[string]time.Duration{"1m": 1 * time.Minute})
	now := time.Now().UTC()

	engine.AddSample(model.TelemetryEvent{
		DeviceID:  "router-01",
		Timestamp: now,
		CPU:       90.0,
	})

	engine.AddSample(model.TelemetryEvent{
		DeviceID:  "router-02",
		Timestamp: now,
		CPU:       10.0,
	})

	m1, _ := engine.GetMetrics("router-01", "1m")
	m2, _ := engine.GetMetrics("router-02", "1m")

	if m1.AvgCPU != 90.0 || m2.AvgCPU != 10.0 {
		t.Fatalf("devices affected each other: router-01 CPU=%.1f, router-02 CPU=%.1f", m1.AvgCPU, m2.AvgCPU)
	}
}

func TestEngineConcurrentAccess(t *testing.T) {
	engine := NewEngine(map[string]time.Duration{"1m": 1 * time.Minute, "5m": 5 * time.Minute})
	now := time.Now().UTC()

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			devID := "router-01"
			if idx%2 == 1 {
				devID = "router-02"
			}
			engine.AddSample(model.TelemetryEvent{
				DeviceID:  devID,
				Timestamp: now.Add(time.Duration(idx) * time.Millisecond),
				CPU:       float64(idx * 2),
				LatencyMS: idx,
			})
			_, _ = engine.GetMetrics(devID, "1m")
			_, _ = engine.GetMetrics(devID, "5m")
		}(i)
	}

	wg.Wait()
}
