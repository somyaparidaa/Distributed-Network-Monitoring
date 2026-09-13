package anomaly

import (
	"testing"
	"time"

	"distributed-network-monitor/services/analysis/model"
)

func TestDetectorSampleCountGate(t *testing.T) {
	d := NewDetector("1m")

	// Single sample with high latency should NOT trigger sustained anomaly (requires >= 2 samples)
	metrics := []model.RollingMetrics{
		{
			DeviceID:     "router-01",
			Window:       "1m",
			SampleCount:  1,
			AvgLatencyMS: 200.0,
		},
	}

	analysis := d.EvaluateMetrics("router-01", metrics)
	if len(analysis.ActiveAnomalies) != 0 {
		t.Fatalf("expected 0 anomalies with single noisy sample, got %d", len(analysis.ActiveAnomalies))
	}
}

func TestDetectorThresholdBoundaries(t *testing.T) {
	d := NewDetector("1m")

	// Just below warning: 49.9ms
	analysis := d.EvaluateMetrics("router-01", []model.RollingMetrics{
		{DeviceID: "router-01", Window: "1m", SampleCount: 2, AvgLatencyMS: 49.9},
	})
	if len(analysis.ActiveAnomalies) != 0 {
		t.Fatalf("expected 0 anomalies for latency 49.9ms, got %d", len(analysis.ActiveAnomalies))
	}

	// At warning: 50.0ms -> WARNING
	analysis = d.EvaluateMetrics("router-01", []model.RollingMetrics{
		{DeviceID: "router-01", Window: "1m", SampleCount: 2, AvgLatencyMS: 50.0},
	})
	if len(analysis.ActiveAnomalies) != 1 || analysis.ActiveAnomalies[0].Severity != model.SeverityWarning {
		t.Fatalf("expected 1 WARNING anomaly for latency 50.0ms, got %+v", analysis.ActiveAnomalies)
	}

	// At critical: 150.0ms -> CRITICAL
	analysis = d.EvaluateMetrics("router-01", []model.RollingMetrics{
		{DeviceID: "router-01", Window: "1m", SampleCount: 2, AvgLatencyMS: 150.0},
	})
	if len(analysis.ActiveAnomalies) != 1 || analysis.ActiveAnomalies[0].Severity != model.SeverityCritical {
		t.Fatalf("expected 1 CRITICAL anomaly for latency 150.0ms, got %+v", analysis.ActiveAnomalies)
	}
}

func TestDetectorMultipleSimultaneousSignals(t *testing.T) {
	d := NewDetector("1m")

	// Simultaneous high latency, packet loss, CPU, and memory
	metrics := []model.RollingMetrics{
		{
			DeviceID:      "router-01",
			Window:        "1m",
			SampleCount:   3,
			AvgLatencyMS:  60.0, // Warning
			AvgPacketLoss: 6.0,  // Critical
			AvgCPU:        85.0, // Warning
			AvgMemory:     95.0, // Critical
		},
	}

	analysis := d.EvaluateMetrics("router-01", metrics)
	if len(analysis.ActiveAnomalies) != 4 {
		t.Fatalf("expected 4 active anomalies, got %d", len(analysis.ActiveAnomalies))
	}

	signals := make(map[string]model.Severity)
	for _, a := range analysis.ActiveAnomalies {
		signals[a.Signal] = a.Severity
	}

	if signals["LATENCY"] != model.SeverityWarning {
		t.Errorf("expected LATENCY Warning, got %v", signals["LATENCY"])
	}
	if signals["PACKET_LOSS"] != model.SeverityCritical {
		t.Errorf("expected PACKET_LOSS Critical, got %v", signals["PACKET_LOSS"])
	}
	if signals["CPU"] != model.SeverityWarning {
		t.Errorf("expected CPU Warning, got %v", signals["CPU"])
	}
	if signals["MEMORY"] != model.SeverityCritical {
		t.Errorf("expected MEMORY Critical, got %v", signals["MEMORY"])
	}
}

func TestDetectorRecoveryClearsAnomalies(t *testing.T) {
	d := NewDetector("1m")

	// Degraded metrics
	d.EvaluateMetrics("router-01", []model.RollingMetrics{
		{DeviceID: "router-01", Window: "1m", SampleCount: 2, AvgLatencyMS: 80.0},
	})

	// Metrics recover back to normal
	recovered := d.EvaluateMetrics("router-01", []model.RollingMetrics{
		{DeviceID: "router-01", Window: "1m", SampleCount: 2, AvgLatencyMS: 20.0},
	})

	if len(recovered.ActiveAnomalies) != 0 {
		t.Fatalf("expected 0 active anomalies after recovery, got %d", len(recovered.ActiveAnomalies))
	}
}

func TestDetectorHealthEventEvaluation(t *testing.T) {
	d := NewDetector("1m")
	now := time.Now().UTC()

	// 1. Health DOWN
	downAnalysis := d.EvaluateHealth(model.HealthEvent{
		DeviceID:      "router-02",
		Timestamp:     now,
		CurrentStatus: "DOWN",
		Score:         100,
		Reasons:       []string{"transport unreachable", "interface down"},
	})
	if len(downAnalysis.ActiveAnomalies) != 1 || downAnalysis.ActiveAnomalies[0].Signal != "HEALTH" || downAnalysis.ActiveAnomalies[0].Severity != model.SeverityCritical {
		t.Fatalf("expected 1 Critical HEALTH anomaly for DOWN, got %+v", downAnalysis.ActiveAnomalies)
	}

	// 2. Health recovers to HEALTHY
	healthyAnalysis := d.EvaluateHealth(model.HealthEvent{
		DeviceID:      "router-02",
		Timestamp:     now.Add(time.Second),
		CurrentStatus: "HEALTHY",
		Score:         0,
	})
	if len(healthyAnalysis.ActiveAnomalies) != 0 {
		t.Fatalf("expected 0 anomalies after recovery to HEALTHY, got %d", len(healthyAnalysis.ActiveAnomalies))
	}
}
