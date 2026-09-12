package aggregation

import (
	"math"
	"time"

	"distributed-network-monitor/services/analysis/model"
)

type telemetrySample struct {
	timestamp  time.Time
	cpu        float64
	memory     float64
	latencyMS  int
	packetLoss float64
}

// SlidingWindow maintains an in-memory, bounded buffer of telemetry samples for a specific window duration.
type SlidingWindow struct {
	duration time.Duration
	samples  []telemetrySample
}

// NewSlidingWindow constructs an empty sliding window buffer for the given duration.
func NewSlidingWindow(duration time.Duration) *SlidingWindow {
	return &SlidingWindow{
		duration: duration,
		samples:  make([]telemetrySample, 0),
	}
}

// AddSample appends the sample and evicts any samples older than (event.Timestamp - duration).
// Boundary condition: sample.Timestamp == cutoff is preserved.
func (w *SlidingWindow) AddSample(event model.TelemetryEvent) model.RollingMetrics {
	sample := telemetrySample{
		timestamp:  event.Timestamp,
		cpu:        event.CPU,
		memory:     event.Memory,
		latencyMS:  event.LatencyMS,
		packetLoss: event.PacketLoss,
	}
	w.samples = append(w.samples, sample)

	cutoff := event.Timestamp.Add(-w.duration)
	// Evict samples strictly older than cutoff: keep sample.Timestamp >= cutoff
	startIdx := 0
	for startIdx < len(w.samples) && w.samples[startIdx].timestamp.Before(cutoff) {
		startIdx++
	}
	if startIdx > 0 {
		w.samples = w.samples[startIdx:]
	}

	return w.calculateMetrics(event.DeviceID, event.Timestamp)
}

func (w *SlidingWindow) calculateMetrics(deviceID string, now time.Time) model.RollingMetrics {
	count := len(w.samples)
	if count == 0 {
		return model.RollingMetrics{
			DeviceID:     deviceID,
			CalculatedAt: now,
		}
	}

	var totalCPU, totalMem, totalLoss float64
	var totalLatency int
	minLatency := math.MaxInt
	maxLatency := math.MinInt
	maxLoss := 0.0
	maxCPU := 0.0
	maxMem := 0.0

	for _, s := range w.samples {
		totalCPU += s.cpu
		if s.cpu > maxCPU {
			maxCPU = s.cpu
		}

		totalMem += s.memory
		if s.memory > maxMem {
			maxMem = s.memory
		}

		totalLatency += s.latencyMS
		if s.latencyMS < minLatency {
			minLatency = s.latencyMS
		}
		if s.latencyMS > maxLatency {
			maxLatency = s.latencyMS
		}

		totalLoss += s.packetLoss
		if s.packetLoss > maxLoss {
			maxLoss = s.packetLoss
		}
	}

	return model.RollingMetrics{
		DeviceID:         deviceID,
		SampleCount:      count,
		AvgLatencyMS:     float64(totalLatency) / float64(count),
		MinLatencyMS:     minLatency,
		MaxLatencyMS:     maxLatency,
		AvgPacketLoss:    totalLoss / float64(count),
		MaxPacketLoss:    maxLoss,
		AvgCPU:           totalCPU / float64(count),
		MaxCPU:           maxCPU,
		AvgMemory:        totalMem / float64(count),
		MaxMemory:        maxMem,
		FirstSampleTime:  w.samples[0].timestamp,
		LatestSampleTime: w.samples[count-1].timestamp,
		CalculatedAt:     now,
	}
}

// CurrentMetrics calculates the current metrics without modifying the buffer.
func (w *SlidingWindow) CurrentMetrics(deviceID string, now time.Time) (model.RollingMetrics, bool) {
	if len(w.samples) == 0 {
		return model.RollingMetrics{}, false
	}
	return w.calculateMetrics(deviceID, now), true
}
