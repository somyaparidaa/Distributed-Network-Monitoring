package model

import (
	"time"
)

// RollingMetrics represents time-windowed aggregated statistics for a device.
type RollingMetrics struct {
	DeviceID         string    `json:"device_id"`
	Window           string    `json:"window"`
	SampleCount      int       `json:"sample_count"`
	AvgLatencyMS     float64   `json:"avg_latency_ms"`
	MinLatencyMS     int       `json:"min_latency_ms"`
	MaxLatencyMS     int       `json:"max_latency_ms"`
	AvgPacketLoss    float64   `json:"avg_packet_loss"`
	MaxPacketLoss    float64   `json:"max_packet_loss"`
	AvgCPU           float64   `json:"avg_cpu"`
	MaxCPU           float64   `json:"max_cpu"`
	AvgMemory        float64   `json:"avg_memory"`
	MaxMemory        float64   `json:"max_memory"`
	FirstSampleTime  time.Time `json:"first_sample_time"`
	LatestSampleTime time.Time `json:"latest_sample_time"`
	CalculatedAt     time.Time `json:"calculated_at"`
}
