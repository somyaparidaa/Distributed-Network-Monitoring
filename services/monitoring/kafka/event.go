package kafka

import (
	"crypto/rand"
	"fmt"
	"time"

	"distributed-network-monitor/services/monitoring/health"
	"distributed-network-monitor/services/monitoring/polling"
)

// TelemetryEvent represents a normalized telemetry event published to Kafka.
type TelemetryEvent struct {
	EventID      string    `json:"event_id"`
	DeviceID     string    `json:"device_id"`
	Timestamp    time.Time `json:"timestamp"`
	CPU          float64   `json:"cpu"`
	Memory       float64   `json:"memory"`
	LatencyMS    int       `json:"latency_ms"`
	PacketLoss   float64   `json:"packet_loss"`
	InterfaceUp  bool      `json:"interface_up"`
	Connectivity bool      `json:"connectivity"`
}

// HealthEvent represents an event emitted when a device's health status changes.
type HealthEvent struct {
	EventID        string              `json:"event_id"`
	DeviceID       string              `json:"device_id"`
	Timestamp      time.Time           `json:"timestamp"`
	PreviousStatus health.HealthStatus `json:"previous_status"`
	CurrentStatus  health.HealthStatus `json:"current_status"`
	Score          int                 `json:"score"`
	Reasons        []string            `json:"reasons,omitempty"`
}

// NewTelemetryEvent converts a polling.Telemetry model into a stable TelemetryEvent.
func NewTelemetryEvent(t polling.Telemetry) TelemetryEvent {
	return TelemetryEvent{
		EventID:      generateEventID(),
		DeviceID:     t.DeviceID,
		Timestamp:    t.Timestamp.UTC(),
		CPU:          t.CPU,
		Memory:       t.Memory,
		LatencyMS:    t.LatencyMS,
		PacketLoss:   t.PacketLoss,
		InterfaceUp:  t.InterfaceUp,
		Connectivity: t.Connectivity,
	}
}

// NewHealthEvent creates a HealthEvent capturing a health state transition.
func NewHealthEvent(
	deviceID string,
	prevStatus health.HealthStatus,
	currStatus health.HealthStatus,
	score int,
	reasons []string,
) HealthEvent {
	return HealthEvent{
		EventID:        generateEventID(),
		DeviceID:       deviceID,
		Timestamp:      time.Now().UTC(),
		PreviousStatus: prevStatus,
		CurrentStatus:  currStatus,
		Score:          score,
		Reasons:        reasons,
	}
}

func generateEventID() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return fmt.Sprintf("%x-%d", b, time.Now().UnixNano())
}
