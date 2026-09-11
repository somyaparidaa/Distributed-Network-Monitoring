package model

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

// TelemetryEvent represents a normalized telemetry event consumed from Kafka.
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

// Validate ensures all required fields in a TelemetryEvent are present and valid.
func (e TelemetryEvent) Validate() error {
	if strings.TrimSpace(e.EventID) == "" {
		return errors.New("event_id cannot be empty")
	}
	if strings.TrimSpace(e.DeviceID) == "" {
		return errors.New("device_id cannot be empty")
	}
	if e.Timestamp.IsZero() {
		return errors.New("timestamp cannot be zero")
	}
	return nil
}

// HealthEvent represents a device health state transition event consumed from Kafka.
type HealthEvent struct {
	EventID        string    `json:"event_id"`
	DeviceID       string    `json:"device_id"`
	Timestamp      time.Time `json:"timestamp"`
	PreviousStatus string    `json:"previous_status"`
	CurrentStatus  string    `json:"current_status"`
	Score          int       `json:"score"`
	Reasons        []string  `json:"reasons,omitempty"`
}

// Validate ensures all required fields in a HealthEvent are present and valid.
func (e HealthEvent) Validate() error {
	if strings.TrimSpace(e.EventID) == "" {
		return errors.New("event_id cannot be empty")
	}
	if strings.TrimSpace(e.DeviceID) == "" {
		return errors.New("device_id cannot be empty")
	}
	if e.Timestamp.IsZero() {
		return errors.New("timestamp cannot be zero")
	}
	if strings.TrimSpace(e.CurrentStatus) == "" {
		return errors.New("current_status cannot be empty")
	}
	switch e.CurrentStatus {
	case "HEALTHY", "WARNING", "CRITICAL", "DOWN":
		// Known valid health status values from Monitoring health evaluator
	default:
		return fmt.Errorf("unknown current_status %q", e.CurrentStatus)
	}
	return nil
}
