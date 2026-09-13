package model

import (
	"time"
)

// Severity indicates the operational severity of an anomaly.
type Severity string

const (
	SeverityWarning  Severity = "WARNING"
	SeverityCritical Severity = "CRITICAL"
)

// Anomaly represents a discrete abnormal condition detected on a device.
type Anomaly struct {
	Signal    string   `json:"signal"`
	Severity  Severity `json:"severity"`
	Value     float64  `json:"value"`
	Threshold float64  `json:"threshold"`
	Reason    string   `json:"reason"`
}

// DeviceAnalysis holds the latest active anomalies and analysis summary for a device.
type DeviceAnalysis struct {
	DeviceID        string    `json:"device_id"`
	ActiveAnomalies []Anomaly `json:"active_anomalies"`
	CalculatedAt    time.Time `json:"calculated_at"`
}
