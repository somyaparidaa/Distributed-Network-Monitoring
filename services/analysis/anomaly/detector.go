package anomaly

import (
	"fmt"
	"sync"
	"time"

	"distributed-network-monitor/services/analysis/model"
)

// Explicit threshold constants for deterministic anomaly detection.
const (
	LatencyWarningThresholdMS  = 50.0
	LatencyCriticalThresholdMS = 150.0

	PacketLossWarningThreshold  = 1.0
	PacketLossCriticalThreshold = 5.0

	CPUWarningThreshold  = 80.0
	CPUCriticalThreshold = 90.0

	MemoryWarningThreshold  = 85.0
	MemoryCriticalThreshold = 92.0

	MinSamplesForRollingAnomaly = 2
)

// Detector evaluates incoming rolling metrics and health events against deterministic rules.
type Detector struct {
	mu           sync.RWMutex
	targetWindow string
	healthState  map[string]*model.HealthEvent
}

// NewDetector constructs a new Detector evaluating rolling metrics on the target window (defaults to "1m").
func NewDetector(targetWindow string) *Detector {
	if targetWindow == "" {
		targetWindow = "1m"
	}
	return &Detector{
		targetWindow: targetWindow,
		healthState:  make(map[string]*model.HealthEvent),
	}
}

// EvaluateMetrics evaluates a slice of RollingMetrics (from different windows) for a device,
// combines metric anomalies with any active health anomalies, and returns the updated DeviceAnalysis.
func (d *Detector) EvaluateMetrics(deviceID string, metricsList []model.RollingMetrics) model.DeviceAnalysis {
	d.mu.Lock()
	defer d.mu.Unlock()

	var targetMetric *model.RollingMetrics
	for i := range metricsList {
		if metricsList[i].Window == d.targetWindow {
			targetMetric = &metricsList[i]
			break
		}
	}
	if targetMetric == nil && len(metricsList) > 0 {
		targetMetric = &metricsList[0]
	}

	var anomalies []model.Anomaly

	if targetMetric != nil && targetMetric.SampleCount >= MinSamplesForRollingAnomaly {
		// 1. Latency check
		if targetMetric.AvgLatencyMS >= LatencyCriticalThresholdMS {
			anomalies = append(anomalies, model.Anomaly{
				Signal:    "LATENCY",
				Severity:  model.SeverityCritical,
				Value:     targetMetric.AvgLatencyMS,
				Threshold: LatencyCriticalThresholdMS,
				Reason:    fmt.Sprintf("sustained critical latency: average of %.1fms exceeds critical threshold of %.1fms over %s window", targetMetric.AvgLatencyMS, LatencyCriticalThresholdMS, targetMetric.Window),
			})
		} else if targetMetric.AvgLatencyMS >= LatencyWarningThresholdMS {
			anomalies = append(anomalies, model.Anomaly{
				Signal:    "LATENCY",
				Severity:  model.SeverityWarning,
				Value:     targetMetric.AvgLatencyMS,
				Threshold: LatencyWarningThresholdMS,
				Reason:    fmt.Sprintf("sustained high latency: average of %.1fms exceeds warning threshold of %.1fms over %s window", targetMetric.AvgLatencyMS, LatencyWarningThresholdMS, targetMetric.Window),
			})
		}

		// 2. Packet loss check
		if targetMetric.AvgPacketLoss >= PacketLossCriticalThreshold {
			anomalies = append(anomalies, model.Anomaly{
				Signal:    "PACKET_LOSS",
				Severity:  model.SeverityCritical,
				Value:     targetMetric.AvgPacketLoss,
				Threshold: PacketLossCriticalThreshold,
				Reason:    fmt.Sprintf("sustained critical packet loss: average of %.2f%% exceeds critical threshold of %.1f%% over %s window", targetMetric.AvgPacketLoss, PacketLossCriticalThreshold, targetMetric.Window),
			})
		} else if targetMetric.AvgPacketLoss >= PacketLossWarningThreshold {
			anomalies = append(anomalies, model.Anomaly{
				Signal:    "PACKET_LOSS",
				Severity:  model.SeverityWarning,
				Value:     targetMetric.AvgPacketLoss,
				Threshold: PacketLossWarningThreshold,
				Reason:    fmt.Sprintf("sustained high packet loss: average of %.2f%% exceeds warning threshold of %.1f%% over %s window", targetMetric.AvgPacketLoss, PacketLossWarningThreshold, targetMetric.Window),
			})
		}

		// 3. CPU utilization check
		if targetMetric.AvgCPU >= CPUCriticalThreshold {
			anomalies = append(anomalies, model.Anomaly{
				Signal:    "CPU",
				Severity:  model.SeverityCritical,
				Value:     targetMetric.AvgCPU,
				Threshold: CPUCriticalThreshold,
				Reason:    fmt.Sprintf("sustained critical CPU utilization: average of %.1f%% exceeds critical threshold of %.1f%% over %s window", targetMetric.AvgCPU, CPUCriticalThreshold, targetMetric.Window),
			})
		} else if targetMetric.AvgCPU >= CPUWarningThreshold {
			anomalies = append(anomalies, model.Anomaly{
				Signal:    "CPU",
				Severity:  model.SeverityWarning,
				Value:     targetMetric.AvgCPU,
				Threshold: CPUWarningThreshold,
				Reason:    fmt.Sprintf("sustained high CPU utilization: average of %.1f%% exceeds warning threshold of %.1f%% over %s window", targetMetric.AvgCPU, CPUWarningThreshold, targetMetric.Window),
			})
		}

		// 4. Memory utilization check
		if targetMetric.AvgMemory >= MemoryCriticalThreshold {
			anomalies = append(anomalies, model.Anomaly{
				Signal:    "MEMORY",
				Severity:  model.SeverityCritical,
				Value:     targetMetric.AvgMemory,
				Threshold: MemoryCriticalThreshold,
				Reason:    fmt.Sprintf("sustained critical memory utilization: average of %.1f%% exceeds critical threshold of %.1f%% over %s window", targetMetric.AvgMemory, MemoryCriticalThreshold, targetMetric.Window),
			})
		} else if targetMetric.AvgMemory >= MemoryWarningThreshold {
			anomalies = append(anomalies, model.Anomaly{
				Signal:    "MEMORY",
				Severity:  model.SeverityWarning,
				Value:     targetMetric.AvgMemory,
				Threshold: MemoryWarningThreshold,
				Reason:    fmt.Sprintf("sustained high memory utilization: average of %.1f%% exceeds warning threshold of %.1f%% over %s window", targetMetric.AvgMemory, MemoryWarningThreshold, targetMetric.Window),
			})
		}
	}

	// 5. Append any active health degradation anomaly
	if h, exists := d.healthState[deviceID]; exists && h != nil {
		if healthAnomaly, hasAnomaly := healthToAnomaly(*h); hasAnomaly {
			anomalies = append(anomalies, healthAnomaly)
		}
	}

	if anomalies == nil {
		anomalies = make([]model.Anomaly, 0)
	}

	return model.DeviceAnalysis{
		DeviceID:        deviceID,
		ActiveAnomalies: anomalies,
		CalculatedAt:    time.Now().UTC(),
	}
}

// EvaluateHealth records the device's latest health state, re-evaluates active anomalies,
// and returns the updated DeviceAnalysis.
func (d *Detector) EvaluateHealth(event model.HealthEvent) model.DeviceAnalysis {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.healthState[event.DeviceID] = &event

	var anomalies []model.Anomaly
	if healthAnomaly, hasAnomaly := healthToAnomaly(event); hasAnomaly {
		anomalies = append(anomalies, healthAnomaly)
	} else {
		anomalies = make([]model.Anomaly, 0)
	}

	return model.DeviceAnalysis{
		DeviceID:        event.DeviceID,
		ActiveAnomalies: anomalies,
		CalculatedAt:    event.Timestamp,
	}
}

func healthToAnomaly(event model.HealthEvent) (model.Anomaly, bool) {
	switch event.CurrentStatus {
	case "DOWN":
		return model.Anomaly{
			Signal:    "HEALTH",
			Severity:  model.SeverityCritical,
			Value:     float64(event.Score),
			Threshold: 100,
			Reason:    fmt.Sprintf("device is DOWN: %s", formatReasons(event.Reasons)),
		}, true
	case "CRITICAL":
		return model.Anomaly{
			Signal:    "HEALTH",
			Severity:  model.SeverityCritical,
			Value:     float64(event.Score),
			Threshold: 50,
			Reason:    fmt.Sprintf("device operational health is CRITICAL (score: %d): %s", event.Score, formatReasons(event.Reasons)),
		}, true
	case "WARNING":
		return model.Anomaly{
			Signal:    "HEALTH",
			Severity:  model.SeverityWarning,
			Value:     float64(event.Score),
			Threshold: 10,
			Reason:    fmt.Sprintf("device operational health is WARNING (score: %d): %s", event.Score, formatReasons(event.Reasons)),
		}, true
	default:
		// HEALTHY status: no anomaly
		return model.Anomaly{}, false
	}
}

func formatReasons(reasons []string) string {
	if len(reasons) == 0 {
		return "operational state change"
	}
	res := reasons[0]
	for i := 1; i < len(reasons); i++ {
		res += "; " + reasons[i]
	}
	return res
}
