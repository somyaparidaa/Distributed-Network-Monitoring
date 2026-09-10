package health

import (
	"fmt"
	"time"

	"distributed-network-monitor/services/monitoring/polling"
)

// Explicit metric thresholds for health classification.
const (
	WarningCPU        = 70.0
	CriticalCPU       = 85.0
	WarningMemory     = 75.0
	CriticalMemory    = 90.0
	WarningLatencyMS  = 50
	CriticalLatencyMS = 150
	WarningPacketLoss = 1.0
	CriticalPacketLoss = 5.0
)

// Evaluate performs a purely deterministic health evaluation of a device.
//
// Evaluation precedence:
// 1. DOWN (transport failure from StateTracker, InterfaceUp == false, or Connectivity == false)
// 2. CRITICAL (at least one metric meets or exceeds its critical threshold)
// 3. WARNING (at least one metric meets or exceeds its warning threshold)
// 4. HEALTHY (all metrics within normal operating bounds)
//
// Score calculation:
// - DOWN: 100
// - CRITICAL: 50 + (10 per violated critical threshold) + (5 per violated warning threshold)
// - WARNING: 10 + (5 per violated warning threshold)
// - HEALTHY: 0
func Evaluate(deviceID string, t polling.Telemetry, isTransportDown bool) Assessment {
	now := time.Now().UTC()

	// 1. DOWN Evaluation (Highest Precedence)
	var downReasons []string
	if isTransportDown {
		downReasons = append(downReasons, "transport unavailable")
	}
	if !t.InterfaceUp {
		downReasons = append(downReasons, "network interface is down")
	}
	if !t.Connectivity {
		downReasons = append(downReasons, "network connectivity is lost")
	}

	if len(downReasons) > 0 {
		return Assessment{
			DeviceID:    deviceID,
			Status:      StatusDown,
			Score:       100,
			Reasons:     downReasons,
			EvaluatedAt: now,
		}
	}

	// 2 & 3. Multi-signal metric evaluation (CRITICAL and WARNING)
	var criticalReasons []string
	var warningReasons []string

	// CPU
	if t.CPU >= CriticalCPU {
		criticalReasons = append(criticalReasons, fmt.Sprintf("CPU utilization %.1f%% >= %.1f%% critical threshold", t.CPU, CriticalCPU))
	} else if t.CPU >= WarningCPU {
		warningReasons = append(warningReasons, fmt.Sprintf("CPU utilization %.1f%% >= %.1f%% warning threshold", t.CPU, WarningCPU))
	}

	// Memory
	if t.Memory >= CriticalMemory {
		criticalReasons = append(criticalReasons, fmt.Sprintf("memory utilization %.1f%% >= %.1f%% critical threshold", t.Memory, CriticalMemory))
	} else if t.Memory >= WarningMemory {
		warningReasons = append(warningReasons, fmt.Sprintf("memory utilization %.1f%% >= %.1f%% warning threshold", t.Memory, WarningMemory))
	}

	// Latency
	if t.LatencyMS >= CriticalLatencyMS {
		criticalReasons = append(criticalReasons, fmt.Sprintf("latency %dms >= %dms critical threshold", t.LatencyMS, CriticalLatencyMS))
	} else if t.LatencyMS >= WarningLatencyMS {
		warningReasons = append(warningReasons, fmt.Sprintf("latency %dms >= %dms warning threshold", t.LatencyMS, WarningLatencyMS))
	}

	// Packet Loss
	if t.PacketLoss >= CriticalPacketLoss {
		criticalReasons = append(criticalReasons, fmt.Sprintf("packet loss %.2f%% >= %.2f%% critical threshold", t.PacketLoss, CriticalPacketLoss))
	} else if t.PacketLoss >= WarningPacketLoss {
		warningReasons = append(warningReasons, fmt.Sprintf("packet loss %.2f%% >= %.2f%% warning threshold", t.PacketLoss, WarningPacketLoss))
	}

	// Determine classification
	if len(criticalReasons) > 0 {
		allReasons := append(criticalReasons, warningReasons...)
		score := 50 + (len(criticalReasons) * 10) + (len(warningReasons) * 5)
		if score > 95 {
			score = 95
		}
		return Assessment{
			DeviceID:    deviceID,
			Status:      StatusCritical,
			Score:       score,
			Reasons:     allReasons,
			EvaluatedAt: now,
		}
	}

	if len(warningReasons) > 0 {
		score := 10 + (len(warningReasons) * 5)
		return Assessment{
			DeviceID:    deviceID,
			Status:      StatusWarning,
			Score:       score,
			Reasons:     warningReasons,
			EvaluatedAt: now,
		}
	}

	// 4. HEALTHY
	return Assessment{
		DeviceID:    deviceID,
		Status:      StatusHealthy,
		Score:       0,
		Reasons:     nil,
		EvaluatedAt: now,
	}
}
