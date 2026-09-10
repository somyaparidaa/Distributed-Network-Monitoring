package health

import (
	"log"
	"strings"

	"distributed-network-monitor/services/monitoring/polling"
)

// ServiceEvaluator coordinates the evaluation and storage of device health assessments.
type ServiceEvaluator struct {
	telemetryStore *polling.Store
	healthStore    *Store
}

// NewServiceEvaluator constructs a ServiceEvaluator.
func NewServiceEvaluator(telemetryStore *polling.Store, healthStore *Store) *ServiceEvaluator {
	return &ServiceEvaluator{
		telemetryStore: telemetryStore,
		healthStore:    healthStore,
	}
}

// RecordPollSuccess evaluates and stores the health assessment for a device after a successful poll.
func (se *ServiceEvaluator) RecordPollSuccess(deviceID string, t polling.Telemetry) {
	prev, exists := se.healthStore.Get(deviceID)
	assessment := Evaluate(deviceID, t, false)
	se.healthStore.Set(deviceID, assessment)

	if !exists {
		log.Printf("[HEALTH] device [%s] initial status: %s (score: %d)", deviceID, assessment.Status, assessment.Score)
	} else if prev.Status != assessment.Status {
		reasonStr := ""
		if len(assessment.Reasons) > 0 {
			reasonStr = " | reasons: " + strings.Join(assessment.Reasons, "; ")
		}
		log.Printf("[HEALTH] device [%s] transitioned: %s -> %s (score: %d%s)",
			deviceID, prev.Status, assessment.Status, assessment.Score, reasonStr)
	}
}

// RecordPollFailure handles health assessment updates when a poll cycle fails.
// If isTransportDown is true, the device transitions to DOWN while preserving its last telemetry.
func (se *ServiceEvaluator) RecordPollFailure(deviceID string, isTransportDown bool) {
	if !isTransportDown {
		return
	}

	prev, exists := se.healthStore.Get(deviceID)

	// Retrieve existing telemetry snapshot if available (do not overwrite with fake metrics)
	t, found := se.telemetryStore.Get(deviceID)
	if !found {
		t = polling.Telemetry{
			DeviceID: deviceID,
		}
	}

	assessment := Evaluate(deviceID, t, true)
	se.healthStore.Set(deviceID, assessment)

	if !exists || prev.Status != StatusDown {
		reasonStr := ""
		if len(assessment.Reasons) > 0 {
			reasonStr = " | reasons: " + strings.Join(assessment.Reasons, "; ")
		}
		log.Printf("[HEALTH] device [%s] transitioned: %s -> %s (score: %d%s)",
			deviceID, prev.Status, StatusDown, assessment.Score, reasonStr)
	}
}
