package health

import (
	"log"
	"strings"

	"distributed-network-monitor/services/monitoring/metrics"
	"distributed-network-monitor/services/monitoring/polling"
)

// TransitionListener receives notifications when a device's health status transitions.
type TransitionListener interface {
	OnHealthTransition(deviceID string, prevStatus, currStatus HealthStatus, score int, reasons []string)
}

// ServiceEvaluator coordinates the evaluation and storage of device health assessments.
type ServiceEvaluator struct {
	telemetryStore     *polling.Store
	healthStore        *Store
	transitionListener TransitionListener
}

// NewServiceEvaluator constructs a ServiceEvaluator.
func NewServiceEvaluator(telemetryStore *polling.Store, healthStore *Store) *ServiceEvaluator {
	return &ServiceEvaluator{
		telemetryStore: telemetryStore,
		healthStore:    healthStore,
	}
}

// SetTransitionListener registers a listener for health transitions.
func (se *ServiceEvaluator) SetTransitionListener(listener TransitionListener) {
	se.transitionListener = listener
}

// RecordPollSuccess evaluates and stores the health assessment for a device after a successful poll.
func (se *ServiceEvaluator) RecordPollSuccess(deviceID string, t polling.Telemetry) {
	prev, exists := se.healthStore.Get(deviceID)
	assessment := Evaluate(deviceID, t, false)
	se.healthStore.Set(deviceID, assessment)

	metrics.HealthEvaluationsTotal.WithLabelValues(string(assessment.Status)).Inc()

	if !exists {
		log.Printf("[HEALTH] device [%s] initial status: %s (score: %d)", deviceID, assessment.Status, assessment.Score)
		if se.transitionListener != nil {
			se.transitionListener.OnHealthTransition(deviceID, "", assessment.Status, assessment.Score, assessment.Reasons)
		}
	} else if prev.Status != assessment.Status {
		metrics.HealthTransitionsTotal.WithLabelValues(string(prev.Status), string(assessment.Status)).Inc()
		reasonStr := ""
		if len(assessment.Reasons) > 0 {
			reasonStr = " | reasons: " + strings.Join(assessment.Reasons, "; ")
		}
		log.Printf("[HEALTH] device [%s] transitioned: %s -> %s (score: %d%s)",
			deviceID, prev.Status, assessment.Status, assessment.Score, reasonStr)

		if se.transitionListener != nil {
			se.transitionListener.OnHealthTransition(deviceID, prev.Status, assessment.Status, assessment.Score, assessment.Reasons)
		}
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

	metrics.HealthEvaluationsTotal.WithLabelValues(string(assessment.Status)).Inc()

	if !exists || prev.Status != StatusDown {
		prevStatus := StatusHealthy
		if exists {
			prevStatus = prev.Status
		}

		metrics.HealthTransitionsTotal.WithLabelValues(string(prevStatus), string(StatusDown)).Inc()

		reasonStr := ""
		if len(assessment.Reasons) > 0 {
			reasonStr = " | reasons: " + strings.Join(assessment.Reasons, "; ")
		}
		log.Printf("[HEALTH] device [%s] transitioned: %s -> %s (score: %d%s)",
			deviceID, prevStatus, StatusDown, assessment.Score, reasonStr)

		if se.transitionListener != nil {
			se.transitionListener.OnHealthTransition(deviceID, prevStatus, StatusDown, assessment.Score, assessment.Reasons)
		}
	}
}
