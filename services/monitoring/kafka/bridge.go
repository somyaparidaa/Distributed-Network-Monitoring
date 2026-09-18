package kafka

import (
	"context"
	"log"

	"distributed-network-monitor/services/monitoring/health"
	"distributed-network-monitor/services/monitoring/metrics"
	"distributed-network-monitor/services/monitoring/polling"
)

// EventPublisher bridges the monitoring engine with Kafka publishing.
type EventPublisher struct {
	producer Producer
}

// NewEventPublisher creates an EventPublisher wrapping a Producer.
func NewEventPublisher(producer Producer) *EventPublisher {
	return &EventPublisher{
		producer: producer,
	}
}

// PublishTelemetry satisfies polling.TelemetryPublisher.
func (ep *EventPublisher) PublishTelemetry(ctx context.Context, t polling.Telemetry) {
	if ep.producer == nil {
		return
	}

	event := NewTelemetryEvent(t)
	if err := ep.producer.PublishTelemetry(ctx, event); err != nil {
		metrics.KafkaPublishesTotal.WithLabelValues("telemetry", "failure").Inc()
		metrics.KafkaAvailable.Set(0)
		log.Printf("[KAFKA] warning: failed to publish telemetry event for device [%s]: %v", t.DeviceID, err)
		return
	}
	metrics.KafkaPublishesTotal.WithLabelValues("telemetry", "success").Inc()
	metrics.KafkaAvailable.Set(1)
}

// OnHealthTransition satisfies health.TransitionListener, ensuring health events are only emitted when status changes.
func (ep *EventPublisher) OnHealthTransition(
	deviceID string,
	prevStatus health.HealthStatus,
	currStatus health.HealthStatus,
	score int,
	reasons []string,
) {
	if ep.producer == nil {
		return
	}

	// Invariant: Health events must only be emitted when the evaluated health status actually changes
	if prevStatus == currStatus {
		return
	}

	event := NewHealthEvent(deviceID, prevStatus, currStatus, score, reasons)
	if err := ep.producer.PublishHealth(context.Background(), event); err != nil {
		metrics.KafkaPublishesTotal.WithLabelValues("health", "failure").Inc()
		metrics.KafkaAvailable.Set(0)
		log.Printf("[KAFKA] warning: failed to publish health event for device [%s]: %v", deviceID, err)
		return
	}
	metrics.KafkaPublishesTotal.WithLabelValues("health", "success").Inc()
	metrics.KafkaAvailable.Set(1)
}
