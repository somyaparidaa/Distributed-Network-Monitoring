package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"distributed-network-monitor/services/monitoring/health"
	"distributed-network-monitor/services/monitoring/polling"
)

func TestEventSerialization(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Millisecond)

	telem := polling.Telemetry{
		DeviceID:     "router-01",
		CPU:          45.2,
		Memory:       58.1,
		LatencyMS:    35,
		PacketLoss:   0.25,
		InterfaceUp:  true,
		Connectivity: true,
		Timestamp:    now,
	}

	telemEvent := NewTelemetryEvent(telem)
	if telemEvent.EventID == "" || telemEvent.DeviceID != "router-01" {
		t.Fatalf("unexpected TelemetryEvent fields: %+v", telemEvent)
	}

	data, err := json.Marshal(telemEvent)
	if err != nil {
		t.Fatalf("failed to marshal TelemetryEvent: %v", err)
	}

	var decodedTelem TelemetryEvent
	if err := json.Unmarshal(data, &decodedTelem); err != nil {
		t.Fatalf("failed to unmarshal TelemetryEvent: %v", err)
	}
	if decodedTelem.DeviceID != "router-01" || decodedTelem.CPU != 45.2 || decodedTelem.LatencyMS != 35 {
		t.Fatalf("decoded TelemetryEvent mismatch: %+v", decodedTelem)
	}

	healthEvent := NewHealthEvent("router-02", health.StatusHealthy, health.StatusWarning, 15, []string{"latency high"})
	if healthEvent.EventID == "" || healthEvent.DeviceID != "router-02" {
		t.Fatalf("unexpected HealthEvent fields: %+v", healthEvent)
	}

	hData, err := json.Marshal(healthEvent)
	if err != nil {
		t.Fatalf("failed to marshal HealthEvent: %v", err)
	}

	var decodedHealth HealthEvent
	if err := json.Unmarshal(hData, &decodedHealth); err != nil {
		t.Fatalf("failed to unmarshal HealthEvent: %v", err)
	}
	if decodedHealth.PreviousStatus != health.StatusHealthy || decodedHealth.CurrentStatus != health.StatusWarning || decodedHealth.Score != 15 {
		t.Fatalf("decoded HealthEvent mismatch: %+v", decodedHealth)
	}
}

func TestHealthEventOnlyEmittedWhenStatusActuallyChanges(t *testing.T) {
	producer := NewMemoryProducer()
	publisher := NewEventPublisher(producer)

	// 1. Initial transition from empty -> HEALTHY -> publishes event
	publisher.OnHealthTransition("router-01", "", health.StatusHealthy, 0, nil)
	if len(producer.HealthEvents()) != 1 {
		t.Fatalf("expected 1 health event on initial transition, got %d", len(producer.HealthEvents()))
	}

	// 2. Same status -> must NOT publish duplicate event
	publisher.OnHealthTransition("router-01", health.StatusHealthy, health.StatusHealthy, 0, nil)
	if len(producer.HealthEvents()) != 1 {
		t.Fatalf("expected no new health event when status is unchanged, got %d", len(producer.HealthEvents()))
	}

	// 3. Status changes HEALTHY -> WARNING -> publishes 2nd event
	publisher.OnHealthTransition("router-01", health.StatusHealthy, health.StatusWarning, 15, []string{"packet loss 1.2%"})
	if len(producer.HealthEvents()) != 2 {
		t.Fatalf("expected 2 health events after change to WARNING, got %d", len(producer.HealthEvents()))
	}

	// 4. Stays WARNING -> must NOT publish duplicate
	publisher.OnHealthTransition("router-01", health.StatusWarning, health.StatusWarning, 15, []string{"packet loss 1.4%"})
	if len(producer.HealthEvents()) != 2 {
		t.Fatalf("expected no new health event when status is still WARNING, got %d", len(producer.HealthEvents()))
	}

	// 5. Status changes WARNING -> DOWN -> publishes 3rd event
	publisher.OnHealthTransition("router-01", health.StatusWarning, health.StatusDown, 100, []string{"transport unavailable"})
	if len(producer.HealthEvents()) != 3 {
		t.Fatalf("expected 3 health events after change to DOWN, got %d", len(producer.HealthEvents()))
	}
}

func TestTelemetryEventEmittedForEverySuccessfulPoll(t *testing.T) {
	producer := NewMemoryProducer()
	publisher := NewEventPublisher(producer)
	ctx := context.Background()

	telem := polling.Telemetry{
		DeviceID:     "router-01",
		CPU:          30.0,
		Memory:       50.0,
		LatencyMS:    20,
		PacketLoss:   0.1,
		InterfaceUp:  true,
		Connectivity: true,
		Timestamp:    time.Now().UTC(),
	}

	// 3 successive poll events
	for range 3 {
		publisher.PublishTelemetry(ctx, telem)
	}

	if len(producer.TelemetryEvents()) != 3 {
		t.Fatalf("expected 3 telemetry events emitted, got %d", len(producer.TelemetryEvents()))
	}
}

func TestKafkaFailuresDoNotCrashOrPreventOperations(t *testing.T) {
	producer := NewMemoryProducer()
	producer.SetInjectedError(errors.New("kafka broker connection refused"))
	publisher := NewEventPublisher(producer)

	// Publishing telemetry with broken Kafka should not panic or fail the caller
	telem := polling.Telemetry{
		DeviceID:  "router-01",
		CPU:       40.0,
		Timestamp: time.Now().UTC(),
	}
	publisher.PublishTelemetry(context.Background(), telem)

	// Publishing health transition with broken Kafka should not panic
	publisher.OnHealthTransition("router-01", health.StatusHealthy, health.StatusCritical, 65, []string{"high cpu"})

	// Producer was closed
	if err := producer.Close(); err != nil {
		t.Fatalf("unexpected error closing producer: %v", err)
	}
}
