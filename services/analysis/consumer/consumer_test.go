package consumer

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"distributed-network-monitor/services/analysis/model"
)

type mockHandler struct {
	mu              sync.Mutex
	telemetryEvents []model.TelemetryEvent
	healthEvents    []model.HealthEvent
	injectedErr     error
}

func (m *mockHandler) HandleTelemetry(ctx context.Context, event model.TelemetryEvent) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.injectedErr != nil {
		return m.injectedErr
	}
	m.telemetryEvents = append(m.telemetryEvents, event)
	return nil
}

func (m *mockHandler) HandleHealth(ctx context.Context, event model.HealthEvent) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.injectedErr != nil {
		return m.injectedErr
	}
	m.healthEvents = append(m.healthEvents, event)
	return nil
}

func (m *mockHandler) TelemetryCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.telemetryEvents)
}

func (m *mockHandler) HealthCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.healthEvents)
}

func TestValidTelemetryEventDecodedAndHandled(t *testing.T) {
	handler := &mockHandler{}
	dispatcher := NewDispatcher(handler, "network.telemetry", "network.health-events")

	now := time.Now().UTC().Truncate(time.Millisecond)
	telem := model.TelemetryEvent{
		EventID:      "evt-telem-101",
		DeviceID:     "router-01",
		Timestamp:    now,
		CPU:          42.5,
		Memory:       61.2,
		LatencyMS:    28,
		PacketLoss:   0.05,
		InterfaceUp:  true,
		Connectivity: true,
	}

	payload, err := json.Marshal(telem)
	if err != nil {
		t.Fatalf("failed to marshal telemetry: %v", err)
	}

	msg := Message{
		Topic: "network.telemetry",
		Key:   []byte("router-01"),
		Value: payload,
	}

	if err := dispatcher.ProcessMessage(context.Background(), msg); err != nil {
		t.Fatalf("expected nil error processing valid telemetry, got: %v", err)
	}

	if handler.TelemetryCount() != 1 {
		t.Fatalf("expected 1 telemetry event received by handler, got %d", handler.TelemetryCount())
	}

	received := handler.telemetryEvents[0]
	if received.EventID != "evt-telem-101" || received.DeviceID != "router-01" || received.CPU != 42.5 {
		t.Fatalf("received event content mismatch: %+v", received)
	}
}

func TestValidHealthEventDecodedAndHandled(t *testing.T) {
	handler := &mockHandler{}
	dispatcher := NewDispatcher(handler, "network.telemetry", "network.health-events")

	now := time.Now().UTC().Truncate(time.Millisecond)
	health := model.HealthEvent{
		EventID:        "evt-health-201",
		DeviceID:       "router-02",
		Timestamp:      now,
		PreviousStatus: "HEALTHY",
		CurrentStatus:  "WARNING",
		Score:          25,
		Reasons:        []string{"high latency"},
	}

	payload, err := json.Marshal(health)
	if err != nil {
		t.Fatalf("failed to marshal health event: %v", err)
	}

	msg := Message{
		Topic: "network.health-events",
		Key:   []byte("router-02"),
		Value: payload,
	}

	if err := dispatcher.ProcessMessage(context.Background(), msg); err != nil {
		t.Fatalf("expected nil error processing valid health event, got: %v", err)
	}

	if handler.HealthCount() != 1 {
		t.Fatalf("expected 1 health event received by handler, got %d", handler.HealthCount())
	}

	received := handler.healthEvents[0]
	if received.EventID != "evt-health-201" || received.CurrentStatus != "WARNING" || received.Score != 25 {
		t.Fatalf("received health event content mismatch: %+v", received)
	}
}

func TestMalformedJSONDoesNotTerminateOrCrash(t *testing.T) {
	handler := &mockHandler{}
	dispatcher := NewDispatcher(handler, "network.telemetry", "network.health-events")
	consumer := NewMemoryConsumer(dispatcher, 10)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	consumerErrCh := make(chan error, 1)
	go func() {
		consumerErrCh <- consumer.Start(ctx)
	}()

	// 1. Send malformed JSON message to telemetry topic
	_ = consumer.Enqueue(Message{
		Topic: "network.telemetry",
		Value: []byte(`{ invalid json payload ...`),
	})

	// 2. Send malformed JSON message to health topic
	_ = consumer.Enqueue(Message{
		Topic: "network.health-events",
		Value: []byte(`not even json`),
	})

	// 3. Send valid telemetry event right after to confirm consumption continues
	validTelem := model.TelemetryEvent{
		EventID:      "evt-valid-301",
		DeviceID:     "router-03",
		Timestamp:    time.Now().UTC(),
		CPU:          30.0,
		Connectivity: true,
	}
	validPayload, _ := json.Marshal(validTelem)
	_ = consumer.Enqueue(Message{
		Topic: "network.telemetry",
		Value: validPayload,
	})

	// Allow worker time to process queue
	time.Sleep(50 * time.Millisecond)

	// Verify valid event processed despite previous malformed messages
	if handler.TelemetryCount() != 1 {
		t.Fatalf("expected consumer to process valid event after malformed ones, got %d events", handler.TelemetryCount())
	}

	cancel()
	err := <-consumerErrCh
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("unexpected consumer error: %v", err)
	}
}

func TestMissingOrInvalidRequiredFieldsDiscarded(t *testing.T) {
	handler := &mockHandler{}
	dispatcher := NewDispatcher(handler, "network.telemetry", "network.health-events")

	// Missing device_id in telemetry
	invalidTelem := model.TelemetryEvent{
		EventID:   "evt-1",
		Timestamp: time.Now().UTC(),
	}
	invalidTelemJSON, _ := json.Marshal(invalidTelem)
	_ = dispatcher.ProcessMessage(context.Background(), Message{
		Topic: "network.telemetry",
		Value: invalidTelemJSON,
	})

	// Missing timestamp in health event
	invalidHealth := model.HealthEvent{
		EventID:       "evt-2",
		DeviceID:      "router-01",
		CurrentStatus: "HEALTHY",
	}
	invalidHealthJSON, _ := json.Marshal(invalidHealth)
	_ = dispatcher.ProcessMessage(context.Background(), Message{
		Topic: "network.health-events",
		Value: invalidHealthJSON,
	})

	// Unknown health status value
	unknownStatusHealth := model.HealthEvent{
		EventID:       "evt-3",
		DeviceID:      "router-01",
		Timestamp:     time.Now().UTC(),
		CurrentStatus: "UNKNOWN_BOGUS_STATUS",
	}
	unknownStatusJSON, _ := json.Marshal(unknownStatusHealth)
	_ = dispatcher.ProcessMessage(context.Background(), Message{
		Topic: "network.health-events",
		Value: unknownStatusJSON,
	})

	if handler.TelemetryCount() != 0 || handler.HealthCount() != 0 {
		t.Fatalf("expected 0 events dispatched due to schema validation failures, got telem=%d health=%d",
			handler.TelemetryCount(), handler.HealthCount())
	}
}

func TestContextCancellationGracefulShutdown(t *testing.T) {
	handler := &mockHandler{}
	dispatcher := NewDispatcher(handler, "network.telemetry", "network.health-events")
	consumer := NewMemoryConsumer(dispatcher, 10)

	ctx, cancel := context.WithCancel(context.Background())

	errCh := make(chan error, 1)
	go func() {
		errCh <- consumer.Start(ctx)
	}()

	// Cancel context to trigger shutdown
	cancel()

	select {
	case err := <-errCh:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context.Canceled, got: %v", err)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("consumer did not stop within deadline upon context cancellation")
	}

	// Ensure Close can be called safely
	if err := consumer.Close(); err != nil {
		t.Fatalf("unexpected error on Close: %v", err)
	}
}

func TestConsumerInjectedErrorHandledSafely(t *testing.T) {
	handler := &mockHandler{
		injectedErr: errors.New("pipeline temporarily overloaded"),
	}
	dispatcher := NewDispatcher(handler, "network.telemetry", "network.health-events")
	consumer := NewMemoryConsumer(dispatcher, 10)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- consumer.Start(ctx)
	}()

	// Enqueue valid telemetry; handler will return error
	validTelem := model.TelemetryEvent{
		EventID:      "evt-err-1",
		DeviceID:     "router-01",
		Timestamp:    time.Now().UTC(),
		CPU:          50.0,
		Connectivity: true,
	}
	data, _ := json.Marshal(validTelem)
	_ = consumer.Enqueue(Message{Topic: "network.telemetry", Value: data})

	time.Sleep(30 * time.Millisecond)

	// Now remove the injected handler error and enqueue another event
	handler.mu.Lock()
	handler.injectedErr = nil
	handler.mu.Unlock()

	validTelem.EventID = "evt-ok-2"
	data2, _ := json.Marshal(validTelem)
	_ = consumer.Enqueue(Message{Topic: "network.telemetry", Value: data2})

	time.Sleep(30 * time.Millisecond)

	if handler.TelemetryCount() != 1 {
		t.Fatalf("expected consumer to remain running and process 2nd event, got %d", handler.TelemetryCount())
	}

	cancel()
	<-errCh
}
