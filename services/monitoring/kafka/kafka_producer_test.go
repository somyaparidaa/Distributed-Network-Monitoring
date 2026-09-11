package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"distributed-network-monitor/services/monitoring/health"
	"github.com/segmentio/kafka-go"
)

type mockMessageWriter struct {
	mu       sync.Mutex
	messages []kafka.Message
	closed   bool
	writeErr error
}

func newMockWriter() *mockMessageWriter {
	return &mockMessageWriter{
		messages: make([]kafka.Message, 0),
	}
}

func (m *mockMessageWriter) WriteMessages(ctx context.Context, msgs ...kafka.Message) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return errors.New("writer closed")
	}
	if m.writeErr != nil {
		return m.writeErr
	}
	m.messages = append(m.messages, msgs...)
	return nil
}

func (m *mockMessageWriter) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closed = true
	return nil
}

func (m *mockMessageWriter) Messages() []kafka.Message {
	m.mu.Lock()
	defer m.mu.Unlock()
	res := make([]kafka.Message, len(m.messages))
	copy(res, m.messages)
	return res
}

func TestKafkaProducerPublishTelemetryAndHealth(t *testing.T) {
	telemWriter := newMockWriter()
	healthWriter := newMockWriter()

	factory := func(brokers []string, topic string) MessageWriter {
		if topic == "network.telemetry" {
			return telemWriter
		}
		return healthWriter
	}

	cfg := Config{
		Enabled:        true,
		Brokers:        []string{"localhost:9092"},
		TelemetryTopic: "network.telemetry",
		HealthTopic:    "network.health-events",
	}

	producer, err := NewKafkaProducer(cfg, factory)
	if err != nil {
		t.Fatalf("failed to create KafkaProducer: %v", err)
	}

	now := time.Now().UTC()
	telem := TelemetryEvent{
		EventID:      "telem-1",
		DeviceID:     "router-01",
		Timestamp:    now,
		CPU:          42.0,
		Memory:       50.0,
		LatencyMS:    25,
		PacketLoss:   0.1,
		InterfaceUp:  true,
		Connectivity: true,
	}

	if err := producer.PublishTelemetry(context.Background(), telem); err != nil {
		t.Fatalf("unexpected error publishing telemetry: %v", err)
	}

	telemMsgs := telemWriter.Messages()
	if len(telemMsgs) != 1 {
		t.Fatalf("expected 1 telemetry message written, got %d", len(telemMsgs))
	}
	if string(telemMsgs[0].Key) != "router-01" {
		t.Fatalf("expected key 'router-01', got %q", string(telemMsgs[0].Key))
	}

	var decodedTelem TelemetryEvent
	if err := json.Unmarshal(telemMsgs[0].Value, &decodedTelem); err != nil {
		t.Fatalf("failed to unmarshal written telemetry: %v", err)
	}
	if decodedTelem.EventID != "telem-1" || decodedTelem.CPU != 42.0 {
		t.Fatalf("decoded telemetry mismatch: %+v", decodedTelem)
	}

	// Publish HealthEvent
	healthEvt := HealthEvent{
		EventID:        "health-1",
		DeviceID:       "router-02",
		Timestamp:      now,
		PreviousStatus: health.StatusHealthy,
		CurrentStatus:  health.StatusWarning,
		Score:          20,
		Reasons:        []string{"latency high"},
	}

	if err := producer.PublishHealth(context.Background(), healthEvt); err != nil {
		t.Fatalf("unexpected error publishing health: %v", err)
	}

	healthMsgs := healthWriter.Messages()
	if len(healthMsgs) != 1 {
		t.Fatalf("expected 1 health message written, got %d", len(healthMsgs))
	}
	if string(healthMsgs[0].Key) != "router-02" {
		t.Fatalf("expected key 'router-02', got %q", string(healthMsgs[0].Key))
	}

	var decodedHealth HealthEvent
	if err := json.Unmarshal(healthMsgs[0].Value, &decodedHealth); err != nil {
		t.Fatalf("failed to unmarshal written health: %v", err)
	}
	if decodedHealth.EventID != "health-1" || decodedHealth.CurrentStatus != health.StatusWarning {
		t.Fatalf("decoded health mismatch: %+v", decodedHealth)
	}

	// Test clean Close
	if err := producer.Close(); err != nil {
		t.Fatalf("unexpected error closing producer: %v", err)
	}
	if !telemWriter.closed || !healthWriter.closed {
		t.Fatalf("expected both writers to be closed: telem=%v health=%v", telemWriter.closed, healthWriter.closed)
	}

	// Writing after close should fail
	if err := producer.PublishTelemetry(context.Background(), telem); err == nil {
		t.Fatal("expected error publishing after close, got nil")
	}
}

func TestKafkaProducerPropagatesWriteErrors(t *testing.T) {
	telemWriter := newMockWriter()
	telemWriter.writeErr = errors.New("network timeout to broker")

	factory := func(brokers []string, topic string) MessageWriter {
		return telemWriter
	}

	cfg := Config{
		Enabled:        true,
		Brokers:        []string{"localhost:9092"},
		TelemetryTopic: "network.telemetry",
		HealthTopic:    "network.health-events",
	}

	producer, err := NewKafkaProducer(cfg, factory)
	if err != nil {
		t.Fatalf("failed to create KafkaProducer: %v", err)
	}

	err = producer.PublishTelemetry(context.Background(), TelemetryEvent{DeviceID: "router-01"})
	if err == nil {
		t.Fatal("expected error from PublishTelemetry when writer fails, got nil")
	}
}
