package consumer

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"distributed-network-monitor/services/analysis/model"
	"github.com/segmentio/kafka-go"
)

type mockMessageReader struct {
	mu           sync.Mutex
	messages     []kafka.Message
	committed    []kafka.Message
	closed       bool
	fetchErr     error
	commitErr    error
	fetchBlockCh chan struct{}
}

func newMockReader(messages []kafka.Message) *mockMessageReader {
	return &mockMessageReader{
		messages:     messages,
		committed:    make([]kafka.Message, 0),
		fetchBlockCh: make(chan struct{}),
	}
}

func (r *mockMessageReader) FetchMessage(ctx context.Context) (kafka.Message, error) {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return kafka.Message{}, errors.New("reader closed")
	}
	if r.fetchErr != nil {
		err := r.fetchErr
		r.mu.Unlock()
		return kafka.Message{}, err
	}
	if len(r.messages) > 0 {
		msg := r.messages[0]
		r.messages = r.messages[1:]
		r.mu.Unlock()
		return msg, nil
	}
	r.mu.Unlock()

	// Wait for context cancellation or Close
	select {
	case <-ctx.Done():
		return kafka.Message{}, ctx.Err()
	case <-r.fetchBlockCh:
		return kafka.Message{}, errors.New("reader closed")
	}
}

func (r *mockMessageReader) CommitMessages(ctx context.Context, msgs ...kafka.Message) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.commitErr != nil {
		return r.commitErr
	}
	r.committed = append(r.committed, msgs...)
	return nil
}

func (r *mockMessageReader) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.closed {
		r.closed = true
		close(r.fetchBlockCh)
	}
	return nil
}

func TestKafkaConsumerConsumesBothTopicsAndCommits(t *testing.T) {
	handler := &mockHandler{}
	dispatcher := NewDispatcher(handler, "network.telemetry", "network.health-events")

	now := time.Now().UTC()
	telemPayload, _ := json.Marshal(model.TelemetryEvent{
		EventID:      "telem-k-1",
		DeviceID:     "router-01",
		Timestamp:    now,
		CPU:          35.5,
		Connectivity: true,
	})

	healthPayload, _ := json.Marshal(model.HealthEvent{
		EventID:       "health-k-1",
		DeviceID:      "router-02",
		Timestamp:     now,
		CurrentStatus: "CRITICAL",
		Score:         80,
	})

	readers := map[string]*mockMessageReader{
		"network.telemetry": newMockReader([]kafka.Message{
			{Topic: "network.telemetry", Key: []byte("router-01"), Value: telemPayload, Offset: 10},
		}),
		"network.health-events": newMockReader([]kafka.Message{
			{Topic: "network.health-events", Key: []byte("router-02"), Value: healthPayload, Offset: 20},
		}),
	}

	factory := func(brokers []string, groupID string, topic string) MessageReader {
		return readers[topic]
	}

	consumerCfg := KafkaConsumerConfig{
		Brokers: []string{"test-broker:9092"},
		GroupID: "test-group",
		Topics:  []string{"network.telemetry", "network.health-events"},
	}

	kc := NewKafkaConsumer(consumerCfg, dispatcher, factory)

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		errCh <- kc.Start(ctx)
	}()

	// Allow time for messages to be consumed and committed
	time.Sleep(50 * time.Millisecond)

	if handler.TelemetryCount() != 1 {
		t.Fatalf("expected 1 telemetry event handled, got %d", handler.TelemetryCount())
	}
	if handler.HealthCount() != 1 {
		t.Fatalf("expected 1 health event handled, got %d", handler.HealthCount())
	}

	// Verify offsets were committed
	readers["network.telemetry"].mu.Lock()
	if len(readers["network.telemetry"].committed) != 1 || readers["network.telemetry"].committed[0].Offset != 10 {
		t.Fatalf("expected offset 10 committed for telemetry, got %+v", readers["network.telemetry"].committed)
	}
	readers["network.telemetry"].mu.Unlock()

	readers["network.health-events"].mu.Lock()
	if len(readers["network.health-events"].committed) != 1 || readers["network.health-events"].committed[0].Offset != 20 {
		t.Fatalf("expected offset 20 committed for health, got %+v", readers["network.health-events"].committed)
	}
	readers["network.health-events"].mu.Unlock()

	// Shutdown cleanly
	cancel()
	_ = kc.Close()

	select {
	case err := <-errCh:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("unexpected error from kc.Start: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("KafkaConsumer did not stop within deadline")
	}
}

func TestKafkaConsumerHandlesMalformedMessageWithoutCrashing(t *testing.T) {
	handler := &mockHandler{}
	dispatcher := NewDispatcher(handler, "network.telemetry", "network.health-events")

	validPayload, _ := json.Marshal(model.TelemetryEvent{
		EventID:      "telem-ok",
		DeviceID:     "router-01",
		Timestamp:    time.Now().UTC(),
		CPU:          25.0,
		Connectivity: true,
	})

	telemReader := newMockReader([]kafka.Message{
		{Topic: "network.telemetry", Value: []byte("{ malformed json"), Offset: 1},
		{Topic: "network.telemetry", Value: validPayload, Offset: 2},
	})
	healthReader := newMockReader(nil)

	factory := func(brokers []string, groupID string, topic string) MessageReader {
		if topic == "network.telemetry" {
			return telemReader
		}
		return healthReader
	}

	kc := NewKafkaConsumer(KafkaConsumerConfig{
		Brokers: []string{"broker:9092"},
		GroupID: "test-grp",
		Topics:  []string{"network.telemetry", "network.health-events"},
	}, dispatcher, factory)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		_ = kc.Start(ctx)
	}()

	time.Sleep(50 * time.Millisecond)

	// Valid event was processed despite the preceding malformed message
	if handler.TelemetryCount() != 1 {
		t.Fatalf("expected valid event to be processed after malformed event, got %d", handler.TelemetryCount())
	}

	// Both offsets committed
	telemReader.mu.Lock()
	if len(telemReader.committed) != 2 {
		t.Fatalf("expected 2 messages committed (malformed and valid), got %d", len(telemReader.committed))
	}
	telemReader.mu.Unlock()

	_ = kc.Close()
}
