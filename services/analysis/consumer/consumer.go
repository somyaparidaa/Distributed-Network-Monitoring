package consumer

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"sync"

	"distributed-network-monitor/services/analysis/metrics"
	"distributed-network-monitor/services/analysis/model"
)

// Handler represents the downstream processing pipeline for decoded events.
type Handler interface {
	HandleTelemetry(ctx context.Context, event model.TelemetryEvent) error
	HandleHealth(ctx context.Context, event model.HealthEvent) error
}

// Message represents an abstract transport message received from a message broker.
type Message struct {
	Topic string
	Key   []byte
	Value []byte
}

// Consumer defines the interface for consuming messages from an event transport.
type Consumer interface {
	Start(ctx context.Context) error
	Close() error
}

// Dispatcher processes raw transport messages, decodes them safely, and routes them to the Handler.
type Dispatcher struct {
	handler        Handler
	telemetryTopic string
	healthTopic    string
}

// NewDispatcher constructs a Dispatcher for routing messages from designated topics to a Handler.
func NewDispatcher(handler Handler, telemetryTopic, healthTopic string) *Dispatcher {
	return &Dispatcher{
		handler:        handler,
		telemetryTopic: telemetryTopic,
		healthTopic:    healthTopic,
	}
}

// ProcessMessage safely deserializes, validates, and forwards a message to the handler.
// Malformed JSON or invalid schemas are logged and discarded without failing the caller.
func (d *Dispatcher) ProcessMessage(ctx context.Context, msg Message) error {
	if d.handler == nil {
		return errors.New("handler is nil")
	}

	switch msg.Topic {
	case d.telemetryTopic:
		var event model.TelemetryEvent
		if err := json.Unmarshal(msg.Value, &event); err != nil {
			metrics.KafkaEventsConsumedTotal.WithLabelValues(msg.Topic, "malformed_json").Inc()
			log.Printf("[ANALYSIS] warning: discarding malformed telemetry event on topic %q: %v | payload: %s", msg.Topic, err, string(msg.Value))
			return nil
		}
		if err := event.Validate(); err != nil {
			metrics.KafkaEventsConsumedTotal.WithLabelValues(msg.Topic, "invalid_schema").Inc()
			log.Printf("[ANALYSIS] warning: discarding invalid telemetry event: %v | payload: %s", err, string(msg.Value))
			return nil
		}
		metrics.KafkaEventsConsumedTotal.WithLabelValues(msg.Topic, "valid").Inc()
		if err := d.handler.HandleTelemetry(ctx, event); err != nil {
			log.Printf("[ANALYSIS] error handling telemetry event [%s] for device [%s]: %v", event.EventID, event.DeviceID, err)
			return err
		}
		return nil

	case d.healthTopic:
		var event model.HealthEvent
		if err := json.Unmarshal(msg.Value, &event); err != nil {
			metrics.KafkaEventsConsumedTotal.WithLabelValues(msg.Topic, "malformed_json").Inc()
			log.Printf("[ANALYSIS] warning: discarding malformed health event on topic %q: %v | payload: %s", msg.Topic, err, string(msg.Value))
			return nil
		}
		if err := event.Validate(); err != nil {
			metrics.KafkaEventsConsumedTotal.WithLabelValues(msg.Topic, "invalid_schema").Inc()
			log.Printf("[ANALYSIS] warning: discarding invalid health event: %v | payload: %s", err, string(msg.Value))
			return nil
		}
		metrics.KafkaEventsConsumedTotal.WithLabelValues(msg.Topic, "valid").Inc()
		if err := d.handler.HandleHealth(ctx, event); err != nil {
			log.Printf("[ANALYSIS] error handling health event [%s] for device [%s]: %v", event.EventID, event.DeviceID, err)
			return err
		}
		return nil

	default:
		log.Printf("[ANALYSIS] info: ignoring message from untracked topic %q", msg.Topic)
		return nil
	}
}

// MemoryConsumer provides a thread-safe, in-memory implementation of Consumer for tests and offline mode.
type MemoryConsumer struct {
	dispatcher  *Dispatcher
	msgCh       chan Message
	closed      bool
	mu          sync.Mutex
	injectedErr error
}

// NewMemoryConsumer constructs an in-memory consumer queue for testing.
func NewMemoryConsumer(dispatcher *Dispatcher, bufferSize int) *MemoryConsumer {
	if bufferSize <= 0 {
		bufferSize = 100
	}
	return &MemoryConsumer{
		dispatcher: dispatcher,
		msgCh:      make(chan Message, bufferSize),
	}
}

// SetInjectedError injects an error into the consumer for testing error recovery.
func (mc *MemoryConsumer) SetInjectedError(err error) {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	mc.injectedErr = err
}

// Enqueue feeds a message into the in-memory consumer queue.
func (mc *MemoryConsumer) Enqueue(msg Message) error {
	mc.mu.Lock()
	defer mc.mu.Unlock()

	if mc.closed {
		return errors.New("consumer is closed")
	}
	mc.msgCh <- msg
	return nil
}

// Start begins processing enqueued messages until context cancellation or Close.
func (mc *MemoryConsumer) Start(ctx context.Context) error {
	for {
		mc.mu.Lock()
		if mc.closed {
			mc.mu.Unlock()
			return nil
		}
		if mc.injectedErr != nil {
			err := mc.injectedErr
			mc.mu.Unlock()
			return err
		}
		mc.mu.Unlock()

		select {
		case <-ctx.Done():
			return ctx.Err()
		case msg, ok := <-mc.msgCh:
			if !ok {
				return nil
			}
			if err := mc.dispatcher.ProcessMessage(ctx, msg); err != nil {
				// Handler errors do not terminate consumption loop
				log.Printf("[ANALYSIS] consumer worker encountered handler error: %v", err)
			}
		}
	}
}

// Close gracefully closes the consumer channel and marks it closed.
func (mc *MemoryConsumer) Close() error {
	mc.mu.Lock()
	defer mc.mu.Unlock()

	if !mc.closed {
		mc.closed = true
		close(mc.msgCh)
	}
	return nil
}

// LoggingConsumer wraps a Consumer, logging lifecycle milestones.
type LoggingConsumer struct {
	underlying Consumer
	brokers    []string
	group      string
	topics     []string
}

// NewLoggingConsumer wraps an underlying Consumer with structured lifecycle logging.
func NewLoggingConsumer(underlying Consumer, brokers []string, group string, topics []string) *LoggingConsumer {
	return &LoggingConsumer{
		underlying: underlying,
		brokers:    brokers,
		group:      group,
		topics:     topics,
	}
}

// Start starts consumption with logging.
func (lc *LoggingConsumer) Start(ctx context.Context) error {
	log.Printf("[ANALYSIS] starting consumer group %q on topics %v via brokers %v", lc.group, lc.topics, lc.brokers)
	if lc.underlying != nil {
		return lc.underlying.Start(ctx)
	}
	<-ctx.Done()
	return ctx.Err()
}

// Close shuts down the consumer.
func (lc *LoggingConsumer) Close() error {
	log.Printf("[ANALYSIS] closing consumer group %q", lc.group)
	if lc.underlying != nil {
		return lc.underlying.Close()
	}
	return nil
}
