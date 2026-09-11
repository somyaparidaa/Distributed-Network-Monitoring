package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
)

// Producer defines the interface for publishing telemetry and health events to Kafka topics.
type Producer interface {
	PublishTelemetry(ctx context.Context, event TelemetryEvent) error
	PublishHealth(ctx context.Context, event HealthEvent) error
	Close() error
}

// Config specifies Kafka producer tuning and target topics.
type Config struct {
	Enabled        bool
	Brokers        []string
	TelemetryTopic string
	HealthTopic    string
}

// MemoryProducer provides an in-memory testable implementation of Producer.
type MemoryProducer struct {
	mu              sync.RWMutex
	telemetryEvents []TelemetryEvent
	healthEvents    []HealthEvent
	closed          bool
	injectedErr     error
}

// NewMemoryProducer constructs a MemoryProducer for testing or offline operation.
func NewMemoryProducer() *MemoryProducer {
	return &MemoryProducer{
		telemetryEvents: make([]TelemetryEvent, 0),
		healthEvents:    make([]HealthEvent, 0),
	}
}

// SetInjectedError allows tests to inject errors into publish calls.
func (m *MemoryProducer) SetInjectedError(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.injectedErr = err
}

// PublishTelemetry records a telemetry event.
func (m *MemoryProducer) PublishTelemetry(ctx context.Context, event TelemetryEvent) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return fmt.Errorf("producer is closed")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if m.injectedErr != nil {
		return m.injectedErr
	}

	m.telemetryEvents = append(m.telemetryEvents, event)
	return nil
}

// PublishHealth records a health transition event.
func (m *MemoryProducer) PublishHealth(ctx context.Context, event HealthEvent) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return fmt.Errorf("producer is closed")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if m.injectedErr != nil {
		return m.injectedErr
	}

	m.healthEvents = append(m.healthEvents, event)
	return nil
}

// Close marks the producer as closed.
func (m *MemoryProducer) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closed = true
	return nil
}

// TelemetryEvents returns a copy of all published telemetry events.
func (m *MemoryProducer) TelemetryEvents() []TelemetryEvent {
	m.mu.RLock()
	defer m.mu.RUnlock()
	events := make([]TelemetryEvent, len(m.telemetryEvents))
	copy(events, m.telemetryEvents)
	return events
}

// HealthEvents returns a copy of all published health events.
func (m *MemoryProducer) HealthEvents() []HealthEvent {
	m.mu.RLock()
	defer m.mu.RUnlock()
	events := make([]HealthEvent, len(m.healthEvents))
	copy(events, m.healthEvents)
	return events
}

// LoggingProducer wraps a Producer, logging serialized events and delegating to the underlying producer.
// It also provides a fallback when no live broker is enabled.
type LoggingProducer struct {
	underlying Producer
	config     Config
}

// NewLoggingProducer wraps an underlying producer with structured event logging.
func NewLoggingProducer(underlying Producer, cfg Config) *LoggingProducer {
	return &LoggingProducer{
		underlying: underlying,
		config:     cfg,
	}
}

// PublishTelemetry publishes a telemetry event, logging the action.
func (lp *LoggingProducer) PublishTelemetry(ctx context.Context, event TelemetryEvent) error {
	if lp.underlying != nil {
		if err := lp.underlying.PublishTelemetry(ctx, event); err != nil {
			return err
		}
	}
	return nil
}

// PublishHealth publishes a health transition event and logs the transition payload.
func (lp *LoggingProducer) PublishHealth(ctx context.Context, event HealthEvent) error {
	if lp.underlying != nil {
		if err := lp.underlying.PublishHealth(ctx, event); err != nil {
			return err
		}
	}
	data, _ := json.Marshal(event)
	log.Printf("[KAFKA] published health event to topic %q: %s", lp.config.HealthTopic, string(data))
	return nil
}

// Close closes the underlying producer.
func (lp *LoggingProducer) Close() error {
	if lp.underlying != nil {
		return lp.underlying.Close()
	}
	return nil
}

// Underlying returns the wrapped Producer instance.
func (lp *LoggingProducer) Underlying() Producer {
	return lp.underlying
}
