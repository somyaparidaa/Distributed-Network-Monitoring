package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
)

// MessageWriter defines the minimal interface for writing messages to Kafka.
// This interface allows mocking kafka.Writer in tests without a live broker.
type MessageWriter interface {
	WriteMessages(ctx context.Context, msgs ...kafka.Message) error
	io.Closer
}

// WriterFactory constructs a MessageWriter for a specific topic.
type WriterFactory func(brokers []string, topic string) MessageWriter

// DefaultWriterFactory constructs standard segmentio/kafka-go Writers.
func DefaultWriterFactory(brokers []string, topic string) MessageWriter {
	return &kafka.Writer{
		Addr:                   kafka.TCP(brokers...),
		Topic:                  topic,
		Balancer:               &kafka.Hash{},
		WriteTimeout:           5 * time.Second,
		RequiredAcks:           kafka.RequireOne,
		AllowAutoTopicCreation: true,
	}
}

// KafkaProducer implements Producer using live segmentio/kafka-go writers.
// It publishes TelemetryEvent and HealthEvent to dedicated topics, keyed by device ID.
type KafkaProducer struct {
	telemetryWriter MessageWriter
	healthWriter    MessageWriter
	telemetryTopic  string
	healthTopic     string
	closed          bool
	mu              sync.RWMutex
}

// NewKafkaProducer creates a KafkaProducer connected to the specified brokers and topics.
func NewKafkaProducer(cfg Config, factory WriterFactory) (*KafkaProducer, error) {
	if len(cfg.Brokers) == 0 {
		return nil, errors.New("kafka brokers cannot be empty")
	}
	if factory == nil {
		factory = DefaultWriterFactory
	}

	telemWriter := factory(cfg.Brokers, cfg.TelemetryTopic)
	healthWriter := factory(cfg.Brokers, cfg.HealthTopic)

	return &KafkaProducer{
		telemetryWriter: telemWriter,
		healthWriter:    healthWriter,
		telemetryTopic:  cfg.TelemetryTopic,
		healthTopic:     cfg.HealthTopic,
	}, nil
}

// PublishTelemetry serializes and writes a TelemetryEvent to the telemetry topic.
func (kp *KafkaProducer) PublishTelemetry(ctx context.Context, event TelemetryEvent) error {
	kp.mu.RLock()
	if kp.closed {
		kp.mu.RUnlock()
		return errors.New("kafka producer is closed")
	}
	writer := kp.telemetryWriter
	kp.mu.RUnlock()

	data, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal telemetry event: %w", err)
	}

	msg := kafka.Message{
		Key:   []byte(event.DeviceID),
		Value: data,
		Time:  event.Timestamp,
	}

	if err := writer.WriteMessages(ctx, msg); err != nil {
		return fmt.Errorf("publish telemetry to topic %q: %w", kp.telemetryTopic, err)
	}
	return nil
}

// PublishHealth serializes and writes a HealthEvent to the health topic.
func (kp *KafkaProducer) PublishHealth(ctx context.Context, event HealthEvent) error {
	kp.mu.RLock()
	if kp.closed {
		kp.mu.RUnlock()
		return errors.New("kafka producer is closed")
	}
	writer := kp.healthWriter
	kp.mu.RUnlock()

	data, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal health event: %w", err)
	}

	msg := kafka.Message{
		Key:   []byte(event.DeviceID),
		Value: data,
		Time:  event.Timestamp,
	}

	if err := writer.WriteMessages(ctx, msg); err != nil {
		return fmt.Errorf("publish health event to topic %q: %w", kp.healthTopic, err)
	}
	return nil
}

// Close closes both Kafka writers cleanly.
func (kp *KafkaProducer) Close() error {
	kp.mu.Lock()
	defer kp.mu.Unlock()

	if kp.closed {
		return nil
	}
	kp.closed = true

	var telemErr, healthErr error
	if kp.telemetryWriter != nil {
		if err := kp.telemetryWriter.Close(); err != nil && !errors.Is(err, io.EOF) {
			telemErr = fmt.Errorf("close telemetry writer: %w", err)
			log.Printf("[KAFKA] error closing telemetry writer: %v", err)
		}
	}
	if kp.healthWriter != nil {
		if err := kp.healthWriter.Close(); err != nil && !errors.Is(err, io.EOF) {
			healthErr = fmt.Errorf("close health writer: %w", err)
			log.Printf("[KAFKA] error closing health writer: %v", err)
		}
	}

	if telemErr != nil {
		return telemErr
	}
	return healthErr
}
