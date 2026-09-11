package consumer

import (
	"context"
	"io"
	"time"

	"github.com/segmentio/kafka-go"
)

// MessageReader defines the minimal interface needed to fetch messages from Kafka.
// This interface allows mocking kafka.Reader in unit tests without a live broker.
type MessageReader interface {
	FetchMessage(ctx context.Context) (kafka.Message, error)
	CommitMessages(ctx context.Context, msgs ...kafka.Message) error
	io.Closer
}

// KafkaConsumerConfig specifies the broker, group, and topic configuration for a Kafka consumer.
type KafkaConsumerConfig struct {
	Brokers       []string
	GroupID       string
	Topics        []string
	CommitTimeout time.Duration
}

// DefaultReaderFactory constructs standard segmentio/kafka-go Readers.
func DefaultReaderFactory(brokers []string, groupID string, topic string) MessageReader {
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:        brokers,
		GroupID:        groupID,
		Topic:          topic,
		MinBytes:       1,
		MaxBytes:       10e6, // 10MB
		CommitInterval: 0,    // Synchronous / explicit commits
	})
}
