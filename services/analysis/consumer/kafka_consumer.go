package consumer

import (
	"context"
	"errors"
	"io"
	"log"
	"sync"
	"time"
)

// ReaderFactory is a function that creates a MessageReader for a specific topic.
type ReaderFactory func(brokers []string, groupID string, topic string) MessageReader

// KafkaConsumer implements Consumer using segmentio/kafka-go readers.
// It subscribes to multiple topics, converts Kafka messages to internal Messages,
// processes them through the Dispatcher, commits offsets, and shuts down cleanly on context cancellation.
type KafkaConsumer struct {
	config        KafkaConsumerConfig
	dispatcher    *Dispatcher
	readerFactory ReaderFactory

	readers   []MessageReader
	readersMu sync.Mutex

	wg        sync.WaitGroup
	closeOnce sync.Once
	closed    bool
	closedMu  sync.RWMutex
}

// NewKafkaConsumer constructs a KafkaConsumer connected to the configured brokers and topics.
func NewKafkaConsumer(cfg KafkaConsumerConfig, dispatcher *Dispatcher, factory ReaderFactory) *KafkaConsumer {
	if factory == nil {
		factory = DefaultReaderFactory
	}
	return &KafkaConsumer{
		config:        cfg,
		dispatcher:    dispatcher,
		readerFactory: factory,
		readers:       make([]MessageReader, 0, len(cfg.Topics)),
	}
}

// Start spawns a consumption loop goroutine for each configured topic.
// It runs until ctx is cancelled or Close() is called.
func (kc *KafkaConsumer) Start(ctx context.Context) error {
	if kc.dispatcher == nil {
		return errors.New("cannot start KafkaConsumer: dispatcher is nil")
	}

	kc.closedMu.Lock()
	if kc.closed {
		kc.closedMu.Unlock()
		return errors.New("consumer already closed")
	}
	kc.closedMu.Unlock()

	// Initialize readers for each topic
	kc.readersMu.Lock()
	for _, topic := range kc.config.Topics {
		reader := kc.readerFactory(kc.config.Brokers, kc.config.GroupID, topic)
		kc.readers = append(kc.readers, reader)
	}
	readers := make([]MessageReader, len(kc.readers))
	copy(readers, kc.readers)
	kc.readersMu.Unlock()

	log.Printf("[KAFKA] started consumer group %q on topics %v across brokers %v",
		kc.config.GroupID, kc.config.Topics, kc.config.Brokers)

	// Launch a consumer goroutine per topic
	errCh := make(chan error, len(readers))
	for i, reader := range readers {
		topic := kc.config.Topics[i]
		kc.wg.Add(1)
		go func(r MessageReader, top string) {
			defer kc.wg.Done()
			if err := kc.consumeTopic(ctx, r, top); err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, io.EOF) {
				log.Printf("[KAFKA] consumer loop for topic %q exited with error: %v", top, err)
				select {
				case errCh <- err:
				default:
				}
			}
		}(reader, topic)
	}

	// Wait for context cancellation or fatal worker error
	select {
	case <-ctx.Done():
		// Context was cancelled, begin clean shutdown
	case err := <-errCh:
		log.Printf("[KAFKA] consumer group %q encountered worker failure: %v", kc.config.GroupID, err)
	}

	// Wait for all workers to complete
	kc.wg.Wait()
	return nil
}

// consumeTopic reads and processes messages from a single topic partition stream.
func (kc *KafkaConsumer) consumeTopic(ctx context.Context, reader MessageReader, topic string) error {
	for {
		if kc.isClosed() {
			return nil
		}

		kafkaMsg, err := reader.FetchMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, io.EOF) || kc.isClosed() {
				return nil
			}
			// Transient network / connection error when polling Kafka: log and backoff briefly
			log.Printf("[KAFKA] warning: fetch error on topic %q: %v", topic, err)
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(200 * time.Millisecond):
				continue
			}
		}

		// Convert to internal transport message
		internalMsg := Message{
			Topic: kafkaMsg.Topic,
			Key:   kafkaMsg.Key,
			Value: kafkaMsg.Value,
		}

		// Dispatch message safely (malformed JSON or validation errors are logged and discarded inside ProcessMessage)
		if err := kc.dispatcher.ProcessMessage(ctx, internalMsg); err != nil {
			log.Printf("[KAFKA] error processing message on topic %q: %v", topic, err)
		}

		// Commit message offset
		commitCtx, cancelCommit := context.WithTimeout(context.Background(), 3*time.Second)
		if err := reader.CommitMessages(commitCtx, kafkaMsg); err != nil {
			log.Printf("[KAFKA] warning: failed to commit offset for topic %q (offset %d): %v", topic, kafkaMsg.Offset, err)
		}
		cancelCommit()
	}
}

func (kc *KafkaConsumer) isClosed() bool {
	kc.closedMu.RLock()
	defer kc.closedMu.RUnlock()
	return kc.closed
}

// Close closes all topic readers and signals running worker loops to stop.
func (kc *KafkaConsumer) Close() error {
	var closeErr error
	kc.closeOnce.Do(func() {
		kc.closedMu.Lock()
		kc.closed = true
		kc.closedMu.Unlock()

		kc.readersMu.Lock()
		defer kc.readersMu.Unlock()

		for _, r := range kc.readers {
			if err := r.Close(); err != nil && !errors.Is(err, io.EOF) {
				log.Printf("[KAFKA] error closing reader: %v", err)
				if closeErr == nil {
					closeErr = err
				}
			}
		}
		kc.readers = nil
	})
	return closeErr
}
