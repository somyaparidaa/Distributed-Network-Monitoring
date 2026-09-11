package kafka

import (
	"context"
	"net"
	"testing"
	"time"
)

func TestLiveBrokerConnectivityWhenAvailable(t *testing.T) {
	conn, err := net.DialTimeout("tcp", "localhost:9092", 500*time.Millisecond)
	if err != nil {
		t.Skip("local Kafka broker not running at localhost:9092; skipping live broker integration test")
	}
	_ = conn.Close()

	cfg := Config{
		Enabled:        true,
		Brokers:        []string{"localhost:9092"},
		TelemetryTopic: "network.telemetry",
		HealthTopic:    "network.health-events",
	}

	producer, err := NewKafkaProducer(cfg, nil)
	if err != nil {
		t.Fatalf("failed to create live KafkaProducer: %v", err)
	}
	defer producer.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	telem := TelemetryEvent{
		EventID:      "live-verification-telem-1",
		DeviceID:     "router-01",
		Timestamp:    time.Now().UTC(),
		CPU:          22.0,
		Memory:       45.0,
		LatencyMS:    10,
		PacketLoss:   0.0,
		InterfaceUp:  true,
		Connectivity: true,
	}

	if err := producer.PublishTelemetry(ctx, telem); err != nil {
		t.Fatalf("failed to publish TelemetryEvent to localhost:9092: %v", err)
	}

	health := HealthEvent{
		EventID:       "live-verification-health-1",
		DeviceID:      "router-01",
		Timestamp:     time.Now().UTC(),
		CurrentStatus: "HEALTHY",
		Score:         0,
	}

	if err := producer.PublishHealth(ctx, health); err != nil {
		t.Fatalf("failed to publish HealthEvent to localhost:9092: %v", err)
	}

	t.Log("Successfully published both TelemetryEvent and HealthEvent over network to live Kafka broker at localhost:9092")
}
