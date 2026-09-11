package main

import (
	"os"
	"strconv"
	"strings"
)

// Config defines top-level configuration for the analysis service.
type Config struct {
	KafkaEnabled   bool
	KafkaBrokers   []string
	ConsumerGroup  string
	TelemetryTopic string
	HealthTopic    string
}

// DefaultConfig returns the default configuration for the Analysis Service.
func DefaultConfig() Config {
	kafkaEnabled := true
	if val := os.Getenv("KAFKA_ENABLED"); val != "" {
		if b, err := strconv.ParseBool(val); err == nil {
			kafkaEnabled = b
		}
	}

	kafkaBrokers := []string{"localhost:9092"}
	if val := os.Getenv("KAFKA_BROKERS"); val != "" {
		parts := strings.Split(val, ",")
		var cleaned []string
		for _, p := range parts {
			trimmed := strings.TrimSpace(p)
			if trimmed != "" {
				cleaned = append(cleaned, trimmed)
			}
		}
		if len(cleaned) > 0 {
			kafkaBrokers = cleaned
		}
	}

	consumerGroup := "analysis-service"
	if val := os.Getenv("KAFKA_CONSUMER_GROUP"); strings.TrimSpace(val) != "" {
		consumerGroup = strings.TrimSpace(val)
	}

	telemetryTopic := "network.telemetry"
	if val := os.Getenv("KAFKA_TELEMETRY_TOPIC"); strings.TrimSpace(val) != "" {
		telemetryTopic = strings.TrimSpace(val)
	}

	healthTopic := "network.health-events"
	if val := os.Getenv("KAFKA_HEALTH_TOPIC"); strings.TrimSpace(val) != "" {
		healthTopic = strings.TrimSpace(val)
	}

	return Config{
		KafkaEnabled:   kafkaEnabled,
		KafkaBrokers:   kafkaBrokers,
		ConsumerGroup:  consumerGroup,
		TelemetryTopic: telemetryTopic,
		HealthTopic:    healthTopic,
	}
}
