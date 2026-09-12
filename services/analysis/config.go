package main

import (
	"os"
	"strconv"
	"strings"
	"time"
)

// Config defines top-level configuration for the analysis service.
type Config struct {
	KafkaEnabled   bool
	KafkaBrokers   []string
	ConsumerGroup  string
	TelemetryTopic string
	HealthTopic    string

	RedisEnabled   bool
	RedisAddr      string
	RedisDB        int
	RedisPassword  string
	RedisKeyPrefix string

	AggregationWindows map[string]time.Duration
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

	redisEnabled := true
	if val := os.Getenv("REDIS_ENABLED"); val != "" {
		if b, err := strconv.ParseBool(val); err == nil {
			redisEnabled = b
		}
	}

	redisAddr := "localhost:6379"
	if val := os.Getenv("REDIS_ADDR"); strings.TrimSpace(val) != "" {
		redisAddr = strings.TrimSpace(val)
	}

	redisDB := 0
	if val := os.Getenv("REDIS_DB"); strings.TrimSpace(val) != "" {
		if n, err := strconv.Atoi(strings.TrimSpace(val)); err == nil && n >= 0 {
			redisDB = n
		}
	}

	redisPassword := os.Getenv("REDIS_PASSWORD")

	redisKeyPrefix := "analysis"
	if val := os.Getenv("REDIS_KEY_PREFIX"); strings.TrimSpace(val) != "" {
		redisKeyPrefix = strings.TrimSpace(val)
	}

	aggregationWindows := map[string]time.Duration{
		"1m": 1 * time.Minute,
		"5m": 5 * time.Minute,
	}
	if val := os.Getenv("AGGREGATION_WINDOWS"); strings.TrimSpace(val) != "" {
		parsed := parseAggregationWindows(val)
		if len(parsed) > 0 {
			aggregationWindows = parsed
		}
	}

	return Config{
		KafkaEnabled:       kafkaEnabled,
		KafkaBrokers:       kafkaBrokers,
		ConsumerGroup:      consumerGroup,
		TelemetryTopic:     telemetryTopic,
		HealthTopic:        healthTopic,
		RedisEnabled:       redisEnabled,
		RedisAddr:          redisAddr,
		RedisDB:            redisDB,
		RedisPassword:      redisPassword,
		RedisKeyPrefix:     redisKeyPrefix,
		AggregationWindows: aggregationWindows,
	}
}

func parseAggregationWindows(s string) map[string]time.Duration {
	parts := strings.Split(s, ",")
	res := make(map[string]time.Duration)
	for _, p := range parts {
		name := strings.TrimSpace(p)
		if name == "" {
			continue
		}
		dur, err := time.ParseDuration(name)
		if err == nil && dur > 0 {
			res[name] = dur
		}
	}
	return res
}
