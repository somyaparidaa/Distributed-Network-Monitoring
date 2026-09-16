package main

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"distributed-network-monitor/services/monitoring/device"
)

// DeviceConfig defines external configuration for a monitored device.
type DeviceConfig struct {
	ID         string
	MetricsURL string
}

// Config defines top-level configuration for the monitoring service.
type Config struct {
	Devices          []DeviceConfig
	PollInterval     time.Duration
	PollTimeout      time.Duration
	MaxRetries       int
	RetryBackoff     time.Duration
	FailureThreshold int
	KafkaEnabled     bool
	KafkaBrokers     []string
	TelemetryTopic   string
	HealthTopic      string
	HTTPAddr         string
}

// DefaultConfig returns the default fleet monitoring configuration targeting router-01, router-02, router-03.
func DefaultConfig() Config {
	baseSimulatorURL := os.Getenv("SIMULATOR_URL")
	if baseSimulatorURL == "" {
		baseSimulatorURL = os.Getenv("SIMULATOR_BASE_URL")
	}
	if baseSimulatorURL == "" {
		baseSimulatorURL = "http://localhost:8080"
	}
	baseSimulatorURL = strings.TrimRight(baseSimulatorURL, "/")

	pollInterval := 2 * time.Second
	if val := os.Getenv("POLL_INTERVAL"); val != "" {
		if d, err := time.ParseDuration(val); err == nil && d > 0 {
			pollInterval = d
		}
	}

	pollTimeout := 1 * time.Second
	if val := os.Getenv("POLL_TIMEOUT"); val != "" {
		if d, err := time.ParseDuration(val); err == nil && d > 0 {
			pollTimeout = d
		}
	}

	maxRetries := 2
	if val := os.Getenv("MAX_RETRIES"); val != "" {
		if n, err := strconv.Atoi(val); err == nil && n >= 0 {
			maxRetries = n
		}
	}

	retryBackoff := 50 * time.Millisecond
	if val := os.Getenv("RETRY_BACKOFF"); val != "" {
		if d, err := time.ParseDuration(val); err == nil && d > 0 {
			retryBackoff = d
		}
	}

	failureThreshold := 3
	if val := os.Getenv("FAILURE_THRESHOLD"); val != "" {
		if n, err := strconv.Atoi(val); err == nil && n > 0 {
			failureThreshold = n
		}
	}

	kafkaEnabled := true
	if val := os.Getenv("KAFKA_ENABLED"); val != "" {
		if b, err := strconv.ParseBool(val); err == nil {
			kafkaEnabled = b
		}
	}

	kafkaBrokers := []string{"localhost:9092"}
	if val := os.Getenv("KAFKA_BROKERS"); val != "" {
		kafkaBrokers = strings.Split(val, ",")
	}

	telemetryTopic := "network.telemetry"
	if val := os.Getenv("KAFKA_TELEMETRY_TOPIC"); val != "" {
		telemetryTopic = val
	}

	healthTopic := "network.health-events"
	if val := os.Getenv("KAFKA_HEALTH_TOPIC"); val != "" {
		healthTopic = val
	}

	httpAddr := ":8081"
	if val := os.Getenv("MONITORING_HTTP_ADDR"); val != "" {
		httpAddr = val
	} else if val := os.Getenv("HTTP_ADDR"); val != "" {
		httpAddr = val
	}

	return Config{
		Devices: []DeviceConfig{
			{
				ID:         "router-01",
				MetricsURL: fmt.Sprintf("%s/metrics/router-01", baseSimulatorURL),
			},
			{
				ID:         "router-02",
				MetricsURL: fmt.Sprintf("%s/metrics/router-02", baseSimulatorURL),
			},
			{
				ID:         "router-03",
				MetricsURL: fmt.Sprintf("%s/metrics/router-03", baseSimulatorURL),
			},
		},
		PollInterval:     pollInterval,
		PollTimeout:      pollTimeout,
		MaxRetries:       maxRetries,
		RetryBackoff:     retryBackoff,
		FailureThreshold: failureThreshold,
		KafkaEnabled:     kafkaEnabled,
		KafkaBrokers:     kafkaBrokers,
		TelemetryTopic:   telemetryTopic,
		HealthTopic:      healthTopic,
		HTTPAddr:         httpAddr,
	}
}

// ToMonitoredDevices converts configuration into domain MonitoredDevice models.
func (c Config) ToMonitoredDevices() []device.MonitoredDevice {
	devices := make([]device.MonitoredDevice, 0, len(c.Devices))
	for _, d := range c.Devices {
		devices = append(devices, device.MonitoredDevice{
			ID:         d.ID,
			MetricsURL: d.MetricsURL,
		})
	}
	return devices
}
