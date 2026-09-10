package main

import (
	"fmt"
	"os"
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
	Devices      []DeviceConfig
	PollInterval time.Duration
	PollTimeout  time.Duration
}

// DefaultConfig returns the default fleet monitoring configuration targeting router-01, router-02, router-03.
func DefaultConfig() Config {
	baseSimulatorURL := os.Getenv("SIMULATOR_BASE_URL")
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
		PollInterval: pollInterval,
		PollTimeout:  pollTimeout,
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
