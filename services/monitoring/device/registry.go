package device

import (
	"fmt"
	"net/url"
	"strings"
	"sync"
)

// MonitoredDevice represents a network device target managed by the monitoring service.
type MonitoredDevice struct {
	ID         string
	MetricsURL string
}

// Registry maintains the collection of configured network devices to monitor.
type Registry struct {
	mu      sync.RWMutex
	devices map[string]MonitoredDevice
	order   []string
}

// NewRegistry constructs a Registry from a slice of MonitoredDevice, validating ID uniqueness and URL formats.
func NewRegistry(devices []MonitoredDevice) (*Registry, error) {
	reg := &Registry{
		devices: make(map[string]MonitoredDevice, len(devices)),
		order:   make([]string, 0, len(devices)),
	}

	for _, d := range devices {
		id := strings.TrimSpace(d.ID)
		if id == "" {
			return nil, fmt.Errorf("device ID cannot be empty")
		}

		rawURL := strings.TrimSpace(d.MetricsURL)
		if rawURL == "" {
			return nil, fmt.Errorf("device %q metrics URL cannot be empty", id)
		}

		parsed, err := url.Parse(rawURL)
		if err != nil || parsed.Scheme == "" || parsed.Host == "" {
			return nil, fmt.Errorf("device %q has invalid metrics URL %q", id, rawURL)
		}

		if _, exists := reg.devices[id]; exists {
			return nil, fmt.Errorf("duplicate device ID %q", id)
		}

		device := MonitoredDevice{
			ID:         id,
			MetricsURL: rawURL,
		}
		reg.devices[id] = device
		reg.order = append(reg.order, id)
	}

	return reg, nil
}

// Get returns the MonitoredDevice for the given ID if present.
func (r *Registry) Get(id string) (MonitoredDevice, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	d, exists := r.devices[id]
	return d, exists
}

// List returns a slice of all registered devices in deterministic registration order.
func (r *Registry) List() []MonitoredDevice {
	r.mu.RLock()
	defer r.mu.RUnlock()

	result := make([]MonitoredDevice, 0, len(r.order))
	for _, id := range r.order {
		result = append(result, r.devices[id])
	}
	return result
}

// Len returns the count of registered devices.
func (r *Registry) Len() int {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return len(r.devices)
}
