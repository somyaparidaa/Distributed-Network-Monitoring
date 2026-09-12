package store

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"distributed-network-monitor/services/analysis/model"
)

// ErrNotFound indicates that the requested device state key was not found in storage.
var ErrNotFound = errors.New("device state not found")

// DeviceStateRepository defines the contract for persisting and retrieving latest device state and rolling metrics.
type DeviceStateRepository interface {
	SaveLatestTelemetry(ctx context.Context, deviceID string, event model.TelemetryEvent) error
	SaveLatestHealth(ctx context.Context, deviceID string, event model.HealthEvent) error
	GetLatestTelemetry(ctx context.Context, deviceID string) (*model.TelemetryEvent, error)
	GetLatestHealth(ctx context.Context, deviceID string) (*model.HealthEvent, error)
	ListDevices(ctx context.Context) ([]string, error)

	SaveRollingMetrics(ctx context.Context, metrics model.RollingMetrics) error
	GetRollingMetrics(ctx context.Context, deviceID string, window string) (*model.RollingMetrics, error)

	Close() error
}

// MemoryRepository provides a thread-safe, in-memory implementation of DeviceStateRepository.
type MemoryRepository struct {
	mu        sync.RWMutex
	devices   map[string]struct{}
	telemetry map[string]model.TelemetryEvent
	health    map[string]model.HealthEvent
	metrics   map[string]model.RollingMetrics // key: deviceID + ":" + window
	closed    bool
}

// NewMemoryRepository constructs a new in-memory state repository.
func NewMemoryRepository() *MemoryRepository {
	return &MemoryRepository{
		devices:   make(map[string]struct{}),
		telemetry: make(map[string]model.TelemetryEvent),
		health:    make(map[string]model.HealthEvent),
		metrics:   make(map[string]model.RollingMetrics),
	}
}

// SaveLatestTelemetry stores the latest telemetry event and registers the device ID.
func (m *MemoryRepository) SaveLatestTelemetry(ctx context.Context, deviceID string, event model.TelemetryEvent) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return errors.New("repository is closed")
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	m.devices[deviceID] = struct{}{}
	m.telemetry[deviceID] = event
	return nil
}

// SaveLatestHealth stores the latest health event and registers the device ID.
func (m *MemoryRepository) SaveLatestHealth(ctx context.Context, deviceID string, event model.HealthEvent) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return errors.New("repository is closed")
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	m.devices[deviceID] = struct{}{}
	m.health[deviceID] = event
	return nil
}

// GetLatestTelemetry retrieves the latest telemetry event for a device.
func (m *MemoryRepository) GetLatestTelemetry(ctx context.Context, deviceID string) (*model.TelemetryEvent, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.closed {
		return nil, errors.New("repository is closed")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	event, exists := m.telemetry[deviceID]
	if !exists {
		return nil, ErrNotFound
	}
	return &event, nil
}

// GetLatestHealth retrieves the latest health event for a device.
func (m *MemoryRepository) GetLatestHealth(ctx context.Context, deviceID string) (*model.HealthEvent, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.closed {
		return nil, errors.New("repository is closed")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	event, exists := m.health[deviceID]
	if !exists {
		return nil, ErrNotFound
	}
	return &event, nil
}

// SaveRollingMetrics stores the latest rolling metrics snapshot for a device and window.
func (m *MemoryRepository) SaveRollingMetrics(ctx context.Context, metrics model.RollingMetrics) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return errors.New("repository is closed")
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	m.devices[metrics.DeviceID] = struct{}{}
	key := fmt.Sprintf("%s:%s", metrics.DeviceID, metrics.Window)
	m.metrics[key] = metrics
	return nil
}

// GetRollingMetrics retrieves the latest rolling metrics for a device and window.
func (m *MemoryRepository) GetRollingMetrics(ctx context.Context, deviceID string, window string) (*model.RollingMetrics, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.closed {
		return nil, errors.New("repository is closed")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	key := fmt.Sprintf("%s:%s", deviceID, window)
	metrics, exists := m.metrics[key]
	if !exists {
		return nil, ErrNotFound
	}
	return &metrics, nil
}

// ListDevices returns all known device IDs.
func (m *MemoryRepository) ListDevices(ctx context.Context) ([]string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.closed {
		return nil, errors.New("repository is closed")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	devices := make([]string, 0, len(m.devices))
	for id := range m.devices {
		devices = append(devices, id)
	}
	return devices, nil
}

// Close marks the in-memory repository closed.
func (m *MemoryRepository) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closed = true
	return nil
}
