package health

import (
	"sync"
	"time"
)

// HealthStatus represents the monitoring-evaluated operational health of a device.
type HealthStatus string

const (
	StatusHealthy  HealthStatus = "HEALTHY"
	StatusWarning  HealthStatus = "WARNING"
	StatusCritical HealthStatus = "CRITICAL"
	StatusDown     HealthStatus = "DOWN"
)

// Assessment records the deterministic health evaluation for a device at a specific time.
type Assessment struct {
	DeviceID    string       `json:"device_id"`
	Status      HealthStatus `json:"status"`
	Score       int          `json:"score"`
	Reasons     []string     `json:"reasons,omitempty"`
	EvaluatedAt time.Time    `json:"evaluated_at"`
}

// Store maintains thread-safe storage for the latest health assessment per device.
type Store struct {
	mu          sync.RWMutex
	assessments map[string]Assessment
}

// NewStore initializes an empty in-memory health assessment store.
func NewStore() *Store {
	return &Store{
		assessments: make(map[string]Assessment),
	}
}

// Set stores or updates the latest health assessment for a device.
func (s *Store) Set(deviceID string, a Assessment) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.assessments[deviceID] = a
}

// Get retrieves the latest health assessment for a device.
func (s *Store) Get(deviceID string) (Assessment, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	a, ok := s.assessments[deviceID]
	return a, ok
}

// All returns a snapshot map of all current device assessments.
func (s *Store) All() map[string]Assessment {
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := make(map[string]Assessment, len(s.assessments))
	for k, v := range s.assessments {
		result[k] = v
	}
	return result
}
