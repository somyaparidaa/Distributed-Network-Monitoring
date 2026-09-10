package polling

import (
	"sync"
	"time"
)

// Telemetry mirrors the telemetry data emitted by network device simulators.
type Telemetry struct {
	DeviceID     string    `json:"device_id"`
	Condition    string    `json:"condition"`
	CPU          float64   `json:"cpu"`
	Memory       float64   `json:"memory"`
	LatencyMS    int       `json:"latency_ms"`
	PacketLoss   float64   `json:"packet_loss"`
	InterfaceUp  bool      `json:"interface_up"`
	Connectivity bool      `json:"connectivity"`
	Timestamp    time.Time `json:"timestamp"`
}

// Store maintains thread-safe in-memory cache of the latest successful telemetry per device.
type Store struct {
	mu        sync.RWMutex
	telemetry map[string]Telemetry
}

// NewStore initializes an empty in-memory telemetry store.
func NewStore() *Store {
	return &Store{
		telemetry: make(map[string]Telemetry),
	}
}

// Set saves or overwrites the latest telemetry for a device.
func (s *Store) Set(deviceID string, t Telemetry) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.telemetry[deviceID] = t
}

// Get returns the latest stored telemetry for a device, if any.
func (s *Store) Get(deviceID string) (Telemetry, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	t, ok := s.telemetry[deviceID]
	return t, ok
}

// All returns a snapshot map of all currently stored telemetry.
func (s *Store) All() map[string]Telemetry {
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := make(map[string]Telemetry, len(s.telemetry))
	for k, v := range s.telemetry {
		result[k] = v
	}
	return result
}
