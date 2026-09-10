package polling

import (
	"sync"
	"time"
)

// DeviceStatus represents whether a device is currently operational or has failed.
type DeviceStatus string

const (
	StatusUp   DeviceStatus = "UP"
	StatusDown DeviceStatus = "DOWN"
)

// DeviceState captures the health, failure count, and timestamps for one monitored device.
type DeviceState struct {
	DeviceID            string       `json:"device_id"`
	Status              DeviceStatus `json:"status"`
	ConsecutiveFailures int          `json:"consecutive_failures"`
	LastError           string       `json:"last_error,omitempty"`
	LastSuccess         time.Time    `json:"last_success,omitempty"`
	LastPolled          time.Time    `json:"last_polled"`
}

// StateTracker manages thread-safe tracking of device operational states across polling cycles.
type StateTracker struct {
	mu     sync.RWMutex
	states map[string]DeviceState
}

// NewStateTracker initializes an empty StateTracker.
func NewStateTracker() *StateTracker {
	return &StateTracker{
		states: make(map[string]DeviceState),
	}
}

// RecordSuccess records a successful poll, resetting consecutive failures and setting Status to UP.
// Returns the updated state and whether the device transitioned from DOWN to UP (recovered).
func (st *StateTracker) RecordSuccess(deviceID string) (DeviceState, bool) {
	st.mu.Lock()
	defer st.mu.Unlock()

	now := time.Now().UTC()
	prev, exists := st.states[deviceID]
	recovered := exists && prev.Status == StatusDown

	state := DeviceState{
		DeviceID:            deviceID,
		Status:              StatusUp,
		ConsecutiveFailures: 0,
		LastError:           "",
		LastSuccess:         now,
		LastPolled:          now,
	}
	st.states[deviceID] = state
	return state, recovered
}

// RecordFailure records a failed poll cycle, incrementing consecutive failures.
// If consecutive failures reach or exceed failureThreshold, Status transitions to DOWN.
// Returns the updated state and whether this failure caused a transition to DOWN.
func (st *StateTracker) RecordFailure(deviceID string, err error, failureThreshold int) (DeviceState, bool) {
	st.mu.Lock()
	defer st.mu.Unlock()

	if failureThreshold <= 0 {
		failureThreshold = 1
	}

	now := time.Now().UTC()
	prev, exists := st.states[deviceID]

	consecutive := 1
	lastSuccess := time.Time{}
	initialStatus := StatusUp
	if exists {
		consecutive = prev.ConsecutiveFailures + 1
		lastSuccess = prev.LastSuccess
		initialStatus = prev.Status
	}

	newStatus := initialStatus
	transitionedToDown := false
	if consecutive >= failureThreshold {
		newStatus = StatusDown
		consecutive = failureThreshold // Cap at threshold; do not keep incrementing once DOWN
		if initialStatus != StatusDown {
			transitionedToDown = true
		}
	}

	errMsg := ""
	if err != nil {
		errMsg = err.Error()
	}

	state := DeviceState{
		DeviceID:            deviceID,
		Status:              newStatus,
		ConsecutiveFailures: consecutive,
		LastError:           errMsg,
		LastSuccess:         lastSuccess,
		LastPolled:          now,
	}
	st.states[deviceID] = state
	return state, transitionedToDown
}

// Get returns the current DeviceState for a device if tracked.
func (st *StateTracker) Get(deviceID string) (DeviceState, bool) {
	st.mu.RLock()
	defer st.mu.RUnlock()

	state, ok := st.states[deviceID]
	return state, ok
}

// All returns a snapshot of all tracked device states.
func (st *StateTracker) All() map[string]DeviceState {
	st.mu.RLock()
	defer st.mu.RUnlock()

	result := make(map[string]DeviceState, len(st.states))
	for k, v := range st.states {
		result[k] = v
	}
	return result
}
