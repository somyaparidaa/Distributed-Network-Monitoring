package aggregation

import (
	"sync"
	"time"

	"distributed-network-monitor/services/analysis/model"
)

// Engine manages rolling aggregation windows per device.
type Engine struct {
	mu      sync.RWMutex
	windows map[string]time.Duration
	devices map[string]map[string]*SlidingWindow
}

// NewEngine constructs a new Aggregation Engine for the configured window durations.
func NewEngine(windows map[string]time.Duration) *Engine {
	if len(windows) == 0 {
		windows = map[string]time.Duration{
			"1m": 1 * time.Minute,
			"5m": 5 * time.Minute,
		}
	}
	return &Engine{
		windows: windows,
		devices: make(map[string]map[string]*SlidingWindow),
	}
}

// AddSample ingests a telemetry sample for the event's device, updates all configured windows,
// evicts expired samples using event.Timestamp, and returns the calculated RollingMetrics.
func (e *Engine) AddSample(event model.TelemetryEvent) []model.RollingMetrics {
	e.mu.Lock()
	defer e.mu.Unlock()

	deviceWindows, exists := e.devices[event.DeviceID]
	if !exists {
		deviceWindows = make(map[string]*SlidingWindow, len(e.windows))
		for name, dur := range e.windows {
			deviceWindows[name] = NewSlidingWindow(dur)
		}
		e.devices[event.DeviceID] = deviceWindows
	}

	results := make([]model.RollingMetrics, 0, len(e.windows))
	for name := range e.windows {
		w := deviceWindows[name]
		metrics := w.AddSample(event)
		metrics.Window = name
		results = append(results, metrics)
	}

	return results
}

// GetMetrics returns the current rolling metrics for a given device and window name.
func (e *Engine) GetMetrics(deviceID, windowName string) (model.RollingMetrics, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	deviceWindows, exists := e.devices[deviceID]
	if !exists {
		return model.RollingMetrics{}, false
	}

	w, exists := deviceWindows[windowName]
	if !exists {
		return model.RollingMetrics{}, false
	}

	metrics, ok := w.CurrentMetrics(deviceID, time.Now().UTC())
	if !ok {
		return model.RollingMetrics{}, false
	}
	metrics.Window = windowName
	return metrics, true
}
