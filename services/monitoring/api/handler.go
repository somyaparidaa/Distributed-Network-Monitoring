package api

import (
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"distributed-network-monitor/services/monitoring/device"
	"distributed-network-monitor/services/monitoring/health"
	"distributed-network-monitor/services/monitoring/polling"
)

type errorResponse struct {
	Error string `json:"error"`
}

// writeJSON encodes data to w with application/json Content-Type.
func writeJSON(w http.ResponseWriter, status int, data any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(data)
}

// writeError writes a JSON error response.
func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, errorResponse{Error: message})
}

// DeviceSummary represents the combined monitoring view for one device.
type DeviceSummary struct {
	DeviceID            string               `json:"device_id"`
	MetricsURL          string               `json:"metrics_url"`
	Status              polling.DeviceStatus `json:"status"`
	ConsecutiveFailures int                  `json:"consecutive_failures"`
	LastError           string               `json:"last_error,omitempty"`
	LastSuccess         *time.Time           `json:"last_success,omitempty"`
	LastPolled          *time.Time           `json:"last_polled,omitempty"`
	Health              *health.Assessment   `json:"health,omitempty"`
	LatestTelemetry     *polling.Telemetry   `json:"latest_telemetry,omitempty"`
}

// ServiceHealth represents the operational health of the Monitoring Service itself.
type ServiceHealth struct {
	Status           string    `json:"status"`
	Uptime           string    `json:"uptime"`
	MonitoredDevices int       `json:"monitored_devices"`
	Timestamp        time.Time `json:"timestamp"`
}

// Handler manages HTTP endpoints for the Monitoring Service read API.
type Handler struct {
	registry     *device.Registry
	store        *polling.Store
	healthStore  *health.Store
	stateTracker *polling.StateTracker
	startTime    time.Time
}

// NewHandler constructs an API Handler.
func NewHandler(
	registry *device.Registry,
	store *polling.Store,
	healthStore *health.Store,
	stateTracker *polling.StateTracker,
) *Handler {
	return &Handler{
		registry:     registry,
		store:        store,
		healthStore:  healthStore,
		stateTracker: stateTracker,
		startTime:    time.Now().UTC(),
	}
}

// Routes constructs the http.ServeMux with all API routes.
func (h *Handler) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", h.handleServiceHealth)
	mux.HandleFunc("/devices", h.handleDevicesRoot)
	mux.HandleFunc("/devices/", h.handleDevicesSubtree)
	return mux
}

// handleServiceHealth returns the Monitoring Service's own operational health (not fleet health).
func (h *Handler) handleServiceHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	uptime := time.Since(h.startTime).Truncate(time.Second).String()
	writeJSON(w, http.StatusOK, ServiceHealth{
		Status:           "UP",
		Uptime:           uptime,
		MonitoredDevices: h.registry.Len(),
		Timestamp:        time.Now().UTC(),
	})
}

// handleDevicesRoot handles GET /devices.
func (h *Handler) handleDevicesRoot(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	devices := h.registry.List()
	summaries := make([]DeviceSummary, 0, len(devices))

	for _, d := range devices {
		summaries = append(summaries, h.buildDeviceSummary(d))
	}

	writeJSON(w, http.StatusOK, summaries)
}

// handleDevicesSubtree handles /devices/{id}, /devices/{id}/metrics, /devices/{id}/health.
func (h *Handler) handleDevicesSubtree(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	path := strings.TrimPrefix(r.URL.Path, "/devices/")
	parts := strings.Split(path, "/")

	if len(parts) == 0 || parts[0] == "" {
		writeError(w, http.StatusNotFound, "device id required")
		return
	}

	deviceID := parts[0]
	deviceMeta, exists := h.registry.Get(deviceID)
	if !exists {
		writeError(w, http.StatusNotFound, "device not found")
		return
	}

	switch len(parts) {
	case 1:
		// GET /devices/{id}
		writeJSON(w, http.StatusOK, h.buildDeviceSummary(deviceMeta))

	case 2:
		switch parts[1] {
		case "metrics":
			// GET /devices/{id}/metrics
			telem, ok := h.store.Get(deviceID)
			if !ok {
				writeError(w, http.StatusNotFound, "no telemetry available yet")
				return
			}
			writeJSON(w, http.StatusOK, telem)

		case "health":
			// GET /devices/{id}/health
			assessment, ok := h.healthStore.Get(deviceID)
			if !ok {
				writeError(w, http.StatusNotFound, "no health assessment available yet")
				return
			}
			writeJSON(w, http.StatusOK, assessment)

		default:
			writeError(w, http.StatusNotFound, "endpoint not found")
		}

	default:
		// Nested/unknown subpath
		writeError(w, http.StatusNotFound, "endpoint not found")
	}
}

func (h *Handler) buildDeviceSummary(d device.MonitoredDevice) DeviceSummary {
	summary := DeviceSummary{
		DeviceID:            d.ID,
		MetricsURL:          d.MetricsURL,
		Status:              polling.StatusUp,
		ConsecutiveFailures: 0,
	}

	if state, ok := h.stateTracker.Get(d.ID); ok {
		summary.Status = state.Status
		summary.ConsecutiveFailures = state.ConsecutiveFailures
		summary.LastError = state.LastError
		if !state.LastSuccess.IsZero() {
			t := state.LastSuccess
			summary.LastSuccess = &t
		}
		if !state.LastPolled.IsZero() {
			t := state.LastPolled
			summary.LastPolled = &t
		}
	}

	if telem, ok := h.store.Get(d.ID); ok {
		summary.LatestTelemetry = &telem
	}

	if assessment, ok := h.healthStore.Get(d.ID); ok {
		summary.Health = &assessment
	}

	return summary
}
