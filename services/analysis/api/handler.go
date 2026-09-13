package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	"distributed-network-monitor/services/analysis/model"
	"distributed-network-monitor/services/analysis/store"
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

// writeError writes a standard JSON error response.
func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, errorResponse{Error: message})
}

// ServiceHealth represents the operational health and dependency connectivity of the Analysis Service.
type ServiceHealth struct {
	Status       string            `json:"status"`
	Uptime       string            `json:"uptime"`
	Dependencies map[string]string `json:"dependencies"`
	Timestamp    time.Time         `json:"timestamp"`
}

// DeviceView represents the complete state snapshot for a device under GET /devices/{id}.
type DeviceView struct {
	DeviceID        string               `json:"device_id"`
	LatestTelemetry model.TelemetryEvent `json:"latest_telemetry"`
	LatestHealth    model.HealthEvent    `json:"latest_health"`
	RollingMetrics  model.RollingMetrics `json:"rolling_metrics"`
	Analysis        model.DeviceAnalysis `json:"analysis"`
}

// Handler provides HTTP query endpoints for the Analysis Service.
type Handler struct {
	repo              store.DeviceStateRepository
	kafkaEnabled      bool
	redisEnabled      bool
	configuredWindows []string
	startTime         time.Time
}

// NewHandler constructs an analysis API handler.
func NewHandler(
	repo store.DeviceStateRepository,
	kafkaEnabled bool,
	redisEnabled bool,
	aggregationWindows map[string]time.Duration,
) *Handler {
	windows := make([]string, 0, len(aggregationWindows))
	for w := range aggregationWindows {
		windows = append(windows, w)
	}
	if len(windows) == 0 {
		windows = []string{"1m", "5m"}
	}

	return &Handler{
		repo:              repo,
		kafkaEnabled:      kafkaEnabled,
		redisEnabled:      redisEnabled,
		configuredWindows: windows,
		startTime:         time.Now().UTC(),
	}
}

// Routes registers and returns the API router mux.
func (h *Handler) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", h.handleHealth)
	mux.HandleFunc("/devices", h.handleDevicesRoot)
	mux.HandleFunc("/devices/", h.handleDevicesSubtree)
	return mux
}

// handleHealth serves GET /health.
func (h *Handler) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	dependencies := make(map[string]string)

	if h.kafkaEnabled {
		dependencies["kafka"] = "ENABLED"
	} else {
		dependencies["kafka"] = "DISABLED"
	}

	if !h.redisEnabled {
		dependencies["redis"] = "DISABLED"
	} else {
		ctx, cancel := context.WithTimeout(r.Context(), 500*time.Millisecond)
		defer cancel()
		if err := h.repo.Ping(ctx); err != nil {
			dependencies["redis"] = "DISCONNECTED"
		} else {
			dependencies["redis"] = "CONNECTED"
		}
	}

	uptime := time.Since(h.startTime).Truncate(time.Second).String()
	writeJSON(w, http.StatusOK, ServiceHealth{
		Status:       "UP",
		Uptime:       uptime,
		Dependencies: dependencies,
		Timestamp:    time.Now().UTC(),
	})
}

// handleDevicesRoot serves GET /devices.
func (h *Handler) handleDevicesRoot(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	devices, err := h.repo.ListDevices(r.Context())
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, "storage unavailable")
		return
	}

	if devices == nil {
		devices = []string{}
	}
	writeJSON(w, http.StatusOK, devices)
}

// handleDevicesSubtree routes GET /devices/{id}, /devices/{id}/telemetry, /devices/{id}/health, /devices/{id}/analysis.
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

	switch len(parts) {
	case 1:
		h.handleDeviceComplete(w, r, deviceID)
	case 2:
		switch parts[1] {
		case "telemetry":
			h.handleDeviceTelemetry(w, r, deviceID)
		case "health":
			h.handleDeviceHealth(w, r, deviceID)
		case "analysis":
			h.handleDeviceAnalysis(w, r, deviceID)
		default:
			writeError(w, http.StatusNotFound, "endpoint not found")
		}
	default:
		writeError(w, http.StatusNotFound, "endpoint not found")
	}
}

// handleDeviceComplete serves GET /devices/{id}.
// Returns full composite snapshot: latest telemetry + latest health + 1m rolling metrics + analysis.
// If any element is missing or not yet ingested for this device, returns 404 Not Found.
// If the underlying storage fails (e.g. Redis unavailable), returns 503 Service Unavailable.
func (h *Handler) handleDeviceComplete(w http.ResponseWriter, r *http.Request, deviceID string) {
	ctx := r.Context()

	telem, err := h.repo.GetLatestTelemetry(ctx, deviceID)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			writeError(w, http.StatusNotFound, "device state incomplete: latest telemetry not found")
			return
		}
		writeError(w, http.StatusServiceUnavailable, "storage unavailable")
		return
	}

	health, err := h.repo.GetLatestHealth(ctx, deviceID)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			writeError(w, http.StatusNotFound, "device state incomplete: latest health not found")
			return
		}
		writeError(w, http.StatusServiceUnavailable, "storage unavailable")
		return
	}

	metrics, err := h.repo.GetRollingMetrics(ctx, deviceID, "1m")
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			writeError(w, http.StatusNotFound, "device state incomplete: 1m rolling metrics not found")
			return
		}
		writeError(w, http.StatusServiceUnavailable, "storage unavailable")
		return
	}

	analysis, err := h.repo.GetDeviceAnalysis(ctx, deviceID)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			writeError(w, http.StatusNotFound, "device state incomplete: device analysis not found")
			return
		}
		writeError(w, http.StatusServiceUnavailable, "storage unavailable")
		return
	}

	writeJSON(w, http.StatusOK, DeviceView{
		DeviceID:        deviceID,
		LatestTelemetry: *telem,
		LatestHealth:    *health,
		RollingMetrics:  *metrics,
		Analysis:        *analysis,
	})
}

// handleDeviceTelemetry serves GET /devices/{id}/telemetry[?window=X].
func (h *Handler) handleDeviceTelemetry(w http.ResponseWriter, r *http.Request, deviceID string) {
	ctx := r.Context()

	telem, err := h.repo.GetLatestTelemetry(ctx, deviceID)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			writeError(w, http.StatusNotFound, "telemetry not found for device")
			return
		}
		writeError(w, http.StatusServiceUnavailable, "storage unavailable")
		return
	}

	windowParam := strings.TrimSpace(r.URL.Query().Get("window"))

	if windowParam != "" {
		// Specific window requested
		isConfigured := false
		for _, cw := range h.configuredWindows {
			if cw == windowParam {
				isConfigured = true
				break
			}
		}
		if !isConfigured {
			writeError(w, http.StatusBadRequest, "unsupported or unconfigured window parameter")
			return
		}

		metrics, err := h.repo.GetRollingMetrics(ctx, deviceID, windowParam)
		if err != nil && !errors.Is(err, store.ErrNotFound) {
			writeError(w, http.StatusServiceUnavailable, "storage unavailable")
			return
		}

		response := struct {
			Latest  model.TelemetryEvent  `json:"latest"`
			Rolling *model.RollingMetrics `json:"rolling,omitempty"`
		}{
			Latest:  *telem,
			Rolling: metrics,
		}
		writeJSON(w, http.StatusOK, response)
		return
	}

	// No window specified: return all configured aggregates
	aggregates := make(map[string]*model.RollingMetrics)
	for _, cw := range h.configuredWindows {
		m, err := h.repo.GetRollingMetrics(ctx, deviceID, cw)
		if err == nil {
			aggregates[cw] = m
		}
	}

	response := struct {
		Latest     model.TelemetryEvent             `json:"latest"`
		Aggregates map[string]*model.RollingMetrics `json:"aggregates"`
	}{
		Latest:     *telem,
		Aggregates: aggregates,
	}
	writeJSON(w, http.StatusOK, response)
}

// handleDeviceHealth serves GET /devices/{id}/health.
func (h *Handler) handleDeviceHealth(w http.ResponseWriter, r *http.Request, deviceID string) {
	ctx := r.Context()

	health, err := h.repo.GetLatestHealth(ctx, deviceID)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			writeError(w, http.StatusNotFound, "health not found for device")
			return
		}
		writeError(w, http.StatusServiceUnavailable, "storage unavailable")
		return
	}

	writeJSON(w, http.StatusOK, health)
}

// handleDeviceAnalysis serves GET /devices/{id}/analysis.
func (h *Handler) handleDeviceAnalysis(w http.ResponseWriter, r *http.Request, deviceID string) {
	ctx := r.Context()

	analysis, err := h.repo.GetDeviceAnalysis(ctx, deviceID)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			writeError(w, http.StatusNotFound, "analysis not found for device")
			return
		}
		writeError(w, http.StatusServiceUnavailable, "storage unavailable")
		return
	}

	writeJSON(w, http.StatusOK, analysis)
}
