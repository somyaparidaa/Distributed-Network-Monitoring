package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const (
	maximumPercentage = 100.0
	degradedThreshold = 30.0

	normalCPU        = 35.0
	normalMemory     = 50.0
	normalLatencyMS  = 20.0
	normalPacketLoss = 0.1

	maximumCPUStep        = 3.0
	maximumMemoryStep     = 2.0
	maximumLatencyStepMS  = 8.0
	maximumPacketLossStep = 0.5
)

type DeviceCondition string

const (
	ConditionNormal   DeviceCondition = "NORMAL"
	ConditionDegraded DeviceCondition = "DEGRADED"
	ConditionDown     DeviceCondition = "DOWN"
)

// Telemetry is a point-in-time snapshot of simulated router health.
type Telemetry struct {
	DeviceID     string          `json:"device_id"`
	Condition    DeviceCondition `json:"condition"`
	CPU          float64         `json:"cpu"`
	Memory       float64         `json:"memory"`
	LatencyMS    int             `json:"latency_ms"`
	PacketLoss   float64         `json:"packet_loss"`
	InterfaceUp  bool            `json:"interface_up"`
	Connectivity bool            `json:"connectivity"`
	Timestamp    time.Time       `json:"timestamp"`
}

// SimulationConfig controls the repeatability and timing of a simulator.
type SimulationConfig struct {
	UpdateInterval         time.Duration
	Seed                   int64
	DegradationProbability float64
}

// DeviceConfig identifies one independently simulated device.
type DeviceConfig struct {
	DeviceID   string
	Simulation SimulationConfig
}

func DefaultSimulationConfig() SimulationConfig {
	return SimulationConfig{
		UpdateInterval:         time.Second,
		Seed:                   time.Now().UnixNano(),
		DegradationProbability: 0.08,
	}
}

// Simulator owns the mutable telemetry state for one network device.
type Simulator struct {
	mu          sync.RWMutex
	telemetry   Telemetry
	config      SimulationConfig
	rng         *rand.Rand
	degradation float64
	down        bool
}

func NewSimulator(deviceID string, config SimulationConfig) *Simulator {
	if config.UpdateInterval <= 0 {
		config.UpdateInterval = time.Second
	}
	config.DegradationProbability = clamp(config.DegradationProbability, 0, 1)

	return &Simulator{
		telemetry: Telemetry{
			DeviceID:     deviceID,
			Condition:    ConditionNormal,
			CPU:          normalCPU,
			Memory:       normalMemory,
			LatencyMS:    int(normalLatencyMS),
			PacketLoss:   normalPacketLoss,
			InterfaceUp:  true,
			Connectivity: true,
			Timestamp:    time.Now().UTC(),
		},
		config: config,
		rng:    rand.New(rand.NewSource(config.Seed)),
	}
}

// Fleet manages independent device simulators.
type Fleet struct {
	devices map[string]*Simulator
}

func NewFleet(configs []DeviceConfig) (*Fleet, error) {
	fleet := &Fleet{devices: make(map[string]*Simulator, len(configs))}
	for _, config := range configs {
		if config.DeviceID == "" {
			return nil, fmt.Errorf("device ID cannot be empty")
		}
		if _, exists := fleet.devices[config.DeviceID]; exists {
			return nil, fmt.Errorf("duplicate device ID %q", config.DeviceID)
		}
		fleet.devices[config.DeviceID] = NewSimulator(config.DeviceID, config.Simulation)
	}
	return fleet, nil
}

func NewDefaultFleet() (*Fleet, error) {
	return NewFleet([]DeviceConfig{
		{DeviceID: "router-01", Simulation: SimulationConfig{UpdateInterval: time.Second, Seed: 101, DegradationProbability: 0.08}},
		{DeviceID: "router-02", Simulation: SimulationConfig{UpdateInterval: time.Second, Seed: 202, DegradationProbability: 0.08}},
		{DeviceID: "router-03", Simulation: SimulationConfig{UpdateInterval: time.Second, Seed: 303, DegradationProbability: 0.08}},
	})
}

func (f *Fleet) Device(deviceID string) (*Simulator, bool) {
	device, exists := f.devices[deviceID]
	return device, exists
}

func (f *Fleet) Len() int {
	return len(f.devices)
}

// Run starts an update loop for every device in the fleet.
func (f *Fleet) Run(ctx context.Context) {
	for _, device := range f.devices {
		go device.Run(ctx)
	}
}

// Run updates the device state until the supplied context is cancelled.
func (s *Simulator) Run(ctx context.Context) {
	ticker := time.NewTicker(s.config.UpdateInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.update()
		}
	}
}

func (s *Simulator) update() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.down {
		return
	}

	s.advanceDegradation()

	s.telemetry.CPU = nextMetric(s.telemetry.CPU, normalCPU+s.degradation*0.5+s.randomRange(-2, 2), maximumCPUStep, 0, maximumPercentage)
	s.telemetry.Memory = nextMetric(s.telemetry.Memory, normalMemory+s.degradation*0.3+s.randomRange(-1.5, 1.5), maximumMemoryStep, 0, maximumPercentage)
	latency := nextMetric(float64(s.telemetry.LatencyMS), normalLatencyMS+s.degradation*3+s.randomRange(-3, 3), maximumLatencyStepMS, 0, math.MaxInt)
	s.telemetry.LatencyMS = int(math.Round(latency))
	s.telemetry.PacketLoss = nextMetric(s.telemetry.PacketLoss, normalPacketLoss+s.degradation*0.6+s.randomRange(-0.1, 0.4), maximumPacketLossStep, 0, maximumPercentage)
	s.telemetry.Condition = conditionForDegradation(s.degradation)
	s.telemetry.Timestamp = time.Now().UTC()
}

// SetDown injects or clears a device failure without stopping its update loop.
func (s *Simulator) SetDown(down bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.down = down
	if down {
		s.telemetry.Condition = ConditionDown
		s.telemetry.InterfaceUp = false
		s.telemetry.Connectivity = false
	} else {
		s.telemetry.Condition = conditionForDegradation(s.degradation)
		s.telemetry.InterfaceUp = true
		s.telemetry.Connectivity = true
	}
	s.telemetry.Timestamp = time.Now().UTC()
}

func (s *Simulator) IsDown() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.down
}

func conditionForDegradation(degradation float64) DeviceCondition {
	if degradation >= degradedThreshold {
		return ConditionDegraded
	}
	return ConditionNormal
}

func (s *Simulator) advanceDegradation() {
	if s.rng.Float64() < s.config.DegradationProbability {
		s.degradation = clamp(s.degradation+s.randomRange(8, 15), 0, maximumPercentage)
		return
	}

	s.degradation = clamp(s.degradation-s.randomRange(1, 3), 0, maximumPercentage)
}

func (s *Simulator) randomRange(min, max float64) float64 {
	return min + s.rng.Float64()*(max-min)
}

func (s *Simulator) currentTelemetry() Telemetry {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.telemetry
}

func (s *Simulator) metricsHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	telemetry := s.currentTelemetry()
	if telemetry.Condition == ConditionDown {
		http.Error(w, "device is down", http.StatusServiceUnavailable)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(telemetry); err != nil {
		log.Printf("encode telemetry response: %v", err)
	}
}

func (f *Fleet) metricsHandler(w http.ResponseWriter, r *http.Request) {
	deviceID := strings.TrimPrefix(r.URL.Path, "/metrics/")
	if deviceID == "" || strings.Contains(deviceID, "/") {
		http.NotFound(w, r)
		return
	}

	device, exists := f.Device(deviceID)
	if !exists {
		http.NotFound(w, r)
		return
	}
	device.metricsHandler(w, r)
}

var (
	simReg = prometheus.NewRegistry()

	simHTTPRequestsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "simulator_http_requests_total",
			Help: "Total number of HTTP requests processed by the simulator.",
		},
		[]string{"endpoint", "method", "status"},
	)

	simHTTPRequestDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "simulator_http_request_duration_seconds",
			Help:    "Duration of HTTP requests processed by the simulator in seconds.",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"endpoint"},
	)

	simDevicesTotal = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "simulator_devices_total",
			Help: "Current count of simulated devices by operating condition.",
		},
		[]string{"condition"},
	)

	simFailureInjectionsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "simulator_failure_injections_total",
			Help: "Total number of failure injection actions performed.",
		},
		[]string{"action"},
	)
)

func init() {
	simReg.MustRegister(
		simHTTPRequestsTotal,
		simHTTPRequestDuration,
		simDevicesTotal,
		simFailureInjectionsTotal,
	)
}

type statusLoggingResponseWriter struct {
	http.ResponseWriter
	statusCode int
}

func (w *statusLoggingResponseWriter) WriteHeader(code int) {
	w.statusCode = code
	w.ResponseWriter.WriteHeader(code)
}

func normalizeSimulatorEndpoint(path string) string {
	if path == "/health" {
		return "/health"
	}
	if path == "/metrics" {
		return "/metrics"
	}
	if strings.HasPrefix(path, "/metrics/") {
		return "/metrics/{deviceID}"
	}
	if strings.HasPrefix(path, "/control/") {
		return "/control/{deviceID}/{action}"
	}
	return "other"
}

func updateFleetConditionGauges(f *Fleet) {
	normalCount := 0.0
	degradedCount := 0.0
	downCount := 0.0

	for _, d := range f.devices {
		t := d.currentTelemetry()
		switch t.Condition {
		case ConditionNormal:
			normalCount++
		case ConditionDegraded:
			degradedCount++
		case ConditionDown:
			downCount++
		}
	}

	simDevicesTotal.WithLabelValues("NORMAL").Set(normalCount)
	simDevicesTotal.WithLabelValues("DEGRADED").Set(degradedCount)
	simDevicesTotal.WithLabelValues("DOWN").Set(downCount)
}

func (f *Fleet) controlHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	path := strings.TrimPrefix(r.URL.Path, "/control/")
	parts := strings.Split(path, "/")
	if len(parts) != 2 || parts[0] == "" {
		http.NotFound(w, r)
		return
	}

	device, exists := f.Device(parts[0])
	if !exists {
		http.NotFound(w, r)
		return
	}

	switch parts[1] {
	case "down":
		device.SetDown(true)
		simFailureInjectionsTotal.WithLabelValues("down").Inc()
		updateFleetConditionGauges(f)
	case "recover":
		device.SetDown(false)
		simFailureInjectionsTotal.WithLabelValues("recover").Inc()
		updateFleetConditionGauges(f)
	default:
		http.NotFound(w, r)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func nextMetric(current, target, maximumStep, min, max float64) float64 {
	change := clamp(target-current, -maximumStep, maximumStep)
	return clamp(current+change, min, max)
}

func clamp(value, min, max float64) float64 {
	if value < min {
		return min
	}
	if value > max {
		return max
	}
	return value
}

// NewMux constructs an http.Handler with all metrics and control routes for the fleet.
func NewMux(fleet *Fleet) (http.Handler, error) {
	_, exists := fleet.Device("router-01")
	if !exists {
		return nil, fmt.Errorf("primary device router-01 not found in fleet")
	}

	updateFleetConditionGauges(fleet)

	mux := http.NewServeMux()
	liveHandler := func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", http.MethodGet)
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"UP"}`))
	}
	mux.HandleFunc("/health", liveHandler)
	mux.HandleFunc("/health/live", liveHandler)
	mux.HandleFunc("/health/ready", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", http.MethodGet)
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if _, exists := fleet.Device("router-01"); !exists {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(`{"status":"NOT_READY","error":"primary device router-01 not initialized"}`))
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"READY","fleet_size":3}`))
	})
	promHandler := promhttp.HandlerFor(simReg, promhttp.HandlerOpts{})
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/metrics" {
			if r.Method != http.MethodGet {
				w.Header().Set("Allow", http.MethodGet)
				w.WriteHeader(http.StatusMethodNotAllowed)
				return
			}
			updateFleetConditionGauges(fleet)
			promHandler.ServeHTTP(w, r)
			return
		}
		fleet.metricsHandler(w, r)
	})
	mux.HandleFunc("/metrics/", fleet.metricsHandler)
	mux.HandleFunc("/control/", fleet.controlHandler)

	wrapped := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		sw := &statusLoggingResponseWriter{ResponseWriter: w, statusCode: http.StatusOK}
		mux.ServeHTTP(sw, r)
		duration := time.Since(start).Seconds()

		normPath := normalizeSimulatorEndpoint(r.URL.Path)
		simHTTPRequestsTotal.WithLabelValues(normPath, r.Method, fmt.Sprintf("%d", sw.statusCode)).Inc()
		simHTTPRequestDuration.WithLabelValues(normPath).Observe(duration)
	})

	return wrapped, nil
}

// Run starts the fleet simulation and HTTP server, shutting down gracefully on ctx cancellation.
func Run(ctx context.Context, addr string, fleet *Fleet) error {
	handler, err := NewMux(fleet)
	if err != nil {
		return fmt.Errorf("initialize mux: %w", err)
	}

	fleet.Run(ctx)

	server := &http.Server{
		Addr:    addr,
		Handler: handler,
	}

	serverErr := make(chan error, 1)
	go func() {
		log.Printf("simulator listening on %s", addr)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			serverErr <- err
		}
		close(serverErr)
	}()

	select {
	case err := <-serverErr:
		return fmt.Errorf("server startup failed: %w", err)
	case <-ctx.Done():
		log.Println("shutdown signal received; terminating simulator server...")
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("graceful server shutdown failed: %w", err)
	}

	<-serverErr
	log.Println("simulator server stopped gracefully")
	return nil
}

func main() {
	addr := os.Getenv("SIMULATOR_ADDR")
	if addr == "" {
		addr = os.Getenv("SIMULATOR_HTTP_ADDR")
	}
	if addr == "" {
		addr = os.Getenv("HTTP_ADDR")
	}
	if addr == "" {
		addr = ":8080"
	}

	fleet, err := NewDefaultFleet()
	if err != nil {
		log.Fatalf("create fleet: %v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	log.Println("starting network device simulator fleet (router-01, router-02, router-03)...")
	if err := Run(ctx, addr, fleet); err != nil {
		log.Fatalf("simulator error: %v", err)
	}
	log.Println("simulator process shutdown complete")
}
