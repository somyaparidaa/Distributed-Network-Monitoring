package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"math/rand"
	"net/http"
	"strings"
	"sync"
	"time"
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

	s.advanceDegradation()

	s.telemetry.CPU = nextMetric(s.telemetry.CPU, normalCPU+s.degradation*0.5+s.randomRange(-2, 2), maximumCPUStep, 0, maximumPercentage)
	s.telemetry.Memory = nextMetric(s.telemetry.Memory, normalMemory+s.degradation*0.3+s.randomRange(-1.5, 1.5), maximumMemoryStep, 0, maximumPercentage)
	latency := nextMetric(float64(s.telemetry.LatencyMS), normalLatencyMS+s.degradation*3+s.randomRange(-3, 3), maximumLatencyStepMS, 0, math.MaxInt)
	s.telemetry.LatencyMS = int(math.Round(latency))
	s.telemetry.PacketLoss = nextMetric(s.telemetry.PacketLoss, normalPacketLoss+s.degradation*0.6+s.randomRange(-0.1, 0.4), maximumPacketLossStep, 0, maximumPercentage)
	s.telemetry.Condition = conditionForDegradation(s.degradation)
	s.telemetry.Timestamp = time.Now().UTC()
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

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(s.currentTelemetry()); err != nil {
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

func main() {
	fleet, err := NewDefaultFleet()
	if err != nil {
		log.Fatalf("create fleet: %v", err)
	}
	go fleet.Run(context.Background())

	primaryDevice, _ := fleet.Device("router-01")

	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", primaryDevice.metricsHandler)
	mux.HandleFunc("/metrics/", fleet.metricsHandler)

	server := &http.Server{Addr: ":8080", Handler: mux}
	log.Println("simulator for router-01 listening on :8080")
	if err := server.ListenAndServe(); err != nil {
		log.Fatalf("server failed: %v", err)
	}
}
