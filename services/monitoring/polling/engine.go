package polling

import (
	"context"
	"log"
	"sync"
	"time"

	"distributed-network-monitor/services/monitoring/device"
)

// Poller defines the polling client capability.
type Poller interface {
	Poll(ctx context.Context, metricsURL string) (Telemetry, error)
}

// HealthEvaluator records evaluated health assessments for devices.
type HealthEvaluator interface {
	RecordPollSuccess(deviceID string, t Telemetry)
	RecordPollFailure(deviceID string, isTransportDown bool)
}

// TelemetryPublisher publishes telemetry events to external streaming systems (Kafka).
type TelemetryPublisher interface {
	PublishTelemetry(ctx context.Context, t Telemetry)
}

// EngineConfig aggregates tuning parameters for the polling engine.
type EngineConfig struct {
	PollInterval     time.Duration
	Retry            RetryConfig
	FailureThreshold int
}

// Engine coordinates concurrent background polling for all devices in the registry.
type Engine struct {
	registry     *device.Registry
	store        *Store
	stateTracker *StateTracker
	client       Poller
	healthEval   HealthEvaluator
	telemPub     TelemetryPublisher
	config       EngineConfig
	wg           sync.WaitGroup
}

// NewEngine constructs a new concurrent Polling Engine with retries and state tracking.
func NewEngine(
	registry *device.Registry,
	store *Store,
	stateTracker *StateTracker,
	client Poller,
	config EngineConfig,
) *Engine {
	if config.PollInterval <= 0 {
		config.PollInterval = 2 * time.Second
	}
	if config.FailureThreshold <= 0 {
		config.FailureThreshold = 3
	}
	return &Engine{
		registry:     registry,
		store:        store,
		stateTracker: stateTracker,
		client:       client,
		config:       config,
	}
}

// SetHealthEvaluator attaches a health evaluator to the engine.
func (e *Engine) SetHealthEvaluator(eval HealthEvaluator) {
	e.healthEval = eval
}

// SetTelemetryPublisher attaches an event publisher for telemetry.
func (e *Engine) SetTelemetryPublisher(pub TelemetryPublisher) {
	e.telemPub = pub
}

// Start launches a dedicated polling goroutine for each configured device.
func (e *Engine) Start(ctx context.Context) {
	devices := e.registry.List()
	for _, dev := range devices {
		e.wg.Add(1)
		go e.pollWorker(ctx, dev)
	}
}

// Wait blocks until all per-device worker goroutines have exited.
func (e *Engine) Wait() {
	e.wg.Wait()
}

func (e *Engine) pollWorker(ctx context.Context, dev device.MonitoredDevice) {
	defer e.wg.Done()

	// Perform an initial poll immediately upon startup
	e.pollOnce(ctx, dev)

	ticker := time.NewTicker(e.config.PollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			e.pollOnce(ctx, dev)
		}
	}
}

func (e *Engine) pollOnce(ctx context.Context, dev device.MonitoredDevice) {
	telemetry, err := ExecuteWithRetry(ctx, e.config.Retry, func() (Telemetry, error) {
		return e.client.Poll(ctx, dev.MetricsURL)
	})

	if err != nil {
		state, transitioned := e.stateTracker.RecordFailure(dev.ID, err, e.config.FailureThreshold)
		if transitioned {
			log.Printf("ALERT: device [%s] transitioned to DOWN after %d consecutive failures (last error: %v)",
				dev.ID, state.ConsecutiveFailures, err)
		} else if state.Status == StatusDown {
			log.Printf("device [%s] poll failed (device is DOWN): %v", dev.ID, err)
		} else {
			log.Printf("device [%s] poll failed (%d/%d consecutive): %v",
				dev.ID, state.ConsecutiveFailures, e.config.FailureThreshold, err)
		}

		if e.healthEval != nil {
			e.healthEval.RecordPollFailure(dev.ID, state.Status == StatusDown)
		}
		return
	}

	e.store.Set(dev.ID, telemetry)
	_, recovered := e.stateTracker.RecordSuccess(dev.ID)
	if recovered {
		log.Printf("RECOVERY: device [%s] recovered to UP", dev.ID)
	}

	if e.healthEval != nil {
		e.healthEval.RecordPollSuccess(dev.ID, telemetry)
	}

	if e.telemPub != nil {
		e.telemPub.PublishTelemetry(ctx, telemetry)
	}
}
