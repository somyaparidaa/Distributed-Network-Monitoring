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

// Engine coordinates concurrent background polling for all devices in the registry.
type Engine struct {
	registry *device.Registry
	store    *Store
	client   Poller
	interval time.Duration
	wg       sync.WaitGroup
}

// NewEngine constructs a new concurrent Polling Engine.
func NewEngine(registry *device.Registry, store *Store, client Poller, interval time.Duration) *Engine {
	if interval <= 0 {
		interval = 2 * time.Second
	}
	return &Engine{
		registry: registry,
		store:    store,
		client:   client,
		interval: interval,
	}
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

	ticker := time.NewTicker(e.interval)
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
	telemetry, err := e.client.Poll(ctx, dev.MetricsURL)
	if err != nil {
		log.Printf("device [%s] poll failed: %v", dev.ID, err)
		return
	}

	e.store.Set(dev.ID, telemetry)
}
