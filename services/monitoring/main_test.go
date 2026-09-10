package main

import (
	"context"
	"testing"
	"time"

	"distributed-network-monitor/services/monitoring/device"
)

func TestDefaultConfigHasExpectedDevices(t *testing.T) {
	cfg := DefaultConfig()
	if len(cfg.Devices) != 3 {
		t.Fatalf("expected 3 default devices, got %d", len(cfg.Devices))
	}

	expected := map[string]string{
		"router-01": "http://localhost:8080/metrics/router-01",
		"router-02": "http://localhost:8080/metrics/router-02",
		"router-03": "http://localhost:8080/metrics/router-03",
	}

	for _, d := range cfg.Devices {
		expectedURL, exists := expected[d.ID]
		if !exists {
			t.Errorf("unexpected device ID %q", d.ID)
			continue
		}
		if d.MetricsURL != expectedURL {
			t.Errorf("device %s URL = %q, want %q", d.ID, d.MetricsURL, expectedURL)
		}
	}
}

func TestRegistryValidDevices(t *testing.T) {
	devices := []device.MonitoredDevice{
		{ID: "router-01", MetricsURL: "http://localhost:8080/metrics/router-01"},
		{ID: "router-02", MetricsURL: "http://localhost:8080/metrics/router-02"},
	}

	reg, err := device.NewRegistry(devices)
	if err != nil {
		t.Fatalf("unexpected error creating registry: %v", err)
	}

	if reg.Len() != 2 {
		t.Fatalf("registry length = %d, want 2", reg.Len())
	}

	d1, ok := reg.Get("router-01")
	if !ok || d1.ID != "router-01" || d1.MetricsURL != "http://localhost:8080/metrics/router-01" {
		t.Fatalf("unexpected device router-01: %+v", d1)
	}

	_, ok = reg.Get("router-99")
	if ok {
		t.Fatal("expected router-99 not to be present")
	}

	list := reg.List()
	if len(list) != 2 || list[0].ID != "router-01" || list[1].ID != "router-02" {
		t.Fatalf("unexpected ordered list: %+v", list)
	}
}

func TestRegistryValidationErrors(t *testing.T) {
	tests := []struct {
		name    string
		devices []device.MonitoredDevice
	}{
		{
			name: "empty device ID",
			devices: []device.MonitoredDevice{
				{ID: "", MetricsURL: "http://localhost:8080/metrics/router-01"},
			},
		},
		{
			name: "whitespace device ID",
			devices: []device.MonitoredDevice{
				{ID: "   ", MetricsURL: "http://localhost:8080/metrics/router-01"},
			},
		},
		{
			name: "empty URL",
			devices: []device.MonitoredDevice{
				{ID: "router-01", MetricsURL: ""},
			},
		},
		{
			name: "invalid URL without host",
			devices: []device.MonitoredDevice{
				{ID: "router-01", MetricsURL: "not-a-valid-url"},
			},
		},
		{
			name: "duplicate device ID",
			devices: []device.MonitoredDevice{
				{ID: "router-01", MetricsURL: "http://localhost:8080/metrics/router-01"},
				{ID: "router-01", MetricsURL: "http://localhost:8080/metrics/other"},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := device.NewRegistry(tc.devices)
			if err == nil {
				t.Fatalf("expected error for case %q, got nil", tc.name)
			}
		})
	}
}

func TestNewServiceAndRunLifecycle(t *testing.T) {
	cfg := DefaultConfig()
	cfg.HTTPAddr = "127.0.0.1:0" // Ephemeral port for test
	service, err := NewService(cfg)
	if err != nil {
		t.Fatalf("failed to create service: %v", err)
	}

	if service.Registry().Len() != 3 {
		t.Fatalf("expected 3 devices in service registry, got %d", service.Registry().Len())
	}
	if service.HealthStore() == nil {
		t.Fatal("expected non-nil HealthStore in service")
	}

	ctx, cancel := context.WithCancel(context.Background())

	errCh := make(chan error, 1)
	go func() {
		errCh <- service.Run(ctx)
	}()

	// Verify it runs and shuts down cleanly upon cancel
	time.Sleep(20 * time.Millisecond)
	cancel()

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("service.Run returned error on cancellation: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("service.Run did not exit within timeout on cancellation")
	}
}
