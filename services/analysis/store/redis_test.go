package store

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"distributed-network-monitor/services/analysis/model"
)

func TestMemoryRepositoryOperations(t *testing.T) {
	repo := NewMemoryRepository()
	defer repo.Close()

	ctx := context.Background()
	now := time.Now().UTC()

	// 1. Initially empty
	devices, err := repo.ListDevices(ctx)
	if err != nil {
		t.Fatalf("unexpected error listing devices: %v", err)
	}
	if len(devices) != 0 {
		t.Fatalf("expected 0 devices, got %d", len(devices))
	}

	// 2. Querying unknown device returns ErrNotFound
	_, err = repo.GetLatestTelemetry(ctx, "router-01")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound for missing telemetry, got: %v", err)
	}

	_, err = repo.GetLatestHealth(ctx, "router-01")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound for missing health, got: %v", err)
	}

	// 3. Save telemetry
	telem := model.TelemetryEvent{
		EventID:      "evt-t1",
		DeviceID:     "router-01",
		Timestamp:    now,
		CPU:          35.5,
		Memory:       55.0,
		LatencyMS:    22,
		PacketLoss:   0.0,
		InterfaceUp:  true,
		Connectivity: true,
	}
	if err := repo.SaveLatestTelemetry(ctx, "router-01", telem); err != nil {
		t.Fatalf("failed to save telemetry: %v", err)
	}

	// Device set should now have router-01
	devices, err = repo.ListDevices(ctx)
	if err != nil || len(devices) != 1 || devices[0] != "router-01" {
		t.Fatalf("expected devices ['router-01'], got %v", devices)
	}

	fetchedTelem, err := repo.GetLatestTelemetry(ctx, "router-01")
	if err != nil {
		t.Fatalf("failed to get telemetry: %v", err)
	}
	if fetchedTelem.EventID != "evt-t1" || fetchedTelem.CPU != 35.5 {
		t.Fatalf("fetched telemetry mismatch: %+v", fetchedTelem)
	}

	// 4. Save health
	health := model.HealthEvent{
		EventID:        "evt-h1",
		DeviceID:       "router-02",
		Timestamp:      now,
		PreviousStatus: "HEALTHY",
		CurrentStatus:  "WARNING",
		Score:          20,
		Reasons:        []string{"high latency"},
	}
	if err := repo.SaveLatestHealth(ctx, "router-02", health); err != nil {
		t.Fatalf("failed to save health: %v", err)
	}

	devices, err = repo.ListDevices(ctx)
	if err != nil || len(devices) != 2 {
		t.Fatalf("expected 2 devices, got %v", devices)
	}

	fetchedHealth, err := repo.GetLatestHealth(ctx, "router-02")
	if err != nil {
		t.Fatalf("failed to get health: %v", err)
	}
	if fetchedHealth.EventID != "evt-h1" || fetchedHealth.CurrentStatus != "WARNING" {
		t.Fatalf("fetched health mismatch: %+v", fetchedHealth)
	}

	// 5. Test close
	if err := repo.Close(); err != nil {
		t.Fatalf("unexpected error closing repo: %v", err)
	}

	if err := repo.SaveLatestTelemetry(ctx, "router-01", telem); err == nil {
		t.Fatal("expected error saving telemetry after close, got nil")
	}
}

func TestLiveRedisWhenAvailable(t *testing.T) {
	conn, err := net.DialTimeout("tcp", "localhost:6379", 200*time.Millisecond)
	if err != nil {
		t.Skip("Redis not available on localhost:6379, skipping live Redis integration test")
	}
	_ = conn.Close()

	cfg := RedisConfig{
		Addr:      "localhost:6379",
		DB:        15, // Isolated test database
		KeyPrefix: "test_analysis",
	}

	repo, err := NewRedisRepository(cfg)
	if err != nil {
		t.Fatalf("failed to initialize live RedisRepository: %v", err)
	}
	defer repo.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// Verify ErrNotFound on empty key
	_, err = repo.GetLatestTelemetry(ctx, "nonexistent-router")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound for missing key, got %v", err)
	}

	now := time.Now().UTC().Truncate(time.Millisecond)
	telem := model.TelemetryEvent{
		EventID:      "live-telem-1",
		DeviceID:     "router-live-01",
		Timestamp:    now,
		CPU:          40.2,
		Memory:       60.1,
		LatencyMS:    18,
		PacketLoss:   0.05,
		InterfaceUp:  true,
		Connectivity: true,
	}

	if err := repo.SaveLatestTelemetry(ctx, "router-live-01", telem); err != nil {
		t.Fatalf("failed to save telemetry to live Redis: %v", err)
	}

	fetched, err := repo.GetLatestTelemetry(ctx, "router-live-01")
	if err != nil {
		t.Fatalf("failed to get telemetry from live Redis: %v", err)
	}
	if fetched.EventID != "live-telem-1" || fetched.CPU != 40.2 {
		t.Fatalf("telemetry mismatch from live Redis: %+v", fetched)
	}

	devices, err := repo.ListDevices(ctx)
	if err != nil {
		t.Fatalf("failed to list devices from live Redis: %v", err)
	}
	found := false
	for _, d := range devices {
		if d == "router-live-01" {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected 'router-live-01' in devices set %v", devices)
	}
}

func TestMemoryRepositoryRollingMetrics(t *testing.T) {
	repo := NewMemoryRepository()
	defer repo.Close()

	ctx := context.Background()
	now := time.Now().UTC()

	// Missing metrics returns ErrNotFound
	_, err := repo.GetRollingMetrics(ctx, "router-01", "1m")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound for missing metrics, got: %v", err)
	}

	metrics := model.RollingMetrics{
		DeviceID:         "router-01",
		Window:           "1m",
		SampleCount:      10,
		AvgLatencyMS:     25.5,
		MinLatencyMS:     15,
		MaxLatencyMS:     40,
		AvgPacketLoss:    0.2,
		MaxPacketLoss:    1.0,
		AvgCPU:           50.0,
		MaxCPU:           75.0,
		AvgMemory:        60.0,
		MaxMemory:        70.0,
		FirstSampleTime:  now.Add(-1 * time.Minute),
		LatestSampleTime: now,
		CalculatedAt:     now,
	}

	if err := repo.SaveRollingMetrics(ctx, metrics); err != nil {
		t.Fatalf("failed to save rolling metrics: %v", err)
	}

	fetched, err := repo.GetRollingMetrics(ctx, "router-01", "1m")
	if err != nil {
		t.Fatalf("failed to get rolling metrics: %v", err)
	}
	if fetched.SampleCount != 10 || fetched.AvgCPU != 50.0 || fetched.MaxLatencyMS != 40 {
		t.Fatalf("metrics mismatch: %+v", fetched)
	}

	// Device set should include router-01
	devices, err := repo.ListDevices(ctx)
	if err != nil || len(devices) != 1 || devices[0] != "router-01" {
		t.Fatalf("expected devices ['router-01'], got %v", devices)
	}
}

func TestLiveRedisRollingMetrics(t *testing.T) {
	conn, err := net.DialTimeout("tcp", "localhost:6379", 200*time.Millisecond)
	if err != nil {
		t.Skip("Redis not available on localhost:6379, skipping live Redis integration test")
	}
	_ = conn.Close()

	cfg := RedisConfig{
		Addr:      "localhost:6379",
		DB:        15,
		KeyPrefix: "test_analysis_metrics",
	}

	repo, err := NewRedisRepository(cfg)
	if err != nil {
		t.Fatalf("failed to initialize live RedisRepository: %v", err)
	}
	defer repo.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// Missing metrics returns ErrNotFound
	_, err = repo.GetRollingMetrics(ctx, "nonexistent", "1m")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound for missing metrics in live Redis, got: %v", err)
	}

	now := time.Now().UTC().Truncate(time.Millisecond)
	metrics := model.RollingMetrics{
		DeviceID:         "router-metrics-01",
		Window:           "1m",
		SampleCount:      5,
		AvgLatencyMS:     32.1,
		MinLatencyMS:     20,
		MaxLatencyMS:     50,
		AvgPacketLoss:    0.1,
		MaxPacketLoss:    0.5,
		AvgCPU:           42.0,
		MaxCPU:           55.0,
		AvgMemory:        58.0,
		MaxMemory:        65.0,
		FirstSampleTime:  now.Add(-40 * time.Second),
		LatestSampleTime: now,
		CalculatedAt:     now,
	}

	if err := repo.SaveRollingMetrics(ctx, metrics); err != nil {
		t.Fatalf("failed to save rolling metrics to live Redis: %v", err)
	}

	fetched, err := repo.GetRollingMetrics(ctx, "router-metrics-01", "1m")
	if err != nil {
		t.Fatalf("failed to get rolling metrics from live Redis: %v", err)
	}
	if fetched.SampleCount != 5 || fetched.AvgLatencyMS != 32.1 || fetched.MaxCPU != 55.0 {
		t.Fatalf("fetched metrics mismatch: %+v", fetched)
	}
}
