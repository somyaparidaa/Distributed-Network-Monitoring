package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"distributed-network-monitor/services/analysis/model"

	"github.com/redis/go-redis/v9"
)

// RedisConfig holds connection and namespace configuration for Redis.
type RedisConfig struct {
	Addr      string
	DB        int
	Password  string
	KeyPrefix string
}

// RedisRepository persists and retrieves device state using a Redis backend.
type RedisRepository struct {
	client *redis.Client
	prefix string
}

// NewRedisRepository initializes a new RedisRepository instance.
func NewRedisRepository(cfg RedisConfig) (*RedisRepository, error) {
	addr := strings.TrimSpace(cfg.Addr)
	if addr == "" {
		addr = "localhost:6379"
	}

	prefix := strings.TrimSpace(cfg.KeyPrefix)
	if prefix == "" {
		prefix = "analysis"
	}

	client := redis.NewClient(&redis.Options{
		Addr:     addr,
		DB:       cfg.DB,
		Password: cfg.Password,
	})

	return &RedisRepository{
		client: client,
		prefix: prefix,
	}, nil
}

// NewRedisRepositoryWithClient creates a RedisRepository using an existing redis.Client (useful for tests).
func NewRedisRepositoryWithClient(client *redis.Client, prefix string) *RedisRepository {
	if strings.TrimSpace(prefix) == "" {
		prefix = "analysis"
	}
	return &RedisRepository{
		client: client,
		prefix: prefix,
	}
}

func (r *RedisRepository) telemetryKey(deviceID string) string {
	return fmt.Sprintf("%s:device:%s:telemetry", r.prefix, deviceID)
}

func (r *RedisRepository) healthKey(deviceID string) string {
	return fmt.Sprintf("%s:device:%s:health", r.prefix, deviceID)
}

func (r *RedisRepository) devicesKey() string {
	return fmt.Sprintf("%s:devices", r.prefix)
}

// SaveLatestTelemetry serializes the telemetry event to JSON and stores it under analysis:device:{id}:telemetry with NO TTL.
// It also registers the device ID into the devices set.
func (r *RedisRepository) SaveLatestTelemetry(ctx context.Context, deviceID string, event model.TelemetryEvent) error {
	data, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal telemetry event: %w", err)
	}

	pipe := r.client.Pipeline()
	pipe.Set(ctx, r.telemetryKey(deviceID), data, 0)
	pipe.SAdd(ctx, r.devicesKey(), deviceID)

	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("save latest telemetry in redis: %w", err)
	}
	return nil
}

// SaveLatestHealth serializes the health event to JSON and stores it under analysis:device:{id}:health with NO TTL.
// It also registers the device ID into the devices set.
func (r *RedisRepository) SaveLatestHealth(ctx context.Context, deviceID string, event model.HealthEvent) error {
	data, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal health event: %w", err)
	}

	pipe := r.client.Pipeline()
	pipe.Set(ctx, r.healthKey(deviceID), data, 0)
	pipe.SAdd(ctx, r.devicesKey(), deviceID)

	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("save latest health in redis: %w", err)
	}
	return nil
}

// GetLatestTelemetry retrieves and deserializes the latest telemetry event for a device.
func (r *RedisRepository) GetLatestTelemetry(ctx context.Context, deviceID string) (*model.TelemetryEvent, error) {
	val, err := r.client.Get(ctx, r.telemetryKey(deviceID)).Bytes()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("get telemetry from redis: %w", err)
	}

	var event model.TelemetryEvent
	if err := json.Unmarshal(val, &event); err != nil {
		return nil, fmt.Errorf("unmarshal telemetry from redis: %w", err)
	}
	return &event, nil
}

// GetLatestHealth retrieves and deserializes the latest health event for a device.
func (r *RedisRepository) GetLatestHealth(ctx context.Context, deviceID string) (*model.HealthEvent, error) {
	val, err := r.client.Get(ctx, r.healthKey(deviceID)).Bytes()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("get health from redis: %w", err)
	}

	var event model.HealthEvent
	if err := json.Unmarshal(val, &event); err != nil {
		return nil, fmt.Errorf("unmarshal health from redis: %w", err)
	}
	return &event, nil
}

// ListDevices returns all known device IDs tracked in the devices set.
func (r *RedisRepository) ListDevices(ctx context.Context) ([]string, error) {
	members, err := r.client.SMembers(ctx, r.devicesKey()).Result()
	if err != nil {
		return nil, fmt.Errorf("list devices from redis: %w", err)
	}
	return members, nil
}

// Ping verifies connectivity to the Redis instance.
func (r *RedisRepository) Ping(ctx context.Context) error {
	return r.client.Ping(ctx).Err()
}

// Close closes the underlying Redis client connection.
func (r *RedisRepository) Close() error {
	return r.client.Close()
}

func (r *RedisRepository) aggregateKey(deviceID, window string) string {
	return fmt.Sprintf("%s:device:%s:aggregate:%s", r.prefix, deviceID, window)
}

// SaveRollingMetrics serializes RollingMetrics to JSON and stores it under analysis:device:{id}:aggregate:{window} with NO TTL.
func (r *RedisRepository) SaveRollingMetrics(ctx context.Context, metrics model.RollingMetrics) error {
	data, err := json.Marshal(metrics)
	if err != nil {
		return fmt.Errorf("marshal rolling metrics: %w", err)
	}

	pipe := r.client.Pipeline()
	pipe.Set(ctx, r.aggregateKey(metrics.DeviceID, metrics.Window), data, 0)
	pipe.SAdd(ctx, r.devicesKey(), metrics.DeviceID)

	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("save rolling metrics in redis: %w", err)
	}
	return nil
}

// GetRollingMetrics retrieves and deserializes the latest rolling metrics for a device and window.
func (r *RedisRepository) GetRollingMetrics(ctx context.Context, deviceID string, window string) (*model.RollingMetrics, error) {
	val, err := r.client.Get(ctx, r.aggregateKey(deviceID, window)).Bytes()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("get rolling metrics from redis: %w", err)
	}

	var metrics model.RollingMetrics
	if err := json.Unmarshal(val, &metrics); err != nil {
		return nil, fmt.Errorf("unmarshal rolling metrics from redis: %w", err)
	}
	return &metrics, nil
}

func (r *RedisRepository) analysisKey(deviceID string) string {
	return fmt.Sprintf("%s:device:%s:analysis", r.prefix, deviceID)
}

// SaveDeviceAnalysis serializes DeviceAnalysis to JSON and stores it under analysis:device:{id}:analysis with NO TTL.
func (r *RedisRepository) SaveDeviceAnalysis(ctx context.Context, analysis model.DeviceAnalysis) error {
	data, err := json.Marshal(analysis)
	if err != nil {
		return fmt.Errorf("marshal device analysis: %w", err)
	}

	pipe := r.client.Pipeline()
	pipe.Set(ctx, r.analysisKey(analysis.DeviceID), data, 0)
	pipe.SAdd(ctx, r.devicesKey(), analysis.DeviceID)

	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("save device analysis in redis: %w", err)
	}
	return nil
}

// GetDeviceAnalysis retrieves and deserializes the latest device analysis for a device.
func (r *RedisRepository) GetDeviceAnalysis(ctx context.Context, deviceID string) (*model.DeviceAnalysis, error) {
	val, err := r.client.Get(ctx, r.analysisKey(deviceID)).Bytes()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("get device analysis from redis: %w", err)
	}

	var analysis model.DeviceAnalysis
	if err := json.Unmarshal(val, &analysis); err != nil {
		return nil, fmt.Errorf("unmarshal device analysis from redis: %w", err)
	}
	return &analysis, nil
}
