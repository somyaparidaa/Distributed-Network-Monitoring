# Analysis Service & Telemetry Intelligence

The **Analysis Service** is a core analytical and state-projection component of the Distributed Network Monitoring & Telemetry Platform. It consumes streaming telemetry events and device health state transitions from Apache Kafka, projects device point-in-time state into a Redis store, computes in-memory rolling-window statistical aggregates (e.g. 1m, 5m), detects multi-signal performance anomalies and outages deterministically, and serves a read-only HTTP API.

---

## Architecture

```text
                  Kafka Topics
         ┌─────────────────────────────┐
         │ network.telemetry           │
         │ network.health-events       │
         └──────────────┬──────────────┘
                        │
                        ▼
         ┌─────────────────────────────┐
         │       Kafka Consumer        │
         └──────────────┬──────────────┘
                        │
                        ▼
         ┌─────────────────────────────┐
         │     Topic Dispatcher        │
         └──────────────┬──────────────┘
                        │
                        ▼
         ┌─────────────────────────────┐
         │      Analysis Pipeline      │
         └──────┬───────┬───────┬──────┘
                │       │       │
    ┌───────────┘       │       └───────────┐
    ▼                   ▼                   ▼
┌──────────────┐ ┌──────────────┐ ┌──────────────────┐
│ In-Memory    │ │ Deterministic│ │ DeviceState      │
│ Aggregation  │ │ Anomaly      │ │ Repository       │
│ Engine       │ │ Detector     │ │ (Redis / Memory) │
└──────┬───────┘ └──────┬───────┘ └─────────┬────────┘
       │                │                   │
       └────────────────┼───────────────────┘
                        ▼
         ┌─────────────────────────────┐
         │     HTTP Read API           │
         │     (port :8082)            │
         └─────────────────────────────┘
```

### Components

- **Kafka Consumer (`consumer/`)**: Consumes `network.telemetry` and `network.health-events` using `segmentio/kafka-go` with consumer groups. Resiliently skips malformed JSON or poison pill messages without interrupting consumption.
- **Topic Dispatcher (`consumer/dispatcher.go`)**: Routes incoming Kafka messages to appropriate pipeline handlers based on topic name.
- **Analysis Pipeline (`main.go`)**: Coordinates data projection, sliding window aggregation, anomaly detection, and state storage.
- **State Repository (`store/`)**: Provides the `DeviceStateRepository` abstraction backed by Redis (`go-redis/v9`) or an in-memory fallback. Tracks:
  - Latest telemetry snapshot: `analysis:device:{id}:telemetry` (No TTL)
  - Latest health assessment: `analysis:device:{id}:health` (No TTL)
  - Rolling metric windows: `analysis:device:{id}:aggregate:{window}` (No TTL)
  - Device analysis & anomalies: `analysis:device:{id}:analysis` (No TTL)
  - Active device set: `analysis:devices`
- **Rolling Aggregation Engine (`aggregation/`)**: Computes sliding time-window metrics (`1m`, `5m`) in-memory. Enforces strict boundary-preserving sample eviction (`sample.Timestamp >= event.Timestamp - windowDuration`) and computes min, max, avg for latency, CPU, memory, and packet loss.
- **Deterministic Anomaly Detector (`anomaly/`)**: Evaluates rolling window metrics and health state against multi-signal operational thresholds. Requires at least 2 samples in the rolling window to trigger rolling metric anomalies, preventing single-spike noise.
- **HTTP Read API (`api/`)**: Serves read-only JSON query endpoints on port `:8082`. Non-GET requests return `405 Method Not Allowed` with `Allow: GET`. Returns `503 Service Unavailable` if the storage backend is unreachable.

---

## Configuration Reference

All settings are configurable via environment variables:

| Environment Variable | Default Value | Description |
|---|---|---|
| `ANALYSIS_HTTP_ADDR` | `:8082` | HTTP read API listen address (fallback: `HTTP_ADDR`) |
| `KAFKA_ENABLED` | `true` | Enables or disables Kafka event consumption |
| `KAFKA_BROKERS` | `localhost:9092` | Comma-separated list of Kafka broker addresses |
| `KAFKA_CONSUMER_GROUP` | `analysis-service` | Kafka consumer group identifier |
| `KAFKA_TELEMETRY_TOPIC` | `network.telemetry` | Topic for point-in-time telemetry events |
| `KAFKA_HEALTH_TOPIC` | `network.health-events` | Topic for device health state transition events |
| `REDIS_ENABLED` | `true` | Enables Redis backend (`false` falls back to `MemoryRepository`) |
| `REDIS_ADDR` | `localhost:6379` | Redis server address |
| `REDIS_DB` | `0` | Redis database index |
| `REDIS_PASSWORD` | `""` | Redis authentication password |
| `REDIS_KEY_PREFIX` | `analysis` | Redis namespace prefix for all keys |
| `AGGREGATION_WINDOWS` | `1m,5m` | Comma-separated rolling window durations |

---

## Running Locally

### Prerequisites
1. **Kafka** running on `localhost:9092`
2. **Redis** running on `localhost:6379`
3. **Simulator** running on `localhost:8080`
4. **Monitoring Service** running on `localhost:8081`

### Start the Analysis Service
From the repository root:
```bash
go run ./services/analysis/...
```
*(Listens on `:8082`)*

---

## HTTP Read API

The Analysis Service provides read-only HTTP endpoints. All responses are JSON (`Content-Type: application/json`). Any non-`GET` method returns `405 Method Not Allowed`.

### Endpoints

#### 1. Service Health
```bash
curl http://localhost:8082/health
```
**Response (`200 OK`):**
```json
{
  "status": "UP",
  "uptime": "15m42s",
  "dependencies": {
    "kafka": "ENABLED",
    "redis": "CONNECTED"
  },
  "timestamp": "2026-09-13T12:00:00Z"
}
```
*Note: If Redis is unavailable, the service reports `"redis": "DISCONNECTED"` with `200 OK` without crashing.*

#### 2. Known Devices
```bash
curl http://localhost:8082/devices
```
**Response (`200 OK`):**
```json
["router-01", "router-02", "router-03"]
```

#### 3. Complete Device State Snapshot
```bash
curl http://localhost:8082/devices/router-01
```
**Response (`200 OK`):**
Returns the composite state:
```json
{
  "device_id": "router-01",
  "latest_telemetry": {
    "event_id": "evt-12345",
    "device_id": "router-01",
    "timestamp": "2026-09-13T12:00:00Z",
    "cpu": 45.2,
    "memory": 58.1,
    "latency_ms": 24,
    "packet_loss": 0.05,
    "interface_up": true,
    "connectivity": true
  },
  "latest_health": {
    "event_id": "evt-health-123",
    "device_id": "router-01",
    "timestamp": "2026-09-13T12:00:00Z",
    "previous_status": "HEALTHY",
    "current_status": "HEALTHY",
    "score": 0
  },
  "rolling_metrics": {
    "device_id": "router-01",
    "window": "1m",
    "sample_count": 30,
    "avg_latency_ms": 22.4,
    "min_latency_ms": 18,
    "max_latency_ms": 32,
    "avg_packet_loss": 0.02,
    "max_packet_loss": 0.1,
    "avg_cpu": 44.1,
    "max_cpu": 52.0,
    "avg_memory": 57.5,
    "max_memory": 60.0,
    "first_sample_time": "2026-09-13T11:59:00Z",
    "latest_sample_time": "2026-09-13T12:00:00Z",
    "calculated_at": "2026-09-13T12:00:00Z"
  },
  "analysis": {
    "device_id": "router-01",
    "active_anomalies": [],
    "calculated_at": "2026-09-13T12:00:00Z"
  }
}
```
*Note: Returns `404 Not Found` if any component (telemetry, health, 1m aggregate, or analysis) has not yet been ingested for that device.*

#### 4. Latest Telemetry & Aggregates
- **All configured windows**:
  ```bash
  curl http://localhost:8082/devices/router-01/telemetry
  ```
  Returns `latest` point-in-time telemetry and a map of `aggregates` (`1m`, `5m`).

- **Specific window**:
  ```bash
  curl "http://localhost:8082/devices/router-01/telemetry?window=1m"
  ```
  Returns `latest` point-in-time telemetry and the single requested `rolling` aggregate.

- **Unconfigured window**:
  ```bash
  curl "http://localhost:8082/devices/router-01/telemetry?window=10m"
  ```
  Returns `400 Bad Request`.

#### 5. Latest Device Health Event
```bash
curl http://localhost:8082/devices/router-01/health
```
Returns the last persisted `HealthEvent`.

#### 6. Latest Anomaly Analysis
```bash
curl http://localhost:8082/devices/router-01/analysis
```
**Response (`200 OK`):**
```json
{
  "device_id": "router-01",
  "active_anomalies": [
    {
      "signal": "LATENCY",
      "severity": "WARNING",
      "value": 62.5,
      "threshold": 50.0,
      "reason": "sustained high latency: average of 62.5ms exceeds warning threshold of 50.0ms over 1m window"
    }
  ],
  "calculated_at": "2026-09-13T12:00:00Z"
}
```

---

## Fault Tolerance & Resilience

- **Redis Resilience**: If Redis goes down, Kafka event consumption continues uninterrupted. Errors are logged as warnings and in-memory aggregation continues. API requests that query storage return `503 Service Unavailable` with `{"error":"storage unavailable"}` instead of misleading 404s.
- **Poison Pill Resilience**: Malformed JSON payloads or events missing mandatory fields (`device_id`, `timestamp`) are logged and discarded. Subsequent valid events process without blockage.
- **Noise Filtering**: Rolling metric anomalies require `sample_count >= 2` within the window to prevent transient spikes from triggering false alarms.
- **Graceful Shutdown**: On `SIGINT` / `SIGTERM`:
  1. HTTP server stops accepting new connections and gracefully completes active requests (`server.Shutdown`).
  2. Kafka consumer worker stops and closes topic readers (`consumer.Close`).
  3. DeviceStateRepository connection is closed cleanly.

---

## Testing

```bash
# Run unit and integration tests
go test -v ./services/analysis/...

# Run with race detector
go test -race ./...

# Run static analysis
go vet ./...
```
