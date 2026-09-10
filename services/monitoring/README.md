# Monitoring Service & Telemetry Ingestion

The **Monitoring Service** is a core service in the Distributed Network Monitoring & Telemetry Platform. It actively monitors a fleet of simulated network devices (routers) over HTTP, ingests real-time telemetry, detects operational degradation and device failures with retries, evaluates device health, produces Kafka events for downstream consumers, and serves a read-only HTTP state API.

---

## Architecture

```text
                    ┌─────────────────┐
                    │ Device Registry │
                    └────────┬────────┘
                             │
                             ▼
                    ┌─────────────────┐
                    │ Polling Engine  │
                    └────────┬────────┘
                             │
                             ▼
                    ┌─────────────────┐
                    │  Telemetry &    │
                    │  State Stores   │
                    └───┬─────────┬───┘
                        │         │
               ┌────────┴───┐ ┌───┴──────────┐
               │   Health   │ │   HTTP API   │
               │ Evaluator  │ │ (port 8081)  │
               └────────┬───┘ └──────────────┘
                        │
                        ▼
               ┌─────────────────┐
               │  Kafka Producer │
               └─────────────────┘
```

### Components

- **Device Registry (`device/`)**: Validates and maintains the registered list of monitored network devices (`router-01`, `router-02`, `router-03`) and their metric endpoints.
- **Concurrent Polling Engine (`polling/`)**: Runs independent worker goroutines per device on a configurable ticker. A slow or failing device never blocks other devices.
- **Transport Failure Detector (`polling/`)**: Classifies failures:
  - HTTP `503` as `ErrDeviceUnavailable`
  - Timeouts and connection errors as `ErrDeviceUnreachable`
  - Executes bounded retries with exponential backoff on transient errors.
  - Transitions devices to `DOWN` upon reaching consecutive failure thresholds; caps counter while `DOWN`.
- **Health Evaluator (`health/`)**: Evaluates device operational health (`HEALTHY`, `WARNING`, `CRITICAL`, `DOWN`) independently of simulator condition strings using strict precedence and multi-signal thresholds.
- **Kafka Producer (`kafka/`)**: Publishes normalized `TelemetryEvent` on every successful poll and `HealthEvent` on health state transitions to Kafka topics.
- **HTTP State API (`api/`)**: Exposes current in-memory monitoring state, device summaries, preserved telemetry, and health assessments over read-only HTTP endpoints.

---

## Running

### 1. Start the Network Device Simulator
From the repository root:
```bash
go run ./services/simulator/...
```
*(Listens on `:8080`)*

### 2. Start the Monitoring Service
From the repository root:
```bash
go run ./services/monitoring/...
```
*(Listens on `:8081`)*

---

## Configuration Reference

All settings can be customized via environment variables:

| Environment Variable | Default Value | Description |
|---|---|---|
| `MONITORING_HTTP_ADDR` | `:8081` | Address and port for the monitoring read HTTP API |
| `SIMULATOR_BASE_URL` | `http://localhost:8080` | Base URL of the network device simulator |
| `POLL_INTERVAL` | `2s` | Frequency at which each device is polled |
| `POLL_TIMEOUT` | `1s` | Bounded HTTP client timeout for each poll request |
| `MAX_RETRIES` | `2` | Number of retries per poll cycle on transient errors |
| `RETRY_BACKOFF` | `50ms` | Initial backoff delay between retry attempts |
| `FAILURE_THRESHOLD` | `3` | Consecutive poll cycle failures before marking device `DOWN` |
| `KAFKA_ENABLED` | `true` | Enables or disables Kafka event publishing |
| `KAFKA_BROKERS` | `localhost:9092` | Comma-separated list of Kafka broker addresses |
| `KAFKA_TELEMETRY_TOPIC` | `network.telemetry` | Kafka topic for point-in-time telemetry events |
| `KAFKA_HEALTH_TOPIC` | `network.health-events` | Kafka topic for device health state transition events |

---

## HTTP Read API

The API provides read-only visibility into current in-memory monitoring state. All responses (including errors) use `Content-Type: application/json`. Non-GET requests return `405 Method Not Allowed`.

### Endpoints

#### 1. Service Health
```bash
curl http://localhost:8081/health
```
**Response (`200 OK`):**
```json
{
  "status": "UP",
  "uptime": "5m12s",
  "monitored_devices": 3,
  "timestamp": "2026-09-11T02:00:00Z"
}
```
*Note: This represents Monitoring Service operational health, not fleet device health. It remains `UP` even if monitored routers are `DOWN`.*

#### 2. Fleet Device Summaries
```bash
curl http://localhost:8081/devices
```
**Response (`200 OK`):**
Returns an array of device summaries including transport status, consecutive failure counts, health assessment, and latest telemetry.

#### 3. Single Device Summary
```bash
curl http://localhost:8081/devices/router-01
```
*Returns the complete monitoring state for `router-01`.*

#### 4. Latest Telemetry
```bash
curl http://localhost:8081/devices/router-01/metrics
```
**Response (`200 OK`):**
```json
{
  "device_id": "router-01",
  "condition": "NORMAL",
  "cpu": 35.2,
  "memory": 50.8,
  "latency_ms": 21,
  "packet_loss": 0.1,
  "interface_up": true,
  "connectivity": true,
  "timestamp": "2026-09-11T02:00:00Z"
}
```
*When a device is `DOWN`, this endpoint continues returning the preserved last successful telemetry snapshot.*

#### 5. Latest Health Assessment
```bash
curl http://localhost:8081/devices/router-01/health
```
**Response (`200 OK`):**
```json
{
  "device_id": "router-01",
  "status": "HEALTHY",
  "score": 0,
  "evaluated_at": "2026-09-11T02:00:00Z"
}
```

---

## Failure Detection & Health Evaluation

### Evaluation Precedence
Health status is determined strictly in this order:
1. **`DOWN`** (Score: 100):
   - Transport failure from StateTracker (reached consecutive failure threshold)
   - `InterfaceUp == false`
   - `Connectivity == false`
2. **`CRITICAL`** (Score: 50–95):
   - CPU $\ge 85.0\%$
   - Memory $\ge 90.0\%$
   - Latency $\ge 150\text{ ms}$
   - Packet Loss $\ge 5.0\%$
3. **`WARNING`** (Score: 10–30):
   - CPU $\ge 70.0\%$
   - Memory $\ge 75.0\%$
   - Latency $\ge 50\text{ ms}$
   - Packet Loss $\ge 1.0\%$
4. **`HEALTHY`** (Score: 0):
   - All signals within normal operating ranges.

---

## Kafka Event Publishing

- **Telemetry Topic (`network.telemetry`)**: Emitted on **every successful poll cycle** for each device.
- **Health Topic (`network.health-events`)**: Emitted **only when the evaluated health status actually changes** (e.g. `HEALTHY` $\to$ `WARNING`, `WARNING` $\to$ `CRITICAL`, `CRITICAL` $\to$ `DOWN`, `DOWN` $\to$ `HEALTHY`).
- **Resilience**: Kafka publishing failures never crash, block, or delay the polling loops, health evaluation, or HTTP API.

---

## Graceful Shutdown Ordering

When the service receives `SIGINT` (`Ctrl+C`) or `SIGTERM`, shutdown is strictly executed in this deterministic sequence:
1. **HTTP Server**: Stop accepting new HTTP requests and gracefully terminate active connections (`server.Shutdown`).
2. **Polling Engine**: Cancel polling worker context and wait for all goroutines to finish (`engine.Wait`).
3. **Kafka Producer**: Flush remaining events and close the producer (`producer.Close`).
4. **Process Exit**: Log completion and return from `Service.Run()`.

---

## Testing

Run all unit, integration, and race-detection test suites:

```bash
# Run unit tests
go test -v ./...

# Run all tests with race detector
go test -race ./...

# Run static analysis
go vet ./...
```
