# Distributed Network Monitoring & Telemetry Platform

A distributed network monitoring and telemetry platform that simulates network devices, continuously collects telemetry, evaluates device health, streams events over Kafka, computes rolling-window metrics, detects anomalies, and exposes monitoring and analytical state through HTTP APIs.

## Architecture

```text
                    ┌─────────────────────────┐
                    │ Network Device Simulator│
                    │      (port :8080)       │
                    └────────────┬────────────┘
                                 │ HTTP
                                 ▼
                    ┌─────────────────────────┐
                    │   Monitoring Service    │
                    │      (port :8081)       │
                    └────────────┬────────────┘
                                 │ Kafka Events
                                 ▼
                    ┌─────────────────────────┐
                    │       Apache Kafka      │
                    │  • network.telemetry    │
                    │  • network.health-events│
                    └────────────┬────────────┘
                                 │ Streaming
                                 ▼
                    ┌─────────────────────────┐
                    │     Analysis Service    │
                    │      (port :8082)       │
                    └───────┬─────────┬───────┘
                            │         │
                 Projection │         │ Query API
                            ▼         ▼
                     ┌──────────┐ ┌────────┐
                     │  Redis   │ │ Client │
                     │  State   │ └────────┘
                     └──────────┘
```

# Current Progress

### Epic 1 — Network Device Simulator
- Concurrent network device simulation (`router-01`, `router-02`, `router-03`)
- Realistic, bounded telemetry generation and correlated degradation
- Configurable operating conditions (`NORMAL`, `DEGRADED`, `DOWN`)
- HTTP metrics and failure injection control APIs (`POST /control/{id}/down`, `/recover`)
- Graceful shutdown and concurrency safety

### Epic 2 — Monitoring & Telemetry Ingestion
- Concurrent per-device telemetry polling engine with bounded tickers
- Transport failure classification (HTTP 503 vs network timeout/unreachable)
- Bounded retries with exponential backoff on transient errors
- Multi-signal deterministic health evaluation (`HEALTHY`, `WARNING`, `CRITICAL`, `DOWN`)
- Streaming Kafka publishing (`network.telemetry`, `network.health-events`)
- Read-only HTTP monitoring state API (`:8081`)
- Graceful shutdown and race-safe testing

### Epic 3 — Telemetry Processing & Persistence
- Streaming Kafka consumer group with topic dispatching and error resilience
- Dual-mode `DeviceStateRepository` (Production Redis with key schemas, thread-safe in-memory fallback)
- In-memory bounded sliding aggregation engine (`1m`, `5m`) with strict boundary preservation
- Deterministic multi-signal anomaly detector with noise suppression (`sample_count >= 2`)
- Read-only HTTP analysis API (`:8082`) with composite device snapshots, query filtering, and 503 storage degradation handling
- End-to-end integration and resilience testing under degraded storage and poison pill conditions

# Running Locally

### 1. Start Prerequisites
Ensure Kafka and Redis are running locally:
- Kafka broker on `localhost:9092`
- Redis server on `localhost:6379`

### 2. Start Services
```bash
# Terminal 1: Simulator (listens on :8080)
go run ./services/simulator/...

# Terminal 2: Monitoring Service (listens on :8081)
go run ./services/monitoring/...

# Terminal 3: Analysis Service (listens on :8082)
go run ./services/analysis/...
```

### 3. Verify End-to-End Operation
```bash
# Check service health
curl http://localhost:8080/metrics
curl http://localhost:8081/health
curl http://localhost:8082/health

# View monitoring state
curl http://localhost:8081/devices

# View analytical state and rolling metrics
curl http://localhost:8082/devices
curl http://localhost:8082/devices/router-01
curl "http://localhost:8082/devices/router-01/telemetry?window=1m"
curl http://localhost:8082/devices/router-01/analysis

# Inject failure and observe state propagation
curl -X POST http://localhost:8080/control/router-02/down
curl http://localhost:8081/devices/router-02
curl http://localhost:8082/devices/router-02/analysis

# Recover device
curl -X POST http://localhost:8080/control/router-02/recover
```

# Testing

Run the full automated test suite with race detection:
```bash
go test ./...
go test -race ./...
go vet ./...
```

# Roadmap
- **Epic 4** — Distributed processing, persistence to PostgreSQL, and resilience
- **Epic 5** — Containerization, Kubernetes orchestration, Prometheus metrics, and Grafana dashboards
