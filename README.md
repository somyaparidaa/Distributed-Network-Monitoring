# Distributed Network Monitoring & Telemetry Platform

A distributed network monitoring platform that simulates network devices, continuously collects telemetry, evaluates device health, and exposes monitoring state through an HTTP API.

## Architecture

```text
Network Device Simulators
          ↓
   Monitoring Service
          ↓
 ┌────────┼─────────┐
 ↓        ↓         ↓
Polling  Health   Kafka
Engine   Engine   Events
          ↓
      HTTP API
```
# Current Progress
Epic 1 — Network Device Simulator
Concurrent network device simulation
Realistic telemetry generation
Configurable device conditions and failures
HTTP metrics and control APIs
Graceful shutdown

Epic 2 — Monitoring & Telemetry Ingestion
Concurrent telemetry polling
Timeout and failure classification
Retry and backoff handling
HEALTHY / WARNING / CRITICAL / DOWN health states
Kafka telemetry and health events
Read-only monitoring HTTP API
Thread-safe state management
Graceful shutdown and race-safe testing

# Running Locally
```text
Start the simulator:
go run ./services/simulator/...

Start the monitoring service:
go run ./services/monitoring/...

The simulator runs on :8080 and the monitoring API runs on :8081.

Check monitoring health:
curl http://localhost:8081/health

View monitored devices:
curl http://localhost:8081/devices | jq .

View a device's telemetry:
curl http://localhost:8081/devices/router-01/metrics | jq .

Inject a device failure:
curl -X POST http://localhost:8080/control/router-02/down

Check the resulting monitoring state:
curl http://localhost:8081/devices/router-02 | jq .

Recover the device:
curl -X POST http://localhost:8080/control/router-02/recover
```

# Testing
```text
go test ./...
go test -race ./...
go vet ./...
```

# Roadmap
Epic 3 — Telemetry processing and persistence

Epic 4 — Distributed processing and resilience

Epic 5 — Containerization, orchestration, and deployment
