# Network Device Simulator

The **Network Device Simulator** simulates multiple virtual network devices (routers) generating real-time telemetry metrics. It acts as the foundational infrastructure producer for the telemetry collection and monitoring platform built in subsequent epics.

---

## Overview

In real-world networks, monitoring systems ingest telemetry metrics (such as CPU, memory utilization, latency, and packet loss) from physical or virtual routers. This service simulates multiple distinct routers running concurrently in a single lightweight process, modeling realistic telemetry behavior, operational degradation, and controllable device failure.

---

## Current Architecture

```text
Fleet
 ├── router-01 (Primary device)
 ├── router-02
 └── router-03
```

- Each device maintains its own independent state protected by `sync.RWMutex`.
- Devices run independent simulation loops with distinct deterministic pseudo-random seeds.
- Devices transition between `NORMAL`, `DEGRADED`, and `DOWN` conditions.
- Failure of an individual device isolates the failure to that specific router: other devices and the overall simulator process remain operational.

---

## Running

Start the simulator from the repository root:

```bash
go run ./services/simulator
```

### Configuration

Configuration defaults are built-in and require no external setup:

| Setting | Default | Description |
|---|---|---|
| Listen Address | `:8080` (or `SIMULATOR_ADDR` env) | Port and address the HTTP server binds to |
| Update Interval | `1s` | Interval at which each device updates telemetry |
| Devices | `router-01`, `router-02`, `router-03` | Default fleet devices |
| Seeds | `101`, `202`, `303` | Deterministic random seeds per device |
| Degradation Threshold | `30.0` | Threshold where condition shifts `NORMAL` -> `DEGRADED` |

---

## Telemetry Metrics Endpoints

### Primary Device (`router-01`)
```bash
curl http://localhost:8080/metrics
```

### Specific Fleet Device
```bash
curl http://localhost:8080/metrics/router-01
curl http://localhost:8080/metrics/router-02
curl http://localhost:8080/metrics/router-03
```

### Example Response (`HTTP 200 OK`)
```json
{
  "device_id": "router-01",
  "condition": "NORMAL",
  "cpu": 35.8,
  "memory": 50.4,
  "latency_ms": 21,
  "packet_loss": 0.12,
  "interface_up": true,
  "connectivity": true,
  "timestamp": "2026-09-10T00:50:00Z"
}
```

Unknown devices (e.g. `/metrics/router-99`) return `HTTP 404 Not Found`.

---

## Failure Injection & Recovery

You can simulate device failure without stopping or restarting the simulator process.

### Inject Failure (`DOWN`)
```bash
curl -X POST http://localhost:8080/control/router-02/down
```
- Status code returned: `HTTP 204 No Content`.
- The device condition becomes `DOWN`, with `interface_up: false` and `connectivity: false`.
- Normal telemetry updates are paused for this device.
- Subsequent queries to its metrics endpoint return `HTTP 503 Service Unavailable`:
  ```bash
  curl -i http://localhost:8080/metrics/router-02
  # HTTP/1.1 503 Service Unavailable
  ```
- Other routers (`router-01`, `router-03`) remain unaffected and continue returning `HTTP 200 OK`.

### Recover Device
```bash
curl -X POST http://localhost:8080/control/router-02/recover
```
- Status code returned: `HTTP 204 No Content`.
- The device condition returns to its pre-failure state (`NORMAL` or `DEGRADED`).
- Telemetry updates resume, and `HTTP 200 OK` is returned on `/metrics/router-02`.

---

## Graceful Shutdown

The simulator handles termination signals (`SIGINT` / `Ctrl+C`, `SIGTERM`) gracefully using `signal.NotifyContext` and standard library `http.Server.Shutdown`:

1. Signal received.
2. Root context is canceled, signaling background simulation goroutines to exit.
3. HTTP server stops accepting new connections and finishes active requests (with a 5-second shutdown deadline).
4. Process exits cleanly with code 0.

---

## Testing

Run all unit, integration, and race detection tests:

```bash
# Run unit tests
go test ./...

# Run tests with race detection
go test -race ./...

# Run static analysis
go vet ./...
```

---

## Development Notes

- **Concurrency**: Handlers and background update loops access device telemetry concurrently; all mutable access is synchronized with `sync.RWMutex`.
- **Zero Third-Party Dependencies**: Uses Go standard library packages exclusively (`net/http`, `sync`, `os/signal`, `context`, etc.).
