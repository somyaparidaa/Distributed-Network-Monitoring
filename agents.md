# AGENTS.md

## Project: Distributed Network Monitoring & Telemetry Platform

This repository is a portfolio project focused on **computer networking, distributed systems, infrastructure engineering, observability, fault tolerance, and cloud-native systems**.

The goal is to build a realistic distributed network monitoring and telemetry platform rather than a conventional CRUD/full-stack application.

The project should demonstrate engineering concepts that are relevant to infrastructure, networking, distributed systems, and backend/SDE roles.

---

# 1. Core Engineering Goals

Prioritize:

* Computer networking
* Distributed systems
* Go
* Microservices
* Kafka
* Docker
* Kubernetes
* Fault tolerance
* Failure detection and recovery
* Observability
* Prometheus
* Grafana
* PostgreSQL
* HTTP APIs
* Concurrency
* Testing
* Scalability

Avoid turning the project into a generic CRUD application.

Every major component should have a clear engineering reason for existing.

Prefer simple, understandable implementations over unnecessary abstraction.

---

# 2. Target Architecture

The intended high-level architecture is:

```text
                    ┌─────────────────────┐
                    │ Network Device      │
                    │ Simulators          │
                    │                     │
                    │ router-01           │
                    │ router-02           │
                    │ router-03           │
                    │ ...                 │
                    └──────────┬──────────┘
                               │
                               ▼
                    ┌─────────────────────┐
                    │ Monitoring Service  │
                    └──────────┬──────────┘
                               │
                               ▼
                         ┌───────────┐
                         │   Kafka   │
                         └─────┬─────┘
                               │
                               ▼
                    ┌─────────────────────┐
                    │ Analysis Service    │
                    └──────────┬──────────┘
                               │
                               ▼
                       ┌──────────────┐
                       │ PostgreSQL   │
                       └──────────────┘

             ┌────────────────────────────┐
             │ API Service / API Gateway  │
             └────────────┬───────────────┘
                          │
                    ┌─────┴─────┐
                    │ Dashboard │
                    └───────────┘

             Prometheus ───────► Services
             Grafana    ───────► Prometheus
```

This architecture will be built incrementally.

Do not implement future architecture prematurely.

---

# 3. Development Philosophy

Development is organized as **Epics → Stories**.

Each story should:

1. Have one clear objective.
2. Build on the previous story.
3. Leave the repository in a working state.
4. Include appropriate tests.
5. Be independently understandable.
6. Avoid unrelated refactoring.
7. Avoid implementing future stories.

Do not combine multiple future stories into one implementation merely because they are technically related.

If a future feature is required for the current story, implement only the minimum necessary foundation.

---

# 4. Current Development Workflow

For every story, follow this workflow:

### Step 1 — Understand

Before changing code:

* Inspect the existing implementation.
* Understand current abstractions.
* Understand existing tests.
* Do not assume the repository matches an imagined design.

### Step 2 — Design

Briefly determine:

* What changes are needed?
* Why are they needed?
* Which existing abstractions should be reused?
* What behavior should remain unchanged?
* What tests are required?

### Step 3 — Implement

Make the smallest clean implementation satisfying the story.

### Step 4 — Test

Run relevant tests.

At minimum, for Go changes:

```bash
go test ./...
```

For concurrency-sensitive changes:

```bash
go test -race ./...
```

Also use:

```bash
go vet ./...
```

when appropriate.

### Step 5 — Manually Verify

When the story changes externally visible behavior, manually exercise the relevant API or service.

### Step 6 — Report

After implementation, report:

* Files changed
* What was implemented
* Tests added
* Validation commands
* Validation results
* Any design decisions
* Any deviations from the requested story

Do not claim a test or command was run unless it was actually run.

---

# 5. Git Conventions

Git history should represent meaningful engineering milestones.

Use **one coherent commit per story**.

Do not create noisy commits such as:

```text
fix typo
fix test
fix another thing
small change
rename variable
```

when those changes belong to the same story.

Preferred format:

```text
NETMON-1.6: Add device failure injection
```

The commit body should briefly describe the meaningful changes.

Do not fabricate historical commits or pretend work happened in a particular commit if it did not.

Push completed story commits to the remote repository when requested.

---

# 6. Epic 1 — Network Device Simulation

Epic 1 establishes the simulated network-device environment that later services will monitor.

Stories:

```text
NETMON-1.1 — Basic HTTP Device Simulator
NETMON-1.2 — Stateful Telemetry Engine
NETMON-1.3 — Realistic Telemetry Behaviour
NETMON-1.4 — Device Operating Conditions
NETMON-1.5 — Multiple Device Simulation
NETMON-1.6 — Device Failure Injection
NETMON-1.7 — Simulator Testing & Hardening
```

---

# 7. Completed Stories

## NETMON-1.1 — Basic HTTP Device Simulator

Implemented:

* Go-based network device simulator
* HTTP server
* `router-01`
* TCP port `8080`
* `GET /metrics`
* JSON telemetry response

---

## NETMON-1.2 — Stateful Telemetry Engine

Implemented:

* Persistent in-memory telemetry state
* Background periodic updates
* Goroutines
* Mutex/RWMutex protection
* Race-safe concurrent state access

---

## NETMON-1.3 — Realistic Telemetry Behaviour

Implemented:

* Gradual metric changes
* Bounded metric values
* Correlated degradation
* Randomized but deterministic simulation when seeded

Metrics include:

* CPU
* Memory
* Latency
* Packet loss
* Interface status
* Connectivity
* Timestamp
* Device ID

---

## NETMON-1.4 — Device Operating Conditions

Implemented:

```text
NORMAL
DEGRADED
```

The simulator maintains an internal degradation value and exposes an explicit device condition.

The degradation threshold is currently:

```text
30.0
```

The existing internal degradation model should not be unnecessarily replaced.

---

## NETMON-1.5 — Multiple Device Simulation

Implemented:

```text
router-01
router-02
router-03
```

with:

* Fleet abstraction
* Independent device state
* Independent simulation loops
* Unique device IDs
* Independent deterministic random seeds
* Device-specific metrics endpoints
* Validation for empty IDs
* Validation for duplicate IDs

Current endpoints include:

```text
GET /metrics
GET /metrics/router-01
GET /metrics/router-02
GET /metrics/router-03
```

`GET /metrics` represents the primary device, `router-01`.

Unknown devices correctly return:

```text
404 Not Found
```

For example:

```text
GET /metrics/router-04
→ 404
```

The primary-device startup check is intentionally defensive:

```go
primaryDevice, exists := fleet.Device("router-01")
if !exists {
    log.Fatal("primary device router-01 not found")
}
```

Do not remove this merely because `router-01` is currently hardcoded.

---

# 8. Current Story — NETMON-1.6

## Device Failure Injection

The objective is to simulate failure of an **individual network device** without killing the simulator process.

Expected behavior:

```text
Fleet
 ├── router-01 → NORMAL → available
 ├── router-02 → DOWN   → unavailable
 └── router-03 → NORMAL → available
```

This is intentionally different from a simulator/service failure.

### Device failure

```text
router-02 fails
```

while:

```text
router-01 remains available
router-03 remains available
simulator process remains alive
```

This distinction will later be useful when demonstrating distributed-system failure scenarios.

---

# 9. NETMON-1.6 Design Constraints

Add:

```text
NORMAL
DEGRADED
DOWN
```

as device conditions.

A DOWN device should:

* Have condition `DOWN`
* Have `InterfaceUp == false`
* Have `Connectivity == false`
* Stop normal telemetry updates
* Remain represented inside the fleet
* Keep its simulation goroutine/process alive
* Be recoverable
* Become available again after recovery

The simulator itself must **not** terminate when an individual device goes DOWN.

---

# 10. Failure Injection API

Use a simple control API for manually injecting failures.

Preferred endpoints:

```text
POST /control/{deviceID}/down
POST /control/{deviceID}/recover
```

Example:

```bash
curl -X POST http://localhost:8080/control/router-02/down
```

Then:

```bash
curl http://localhost:8080/metrics/router-02
```

should produce:

```text
503 Service Unavailable
```

Other devices should continue working:

```bash
curl http://localhost:8080/metrics/router-01
curl http://localhost:8080/metrics/router-03
```

Both should continue returning successful telemetry responses.

Recovery:

```bash
curl -X POST http://localhost:8080/control/router-02/recover
```

After recovery:

```bash
curl http://localhost:8080/metrics/router-02
```

should again return telemetry successfully.

Unknown devices should return:

```text
404 Not Found
```

Non-POST methods on control endpoints should return:

```text
405 Method Not Allowed
```

---

# 11. Concurrency Requirements

The simulator already uses `sync.RWMutex`.

Continue using the existing synchronization model.

Failure state must be protected against concurrent access.

The following may happen concurrently:

```text
Device update goroutine
        +
HTTP metrics request
        +
Failure injection request
        +
Recovery request
```

The implementation must remain race-safe.

Run:

```bash
go test -race ./...
```

for concurrency-sensitive changes.

Do not introduce unnecessary synchronization mechanisms.

---

# 12. Failure Model

Keep the failure model intentionally simple.

A DOWN device represents:

```text
The monitoring system cannot successfully obtain telemetry
from this device.
```

For the simulator's HTTP interface, represent this as:

```text
HTTP 503 Service Unavailable
```

Do not implement artificial long-running sleeps, socket hijacking, or complicated timeout simulation unless a later story explicitly requires it.

Actual retry/timeout/failure-detection behavior belongs primarily to the future **Monitoring Service**.

---

# 13. Current Simulator Structure

The simulator currently contains concepts including:

```go
Telemetry
SimulationConfig
DeviceConfig
Simulator
Fleet
DeviceCondition
```

Important existing behavior includes:

* `Simulator.Run(ctx)`
* periodic telemetry updates
* `currentTelemetry()`
* metrics HTTP handlers
* `Fleet.Device(...)`
* `Fleet.Run(...)`
* deterministic per-device random seeds
* degradation progression
* bounded metric updates

Preserve these abstractions unless the current story genuinely requires modification.

Avoid unnecessary rewrites.

---

# 14. Testing Philosophy

Tests should verify behavior, not implementation trivia.

Prefer tests such as:

```text
device starts operational
device can enter DOWN state
DOWN changes condition
DOWN changes connectivity/interface state
DOWN device returns 503
recovery restores availability
recovered device returns telemetry
unknown device returns 404
invalid HTTP method returns 405
one failed device does not affect another
concurrent access remains race-safe
```

Existing tests must continue to pass.

Do not delete existing tests simply because implementation changes.

If an existing test becomes invalid because the intended behavior changed, update it deliberately and explain why.

---

# 15. Scope Discipline

For NETMON-1.6, do NOT implement:

* Monitoring Service
* Kafka
* PostgreSQL
* Alert Service
* Dashboard
* Kubernetes
* Docker
* Prometheus
* Grafana
* HPA
* retry systems
* authentication
* distributed tracing
* production-grade service discovery
* graceful server shutdown

These belong to later stories.

Do not prematurely build future infrastructure.

---

# 16. Future Failure Scenarios

The complete project is expected to demonstrate several types of failure.

### Device failure

```text
router-07 stops responding
        ↓
monitoring service detects failure
        ↓
DOWN
        ↓
alert
```

### Pod/service failure

```text
Monitoring Service
 ├── Pod 1
 ├── Pod 2
 └── Pod 3

Pod 2 dies
   ↓
Kubernetes replaces it
   ↓
3 replicas again
```

### Network degradation

```text
Latency:
20 → 50 → 100 → 300 → 500 ms

Packet loss:
0.1% → 1% → 5% → 10%

State:
HEALTHY → WARNING → CRITICAL
```

### Service overload

```text
2 replicas
   ↓
4 replicas
   ↓
8 replicas
```

through Kubernetes HPA.

Do not implement these future scenarios during NETMON-1.6 unless explicitly requested.

---

# 17. Code Quality

Prefer:

* idiomatic Go
* small functions
* clear names
* explicit behavior
* standard library where sufficient
* minimal dependencies
* deterministic tests where possible
* proper error handling
* concurrency safety

Avoid:

* premature abstractions
* unnecessary interfaces
* excessive helper layers
* giant frameworks
* speculative configuration systems
* duplicated logic
* magic values when a named constant is appropriate

Do not refactor working code merely for stylistic preference.

---

# 18. Agent Behavior

When working on this repository:

### Always

* Inspect existing code before modifying it.
* Preserve working behavior.
* Follow the current story's scope.
* Add tests for new behavior.
* Run validation commands.
* Explain significant design decisions.
* Keep changes reviewable.
* Treat the repository as an evolving engineering project, not a disposable coding exercise.

### Never

* Jump ahead to future stories.
* Rewrite large portions of the project unnecessarily.
* Remove working tests without justification.
* Add dependencies without a reason.
* Claim commands were run when they were not.
* Create fake Git history.
* Turn the project into generic CRUD.
* Implement features simply because they might be useful later.

---

# 19. Story Completion Standard

A story is complete only when:

```text
Implementation
      ↓
Tests
      ↓
Race / static validation where applicable
      ↓
Manual verification where applicable
      ↓
Working repository
      ↓
Meaningful Git commit
```

The agent should not declare a story complete merely because the code compiles.

---

# 20. Engineering Intent

This project is intended to demonstrate that the developer understands not only how to write code, but also:

* why distributed systems fail
* how failures are detected
* how services communicate
* how state is managed concurrently
* how systems recover
* how services scale
* how observability works
* how infrastructure behaves under failure
* how networking concepts map to real systems

When making design decisions, prefer solutions that make these engineering concepts **clear and demonstrable** without artificially increasing complexity.