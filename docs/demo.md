# NETMON Distributed Telemetry Platform — Demo Script

A concise, step-by-step 5–10 minute demonstration script designed for technical presentations, portfolio showcases, and engineering interviews.

---

## 1. Start the Stack

Start all services from scratch using Docker Compose:

```bash
docker compose up -d --build
```

Explain to the audience:
> *"We are launching a multi-service distributed network telemetry platform composed of 7 independent containers: a Go-based Network Device Simulator, a Monitoring Service, an Apache Kafka broker, a Telemetry Analysis Engine, Redis for state projection, Prometheus for metric scraping, and Grafana for visualization."*

---

## 2. Verify Health & Service Topology

Confirm that all containers are healthy:

```bash
docker compose ps
```

Verify service liveness and readiness:

```bash
# Simulator readiness
curl -s http://localhost:8080/health/ready

# Monitoring readiness (probes Kafka and Simulator)
curl -s http://localhost:8081/health/ready

# Analysis readiness (probes Redis and Kafka)
curl -s http://localhost:8082/health/ready
```

**Key talking point:**
> *"Every service distinguishes Liveness (`/health/live`) from Readiness (`/health/ready`). A service stays alive even if an external downstream dependency drops, but its readiness probe immediately alerts orchestrators that it cannot currently serve production workloads."*

---

## 3. Open Observability Dashboards

- **Grafana**: Open [http://localhost:3000](http://localhost:3000) (Username: `admin`, Password: `admin`).
  - Navigate to **Dashboards $\rightarrow$ Network Telemetry & Monitoring Overview** (`uid: netmon-overview`).
- **Prometheus**: Open [http://localhost:9090/targets](http://localhost:9090/targets) to observe scrape health.

---

## 4. Demonstrate Normal Steady-State Operation

Query the read-only APIs to demonstrate the live telemetry pipeline:

```bash
# 1. Simulator: View raw router-01 point-in-time telemetry
curl -s http://localhost:8080/metrics/router-01 | jq

# 2. Monitoring: View active polling status across all routers
curl -s http://localhost:8081/devices | jq

# 3. Analysis: View Redis-backed composite state with rolling window statistics
curl -s http://localhost:8082/devices/router-01 | jq
```

**What to point out:**
- In Grafana: All 3 devices are in `NORMAL` condition; `Monitored Devices UP = 3`.
- In Analysis API: Point out `rolling_metrics` calculating rolling average and maximum latency/loss across bounded sliding windows (`1m`, `5m`).

---

## 5. Controlled Device Failure Injection

Simulate an interface outage on an individual router (`router-02`):

```bash
curl -X POST http://localhost:8080/control/router-02/down
```

Watch the pipeline react:

1. **Simulator**: Immediately responds with HTTP `503 Service Unavailable` for `router-02` metrics, while `router-01` and `router-03` remain completely unaffected.
2. **Monitoring**:
   - Executes bounded retries with exponential backoff (2 retries, 50ms initial backoff).
   - After 3 consecutive failures, marks `router-02` as `DOWN` with reason `transport unavailable` (score: 100).
   - Publishes a state transition event to Kafka topic `network.health-events`.
3. **Analysis**: Consumes the event from Kafka and projects the updated health to Redis.
4. **Grafana**:
   - The **Down Devices** stat increments to `1`.
   - **Monitored Devices DOWN** increments to `1`.
   - Device health transitions and anomaly counters update on the dashboard.

Inspect the failure in the Monitoring API:
```bash
curl -s http://localhost:8081/devices/router-02 | jq .health
```

---

## 6. Automatic Device Recovery

Restore `router-02`:

```bash
curl -X POST http://localhost:8080/control/router-02/recover
```

Watch the pipeline heal:
- Within 2 seconds (the next polling tick), Monitoring successfully polls `router-02`.
- `consecutive_failures` resets to 0, and status transitions back to `UP`.
- In Grafana: **Down Devices** drops back to 0; **Monitored Devices UP** returns to 3.
- **Talking point**: *"The platform is completely self-healing without requiring any human intervention or service restarts."*

---

## 7. Demonstrate Infrastructure Dependency Failure (Kafka or Redis Outage)

Demonstrate what happens when an infrastructure component fails:

```bash
# Stop Redis
docker compose stop redis
```

Check the Analysis service health:
```bash
# Liveness probe remains 200 UP (process does not crash)
curl -i -s http://localhost:8082/health/live

# Readiness probe returns 503 NOT_READY with diagnosable dependency breakdown
curl -i -s http://localhost:8082/health/ready
```

**Talking point:**
> *"Notice that the Analysis service process does NOT panic, enter a crash loop, or exit. Its HTTP server stays alive, but its readiness endpoint returns 503 Service Unavailable with `dependencies.redis: DISCONNECTED`. In Kubernetes, this keeps the pod alive while removing it from ingress endpoints."*

Now restore Redis:
```bash
docker compose start redis
sleep 2
curl -i -s http://localhost:8082/health/ready
```
Analysis immediately reconnects and returns `200 OK` `{"status":"READY"}`.

---

## 8. Demonstrate Zero-Downtime Microservice Restart

Restart the Monitoring microservice:

```bash
docker compose restart monitoring
```

Verify rapid recovery:
```bash
sleep 3
curl -s http://localhost:8081/health/ready | jq
curl -s http://localhost:8081/devices | jq
```

**Talking point:**
> *"Monitoring cleanly shuts down its goroutines and HTTP server, restarts in under 2 seconds, reconnects to Kafka, and immediately resumes polling without dropping state or requiring Simulator or Kafka restarts."*

---

## 9. Teardown

Conclude the demonstration with clean container shutdown:

```bash
docker compose down
```

