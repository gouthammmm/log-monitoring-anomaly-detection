# Log Anomaly Detector

A local, containerized demo that generates application-style logs, detects high error rates, and displays logs and alerts in a browser dashboard.

**This demo uses simulated logs. It does not collect logs from your computer or other applications.**

## How it works

```text
producer services ──> Kafka: raw-logs ──┬──> ingestion consumers ──> ClickHouse
                                        └──> detection service ──> Kafka: anomalies
                                                                    │
                                                                    v
                                                        AI summary service ──> ClickHouse
                                                                                │
                                                                                v
                                                               FastAPI dashboard/API
```

- Two producers generate sample events for four pretend services and occasionally simulate an incident.
- Kafka carries raw logs and detected anomalies between the pipeline services. Kafka is available to the other Compose services, but its port is not published on the host.
- Two ingestion consumers write raw events to ClickHouse. The detector evaluates a rolling 30-second window per service and raises an alert when more than 40% of at least 10 events are errors.
- The summary service adds investigation suggestions. Without an `ANTHROPIC_API_KEY`, it uses a built-in template.
- The FastAPI API reads from ClickHouse and serves the dashboard at `http://localhost:8000`.

## Run locally

Requirements: Docker Desktop with the WSL 2 backend on Windows, or Docker Engine with the Compose plugin on Linux/macOS.

In PowerShell, run:

```powershell
cd "C:\path\to\log-anomaly-platform"
Copy-Item .env.example .env
docker compose up --build
```

On Linux/macOS, use `cp .env.example .env` instead of `Copy-Item`.

The `.env` file is optional. The example provides a local ClickHouse password; add an Anthropic API key if you want AI-generated summaries. Without the key, template summaries are used. `.env` is excluded by `.gitignore`; never commit secrets.

Open [http://localhost:8000](http://localhost:8000). The dashboard polls the API every five seconds. Initial startup can take a little while as Kafka and ClickHouse become healthy. Simulated incidents occur occasionally, so alerts may take a short time to appear.

To stop a foreground run, press **Ctrl+C** in the PowerShell window running Compose. If you started Compose with `-d`, stop it from the project folder with:

```powershell
docker compose down
```

## Services

| Service | Purpose |
| --- | --- |
| `producer` | Generates sample logs and sends them to Kafka. Runs two replicas. |
| `ingestion-consumer` | Reads `raw-logs` and stores events in ClickHouse. Runs as a two-replica Kafka consumer group. |
| `detection-service` | Calculates per-service rolling error rates and publishes anomaly events. |
| `ai-summary-service` | Adds a template or AI-generated summary and stores anomalies in ClickHouse. |
| `clickhouse` | Stores raw logs and enriched anomalies. |
| `api-gateway` | Exposes the REST API and serves the static dashboard. |
| `kafka` | Carries the `raw-logs` and `anomalies` event streams inside the Compose network. |

## API endpoints

- `GET /api/health` — API health check
- `GET /api/logs` — recent logs, with optional service and severity filters
- `GET /api/logs/error-rate-timeseries` — error-rate buckets by service
- `GET /api/anomalies` — detected anomalies and summaries

## Current scope and next steps

This is a learning/demo prototype, not a production monitoring system. Logs are simulated; the detector uses a simple threshold; the dashboard has no authentication. Do not expose it publicly as-is. To monitor a real application, add a configured log collector and map that application's log format to the event fields consumed by this pipeline.

Possible extensions include a file-based log source, latency-based detection, more resilient Kafka offset handling, authentication, and deployment manifests.

## Project structure

```text
producer/                simulated log generator
ingestion-consumer/       Kafka to ClickHouse writer
detection-service/        rolling-window anomaly detection
ai-summary-service/      anomaly summaries
api-gateway/              FastAPI endpoints and static dashboard hosting
dashboard/                HTML/JavaScript dashboard
infra/clickhouse/         ClickHouse schema initialization
.github/workflows/ci.yml  lint, image build, and Compose smoke workflow
```
