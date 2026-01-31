# Real-Time Taxi Analytics (Python + Kafka + Docker)

A step-by-step, production-style streaming analytics project using **Python**, **Kafka**, and **Docker Compose**. The pipeline ingests real-world taxi trip data (NYC TLC), processes it in real time, and serves analytics via an API — all locally.

## Why this project
- Practice real Kafka usage: producers, consumers, consumer groups, and topic design
- Build streaming analytics with windowed aggregations
- Serve results via a Python API
- Add monitoring to understand throughput, lag, and errors

## High-level architecture
1. **Producer (Python)** reads NYC TLC CSV data and publishes events to Kafka.
2. **Stream processor (Python)** cleans/validates data and computes aggregates.
3. **API (FastAPI)** exposes analytics from compacted aggregates.
4. **Observability** via Prometheus + Grafana.

## Tech stack
- Kafka (local, single broker)
- Python (producer + processor + API)
- FastAPI
- Prometheus + Grafana
- Docker Compose

## Dataset
**NYC TLC Taxi Trips (CSV)**
- Use a small sample for local development.
- Optionally map NYC zones to London-style boroughs for a London theme.

## Repository layout (planned)
```
project-root/
├─ docker/
│  └─ docker-compose.yml
├─ data/
│  ├─ raw/
│  └─ processed/
├─ apps/
│  ├─ producer/
│  ├─ processor/
│  └─ api/
├─ monitoring/
│  ├─ prometheus.yml
│  └─ grafana/
├─ docs/
│  ├─ architecture.md
│  └─ decisions.md
└─ README.md
```

## Kafka topics (planned)
- `taxi_rides.raw`
- `taxi_rides.cleaned`
- `taxi_rides.dlq`
- `taxi_aggregates` (compacted)

## Step-by-step plan (summary)
**Phase 1 — Infra + Ingestion**
- Docker Compose for Kafka + UI
- Python producer to publish taxi trips

**Phase 2 — Stream Processing**
- Clean + normalize events
- Windowed aggregates (active rides, avg fare)
- DLQ for bad records

**Phase 3 — Serving Layer**
- FastAPI service
- Endpoints for analytics

**Phase 4 — Observability**
- Prometheus + Grafana
- Metrics for throughput, lag, and errors

## Getting started (placeholder)
When the repo is ready, you’ll be able to:
1. Start services with Docker Compose
2. Run the producer
3. Run the stream processor
4. Run the API

## Status
- Repo creation in progress
- Initial plan and structure ready

## Next steps
- Add `docker-compose.yml`
- Implement producer
- Implement stream processor
- Implement API
- Add monitoring
