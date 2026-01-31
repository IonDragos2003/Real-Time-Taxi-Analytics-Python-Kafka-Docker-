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
**NYC TLC Yellow Taxi Trips (Parquet)**
- Current file: `data/raw/yellow_tripdata_2025-01.parquet`
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

## Getting started (current)
1. Start services:
   - `docker compose -f docker/docker-compose.yml up -d`
2. Run the producer:
   - `python apps/producer/main.py --file data/raw/yellow_tripdata_2025-01.parquet`
3. Inspect messages:
   - Kafka UI at `http://localhost:8080` → Topics → `taxi_rides.raw` → Messages

## Progress
- Docker Desktop installed and running
- Docker Compose stack created (Kafka, Zookeeper, Schema Registry, Kafka UI)
- Kafka UI accessible at `http://localhost:8080`
- Dataset downloaded: `data/raw/yellow_tripdata_2025-01.parquet`
- Python producer created (parquet → Kafka JSON)

## Current status
- Ingestion working: Kafka topic `taxi_rides.raw` has JSON messages
- Producer adds `ride_id` (if missing) and `event_ts` to each event

## Next steps
- Create `apps/processor/` (stream processing)
- Define clean schema and validation
- Add aggregates + DLQ topic
- Add FastAPI service
- Add Prometheus + Grafana
