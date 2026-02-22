# Real-Time Taxi Analytics (Python + Kafka + Docker)

A production-style streaming analytics project using **Python**, **Kafka**, and **Docker Compose**. The pipeline ingests real-world taxi trip data (NYC TLC), processes it in real time, and serves analytics via an API and live dashboard — all locally.

## Why this project

- Practice real Kafka usage: producers, processors, consumer groups, and topic design
- Build streaming analytics with windowed aggregations
- Demonstrate parallel consumption, partition routing, and consumer group rebalancing
- Serve results via a Python API and live Streamlit dashboard

## High-level architecture

```
parquet file
    │
    ▼
[Producer]  ──────────────────► taxi_rides.raw  (3 partitions, keyed by PULocationID)
                                       │
                              ┌────────┴────────┐
                              ▼                 ▼
                         [Processor 1]    [Processor 2]     ← consumer group "processor"
                              │                 │            (each owns a subset of partitions)
                              └────────┬────────┘
                                       │
                           ├──► taxi_rides.cleaned   (valid records)
                           ├──► taxi_rides.dlq       (bad records)
                           └──► taxi_aggregates      (rolling stats, compacted)
                                            │
                                            ▼
                                       [FastAPI]
                                            │
                                            ▼
                                  [Streamlit dashboard]
```

1. **Producer** — reads NYC TLC parquet data row by row, publishes each trip as a JSON event to `taxi_rides.raw`, keyed by `PULocationID` so all rides from the same zone route to the same partition
2. **Processor** — consumes `taxi_rides.raw`, validates and normalises records, computes windowed aggregations per zone (5-min active count, 15-min avg fare), routes bad records to DLQ; multiple instances share load via consumer group
3. **API (FastAPI)** — reads `taxi_aggregates` into memory, serves HTTP endpoints *(coming soon)*
4. **Dashboard (Streamlit)** — live auto-refreshing view of pipeline metrics, analytics, and health *(coming soon)*

## Tech stack

- **Kafka** (single broker, local) + Zookeeper + Schema Registry + Kafka UI
- **Python** — `kafka-python` for producer and processor, FastAPI, Streamlit
- **Docker Compose** — runs the full Kafka stack locally

## Dataset

**NYC TLC Yellow Taxi Trips (Parquet)**

- `data/raw/yellow_tripdata_2025-01.parquet`
- `data/raw/yellow_tripdata_2025-02.parquet`
- `data/raw/yellow_tripdata_2025-03.parquet`
- `data/raw/yellow_tripdata_2025-04.parquet`
- `data/raw/yellow_tripdata_2025-05.parquet`
- `data/raw/yellow_tripdata_2025-06.parquet`
- Run the producer with `--max-rows` to use a manageable sample locally

## Repository layout

```
project-root/
├─ docker/
│  └─ docker-compose.yml
├─ data/
│  └─ raw/                        # parquet source files
├─ apps/
│  ├─ producer/                   # reads parquet → publishes to taxi_rides.raw
│  │  ├─ main.py
│  │  └─ requirements.txt
│  ├─ processor/                  # consumes raw → cleans, aggregates, routes DLQ
│  │  ├─ main.py
│  │  └─ requirements.txt
│  ├─ api/                        # FastAPI serving layer (coming soon)
│  └─ dashboard/                  # Streamlit live dashboard (coming soon)
└─ README.md
```

## Kafka topics

| Topic | Partitions | Config | Description |
| --- | --- | --- | --- |
| `taxi_rides.raw` | 3 | keyed by `PULocationID` | Raw events from the producer, one message per trip |
| `taxi_rides.cleaned` | 3 | — | Validated and normalised records |
| `taxi_rides.dlq` | 1 | — | Rejected records with rejection reason attached |
| `taxi_aggregates` | 1 | compacted | Rolling stats per pickup zone (latest value per zone key) |

## Getting started

### 1. Start the Kafka stack

```bash
docker compose -f docker/docker-compose.yml up -d
```

Kafka UI available at **[http://localhost:8080](http://localhost:8080)**

> In the UI, set **Value Serde → JSON** when inspecting topics to render messages correctly.

### 2. Set up the Python environment

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r apps/producer/requirements.txt
pip install -r apps/processor/requirements.txt
```

### 3. Run two processor instances (Terminal 1 and 2)

```bash
# Terminal 1
source .venv/bin/activate
python3 apps/processor/main.py

# Terminal 2
source .venv/bin/activate
python3 apps/processor/main.py
```

Kafka will automatically assign partitions across both instances (consumer group rebalancing). With 3 partitions, one instance gets 2 partitions and the other gets 1.

### 4. Run the producer across all 6 months (Terminal 3)

```bash
source .venv/bin/activate
for month in 01 02 03 04 05 06; do
  echo "--- Processing 2025-${month} ---"
  python3 apps/producer/main.py \
    --file data/raw/yellow_tripdata_2025-${month}.parquet \
    --max-rows 3000 \
    --rows-per-sec 200
done
```

This sends 18,000 messages total (3,000 per month). The processor instances will print progress every 100 messages and publish zone aggregates every 10 seconds.

### 5. Verify partition assignment

```bash
docker exec -it $(docker ps -qf "name=kafka") \
  kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --group processor
```

You'll see which partitions each processor instance owns, and the current lag per partition.

## Producer flags

| Flag | Default | Description |
| --- | --- | --- |
| `--file` | required | Path to parquet file |
| `--topic` | `taxi_rides.raw` | Target Kafka topic |
| `--bootstrap` | `localhost:29092` | Kafka bootstrap server |
| `--rows-per-sec` | `50` | Throttle rate |
| `--max-rows` | `0` (no limit) | Cap total messages sent |
| `--batch-size` | `5000` | Parquet read batch size |

## Key Kafka concepts demonstrated

| Concept | Where |
| --- | --- |
| **Producers** | `apps/producer/main.py` — publishes JSON events from parquet |
| **Message keys** | `PULocationID` as key → consistent partition routing per zone |
| **Partitions** | 3 partitions on `taxi_rides.raw` → parallel consumption |
| **Consumer groups** | Both processor instances share group `"processor"` |
| **Partition assignment** | Kafka distributes partitions across group members automatically |
| **Consumer rebalancing** | Start/stop a processor instance and watch Kafka reassign partitions |
| **Dead Letter Queue** | `taxi_rides.dlq` receives invalid records with rejection reason |
| **Compacted topics** | `taxi_aggregates` retains only the latest aggregate per zone key |
| **Windowed aggregations** | In-memory 5-min active count and 15-min avg fare per zone |
| **Stream processor pattern** | Processor consumes one topic and produces to multiple output topics |

## Next steps

### Phase 3 — FastAPI serving layer

Read the compacted `taxi_aggregates` topic into memory and expose HTTP endpoints:

- `GET /stats/active-rides` — active rides per zone in the last 5 minutes
- `GET /stats/avg-fare` — average fare per zone in the last 15 minutes
- `GET /health` — pipeline health (message counts, DLQ rate)

### Phase 4 — Streamlit dashboard

Single app with three tabs, auto-refreshing while the pipeline runs:

- **Live metrics** — producer throughput, msgs/sec, total processed
- **Analytics** — active rides per zone, avg fare per zone (charts)
- **Health** — DLQ count, bad message rate, consumer lag per partition

### Future improvements

**Benchmarking mode** — a genuine CV differentiator. Run the same 10k messages with different configs and report results:

| Config | Partitions | Consumers | Throughput | Lag |
| --- | --- | --- | --- | --- |
| Default | 1 | 1 | ~400 msg/s | low |
| Parallel | 3 | 3 | ~1100 msg/s | low |
| Overloaded | 1 | 3 | ~400 msg/s | builds up |

Surface results in the Streamlit dashboard under a "Performance" tab.

**Other additions worth exploring:**

- **Offset reset / replay** — add `--from-beginning` flag to processor to reset offset and replay all historical messages, demonstrating Kafka's commit log nature
- **Idempotent producer** — add `acks='all'` to the producer (one-line change) to guarantee at-least-once delivery even on network hiccups
- **Consumer lag monitoring** — track and visualise lag in the Streamlit dashboard (the most-watched metric in real deployments)
- **Replication** — add a second broker to docker-compose for fault tolerance demonstration (higher complexity, lower priority)
- **Schema Registry + Avro** — enforce message schema at the broker level instead of relying on JSON conventions

## Progress

- [x] Docker Compose stack (Kafka, Zookeeper, Schema Registry, Kafka UI)
- [x] Python producer (parquet → `taxi_rides.raw`, keyed by `PULocationID`)
- [x] Python processor (validate, normalise, aggregate, DLQ routing)
- [x] 3 partitions on `taxi_rides.raw`, parallel processor instances via consumer group
- [x] Processed 18,000 messages across 6 months of NYC TLC data
- [ ] FastAPI serving layer
- [ ] Streamlit live dashboard
- [ ] Benchmarking mode
