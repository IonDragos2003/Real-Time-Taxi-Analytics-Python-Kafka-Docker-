# Real-Time Taxi Analytics (Python + Kafka + Docker)

A production-style streaming analytics project using **Python**, **Kafka**, and **Docker Compose**. The pipeline ingests real-world NYC taxi trip data, processes it in real time, and serves analytics via a REST API and live dashboard — all locally.

## Why this project

- Practice real Kafka usage: producers, processors, consumer groups, and topic design
- Build streaming analytics with windowed aggregations
- Demonstrate parallel consumption, partition routing, and consumer group rebalancing
- Serve results via a FastAPI layer and a live Streamlit dashboard with admin controls

## High-level architecture

```text
parquet files (NYC TLC)
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
                           ├──► taxi_rides.dlq       (bad records + rejection reason)
                           └──► taxi_aggregates      (rolling stats per zone, compacted)
                                            │
                                            ▼
                                       [FastAPI]
                                            │
                                            ▼
                                  [Streamlit dashboard]
```

1. **Producer** — reads NYC TLC parquet files row by row, publishes each trip as a JSON event to `taxi_rides.raw`, keyed by `PULocationID` so all rides from the same pickup zone always route to the same partition
2. **Processor** — consumes `taxi_rides.raw`, validates and normalises records, computes windowed aggregations per zone (5-min active count, 15-min avg fare), routes invalid records to the DLQ; multiple instances share load via a consumer group
3. **API (FastAPI)** — three background threads consume `taxi_aggregates`, `taxi_rides.cleaned`, and `taxi_rides.dlq` into memory; serves HTTP endpoints for per-zone stats, enriched analytics, health checks, and monthly trends
4. **Dashboard (Streamlit)** — live auto-refreshing view with 4 tabs, zone name lookup, consumer lag monitoring, and admin controls (topic reset, replay from beginning)

## Tech stack

- **Kafka** (single broker, local) + Zookeeper + Schema Registry + Kafka UI
- **Python** — `kafka-python`, FastAPI, Streamlit, pandas
- **Docker Compose** — runs the full Kafka stack locally

## Dataset

### NYC TLC Yellow Taxi Trips (Parquet)

- `data/raw/yellow_tripdata_2025-01.parquet` through `2025-06.parquet`
- 3,000 rows per file used locally (`MAX_ROWS` in `app/config.py`) — **18,000 total messages**
- Fields used: pickup/dropoff zone, datetime, fare, tip, distance, passenger count, payment type, ratecode, CBD congestion fee

## Repository layout

```text
project-root/
├── docker/
│   └── docker-compose.yml
├── data/
│   └── raw/                   # parquet source files (not committed)
├── logs/                      # per-run processor logs (not committed)
├── app/
│   ├── main.py                # CLI entrypoint — --produce / --process / --api
│   ├── config.py              # all constants (Kafka, topics, partition counts, throttle)
│   ├── utils.py               # shared: serializers, make_producer(), make_consumer(), setup_topics()
│   ├── logger.py              # logging setup — file + console, named by component + PID
│   ├── producer.py            # run_producer() — globs data/raw, streams to taxi_rides.raw
│   ├── processor.py           # run_processor() — validate, normalise, aggregate, DLQ
│   ├── dashboard.py           # Streamlit live dashboard
│   ├── benchmark.py           # standalone throughput benchmark across partition/consumer configs
│   └── api/
│       └── main.py            # FastAPI serving layer
├── requirements.txt
└── README.md
```

## Kafka topics

| Topic | Partitions | Config | Description |
| --- | --- | --- | --- |
| `taxi_rides.raw` | 3 | keyed by `PULocationID` | Raw events from the producer, one message per trip |
| `taxi_rides.cleaned` | 3 | — | Validated and normalised records, enriched with tip/ratecode/congestion fields |
| `taxi_rides.dlq` | 1 | — | Rejected records with categorised rejection reason |
| `taxi_aggregates` | 1 | compacted | Rolling stats per pickup zone — latest value per zone key retained |

Topics are created automatically on first run via `KafkaAdminClient` in `utils.setup_topics()`.

## API endpoints

| Method | Path | Description |
| --- | --- | --- |
| GET | `/health` | Total cleaned/DLQ counts, DLQ error rate %, zones tracked |
| GET | `/stats/active-rides` | Active rides per zone in the last 5 min, sorted busiest first |
| GET | `/stats/avg-fare` | Avg fare per zone in the last 15 min, sorted highest first |
| GET | `/stats/summary` | Top 10 zones by active rides + top 10 by avg fare |
| GET | `/stats/zones/{zone_id}` | Full stats for a single pickup zone |
| GET | `/stats/enriched` | Per-zone avg tip %, avg trip duration, congestion fee coverage |
| GET | `/stats/ratecodes` | RatecodeID distribution across all rides |
| GET | `/stats/dlq-reasons` | DLQ rejection reason breakdown by category |
| GET | `/stats/monthly` | Month-over-month: total rides, avg fare, avg tip %, avg duration |
| POST | `/admin/clear-state` | Wipe all in-memory state (call after a topic reset) |

Interactive docs: **[http://localhost:8000/docs](http://localhost:8000/docs)**

## Dashboard tabs

- **Live Metrics** — real-time message throughput (msg/s), active rides per zone, avg fare per zone; all zone IDs resolved to NYC TLC zone names
- **Analytics** — top zones by fare and ride volume, bar charts with zone name labels
- **Health** — pipeline health counters, DLQ error rate, per-partition consumer lag bar chart
- **Deep Analytics** — avg tip rate per zone, avg trip duration, CBD congestion fee %, RatecodeID distribution, DLQ rejection reasons (categorised), month-over-month trend charts

## Getting started

### 1. Start the Kafka stack

```bash
docker compose -f docker/docker-compose.yml up -d
```

Kafka UI: **[http://localhost:8080](http://localhost:8080)**

> Set **Value Serde → JSON** in the Kafka UI when inspecting topics.

### 2. Set up the Python environment

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 3. Run two processor instances (Terminal 1 and 2)

```bash
python3 -u -m app.main --process
```

Kafka automatically assigns partitions across both instances. With 3 partitions and 2 instances, one gets 2 partitions and the other gets 1.

### 4. Run the producer (Terminal 3)

```bash
python3 -u -m app.main --produce
```

Streams 3,000 rows from each of the 6 parquet files (18,000 total). Rate and row limit are set in `app/config.py`.

### 5. Start the API (Terminal 4)

```bash
python3 -u -m app.main --api
```

> After a topic reset (via the dashboard Admin button), restart the API so its background consumers reconnect to the fresh topics.

### 6. Start the Streamlit dashboard (Terminal 5)

```bash
streamlit run app/dashboard.py
```

Dashboard: **[http://localhost:8501](http://localhost:8501)**

### 7. Verify partition split

```bash
grep "partition=" logs/processor_*.log \
  | awk -F'partition=' '{print $2}' \
  | cut -d' ' -f1 \
  | sort | uniq -c
```

Each partition number appears in exactly one log file — proving no two instances processed the same message.

### Resetting between runs

Use the **Reset all topics** button in the dashboard sidebar to delete and recreate all topics cleanly, then restart the API, processors, and producer.

**Replay from beginning** (sidebar) resets the processor group's committed offsets to 0 on `taxi_rides.raw` without deleting any data — useful to re-process existing raw messages after changing processor logic. Do not re-run the producer when replaying; the raw messages are already there.

## Running the benchmark

```bash
python3 -m app.benchmark
```

Runs 3 configurations back-to-back (topics recreated and consumer group reset between each):

| Config | Partitions | Consumers | Expected result |
| --- | --- | --- | --- |
| 1 partition / 1 consumer | 1 | 1 | Baseline — single-threaded |
| 3 partitions / 2 consumers | 3 | 2 | Faster — load split across 2 consumers |
| 3 partitions / 3 consumers | 3 | 3 | Fastest — each consumer owns exactly 1 partition |

Output includes produce time, process time, throughput (msg/s), and speedup relative to baseline.

> Make sure no processors or producers are running before starting the benchmark.

## Configuration

All runtime knobs are in `app/config.py`:

| Constant | Default | Description |
| --- | --- | --- |
| `KAFKA_BOOTSTRAP` | `localhost:29092` | Kafka broker address |
| `DATA_DIR` | `data/raw` | Producer globs `*.parquet` files here |
| `ROWS_PER_SEC` | `200` | Throttle rate (rows/s per file); `0` = no limit |
| `MAX_ROWS` | `3000` | Max rows per file; `0` = no limit |
| `BATCH_SIZE` | `5000` | Parquet read batch size |
| `ACTIVE_WINDOW_SEC` | `300` | 5-min window for active ride count per zone |
| `FARE_WINDOW_SEC` | `900` | 15-min window for avg fare per zone |
| `AGGREGATE_INTERVAL_SEC` | `10` | How often aggregates are published to Kafka |
| `RAW_PARTITIONS` | `3` | Partition count for `taxi_rides.raw` |

`ROWS_PER_SEC` and `MAX_ROWS` can also be overridden via environment variables (used by the benchmark).

## Logging

Each processor instance writes to its own log file under `logs/`, named `processor_<PID>.log`.

- **Console** — INFO and above (progress every 100 messages, partition assignments, aggregate publishes)
- **File** — DEBUG and above (every message: partition, offset, key, zone)

```text
logs/
├── processor_44821.log   ← instance 1 (partitions 0, 1)
└── processor_44822.log   ← instance 2 (partition 2)
```

## Key Kafka concepts demonstrated

| Concept | Where |
| --- | --- |
| **Producers** | `app/producer.py` — publishes JSON events from parquet files |
| **Message keys** | `PULocationID` as key → consistent partition routing per zone |
| **Partitions** | 3 partitions on `taxi_rides.raw` → parallel consumption |
| **Consumer groups** | Both processor instances share group `"processor"` |
| **Partition assignment** | Kafka distributes partitions across group members automatically |
| **Consumer rebalancing** | Start/stop a processor instance — Kafka reassigns partitions live |
| **Dead Letter Queue** | `taxi_rides.dlq` receives invalid records with categorised rejection reason |
| **Compacted topics** | `taxi_aggregates` retains only the latest aggregate per zone key |
| **Windowed aggregations** | In-memory 5-min active count and 15-min avg fare per zone |
| **Stream processor pattern** | Processor consumes one topic and produces to multiple output topics |
| **Topic creation via AdminClient** | Topics created programmatically with correct partition counts on startup |
| **Topic deletion + reset** | Dashboard admin button deletes and recreates all topics, clears API state |
| **Independent consumers** | API background threads use `group_id=None` — never interfere with processor group |
| **Offset management** | Replay button seeks processor group to offset 0 and commits without an admin API |
| **Consumer lag monitoring** | Dashboard Health tab shows per-partition lag in real time |
| **Throughput benchmarking** | `app/benchmark.py` measures msg/s across partition/consumer configs |

## Progress

- [x] Docker Compose stack (Kafka, Zookeeper, Schema Registry, Kafka UI)
- [x] Modular Python app (`app/`) with shared config, utils, and logger
- [x] Producer — globs `data/raw/*.parquet`, streams to `taxi_rides.raw` keyed by `PULocationID`
- [x] Processor — validate, normalise, aggregate, DLQ routing
- [x] Topics created programmatically via `KafkaAdminClient` (3 partitions on raw, compacted aggregates)
- [x] Two processor instances in parallel — Kafka assigns partitions automatically
- [x] Verified no message overlap across instances via partition logs
- [x] Processed 18,000 messages across 6 months of NYC TLC data
- [x] FastAPI serving layer — 10 endpoints including health, zone stats, enriched analytics, monthly trends
- [x] API background consumers use `group_id=None` — independent of processor group
- [x] Streamlit dashboard — 4 tabs: Live Metrics, Analytics, Health, Deep Analytics
- [x] Zone name lookup — numeric zone IDs mapped to NYC TLC names (e.g. 237 → "Upper East Side South")
- [x] Consumer lag monitoring — per-partition lag displayed in Health tab
- [x] Deep Analytics — tip rate, trip duration, CBD congestion fee %, ratecode distribution, DLQ reasons, monthly trends
- [x] Dashboard admin controls — topic reset with confirmation, replay from beginning
- [x] Benchmarking mode — `app/benchmark.py` compares throughput across partition/consumer configs
- [ ] Idempotent producer — `acks='all'` + `enable_idempotence=True` for at-least-once guarantee
- [ ] Replication — second broker in docker-compose for fault tolerance
- [ ] Schema Registry + Avro — enforce message schema at the broker level
