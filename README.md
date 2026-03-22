# Real-Time Taxi Analytics (Python + Kafka + Docker)

A production-style streaming analytics project using **Python**, **Kafka**, and **Docker Compose**. The pipeline ingests real-world taxi trip data (NYC TLC), processes it in real time, and serves analytics via an API and live dashboard — all locally.

## Why this project

- Practice real Kafka usage: producers, processors, consumer groups, and topic design
- Build streaming analytics with windowed aggregations
- Demonstrate parallel consumption, partition routing, and consumer group rebalancing
- Serve results via a Python API and live Streamlit dashboard

## High-level architecture

```text
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
3. **API (FastAPI)** — reads `taxi_aggregates` and `taxi_rides.cleaned` into memory, serves HTTP endpoints for per-zone stats and enriched analytics
4. **Dashboard (Streamlit)** — live auto-refreshing view of pipeline metrics, zone analytics, deep analytics (tips, duration, congestion, ratecodes, DLQ), and admin controls

## Tech stack

- **Kafka** (single broker, local) + Zookeeper + Schema Registry + Kafka UI
- **Python** — `kafka-python`, FastAPI, Streamlit
- **Docker Compose** — runs the full Kafka stack locally

## Dataset

### NYC TLC Yellow Taxi Trips (Parquet)

- `data/raw/yellow_tripdata_2025-01.parquet` through `2025-06.parquet`
- 3,000 rows per file used locally (`MAX_ROWS` in `app/config.py`) — 18,000 total messages

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
│   └── api/
│       └── main.py            # FastAPI serving layer
├── requirements.txt
└── README.md
```

## Kafka topics

| Topic | Partitions | Config | Description |
| --- | --- | --- | --- |
| `taxi_rides.raw` | 3 | keyed by `PULocationID` | Raw events from the producer, one message per trip |
| `taxi_rides.cleaned` | 3 | — | Validated and normalised records |
| `taxi_rides.dlq` | 1 | — | Rejected records with rejection reason attached |
| `taxi_aggregates` | 1 | compacted | Rolling stats per pickup zone (latest value per zone key) |

Topics are created automatically on first run via `KafkaAdminClient` in `utils.setup_topics()`.

## Getting started

### 1. Start the Kafka stack

```bash
docker compose -f docker/docker-compose.yml up -d
```

Kafka UI at **[http://localhost:8080](http://localhost:8080)**

> Set **Value Serde → JSON** in the UI when inspecting topics.

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

Interactive docs at **[http://localhost:8000/docs](http://localhost:8000/docs)**

> After a topic reset (via the dashboard Admin button), restart the API so its background consumers reconnect to the fresh topics.

### 6. Start the Streamlit dashboard (Terminal 5)

```bash
streamlit run app/dashboard.py
```

Dashboard at **[http://localhost:8501](http://localhost:8501)**

#### Resetting between runs

Use the **🗑️ Reset all topics** button in the dashboard sidebar to wipe all topic data and recreate topics cleanly. Then restart the API and re-run the processors and producer.

### 7. Verify partition split

```bash
grep "partition=" logs/processor_*.log \
  | awk -F'partition=' '{print $2}' \
  | cut -d' ' -f1 \
  | sort | uniq -c
```

Each partition number appears in exactly one log file — proving no two instances processed the same message.

## Configuration

All runtime knobs are in `app/config.py` — no CLI flags needed:

| Constant | Default | Description |
| --- | --- | --- |
| `KAFKA_BOOTSTRAP` | `localhost:29092` | Kafka broker address |
| `DATA_DIR` | `data/raw` | Producer globs `*.parquet` files here |
| `ROWS_PER_SEC` | `200` | Throttle rate per file |
| `MAX_ROWS` | `3000` | Max rows per file (0 = no limit) |
| `BATCH_SIZE` | `5000` | Parquet read batch size |
| `ACTIVE_WINDOW_SEC` | `300` | 5-min window for active ride count |
| `FARE_WINDOW_SEC` | `900` | 15-min window for avg fare |
| `AGGREGATE_INTERVAL_SEC` | `10` | How often aggregates are published |
| `RAW_PARTITIONS` | `3` | Partition count for taxi_rides.raw |

## Logging

Each processor instance writes to its own log file under `logs/`, named `processor_<PID>.log`. This allows you to inspect exactly which partitions and messages each instance handled.

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
| **Consumer rebalancing** | Start/stop a processor instance and watch Kafka reassign partitions |
| **Dead Letter Queue** | `taxi_rides.dlq` receives invalid records with rejection reason |
| **Compacted topics** | `taxi_aggregates` retains only the latest aggregate per zone key |
| **Windowed aggregations** | In-memory 5-min active count and 15-min avg fare per zone |
| **Stream processor pattern** | Processor consumes one topic and produces to multiple output topics |
| **Topic creation via AdminClient** | Topics created programmatically with correct partition counts on startup |
| **Topic deletion + reset** | Dashboard admin button deletes and recreates all topics, clears API state |
| **Independent consumers** | API background threads use `group_id=None` — never interfere with processor group |

## Next steps

### Future improvements

**Benchmarking mode** — Run the same 10k messages with different configs and report results:

| Config | Partitions | Consumers | Throughput | Lag |
| --- | --- | --- | --- | --- |
| Default | 1 | 1 | ~400 msg/s | low |
| Parallel | 3 | 3 | ~1100 msg/s | low |
| Overloaded | 1 | 3 | ~400 msg/s | builds up |

**Other additions worth exploring:**

- **Offset reset / replay** — `--from-beginning` flag to replay all historical messages, demonstrating Kafka's commit log nature
- **Idempotent producer** — `acks='all'` for at-least-once delivery guarantee
- **Consumer lag monitoring** — track and visualise lag in the Streamlit dashboard
- **Replication** — second broker in docker-compose for fault tolerance
- **Schema Registry + Avro** — enforce message schema at the broker level

## Progress

- [x] Docker Compose stack (Kafka, Zookeeper, Schema Registry, Kafka UI)
- [x] Modular Python app (`app/`) with shared config, utils, and logger
- [x] Producer — globs `data/raw/*.parquet`, streams to `taxi_rides.raw` keyed by `PULocationID`
- [x] Processor — validate, normalise, aggregate, DLQ routing
- [x] Topics created programmatically via `KafkaAdminClient` (3 partitions on raw, compacted aggregates)
- [x] Two processor instances in parallel — Kafka assigns partitions automatically
- [x] Verified no message overlap across instances via partition logs
- [x] Processed 18,000 messages across 6 months of NYC TLC data
- [x] FastAPI serving layer — `/health`, `/stats/active-rides`, `/stats/avg-fare`, `/stats/summary`, `/stats/zones/{id}`
- [x] Enriched analytics endpoints — `/stats/enriched`, `/stats/ratecodes`, `/stats/dlq-reasons`
- [x] Streamlit live dashboard — 4 tabs: Live Metrics, Analytics, Health, Deep Analytics
- [x] Deep Analytics tab — avg tip rate, trip duration, CBD congestion fee %, ratecode distribution, DLQ reasons
- [x] Dashboard admin controls — zone lookup sidebar widget, topic reset with confirmation
- [ ] Zone name lookup — map numeric zone IDs to NYC TLC zone names (e.g. 237 → "Upper East Side South")
- [ ] Consumer lag monitoring — track and visualise per-partition lag in the Health tab
- [ ] Benchmarking mode — compare throughput across partition/consumer configs
