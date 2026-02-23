"""
FastAPI serving layer — reads taxi_aggregates (compacted) topic and serves
current per-zone stats via HTTP.

Endpoints:
  GET /health                  — pipeline health (message counts, DLQ rate)
  GET /stats/active-rides      — active rides per zone (last 5 min), sorted by busiest
  GET /stats/avg-fare          — avg fare per zone (last 15 min), sorted highest first
  GET /stats/zones/{zone_id}   — full stats for a single zone
  GET /stats/summary           — top 10 zones by active rides + top 10 by fare
"""

import json
import threading
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from kafka import KafkaConsumer

# ── Kafka config ───────────────────────────────────────────────────────────────
BOOTSTRAP = "localhost:29092"
AGGREGATES_TOPIC = "taxi_aggregates"
CLEANED_TOPIC = "taxi_rides.cleaned"
DLQ_TOPIC = "taxi_rides.dlq"

# ── In-memory state ────────────────────────────────────────────────────────────
# zone_stats: zone_id → latest aggregate dict from taxi_aggregates
zone_stats: dict[str, dict] = {}

# health counters updated by background thread
health_counters: dict[str, int] = {
    "total_cleaned": 0,
    "total_dlq": 0,
}


# ── Background threads ─────────────────────────────────────────────────────────

def _consume_aggregates() -> None:
    """
    Reads taxi_aggregates from the beginning on every start.
    Because it's a compacted topic, reading to end gives the latest value
    per zone. Keeps running to pick up new aggregates as the processor publishes.
    group_id=None → independent consumer, no offset committed, never affects
    the processor's consumer group.
    """
    consumer = KafkaConsumer(
        AGGREGATES_TOPIC,
        bootstrap_servers=BOOTSTRAP,
        group_id=None,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        consumer_timeout_ms=-1,  # block forever — never stop polling
    )
    for msg in consumer:
        zone = msg.key.decode("utf-8") if msg.key else "unknown"
        zone_stats[zone] = msg.value


def _consume_health() -> None:
    """
    Reads taxi_rides.cleaned and taxi_rides.dlq from the beginning to count
    total processed and total bad records. Keeps running to track new messages.
    """
    consumer = KafkaConsumer(
        CLEANED_TOPIC,
        DLQ_TOPIC,
        bootstrap_servers=BOOTSTRAP,
        group_id=None,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        consumer_timeout_ms=-1,
    )
    for msg in consumer:
        if msg.topic == CLEANED_TOPIC:
            health_counters["total_cleaned"] += 1
        else:
            health_counters["total_dlq"] += 1


# ── App lifecycle ──────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    threading.Thread(target=_consume_aggregates, daemon=True).start()
    threading.Thread(target=_consume_health, daemon=True).start()
    yield


app = FastAPI(
    title="Taxi Analytics API",
    description="Real-time per-zone stats from the Kafka pipeline.",
    version="0.1.0",
    lifespan=lifespan,
)


# ── Endpoints ──────────────────────────────────────────────────────────────────

@app.get("/health")
def get_health():
    """Pipeline health: message counts and DLQ error rate."""
    total = health_counters["total_cleaned"] + health_counters["total_dlq"]
    dlq_rate = round(health_counters["total_dlq"] / total * 100, 2) if total else 0.0
    return {
        "status": "ok",
        "total_cleaned": health_counters["total_cleaned"],
        "total_dlq": health_counters["total_dlq"],
        "dlq_rate_pct": dlq_rate,
        "zones_tracked": len(zone_stats),
    }


@app.get("/stats/active-rides")
def get_active_rides():
    """Active rides per zone in the last 5 minutes, sorted busiest first."""
    return {
        zone: {
            "active_rides_5min": stats["active_rides_5min"],
            "computed_at": stats["computed_at"],
        }
        for zone, stats in sorted(
            zone_stats.items(),
            key=lambda x: x[1]["active_rides_5min"],
            reverse=True,
        )
    }


@app.get("/stats/avg-fare")
def get_avg_fare():
    """Average fare per zone in the last 15 minutes, sorted highest first."""
    return {
        zone: {
            "avg_fare_15min": stats["avg_fare_15min"],
            "computed_at": stats["computed_at"],
        }
        for zone, stats in sorted(
            zone_stats.items(),
            key=lambda x: x[1]["avg_fare_15min"],
            reverse=True,
        )
    }


@app.get("/stats/summary")
def get_summary():
    """Top 10 zones by active rides and top 10 zones by average fare."""
    by_rides = sorted(
        zone_stats.items(),
        key=lambda x: x[1]["active_rides_5min"],
        reverse=True,
    )[:10]

    by_fare = sorted(
        zone_stats.items(),
        key=lambda x: x[1]["avg_fare_15min"],
        reverse=True,
    )[:10]

    return {
        "top_zones_by_active_rides": [
            {"zone": z, **{k: v for k, v in stats.items() if k != "zone"}}
            for z, stats in by_rides
        ],
        "top_zones_by_avg_fare": [
            {"zone": z, **{k: v for k, v in stats.items() if k != "zone"}}
            for z, stats in by_fare
        ],
        "total_zones_tracked": len(zone_stats),
    }


@app.get("/stats/zones/{zone_id}")
def get_zone(zone_id: str):
    """Full stats for a single pickup zone."""
    stats = zone_stats.get(zone_id)
    if not stats:
        raise HTTPException(status_code=404, detail=f"Zone {zone_id!r} not found. "
                            "Either it has no recent activity or the processor hasn't published yet.")
    return stats