"""
FastAPI serving layer — reads taxi_aggregates (compacted) topic and serves
current per-zone stats via HTTP.

Endpoints:
  GET /health                  — pipeline health (message counts, DLQ rate)
  GET /stats/active-rides      — active rides per zone (last 5 min), sorted by busiest
  GET /stats/avg-fare          — avg fare per zone (last 15 min), sorted highest first
  GET /stats/zones/{zone_id}   — full stats for a single zone
  GET /stats/summary           — top 10 zones by active rides + top 10 by fare
  GET /stats/enriched          — per-zone tip rate, trip duration, congestion fee
  GET /stats/ratecodes         — RatecodeID distribution across all rides
  GET /stats/dlq-reasons       — rejection reason breakdown from the DLQ
  POST /admin/clear-state      — wipe all in-memory state (call after a topic reset)
"""

import json
import threading
from collections import defaultdict
from contextlib import asynccontextmanager
from datetime import datetime

from fastapi import FastAPI, HTTPException
from kafka import KafkaConsumer

# ── Kafka config ───────────────────────────────────────────────────────────────
BOOTSTRAP = "localhost:29092"
AGGREGATES_TOPIC = "taxi_aggregates"
CLEANED_TOPIC = "taxi_rides.cleaned"
DLQ_TOPIC = "taxi_rides.dlq"

RATECODE_LABELS = {
    "1": "Standard",
    "2": "JFK Flat Rate",
    "3": "Newark",
    "4": "Nassau / Westchester",
    "5": "Negotiated",
    "6": "Group Ride",
}

# ── In-memory state ────────────────────────────────────────────────────────────
# zone_stats: zone_id → latest aggregate dict from taxi_aggregates
zone_stats: dict[str, dict] = {}

# health counters updated by background thread
health_counters: dict[str, int] = {
    "total_cleaned": 0,
    "total_dlq": 0,
}

# Enriched analytics — per-zone accumulators built from taxi_rides.cleaned / DLQ
_zone_tip: dict       = defaultdict(lambda: {"tip_sum": 0.0, "fare_sum": 0.0, "count": 0})
_zone_duration: dict  = defaultdict(lambda: {"dur_sum": 0.0, "count": 0})
_zone_congestion: dict = defaultdict(lambda: {"fee_sum": 0.0, "rides_with_fee": 0, "total": 0})
_ratecode_counts: dict = defaultdict(int)
_dlq_reasons: dict     = defaultdict(int)
_monthly_stats: dict   = defaultdict(lambda: {
    "total_rides": 0,
    "fare_sum": 0.0,    "fare_count": 0,
    "tip_sum": 0.0,     "tip_fare_sum": 0.0,
    "duration_sum": 0.0, "duration_count": 0,
})


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


def _consume_enriched() -> None:
    """
    Reads taxi_rides.cleaned and taxi_rides.dlq to build enriched per-zone
    analytics: tip rates, trip durations, congestion fees, ratecode distribution,
    and DLQ rejection reasons.
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
        v = msg.value

        if msg.topic == DLQ_TOPIC:
            raw_reason = v.get("dlq_reason", "unknown")
            # Strip the specific value (e.g. "invalid fare_amount: -5.5" → "invalid fare_amount")
            category = raw_reason.split(":")[0].strip() if ":" in raw_reason else raw_reason
            _dlq_reasons[category] += 1
            continue

        zone = str(v.get("pickup_zone", "unknown"))

        # Tips — credit card rides only (payment_type == 1; cash tips aren't recorded)
        if v.get("payment_type") == 1:
            tip  = float(v.get("tip_amount") or 0.0)
            fare = float(v.get("fare_amount") or 0.0)
            if fare > 0:
                _zone_tip[zone]["tip_sum"]  += tip
                _zone_tip[zone]["fare_sum"] += fare
                _zone_tip[zone]["count"]    += 1

        # CBD congestion fee (NYC surcharge introduced Jan 2025)
        fee = float(v.get("cbd_congestion_fee") or 0.0)
        _zone_congestion[zone]["fee_sum"] += fee
        if fee > 0:
            _zone_congestion[zone]["rides_with_fee"] += 1
        _zone_congestion[zone]["total"] += 1

        # RatecodeID
        rc = v.get("ratecode_id")
        if rc is not None:
            try:
                _ratecode_counts[str(int(float(rc)))] += 1
            except (ValueError, TypeError):
                pass

        # Trip duration in minutes
        minutes = None
        try:
            pickup  = datetime.fromisoformat(str(v["pickup_time"]).replace("Z", ""))
            dropoff = datetime.fromisoformat(str(v["dropoff_time"]).replace("Z", ""))
            minutes = (dropoff - pickup).total_seconds() / 60.0
            if 0 < minutes < 120:
                _zone_duration[zone]["dur_sum"] += minutes
                _zone_duration[zone]["count"]   += 1
            else:
                minutes = None
        except Exception:
            pass

        # Monthly aggregation — key is "YYYY-MM" from pickup_time
        month_key = str(v.get("pickup_time", ""))[:7]
        if len(month_key) == 7 and month_key[4] == "-":
            ms = _monthly_stats[month_key]
            ms["total_rides"] += 1
            fare = float(v.get("fare_amount") or 0.0)
            if fare > 0:
                ms["fare_sum"]   += fare
                ms["fare_count"] += 1
            if v.get("payment_type") == 1:
                tip = float(v.get("tip_amount") or 0.0)
                if fare > 0:
                    ms["tip_sum"]      += tip
                    ms["tip_fare_sum"] += fare
            if minutes is not None:
                ms["duration_sum"]   += minutes
                ms["duration_count"] += 1


# ── App lifecycle ──────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    threading.Thread(target=_consume_aggregates, daemon=True).start()
    threading.Thread(target=_consume_health,     daemon=True).start()
    threading.Thread(target=_consume_enriched,   daemon=True).start()
    yield


app = FastAPI(
    title="Taxi Analytics API",
    description="Real-time per-zone stats from the Kafka pipeline.",
    version="0.2.0",
    lifespan=lifespan,
)


# ── Endpoints ──────────────────────────────────────────────────────────────────

@app.get("/health")
def get_health():
    """Pipeline health: message counts and DLQ error rate."""
    total    = health_counters["total_cleaned"] + health_counters["total_dlq"]
    dlq_rate = round(health_counters["total_dlq"] / total * 100, 2) if total else 0.0
    return {
        "status":         "ok",
        "total_cleaned":  health_counters["total_cleaned"],
        "total_dlq":      health_counters["total_dlq"],
        "dlq_rate_pct":   dlq_rate,
        "zones_tracked":  len(zone_stats),
    }


@app.get("/stats/active-rides")
def get_active_rides():
    """Active rides per zone in the last 5 minutes, sorted busiest first."""
    return {
        zone: {
            "active_rides_5min": stats["active_rides_5min"],
            "computed_at":       stats["computed_at"],
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
            "computed_at":    stats["computed_at"],
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
        raise HTTPException(
            status_code=404,
            detail=f"Zone {zone_id!r} not found. "
                   "Either it has no recent activity or the processor hasn't published yet.",
        )
    return stats


@app.get("/stats/enriched")
def get_enriched():
    """Per-zone enriched analytics: avg tip %, avg trip duration, congestion fee coverage."""
    zones = set(_zone_tip) | set(_zone_duration) | set(_zone_congestion)
    result = {}
    for zone in zones:
        tip  = _zone_tip.get(zone, {})
        dur  = _zone_duration.get(zone, {})
        cong = _zone_congestion.get(zone, {})
        result[zone] = {
            "zone": zone,
            "avg_tip_pct": (
                round(tip.get("tip_sum", 0.0) / tip["fare_sum"] * 100, 1)
                if tip.get("fare_sum", 0.0) > 0 else 0.0
            ),
            "avg_duration_min": (
                round(dur["dur_sum"] / dur["count"], 1)
                if dur.get("count", 0) > 0 else 0.0
            ),
            "avg_congestion_fee": (
                round(cong.get("fee_sum", 0.0) / cong["total"], 2)
                if cong.get("total", 0) > 0 else 0.0
            ),
            "congestion_pct": (
                round(cong.get("rides_with_fee", 0) / cong["total"] * 100, 1)
                if cong.get("total", 0) > 0 else 0.0
            ),
            "total_rides": cong.get("total", 0),
        }
    return result


@app.get("/stats/ratecodes")
def get_ratecodes():
    """RatecodeID distribution across all cleaned rides."""
    return {
        code: {"label": RATECODE_LABELS.get(code, "Unknown"), "count": count}
        for code, count in sorted(_ratecode_counts.items(), key=lambda x: -x[1])
    }


@app.get("/stats/dlq-reasons")
def get_dlq_reasons():
    """Breakdown of DLQ rejection reasons by count."""
    return dict(sorted(_dlq_reasons.items(), key=lambda x: -x[1]))


@app.get("/stats/monthly")
def get_monthly():
    """Month-over-month aggregates: rides, avg fare, avg tip %, avg duration."""
    result = {}
    for month, ms in sorted(_monthly_stats.items()):
        result[month] = {
            "month":            month,
            "total_rides":      ms["total_rides"],
            "avg_fare":         round(ms["fare_sum"] / ms["fare_count"], 2) if ms["fare_count"] > 0 else 0.0,
            "avg_tip_pct":      round(ms["tip_sum"] / ms["tip_fare_sum"] * 100, 1) if ms["tip_fare_sum"] > 0 else 0.0,
            "avg_duration_min": round(ms["duration_sum"] / ms["duration_count"], 1) if ms["duration_count"] > 0 else 0.0,
        }
    return result


@app.post("/admin/clear-state")
def clear_state():
    """Wipe all in-memory state. Call after a topic reset so stale data doesn't persist."""
    zone_stats.clear()
    health_counters["total_cleaned"] = 0
    health_counters["total_dlq"]     = 0
    _zone_tip.clear()
    _zone_duration.clear()
    _zone_congestion.clear()
    _ratecode_counts.clear()
    _dlq_reasons.clear()
    _monthly_stats.clear()
    return {"status": "cleared"}
