"""
Processor — consumes taxi_rides.raw, validates, normalises, aggregates, and routes messages.

Topics:
  IN:  taxi_rides.raw
  OUT: taxi_rides.cleaned   — valid, normalised records
       taxi_rides.dlq       — invalid records with rejection reason
       taxi_aggregates      — rolling stats per pickup zone (compacted)

Imported and called by app/main.py.
"""

import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone

from app.config import (
    ACTIVE_WINDOW_SEC,
    AGGREGATE_INTERVAL_SEC,
    AGGREGATES_TOPIC,
    CLEANED_TOPIC,
    DLQ_TOPIC,
    FARE_WINDOW_SEC,
    GROUP_ID,
    RAW_TOPIC,
)
from app.logger import get_logger
from app.utils import make_consumer, make_producer

log = get_logger("processor")

# ── In-memory state: zone_id → [(processed_at, fare_amount)] ──────────────────
zone_events: dict[str, list] = defaultdict(list)


# =============================================================================
# Validation & normalisation
# =============================================================================

def validate(event: dict) -> tuple[bool, str | None]:
    """Return (is_valid, rejection_reason)."""
    if not event.get("PULocationID"):
        return False, "missing PULocationID"
    if not event.get("tpep_pickup_datetime"):
        return False, "missing pickup_datetime"
    if not event.get("tpep_dropoff_datetime"):
        return False, "missing dropoff_datetime"

    fare = event.get("fare_amount")
    if fare is None or fare <= 0:
        return False, f"invalid fare_amount: {fare}"

    distance = event.get("trip_distance")
    if distance is None or distance <= 0:
        return False, f"invalid trip_distance: {distance}"

    pax = event.get("passenger_count")
    if pax is None or pax <= 0:
        return False, f"invalid passenger_count: {pax}"

    return True, None


def normalise(event: dict) -> dict:
    """Rename fields to cleaner names for the cleaned topic."""
    return {
        "ride_id":        event.get("ride_id"),
        "event_ts":       event.get("event_ts"),
        "pickup_zone":    event["PULocationID"],
        "dropoff_zone":   event["DOLocationID"],
        "pickup_time":    event["tpep_pickup_datetime"],
        "dropoff_time":   event["tpep_dropoff_datetime"],
        "passenger_count": event["passenger_count"],
        "distance_km":    round(event["trip_distance"] * 1.60934, 2),
        "fare_amount":    event["fare_amount"],
        "total_amount":   event["total_amount"],
        "payment_type":       event.get("payment_type"),
        "tip_amount":         float(event.get("tip_amount") or 0.0),
        "ratecode_id":        event.get("RatecodeID"),
        "cbd_congestion_fee": float(event.get("cbd_congestion_fee") or 0.0),
    }


# =============================================================================
# Aggregation
# =============================================================================

def record_event(event: dict) -> None:
    zone = str(event["PULocationID"])
    zone_events[zone].append((datetime.now(timezone.utc), event["fare_amount"]))


def purge_old_events() -> None:
    """Drop events outside the largest window to keep memory bounded."""
    cutoff = datetime.now(timezone.utc) - timedelta(seconds=FARE_WINDOW_SEC)
    for zone in list(zone_events):
        zone_events[zone] = [(ts, fare) for ts, fare in zone_events[zone] if ts > cutoff]
        if not zone_events[zone]:
            del zone_events[zone]


def compute_aggregates() -> dict:
    now = datetime.now(timezone.utc)
    active_cutoff = now - timedelta(seconds=ACTIVE_WINDOW_SEC)
    fare_cutoff   = now - timedelta(seconds=FARE_WINDOW_SEC)

    results = {}
    for zone, events in zone_events.items():
        active      = [e for e in events if e[0] > active_cutoff]
        fare_events = [fare for ts, fare in events if ts > fare_cutoff]
        avg_fare    = round(sum(fare_events) / len(fare_events), 2) if fare_events else 0.0
        results[zone] = {
            "zone":              zone,
            "active_rides_5min": len(active),
            "avg_fare_15min":    avg_fare,
            "computed_at":       now.isoformat(),
        }
    return results


# =============================================================================
# Entry point
# =============================================================================

def run_processor() -> None:
    consumer = make_consumer(RAW_TOPIC, GROUP_ID)
    producer = make_producer()

    processed = 0
    cleaned   = 0
    dlq       = 0
    last_agg  = time.time()
    assigned_partitions: set[int] = set()

    log.info(f"Started — consuming '{RAW_TOPIC}' (group: {GROUP_ID})")
    log.info(f"Aggregates published every {AGGREGATE_INTERVAL_SEC}s")

    try:
        while True:
            records = consumer.poll(timeout_ms=1000)

            # Log partition assignment on first poll (or when it changes)
            current = {tp.partition for tp in consumer.assignment()}
            if current != assigned_partitions:
                assigned_partitions = current
                log.info(f"Partition assignment: {sorted(assigned_partitions)}")

            for _, messages in records.items():
                for msg in messages:
                    processed += 1
                    event = msg.value

                    # DEBUG — goes to file only, not console
                    log.debug(
                        f"partition={msg.partition} offset={msg.offset} "
                        f"key={msg.key} zone={event.get('PULocationID')}"
                    )

                    is_valid, reason = validate(event)

                    if is_valid:
                        cleaned += 1
                        clean = normalise(event)
                        producer.send(CLEANED_TOPIC, key=clean["ride_id"] or str(processed), value=clean)
                        record_event(event)
                    else:
                        dlq += 1
                        producer.send(DLQ_TOPIC, key=event.get("ride_id") or str(processed), value={
                            **event,
                            "dlq_reason": reason,
                        })

                    if processed % 100 == 0:
                        log.info(f"processed={processed}  cleaned={cleaned}  dlq={dlq}")

            # ── Publish aggregates on interval ─────────────────────────────────
            now = time.time()
            if now - last_agg >= AGGREGATE_INTERVAL_SEC:
                purge_old_events()
                aggregates = compute_aggregates()
                for zone, agg in aggregates.items():
                    producer.send(AGGREGATES_TOPIC, key=zone, value=agg)
                if aggregates:
                    producer.flush()
                    log.info(f"[agg] published stats for {len(aggregates)} zones")
                last_agg = now

    except KeyboardInterrupt:
        log.info(f"Stopped. processed={processed}  cleaned={cleaned}  dlq={dlq}")
        log.info(f"Partitions handled by this instance: {sorted(assigned_partitions)}")
    finally:
        producer.flush()
        consumer.close()
