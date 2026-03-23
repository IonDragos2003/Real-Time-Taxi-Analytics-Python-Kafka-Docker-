"""
Pipeline benchmark — compares throughput across 3 partition/consumer configurations.

Run with:
    python3 -m app.benchmark

Kafka must be running (docker compose up -d) before starting.
The benchmark creates and destroys topics automatically for each run.
Do NOT have processors or producers running when you start this.
"""

import os
import sys
import time
import subprocess
from collections import namedtuple

from kafka import KafkaConsumer, TopicPartition
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError, UnknownTopicOrPartitionError

PYTHON    = sys.executable
BOOTSTRAP = "localhost:29092"
RAW_TOPIC = "taxi_rides.raw"

# Configs: vary partition count and number of consumers.
# Each run uses no throttle (ROWS_PER_SEC=0) for accurate measurement.
CONFIGS = [
    {"label": "1 partition  / 1 consumer",  "partitions": 1, "consumers": 1},
    {"label": "3 partitions / 2 consumers", "partitions": 3, "consumers": 2},
    {"label": "3 partitions / 3 consumers", "partitions": 3, "consumers": 3},
]

Result = namedtuple(
    "Result",
    ["label", "partitions", "consumers", "total_msgs", "produce_s", "process_s", "throughput"],
)


# ── Kafka helpers ──────────────────────────────────────────────────────────────

def recreate_topics(n_partitions: int) -> None:
    all_topics = [RAW_TOPIC, "taxi_rides.cleaned", "taxi_rides.dlq", "taxi_aggregates"]
    admin = KafkaAdminClient(bootstrap_servers=BOOTSTRAP)
    try:
        admin.delete_topics(all_topics)
    except (UnknownTopicOrPartitionError, Exception):
        pass

    # Wipe stale committed offsets so lag measurement starts from zero.
    # Without this, old offsets make wait_for_lag_zero return immediately.
    try:
        admin.delete_consumer_groups(["processor"])
    except Exception:
        pass

    new_topics = [
        NewTopic(RAW_TOPIC,            num_partitions=n_partitions, replication_factor=1),
        NewTopic("taxi_rides.cleaned", num_partitions=n_partitions, replication_factor=1),
        NewTopic("taxi_rides.dlq",     num_partitions=1,            replication_factor=1),
        NewTopic("taxi_aggregates",    num_partitions=1,            replication_factor=1,
                 topic_configs={"cleanup.policy": "compact"}),
    ]
    for topic in new_topics:
        for _ in range(40):
            try:
                admin.create_topics([topic])
                break
            except TopicAlreadyExistsError:
                time.sleep(0.5)
    admin.close()


def get_end_offset(n_partitions: int) -> int:
    consumer = KafkaConsumer(bootstrap_servers=BOOTSTRAP)
    tps      = [TopicPartition(RAW_TOPIC, i) for i in range(n_partitions)]
    end      = consumer.end_offsets(tps)
    consumer.close()
    return sum(end.values())


def get_committed_offset() -> int:
    admin = KafkaAdminClient(bootstrap_servers=BOOTSTRAP)
    try:
        committed = admin.list_consumer_group_offsets("processor")
    except Exception:
        committed = {}
    admin.close()
    return sum(
        meta.offset
        for tp, meta in committed.items()
        if tp.topic == RAW_TOPIC and meta
    )


def wait_for_lag_zero(n_partitions: int, timeout: float = 180.0) -> tuple[int, float]:
    """
    Poll until consumer lag reaches 0.
    Returns (total_messages_produced, elapsed_seconds_since_call).
    """
    start        = time.time()
    zeroes_in_a_row = 0

    while time.time() - start < timeout:
        end_offset = get_end_offset(n_partitions)
        committed  = get_committed_offset()
        lag        = end_offset - committed

        if end_offset > 0 and lag <= 0:
            zeroes_in_a_row += 1
            if zeroes_in_a_row >= 3:           # 3 × 0.5s = 1.5s confirmation
                return end_offset, time.time() - start
        else:
            zeroes_in_a_row = 0

        time.sleep(0.5)

    return get_end_offset(n_partitions), timeout   # timed out


# ── Main benchmark ─────────────────────────────────────────────────────────────

def run_benchmark() -> None:
    results: list[Result] = []
    env_no_throttle = {**os.environ, "ROWS_PER_SEC": "0"}

    print("\n" + "=" * 64)
    print("  KAFKA PIPELINE BENCHMARK")
    print(f"  Broker: {BOOTSTRAP}")
    print("=" * 64)

    for config in CONFIGS:
        label   = config["label"]
        n_parts = config["partitions"]
        n_cons  = config["consumers"]

        print(f"\n[ {label} ]")

        print(f"  Recreating topics ({n_parts} partition(s))…")
        recreate_topics(n_parts)

        print(f"  Starting {n_cons} processor(s)…")
        procs = [
            subprocess.Popen(
                [PYTHON, "-u", "-m", "app.main", "--process"],
                env=env_no_throttle,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            for _ in range(n_cons)
        ]
        time.sleep(3)   # let consumers connect and get partition assignments

        print("  Running producer (no throttle)…")
        t0 = time.time()
        subprocess.run(
            [PYTHON, "-u", "-m", "app.main", "--produce"],
            env=env_no_throttle,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        produce_s = time.time() - t0
        print(f"  Producer done in {produce_s:.2f}s")

        print("  Waiting for consumers to clear lag…")
        total_msgs, process_s = wait_for_lag_zero(n_parts)
        throughput = round(total_msgs / process_s, 0) if process_s > 0 else 0

        print(f"  Lag cleared: {total_msgs} msgs in {process_s:.2f}s → {throughput:.0f} msg/s")

        for p in procs:
            p.terminate()
            p.wait()

        results.append(Result(
            label=label,
            partitions=n_parts,
            consumers=n_cons,
            total_msgs=total_msgs,
            produce_s=round(produce_s, 2),
            process_s=round(process_s, 2),
            throughput=throughput,
        ))

        time.sleep(1)

    # ── Summary table ──────────────────────────────────────────────────────────
    W = 76
    print("\n" + "=" * W)
    print("  RESULTS")
    print("=" * W)
    print(f"  {'Config':<30} {'Parts':>6} {'Cons':>5} {'Msgs':>7} "
          f"{'Produce':>9} {'Process':>9} {'Msg/s':>8}")
    print("  " + "-" * (W - 2))
    for r in results:
        print(
            f"  {r.label:<30} {r.partitions:>6} {r.consumers:>5} {r.total_msgs:>7} "
            f"{r.produce_s:>8.2f}s {r.process_s:>8.2f}s {r.throughput:>7.0f}"
        )
    print("=" * W + "\n")

    # ── Speedup relative to first config ──────────────────────────────────────
    if results and results[0].throughput > 0:
        baseline = results[0].throughput
        print("  Speedup vs baseline:")
        for r in results:
            factor = r.throughput / baseline
            print(f"    {r.label:<30}  {factor:.2f}×")
        print()


if __name__ == "__main__":
    run_benchmark()
