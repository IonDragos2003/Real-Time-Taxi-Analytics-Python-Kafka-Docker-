"""
Producer — streams parquet files to taxi_rides.raw.

Globs all *.parquet files from DATA_DIR (set in config.py) and sends
each row as a JSON event, keyed by PULocationID.

Imported and called by app/main.py.
"""

import time
from pathlib import Path

from pyarrow import parquet as pq

from app.config import (
    BATCH_SIZE,
    DATA_DIR,
    MAX_ROWS,
    RAW_TOPIC,
    ROWS_PER_SEC,
)
from app.utils import make_producer, row_to_event


def _stream_file(producer, file_path: Path) -> int:
    """
    Stream one parquet file to Kafka. Returns the number of rows sent.
    """
    parquet_file = pq.ParquetFile(file_path)
    sent = 0
    sleep_per_row = 1.0 / ROWS_PER_SEC if ROWS_PER_SEC > 0 else 0

    try:
        for batch in parquet_file.iter_batches(batch_size=BATCH_SIZE):
            df = batch.to_pandas()
            for _, row in df.iterrows():
                if MAX_ROWS and sent >= MAX_ROWS:
                    raise StopIteration

                event = row_to_event(row, sent + 1)
                producer.send(RAW_TOPIC, key=event.get("PULocationID"), value=event)
                sent += 1

                if sleep_per_row:
                    time.sleep(sleep_per_row)

            producer.flush()
    except StopIteration:
        pass
    finally:
        producer.flush()

    return sent


def run_producer() -> None:
    """
    Entry point called by main.py.
    Finds all parquet files in DATA_DIR and streams them sequentially.
    """
    files = sorted(Path(DATA_DIR).glob("*.parquet"))

    if not files:
        print(f"[producer] No parquet files found in '{DATA_DIR}'")
        return

    producer = make_producer()
    total_sent = 0
    overall_start = time.time()

    for file_path in files:
        print(f"[producer] Streaming {file_path.name} ...")
        start = time.time()
        sent = _stream_file(producer, file_path)
        elapsed = max(time.time() - start, 0.0001)
        print(f"[producer] {file_path.name} — {sent} msgs in {elapsed:.2f}s ({sent/elapsed:.2f} msg/s)")
        total_sent += sent

    producer.flush()

    total_elapsed = max(time.time() - overall_start, 0.0001)
    print(f"\n[producer] Done — {total_sent} total msgs in {total_elapsed:.2f}s")
