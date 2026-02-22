import argparse
import json
import time
from datetime import datetime

import pandas as pd
from kafka import KafkaProducer
from pyarrow import parquet as pq


def parse_args():
    parser = argparse.ArgumentParser(description="Stream parquet rows to Kafka as JSON.")
    parser.add_argument("--file", required=True, help="Path to parquet file")
    parser.add_argument("--topic", default="taxi_rides.raw", help="Kafka topic")
    parser.add_argument("--bootstrap", default="localhost:29092", help="Kafka bootstrap server")
    parser.add_argument("--rows-per-sec", type=float, default=50.0, help="Target rows per second")
    parser.add_argument("--max-rows", type=int, default=0, help="Max rows to send (0 = no limit)")
    parser.add_argument("--batch-size", type=int, default=5000, help="Parquet batch size")
    return parser.parse_args()


def row_to_event(row, row_id):
    event = row.to_dict()
    if "ride_id" not in event:
        event["ride_id"] = str(row_id)
    event["event_ts"] = datetime.utcnow().isoformat() + "Z"
    return event


def main():
    args = parse_args()

    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap,
        value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
        key_serializer=lambda k: str(k).encode("utf-8"),
    )

    parquet_file = pq.ParquetFile(args.file)
    sent = 0
    start = time.time()
    sleep_per_row = 1.0 / args.rows_per_sec if args.rows_per_sec > 0 else 0

    try:
        for batch in parquet_file.iter_batches(batch_size=args.batch_size):
            df = batch.to_pandas()
            for _, row in df.iterrows():
                if args.max_rows and sent >= args.max_rows:
                    raise StopIteration

                event = row_to_event(row, sent + 1)
                producer.send(args.topic, key=event.get("PULocationID"), value=event)
                sent += 1

                if sleep_per_row:
                    time.sleep(sleep_per_row)

            producer.flush()
    except StopIteration:
        pass
    finally:
        producer.flush()

    elapsed = max(time.time() - start, 0.0001)
    print(f"Sent {sent} messages in {elapsed:.2f}s ({sent/elapsed:.2f} msg/s)")


if __name__ == "__main__":
    main()
