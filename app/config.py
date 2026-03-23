# =============================================================================
# Kafka
# =============================================================================
KAFKA_BOOTSTRAP = "localhost:29092"

# Topics
RAW_TOPIC        = "taxi_rides.raw"
CLEANED_TOPIC    = "taxi_rides.cleaned"
DLQ_TOPIC        = "taxi_rides.dlq"
AGGREGATES_TOPIC = "taxi_aggregates"

# Consumer group
GROUP_ID = "processor"

# =============================================================================
# Topic partitions
# =============================================================================
RAW_PARTITIONS        = 3
CLEANED_PARTITIONS    = 3
DLQ_PARTITIONS        = 1
AGGREGATES_PARTITIONS = 1

# =============================================================================
# Producer
# =============================================================================
import os

DATA_DIR     = "data/raw"                             # producer globs all *.parquet files here
ROWS_PER_SEC = int(os.environ.get("ROWS_PER_SEC", "200"))   # throttle (0 = no limit)
MAX_ROWS     = int(os.environ.get("MAX_ROWS",     "3000"))  # max rows per file (0 = no limit)
BATCH_SIZE   = 5000                                   # parquet read batch size

# =============================================================================
# Processor
# =============================================================================
ACTIVE_WINDOW_SEC      = 300   # 5 min  — active ride count per zone
FARE_WINDOW_SEC        = 900   # 15 min — avg fare per zone
AGGREGATE_INTERVAL_SEC = 10    # how often aggregates are published to Kafka
