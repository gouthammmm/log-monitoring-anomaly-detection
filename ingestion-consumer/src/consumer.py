"""
Consumer group member: reads raw logs from Kafka and persists them to ClickHouse.
Run multiple replicas of this service (see docker-compose 'deploy.replicas') -
Kafka will automatically split the 'raw-logs' partitions across them, which is
the actual mechanism that makes this "distributed" rather than a single worker.
"""
import json
import os
import socket
import time
import uuid
from datetime import datetime

import clickhouse_connect
from kafka import KafkaConsumer

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
CLICKHOUSE_HOST = os.environ.get("CLICKHOUSE_HOST", "localhost")
CLICKHOUSE_USER = os.environ.get("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.environ.get("CLICKHOUSE_PASSWORD", "")
TOPIC = "raw-logs"
GROUP_ID = "ingestion-consumer-group"
CONSUMER_ID = f"{socket.gethostname()}-{uuid.uuid4().hex[:6]}"

BATCH_SIZE = 200
BATCH_TIMEOUT_S = 2.0


def make_consumer():
    for attempt in range(30):
        try:
            return KafkaConsumer(
                TOPIC,
                bootstrap_servers=KAFKA_BOOTSTRAP,
                group_id=GROUP_ID,
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                auto_offset_reset="earliest",
                enable_auto_commit=True,
            )
        except Exception as e:
            print(f"[{CONSUMER_ID}] waiting for kafka... ({attempt}) {e}")
            time.sleep(3)
    raise RuntimeError("Could not connect to Kafka")


def make_clickhouse():
    for attempt in range(30):
        try:
            return clickhouse_connect.get_client(
                host=CLICKHOUSE_HOST,
                port=8123,
                username=CLICKHOUSE_USER,
                password=CLICKHOUSE_PASSWORD,
            )
        except Exception as e:
            print(f"[{CONSUMER_ID}] waiting for clickhouse... ({attempt}) {e}")
            time.sleep(3)
    raise RuntimeError("Could not connect to ClickHouse")


def main():
    consumer = make_consumer()
    client = make_clickhouse()
    print(f"[{CONSUMER_ID}] consuming '{TOPIC}' -> ClickHouse (group={GROUP_ID})")

    batch = []
    last_flush = time.time()

    def flush():
        nonlocal batch, last_flush
        if not batch:
            return
        rows = [
            [
                datetime.fromisoformat(r["ts"].replace("Z", "+00:00")),
                r["service"],
                r["severity"],
                r["message"],
                r["latency_ms"],
                r["trace_id"],
            ]
            for r in batch
        ]
        client.insert(
            "logs.raw_logs",
            rows,
            column_names=["ts", "service", "severity", "message", "latency_ms", "trace_id"],
        )
        print(f"[{CONSUMER_ID}] flushed {len(batch)} rows")
        batch = []
        last_flush = time.time()

    for message in consumer:
        batch.append(message.value)
        if len(batch) >= BATCH_SIZE or (time.time() - last_flush) > BATCH_TIMEOUT_S:
            flush()


if __name__ == "__main__":
    main()
