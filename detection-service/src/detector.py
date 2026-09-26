"""
Reads raw logs, keeps a rolling per-service window in memory, and flags an
anomaly when the error rate in that window crosses a threshold.

Deliberately simple (explainable) rule-based detection to start:
    error_rate = errors_in_window / total_in_window
    flag if error_rate > ERROR_RATE_THRESHOLD and total_in_window >= MIN_EVENTS

This is the natural place to later swap in a statistical/ML model
(e.g. EWMA, seasonal decomposition, Isolation Forest on [error_rate, p95_latency])
without changing anything upstream or downstream - the service still just
reads 'raw-logs' and writes 'anomalies'.
"""
import json
import os
import time
import uuid
from collections import defaultdict, deque
from datetime import datetime, timezone

from kafka import KafkaConsumer, KafkaProducer

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
WINDOW_SECONDS = float(os.environ.get("WINDOW_SECONDS", "30"))
ERROR_RATE_THRESHOLD = float(os.environ.get("ERROR_RATE_THRESHOLD", "0.4"))
MIN_EVENTS_IN_WINDOW = int(os.environ.get("MIN_EVENTS_IN_WINDOW", "10"))

RAW_TOPIC = "raw-logs"
ANOMALY_TOPIC = "anomalies"
GROUP_ID = "detection-service-group"

# service -> deque[(epoch_seconds, severity, latency_ms)]
windows = defaultdict(deque)
# service -> epoch_seconds of last anomaly raised (avoid spamming)
last_alert = defaultdict(float)
COOLDOWN_SECONDS = 60


def make_consumer():
    for attempt in range(30):
        try:
            return KafkaConsumer(
                RAW_TOPIC,
                bootstrap_servers=KAFKA_BOOTSTRAP,
                group_id=GROUP_ID,
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                auto_offset_reset="earliest",
                enable_auto_commit=True,
            )
        except Exception as e:
            print(f"[detector] waiting for kafka... ({attempt}) {e}")
            time.sleep(3)
    raise RuntimeError("Could not connect to Kafka")


def make_producer():
    for attempt in range(30):
        try:
            return KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            )
        except Exception as e:
            print(f"[detector] waiting for kafka producer... ({attempt}) {e}")
            time.sleep(3)
    raise RuntimeError("Could not connect to Kafka")


def prune(service: str, now: float):
    dq = windows[service]
    while dq and (now - dq[0][0]) > WINDOW_SECONDS:
        dq.popleft()


def evaluate(service: str, now: float, producer: KafkaProducer):
    dq = windows[service]
    total = len(dq)
    if total < MIN_EVENTS_IN_WINDOW:
        return

    errors = sum(1 for (_, sev, _) in dq if sev == "ERROR")
    error_rate = errors / total

    if error_rate > ERROR_RATE_THRESHOLD and (now - last_alert[service]) > COOLDOWN_SECONDS:
        window_start = dq[0][0]
        anomaly = {
            "id": uuid.uuid4().hex,
            "detected_at": datetime.now(timezone.utc).isoformat(),
            "window_start": datetime.fromtimestamp(window_start, tz=timezone.utc).isoformat(),
            "window_end": datetime.fromtimestamp(now, tz=timezone.utc).isoformat(),
            "service": service,
            "metric": "error_rate",
            "value": round(error_rate, 3),
            "threshold": ERROR_RATE_THRESHOLD,
            # include recent log snippets so the AI summary service has raw
            # material to reason over, without needing a separate DB query
            "sample_messages": [
                msg for (_, sev, msg) in list(dq)[-15:] if sev == "ERROR"
            ][:8],
        }
        producer.send(ANOMALY_TOPIC, value=anomaly)
        producer.flush()
        last_alert[service] = now
        print(f"[detector] ANOMALY {service}: error_rate={error_rate:.2f} over {total} events")


def main():
    consumer = make_consumer()
    producer = make_producer()
    print(f"[detector] watching '{RAW_TOPIC}' (window={WINDOW_SECONDS}s, threshold={ERROR_RATE_THRESHOLD})")

    for message in consumer:
        log = message.value
        service = log["service"]
        try:
            ts = datetime.fromisoformat(log["ts"]).timestamp()
        except Exception:
            ts = time.time()

        windows[service].append((ts, log["severity"], log["message"]))
        now = time.time()
        prune(service, now)
        evaluate(service, now, producer)


if __name__ == "__main__":
    main()
