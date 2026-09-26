"""
Simulates one or more applications emitting logs continuously.
Occasionally injects an "incident" that spikes error rate or latency,
so the detection service has something real to catch.
"""
import json
import os
import random
import socket
import time
import uuid
from datetime import datetime, timezone

from faker import Faker
from kafka import KafkaProducer

fake = Faker()

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
LOGS_PER_SECOND = float(os.environ.get("LOGS_PER_SECOND", "5"))
TOPIC = "raw-logs"

SERVICES = ["payments-api", "auth-service", "checkout-service", "inventory-api"]
SEVERITIES_NORMAL = ["INFO"] * 8 + ["WARN"] * 2 + ["ERROR"] * 1  # ~9% error baseline

# Each producer replica gets its own identity so you can see partitioned load
PRODUCER_ID = f"{socket.gethostname()}-{uuid.uuid4().hex[:6]}"


def make_producer():
    for attempt in range(30):
        try:
            return KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                linger_ms=50,
            )
        except Exception as e:
            print(f"[{PRODUCER_ID}] waiting for kafka... ({attempt}) {e}")
            time.sleep(3)
    raise RuntimeError("Could not connect to Kafka")


def build_log(service: str, incident: bool) -> dict:
    if incident:
        severity = random.choices(["INFO", "WARN", "ERROR"], weights=[1, 2, 7])[0]
        latency = random.uniform(800, 4000)  # degraded latency during incident
    else:
        severity = random.choice(SEVERITIES_NORMAL)
        latency = random.uniform(20, 250)

    if severity == "ERROR":
        message = random.choice(
            [
                f"Unhandled exception in {service}: {fake.sentence()}",
                f"Downstream timeout calling {random.choice(SERVICES)}",
                f"5xx returned to client after {latency:.0f}ms",
                f"Database connection pool exhausted for {service}",
            ]
        )
    elif severity == "WARN":
        message = f"Slow response ({latency:.0f}ms) in {service}"
    else:
        message = f"Request handled successfully by {service}"

    return {
        "ts": datetime.now(timezone.utc).isoformat(),
        "service": service,
        "severity": severity,
        "message": message,
        "latency_ms": round(latency, 2),
        "trace_id": uuid.uuid4().hex,
    }


def main():
    producer = make_producer()
    print(f"[{PRODUCER_ID}] connected to Kafka at {KAFKA_BOOTSTRAP}, streaming to '{TOPIC}'")

    incident_service = None
    incident_until = 0

    while True:
        now = time.time()

        # Randomly start an incident on a service ~ every couple of minutes
        if incident_service is None and random.random() < 0.01:
            incident_service = random.choice(SERVICES)
            incident_until = now + random.uniform(20, 45)
            print(f"[{PRODUCER_ID}] !! injecting incident on {incident_service} !!")

        if incident_service and now > incident_until:
            incident_service = None

        service = random.choice(SERVICES)
        incident = service == incident_service

        log = build_log(service, incident)
        producer.send(TOPIC, value=log)

        time.sleep(1.0 / max(LOGS_PER_SECOND, 0.1))


if __name__ == "__main__":
    main()
