"""
Consumes detected anomalies, asks an LLM to produce a short human-readable
summary + suggested investigation steps (clearly labeled as suggestions,
not confirmed diagnoses), and writes the enriched anomaly to ClickHouse.

Isolated as its own service on purpose: LLM calls are slow and can be
rate-limited, so they must never block the detection path.

If ANTHROPIC_API_KEY is not set, falls back to a template-based summary
so the rest of the pipeline still works end-to-end without a key.
"""
import json
import os
import time
import uuid
from datetime import datetime

import clickhouse_connect
from kafka import KafkaConsumer

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
CLICKHOUSE_HOST = os.environ.get("CLICKHOUSE_HOST", "localhost")
CLICKHOUSE_USER = os.environ.get("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.environ.get("CLICKHOUSE_PASSWORD", "")
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
TOPIC = "anomalies"
GROUP_ID = "ai-summary-service-group"

client_anthropic = None
if ANTHROPIC_API_KEY:
    import anthropic
    client_anthropic = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)


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
            print(f"[summarizer] waiting for kafka... ({attempt}) {e}")
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
            print(f"[summarizer] waiting for clickhouse... ({attempt}) {e}")
            time.sleep(3)
    raise RuntimeError("Could not connect to ClickHouse")


def build_prompt(anomaly: dict) -> str:
    samples = "\n".join(f"- {m}" for m in anomaly.get("sample_messages", [])) or "(no sample messages captured)"
    return f"""You are helping a developer triage a production alert.

Service: {anomaly['service']}
Metric: {anomaly['metric']} = {anomaly['value']} (threshold: {anomaly['threshold']})
Window: {anomaly['window_start']} to {anomaly['window_end']}

Sample error log messages from this window:
{samples}

Write:
1. A one-sentence plain-English summary of what happened.
2. 2-4 concrete next steps to investigate, labeled as SUGGESTIONS not confirmed causes.

Keep it under 120 words total. Do not claim a root cause with certainty."""


def summarize(anomaly: dict) -> tuple[str, str]:
    if client_anthropic:
        try:
            resp = client_anthropic.messages.create(
                model="claude-sonnet-4-6",
                max_tokens=300,
                messages=[{"role": "user", "content": build_prompt(anomaly)}],
            )
            text = "".join(b.text for b in resp.content if b.type == "text")
            if "\n\n" in text:
                summary, steps = text.split("\n\n", 1)
            else:
                summary, steps = text, ""
            return summary.strip(), steps.strip()
        except Exception as e:
            print(f"[summarizer] LLM call failed, falling back to template: {e}")

    # Fallback: deterministic template, keeps the pipeline runnable with no API key
    summary = (
        f"{anomaly['service']} error rate hit {anomaly['value']*100:.0f}% "
        f"(threshold {anomaly['threshold']*100:.0f}%) between "
        f"{anomaly['window_start']} and {anomaly['window_end']}."
    )
    steps = (
        "SUGGESTIONS (not confirmed):\n"
        "- Check recent deploys/config changes to this service.\n"
        "- Inspect downstream dependency health and latency.\n"
        "- Review the sample error messages for a common exception type."
    )
    return summary, steps


def main():
    consumer = make_consumer()
    ch = make_clickhouse()
    print(f"[summarizer] watching '{TOPIC}', anthropic_enabled={bool(client_anthropic)}")

    for message in consumer:
        anomaly = message.value
        summary, steps = summarize(anomaly)

        ch.insert(
            "logs.anomalies",
            [[
                anomaly["id"],
                datetime.fromisoformat(anomaly["detected_at"].replace("Z", "+00:00")),
                datetime.fromisoformat(anomaly["window_start"].replace("Z", "+00:00")),
                datetime.fromisoformat(anomaly["window_end"].replace("Z", "+00:00")),
                anomaly["service"],
                anomaly["metric"],
                anomaly["value"],
                anomaly["threshold"],
                summary,
                steps,
            ]],
            column_names=[
                "id", "detected_at", "window_start", "window_end",
                "service", "metric", "value", "threshold", "summary", "suggested_steps",
            ],
        )
        print(f"[summarizer] wrote summary for anomaly {anomaly['id']} ({anomaly['service']})")


if __name__ == "__main__":
    main()
