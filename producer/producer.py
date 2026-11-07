"""Kafka event producer untuk ``transactions`` topic.

Mengirim satu transaction event setiap 1-2 detik untuk tugas streaming. Pada jadwal tertentu menyisipkan
satu transaction event invalid (amount negatif, amount terlalu besar, 
timestamp invalid, source tidak diketahui, missing field, invalid data type, dan duplicate)
dan event yang telat (LATE events) dengan timestamp telat 5-15 menit.
"""

import argparse
import json
import os
import random
import time
from datetime import datetime, timedelta, timezone

from confluent_kafka import Producer

DEFAULT_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = "transactions"
VALID_SOURCES = ("mobile", "web", "pos")

# Skenario disisipkan pada posisi yang telah ditentukan pada tiap siklus 60 event
CYCLE_LENGTH = 60
SCENARIO_SCHEDULE = {
    8: "negative_amount",
    12: "amount_too_large",
    16: "invalid_timestamp",
    20: "unknown_source",
    24: "duplicate",
    28: "late_5_min",
    34: "late_8_min",
    40: "late_15_min",
    44: "duplicate",
    48: "missing_field",
    52: "invalid_type",
    56: "duplicate",
}
LATE_MINUTES = {"late_5_min": 5, "late_8_min": 8, "late_15_min": 15}


def format_timestamp(moment: datetime) -> str:
    """Format datetime sesuai ISO-8601 UTC: 2025-12-14T09:00:20Z."""
    return moment.strftime("%Y-%m-%dT%H:%M:%SZ")


def build_valid_event(moment: datetime = None) -> dict:
    """Membuat event transaction yang valid."""
    moment = moment or datetime.now(timezone.utc)
    return {
        "user_id": f"U{random.randint(10000, 99999)}",
        "amount": random.randint(10, 500) * 1000,
        "timestamp": format_timestamp(moment),
        "source": random.choice(VALID_SOURCES),
    }


def build_event(scenario: str, last_valid_event: dict) -> dict:
    """Membuat event sesuai skenario ("valid" atau invalid/late case)."""
    if scenario in LATE_MINUTES:
        late_moment = datetime.now(timezone.utc) - timedelta(minutes=LATE_MINUTES[scenario])
        return build_valid_event(late_moment)

    if scenario == "duplicate" and last_valid_event:
        return dict(last_valid_event) 

    event = build_valid_event()
    if scenario == "negative_amount":
        event["amount"] = -50000
    elif scenario == "amount_too_large":
        event["amount"] = 99_999_999_999
    elif scenario == "invalid_timestamp":
        event["timestamp"] = "2025-13-45T99:99:99Z"
    elif scenario == "unknown_source":
        event["source"] = "atm"
    elif scenario == "missing_field":
        del event["user_id"]
    elif scenario == "invalid_type":
        event["amount"] = "seratus ribu"
    return event


def on_delivery(error, message) -> None:
    """Mengirim report callback: hanya log yang gagal."""
    if error is not None:
        print(f"[DELIVERY FAILED] {error}")


def run(bootstrap: str, total_events: int, min_delay: float, max_delay: float) -> None:
    """Membuat events sampai ``total_events`` telah terpenuhi (0 = run selamanya)."""
    producer = Producer({"bootstrap.servers": bootstrap})
    last_valid_event = None
    index = 0

    print(f"Producing to topic '{TOPIC}' on {bootstrap} (Ctrl+C to stop)")
    try:
        while total_events == 0 or index < total_events:
            index += 1
            position = (index - 1) % CYCLE_LENGTH + 1
            scenario = SCENARIO_SCHEDULE.get(position, "valid")

            event = build_event(scenario, last_valid_event)
            if scenario == "valid":
                last_valid_event = event

            producer.produce(
                TOPIC,
                key=str(event.get("user_id", "")),
                value=json.dumps(event),
                callback=on_delivery,
            )
            producer.poll(0)
            print(f"[{index:03d}] {scenario.upper():<18} {json.dumps(event)}")
            time.sleep(random.uniform(min_delay, max_delay))
    except KeyboardInterrupt:
        print("\nStopped by user.")
    finally:
        producer.flush()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Transaction event producer")
    parser.add_argument("--bootstrap", default=DEFAULT_BOOTSTRAP)
    parser.add_argument("--events", type=int, default=60, help="0 = run forever")
    parser.add_argument("--min-delay", type=float, default=1.0)
    parser.add_argument("--max-delay", type=float, default=2.0)
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run(args.bootstrap, args.events, args.min_delay, args.max_delay)
