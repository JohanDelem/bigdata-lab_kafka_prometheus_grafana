#!/usr/bin/env python3

import json
import time
from collections import Counter
from datetime import datetime, timezone
from kafka import KafkaConsumer, KafkaProducer

BOOTSTRAP = "localhost:29092"
SOURCE = "events"
SINK = "events-stats"
GROUP = "processor"
WINDOW_SECONDS = 10

consumer = KafkaConsumer(
    SOURCE,
    bootstrap_servers=BOOTSTRAP,
    group_id=GROUP,
    auto_offset_reset="latest",
    enable_auto_commit=True,
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
)

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda k: k.encode("utf-8"),
)

def fresh():
    return {
        "total": 0,
        "by_action": Counter(),
        "revenue": 0.0,
        "users": set()
    }

def iso(epoch):
    return datetime.fromtimestamp(epoch, tz=timezone.utc).isoformat()

def flush(window_start, bucket):
    stats = {
        "window_start": iso(window_start),
        "window_end": iso(window_start + WINDOW_SECONDS),
        "events_total": bucket["total"],
        "unique_users": len(bucket["users"]),
        "revenue": round(bucket["revenue"], 2),
        "by_action": dict(bucket["by_action"]),
    }

    producer.send(SINK, key=str(window_start), value=stats)
    producer.flush()

    print(
        f"[processor] fenêtre "
        f"{stats['window_start']} -> "
        f"{stats['events_total']} évts, "
        f"{stats['unique_users']} users, "
        f"{stats['revenue']} € | "
        f"{dict(bucket['by_action'])}"
    )

print(f"[processor] {SOURCE} -> agrégation {WINDOW_SECONDS}s -> {SINK}  (Ctrl+C)")

buckets = {}

try:
    while True:
        records = consumer.poll(timeout_ms=1000)
        now = time.time()
        cur = int(now // WINDOW_SECONDS) * WINDOW_SECONDS

        for _tp, msgs in records.items():
            for m in msgs:
                e = m.value
                b = buckets.setdefault(cur, fresh())
                b["total"] += 1
                b["by_action"][e["action"]] += 1
                b["revenue"] += e.get("price", 0.0)
                b["users"].add(m.key.decode("utf-8") if m.key else "unknown")

        for ws in sorted(w for w in buckets if w < cur):
            flush(ws, buckets.pop(ws))

except KeyboardInterrupt:
    print("\n[processor] arrêt — clôture des fenêtres en cours")
    for ws in sorted(buckets):
        flush(ws, buckets[ws])

finally:
    consumer.close()
    producer.close()