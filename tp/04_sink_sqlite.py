#!/usr/bin/env python3
import json
import os
import sqlite3
from kafka import KafkaConsumer

BOOTSTRAP = "localhost:29092"
TOPIC = "events-stats"
GROUP = "sink-sqlite"
DB_PATH = os.path.join(os.path.dirname(__file__), "tp.db")

conn = sqlite3.connect(DB_PATH)
conn.execute("""
    CREATE TABLE IF NOT EXISTS window_stats (
        window_start TEXT PRIMARY KEY,
        window_end   TEXT,
        events_total INTEGER,
        unique_users INTEGER,
        revenue      REAL
    )
""")
conn.execute("""
    CREATE TABLE IF NOT EXISTS action_counts (
        window_start TEXT,
        action       TEXT,
        count        INTEGER,
        PRIMARY KEY (window_start, action)
    )
""")
conn.commit()

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=BOOTSTRAP,
    group_id=GROUP,
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
)

print(f"[sink] {TOPIC} -> SQLite ({DB_PATH}) (Ctrl+C pour arrêter)")

n = 0
try:
    for msg in consumer:
        s = msg.value
        conn.execute(
            "INSERT OR REPLACE INTO window_stats VALUES (?,?,?,?,?)",
            (s["window_start"], s["window_end"], s["events_total"], s["unique_users"], s["revenue"]),
        )
        for action, count in s["by_action"].items():  # indenté dans la boucle
            conn.execute(
                "INSERT OR REPLACE INTO action_counts VALUES (?,?,?)",
                (s["window_start"], action, count),
            )
        conn.commit()  # indenté dans la boucle
        n += 1
        print(f"[sink] fenêtre stockée : {s['window_start']} ({s['events_total']} évts)")

except KeyboardInterrupt:
    print(f"\n[sink] arrêt — {n} fenêtres stockées")
finally:
    consumer.close()
    conn.close()