#!/usr/bin/env python3
import json
import os
from kafka import KafkaConsumer

BOOTSTRAP = "localhost:29092"
TOPIC = "events"
GROUP = "viewer"
consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=BOOTSTRAP,
    group_id=GROUP,
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
)
me = os.getpid()
print(f"[consumer {me}] group={GROUP} <- {TOPIC}  (Ctrl+C pour arrêter)")
n = 0
try:
    for msg in consumer:
        e = msg.value
        n += 1
        if n % 50 == 0:
            print(
                f"[consumer {me}] "
                f"p{msg.partition} off={msg.offset} "
                f"| {e['action']:<11} "
                f"{e['page']:<14} "
                f"{msg.key.decode('utf-8') if msg.key else 'unknown'}"
            )
except Exception as e:
    print(f"erreur: {e}")
except KeyboardInterrupt:

    print(
        f"\n[consumer {me}] "
        f"arrêt — {n} messages consommés"
    )
finally:
    consumer.close()