#!/usr/bin/env python3
import json
import random
import time
import uuid

from datetime import datetime, timezone
from kafka import KafkaProducer

BOOTSTRAP = "localhost:29092"
TOPIC = "events"

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP,
    key_serializer=lambda k: k.encode("utf-8"),
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    linger_ms=20,
    acks="all",
)

PAGES = [
    "/",
    "/produits",
    "/produit/123",
    "/panier",
    "/checkout",
    "/blog",
    "/contact"
]

ACTIONS = [
    "view",
    "click",
    "scroll",
    "add_to_cart",
    "purchase"
]

WEIGHTS = [50, 25, 12, 8, 5]

def make_event():
    user = f"user-{random.randint(1, 200)}"
    action = random.choices(
        ACTIONS,
        weights=WEIGHTS,
        k=1
    )[0]

    if action == "purchase":
        price = round(random.uniform(5, 200), 2)
    else:
        price = 0.0

    event = {
        "event_id": str(uuid.uuid4()),
        "session_id": f"sess-{random.randint(1, 500)}",
        "page": random.choice(PAGES),
        "action": action,
        "price": price,
        "ts": datetime.now(timezone.utc).isoformat(),
    }

    return user, event

print(
    f"[producer] -> {TOPIC} sur {BOOTSTRAP} "
    "(Ctrl+C pour arrêter)"
)
n = 0
try:
    while True:
        key, event = make_event()
        producer.send(
            TOPIC,
            key=key,
            value=event
        )
        n += 1
        if n % 200 == 0:
            print(f"[producer] {n} événements envoyés")
        time.sleep(random.uniform(0.01, 0.06))
except KeyboardInterrupt:

    print(
        f"\n[producer] arrêt — {n} événements au total"
    )
finally:
    producer.flush()
    producer.close()