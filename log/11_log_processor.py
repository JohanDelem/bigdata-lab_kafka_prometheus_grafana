#!/usr/bin/env python3
"""
Lit le topic 'web-logs', agrège les requêtes par fenêtres de 10s
et publie un résumé dans le topic 'log-stats'.

Métriques produites par fenêtre :
  - total de requêtes
  - répartition des codes HTTP (2xx, 3xx, 4xx, 5xx)
  - top 5 des paths les plus demandés
  - nombre d'IPs uniques
  - volume de données transféré (bytes)
  - détection d'IPs suspectes (> 50 requêtes sur la fenêtre)
"""
import json
import time
from collections import Counter
from datetime import datetime, timezone
from kafka import KafkaConsumer, KafkaProducer

BOOTSTRAP      = "localhost:29092"
SOURCE         = "web-logs"
SINK           = "log-stats"
GROUP          = "log-processor"
WINDOW_SECONDS = 10

consumer = KafkaConsumer(
    SOURCE,
    bootstrap_servers=BOOTSTRAP,
    group_id=GROUP,
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
)

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda k: k.encode("utf-8"),
)

def fresh():
    """Initialise un bucket vide pour une nouvelle fenêtre."""
    return {
        "total":        0,
        "status_codes": Counter(),   # ex: {200: 120, 404: 5}
        "paths":        Counter(),   # ex: {"/produits": 80}
        "ips":          Counter(),   # ex: {"1.2.3.4": 30}
        "bytes_total":  0,
        "methods":      Counter(),   # ex: {"GET": 110, "POST": 10}
    }

def iso(epoch):
    return datetime.fromtimestamp(epoch, tz=timezone.utc).isoformat()

def status_class(code: int) -> str:
    """Retourne la classe du code HTTP : '2xx', '3xx', '4xx', '5xx'."""
    return f"{code // 100}xx"

def flush(window_start, bucket):
    """Construit le résumé de la fenêtre et le publie dans log-stats."""

    # Détection d'IPs suspectes : plus de 50 requêtes sur 10s
    suspicious = [
        ip for ip, count in bucket["ips"].items()
        if count > 50
    ]

    stats = {
        "window_start":   iso(window_start),
        "window_end":     iso(window_start + WINDOW_SECONDS),
        "requests_total": bucket["total"],
        "unique_ips":     len(bucket["ips"]),
        "bytes_total":    bucket["bytes_total"],
        # Répartition par classe de statut HTTP
        "status_classes": {
            status_class(code): count
            for code, count in bucket["status_codes"].items()
        },
        # Top 5 des paths les plus demandés
        "top_paths": dict(bucket["paths"].most_common(5)),
        # Répartition GET / POST / etc.
        "methods": dict(bucket["methods"]),
        # IPs avec activité anormalement élevée
        "suspicious_ips": suspicious,
    }

    producer.send(SINK, key=str(window_start), value=stats)
    producer.flush()

    flag = " !! SUSPICIOUS" if suspicious else ""
    print(
        f"[log-processor] {stats['window_start']} "
        f"-> {stats['requests_total']} req, "
        f"{stats['unique_ips']} IPs, "
        f"{stats['bytes_total'] // 1024} KB"
        f"{flag}"
    )

print(f"[log-processor] {SOURCE} -> agrégation {WINDOW_SECONDS}s -> {SINK}  (Ctrl+C)")

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
                b["status_codes"][e["status"]] += 1
                b["paths"][e["path"]] += 1
                b["ips"][e["ip"]] += 1
                b["bytes_total"] += e.get("bytes", 0)
                b["methods"][e["method"]] += 1

        # Clôture les fenêtres passées
        for ws in sorted(w for w in buckets if w < cur):
            flush(ws, buckets.pop(ws))

except KeyboardInterrupt:
    print("\n[log-processor] arrêt — clôture des fenêtres en cours")
    for ws in sorted(buckets):
        flush(ws, buckets[ws])

finally:
    consumer.close()
    producer.close()
