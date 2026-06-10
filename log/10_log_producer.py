#!/usr/bin/env python3
"""
Lit log/access.log ligne par ligne et publie chaque requête HTTP
dans le topic Kafka 'web-logs' au format JSON.

Format Apache Combined Log attendu :
  IP - - [date] "METHODE /path HTTP/x.x" status bytes "-" "user-agent"

Clé Kafka : adresse IP (pour que les requêtes d'un même IP
            atterrissent toujours sur la même partition)
"""
import json
import os
import re
import time

from kafka import KafkaProducer

BOOTSTRAP = "localhost:29092"
TOPIC     = "web-logs"

# Chemin du fichier de log : relatif à l'emplacement du script
LOG_PATH = os.path.join(os.path.dirname(__file__), "access.log")

# Expression régulière pour parser une ligne Apache Combined Log
# Exemple :
# 85.50.73.127 - - [09/Jun/2026:08:00:01 +0000] "GET /produits HTTP/1.1" 200 39971 "-" "Mozilla/5.0"
LOG_PATTERN = re.compile(
    r'(?P<ip>\S+)'           # adresse IP
    r' \S+ \S+ '             # ident et authuser (ignorés)
    r'\[(?P<ts>[^\]]+)\]'    # timestamp entre crochets
    r' "(?P<method>\S+)'     # méthode HTTP
    r' (?P<path>\S+)'        # chemin de la requête
    r' \S+" '                # version HTTP (ignorée)
    r'(?P<status>\d{3})'     # code de statut
    r' (?P<bytes>\S+)'       # taille de la réponse
    r' "[^"]*"'              # referer (ignoré)
    r' "(?P<ua>[^"]*)"'      # user-agent
)

def parse_line(line: str) -> dict | None:
    """
    Parse une ligne de log Apache.
    Retourne un dict JSON-sérialisable ou None si la ligne est invalide.
    """
    m = LOG_PATTERN.match(line.strip())
    if not m:
        return None

    return {
        "ip":     m.group("ip"),
        "ts":     m.group("ts"),
        "method": m.group("method"),
        "path":   m.group("path"),
        "status": int(m.group("status")),
        # bytes peut être "-" pour les réponses vides (ex: 304)
        "bytes":  int(m.group("bytes")) if m.group("bytes") != "-" else 0,
        "ua":     m.group("ua"),
    }

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP,
    key_serializer=lambda k: k.encode("utf-8"),
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    linger_ms=20,
    acks="all",
)

if not os.path.exists(LOG_PATH):
    raise SystemExit(f"Fichier introuvable : {LOG_PATH}")

print(f"[log-producer] {LOG_PATH} -> {TOPIC} sur {BOOTSTRAP}")

sent = skipped = 0

with open(LOG_PATH, encoding="utf-8") as f:
    for raw in f:
        event = parse_line(raw)
        if event is None:
            skipped += 1
            continue

        # La clé est l'IP : garantit que toutes les requêtes
        # d'un même IP vont sur la même partition (localité)
        producer.send(TOPIC, key=event["ip"], value=event)
        sent += 1

        # Simule un flux en temps réel : 1000 lignes/s environ
        if sent % 100 == 0:
            print(f"[log-producer] {sent} lignes envoyées...")
            time.sleep(0.1)

producer.flush()
producer.close()

print(f"[log-producer] terminé — {sent} envoyées, {skipped} ignorées")
