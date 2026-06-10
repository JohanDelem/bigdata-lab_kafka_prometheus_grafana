#!/usr/bin/env python3
"""
Consomme le topic 'log-stats' et affiche un rapport d'analyse
des logs web : trafic, erreurs, top pages, IPs suspectes.
Ce script lit TOUS les messages depuis le début (auto_offset_reset="earliest")
et s'arrête quand il n'y a plus rien à lire (timeout).
"""
import json
from kafka import KafkaConsumer
from collections import Counter, defaultdict

BOOTSTRAP = "localhost:29092"
TOPIC     = "log-stats"
GROUP     = "log-query"

def line(c="-"):
    print(c * 60)

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=BOOTSTRAP,
    group_id=GROUP,
    auto_offset_reset="earliest",
    enable_auto_commit=False,
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    # S'arrête automatiquement après 3s sans nouveau message
    consumer_timeout_ms=3000,
)

# Accumulation de toutes les fenêtres
windows        = []
total_requests = 0
total_bytes    = 0
all_paths      = Counter()
all_statuses   = Counter()
all_methods    = Counter()
all_suspicious = set()

print("[log-query] lecture du topic log-stats...")

for msg in consumer:
    s = msg.value
    windows.append(s)
    total_requests += s.get("requests_total", 0)
    total_bytes    += s.get("bytes_total", 0)

    for path, count in s.get("top_paths", {}).items():
        all_paths[path] += count

    for cls, count in s.get("status_classes", {}).items():
        all_statuses[cls] += count

    for method, count in s.get("methods", {}).items():
        all_methods[method] += count

    for ip in s.get("suspicious_ips", []):
        all_suspicious.add(ip)

consumer.close()

if not windows:
    raise SystemExit("Aucune donnée — lancez d'abord 10_log_producer.py et 11_log_processor.py")

# --- RAPPORT ---

line("=")
print(" SYNTHESE GLOBALE")
line("=")
print(f"  Fenetres analysees  : {len(windows)}")
print(f"  Requetes totales    : {total_requests}")
print(f"  Volume transfere    : {total_bytes / (1024*1024):.2f} MB")
print(f"  IPs suspectes       : {len(all_suspicious)}")

print("\n REPARTITION DES CODES HTTP")
line()
grand = sum(all_statuses.values()) or 1
for cls in sorted(all_statuses):
    count = all_statuses[cls]
    pct   = 100 * count / grand
    bar   = "#" * int(pct / 2)
    print(f"  {cls:<6} {count:>6}  {pct:5.1f}%  {bar}")

print("\n METHODES HTTP")
line()
for method, count in all_methods.most_common():
    pct = 100 * count / grand
    print(f"  {method:<8} {count:>6}  {pct:5.1f}%")

print("\n TOP 10 PAGES LES PLUS DEMANDEES")
line()
print(f"  {'path':<30} {'requetes':>8}")
for path, count in all_paths.most_common(10):
    print(f"  {path:<30} {count:>8}")

print("\n TRAFIC PAR FENETRE (10 dernieres)")
line()
print(f"  {'fenetre (debut)':<28} {'req':>5} {'IPs':>5} {'KB':>8} {'suspect':>8}")
for s in windows[-10:]:
    flag = "OUI" if s.get("suspicious_ips") else "-"
    print(
        f"  {s['window_start']:<28} "
        f"{s['requests_total']:>5} "
        f"{s['unique_ips']:>5} "
        f"{s['bytes_total']//1024:>8} "
        f"{flag:>8}"
    )

if all_suspicious:
    print("\n IPS SUSPECTES (> 50 req / fenetre de 10s)")
    line()
    for ip in sorted(all_suspicious):
        print(f"  {ip}")

line("=")
