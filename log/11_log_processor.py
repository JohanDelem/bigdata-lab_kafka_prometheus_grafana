#!/usr/bin/env python3
"""
Lit le topic 'web-logs', agrège les requêtes par fenêtres de 10s
et publie deux flux :
  - log-stats       : résumé de trafic (inchangé)
  - security-alerts : alertes de sécurité détectées

Règles de détection :
  CRITICAL  IP > 50 req/fenêtre                  (brute force / DDoS)
  HIGH      taux 5xx > 20%                        (erreurs serveur anormales)
  HIGH      path suspect détecté                  (scan de vulnérabilités)
  MEDIUM    user-agent outil offensif détecté     (scanner connu)
  MEDIUM    taux 4xx > 30%                        (scan de ressources)
"""
import json
import time
from collections import Counter
from datetime import datetime, timezone
from kafka import KafkaConsumer, KafkaProducer

BOOTSTRAP      = "localhost:29092"
SOURCE         = "web-logs"
SINK_STATS     = "log-stats"
SINK_ALERTS    = "security-alerts"
GROUP          = "log-processor"
WINDOW_SECONDS = 10

# --- Règles de détection ---

# Seuil de requêtes par IP sur une fenêtre de 10s
THRESHOLD_IP_REQUESTS = 50

# Paths connus comme suspects (scans, exploitation)
SUSPICIOUS_PATHS = {
    "/.env", "/etc/passwd", "/wp-admin/", "/admin/",
    "/phpmyadmin/", "/.git/config", "/config.php",
    "/backup.sql", "/wp-login.php", "/login",
}

# User-agents d'outils offensifs connus
MALICIOUS_UAS = {
    "sqlmap", "nikto", "masscan", "zgrab",
    "nmap", "dirbuster", "gobuster", "wfuzz",
}

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
        "total":        0,
        "status_codes": Counter(),
        "paths":        Counter(),
        "ips":          Counter(),
        "bytes_total":  0,
        "methods":      Counter(),
        "uas":          Counter(),
        "suspicious_paths_hits": [],
        "malicious_ua_hits":     [],
    }

def iso(epoch):
    return datetime.fromtimestamp(epoch, tz=timezone.utc).isoformat()

def status_class(code):
    return f"{code // 100}xx"

def detect_alerts(window_start, bucket):
    """
    Applique les règles de détection sur un bucket agrégé.
    Retourne une liste d'alertes (peut être vide).
    """
    alerts = []
    total  = bucket["total"] or 1
    ts     = iso(window_start)
    ts_end = iso(window_start + WINDOW_SECONDS)

    # CRITICAL — IP avec trop de requêtes (brute force / DDoS)
    for ip, count in bucket["ips"].items():
        if count > THRESHOLD_IP_REQUESTS:
            alerts.append({
                "severity":    "CRITICAL",
                "rule":        "HIGH_REQUEST_RATE",
                "description": f"IP {ip} a envoyé {count} requêtes en {WINDOW_SECONDS}s",
                "ip":          ip,
                "count":       count,
                "window_start": ts,
                "window_end":   ts_end,
            })

    # HIGH — Taux d'erreurs 5xx anormal
    errors_5xx = sum(
        v for k, v in bucket["status_codes"].items()
        if 500 <= k < 600
    )
    rate_5xx = errors_5xx / total
    if rate_5xx > 0.20:
        alerts.append({
            "severity":    "HIGH",
            "rule":        "HIGH_5XX_RATE",
            "description": f"Taux d'erreurs 5xx : {rate_5xx:.1%} ({errors_5xx}/{total} req)",
            "rate":        round(rate_5xx, 3),
            "count":       errors_5xx,
            "window_start": ts,
            "window_end":   ts_end,
        })

    # HIGH — Paths suspects détectés
    if bucket["suspicious_paths_hits"]:
        unique_paths = list(set(bucket["suspicious_paths_hits"]))
        alerts.append({
            "severity":    "HIGH",
            "rule":        "SUSPICIOUS_PATH",
            "description": f"Paths suspects détectés : {unique_paths}",
            "paths":       unique_paths,
            "window_start": ts,
            "window_end":   ts_end,
        })

    # MEDIUM — User-agent offensif détecté
    if bucket["malicious_ua_hits"]:
        unique_uas = list(set(bucket["malicious_ua_hits"]))
        alerts.append({
            "severity":    "MEDIUM",
            "rule":        "MALICIOUS_USER_AGENT",
            "description": f"User-agents offensifs : {unique_uas}",
            "user_agents": unique_uas,
            "window_start": ts,
            "window_end":   ts_end,
        })

    # MEDIUM — Taux d'erreurs 4xx anormal (scan de ressources)
    errors_4xx = sum(
        v for k, v in bucket["status_codes"].items()
        if 400 <= k < 500
    )
    rate_4xx = errors_4xx / total
    if rate_4xx > 0.30:
        alerts.append({
            "severity":    "MEDIUM",
            "rule":        "HIGH_4XX_RATE",
            "description": f"Taux d'erreurs 4xx : {rate_4xx:.1%} ({errors_4xx}/{total} req)",
            "rate":        round(rate_4xx, 3),
            "count":       errors_4xx,
            "window_start": ts,
            "window_end":   ts_end,
        })

    return alerts

def flush(window_start, bucket):
    """Publie les stats et les alertes détectées."""

    # --- Publication dans log-stats (inchangé) ---
    suspicious_ips = [
        ip for ip, count in bucket["ips"].items()
        if count > THRESHOLD_IP_REQUESTS
    ]
    stats = {
        "window_start":   iso(window_start),
        "window_end":     iso(window_start + WINDOW_SECONDS),
        "requests_total": bucket["total"],
        "unique_ips":     len(bucket["ips"]),
        "bytes_total":    bucket["bytes_total"],
        "status_classes": {
            status_class(code): count
            for code, count in bucket["status_codes"].items()
        },
        "top_paths":      dict(bucket["paths"].most_common(5)),
        "methods":        dict(bucket["methods"]),
        "suspicious_ips": suspicious_ips,
    }
    producer.send(SINK_STATS, key=str(window_start), value=stats)

    # --- Détection et publication des alertes ---
    alerts = detect_alerts(window_start, bucket)
    for alert in alerts:
        producer.send(SINK_ALERTS, key=alert["severity"], value=alert)
        print(
            f"[processor] ALERTE {alert['severity']:<8} "
            f"{alert['rule']:<25} | {alert['description'][:60]}"
        )

    producer.flush()

    if not alerts:
        print(
            f"[processor] {iso(window_start)} "
            f"-> {bucket['total']} req, "
            f"{len(bucket['ips'])} IPs — OK"
        )

print(f"[processor] {SOURCE} -> {SINK_STATS} + {SINK_ALERTS}  (Ctrl+C)")

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
                b["uas"][e.get("ua", "")] += 1

                # Vérification path suspect
                if e["path"] in SUSPICIOUS_PATHS:
                    b["suspicious_paths_hits"].append(e["path"])

                # Vérification user-agent offensif
                ua_lower = e.get("ua", "").lower()
                for malicious in MALICIOUS_UAS:
                    if malicious in ua_lower:
                        b["malicious_ua_hits"].append(e.get("ua", ""))
                        break

        for ws in sorted(w for w in buckets if w < cur):
            flush(ws, buckets.pop(ws))

except KeyboardInterrupt:
    print("\n[processor] arrêt — clôture des fenêtres en cours")
    for ws in sorted(buckets):
        flush(ws, buckets[ws])
finally:
    consumer.close()
    producer.close()
