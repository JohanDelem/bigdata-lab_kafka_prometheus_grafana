#!/usr/bin/env python3
"""
Consomme le topic 'db-alerts' et affiche les alertes
d'exfiltration en temps réel avec mise en forme par sévérité.

Ce script simule ce qu'un analyste DLP (Data Loss Prevention)
ou un RSSI verrait sur son écran de supervision.
"""
import json
from kafka import KafkaConsumer
from collections import Counter

BOOTSTRAP = "localhost:29092"
TOPIC     = "db-alerts"
GROUP     = "db-analyst"

# Codes couleur ANSI
RED    = "\033[91m"
ORANGE = "\033[93m"
YELLOW = "\033[33m"
GREEN  = "\033[92m"
RESET  = "\033[0m"
BOLD   = "\033[1m"

SEVERITY_FORMAT = {
    "CRITICAL": (RED,    "[ CRITICAL ]"),
    "HIGH":     (ORANGE, "[   HIGH   ]"),
    "MEDIUM":   (YELLOW, "[  MEDIUM  ]"),
    "LOW":      (GREEN,  "[   LOW    ]"),
}

# Descriptions pédagogiques des règles
RULE_DESCRIPTIONS = {
    "TABLE_DUMP":        "Dump complet de table sensible (SELECT * sans WHERE)",
    "UNAUTHORIZED_DDL":  "Opération DDL par un compte non autorisé",
    "HIGH_QUERY_VOLUME": "Volume anormal de requêtes — possible scraping/exfiltration",
    "DB_RECON":          "Reconnaissance de la structure BDD — phase de préparation d'attaque",
    "HIGH_ROWS_RETURNED":"Volume anormal de lignes retournées — exfiltration de masse",
    "MULTI_TABLE_ACCESS": "Accès à plusieurs tables sensibles — cartographie des données",
}

def format_alert(alert: dict) -> str:
    severity = alert.get("severity", "UNKNOWN")
    color, label = SEVERITY_FORMAT.get(severity, (RESET, f"[{severity:^10}]"))
    rule = alert.get("rule", "N/A")

    lines = [
        f"{color}{BOLD}{label}{RESET} {alert.get('window_start', '')}",
        f"  Règle       : {rule}",
        f"  Explication : {RULE_DESCRIPTIONS.get(rule, 'N/A')}",
        f"  Détail      : {alert.get('description', 'N/A')}",
    ]

    if "user_db" in alert:
        lines.append(f"  User DB     : {alert['user_db']}")
    if "tables" in alert:
        lines.append(f"  Tables      : {', '.join(alert['tables'])}")
    if "query" in alert:
        lines.append(f"  Requête     : {alert['query'][:80]}")
    if "rows" in alert:
        lines.append(f"  Lignes      : {alert['rows']}")
    if "count" in alert:
        lines.append(f"  Nb requêtes : {alert['count']}")

    lines.append("-" * 65)
    return "\n".join(lines)

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=BOOTSTRAP,
    group_id=GROUP,
    auto_offset_reset="latest",
    enable_auto_commit=True,
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
)

print(f"{BOLD}[DLP] Surveillance active — topic : {TOPIC}{RESET}")
print(f"[DLP] En attente d'alertes... (Ctrl+C pour arrêter)")
print("=" * 65)

counts = Counter()

try:
    for msg in consumer:
        alert    = msg.value
        severity = alert.get("severity", "UNKNOWN")
        counts[severity] += 1
        print(format_alert(alert))

except KeyboardInterrupt:
    print(f"\n{BOLD}[DLP] Session terminée{RESET}")
    print("=" * 65)
    print(" Récapitulatif des alertes reçues :")
    for sev, (color, label) in SEVERITY_FORMAT.items():
        print(f"  {color}{label}{RESET} : {counts[sev]}")
    print("=" * 65)

finally:
    consumer.close()
