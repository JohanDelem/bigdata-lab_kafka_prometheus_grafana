#!/usr/bin/env python3
"""
Consomme le topic 'security-alerts' et affiche les alertes
en temps réel avec mise en forme par niveau de sévérité.

Ce script simule ce qu'un analyste SOC verrait sur son écran
de supervision — un flux d'alertes classées par criticité.
"""
import json
from datetime import datetime, timezone
from kafka import KafkaConsumer

BOOTSTRAP = "localhost:29092"
TOPIC     = "security-alerts"
GROUP     = "soc-analyst"

# Codes couleur ANSI pour le terminal
RED     = "\033[91m"
ORANGE  = "\033[93m"
YELLOW  = "\033[33m"
GREEN   = "\033[92m"
RESET   = "\033[0m"
BOLD    = "\033[1m"

SEVERITY_FORMAT = {
    "CRITICAL": (RED,    "[ CRITICAL ]"),
    "HIGH":     (ORANGE, "[   HIGH   ]"),
    "MEDIUM":   (YELLOW, "[  MEDIUM  ]"),
    "LOW":      (GREEN,  "[   LOW   ]"),
}

def format_alert(alert: dict) -> str:
    """Formate une alerte pour affichage terminal."""
    severity = alert.get("severity", "UNKNOWN")
    color, label = SEVERITY_FORMAT.get(severity, (RESET, f"[{severity:^10}]"))

    lines = [
        f"{color}{BOLD}{label}{RESET} "
        f"{alert.get('window_start', '')}",
        f"  Règle       : {alert.get('rule', 'N/A')}",
        f"  Description : {alert.get('description', 'N/A')}",
    ]

    # Détails supplémentaires selon le type d'alerte
    if "ip" in alert:
        lines.append(f"  IP source   : {alert['ip']} ({alert.get('count', '?')} req)")
    if "paths" in alert:
        lines.append(f"  Paths       : {', '.join(alert['paths'])}")
    if "user_agents" in alert:
        lines.append(f"  User-agents : {', '.join(alert['user_agents'])}")
    if "rate" in alert:
        lines.append(f"  Taux        : {alert['rate']:.1%}")

    lines.append("-" * 60)
    return "\n".join(lines)

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=BOOTSTRAP,
    group_id=GROUP,
    auto_offset_reset="latest",
    enable_auto_commit=True,
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
)

print(f"{BOLD}[SOC] Surveillance active sur {TOPIC}{RESET}")
print(f"[SOC] En attente d'alertes... (Ctrl+C pour arrêter)")
print("=" * 60)

counts = {"CRITICAL": 0, "HIGH": 0, "MEDIUM": 0, "LOW": 0}

try:
    for msg in consumer:
        alert = msg.value
        severity = alert.get("severity", "UNKNOWN")

        if severity in counts:
            counts[severity] += 1

        print(format_alert(alert))

except KeyboardInterrupt:
    print(f"\n{BOLD}[SOC] Session terminée{RESET}")
    print("=" * 60)
    print(" Récapitulatif des alertes reçues :")
    for sev, color_label in SEVERITY_FORMAT.items():
        color, label = color_label
        print(f"  {color}{label}{RESET} : {counts[sev]}")
    print("=" * 60)

finally:
    consumer.close()
