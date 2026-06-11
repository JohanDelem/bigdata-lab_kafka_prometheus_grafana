#!/usr/bin/env python3
"""
Consomme le topic 'corr-alerts' et affiche les alertes de
corrélation en temps réel avec leur chaîne de preuves.

Ce script simule ce qu'un analyste SOC voit lors d'une
investigation — pas juste une alerte isolée, mais la chaîne
d'événements qui l'a déclenchée.

Usage :
  python siem/22_corr_consumer.py
"""
import json
from kafka import KafkaConsumer
from collections import Counter

BOOTSTRAP = "localhost:29092"
TOPIC     = "corr-alerts"
GROUP     = "soc-investigator"

RED    = "\033[91m"
ORANGE = "\033[93m"
YELLOW = "\033[33m"
RESET  = "\033[0m"
BOLD   = "\033[1m"
DIM    = "\033[2m"

SEVERITY_FORMAT = {
    "CRITICAL": (RED,    "[ CRITICAL ]"),
    "HIGH":     (ORANGE, "[   HIGH   ]"),
    "MEDIUM":   (YELLOW, "[  MEDIUM  ]"),
}

RULE_EXPLANATIONS = {
    "RECON_TO_EXFIL": (
        "Chaîne reconnaissance → exfiltration détectée. "
        "Un scan de vulnérabilités web a précédé un dump de table BDD "
        "dans la même fenêtre de 5 min. Pattern classique d'une attaque en 2 phases."
    ),
    "MULTI_SOURCE_CRITICAL": (
        "Incident multi-vecteur confirmé. "
        "Des alertes CRITICAL simultanées sur le réseau web ET la base de données "
        "indiquent une attaque coordonnée, pas une anomalie isolée."
    ),
    "SUSTAINED_ATTACK": (
        "Attaquant persistant identifié. "
        "Une même IP a déclenché plusieurs règles de détection distinctes "
        "en moins de 3 minutes — comportement cohérent avec un outil automatisé."
    ),
}

def format_evidence(events: list) -> str:
    """Formate la chaîne de preuves de façon lisible."""
    if not events:
        return f"  {DIM}(aucune preuve disponible){RESET}"
    lines = []
    for i, e in enumerate(events[:4], 1):
        ts       = e.get("@timestamp", "")[:19].replace("T", " ")
        src      = e.get("source_type", "?")
        sev      = e.get("severity", "?")
        rule     = e.get("rule", "?")
        ip       = e.get("ip_source", "")
        user     = e.get("user", "")
        who      = ip or user or "?"
        lines.append(
            f"  {DIM}[{i}] {ts} | {src:<8} | {sev:<8} | "
            f"{rule:<25} | {who}{RESET}"
        )
    if len(events) > 4:
        lines.append(f"  {DIM}... et {len(events) - 4} événements supplémentaires{RESET}")
    return "\n".join(lines)

def format_alert(alert: dict) -> str:
    severity = alert.get("severity", "UNKNOWN")
    color, label = SEVERITY_FORMAT.get(severity, (RESET, f"[{severity:^10}]"))
    rule     = alert.get("rule", "N/A")
    ctx      = alert.get("context", {})
    evidence = alert.get("evidence", [])

    lines = [
        "",
        f"{color}{BOLD}{label}{RESET}  {alert.get('@timestamp', '')[:19].replace('T', ' ')}",
        f"  Règle      : {BOLD}{rule}{RESET}",
        f"  Sévérité   : {color}{severity}{RESET}",
        f"  Description: {alert.get('description', '')}",
        "",
        f"  {BOLD}Explication :{RESET}",
        f"  {RULE_EXPLANATIONS.get(rule, 'Règle de corrélation personnalisée.')}",
    ]

    if ctx:
        lines.append("")
        lines.append(f"  {BOLD}Contexte :{RESET}")
        for k, v in ctx.items():
            if isinstance(v, list):
                v_str = ", ".join(str(x) for x in v[:5])
            else:
                v_str = str(v)
            lines.append(f"    {k:<20} : {v_str}")

    if evidence:
        lines.append("")
        lines.append(f"  {BOLD}Chaîne de preuves ({len(evidence)} events) :{RESET}")
        lines.append(format_evidence(evidence))

    lines.append("")
    lines.append("  " + "─" * 58)
    return "\n".join(lines)

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=BOOTSTRAP,
    group_id=GROUP,
    auto_offset_reset="latest",
    enable_auto_commit=True,
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
)

print(f"{BOLD}[SOC] Console investigation — topic : {TOPIC}{RESET}")
print(f"[SOC] En attente d'alertes de corrélation... (Ctrl+C pour arrêter)")
print("═" * 62)

counts = Counter()

try:
    for msg in consumer:
        alert    = msg.value
        severity = alert.get("severity", "UNKNOWN")
        counts[severity] += 1
        print(format_alert(alert))

except KeyboardInterrupt:
    print(f"\n{BOLD}[SOC] Session terminée{RESET}")
    print("═" * 62)
    print(" Alertes de corrélation reçues :")
    for sev, (color, label) in SEVERITY_FORMAT.items():
        print(f"  {color}{label}{RESET} : {counts[sev]}")
    print("═" * 62)

finally:
    consumer.close()
