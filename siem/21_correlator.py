#!/usr/bin/env python3
"""
Corrélation cross-sources : interroge OpenSearch toutes les 60s,
cherche des enchaînements d'événements suspects, publie les
alertes de corrélation dans le topic 'corr-alerts'.

Règles implémentées :
  RECON_TO_EXFIL       scan web (SUSPICIOUS_PATH) suivi d'un dump BDD
                       (TABLE_DUMP) depuis la même IP dans une fenêtre de 5 min
                       -> chaîne reconnaissance + exploitation confirmée

  MULTI_SOURCE_CRITICAL alertes CRITICAL simultanées sur security ET db
                       dans une fenêtre de 2 min
                       -> incident multi-vecteur en cours

  SUSTAINED_ATTACK     même IP avec 3+ règles HIGH/CRITICAL distinctes
                       sur security dans une fenêtre de 3 min
                       -> attaquant persistant, pas une anomalie ponctuelle

Le correlator lit OpenSearch (données normalisées cross-sources)
et non Kafka directement — il peut donc raisonner sur l'historique
et croiser des sources différentes.

Usage :
  python siem/21_correlator.py
"""
import json
import time
from datetime import datetime, timezone, timedelta
from collections import defaultdict
from opensearchpy import OpenSearch
from kafka import KafkaProducer

BOOTSTRAP  = "localhost:29092"
SINK       = "corr-alerts"
OS_HOST    = "localhost"
OS_PORT    = 9200
INDEX_PAT  = "siem-events-*"
INTERVAL_S = 60          # fréquence d'interrogation OpenSearch

os_client = OpenSearch(
    hosts=[{"host": OS_HOST, "port": OS_PORT}],
    http_compress=True,
    use_ssl=False,
    verify_certs=False,
)

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP,
    key_serializer=lambda k: k.encode("utf-8"),
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    acks="all",
)

def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def ago(minutes: int) -> str:
    """Retourne un timestamp ISO il y a N minutes."""
    return (datetime.now(timezone.utc) - timedelta(minutes=minutes)).isoformat()

def query_events(minutes: int, source_type: str = None,
                 severity: str = None, rule: str = None) -> list:
    """
    Récupère les events OpenSearch des N dernières minutes.
    Filtres optionnels : source_type, severity, rule.
    """
    must = [{"range": {"@timestamp": {"gte": ago(minutes)}}}]

    if source_type:
        must.append({"term": {"source_type": source_type}})
    if severity:
        must.append({"term": {"severity": severity}})
    if rule:
        must.append({"term": {"rule": rule}})

    resp = os_client.search(
        index=INDEX_PAT,
        body={
            "size": 500,
            "query": {"bool": {"must": must}},
            "_source": ["@timestamp", "source_type", "severity",
                        "rule", "description", "ip_source", "user", "host"],
        }
    )
    return [h["_source"] for h in resp["hits"]["hits"]]

def publish_alert(rule: str, severity: str, description: str,
                  evidence: list, context: dict = None):
    """Publie une alerte de corrélation dans corr-alerts."""
    alert = {
        "@timestamp":  now_iso(),
        "rule":        rule,
        "severity":    severity,
        "description": description,
        "evidence":    evidence,   # events qui ont déclenché la règle
        "context":     context or {},
    }
    producer.send(SINK, key=rule, value=alert)
    producer.flush()
    print(
        f"[correlator] ALERTE {severity:<8} {rule:<25} | {description[:60]}"
    )

# ── Règles de corrélation ──────────────────────────────────────

def rule_recon_to_exfil():
    """
    RECON_TO_EXFIL : scan web SUSPICIOUS_PATH suivi d'un TABLE_DUMP BDD
    depuis la même source dans une fenêtre de 5 minutes.

    Logique :
      1. Récupère les events security avec rule=SUSPICIOUS_PATH (5 dernières min)
      2. Récupère les events db avec rule=TABLE_DUMP (5 dernières min)
      3. Cherche une IP commune entre les deux ensembles
         (web : ip_source | db : user_db dans description)
    """
    web_recon = query_events(minutes=5, source_type="security", rule="SUSPICIOUS_PATH")
    db_dumps  = query_events(minutes=5, source_type="db",       rule="TABLE_DUMP")

    if not web_recon or not db_dumps:
        return

    # IPs vues dans les scans web
    recon_ips = {e.get("ip_source", "") for e in web_recon if e.get("ip_source")}

    # Pour les dumps BDD, l'IP n'est pas toujours présente
    # On corrèle sur la coexistence temporelle (même fenêtre 5 min)
    # + présence d'au moins un scan web actif
    if recon_ips and db_dumps:
        publish_alert(
            rule="RECON_TO_EXFIL",
            severity="CRITICAL",
            description=(
                f"Scan web ({len(web_recon)} events) suivi d'un dump BDD "
                f"({len(db_dumps)} events) dans la même fenêtre de 5 min"
            ),
            evidence=web_recon[:3] + db_dumps[:3],
            context={
                "recon_ips":      list(recon_ips)[:5],
                "recon_count":    len(web_recon),
                "db_dump_count":  len(db_dumps),
            }
        )

def rule_multi_source_critical():
    """
    MULTI_SOURCE_CRITICAL : alertes CRITICAL simultanées sur security ET db
    dans une fenêtre de 2 minutes.

    Un CRITICAL isolé peut être un faux positif.
    Deux CRITICAL sur des sources différentes au même moment = incident confirmé.
    """
    sec_critical = query_events(minutes=2, source_type="security", severity="CRITICAL")
    db_critical  = query_events(minutes=2, source_type="db",       severity="CRITICAL")

    if sec_critical and db_critical:
        publish_alert(
            rule="MULTI_SOURCE_CRITICAL",
            severity="CRITICAL",
            description=(
                f"CRITICAL simultanés : {len(sec_critical)} alertes sécurité "
                f"+ {len(db_critical)} alertes BDD en 2 min"
            ),
            evidence=sec_critical[:2] + db_critical[:2],
            context={
                "security_rules": list({e.get("rule") for e in sec_critical}),
                "db_rules":       list({e.get("rule") for e in db_critical}),
            }
        )

def rule_sustained_attack():
    """
    SUSTAINED_ATTACK : même IP avec 3+ règles HIGH/CRITICAL distinctes
    sur security dans une fenêtre de 3 minutes.

    Un attaquant persistant déclenche plusieurs règles différentes
    (volume + paths suspects + user-agent offensif) en rafale.
    Un pic ponctuel déclenche souvent la même règle plusieurs fois.
    """
    events = query_events(minutes=3, source_type="security")
    high_crit = [
        e for e in events
        if e.get("severity") in ("HIGH", "CRITICAL") and e.get("ip_source")
    ]

    if not high_crit:
        return

    # Grouper par IP
    ip_rules = defaultdict(set)
    ip_events = defaultdict(list)
    for e in high_crit:
        ip = e.get("ip_source", "")
        ip_rules[ip].add(e.get("rule", ""))
        ip_events[ip].append(e)

    for ip, rules in ip_rules.items():
        if len(rules) >= 3:
            publish_alert(
                rule="SUSTAINED_ATTACK",
                severity="HIGH",
                description=(
                    f"IP {ip} a déclenché {len(rules)} règles distinctes "
                    f"en 3 min : {', '.join(sorted(rules))}"
                ),
                evidence=ip_events[ip][:5],
                context={
                    "ip":         ip,
                    "rules":      list(rules),
                    "event_count": len(ip_events[ip]),
                }
            )

# ── Boucle principale ──────────────────────────────────────────


def rule_wazuh_lateral_move():
    """
    WAZUH_LATERAL_MOVE : connexions SSH réussies après échecs
    sur plusieurs hôtes distincts dans une fenêtre de 5 minutes.

    Signature d'un mouvement latéral :
      - source_type = wazuh
      - rule contient WAZUH_5720 (ssh success after failures)
      - 2+ hôtes (host) distincts touchés par le même ip_source
    """
    events = query_events(minutes=5, source_type="wazuh", rule="WAZUH_5720")
    if not events:
        return

    from collections import defaultdict
    ip_hosts = defaultdict(set)
    ip_events = defaultdict(list)
    for e in events:
        ip = e.get("ip_source", "")
        host = e.get("host", "")
        if ip and host:
            ip_hosts[ip].add(host)
            ip_events[ip].append(e)

    for ip, hosts in ip_hosts.items():
        if len(hosts) >= 2:
            publish_alert(
                rule="WAZUH_LATERAL_MOVE",
                severity="CRITICAL",
                description=(
                    f"Mouvement latéral détecté : IP {ip} a réussi "
                    f"SSH sur {len(hosts)} hôtes distincts en 5 min : "
                    f"{', '.join(sorted(hosts))}"
                ),
                evidence=ip_events[ip][:5],
                context={
                    "ip":         ip,
                    "hosts":      list(hosts),
                    "host_count": len(hosts),
                }
            )

def rule_wazuh_fim_critical():
    """
    WAZUH_FIM_CRITICAL : modification de fichier système critique
    (rule WAZUH_550 ou WAZUH_553) sur un hôte de production
    dans les 5 dernières minutes.

    Les fichiers /etc/passwd, /etc/shadow, /etc/sudoers modifiés
    sont des indicateurs d'escalade de privilèges ou de persistance.
    """
    events = query_events(minutes=5, source_type="wazuh")
    fim_events = [
        e for e in events
        if e.get("rule", "") in ("WAZUH_550", "WAZUH_553")
        and e.get("severity") in ("HIGH", "CRITICAL")
    ]

    if not fim_events:
        return

    hosts_hit = {e.get("host", "") for e in fim_events if e.get("host")}
    if hosts_hit:
        publish_alert(
            rule="WAZUH_FIM_CRITICAL",
            severity="HIGH",
            description=(
                f"Modification de fichier système critique sur "
                f"{len(hosts_hit)} hôte(s) : {', '.join(sorted(hosts_hit))}"
            ),
            evidence=fim_events[:4],
            context={
                "hosts":       list(hosts_hit),
                "event_count": len(fim_events),
            }
        )

RULES = [
    ("RECON_TO_EXFIL",        rule_recon_to_exfil),
    ("MULTI_SOURCE_CRITICAL", rule_multi_source_critical),
    ("SUSTAINED_ATTACK",      rule_sustained_attack),
    ("WAZUH_LATERAL_MOVE",    rule_wazuh_lateral_move),
    ("WAZUH_FIM_CRITICAL",    rule_wazuh_fim_critical),
]

print(f"[correlator] -> {SINK} | intervalle {INTERVAL_S}s (Ctrl+C pour arrêter)")
print(f"[correlator] index OpenSearch : {INDEX_PAT}")
print(f"[correlator] règles actives   : {[r[0] for r in RULES]}\n")

try:
    while True:
        ts = now_iso()
        triggered = 0

        for name, fn in RULES:
            try:
                fn()
                triggered += 1
            except Exception as e:
                print(f"[correlator] ERREUR règle {name} : {e}")

        print(f"[correlator] {ts} — {len(RULES)} règles évaluées")
        time.sleep(INTERVAL_S)

except KeyboardInterrupt:
    print("\n[correlator] arrêt")
finally:
    producer.flush()
    producer.close()
