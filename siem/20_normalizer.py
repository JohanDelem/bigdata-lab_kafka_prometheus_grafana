#!/usr/bin/env python3
"""
Consomme les topics security-alerts, db-alerts et web-logs,
normalise chaque événement vers un schéma SIEM commun,
et indexe dans OpenSearch (index siem-events-YYYY.MM.DD).

Schéma commun :
  @timestamp    datetime ISO 8601
  source_type   "web" | "security" | "db"
  severity      CRITICAL / HIGH / MEDIUM / LOW / INFO
  rule          règle déclenchée
  description   texte lisible
  ip_source     IP concernée
  user          utilisateur concerné
  host          hôte concerné
  tags          labels libres
  raw           event original complet

Usage :
  python siem/20_normalizer.py
"""
import json
from datetime import datetime, timezone
from opensearchpy import OpenSearch, helpers
from kafka import KafkaConsumer

BOOTSTRAP   = "localhost:29092"
TOPICS      = ["security-alerts", "db-alerts", "web-logs", "wazuh-alerts"]
GROUP       = "siem-normalizer"
OS_HOST     = "localhost"
OS_PORT     = 9200

os_client = OpenSearch(
    hosts=[{"host": OS_HOST, "port": OS_PORT}],
    http_compress=True,
    use_ssl=False,
    verify_certs=False,
)

def index_name() -> str:
    """Index du jour : siem-events-YYYY.MM.DD"""
    return f"siem-events-{datetime.now(timezone.utc).strftime('%Y.%m.%d')}"

def ensure_index(name: str):
    """Crée l'index avec le bon mapping si absent."""
    if os_client.indices.exists(index=name):
        return
    os_client.indices.create(index=name, body={
        "settings": {"number_of_shards": 1, "number_of_replicas": 0},
        "mappings": {
            "properties": {
                "@timestamp":  {"type": "date"},
                "source_type": {"type": "keyword"},
                "severity":    {"type": "keyword"},
                "rule":        {"type": "keyword"},
                "description": {"type": "text"},
                "ip_source":   {"type": "ip",      "ignore_malformed": True},
                "user":        {"type": "keyword"},
                "host":        {"type": "keyword"},
                "tags":        {"type": "keyword"},
                "raw":         {"type": "object", "enabled": False},
            }
        }
    })
    print(f"[normalizer] index créé : {name}")

# ── Fonctions de normalisation par source ──────────────────────

def from_security_alerts(raw: dict) -> dict:
    """
    Format source (11_log_processor.py) :
      severity, rule, description, ip, window_start, ...
    """
    return {
        "@timestamp":  raw.get("window_start", datetime.now(timezone.utc).isoformat()),
        "source_type": "security",
        "severity":    raw.get("severity", "MEDIUM"),
        "rule":        raw.get("rule", "UNKNOWN"),
        "description": raw.get("description", ""),
        "ip_source":   raw.get("ip", ""),
        "user":        "",
        "host":        "",
        "tags":        ["web", "soc"],
        "raw":         raw,
    }

def from_db_alerts(raw: dict) -> dict:
    """
    Format source (18_db_monitor.py) :
      severity, rule, description, user_db, tables, window_start, ...
    """
    tables = raw.get("tables", [])
    return {
        "@timestamp":  raw.get("window_start", datetime.now(timezone.utc).isoformat()),
        "source_type": "db",
        "severity":    raw.get("severity", "MEDIUM"),
        "rule":        raw.get("rule", "UNKNOWN"),
        "description": raw.get("description", ""),
        "ip_source":   "",
        "user":        raw.get("user_db", ""),
        "host":        "postgres-lab",
        "tags":        ["database", "dlp"] + tables,
        "raw":         raw,
    }

def from_web_logs(raw: dict) -> dict:
    """
    Format source (13_live_generator.py / 10_log_producer.py) :
      ip, ts, method, path, status, bytes, ua
    Seuls les events avec status >= 400 sont indexés comme alertes INFO.
    Les requêtes normales (2xx/3xx) sont ignorées pour ne pas saturer l'index.
    """
    status = raw.get("status", 200)
    if status < 400:
        return None   # on n'indexe pas le trafic normal

    severity = "HIGH" if status >= 500 else "MEDIUM" if status >= 400 else "INFO"
    return {
        "@timestamp":  datetime.now(timezone.utc).isoformat(),
        "source_type": "web",
        "severity":    severity,
        "rule":        f"HTTP_{status}",
        "description": f"{raw.get('method')} {raw.get('path')} -> {status}",
        "ip_source":   raw.get("ip", ""),
        "user":        "",
        "host":        "",
        "tags":        ["web", "http", raw.get("method", "").lower()],
        "raw":         raw,
    }


def from_wazuh_alerts(raw: dict) -> dict:
    """
    Format source (24_wazuh_simulator.py / vrai agent Wazuh) :
      timestamp, rule.id, rule.description, rule.level,
      rule.groups, agent.name, agent.ip, data.srcip, data.dstuser

    Mapping rule.level -> severity :
      1-3   INFO
      4-6   MEDIUM
      7-9   HIGH
      10-15 CRITICAL
    """
    rule    = raw.get("rule", {})
    agent   = raw.get("agent", {})
    data    = raw.get("data", {})
    level   = rule.get("level", 1)
    groups  = rule.get("groups", [])

    if level >= 10:
        severity = "CRITICAL"
    elif level >= 7:
        severity = "HIGH"
    elif level >= 4:
        severity = "MEDIUM"
    else:
        severity = "INFO"

    return {
        "@timestamp":  raw.get("timestamp", datetime.now(timezone.utc).isoformat()),
        "source_type": "wazuh",
        "severity":    severity,
        "rule":        f"WAZUH_{rule.get('id', 'UNKNOWN')}",
        "description": rule.get("description", ""),
        "ip_source":   data.get("srcip", agent.get("ip", "")),
        "user":        data.get("dstuser", ""),
        "host":        agent.get("name", ""),
        "tags":        ["wazuh", "hids"] + groups,
        "raw":         raw,
    }

NORMALIZERS = {
    "security-alerts": from_security_alerts,
    "db-alerts":       from_db_alerts,
    "web-logs":        from_web_logs,
    "wazuh-alerts":    from_wazuh_alerts,
}

# ── Consumer ───────────────────────────────────────────────────

consumer = KafkaConsumer(
    *TOPICS,
    bootstrap_servers=BOOTSTRAP,
    group_id=GROUP,
    auto_offset_reset="latest",
    enable_auto_commit=True,
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
)

print(f"[normalizer] topics : {TOPICS}")
print(f"[normalizer] -> OpenSearch {OS_HOST}:{OS_PORT}  (Ctrl+C pour arrêter)\n")

indexed = skipped = errors = 0
current_index = index_name()
ensure_index(current_index)

try:
    for msg in consumer:
        topic    = msg.topic
        raw_data = msg.value

        # Rotation de l'index à minuit
        today = index_name()
        if today != current_index:
            current_index = today
            ensure_index(current_index)

        normalize = NORMALIZERS.get(topic)
        if normalize is None:
            skipped += 1
            continue

        doc = normalize(raw_data)
        if doc is None:
            skipped += 1
            continue

        try:
            os_client.index(index=current_index, body=doc)
            indexed += 1

            if indexed % 50 == 0:
                print(
                    f"[normalizer] {indexed} indexés | "
                    f"{skipped} ignorés | "
                    f"{errors} erreurs | "
                    f"topic: {topic}"
                )

        except Exception as e:
            errors += 1
            print(f"[normalizer] ERREUR indexation : {e}")

except KeyboardInterrupt:
    print(f"\n[normalizer] arrêt — {indexed} indexés, {skipped} ignorés, {errors} erreurs")
finally:
    consumer.close()
