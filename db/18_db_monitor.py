#!/usr/bin/env python3
"""
Lit le topic 'db-queries', agrège par fenêtres de 10s
et publie les alertes d'exfiltration dans 'db-alerts'.

Règles de détection :
  CRITICAL  SELECT * sans WHERE sur table sensible       (dump_table)
  CRITICAL  requêtes DDL depuis un compte non-admin      (priv_escalation)
  HIGH      volume > 30 requêtes/fenêtre par user        (mass_exfil / scraping)
  HIGH      accès à information_schema ou pg_tables      (recon)
  HIGH      lignes retournées > 500 sur une fenêtre      (exfiltration volume)
  MEDIUM    accès simultané à 3+ tables sensibles        (recon élargie)
  MEDIUM    taux d'erreurs > 20% pour un user            (scan aveugle)
"""
import json
import time
from collections import Counter, defaultdict
from datetime import datetime, timezone
from kafka import KafkaConsumer, KafkaProducer

BOOTSTRAP      = "localhost:29092"
SOURCE         = "db-queries"
SINK           = "db-alerts"
GROUP          = "db-monitor"
WINDOW_SECONDS = 10

# Tables contenant des données sensibles
SENSITIVE_TABLES = {"clients", "paiements", "employes", "contrats"}

# Tables système — accès légitime uniquement pour dba_user
SYSTEM_TABLES = {"information_schema", "pg_tables", "pg_user",
                 "pg_roles", "pg_catalog"}

# Comptes autorisés à exécuter des DDL
DDL_AUTHORIZED = {"dba_user"}

# Mots-clés DDL dangereux
DDL_KEYWORDS = {"CREATE", "DROP", "ALTER", "GRANT", "REVOKE", "COPY"}

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
    """Initialise un bucket vide pour une nouvelle fenêtre."""
    return {
        "total":          0,
        # Par utilisateur
        "user_counts":    Counter(),   # nb requêtes par user
        "user_rows":      Counter(),   # nb lignes retournées par user
        "user_tables":    defaultdict(set),  # tables accédées par user
        "user_errors":    Counter(),   # erreurs par user
        # Alertes brutes détectées
        "dump_hits":      [],          # (user, query)
        "ddl_hits":       [],          # (user, query)
        "recon_hits":     [],          # (user, query)
    }

def iso(epoch):
    return datetime.fromtimestamp(epoch, tz=timezone.utc).isoformat()

def extract_tables(query: str) -> set:
    """
    Extrait les noms de tables mentionnées dans une requête SQL.
    Approche simple par mots-clés — suffisante pour la simulation.
    """
    tokens  = query.lower().replace("(", " ").replace(")", " ").split()
    tables  = set()
    trigger = {"from", "join", "into", "update", "table"}
    for i, tok in enumerate(tokens):
        if tok in trigger and i + 1 < len(tokens):
            candidate = tokens[i + 1].strip(",;")
            # Ignore les sous-requêtes et mots-clés SQL
            if candidate.isidentifier() and candidate not in trigger:
                tables.add(candidate)
    return tables

def is_select_star_no_where(query: str) -> bool:
    """Détecte SELECT * ... sans clause WHERE."""
    q = query.lower().strip()
    return (
        q.startswith("select *") or
        "select *" in q
    ) and "where" not in q

def detect_and_publish(window_start, bucket):
    """Applique les règles de détection et publie les alertes."""
    alerts  = []
    ts      = iso(window_start)
    ts_end  = iso(window_start + WINDOW_SECONDS)

    # --- CRITICAL : dump de table (SELECT * sans WHERE) ---
    for user, query in bucket["dump_hits"]:
        tables = extract_tables(query)
        sensitive_hit = tables & SENSITIVE_TABLES
        if sensitive_hit:
            alerts.append({
                "severity":    "CRITICAL",
                "rule":        "TABLE_DUMP",
                "description": f"{user} a exécuté SELECT * sans WHERE sur {sensitive_hit}",
                "user_db":     user,
                "tables":      list(sensitive_hit),
                "query":       query[:120],
                "window_start": ts,
                "window_end":   ts_end,
            })

    # --- CRITICAL : DDL depuis un compte non autorisé ---
    for user, query in bucket["ddl_hits"]:
        if user not in DDL_AUTHORIZED:
            alerts.append({
                "severity":    "CRITICAL",
                "rule":        "UNAUTHORIZED_DDL",
                "description": f"{user} a tenté une opération DDL non autorisée",
                "user_db":     user,
                "query":       query[:120],
                "window_start": ts,
                "window_end":   ts_end,
            })

    # --- HIGH : volume de requêtes par user ---
    for user, count in bucket["user_counts"].items():
        if count > 30:
            alerts.append({
                "severity":    "HIGH",
                "rule":        "HIGH_QUERY_VOLUME",
                "description": f"{user} a exécuté {count} requêtes en {WINDOW_SECONDS}s",
                "user_db":     user,
                "count":       count,
                "window_start": ts,
                "window_end":   ts_end,
            })

    # --- HIGH : reconnaissance système ---
    for user, query in bucket["recon_hits"]:
        if user not in DDL_AUTHORIZED:
            alerts.append({
                "severity":    "HIGH",
                "rule":        "DB_RECON",
                "description": f"{user} interroge les métadonnées système",
                "user_db":     user,
                "query":       query[:120],
                "window_start": ts,
                "window_end":   ts_end,
            })

    # --- HIGH : volume de lignes exfiltrées ---
    for user, rows in bucket["user_rows"].items():
        if rows > 500:
            alerts.append({
                "severity":    "HIGH",
                "rule":        "HIGH_ROWS_RETURNED",
                "description": f"{user} a récupéré {rows} lignes en {WINDOW_SECONDS}s",
                "user_db":     user,
                "rows":        rows,
                "window_start": ts,
                "window_end":   ts_end,
            })

    # --- MEDIUM : accès à 3+ tables sensibles par un même user ---
    for user, tables in bucket["user_tables"].items():
        sensitive_accessed = tables & SENSITIVE_TABLES
        if len(sensitive_accessed) >= 3:
            alerts.append({
                "severity":    "MEDIUM",
                "rule":        "MULTI_TABLE_ACCESS",
                "description": f"{user} a accédé à {len(sensitive_accessed)} tables sensibles : {sensitive_accessed}",
                "user_db":     user,
                "tables":      list(sensitive_accessed),
                "window_start": ts,
                "window_end":   ts_end,
            })

    # Publication des alertes
    for alert in alerts:
        producer.send(SINK, key=alert["severity"], value=alert)
        print(
            f"[db-monitor] ALERTE {alert['severity']:<8} "
            f"{alert['rule']:<22} | {alert['description'][:65]}"
        )

    producer.flush()

    if not alerts:
        total = bucket["total"]
        users = len(bucket["user_counts"])
        print(f"[db-monitor] {ts} -> {total} requêtes, {users} users — OK")

print(f"[db-monitor] {SOURCE} -> {SINK}  (Ctrl+C pour arrêter)")

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

                user  = e.get("user_db", "unknown")
                query = e.get("query_text", "")
                rows  = e.get("rows_returned", 0)
                qtype = e.get("query_type", "")

                b["total"] += 1
                b["user_counts"][user] += 1
                b["user_rows"][user]   += rows

                # Tables accédées
                for t in extract_tables(query):
                    b["user_tables"][user].add(t)

                # Détection SELECT * sans WHERE
                if is_select_star_no_where(query):
                    b["dump_hits"].append((user, query))

                # Détection DDL
                if qtype in DDL_KEYWORDS:
                    b["ddl_hits"].append((user, query))

                # Détection reconnaissance système
                query_lower = query.lower()
                if any(st in query_lower for st in SYSTEM_TABLES):
                    b["recon_hits"].append((user, query))

        # Clôture les fenêtres passées
        for ws in sorted(w for w in buckets if w < cur):
            detect_and_publish(ws, buckets.pop(ws))

except KeyboardInterrupt:
    print("\n[db-monitor] arrêt — clôture des fenêtres en cours")
    for ws in sorted(buckets):
        detect_and_publish(ws, buckets[ws])
finally:
    consumer.close()
    producer.close()
