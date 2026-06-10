#!/usr/bin/env python3
"""
Simule des requêtes SQL sur sensitive_db avec injection
de scénarios d'exfiltration de données.

Utilisateurs simulés :
  app_user      — application web, requêtes normales ciblées
  analyst       — analyste métier, agrégats et rapports
  dba_user      — DBA, requêtes système légitimes
  intern_user   — stagiaire, accès limité
  attacker      — compte compromis, comportement malveillant

Scénarios d'exfiltration (cycle automatique) :
  normal          trafic légitime varié
  dump_table      SELECT * sans WHERE sur table sensible
  recon           requêtes sur pg_tables, information_schema
  mass_exfil      rafale de SELECT sur clients/paiements
  priv_escalation requêtes DDL depuis un compte non-admin
  scraping        SELECT avec grands LIMIT sur données sensibles

La clé Kafka est le user_db — toutes les requêtes
d'un même utilisateur vont sur la même partition.
"""
import json
import random
import time
from datetime import datetime, timezone

import psycopg2
from kafka import KafkaProducer

# --- Connexions ---
DB = {
    "host": "localhost", "port": 5432,
    "dbname": "sensitive_db",
    "user": "dbadmin", "password": "dbpassword",
}
BOOTSTRAP = "localhost:29092"
TOPIC     = "db-queries"

# --- Utilisateurs simulés ---
USERS = {
    "app_user":   {"normal": True,  "can_ddl": False, "ip": "10.0.1.10"},
    "analyst":    {"normal": True,  "can_ddl": False, "ip": "10.0.1.20"},
    "dba_user":   {"normal": True,  "can_ddl": True,  "ip": "10.0.1.5"},
    "intern_user":{"normal": True,  "can_ddl": False, "ip": "10.0.1.42"},
    "attacker":   {"normal": False, "can_ddl": False, "ip": "185.220.101.45"},
}

# --- Requêtes normales par profil ---
NORMAL_QUERIES = {
    "app_user": [
        "SELECT id, nom, prenom, email FROM clients WHERE id = %s",
        "SELECT montant, statut, created_at FROM paiements WHERE client_id = %s",
        "SELECT reference, type_contrat, date_debut FROM contrats WHERE client_id = %s",
        "SELECT COUNT(*) FROM paiements WHERE statut = 'validé'",
        "SELECT id, nom, prenom FROM clients WHERE email = %s",
    ],
    "analyst": [
        "SELECT type_contrat, COUNT(*), AVG(valeur) FROM contrats GROUP BY type_contrat",
        "SELECT statut, COUNT(*), SUM(montant) FROM paiements GROUP BY statut",
        "SELECT departement, COUNT(*) FROM employes GROUP BY departement",
        "SELECT DATE_TRUNC('month', created_at), COUNT(*) FROM clients GROUP BY 1",
        "SELECT AVG(montant) FROM paiements WHERE statut = 'validé'",
    ],
    "dba_user": [
        "SELECT schemaname, tablename FROM pg_tables WHERE schemaname = 'public'",
        "SELECT COUNT(*) FROM clients",
        "SELECT pg_size_pretty(pg_database_size('sensitive_db'))",
        "SELECT COUNT(*) FROM paiements",
        "VACUUM ANALYZE clients",
    ],
    "intern_user": [
        "SELECT COUNT(*) FROM clients",
        "SELECT type_contrat, COUNT(*) FROM contrats GROUP BY type_contrat",
        "SELECT statut, COUNT(*) FROM paiements GROUP BY statut",
    ],
}

# --- Requêtes d'exfiltration par scénario ---
EXFIL_QUERIES = {
    "dump_table": [
        "SELECT * FROM clients",
        "SELECT * FROM paiements",
        "SELECT * FROM employes",
        "SELECT * FROM contrats",
        "SELECT nom, prenom, email, telephone, adresse, date_naissance FROM clients",
        "SELECT numero_carte, iban, montant, client_id FROM paiements",
        "SELECT nom, prenom, salaire, numero_secu FROM employes",
    ],
    "recon": [
        "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'",
        "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'clients'",
        "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'employes'",
        "SELECT schemaname, tablename, tableowner FROM pg_tables",
        "SELECT usename, usesuper, usecreatedb FROM pg_user",
        "SELECT * FROM pg_roles",
        "SELECT current_user, current_database()",
    ],
    "mass_exfil": [
        "SELECT * FROM clients LIMIT 1000",
        "SELECT * FROM paiements LIMIT 1000",
        "SELECT id, nom, prenom, email, telephone FROM clients OFFSET %s LIMIT 50",
        "SELECT id, numero_carte, iban, montant FROM paiements OFFSET %s LIMIT 50",
        "SELECT * FROM employes LIMIT 500",
    ],
    "priv_escalation": [
        "CREATE USER hacker WITH PASSWORD 'backdoor123'",
        "GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO hacker",
        "ALTER USER intern_user WITH SUPERUSER",
        "CREATE TABLE exfil_staging AS SELECT * FROM clients",
        "COPY clients TO '/tmp/clients_dump.csv' CSV HEADER",
    ],
    "scraping": [
        "SELECT id, nom, prenom, email FROM clients ORDER BY id LIMIT 100 OFFSET %s",
        "SELECT id, numero_carte, montant FROM paiements ORDER BY id LIMIT 100 OFFSET %s",
        "SELECT nom, prenom, salaire, departement FROM employes ORDER BY salaire DESC",
        "SELECT * FROM clients WHERE id BETWEEN %s AND %s",
        "SELECT email, telephone FROM clients ORDER BY id LIMIT 200",
    ],
}

# --- Scénarios (nom, durée en secondes) ---
SCENARIOS = [
    ("normal",          40),
    ("dump_table",      15),
    ("normal",          20),
    ("recon",           15),
    ("normal",          20),
    ("mass_exfil",      20),
    ("normal",          20),
    ("priv_escalation", 10),
    ("normal",          20),
    ("scraping",        20),
]

def make_event(user, query, scenario, rows_returned=0, duration_ms=0):
    """Construit l'événement JSON publié dans Kafka."""
    return {
        "user_db":       user,
        "ip_source":     USERS[user]["ip"],
        "query_text":    query,
        "query_type":    query.strip().split()[0].upper(),
        "scenario":      scenario,
        "rows_returned": rows_returned,
        "duration_ms":   duration_ms,
        "ts":            datetime.now(timezone.utc).isoformat(),
    }

def execute_query(conn, query):
    """
    Exécute la requête et retourne (rows_returned, duration_ms).
    Les requêtes DDL dangereuses sont simulées sans exécution réelle
    pour ne pas corrompre la base — on retourne juste des métriques.
    """
    DDL_KEYWORDS = {"CREATE", "DROP", "ALTER", "GRANT", "REVOKE", "COPY"}
    first_word   = query.strip().split()[0].upper()

    if first_word in DDL_KEYWORDS:
        # Simulation sans exécution — réaliste pour le monitoring
        return 0, random.randint(5, 50)

    # Remplace les placeholders %s par des valeurs aléatoires
    query_exec = query
    param_count = query.count("%s")
    params = tuple(random.randint(1, 100) for _ in range(param_count))

    try:
        t0  = time.time()
        cur = conn.cursor()
        cur.execute(query_exec, params if params else None)

        try:
            rows = cur.fetchall()
            rows_returned = len(rows)
        except psycopg2.ProgrammingError:
            rows_returned = 0

        duration_ms = int((time.time() - t0) * 1000)
        cur.close()
        return rows_returned, duration_ms

    except Exception:
        conn.rollback()
        return 0, 0

# --- Producer Kafka ---
producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP,
    key_serializer=lambda k: k.encode("utf-8"),
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    linger_ms=5,
    acks="all",
)

conn = psycopg2.connect(**DB)
conn.autocommit = True

print(f"[db-simulator] -> {TOPIC} sur {BOOTSTRAP}")
print("[db-simulator] Ctrl+C pour arrêter\n")

sent = 0
scenario_idx   = 0
scenario_name, scenario_duration = SCENARIOS[0]
scenario_start = time.time()

# Attaquant fixe pour cette session
attacker_user = "attacker"

try:
    while True:
        now = time.time()

        # Rotation du scénario
        if now - scenario_start >= scenario_duration:
            scenario_idx = (scenario_idx + 1) % len(SCENARIOS)
            scenario_name, scenario_duration = SCENARIOS[scenario_idx]
            scenario_start = now
            print(f"\n[db-simulator] >>> Scénario : {scenario_name.upper()} ({scenario_duration}s)")

        if scenario_name == "normal":
            # Trafic normal : utilisateurs légitimes variés
            user  = random.choice(["app_user", "analyst", "dba_user", "intern_user"])
            query = random.choice(NORMAL_QUERIES[user])
            sleep = random.uniform(0.3, 1.0)

        else:
            # Scénario d'attaque : l'attaquant exécute les requêtes
            user  = attacker_user
            query = random.choice(EXFIL_QUERIES[scenario_name])
            # Rafale plus rapide selon le scénario
            if scenario_name == "mass_exfil":
                sleep = random.uniform(0.02, 0.08)
            elif scenario_name == "scraping":
                sleep = random.uniform(0.05, 0.15)
            else:
                sleep = random.uniform(0.1, 0.3)

        rows, duration = execute_query(conn, query)
        event = make_event(user, query, scenario_name, rows, duration)

        producer.send(TOPIC, key=user, value=event)
        sent += 1

        if sent % 50 == 0:
            print(f"[db-simulator] {sent} requêtes | scénario: {scenario_name}")

        time.sleep(sleep)

except KeyboardInterrupt:
    print(f"\n[db-simulator] arrêt — {sent} requêtes envoyées")
finally:
    producer.flush()
    producer.close()
    conn.close()
