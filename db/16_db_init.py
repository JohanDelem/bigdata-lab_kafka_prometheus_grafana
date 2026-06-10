#!/usr/bin/env python3
"""
Initialise la base de données sensitive_db avec des tables
contenant des données sensibles typiques d'une entreprise.

Tables créées :
  clients       — données personnelles (RGPD critique)
  paiements     — données bancaires (PCI-DSS critique)
  contrats      — données contractuelles confidentielles
  employes      — données RH sensibles
  audit_log     — journal des accès (table de contrôle)

Usage :
  python db/16_db_init.py
"""
import psycopg2
from psycopg2.extras import execute_values
import random
from datetime import datetime, timedelta

DB = {
    "host":     "localhost",
    "port":     5432,
    "dbname":   "sensitive_db",
    "user":     "dbadmin",
    "password": "dbpassword",
}

conn = psycopg2.connect(**DB)
cur  = conn.cursor()

print("[db-init] Connexion à sensitive_db OK")

# ============================================================
# Création des tables
# ============================================================

cur.execute("""
CREATE TABLE IF NOT EXISTS clients (
    id            SERIAL PRIMARY KEY,
    nom           VARCHAR(100),
    prenom        VARCHAR(100),
    email         VARCHAR(150) UNIQUE,
    telephone     VARCHAR(20),
    adresse       TEXT,
    date_naissance DATE,
    numero_client VARCHAR(20) UNIQUE,
    created_at    TIMESTAMP DEFAULT NOW()
)
""")

cur.execute("""
CREATE TABLE IF NOT EXISTS paiements (
    id              SERIAL PRIMARY KEY,
    client_id       INTEGER REFERENCES clients(id),
    numero_carte    VARCHAR(20),   -- masqué en prod, visible ici pour simulation
    montant         NUMERIC(10,2),
    devise          VARCHAR(3) DEFAULT 'EUR',
    statut          VARCHAR(20),
    iban            VARCHAR(34),
    created_at      TIMESTAMP DEFAULT NOW()
)
""")

cur.execute("""
CREATE TABLE IF NOT EXISTS contrats (
    id              SERIAL PRIMARY KEY,
    client_id       INTEGER REFERENCES clients(id),
    reference       VARCHAR(30) UNIQUE,
    type_contrat    VARCHAR(50),
    valeur          NUMERIC(12,2),
    date_debut      DATE,
    date_fin        DATE,
    contenu         TEXT,          -- clauses confidentielles
    created_at      TIMESTAMP DEFAULT NOW()
)
""")

cur.execute("""
CREATE TABLE IF NOT EXISTS employes (
    id              SERIAL PRIMARY KEY,
    nom             VARCHAR(100),
    prenom          VARCHAR(100),
    email_pro       VARCHAR(150) UNIQUE,
    poste           VARCHAR(100),
    salaire         NUMERIC(10,2),  -- donnée très sensible
    numero_secu     VARCHAR(20),    -- NIR — donnée critique
    departement     VARCHAR(50),
    date_embauche   DATE,
    created_at      TIMESTAMP DEFAULT NOW()
)
""")

cur.execute("""
CREATE TABLE IF NOT EXISTS audit_log (
    id          SERIAL PRIMARY KEY,
    user_db     VARCHAR(50),
    action      VARCHAR(20),
    table_name  VARCHAR(50),
    query_text  TEXT,
    ip_source   VARCHAR(45),
    created_at  TIMESTAMP DEFAULT NOW()
)
""")

conn.commit()
print("[db-init] Tables créées")

# ============================================================
# Insertion de données réalistes
# ============================================================

# Données clients
PRENOMS = ["Alice", "Bob", "Claire", "David", "Emma", "François",
           "Gilles", "Hélène", "Igor", "Julie", "Kevin", "Laura"]
NOMS    = ["Martin", "Bernard", "Dubois", "Thomas", "Robert",
           "Petit", "Durand", "Leroy", "Moreau", "Simon"]

clients = []
for i in range(200):
    prenom = random.choice(PRENOMS)
    nom    = random.choice(NOMS)
    email  = f"{prenom.lower()}.{nom.lower()}{i}@example.com"
    tel    = f"06{random.randint(10000000, 99999999)}"
    ddn    = datetime(1960, 1, 1) + timedelta(days=random.randint(0, 20000))
    clients.append((
        nom, prenom, email, tel,
        f"{random.randint(1,150)} rue de la Paix, Paris",
        ddn.date(),
        f"CLI{i:06d}"
    ))

execute_values(cur, """
    INSERT INTO clients (nom, prenom, email, telephone, adresse, date_naissance, numero_client)
    VALUES %s ON CONFLICT DO NOTHING
""", clients)

conn.commit()
print(f"[db-init] {len(clients)} clients insérés")

# Données paiements
cur.execute("SELECT id FROM clients LIMIT 200")
client_ids = [r[0] for r in cur.fetchall()]

paiements = []
for cid in client_ids:
    for _ in range(random.randint(1, 5)):
        carte = f"4{random.randint(100,999)} {random.randint(1000,9999)} {random.randint(1000,9999)} {random.randint(1000,9999)}"
        iban  = f"FR76{random.randint(10000000000000000000, 99999999999999999999)}"
        paiements.append((
            cid, carte,
            round(random.uniform(10, 5000), 2),
            random.choice(["validé", "refusé", "en_attente"]),
            iban
        ))

execute_values(cur, """
    INSERT INTO paiements (client_id, numero_carte, montant, statut, iban)
    VALUES %s
""", paiements)

conn.commit()
print(f"[db-init] {len(paiements)} paiements insérés")

# Données contrats
types = ["assurance_vie", "prevoyance", "epargne", "credit_immo", "credit_conso"]
contrats = []
for i, cid in enumerate(client_ids[:100]):
    contrats.append((
        cid,
        f"CTR{i:06d}",
        random.choice(types),
        round(random.uniform(5000, 500000), 2),
        datetime(2020, 1, 1).date() + timedelta(days=random.randint(0, 1500)),
        datetime(2025, 1, 1).date() + timedelta(days=random.randint(0, 1825)),
        f"Clauses confidentielles du contrat {i} — données propriétaires"
    ))

execute_values(cur, """
    INSERT INTO contrats (client_id, reference, type_contrat, valeur, date_debut, date_fin, contenu)
    VALUES %s ON CONFLICT DO NOTHING
""", contrats)

conn.commit()
print(f"[db-init] {len(contrats)} contrats insérés")

# Données employés
POSTES = ["Développeur", "Analyste", "DBA", "DevOps", "Manager",
          "Commercial", "Comptable", "Juriste", "RH", "RSSI"]
DEPTS  = ["IT", "Finance", "RH", "Commercial", "Juridique", "Direction"]

employes = []
for i in range(50):
    prenom = random.choice(PRENOMS)
    nom    = random.choice(NOMS)
    employes.append((
        nom, prenom,
        f"{prenom.lower()}.{nom.lower()}{i}@entreprise.com",
        random.choice(POSTES),
        round(random.uniform(28000, 120000), 2),
        f"1{random.randint(10,99)}{random.randint(10,99)}{random.randint(10,99)}{random.randint(100,999)}{random.randint(100,999)}{random.randint(10,99)}",
        random.choice(DEPTS),
        datetime(2010, 1, 1).date() + timedelta(days=random.randint(0, 5000))
    ))

execute_values(cur, """
    INSERT INTO employes (nom, prenom, email_pro, poste, salaire, numero_secu, departement, date_embauche)
    VALUES %s ON CONFLICT DO NOTHING
""", employes)

conn.commit()
print(f"[db-init] {len(employes)} employés insérés")

# ============================================================
# Résumé
# ============================================================
cur.execute("SELECT COUNT(*) FROM clients")
print(f"\n[db-init] Résumé sensitive_db :")
for table in ["clients", "paiements", "contrats", "employes"]:
    cur.execute(f"SELECT COUNT(*) FROM {table}")
    print(f"  {table:<12} : {cur.fetchone()[0]} lignes")

cur.close()
conn.close()
print("\n[db-init] Base initialisée et prête")
