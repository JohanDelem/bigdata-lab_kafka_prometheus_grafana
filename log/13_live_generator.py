#!/usr/bin/env python3
"""
Génère un flux de logs HTTP en temps réel avec injection de scénarios d'attaque.

Scénarios simulés (cycle automatique toutes les 60s) :
  - normal      : trafic légitime, IPs variées, GET majoritaire
  - brute_force : 1 IP fixe, rafale sur /admin ou /login
  - scan        : 1 IP fixe, paths suspects (/.env, /etc/passwd...)
  - ddos        : 5 IPs coordonnées, volume x10
  - errors      : burst de réponses 500 sur un path spécifique

La clé Kafka est toujours l'IP source.
"""
import json
import random
import time
from datetime import datetime, timezone
from kafka import KafkaProducer

BOOTSTRAP = "localhost:29092"
TOPIC     = "web-logs"

# --- Données de simulation ---

NORMAL_IPS = [f"85.{random.randint(1,254)}.{random.randint(1,254)}.{random.randint(1,254)}"
              for _ in range(50)]

ATTACKER_IPS = [
    "45.33.32.156",    # IP connue scanner Shodan
    "185.220.101.45",  # plage Tor exit node
    "193.32.162.10",   # plage suspecte
    "91.108.4.200",    # plage botnet connue
    "198.199.88.42",   # DigitalOcean abuse
]

NORMAL_PATHS = [
    "/", "/index.html", "/produits", "/produits/123",
    "/produits/456", "/panier", "/checkout", "/blog",
    "/contact", "/about", "/faq",
]

SUSPICIOUS_PATHS = [
    "/.env", "/etc/passwd", "/wp-admin/", "/admin/",
    "/phpmyadmin/", "/.git/config", "/config.php",
    "/backup.sql", "/../../../etc/shadow",
    "/login?user=admin'--", "/search?q=<script>alert(1)</script>",
]

NORMAL_UAS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 16_0 like Mac OS X)",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36",
]

ATTACK_UAS = [
    "sqlmap/1.7.8#stable",
    "Nikto/2.1.6",
    "masscan/1.0",
    "python-requests/2.28.0",
    "curl/7.68.0",
    "zgrab/0.x",
]

METHODS       = ["GET"] * 8 + ["POST"] * 2
NORMAL_STATUS = [200] * 7 + [304] * 1 + [404] * 1 + [500] * 1

# --- Scénarios ---

SCENARIOS = [
    ("normal",      45),   # 45s de trafic normal
    ("brute_force", 15),   # 15s de brute force
    ("normal",      20),
    ("scan",        15),   # 15s de scan de vulnérabilités
    ("normal",      20),
    ("ddos",        10),   # 10s de DDoS
    ("normal",      20),
    ("errors",      10),   # 10s de burst d'erreurs 500
]

def make_event(ip, path, method, status, ua, size=None):
    ts = datetime.now(timezone.utc).strftime("%d/%b/%Y:%H:%M:%S +0000")
    return {
        "ip":     ip,
        "ts":     ts,
        "method": method,
        "path":   path,
        "status": status,
        "bytes":  size if size else random.randint(500, 50000),
        "ua":     ua,
    }

def gen_normal():
    """Génère un événement de trafic normal."""
    ip     = random.choice(NORMAL_IPS)
    path   = random.choice(NORMAL_PATHS)
    method = random.choice(METHODS)
    status = random.choice(NORMAL_STATUS)
    ua     = random.choice(NORMAL_UAS)
    return ip, make_event(ip, path, method, status, ua)

def gen_brute_force(attacker_ip):
    """Simule une attaque brute force sur /login ou /admin."""
    path   = random.choice(["/login", "/admin/", "/wp-login.php"])
    status = random.choice([401, 403, 200])
    ua     = random.choice(ATTACK_UAS)
    return attacker_ip, make_event(attacker_ip, path, "POST", status, ua, 512)

def gen_scan(attacker_ip):
    """Simule un scan de vulnérabilités."""
    path   = random.choice(SUSPICIOUS_PATHS)
    status = random.choice([404, 403, 200, 500])
    ua     = random.choice(ATTACK_UAS)
    return attacker_ip, make_event(attacker_ip, path, "GET", status, ua, 256)

def gen_ddos(attacker_ips):
    """Simule un DDoS depuis plusieurs IPs coordonnées."""
    ip     = random.choice(attacker_ips)
    path   = random.choice(["/", "/index.html", "/produits"])
    ua     = random.choice(ATTACK_UAS)
    return ip, make_event(ip, path, "GET", 200, ua)

def gen_errors():
    """Simule un burst d'erreurs 500 sur un path spécifique."""
    ip     = random.choice(NORMAL_IPS)
    path   = "/api/checkout"
    ua     = random.choice(NORMAL_UAS)
    return ip, make_event(ip, path, "POST", 500, ua, 128)


producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP,
    key_serializer=lambda k: k.encode("utf-8"),
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    linger_ms=5,
    acks="all",
)

print(f"[live-generator] -> {TOPIC} sur {BOOTSTRAP}")
print("[live-generator] Ctrl+C pour arrêter\n")

# IPs d'attaquants fixes pour cette session
session_attacker  = random.choice(ATTACKER_IPS)
session_ddos_ips  = random.sample(ATTACKER_IPS, 3)

sent = 0
scenario_idx = 0
scenario_name, scenario_duration = SCENARIOS[0]
scenario_start = time.time()

try:
    while True:
        now = time.time()

        # Rotation du scénario
        if now - scenario_start >= scenario_duration:
            scenario_idx   = (scenario_idx + 1) % len(SCENARIOS)
            scenario_name, scenario_duration = SCENARIOS[scenario_idx]
            scenario_start = now
            print(f"\n[live-generator] >>> Scénario : {scenario_name.upper()} ({scenario_duration}s)")

        # Génération selon le scénario actif
        if scenario_name == "normal":
            ip, event = gen_normal()
            sleep = random.uniform(0.05, 0.15)

        elif scenario_name == "brute_force":
            ip, event = gen_brute_force(session_attacker)
            sleep = random.uniform(0.01, 0.05)   # rafale rapide

        elif scenario_name == "scan":
            ip, event = gen_scan(session_attacker)
            sleep = random.uniform(0.02, 0.08)

        elif scenario_name == "ddos":
            ip, event = gen_ddos(session_ddos_ips)
            sleep = random.uniform(0.005, 0.02)  # très rapide

        elif scenario_name == "errors":
            ip, event = gen_errors()
            sleep = random.uniform(0.02, 0.06)

        producer.send(TOPIC, key=ip, value=event)
        sent += 1

        if sent % 100 == 0:
            print(f"[live-generator] {sent} événements | scénario: {scenario_name}")

        time.sleep(sleep)

except KeyboardInterrupt:
    print(f"\n[live-generator] arrêt — {sent} événements envoyés")
finally:
    producer.flush()
    producer.close()
