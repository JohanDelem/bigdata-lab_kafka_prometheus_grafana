#!/usr/bin/env python3
"""
Simule un flux d'alertes Wazuh au format exact qu'un vrai
Wazuh Server publierait dans Kafka depuis ses agents.

Agents simulés (futures VMs) :
  srv-web-01    serveur web frontend  (10.0.1.10)
  srv-db-01     serveur base de données (10.0.1.20)
  srv-admin-01  serveur d'administration (10.0.1.5)
  workstation-01 poste développeur (10.0.1.100)

Scénarios simulés :
  normal          activité système légitime
  ssh_bruteforce  rafale d'échecs SSH depuis une IP externe
  fim_alert       modification de fichier système critique
  privilege_esc   élévation de privilèges suspecte
  lateral_move    mouvement latéral entre hôtes
  webshell        exécution de commande suspecte via web

Format exact Wazuh :
  timestamp, rule.id, rule.description, rule.level,
  rule.groups, agent.id, agent.name, agent.ip,
  data.srcip, data.dstuser, location

La clé Kafka est agent.name — même partition pour
toutes les alertes d'un même hôte.

Usage :
  python siem/24_wazuh_simulator.py
"""
import json
import random
import time
from datetime import datetime, timezone
from kafka import KafkaProducer

BOOTSTRAP = "localhost:29092"
TOPIC     = "wazuh-alerts"

# --- Agents simulés (futures VMs) ---
AGENTS = [
    {"id": "001", "name": "srv-web-01",     "ip": "10.0.1.10"},
    {"id": "002", "name": "srv-db-01",      "ip": "10.0.1.20"},
    {"id": "003", "name": "srv-admin-01",   "ip": "10.0.1.5"},
    {"id": "004", "name": "workstation-01", "ip": "10.0.1.100"},
]

# --- IPs attaquantes ---
ATTACKER_IPS = [
    "185.220.101.45",   # Tor exit node
    "91.108.4.200",     # déjà vu dans les pipelines web/db
    "193.32.162.10",    # même attaquant DDoS
    "45.33.32.156",     # scanner connu
]

LEGIT_USERS  = ["alice", "bob", "charlie", "deploy", "backup"]
SYSTEM_FILES = [
    "/etc/passwd", "/etc/shadow", "/etc/sudoers",
    "/etc/ssh/sshd_config", "/var/www/html/index.php",
    "/usr/local/bin/deploy.sh",
]

# --- Règles Wazuh réelles ---
RULES = {
    # SSH
    "ssh_fail": {
        "id": "5710", "level": 5,
        "description": "SSH authentication failed",
        "groups": ["syslog", "sshd", "authentication_failed"],
        "location": "/var/log/auth.log",
    },
    "ssh_success_after_fail": {
        "id": "5720", "level": 10,
        "description": "SSH authentication success after several failures",
        "groups": ["syslog", "sshd", "authentication_success"],
        "location": "/var/log/auth.log",
    },
    "ssh_scan": {
        "id": "5700", "level": 8,
        "description": "SSH brute force attempt detected",
        "groups": ["syslog", "sshd", "authentication_failed"],
        "location": "/var/log/auth.log",
    },
    # Auth
    "pam_fail": {
        "id": "5503", "level": 5,
        "description": "PAM: User login failed",
        "groups": ["pam", "syslog", "authentication_failed"],
        "location": "/var/log/auth.log",
    },
    "sudo_escalation": {
        "id": "5402", "level": 9,
        "description": "Successful sudo to root",
        "groups": ["syslog", "sudo", "privilege_escalation"],
        "location": "/var/log/auth.log",
    },
    "new_user_created": {
        "id": "5902", "level": 8,
        "description": "New user added to the system",
        "groups": ["syslog", "useradd"],
        "location": "/var/log/auth.log",
    },
    # FIM (File Integrity Monitoring)
    "fim_modified": {
        "id": "550", "level": 7,
        "description": "Integrity checksum changed",
        "groups": ["ossec", "fim", "fim_event"],
        "location": "syscheck",
    },
    "fim_deleted": {
        "id": "553", "level": 7,
        "description": "File deleted — integrity checksum changed",
        "groups": ["ossec", "fim", "fim_event"],
        "location": "syscheck",
    },
    # Web
    "web_attack_sql": {
        "id": "31151", "level": 10,
        "description": "SQL injection attempt detected",
        "groups": ["web", "accesslog", "attack", "sql_injection"],
        "location": "/var/log/nginx/access.log",
    },
    "web_attack_xss": {
        "id": "31152", "level": 8,
        "description": "XSS attack attempt detected",
        "groups": ["web", "accesslog", "attack", "xss"],
        "location": "/var/log/nginx/access.log",
    },
    "webshell": {
        "id": "31301", "level": 15,
        "description": "Web shell detected — command execution via HTTP",
        "groups": ["web", "attack", "webshell"],
        "location": "/var/log/nginx/access.log",
    },
    # Processus suspects
    "rootkit_trojan": {
        "id": "510", "level": 12,
        "description": "Rootkit detection — suspicious process",
        "groups": ["rootcheck", "trojan"],
        "location": "rootcheck",
    },
    "proc_suspicious": {
        "id": "533", "level": 8,
        "description": "Suspicious process executed",
        "groups": ["ossec", "rootcheck"],
        "location": "/var/log/syslog",
    },
}

# --- Scénarios ---
SCENARIOS = [
    ("normal",         50),
    ("ssh_bruteforce", 20),
    ("normal",         30),
    ("fim_alert",      15),
    ("normal",         20),
    ("privilege_esc",  15),
    ("normal",         20),
    ("lateral_move",   20),
    ("normal",         20),
    ("webshell",       15),
]

def ts() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000+0000")

def make_event(agent: dict, rule_key: str, extra_data: dict = None) -> dict:
    """Construit un event au format exact Wazuh."""
    rule = RULES[rule_key]
    event = {
        "timestamp": ts(),
        "rule": {
            "id":          rule["id"],
            "description": rule["description"],
            "level":       rule["level"],
            "groups":      rule["groups"],
        },
        "agent": {
            "id":   agent["id"],
            "name": agent["name"],
            "ip":   agent["ip"],
        },
        "manager": {"name": "wazuh-manager"},
        "location": rule["location"],
        "data": extra_data or {},
    }
    return event

def gen_normal(agent):
    """Activité système légitime."""
    rule_key = random.choice(["pam_fail", "fim_modified", "ssh_fail"])
    data = {
        "srcip":   f"10.0.{random.randint(1,3)}.{random.randint(1,50)}",
        "dstuser": random.choice(LEGIT_USERS),
    }
    if rule_key == "fim_modified":
        data = {"file": random.choice(SYSTEM_FILES), "changed_attributes": ["mtime", "md5"]}
    return make_event(agent, rule_key, data)

def gen_ssh_bruteforce(attacker_ip):
    """Rafale SSH depuis une IP externe sur srv-web-01."""
    agent = AGENTS[0]  # srv-web-01
    # Majorité d'échecs, parfois un succès
    rule_key = "ssh_fail" if random.random() < 0.85 else "ssh_success_after_fail"
    if random.random() < 0.1:
        rule_key = "ssh_scan"
    return make_event(agent, rule_key, {
        "srcip":   attacker_ip,
        "dstuser": random.choice(["root", "admin", "ubuntu", "deploy"]),
    })

def gen_fim_alert():
    """Modification de fichier système critique."""
    agent  = random.choice(AGENTS[:2])  # web ou db
    rule_key = random.choice(["fim_modified", "fim_deleted"])
    return make_event(agent, rule_key, {
        "file":               random.choice(SYSTEM_FILES),
        "changed_attributes": ["mtime", "md5", "size"],
        "sha1_before":        "da39a3ee5e6b4b0d3255bfef95601890afd80709",
        "sha1_after":         "adc83b19e793491b1c6ea0fd8b46cd9f32e592fc",
    })

def gen_privilege_esc(attacker_ip):
    """Élévation de privilèges — sudo vers root."""
    agent = random.choice(AGENTS)
    rule_key = random.choice(["sudo_escalation", "new_user_created"])
    return make_event(agent, rule_key, {
        "srcuser": random.choice(LEGIT_USERS),
        "dstuser": "root",
        "command": random.choice(["sudo su", "sudo bash", "sudo -i", "useradd hacker"]),
    })

def gen_lateral_move(attacker_ip):
    """
    Mouvement latéral — un attaquant (IP externe fixe) réussit
    SSH sur plusieurs hôtes internes distincts.
    srcip = IP attaquante (fixe) pour que la corrélation
    WAZUH_LATERAL_MOVE regroupe par ip_source et détecte
    plusieurs hosts touchés par la même IP.
    """
    dst_agent = random.choice(AGENTS)
    return make_event(dst_agent, "ssh_success_after_fail", {
        "srcip":   attacker_ip,
        "dstip":   dst_agent["ip"],
        "dstuser": "root",
        "comment": f"lateral movement to {dst_agent['name']}",
    })

def gen_webshell(attacker_ip):
    """Webshell — exécution de commande via HTTP."""
    agent = AGENTS[0]  # srv-web-01
    rule_key = random.choice(["webshell", "web_attack_sql", "web_attack_xss"])
    return make_event(agent, rule_key, {
        "srcip":  attacker_ip,
        "url":    random.choice([
            "/wp-admin/admin-ajax.php?cmd=id",
            "/shell.php?c=cat%20/etc/passwd",
            "/upload/backdoor.php",
            "/api/exec?command=whoami",
        ]),
        "method": "GET",
        "status": random.choice([200, 404, 500]),
    })

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP,
    key_serializer=lambda k: k.encode("utf-8"),
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    linger_ms=5,
    acks="all",
)

print(f"[wazuh-sim] -> {TOPIC} sur {BOOTSTRAP}")
print(f"[wazuh-sim] agents : {[a['name'] for a in AGENTS]}")
print("[wazuh-sim] Ctrl+C pour arrêter\n")

sent = 0
scenario_idx   = 0
scenario_name, scenario_duration = SCENARIOS[0]
scenario_start = time.time()
attacker_ip    = random.choice(ATTACKER_IPS)

try:
    while True:
        now = time.time()

        # Rotation du scénario
        if now - scenario_start >= scenario_duration:
            scenario_idx = (scenario_idx + 1) % len(SCENARIOS)
            scenario_name, scenario_duration = SCENARIOS[scenario_idx]
            scenario_start = now
            attacker_ip    = random.choice(ATTACKER_IPS)
            print(f"\n[wazuh-sim] >>> Scénario : {scenario_name.upper()} ({scenario_duration}s) | attaquant: {attacker_ip}")

        # Génération selon le scénario
        if scenario_name == "normal":
            agent = random.choice(AGENTS)
            event = gen_normal(agent)
            sleep = random.uniform(0.5, 1.5)

        elif scenario_name == "ssh_bruteforce":
            event = gen_ssh_bruteforce(attacker_ip)
            sleep = random.uniform(0.05, 0.15)

        elif scenario_name == "fim_alert":
            event = gen_fim_alert()
            sleep = random.uniform(0.2, 0.5)

        elif scenario_name == "privilege_esc":
            agent = random.choice(AGENTS)
            event = gen_privilege_esc(attacker_ip)
            sleep = random.uniform(0.3, 0.8)

        elif scenario_name == "lateral_move":
            event = gen_lateral_move(attacker_ip)
            sleep = random.uniform(0.2, 0.6)

        elif scenario_name == "webshell":
            event = gen_webshell(attacker_ip)
            sleep = random.uniform(0.1, 0.3)

        else:
            agent = random.choice(AGENTS)
            event = gen_normal(agent)
            sleep = 1.0

        agent_name = event["agent"]["name"]
        producer.send(TOPIC, key=agent_name, value=event)
        sent += 1

        if sent % 50 == 0:
            print(f"[wazuh-sim] {sent} events | scénario: {scenario_name}")

        time.sleep(sleep)

except KeyboardInterrupt:
    print(f"\n[wazuh-sim] arrêt — {sent} events envoyés")
finally:
    producer.flush()
    producer.close()
