#!/usr/bin/env bash
# =============================================================
# 09_setup.sh — Initialisation des topics Kafka pour le
# pipeline d'analyse de logs et de détection sécurité
#
# Topics créés :
#   web-logs         (3 partitions) — flux brut des requêtes HTTP
#                    clé = IP source -> localité par partition
#   log-stats        (1 partition)  — agrégats trafic par fenêtre 10s
#   security-alerts  (1 partition)  — alertes de sécurité détectées
#
# Usage :
#   bash log/09_setup.sh
#
# Prérequis :
#   - conteneur kafka en cours d'exécution (docker compose up -d)
#   - __consumer_offsets existant (sinon : bash tp/00_setup.sh en premier)
# =============================================================

set -euo pipefail

KAFKA_BIN=/opt/kafka/bin
BS=localhost:9092

create_topic () {
  local name=$1
  local parts=$2

  docker exec -e KAFKA_OPTS="" kafka "$KAFKA_BIN/kafka-topics.sh" \
    --bootstrap-server "$BS" \
    --create \
    --if-not-exists \
    --topic "$name" \
    --partitions "$parts" \
    --replication-factor 1

  echo "topic prêt : $name ($parts partition(s))"
}

# --- web-logs : 3 partitions ---
# La clé Kafka est l'IP source.
# Avec 3 partitions, les IPs sont réparties par hachage :
# toutes les requêtes d'une même IP vont sur la même partition.
# Cela rend les attaques visibles dans Grafana (pic sur 1 partition).
create_topic web-logs 3

# --- log-stats : 1 partition ---
# Agrégats produits par le processor toutes les 10 secondes.
# 1 partition suffit : le débit est faible (1 message / 10s).
create_topic log-stats 1

# --- security-alerts : 1 partition ---
# Alertes de sécurité détectées par le processor.
# La clé est la sévérité (CRITICAL, HIGH, MEDIUM).
# 1 partition suffit : les alertes sont moins fréquentes que le trafic brut.
create_topic security-alerts 1

echo ""
echo "----- topics existants -----"
docker exec -e KAFKA_OPTS="" kafka "$KAFKA_BIN/kafka-topics.sh" \
  --bootstrap-server "$BS" \
  --list
