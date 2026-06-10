#!/usr/bin/env bash
# =============================================================
# 15_setup.sh — Initialisation des topics Kafka pour le
# pipeline de surveillance base de données et détection
# d'exfiltration de données sensibles
#
# Topics créés :
#   db-queries   (3 partitions) — flux brut des requêtes SQL
#                clé = user_db -> localité par partition
#   db-alerts    (1 partition)  — alertes d'exfiltration détectées
#                clé = sévérité (CRITICAL, HIGH, MEDIUM)
#
# Usage :
#   bash db/15_setup.sh
#
# Prérequis :
#   - conteneur kafka en cours d'exécution (docker compose up -d)
#   - conteneur postgres-lab en cours d'exécution (docker compose up -d postgres)
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

# --- db-queries : 3 partitions ---
# La clé Kafka est le user_db (utilisateur base de données).
# Toutes les requêtes d'un même user atterrissent sur la même
# partition — un pic sur une partition = un user anormalement actif.
create_topic db-queries 3

# --- db-alerts : 1 partition ---
# Alertes produites par le monitor lors de détection d'exfiltration.
# La clé est la sévérité (CRITICAL, HIGH, MEDIUM).
# 1 partition suffit : débit faible, ordre des alertes important.
create_topic db-alerts 1

echo ""
echo "----- topics existants -----"
docker exec -e KAFKA_OPTS="" kafka "$KAFKA_BIN/kafka-topics.sh" \
  --bootstrap-server "$BS" \
  --list
