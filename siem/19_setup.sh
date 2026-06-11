#!/usr/bin/env bash
# =============================================================
# 19_setup.sh — Initialisation du topic Kafka pour le
# pipeline de corrélation SIEM cross-sources
#
# Topics créés :
#   corr-alerts  (1 partition) — alertes de corrélation
#                clé = règle de corrélation déclenchée
#
# Usage :
#   bash siem/19_setup.sh
#
# Prérequis :
#   - conteneur kafka en cours d'exécution
#   - topics security-alerts, db-alerts, web-logs déjà créés
#     (bash log/09_setup.sh && bash db/15_setup.sh)
#   - OpenSearch démarré (docker compose up -d opensearch)
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

# --- corr-alerts : 1 partition ---
# Alertes de corrélation cross-sources produites par 21_correlator.py.
# La clé est le nom de la règle déclenchée (RECON_TO_EXFIL, etc.).
# 1 partition suffit : débit faible, ordre des alertes important.
create_topic corr-alerts 1

echo ""
echo "----- topics existants -----"
docker exec -e KAFKA_OPTS="" kafka "$KAFKA_BIN/kafka-topics.sh" \
  --bootstrap-server "$BS" \
  --list
