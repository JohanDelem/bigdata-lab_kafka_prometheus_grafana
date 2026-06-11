#!/usr/bin/env bash
# =============================================================
# 23_setup_wazuh.sh — Initialisation du topic Kafka pour le
# pipeline de simulation Wazuh (Phase 3 SIEM)
#
# Topics créés :
#   wazuh-alerts  (3 partitions) — alertes simulées Wazuh
#                 clé = agent.name (nom de la machine surveillée)
#                 -> toutes les alertes d'un même hôte sur
#                    la même partition
#
# Usage :
#   bash siem/23_setup_wazuh.sh
#
# Contexte :
#   Simule ce qu'un vrai Wazuh Server ferait en production :
#   publier les alertes de ses agents dans Kafka.
#   Quand de vraies VMs seront disponibles, seul ce topic
#   change de producteur (agent réel -> simulateur Python).
#   Le normalizer (20_normalizer.py) et la corrélation
#   (21_correlator.py) n'ont pas à être modifiés.
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

# --- wazuh-alerts : 3 partitions ---
# La clé Kafka est agent.name (nom de la machine surveillée).
# En production, chaque agent Wazuh sur chaque VM publie
# avec son nom comme clé -> toutes ses alertes sur la même
# partition -> pic sur une partition = hôte sous attaque.
create_topic wazuh-alerts 3

echo ""
echo "----- topics existants -----"
docker exec -e KAFKA_OPTS="" kafka "$KAFKA_BIN/kafka-topics.sh" \
  --bootstrap-server "$BS" \
  --list
