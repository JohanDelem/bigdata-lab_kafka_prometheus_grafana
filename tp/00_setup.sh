#!/usr/bin/env bash

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

create_topic events 3

create_topic events-stats 1

echo "----- topics existants -----"

docker exec -e KAFKA_OPTS="" kafka "$KAFKA_BIN/kafka-topics.sh" \
  --bootstrap-server "$BS" \
  --list