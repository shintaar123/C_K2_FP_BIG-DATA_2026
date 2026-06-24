#!/bin/bash
# kafka/create_topics.sh
# Jalankan SETELAH docker compose up -d (kafka harus sudah healthy).
# Usage: bash kafka/create_topics.sh

set -e

TOPICS=("raw-rss" "raw-x" "raw-reddit" "raw-yt" "raw-threads")

for TOPIC in "${TOPICS[@]}"; do
  echo ">>> Membuat topic: $TOPIC"
  docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
    --create --if-not-exists \
    --topic "$TOPIC" \
    --bootstrap-server localhost:9092 \
    --partitions 3 \
    --replication-factor 1
done

echo ""
echo ">>> Daftar topic sekarang:"
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --list --bootstrap-server localhost:9092
