"""
producer.py
Wrapper kirim record (hasil scraper) ke Kafka topic.
Dipakai oleh semua scraper -- jangan import KafkaProducer langsung di file scraper.

Cara test koneksi Kafka manual (pastikan `docker compose up -d` sudah jalan):
    python kafka/producer.py --test
"""

import argparse
import json
import sys

from kafka import KafkaProducer
from kafka.errors import KafkaError


def get_producer(bootstrap_servers: str = "localhost:9092") -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        retries=3,
        acks="all",  # tunggu konfirmasi broker, lebih aman daripada kecepetan tapi data hilang
    )


def send_records(producer: KafkaProducer, topic: str, records: list[dict]) -> int:
    """Kirim list record ke topic, key = id record (biar partisi konsisten per id)."""
    sent = 0
    for record in records:
        try:
            producer.send(topic, key=record.get("id"), value=record)
            sent += 1
        except KafkaError as e:
            print(f"[producer] Gagal kirim record id={record.get('id')}: {e}", file=sys.stderr)
    producer.flush()  # pastikan semua terkirim sebelum lanjut/exit
    return sent


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--test", action="store_true", help="Kirim 1 record dummy ke topic raw-rss untuk tes koneksi")
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    args = parser.parse_args()

    if args.test:
        p = get_producer(args.bootstrap_servers)
        dummy = [{
            "id": "test-001",
            "source_type": "rss",
            "source_name": "manual_test",
            "raw_text": "Ini record dummy buat tes koneksi Kafka",
            "author": None,
            "url": None,
            "likes": 0,
            "shares": 0,
            "published_at": "",
            "scraped_at": "",
        }]
        n = send_records(p, "raw-rss", dummy)
        print(f"Berhasil kirim {n} record ke topic 'raw-rss'. Cek dengan kafka-console-consumer kalau mau verifikasi.")
