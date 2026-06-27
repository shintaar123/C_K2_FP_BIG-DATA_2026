"""
run_rss_to_kafka.py
Entry point tunggal: scrape semua sumber RSS -> kirim ke Kafka topic 'raw-rss'.
Dipanggil oleh run_pipeline.ps1 / run_continuous (orkestrasi pipeline).

Jalankan manual dari folder ingestion/:
    python run_rss_to_kafka.py
"""

import sys
import os

# ── Bersihkan output: redam warning & log INFO yang tidak penting ───────────
import warnings, logging as _logging
warnings.filterwarnings("ignore")
for _n in ("kafka", "googleapiclient", "google", "google.auth", "urllib3", "botocore", "boto3", "s3transfer"):
    _logging.getLogger(_n).setLevel(_logging.WARNING)

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "kafka"))

from scrapers.rss_scraper import run_all_rss_sources
from scrapers.sources_config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC_RSS
from producer import get_producer, send_records


def main():
    records = run_all_rss_sources()
    print(f"[run_rss_to_kafka] Total {len(records)} record relevan hasil scraping.")

    if not records:
        print("[run_rss_to_kafka] Tidak ada record baru/relevan, skip kirim ke Kafka.")
        return

    producer = get_producer(KAFKA_BOOTSTRAP_SERVERS)
    sent = send_records(producer, KAFKA_TOPIC_RSS, records)
    print(f"[run_rss_to_kafka] Berhasil kirim {sent}/{len(records)} record ke topic '{KAFKA_TOPIC_RSS}'.")


if __name__ == "__main__":
    main()
