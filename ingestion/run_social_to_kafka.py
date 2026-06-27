"""
run_social_to_kafka.py
Entry point: scrape YouTube + data generate X & Reddit -> kirim ke Kafka.
X scraper di-skip karena Twikit GraphQL ID expired (known bug).
Reddit di-skip karena diblokir Kominfo. Keduanya diganti data generate.
"""

import os
import sys

# ── Bersihkan output: redam warning & log INFO yang tidak penting ───────────
import warnings, logging as _logging
warnings.filterwarnings("ignore")
for _n in ("kafka", "googleapiclient", "google", "google.auth", "urllib3", "botocore", "boto3", "s3transfer"):
    _logging.getLogger(_n).setLevel(_logging.WARNING)

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "kafka"))

from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())

from scrapers.social_media.youtube_scraper import run_youtube_scraper
from scrapers.social_media.threads_playwright import scrape_threads_nyata
from scrapers.sources_config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC_YOUTUBE,
    KAFKA_TOPIC_THREADS,
)
from producer import get_producer, send_records


def scrape_and_send(scraper_fn, topic: str, label: str, producer):
    try:
        records = scraper_fn()
        print(f"[{label}] {len(records)} record relevan")
        if records:
            sent = send_records(producer, topic, records)
            print(f"[{label}] Terkirim {sent}/{len(records)} ke topic '{topic}'")
        else:
            print(f"[{label}] Tidak ada record, skip kirim.")
    except Exception as e:
        print(f"[{label}] ERROR: {e}")


def main():
    producer = get_producer(KAFKA_BOOTSTRAP_SERVERS)

    scrape_and_send(run_youtube_scraper, KAFKA_TOPIC_YOUTUBE, "YouTube", producer)
    
    # Menjalankan Threads Nyata via Playwright
    # Kita berikan daftar hashtag target
    tags = ["keluhansurabaya", "pdamsurabaya", "surabaya"]
    scrape_and_send(lambda: scrape_threads_nyata(tags), KAFKA_TOPIC_THREADS, "Threads (Playwright)", producer)

    print("\n[run_social_to_kafka] Selesai semua sumber.")


if __name__ == "__main__":
    main()