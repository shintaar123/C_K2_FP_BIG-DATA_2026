"""
base_scraper.py
Kontrak dasar semua scraper. Tujuannya: APAPUN sumbernya (RSS, X, Reddit, YouTube),
hasil akhirnya harus punya skema field yang SAMA sebelum dikirim ke Kafka -> Bronze.
Ini penting karena Spark Bronze job (punya tim Pipeline+LLM) akan baca skema yang seragam.

Skema standar 1 record bronze:
{
    "id": str,              # unique id, biasanya hash dari url/sumber
    "source_type": str,     # "rss" | "x" | "reddit" | "youtube"
    "source_name": str,     # "detik_jatim", "beritajatim", dst
    "raw_text": str,        # judul + isi/snippet
    "author": str | None,
    "url": str | None,
    "likes": int,           # 0 kalau sumber gak punya konsep likes (mis. RSS)
    "shares": int,
    "published_at": str,    # ISO 8601
    "scraped_at": str,      # ISO 8601, waktu scraping dijalankan
}
"""

import hashlib
import json
import logging
from abc import ABC, abstractmethod
from datetime import datetime, timezone

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)


class BaseScraper(ABC):
    """Semua scraper sumber turunan dari class ini."""

    source_type: str = "unknown"

    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)

    @abstractmethod
    def fetch(self) -> list[dict]:
        """Ambil data mentah dari sumber. Return list of dict (format bebas, sesuai sumber)."""
        raise NotImplementedError

    @abstractmethod
    def normalize(self, raw_items: list[dict]) -> list[dict]:
        """Ubah raw_items jadi skema standar bronze (lihat docstring di atas)."""
        raise NotImplementedError

    def run(self) -> list[dict]:
        """Entry point: fetch -> normalize -> return list record siap kirim ke Kafka."""
        self.logger.info("Mulai scraping...")
        raw_items = self.fetch()
        self.logger.info(f"Dapat {len(raw_items)} item mentah")
        records = self.normalize(raw_items)
        self.logger.info(f"Berhasil normalize {len(records)} record")
        return records

    @staticmethod
    def make_id(*parts: str) -> str:
        """Bikin id unik & stabil dari kombinasi string (misal source_name + url)."""
        raw = "|".join(parts)
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:24]

    @staticmethod
    def now_iso() -> str:
        return datetime.now(timezone.utc).isoformat()

    @staticmethod
    def to_jsonl(records: list[dict]) -> str:
        """Buat debugging lokal -- print/simpan hasil scraping sebagai JSON Lines."""
        return "\n".join(json.dumps(r, ensure_ascii=False) for r in records)
