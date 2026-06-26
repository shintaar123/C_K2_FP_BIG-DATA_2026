"""
threads_scraper.py
Scraper Threads (Meta) untuk hashtag/profil publik Surabaya.

CATATAN REALITAS (Section 3 & 14 implementation plan):
Threads di 2026 menerapkan login wajib + deteksi fingerprint/IP yang agresif.
Tanpa proxy residential & akun yang di-warm-up, scraping langsung hampir pasti
gagal/diblokir. Karena itu scraper ini:
  1. Mencoba best-effort fetch publik via HTTP (requests).
  2. Jika gagal / 0 hasil -> fallback ke data generate realistis
     (pola sama seperti X & Reddit di proyek ini), supaya pipeline tetap jalan.

Skema output = skema standar Bronze (lihat base_scraper.py).
"""

from __future__ import annotations

import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

import requests

from scrapers.base_scraper import BaseScraper
from scrapers.sources_config import THREADS_HASHTAGS, RELEVANT_KEYWORDS


def is_relevant(text: str) -> bool:
    t = text.lower()
    return any(kw in t for kw in RELEVANT_KEYWORDS)


class ThreadsScraper(BaseScraper):
    source_type = "threads"

    def __init__(self, hashtags=None, timeout: int = 15):
        super().__init__()
        self.hashtags = hashtags or THREADS_HASHTAGS
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
            ),
            "Accept-Language": "id-ID,id;q=0.9,en;q=0.8",
        })

    def fetch(self) -> list[dict]:
        """Best-effort: coba endpoint publik hashtag Threads.
        Sangat mungkin gagal (login wall) -> kembalikan [] agar fallback aktif."""
        raw_items = []
        for tag in self.hashtags:
            url = f"https://www.threads.net/search?q=%23{tag}&serp_type=tags"
            try:
                resp = self.session.get(url, timeout=self.timeout)
                if resp.status_code != 200:
                    self.logger.warning(f"Threads #{tag} -> HTTP {resp.status_code}")
                    continue
                # Threads me-render konten via JS + butuh login; HTML mentah biasanya
                # tidak memuat post. Tanpa parser khusus, anggap tak ada hasil.
                self.logger.info(f"Threads #{tag} diakses, namun konten butuh sesi login.")
            except requests.RequestException as e:
                self.logger.warning(f"Gagal akses Threads #{tag}: {e}")
        return raw_items

    def normalize(self, raw_items: list[dict]) -> list[dict]:
        records = []
        for item in raw_items:
            text = (item.get("text") or "").strip()
            if not text or not is_relevant(text):
                continue
            records.append({
                "id": self.make_id("threads", item.get("code", text[:32])),
                "source_type": self.source_type,
                "source_name": f"threads_{item.get('hashtag', 'public')}",
                "raw_text": text[:2000],
                "author": item.get("username"),
                "url": item.get("url"),
                "likes": int(item.get("like_count", 0) or 0),
                "shares": 0,
                "published_at": item.get("published_at") or self.now_iso(),
                "scraped_at": self.now_iso(),
            })
        return records


def run_threads_scraper(fallback_n: int = 20) -> list[dict]:
    """Jalankan scraper Threads; jika 0 hasil nyata -> data generate fallback."""
    try:
        scraper = ThreadsScraper()
        records = scraper.run()
    except Exception as e:  # noqa: BLE001
        import logging
        logging.getLogger("ThreadsScraper").warning(f"Scraper error: {e}")
        records = []

    if not records:
        import logging
        logging.getLogger("ThreadsScraper").info(
            "Threads tanpa hasil nyata (login wall) -> pakai data generate fallback."
        )
        from scrapers.social_media.generate_scraper import generate_threads_records
        records = generate_threads_records(fallback_n)
    return records


if __name__ == "__main__":
    results = run_threads_scraper()
    print(f"\nTotal record Threads: {len(results)}\n")
    from scrapers.base_scraper import BaseScraper as BS
    print(BS.to_jsonl(results[:3]))
