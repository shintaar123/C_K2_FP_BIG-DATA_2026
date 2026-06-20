"""
rss_scraper.py
Scraper untuk semua sumber RSS berita lokal (lihat sources_config.RSS_SOURCES).
Satu class ini dipakai untuk SEMUA sumber RSS -- bedanya cuma feed_url, jadi gak perlu
bikin file terpisah per portal berita.

Cara jalanin manual buat testing (dari folder ingestion/):
    python -m scrapers.rss_scraper
"""

import re
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import feedparser

from scrapers.base_scraper import BaseScraper
from scrapers.sources_config import RSS_SOURCES, RELEVANT_KEYWORDS


def is_relevant(text: str) -> bool:
    """Filter sederhana: simpan kalau ada minimal 1 keyword keluhan publik di judul/snippet."""
    text_lower = text.lower()
    return any(kw in text_lower for kw in RELEVANT_KEYWORDS)


def clean_html(raw_html: str) -> str:
    """RSS summary sering masih ada tag HTML, dibersihkan biar raw_text rapi."""
    if not raw_html:
        return ""
    text = re.sub(r"<[^>]+>", " ", raw_html)
    return re.sub(r"\s+", " ", text).strip()


class RSSScraper(BaseScraper):
    source_type = "rss"

    def __init__(self, source_name: str, feed_url: str, category_hint: str = ""):
        super().__init__()
        self.source_name = source_name
        self.feed_url = feed_url
        self.category_hint = category_hint

    def fetch(self) -> list[dict]:
        parsed = feedparser.parse(self.feed_url)
        if parsed.bozo:
            # bozo=True artinya feed agak rusak/tidak standar -- jangan langsung crash,
            # feedparser biasanya tetap berhasil ambil sebagian entries.
            self.logger.warning(f"Feed '{self.source_name}' bozo (mungkin format tidak standar): {parsed.bozo_exception}")
        return parsed.entries

    def normalize(self, raw_items: list[dict]) -> list[dict]:
        records = []
        for entry in raw_items:
            title = entry.get("title", "")
            summary = clean_html(entry.get("summary", "") or entry.get("description", ""))
            raw_text = f"{title}. {summary}".strip()

            if not is_relevant(raw_text):
                continue  # bukan soal keluhan publik (mis. berita olahraga) -> skip, gak masuk bronze

            url = entry.get("link")
            published = entry.get("published", "") or entry.get("updated", "") or ""

            records.append({
                "id": self.make_id(self.source_name, url or title),
                "source_type": self.source_type,
                "source_name": self.source_name,
                "raw_text": raw_text,
                "author": entry.get("author"),
                "url": url,
                "likes": 0,
                "shares": 0,
                "published_at": published,
                "scraped_at": self.now_iso(),
            })
        return records


def run_all_rss_sources() -> list[dict]:
    """Loop semua sumber di RSS_SOURCES, gabungkan hasilnya jadi satu list record."""
    all_records = []
    for src in RSS_SOURCES:
        scraper = RSSScraper(
            source_name=src["source_name"],
            feed_url=src["feed_url"],
            category_hint=src.get("category_hint", ""),
        )
        try:
            records = scraper.run()
            all_records.extend(records)
        except Exception as e:
            # 1 sumber gagal jangan sampai bikin sumber lain ikut gagal
            scraper.logger.error(f"Gagal scraping {src['source_name']}: {e}")
    return all_records


if __name__ == "__main__":
    results = run_all_rss_sources()
    print(f"\nTotal record relevan dari semua sumber RSS: {len(results)}\n")
    print(BaseScraper.to_jsonl(results[:5]))  # preview 5 pertama saja
