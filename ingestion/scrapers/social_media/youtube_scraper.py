"""
youtube_scraper.py
Scraper YouTube via YouTube Data API v3 (resmi, gratis 10k unit/hari).
"""

import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from googleapiclient.discovery import build
from scrapers.base_scraper import BaseScraper
from scrapers.sources_config import RELEVANT_KEYWORDS, YOUTUBE_SEARCH_QUERIES


def is_relevant(text: str) -> bool:
    text_lower = text.lower()
    return any(kw in text_lower for kw in RELEVANT_KEYWORDS)


class YouTubeScraper(BaseScraper):
    source_type = "youtube"

    def __init__(self, api_key: str):
        super().__init__()
        self.youtube = build("youtube", "v3", developerKey=api_key)

    def fetch(self) -> list[dict]:
        raw_items = []
        for query in YOUTUBE_SEARCH_QUERIES:
            try:
                response = self.youtube.search().list(
                    q=query,
                    part="snippet",
                    type="video",
                    order="date",
                    maxResults=10,
                    relevanceLanguage="id",
                    regionCode="ID",
                ).execute()
                for item in response.get("items", []):
                    raw_items.append({"query": query, "item": item})
            except Exception as e:
                self.logger.warning(f"Gagal search YouTube '{query}': {e}")
        self.logger.info(f"Total {len(raw_items)} video mentah dari YouTube")
        return raw_items

    def normalize(self, raw_items: list[dict]) -> list[dict]:
        records = []
        seen_ids = set()
        for raw in raw_items:
            item = raw["item"]
            snippet = item.get("snippet", {})
            video_id = item.get("id", {}).get("videoId", "")

            if not video_id or video_id in seen_ids:
                continue
            seen_ids.add(video_id)

            title = snippet.get("title", "")
            description = snippet.get("description", "")
            raw_text = f"{title}. {description}".strip()

            if not is_relevant(raw_text):
                continue

            records.append({
                "id": self.make_id("youtube", video_id),
                "source_type": self.source_type,
                "source_name": "youtube_search",
                "raw_text": raw_text[:2000],
                "author": snippet.get("channelTitle"),
                "url": f"https://www.youtube.com/watch?v={video_id}",
                "likes": 0,
                "shares": 0,
                "published_at": snippet.get("publishedAt", ""),
                "scraped_at": self.now_iso(),
            })
        return records


def run_youtube_scraper() -> list[dict]:
    from dotenv import load_dotenv, find_dotenv
    load_dotenv(find_dotenv())

    api_key = os.getenv("YOUTUBE_API_KEY")
    if not api_key:
        import logging
        logging.getLogger("YouTubeScraper").error("YOUTUBE_API_KEY belum diset di .env!")
        return []

    scraper = YouTubeScraper(api_key)
    return scraper.run()


if __name__ == "__main__":
    results = run_youtube_scraper()
    print(f"\nTotal record relevan dari YouTube: {len(results)}\n")
    from scrapers.base_scraper import BaseScraper as BS
    print(BS.to_jsonl(results[:3]))