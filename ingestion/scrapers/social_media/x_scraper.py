"""
x_scraper.py
Scraper X (Twitter) via Twikit.
PENTING: Jaga rate limit! Jangan scraping terlalu agresif.

Setup: buat akun X dummy, isi X_USERNAME, X_EMAIL, X_PASSWORD di .env
"""

from __future__ import annotations

import asyncio
import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from twikit import Client
from scrapers.base_scraper import BaseScraper
from scrapers.sources_config import RELEVANT_KEYWORDS, X_SEARCH_QUERIES

COOKIES_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "x_cookies.json")


def is_relevant(text: str) -> bool:
    text_lower = text.lower()
    return any(kw in text_lower for kw in RELEVANT_KEYWORDS)


class XScraper(BaseScraper):
    source_type = "x"

    def __init__(self, username: str, email: str, password: str):
         super().__init__()
         self.username = username
         self.email = email
         self.password = password
         self.client = Client(language="id-ID", timeout=60.0)

    async def _login_or_load_cookies(self):
        if os.path.exists(COOKIES_PATH):
          self.logger.info("Load cookies dari file...")
          self.client.load_cookies(COOKIES_PATH)
        else:
            self.logger.error("File x_cookies.json tidak ditemukan!")
            raise FileNotFoundError("Taruh file x_cookies.json di folder social_media/")

    async def _fetch_async(self) -> list[dict]:
        await self._login_or_load_cookies()
        raw_items = []
        for query in X_SEARCH_QUERIES:
            try:
                tweets = await self.client.search_tweet(query, product="Latest", count=20)
                for tweet in tweets:
                    raw_items.append({"query": query, "tweet": tweet})
                await asyncio.sleep(3)  # jaga rate limit
            except Exception as e:
                self.logger.warning(f"Gagal search X '{query}': {e}")
        return raw_items

    def fetch(self) -> list[dict]:
        return asyncio.run(self._fetch_async())

    def normalize(self, raw_items: list[dict]) -> list[dict]:
        records = []
        seen_ids = set()
        for item in raw_items:
            tweet = item["tweet"]
            raw_text = tweet.text or ""

            if not is_relevant(raw_text):
                continue

            tweet_id = str(tweet.id)
            if tweet_id in seen_ids:
                continue
            seen_ids.add(tweet_id)

            records.append({
                "id": self.make_id("x", tweet_id),
                "source_type": self.source_type,
                "source_name": "x_twitter",
                "raw_text": raw_text[:1000],
                "author": tweet.user.screen_name if tweet.user else None,
                "url": f"https://x.com/i/web/status/{tweet_id}",
                "likes": tweet.favorite_count or 0,
                "shares": tweet.retweet_count or 0,
                "published_at": str(tweet.created_at) if tweet.created_at else "",
                "scraped_at": self.now_iso(),
            })
        return records


def run_x_scraper() -> list[dict]:
    from dotenv import load_dotenv, find_dotenv
    load_dotenv(find_dotenv())

    username = os.getenv("X_USERNAME")
    email = os.getenv("X_EMAIL")
    password = os.getenv("X_PASSWORD")

    if not username or not email or not password:
        import logging
        logging.getLogger("XScraper").error("X_USERNAME, X_EMAIL, atau X_PASSWORD belum diset di .env!")
        return []

    scraper = XScraper(username, email, password)
    return scraper.run()


if __name__ == "__main__":
    results = run_x_scraper()
    print(f"\nTotal record relevan dari X: {len(results)}\n")
    from scrapers.base_scraper import BaseScraper as BS
    print(BS.to_jsonl(results[:3]))