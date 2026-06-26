"""
reddit_scraper.py
Scraper Reddit via PRAW (API resmi, gratis, stabil).
"""

from __future__ import annotations

import os
import sys
import datetime

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

import praw
from scrapers.base_scraper import BaseScraper
from scrapers.sources_config import RELEVANT_KEYWORDS, REDDIT_SUBREDDITS, REDDIT_SEARCH_KEYWORDS


def is_relevant(text: str) -> bool:
    text_lower = text.lower()
    return any(kw in text_lower for kw in RELEVANT_KEYWORDS)


class RedditScraper(BaseScraper):
    source_type = "reddit"

    def __init__(self, client_id: str, client_secret: str, user_agent: str):
        super().__init__()
        self.reddit = praw.Reddit(
            client_id=client_id,
            client_secret=client_secret,
            user_agent=user_agent,
        )

    def fetch(self) -> list[dict]:
        raw_items = []
        for subreddit_name in REDDIT_SUBREDDITS:
            subreddit = self.reddit.subreddit(subreddit_name)
            for keyword in REDDIT_SEARCH_KEYWORDS:
                try:
                    results = subreddit.search(keyword, sort="new", time_filter="week", limit=25)
                    for post in results:
                        raw_items.append({"subreddit": subreddit_name, "post": post})
                except Exception as e:
                    self.logger.warning(f"Gagal search '{keyword}' di r/{subreddit_name}: {e}")
        self.logger.info(f"Total {len(raw_items)} post mentah dari Reddit")
        return raw_items

    def normalize(self, raw_items: list[dict]) -> list[dict]:
        records = []
        seen_ids = set()
        for item in raw_items:
            post = item["post"]
            raw_text = f"{post.title}. {post.selftext or ''}".strip()

            if not is_relevant(raw_text):
                continue
            if post.id in seen_ids:
                continue
            seen_ids.add(post.id)

            published = datetime.datetime.fromtimestamp(
                post.created_utc, tz=datetime.timezone.utc
            ).isoformat()

            records.append({
                "id": self.make_id("reddit", post.id),
                "source_type": self.source_type,
                "source_name": f"reddit_r_{item['subreddit']}",
                "raw_text": raw_text[:2000],
                "author": str(post.author) if post.author else None,
                "url": f"https://reddit.com{post.permalink}",
                "likes": post.score,
                "shares": 0,
                "published_at": published,
                "scraped_at": self.now_iso(),
            })
        return records


def run_reddit_scraper() -> list[dict]:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(os.path.dirname(__file__), "../../../../.env"))

    client_id = os.getenv("REDDIT_CLIENT_ID")
    client_secret = os.getenv("REDDIT_CLIENT_SECRET")
    user_agent = os.getenv("REDDIT_USER_AGENT", "surabaya-complaint-ews/1.0")

    if not client_id or not client_secret:
        import logging
        logging.getLogger("RedditScraper").error("REDDIT_CLIENT_ID atau REDDIT_CLIENT_SECRET belum diset di .env!")
        return []

    scraper = RedditScraper(client_id, client_secret, user_agent)
    return scraper.run()


if __name__ == "__main__":
    results = run_reddit_scraper()
    print(f"\nTotal record relevan dari Reddit: {len(results)}\n")
    from scrapers.base_scraper import BaseScraper as BS
    print(BS.to_jsonl(results[:3]))