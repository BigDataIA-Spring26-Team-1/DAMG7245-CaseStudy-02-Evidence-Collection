from __future__ import annotations
 
import re

import json

import httpx

from urllib.parse import quote_plus
 
from datetime import datetime

from typing import Any, Dict, List, Optional, Tuple

from email.utils import parsedate_to_datetime
 
# If you already have sha256_text in this file, keep yours and delete this duplicate.

import hashlib

def sha256_text(text: str) -> str:

    return hashlib.sha256(text.encode("utf-8", errors="ignore")).hexdigest()
 
 
def _safe_dt(x: Optional[str]) -> Optional[datetime]:

    if not x:

        return None

    try:

        # RSS pubDate

        return parsedate_to_datetime(x)

    except Exception:

        pass

    try:

        # ISO-ish

        return datetime.fromisoformat(x.replace("Z", "+00:00"))

    except Exception:

        return None
 
 
class ExternalSignalCollector:
    def __init__(self, user_agent: str):
        self.user_agent = user_agent
        self.client = httpx.Client(
            headers={"User-Agent": user_agent},
            timeout=30.0,
            follow_redirects=True,
        )

    def close(self) -> None:
        self.client.close()

    def greenhouse_jobs(self, board_token: str) -> List[Dict[str, Any]]:
        url = f"https://boards-api.greenhouse.io/v1/boards/{board_token}/jobs"
        r = self.client.get(url)
        r.raise_for_status()
        data = r.json()
        out = []
        for j in data.get("jobs", []):
            out.append({
                "title": j.get("title"),
                "url": j.get("absolute_url"),
                "published_at": j.get("updated_at") or j.get("created_at"),
                "location": (j.get("location") or {}).get("name"),
                "department": (j.get("departments") or [{}])[0].get("name") if j.get("departments") else None,
                "raw": j,
            })
        return out

    def lever_jobs(self, company: str) -> List[Dict[str, Any]]:
        url = f"https://api.lever.co/v0/postings/{company}?mode=json"
        r = self.client.get(url)
        r.raise_for_status()
        jobs = r.json()
        out = []
        for j in jobs:
            out.append({
                "title": j.get("text"),
                "url": j.get("hostedUrl") or j.get("applyUrl"),
                "published_at": j.get("createdAt"),
                "location": (j.get("categories") or {}).get("location"),
                "department": (j.get("categories") or {}).get("department"),
                "raw": j,
            })
        return out

    def google_jobs_rss(self, query: str) -> tuple[str, str]:
        # Using Google News RSS here to avoid the 400 error from the Alerts feed
        url = f"https://news.google.com/rss/search?q={quote_plus(query)}&hl=en-US&gl=US&ceid=US:en"
        r = self.client.get(url)
        r.raise_for_status()
        return url, r.text or ""

    def google_news_rss(self, query: str) -> tuple[str, str]:
        url = f"https://news.google.com/rss/search?q={quote_plus(query)}&hl=en-US&gl=US&ceid=US:en"
        r = self.client.get(url)
        r.raise_for_status()
        return url, r.text or ""