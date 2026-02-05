from __future__ import annotations
 
import hashlib
from urllib.parse import quote
 
import httpx
 
 
def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8", errors="ignore")).hexdigest()
 
 
class ExternalSignalCollector:
    def __init__(self, user_agent: str):
        self.client = httpx.Client(
            headers={"User-Agent": user_agent, "Accept": "*/*"},
            timeout=30.0,
        )
 
    def close(self):
        self.client.close()
 
    def google_news_rss(self, query: str) -> tuple[str, str]:
        q = quote(query)
        url = f"https://news.google.com/rss/search?q={q}&hl=en-US&gl=US&ceid=US:en"
        r = self.client.get(url)
        r.raise_for_status()
        return url, r.text