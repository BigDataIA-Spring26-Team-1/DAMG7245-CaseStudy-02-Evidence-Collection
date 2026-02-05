from __future__ import annotations
 
import argparse
import sys
from pathlib import Path
from app.config import settings

 
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
 
from app.services.snowflake import get_snowflake_connection
from app.services.signal_store import SignalStore
from app.pipelines.external_signals import ExternalSignalCollector, sha256_text
 
 
DEFAULT_COMPANIES = {
    "CAT": "Caterpillar",
    "DE": "Deere",
    "UNH": "UnitedHealth",
    "HCA": "HCA Healthcare",
    "ADP": "ADP",
    "PAYX": "Paychex",
    "WMT": "Walmart",
    "TGT": "Target",
    "JPM": "JPMorgan",
    "GS": "Goldman Sachs",
}
 
 
def get_company_id(ticker: str) -> str:
    conn = get_snowflake_connection()
    cur = conn.cursor()
    try:
        cur.execute("SELECT id FROM companies WHERE ticker=%s LIMIT 1", (ticker,))
        row = cur.fetchone()
        if not row:
            raise RuntimeError(f"Missing company row for {ticker}. Run backfill_companies.py")
        return str(row[0])
    finally:
        cur.close()
        conn.close()
 
 
def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--companies", required=True, help="Ticker list like CAT,DE or 'all'")
    args = ap.parse_args()
 
    tickers = list(DEFAULT_COMPANIES.keys()) if args.companies.lower().strip() == "all" else [
        t.strip().upper() for t in args.companies.split(",") if t.strip()
    ]
 
    collector = ExternalSignalCollector(
    user_agent=settings.sec_user_agent
    )
    store = SignalStore()
 
    try:
        for ticker in tickers:
            if ticker not in DEFAULT_COMPANIES:
                print(f"SKIP: unknown ticker {ticker}")
                continue
 
            company_id = get_company_id(ticker)
            q = f"{DEFAULT_COMPANIES[ticker]} {ticker}"
 
            url, rss = collector.google_news_rss(q)
            h = sha256_text(rss)
 
            if store.exists_by_hash(h):
                print(f"SKIP: {ticker} rss already stored (hash={h[:10]})")
                continue
 
            store.insert_signal(
                company_id=company_id,
                ticker=ticker,
                signal_type="news",
                source="google_news_rss",
                title=f"{DEFAULT_COMPANIES[ticker]} news RSS",
                url=url,
                published_at=None,
                content_text=rss[:20000],
                content_hash=h,
                metadata={"query": q, "note": "raw rss stored (truncated to 20k)"},
            )
            print(f"STORED: {ticker} news rss hash={h[:10]}")
 
        return 0
    finally:
        collector.close()
        store.close()
 
 
if __name__ == "__main__":
    raise SystemExit(main())