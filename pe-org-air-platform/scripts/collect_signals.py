from __future__ import annotations
 
import argparse

import json

import sys

from pathlib import Path
 
ROOT = Path(__file__).resolve().parents[1]

if str(ROOT) not in sys.path:

    sys.path.insert(0, str(ROOT))
 
from app.config import settings

from app.services.snowflake import get_snowflake_connection

from app.services.signal_store import SignalStore

from app.pipelines.external_signals import ExternalSignalCollector, sha256_text
 
 
# Keep your 10 required companies here (matches rubric)

DEFAULT_COMPANIES: dict[str, str] = {

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
 
# Optional: if you know the exact job board tokens, fill them in.

# If empty, we fallback to RSS-based “jobs signal” so pipeline still runs end-to-end.

JOB_BOARD_TOKENS: dict[str, dict[str, str]] = {

    "CAT": {"greenhouse": "", "lever": ""},

    "DE": {"greenhouse": "", "lever": ""},

    "UNH": {"greenhouse": "", "lever": ""},

    "HCA": {"greenhouse": "", "lever": ""},

    "ADP": {"greenhouse": "", "lever": ""},

    "PAYX": {"greenhouse": "", "lever": ""},

    "WMT": {"greenhouse": "", "lever": ""},

    "TGT": {"greenhouse": "", "lever": ""},

    "JPM": {"greenhouse": "", "lever": ""},

    "GS": {"greenhouse": "", "lever": ""},

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
 
    tickers = (

        list(DEFAULT_COMPANIES.keys())

        if args.companies.lower().strip() == "all"

        else [t.strip().upper() for t in args.companies.split(",") if t.strip()]

    )
 
    collector = ExternalSignalCollector(user_agent=settings.sec_user_agent)

    store = SignalStore()
 
    try:

        for ticker in tickers:

            if ticker not in DEFAULT_COMPANIES:

                print(f"SKIP: unknown ticker {ticker}")

                continue
 
            company_id = get_company_id(ticker)
 
            # =========================================================

            # 1) JOBS signals (Greenhouse / Lever / RSS fallback)

            # =========================================================

            tokens = JOB_BOARD_TOKENS.get(ticker, {})

            gh = (tokens.get("greenhouse") or "").strip()

            lv = (tokens.get("lever") or "").strip()
 
            jobs: list[dict] = []

            source_used: str | None = None
 
            try:

                if gh:

                    jobs = collector.greenhouse_jobs(gh)

                    source_used = "greenhouse"

                elif lv:

                    jobs = collector.lever_jobs(lv)

                    source_used = "lever"

            except Exception as e:

                print(f"WARN: {ticker} jobs board fetch failed ({e}); falling back to RSS")

                jobs = []

                source_used = None
 
            if jobs:

                inserted = 0

                for j in jobs[:50]:  # safety cap

                    title = (j.get("title") or "").strip()

                    url = j.get("url")

                    published_at = j.get("published_at")
 
                    # stable hash per job

                    content_hash = sha256_text(f"jobs|{ticker}|{title}|{url or ''}")
 
                    if store.signal_exists_by_hash(content_hash):

                        continue
 
                    store.insert_signal(

                        company_id=company_id,

                        ticker=ticker,

                        signal_type="jobs",

                        source=source_used or "job_board",

                        title=title[:500] if title else None,

                        url=url,

                        published_at=published_at,

                        content_text=(json.dumps(j.get("raw", {}))[:20000] if j.get("raw") else None),

                        content_hash=content_hash,

                        metadata={

                            "location": j.get("location"),

                            "department": j.get("department"),

                            "collector": source_used,

                        },

                    )

                    inserted += 1
 
                print(f"STORED: {ticker} jobs inserted={inserted} source={source_used}")
 
            else:

                # RSS fallback jobs signal

                jobs_q = f"{DEFAULT_COMPANIES[ticker]} {ticker} hiring jobs"

                jobs_url, jobs_rss = collector.google_jobs_rss(jobs_q)
 
                if jobs_rss:

                    jobs_hash = sha256_text(f"jobs_rss|{ticker}|{jobs_rss}")

                    if not store.signal_exists_by_hash(jobs_hash):

                        store.insert_signal(

                            company_id=company_id,

                            ticker=ticker,

                            signal_type="jobs",

                            source="google_jobs_rss_fallback",

                            title=f"{DEFAULT_COMPANIES[ticker]} jobs RSS",

                            url=jobs_url,

                            published_at=None,

                            content_text=jobs_rss[:20000],

                            content_hash=jobs_hash,

                            metadata={"query": jobs_q, "note": "fallback rss stored (truncated to 20k)"},

                        )

                        print(f"STORED: {ticker} jobs rss hash={jobs_hash[:10]}")

                    else:

                        print(f"SKIP: {ticker} jobs rss already stored (hash={jobs_hash[:10]})")

                else:

                    print(f"SKIP: {ticker} no jobs rss returned for query={jobs_q}")
 
            # =========================================================

            # 2) NEWS signals (Google News RSS)

            # =========================================================

            news_q = f"{DEFAULT_COMPANIES[ticker]} {ticker}"

            news_url, news_rss = collector.google_news_rss(news_q)
 
            if not news_rss:

                print(f"SKIP: {ticker} no news rss returned for query={news_q}")

                continue
 
            news_hash = sha256_text(f"news_rss|{ticker}|{news_rss}")

            if store.signal_exists_by_hash(news_hash):

                print(f"SKIP: {ticker} news rss already stored (hash={news_hash[:10]})")

                continue
 
            store.insert_signal(

                company_id=company_id,

                ticker=ticker,

                signal_type="news",

                source="google_news_rss",

                title=f"{DEFAULT_COMPANIES[ticker]} news RSS",

                url=news_url,

                published_at=None,

                content_text=news_rss[:20000],

                content_hash=news_hash,

                metadata={"query": news_q, "note": "rss stored (truncated to 20k)"},

            )

            print(f"STORED: {ticker} news rss hash={news_hash[:10]}")
 
        return 0
 
    finally:

        try:

            collector.close()

        except Exception:

            pass

        store.close()
 
 
if __name__ == "__main__":

    raise SystemExit(main())

 