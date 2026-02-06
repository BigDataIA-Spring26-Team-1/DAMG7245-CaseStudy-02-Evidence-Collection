from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.config import settings
from app.services.snowflake import get_snowflake_connection
from app.services.signal_store import SignalStore
from app.pipelines.external_signals import ExternalSignalCollector, sha256_text

# TechStackCollector is optional depending on your file.
# We'll import if available and fallback gracefully if not.
try:
    from app.pipelines.external_signals import TechStackCollector  # type: ignore
except Exception:  # pragma: no cover
    TechStackCollector = None  # type: ignore


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

# Optional job board tokens; leave blank to use RSS fallback.
JOB_BOARD_TOKENS: dict[str, dict[str, str]] = {t: {"greenhouse": "", "lever": ""} for t in DEFAULT_COMPANIES.keys()}


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


def _write_text(path: Path, text: str, limit: int = 20000) -> None:
    path.write_text((text or "")[:limit], encoding="utf-8", errors="ignore")


def _write_json(path: Path, obj: Any) -> None:
    path.write_text(json.dumps(obj, indent=2, sort_keys=True), encoding="utf-8", errors="ignore")


def _safe_get_patents_rss(collector: ExternalSignalCollector, query: str) -> Tuple[Optional[str], Optional[str], str]:
    """
    Returns (url, rss_text, source_label).
    We try a dedicated method if your collector has it; otherwise we fallback to Google News RSS with a "patent" query.
    """
    # If you implemented something like patents_uspto_stub(query)
    if hasattr(collector, "patents_uspto_stub"):
        fn = getattr(collector, "patents_uspto_stub")
        url, rss = fn(query)
        return url, rss, "uspto_stub_rss"

    # If you implemented something like google_patents_rss(query)
    if hasattr(collector, "google_patents_rss"):
        fn = getattr(collector, "google_patents_rss")
        url, rss = fn(query)
        return url, rss, "google_patents_rss"

    # Fallback (still external)
    # This is not “USPTO API”, but it keeps the pipeline end-to-end and produces external patent-related signals.
    url, rss = collector.google_news_rss(f"{query} patent")
    return url, rss, "google_news_rss_patent_fallback"


def _extract_tech_counts(collector: ExternalSignalCollector, tech_obj: Any, blob: str) -> Dict[str, int]:
    """
    Supports multiple possible implementations:
    - TechStackCollector.extract(text) -> dict
    - collector.extract_tech_stack(text) -> dict
    """
    if not blob.strip():
        return {}

    # Preferred: TechStackCollector if present
    if tech_obj is not None:
        # expected method name: extract()
        if hasattr(tech_obj, "extract"):
            counts = tech_obj.extract(blob)
            return counts or {}
        # if someone used extract_tech_stack() inside collector class
        if hasattr(tech_obj, "extract_tech_stack"):
            counts = tech_obj.extract_tech_stack(blob)
            return counts or {}

    # Fallback to collector method if you placed it there
    if hasattr(collector, "extract_tech_stack"):
        counts = collector.extract_tech_stack(blob)
        return counts or {}

    return {}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="data/signals", help="Output folder for proof artifacts")
    ap.add_argument("--companies", required=True, help="Ticker list like CAT,DE or 'all'")
    args = ap.parse_args()

    tickers = (
        list(DEFAULT_COMPANIES.keys())
        if args.companies.lower().strip() == "all"
        else [t.strip().upper() for t in args.companies.split(",") if t.strip()]
    )

    collector = ExternalSignalCollector(user_agent=settings.sec_user_agent)
    store = SignalStore()
    tech = TechStackCollector() if TechStackCollector is not None else None

    try:
        for ticker in tickers:
            out_dir = ROOT / args.out / ticker
            out_dir.mkdir(parents=True, exist_ok=True)

            if ticker not in DEFAULT_COMPANIES:
                print(f"SKIP: unknown ticker {ticker}")
                continue

            company_id = get_company_id(ticker)
            company_name = DEFAULT_COMPANIES[ticker]

            # Keep these defined no matter which branch runs
            jobs_rss: str = ""
            news_rss: str = ""
            patents_rss: str = ""

            # =========================================================
            # 1) JOBS signals (Greenhouse / Lever / RSS fallback)
            # =========================================================
            tokens = JOB_BOARD_TOKENS.get(ticker, {})
            gh = (tokens.get("greenhouse") or "").strip()
            lv = (tokens.get("lever") or "").strip()

            jobs: list[dict] = []
            source_used: Optional[str] = None

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

            inserted_jobs = 0
            if jobs:
                for j in jobs[:50]:
                    title = (j.get("title") or "").strip()
                    url = j.get("url")
                    published_at = j.get("published_at")

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
                    inserted_jobs += 1

                # Proof artifact: small summary JSON
                _write_json(out_dir / "jobs_board_sample.json", {"source": source_used, "inserted": inserted_jobs, "sample": jobs[:3]})
                print(f"STORED: {ticker} jobs inserted={inserted_jobs} source={source_used}")

            else:
                jobs_q = f"{company_name} {ticker} hiring jobs"
                jobs_url, jobs_rss = collector.google_jobs_rss(jobs_q)
                _write_text(out_dir / "jobs_rss.txt", jobs_rss)

                if jobs_rss:
                    jobs_hash = sha256_text(f"jobs_rss|{ticker}|{jobs_rss}")
                    if not store.signal_exists_by_hash(jobs_hash):
                        store.insert_signal(
                            company_id=company_id,
                            ticker=ticker,
                            signal_type="jobs",
                            source="google_jobs_rss_fallback",
                            title=f"{company_name} jobs RSS",
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
            news_q = f"{company_name} {ticker}"
            news_url, news_rss = collector.google_news_rss(news_q)
            _write_text(out_dir / "news_rss.txt", news_rss)

            if news_rss:
                news_hash = sha256_text(f"news_rss|{ticker}|{news_rss}")
                if store.signal_exists_by_hash(news_hash):
                    print(f"SKIP: {ticker} news rss already stored (hash={news_hash[:10]})")
                else:
                    store.insert_signal(
                        company_id=company_id,
                        ticker=ticker,
                        signal_type="news",
                        source="google_news_rss",
                        title=f"{company_name} news RSS",
                        url=news_url,
                        published_at=None,
                        content_text=news_rss[:20000],
                        content_hash=news_hash,
                        metadata={"query": news_q, "note": "rss stored (truncated to 20k)"},
                    )
                    print(f"STORED: {ticker} news rss hash={news_hash[:10]}")
            else:
                print(f"SKIP: {ticker} no news rss returned for query={news_q}")

            # =========================================================
            # 3) TECH STACK signals (external-only proxy)
            # =========================================================
            tech_blob = "\n".join([x for x in [news_rss, jobs_rss] if x])
            tech_counts = _extract_tech_counts(collector, tech, tech_blob)
            _write_json(out_dir / "tech_counts.json", tech_counts)

            if tech_counts:
                tech_hash = sha256_text(f"tech|{ticker}|" + json.dumps(tech_counts, sort_keys=True))
                if store.signal_exists_by_hash(tech_hash):
                    print(f"SKIP: {ticker} tech stack already stored (hash={tech_hash[:10]})")
                else:
                    store.insert_signal(
                        company_id=company_id,
                        ticker=ticker,
                        signal_type="tech",
                        source="tech_stack_extractor",
                        title=f"{company_name} tech stack (extracted)",
                        url=None,
                        published_at=None,
                        content_text=None,
                        content_hash=tech_hash,
                        metadata={
                            "counts": tech_counts,
                            "note": "tech extracted from external RSS blobs (jobs/news)",
                        },
                    )
                    print(f"STORED: {ticker} tech stack hash={tech_hash[:10]}")
            else:
                print(f"SKIP: {ticker} no tech keywords found in external blobs")

            # =========================================================
            # 4) PATENTS signals (external)
            # =========================================================
            pat_q = f"{company_name} {ticker}"
            pat_url, pat_rss, pat_source = _safe_get_patents_rss(collector, pat_q)
            patents_rss = pat_rss or ""
            _write_text(out_dir / "patents_rss.txt", patents_rss)

            if patents_rss:
                pat_hash = sha256_text(f"patents_rss|{ticker}|{patents_rss}")
                if store.signal_exists_by_hash(pat_hash):
                    print(f"SKIP: {ticker} patents rss already stored (hash={pat_hash[:10]})")
                else:
                    store.insert_signal(
                        company_id=company_id,
                        ticker=ticker,
                        signal_type="patents",
                        source=pat_source,
                        title=f"{company_name} patents RSS",
                        url=pat_url,
                        published_at=None,
                        content_text=patents_rss[:20000],
                        content_hash=pat_hash,
                        metadata={"query": pat_q, "note": "patents rss stored (truncated to 20k)"},
                    )
                    print(f"STORED: {ticker} patents rss hash={pat_hash[:10]} source={pat_source}")
            else:
                print(f"SKIP: {ticker} no patents rss returned for query={pat_q}")

        print("\n✅ OK: External signals collection completed")
        return 0

    finally:
        try:
            collector.close()
        except Exception:
            pass
        store.close()


if __name__ == "__main__":
    raise SystemExit(main())