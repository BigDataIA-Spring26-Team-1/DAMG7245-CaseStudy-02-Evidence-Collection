from __future__ import annotations
from uuid import uuid4
from fastapi import APIRouter, BackgroundTasks, Query
from app.config import settings
from app.services.snowflake import get_snowflake_connection
from app.services.evidence_store import EvidenceStore, DocumentRow, ChunkRow, DocumentStatus
from app.services.signal_store import SignalStore
from app.pipelines.sec_edgar import SecEdgarClient, store_raw_filing
from app.pipelines.document_parser import parse_filing_bytes, chunk_document
from app.pipelines.external_signals import ExternalSignalCollector, sha256_text
from pathlib import Path
router = APIRouter(prefix="/collection")
DEFAULT_TICKERS = ["CAT", "DE", "UNH", "HCA", "ADP", "PAYX", "WMT", "TGT", "JPM", "GS"]
DEFAULT_COMPANY_NAMES = {
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
TASKS: dict[str, dict] = {}

def _get_company_id(ticker: str) -> str | None:
   conn = get_snowflake_connection()
   cur = conn.cursor()
   try:
       cur.execute("SELECT id FROM companies WHERE ticker=%s LIMIT 1", (ticker,))
       row = cur.fetchone()
       return str(row[0]) if row else None
   finally:
       cur.close()
       conn.close()

def run_collect_evidence(task_id: str, companies: list[str]) -> None:
   TASKS[task_id] = {"status": "running", "type": "evidence", "companies": companies, "message": ""}
   root = Path(__file__).resolve().parents[2]  # repo root-ish (app/routers -> app -> repo)
   client = SecEdgarClient(user_agent=settings.sec_user_agent, rate_limit_per_sec=5.0)
   store = EvidenceStore()
   try:
       ticker_map = client.get_ticker_to_cik_map()
       for ticker in companies:
           cik = ticker_map.get(ticker)
           if not cik:
               continue
           company_id = _get_company_id(ticker)
           if not company_id:
               continue
           filings = client.list_recent_filings(ticker=ticker, cik_10=cik, forms=["10-K", "10-Q", "8-K"], limit_per_form=1)
           for f in filings:
               doc_id = str(uuid4())
               source_url = f"{f.filing_dir_url}/{f.primary_doc}"
               raw_path = None
               content_hash = None
               try:
                   raw = client.download_primary_document(f)
                   raw_path = store_raw_filing(root, f, raw)
                   parsed = parse_filing_bytes(raw, file_hint=str(raw_path))
                   content_hash = parsed.content_hash
                   if store.document_exists_by_hash(content_hash):
                       continue
                   chunks = chunk_document(parsed)
                   store.insert_document(
                       DocumentRow(
                           id=doc_id,
                           company_id=company_id,
                           ticker=ticker,
                           filing_type=f.form,
                           filing_date=f.filing_date,
                           source_url=source_url,
                           local_path=str(raw_path),
                           content_hash=content_hash,
                           word_count=parsed.word_count,
                           chunk_count=len(chunks),
                           status=DocumentStatus.CHUNKED.value,
                       )
                   )
                   store.insert_chunks_bulk(
                       [
                           ChunkRow(
                               id=str(uuid4()),
                               document_id=doc_id,
                               chunk_index=c.chunk_index,
                               content=c.content,
                               section=c.section,
                               start_char=c.start_char,
                               end_char=c.end_char,
                               word_count=c.word_count,
                           )
                           for c in chunks
                       ]
                   )
                   store.update_document_status(doc_id, DocumentStatus.INDEXED.value)
               except Exception as e:
                   err = str(e)[:8000]
                   try:
                       store.update_document_status(doc_id, DocumentStatus.FAILED.value, error_message=err)
                   except Exception:
                       store.insert_failed_stub(
                           doc_id=doc_id,
                           company_id=company_id,
                           ticker=ticker,
                           filing_type=f.form,
                           filing_date=f.filing_date,
                           source_url=source_url,
                           local_path=str(raw_path) if raw_path else None,
                           content_hash=content_hash,
                           error_message=err,
                       )
                   continue
       TASKS[task_id]["status"] = "done"
       TASKS[task_id]["message"] = "Evidence collection completed"
   except Exception as e:
       TASKS[task_id]["status"] = "failed"
       TASKS[task_id]["message"] = str(e)
   finally:
       try:
           client.close()
       except Exception:
           pass
       store.close()

def run_collect_signals(task_id: str, companies: list[str]) -> None:
   TASKS[task_id] = {"status": "running", "type": "signals", "companies": companies, "message": ""}
   collector = ExternalSignalCollector(user_agent=settings.sec_user_agent)
   store = SignalStore()
   try:
       for ticker in companies:
           company_id = _get_company_id(ticker)
           if not company_id:
               continue
           name = DEFAULT_COMPANY_NAMES.get(ticker, ticker)
           # Jobs RSS (fallback)
           jobs_q = f"{name} {ticker} hiring jobs"
           jobs_url, jobs_rss = collector.google_jobs_rss(jobs_q)
           if jobs_rss:
               jobs_hash = sha256_text(f"jobs_rss|{ticker}|{jobs_rss}")
               if not store.signal_exists_by_hash(jobs_hash):
                   store.insert_signal(
                       company_id=company_id,
                       ticker=ticker,
                       signal_type="jobs",
                       source="google_jobs_rss_fallback",
                       title=f"{name} jobs RSS",
                       url=jobs_url,
                       published_at=None,
                       content_text=jobs_rss[:20000],
                       content_hash=jobs_hash,
                       metadata={"query": jobs_q, "note": "rss stored (truncated)"},
                   )
           # News RSS
           news_q = f"{name} {ticker}"
           news_url, news_rss = collector.google_news_rss(news_q)
           if news_rss:
               news_hash = sha256_text(f"news_rss|{ticker}|{news_rss}")
               if not store.signal_exists_by_hash(news_hash):
                   store.insert_signal(
                       company_id=company_id,
                       ticker=ticker,
                       signal_type="news",
                       source="google_news_rss",
                       title=f"{name} news RSS",
                       url=news_url,
                       published_at=None,
                       content_text=news_rss[:20000],
                       content_hash=news_hash,
                       metadata={"query": news_q, "note": "rss stored (truncated)"},
                   )
       TASKS[task_id]["status"] = "done"
       TASKS[task_id]["message"] = "Signals collection completed"
   except Exception as e:
       TASKS[task_id]["status"] = "failed"
       TASKS[task_id]["message"] = str(e)
   finally:
       try:
           collector.close()
       except Exception:
           pass
       store.close()

@router.post("/evidence")
def collect_evidence(background_tasks: BackgroundTasks, companies: str = Query(..., description="CAT,DE or all")):
   tickers = DEFAULT_TICKERS if companies.lower().strip() == "all" else [t.strip().upper() for t in companies.split(",") if t.strip()]
   task_id = str(uuid4())
   TASKS[task_id] = {"status": "queued", "type": "evidence", "companies": tickers, "message": "queued"}
   background_tasks.add_task(run_collect_evidence, task_id, tickers)
   return {"task_id": task_id, "status": "queued"}

@router.post("/signals")
def collect_signals(background_tasks: BackgroundTasks, companies: str = Query(..., description="CAT,DE or all")):
   tickers = DEFAULT_TICKERS if companies.lower().strip() == "all" else [t.strip().upper() for t in companies.split(",") if t.strip()]
   task_id = str(uuid4())
   TASKS[task_id] = {"status": "queued", "type": "signals", "companies": tickers, "message": "queued"}
   background_tasks.add_task(run_collect_signals, task_id, tickers)
   return {"task_id": task_id, "status": "queued"}

@router.get("/tasks/{task_id}")
def task_status(task_id: str):
   return TASKS.get(task_id, {"status": "unknown", "message": "task_id not found"})