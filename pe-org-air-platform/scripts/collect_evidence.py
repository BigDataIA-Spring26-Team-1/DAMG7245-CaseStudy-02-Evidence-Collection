from __future__ import annotations

import argparse
import sys
from pathlib import Path
from uuid import uuid4
from app.config import settings

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
 
from app.pipelines.sec_edgar import SecEdgarClient, store_raw_filing
from app.pipelines.document_parser import parse_filing_bytes, chunk_document
from app.services.evidence_store import EvidenceStore, DocumentRow, ChunkRow
from app.services.snowflake import get_snowflake_connection
 
DEFAULT_TICKERS = [
    "CAT", "DE", "UNH", "HCA", "ADP",
    "PAYX", "WMT", "TGT", "JPM", "GS",
]
 
TARGET_FORMS = ["10-K", "10-Q", "8-K"]
 
 
def get_company_id_for_ticker(ticker: str) -> str:
    conn = get_snowflake_connection()
    cur = conn.cursor()
    try:
        cur.execute("SELECT id FROM companies WHERE ticker = %s LIMIT 1", (ticker,))
        row = cur.fetchone()
        if not row:
            raise RuntimeError(
                f"Company not found in companies table for ticker={ticker}. Run backfill_companies.py"
            )
        return str(row[0])
    finally:
        cur.close()
        conn.close()
 
 
def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--companies", required=True, help="Ticker list like CAT,DE or 'all'")
    parser.add_argument("--out", default="data/processed", help="Output folder for parsed artifacts")
    args = parser.parse_args()
 
    if args.companies.lower().strip() == "all":
        tickers = DEFAULT_TICKERS
    else:
        tickers = [t.strip().upper() for t in args.companies.split(",") if t.strip()]
 
    base_dir = ROOT
 
    user_agent = settings.sec_user_agent
 
    client = SecEdgarClient(user_agent=user_agent, rate_limit_per_sec=5.0)
    store = EvidenceStore()
 
    try:
        cur = store.conn.cursor()
        cur.execute("SELECT CURRENT_DATABASE(), CURRENT_SCHEMA()")
        print("EvidenceStore session:", cur.fetchone())
        cur.close()
 
        conn2 = get_snowflake_connection()
        cur2 = conn2.cursor()
        cur2.execute("SELECT CURRENT_DATABASE(), CURRENT_SCHEMA()")
        print("get_snowflake_connection session:", cur2.fetchone())
        cur2.close()
        conn2.close()
 
        ticker_map = client.get_ticker_to_cik_map()
 
        for ticker in tickers:
            print(f"\n=== Processing {ticker} ===")
 
            cik = ticker_map.get(ticker)
            if not cik:
                print(f"SKIP: Ticker not found in SEC map: {ticker}")
                continue
 
            try:
                company_id = get_company_id_for_ticker(ticker)
            except Exception as e:
                print(f"SKIP: {ticker} not found in companies table ({e})")
                continue
 
            filings = client.list_recent_filings(
                ticker=ticker,
                cik_10=cik,
                forms=TARGET_FORMS,
                limit_per_form=1,
            )
            if not filings:
                print(f"SKIP: No filings found for {ticker}")
                continue
 
            out_dir = base_dir / args.out / ticker
            out_dir.mkdir(parents=True, exist_ok=True)
 
            for f in filings:
                raw = client.download_primary_document(f)
                raw_path = store_raw_filing(base_dir, f, raw)

                parsed = parse_filing_bytes(raw, file_hint=str(raw_path))
                chunks = chunk_document(parsed)

                if store.document_exists_by_hash(parsed.content_hash):
                    print(
                        f"SKIP: {ticker} {f.form} {f.filing_date} already processed "
                        f"(hash={parsed.content_hash[:10]})"
                    )
                    continue

                if chunks and len(chunks) > 1:
                    print("Overlap proof:")
                    print("chunk0_end:", chunks[0].content[-200:].replace("\n", " "))
                    print("chunk1_start:", chunks[1].content[:200].replace("\n", " "))

 
                doc_id = str(uuid4())
                doc_row = DocumentRow(
                    id=doc_id,
                    company_id=company_id,
                    ticker=ticker,
                    filing_type=f.form,
                    filing_date=f.filing_date,
                    source_url=f"{f.filing_dir_url}/{f.primary_doc}",
                    local_path=str(raw_path),
                    content_hash=parsed.content_hash,
                    word_count=parsed.word_count,
                    chunk_count=len(chunks),
                )
 
                store.insert_document(doc_row)
 
                chunk_rows = [
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
                store.insert_chunks_bulk(chunk_rows)
 
                print(
                    f"STORED: {ticker} {f.form} {f.filing_date} doc_id={doc_id} "
                    f"chunks={len(chunk_rows)}"
                )
 
                (out_dir / f"{f.form}_{f.filing_date}_{f.accession}.txt").write_text(
                    parsed.sections.get("Item 1A") or parsed.full_text[:20000],
                    encoding="utf-8",
                    errors="ignore",
                )
                (out_dir / f"{f.form}_{f.filing_date}_{f.accession}_chunks.txt").write_text(
                    "\n\n--- CHUNK ---\n\n".join([c.content[:1500] for c in chunks[:10]]),
                    encoding="utf-8",
                    errors="ignore",
                )
 
                print(
                    f"{ticker} {f.form} {f.filing_date} saved raw={raw_path} "
                    f"hash={parsed.content_hash[:10]} words={parsed.word_count} chunks={len(chunks)}"
                )
 
        print("\n OK: Evidence collection completed")
        return 0
 
    finally:
        client.close()
        store.close()
 
 
if __name__ == "__main__":
    raise SystemExit(main())
