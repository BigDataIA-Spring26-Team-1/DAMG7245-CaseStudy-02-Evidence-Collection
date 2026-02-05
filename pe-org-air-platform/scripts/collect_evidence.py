from __future__ import annotations

import argparse
from pathlib import Path

from app.pipelines.sec_edgar import SecEdgarClient, store_raw_filing
from app.pipelines.document_parser import parse_filing_bytes, chunk_document


TARGET_FORMS = ["10-K", "10-Q", "8-K"]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--companies", required=True, help="Ticker list like CAT,DE or 'one'")
    parser.add_argument("--out", default="data/processed", help="Output folder for parsed JSON/text")
    args = parser.parse_args()

    # Start with one ticker for proof
    ticker = args.companies.split(",")[0].strip().upper()

    # Set your SEC User-Agent here temporarily; later move to settings/.env
    user_agent = "PE-OrgAIR (Northeastern) yourname@northeastern.edu"

    client = SecEdgarClient(user_agent=user_agent, rate_limit_per_sec=5.0)

    try:
        ticker_map = client.get_ticker_to_cik_map()
        cik = ticker_map.get(ticker)
        if not cik:
            raise SystemExit(f"Ticker not found in SEC map: {ticker}")

        filings = client.list_recent_filings(ticker=ticker, cik_10=cik, forms=TARGET_FORMS, limit_per_form=1)
        if not filings:
            raise SystemExit(f"No filings found for {ticker}")

        base_dir = Path(".")
        out_dir = Path(args.out) / ticker
        out_dir.mkdir(parents=True, exist_ok=True)

        for f in filings:
            raw = client.download_primary_document(f)
            raw_path = store_raw_filing(base_dir, f, raw)

            parsed = parse_filing_bytes(raw, file_hint=str(raw_path))
            chunks = chunk_document(parsed)

            # Save a small proof artifact
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

        print("OK: EDGAR download + parse + chunk proof generated in data/processed/")
        return 0
    finally:
        client.close()


if __name__ == "__main__":
    raise SystemExit(main())
