from __future__ import annotations

import argparse
import sys
from pathlib import Path
from uuid import uuid4

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.services.snowflake import get_snowflake_connection


DEFAULT_COMPANIES: dict[str, dict[str, str]] = {
    # Manufacturing
    "CAT": {
        "name": "Caterpillar Inc",
        "industry_id": "550e8400-e29b-41d4-a716-446655440001",
    },
    "DE": {
        "name": "Deere & Company",
        "industry_id": "550e8400-e29b-41d4-a716-446655440001",
    },
 
    # Healthcare Services
    "UNH": {
        "name": "UnitedHealth Group",
        "industry_id": "550e8400-e29b-41d4-a716-446655440002",
    },
    "HCA": {
        "name": "HCA Healthcare, Inc.",
        "industry_id": "550e8400-e29b-41d4-a716-446655440002",
    },
 
    # Business Services / Payroll
    "ADP": {
        "name": "Automatic Data Processing, Inc.",
        "industry_id": "550e8400-e29b-41d4-a716-446655440003",
    },
    "PAYX": {
        "name": "Paychex, Inc.",
        "industry_id": "550e8400-e29b-41d4-a716-446655440003",
    },
 
    # Retail
    "WMT": {
        "name": "Walmart Inc",
        "industry_id": "550e8400-e29b-41d4-a716-446655440004",
    },
    "TGT": {
        "name": "Target Corporation",
        "industry_id": "550e8400-e29b-41d4-a716-446655440004",
    },
 
    # Financial Services
    "JPM": {
        "name": "JPMorgan Chase & Co.",
        "industry_id": "550e8400-e29b-41d4-a716-446655440005",
    },
    "GS": {
        "name": "The Goldman Sachs Group, Inc.",
        "industry_id": "550e8400-e29b-41d4-a716-446655440005",
    },
}


def upsert_company(cur, ticker: str, name: str, industry_id: str) -> str:
    cur.execute("SELECT id FROM companies WHERE ticker = %s LIMIT 1", (ticker,))
    row = cur.fetchone()
    if row:
        company_id = str(row[0])
        cur.execute(
            """
            UPDATE companies
               SET name = %s,
                   industry_id = %s,
                   updated_at = CURRENT_TIMESTAMP()
             WHERE id = %s
            """,
            (name, industry_id, company_id),
        )
        return "updated"

    company_id = str(uuid4())
    cur.execute(
        """
        INSERT INTO companies (id, name, ticker, industry_id)
        VALUES (%s, %s, %s, %s)
        """,
        (company_id, name, ticker, industry_id),
    )
    return "inserted"


def main() -> int:
    parser = argparse.ArgumentParser(description="Seed companies into Snowflake.")
    parser.add_argument(
        "--companies",
        help="Comma-separated tickers (e.g., CAT,DE). If omitted, seeds defaults.",
    )
    args = parser.parse_args()

    if args.companies:
        requested = [t.strip().upper() for t in args.companies.split(",") if t.strip()]
        selected = {t: DEFAULT_COMPANIES[t] for t in requested if t in DEFAULT_COMPANIES}
        missing = [t for t in requested if t not in DEFAULT_COMPANIES]
    else:
        selected = DEFAULT_COMPANIES
        missing = []

    if not selected:
        raise SystemExit("No companies selected to seed.")

    conn = get_snowflake_connection()
    cur = conn.cursor()
    try:
        inserted = 0
        updated = 0
        for ticker, info in selected.items():
            result = upsert_company(cur, ticker, info["name"], info["industry_id"])
            if result == "inserted":
                inserted += 1
            else:
                updated += 1

        conn.commit()
        print(f"Companies seeded: inserted={inserted}, updated={updated}")
        if missing:
            print(f"Skipped (not in defaults): {', '.join(missing)}")
        return 0
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
