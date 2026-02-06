# app/routers/signal_summaries.py

from __future__ import annotations

from datetime import date

from uuid import uuid4

from fastapi import APIRouter, HTTPException, Query

from app.services.snowflake import get_snowflake_connection

router = APIRouter(prefix="/signal-summaries")


@router.get("")

def list_summaries(

    ticker: str | None = Query(default=None),

    limit: int = Query(default=50, ge=1, le=200),

):

    conn = get_snowflake_connection()

    cur = conn.cursor()

    try:

        q = """

        SELECT id, company_id, ticker, as_of_date, summary_text, signal_count, created_at

          FROM company_signal_summaries

         WHERE (%s IS NULL OR ticker = %s)

         ORDER BY as_of_date DESC, created_at DESC

         LIMIT %s

        """

        cur.execute(q, (ticker, ticker, limit))

        cols = [c[0].lower() for c in cur.description]

        return [dict(zip(cols, r)) for r in cur.fetchall()]

    finally:

        cur.close()

        conn.close()


@router.post("/compute")

def compute_summary(

    ticker: str = Query(..., description="Ticker like CAT"),

    as_of: date | None = Query(default=None, description="Defaults to today"),

):

    as_of_date = as_of or date.today()

    conn = get_snowflake_connection()

    cur = conn.cursor()

    try:

        # 1) Find company_id

        cur.execute("SELECT id FROM companies WHERE ticker=%s LIMIT 1", (ticker,))

        row = cur.fetchone()

        if not row:

            raise HTTPException(status_code=404, detail=f"Company not found for ticker={ticker}")

        company_id = str(row[0])

        # 2) Pull recent signals (last 7 days) and create a simple summary text

        cur.execute(

            """

            SELECT signal_type, COUNT(*) AS cnt

              FROM external_signals

             WHERE ticker=%s

               AND collected_at >= DATEADD(day, -7, CURRENT_TIMESTAMP())

             GROUP BY signal_type

             ORDER BY cnt DESC

            """,

            (ticker,),

        )

        breakdown = cur.fetchall()

        signal_count = sum(int(r[1]) for r in breakdown) if breakdown else 0

        parts = [f"{st}: {cnt}" for (st, cnt) in breakdown] if breakdown else ["No recent signals found (last 7 days)"]

        summary_text = f"Signals last 7 days for {ticker}: " + ", ".join(parts)

        # 3) Upsert into company_signal_summaries

        sid = str(uuid4())

        cur.execute(

            """

            MERGE INTO company_signal_summaries t

            USING (SELECT %s AS company_id, %s AS ticker, %s AS as_of_date) s

               ON t.company_id = s.company_id AND t.as_of_date = s.as_of_date

            WHEN MATCHED THEN UPDATE SET

              summary_text = %s,

              signal_count = %s,

              created_at = CURRENT_TIMESTAMP()

            WHEN NOT MATCHED THEN INSERT

              (id, company_id, ticker, as_of_date, summary_text, signal_count, created_at)

            VALUES

              (%s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP())

            """,

            (

                company_id,

                ticker,

                as_of_date,

                summary_text,

                signal_count,

                sid,

                company_id,

                ticker,

                as_of_date,

                summary_text,

                signal_count,

            ),

        )

        return {

            "ticker": ticker,

            "as_of_date": str(as_of_date),

            "signal_count": signal_count,

            "summary_text": summary_text,

        }

    finally:

        cur.close()

        conn.close()
 