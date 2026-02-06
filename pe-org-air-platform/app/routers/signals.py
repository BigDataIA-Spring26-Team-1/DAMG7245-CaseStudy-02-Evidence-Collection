# app/routers/signals.py

from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query

from app.services.snowflake import get_snowflake_connection

router = APIRouter(prefix="/signals")


@router.get("")

def list_signals(

    ticker: str | None = Query(default=None),

    signal_type: str | None = Query(default=None),

    source: str | None = Query(default=None),

    limit: int = Query(default=100, ge=1, le=500),

):

    conn = get_snowflake_connection()

    cur = conn.cursor()

    try:

        q = """

        SELECT id, company_id, ticker, signal_type, source, title, url,

               published_at, collected_at, content_hash, metadata

          FROM external_signals

         WHERE (%s IS NULL OR ticker = %s)

           AND (%s IS NULL OR signal_type = %s)

           AND (%s IS NULL OR source = %s)

         ORDER BY collected_at DESC

         LIMIT %s

        """

        cur.execute(q, (ticker, ticker, signal_type, signal_type, source, source, limit))

        cols = [c[0].lower() for c in cur.description]

        return [dict(zip(cols, r)) for r in cur.fetchall()]

    finally:

        cur.close()

        conn.close()


@router.get("/{signal_id}")

def get_signal(signal_id: str):

    conn = get_snowflake_connection()

    cur = conn.cursor()

    try:

        q = """

        SELECT id, company_id, ticker, signal_type, source, title, url,

               published_at, collected_at, content_text, content_hash, metadata

          FROM external_signals

         WHERE id = %s

         LIMIT 1

        """

        cur.execute(q, (signal_id,))

        row = cur.fetchone()

        if not row:

            raise HTTPException(status_code=404, detail="Signal not found")

        cols = [c[0].lower() for c in cur.description]

        return dict(zip(cols, row))

    finally:

        cur.close()

        conn.close()
 