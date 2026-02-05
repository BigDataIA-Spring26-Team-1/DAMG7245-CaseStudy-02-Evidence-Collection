from __future__ import annotations
 
import json
from typing import Any, Dict, Optional
from uuid import uuid4
 
from app.services.snowflake import get_snowflake_connection
 
 
class SignalStore:
    def __init__(self):
        self.conn = get_snowflake_connection()
        try:
            self.conn.autocommit(True)
        except Exception:
            pass
 
    def close(self):
        self.conn.close()
 
    def exists_by_hash(self, content_hash: str) -> bool:
        cur = self.conn.cursor()
        try:
            cur.execute("SELECT 1 FROM external_signals WHERE content_hash=%s LIMIT 1", (content_hash,))
            return cur.fetchone() is not None
        finally:
            cur.close()
 
    def insert_signal(
        self,
        company_id: str,
        ticker: str,
        signal_type: str,
        source: str,
        title: Optional[str],
        url: Optional[str],
        published_at,
        content_text: Optional[str],
        content_hash: Optional[str],
        metadata: Dict[str, Any],
    ) -> str:
        sid = str(uuid4())
        cur = self.conn.cursor()
        try:
            cur.execute(
                """
                INSERT INTO external_signals
                (id, company_id, ticker, signal_type, source, title, url, published_at, content_text, content_hash, metadata)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,PARSE_JSON(%s))
                """,
                (
                    sid,
                    company_id,
                    ticker,
                    signal_type,
                    source,
                    title,
                    url,
                    published_at,
                    content_text,
                    content_hash,
                    json.dumps(metadata),
                ),
            )
            return sid
        finally:
            cur.close()