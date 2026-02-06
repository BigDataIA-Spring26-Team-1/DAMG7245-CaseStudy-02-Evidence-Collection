from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Optional, Sequence

from app.services.snowflake import get_snowflake_connection


class DocumentStatus(str, Enum):
    PENDING = "pending"
    DOWNLOADED = "downloaded"
    PARSED = "parsed"
    CHUNKED = "chunked"
    INDEXED = "indexed"
    FAILED = "failed"
    PROCESSED = "processed"  # legacy / CS1-compatible


@dataclass(frozen=True)
class DocumentRow:
    id: str
    company_id: str
    ticker: str
    filing_type: str
    filing_date: str  # YYYY-MM-DD
    source_url: Optional[str]
    local_path: Optional[str]
    content_hash: str
    word_count: int
    chunk_count: int
    status: str = "processed"


@dataclass(frozen=True)
class ChunkRow:
    id: str
    document_id: str
    chunk_index: int
    content: str
    section: Optional[str]
    start_char: int
    end_char: int
    word_count: int


class EvidenceStore:
    def __init__(self) -> None:
        self.conn = get_snowflake_connection()
        try:
            self.conn.autocommit(True)
        except Exception:
            # Some connector versions may not support this; inserts will still work
            pass

    def close(self) -> None:
        self.conn.close()

    def document_exists_by_hash(self, content_hash: str) -> bool:
        q = "SELECT 1 FROM documents WHERE content_hash = %s LIMIT 1"
        cur = self.conn.cursor()
        try:
            cur.execute(q, (content_hash,))
            return cur.fetchone() is not None
        finally:
            cur.close()

    def insert_document(self, doc: DocumentRow) -> None:
        q = """
        INSERT INTO documents (
            id, company_id, ticker, filing_type, filing_date,
            source_url, local_path, content_hash, word_count, chunk_count, status
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """
        cur = self.conn.cursor()
        try:
            cur.execute(
                q,
                (
                    doc.id,
                    doc.company_id,
                    doc.ticker,
                    doc.filing_type,
                    doc.filing_date,
                    doc.source_url,
                    doc.local_path,
                    doc.content_hash,
                    doc.word_count,
                    doc.chunk_count,
                    doc.status,
                ),
            )
        finally:
            cur.close()

    def insert_chunks_bulk(self, chunks: Sequence[ChunkRow]) -> None:
        if not chunks:
            return

        q = """
        INSERT INTO document_chunks (
            id, document_id, chunk_index, content, section, start_char, end_char, word_count
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
        """
        cur = self.conn.cursor()
        try:
            cur.executemany(
                q,
                [
                    (
                        c.id,
                        c.document_id,
                        c.chunk_index,
                        c.content,
                        c.section,
                        c.start_char,
                        c.end_char,
                        c.word_count,
                    )
                    for c in chunks
                ],
            )
        finally:
            cur.close()

    def update_document_chunk_count(self, document_id: str, chunk_count: int) -> None:
        q = "UPDATE documents SET chunk_count=%s WHERE id=%s"
        cur = self.conn.cursor()
        try:
            cur.execute(q, (chunk_count, document_id))
        finally:
            cur.close()

    def update_document_status(
        self, document_id: str, status: str, error_message: str | None = None
    ) -> None:
        q = """
        UPDATE documents
           SET status=%s,
               error_message=COALESCE(%s, error_message),
               processed_at=CASE
                   WHEN %s IN ('indexed','failed') THEN CURRENT_TIMESTAMP()
                   ELSE processed_at
               END
         WHERE id=%s
        """
        cur = self.conn.cursor()
        try:
            cur.execute(q, (status, error_message, status, document_id))
        finally:
            cur.close()

    def insert_failed_stub(
        self,
        doc_id: str,
        company_id: str,
        ticker: str,
        filing_type: str,
        filing_date: str,
        source_url: str | None,
        local_path: str | None,
        content_hash: str | None,
        error_message: str,
    ) -> None:
        """
        Use ONLY when the script fails before insert_document() could run.
        If content_hash is None, this may fail if your table requires NOT NULL.
        Your schema currently allows NULL content_hash, so it's ok.
        """
        q = """
        INSERT INTO documents (
          id, company_id, ticker, filing_type, filing_date,
          source_url, local_path, content_hash, word_count, chunk_count,
          status, error_message, processed_at
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,CURRENT_TIMESTAMP())
        """
        cur = self.conn.cursor()
        try:
            cur.execute(
                q,
                (
                    doc_id,
                    company_id,
                    ticker,
                    filing_type,
                    filing_date,
                    source_url,
                    local_path,
                    content_hash,
                    0,
                    0,
                    DocumentStatus.FAILED.value,
                    error_message,
                ),
            )
        finally:
            cur.close()
