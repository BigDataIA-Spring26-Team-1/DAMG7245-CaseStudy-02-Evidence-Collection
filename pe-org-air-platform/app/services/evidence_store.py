from __future__ import annotations
 
from dataclasses import dataclass
from typing import Optional, Sequence
from uuid import uuid4
 
from app.services.snowflake import get_snowflake_connection
 
 
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
    def __init__(self):
        self.conn = get_snowflake_connection()
        self.conn.autocommit(True)

 
    def close(self):
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
