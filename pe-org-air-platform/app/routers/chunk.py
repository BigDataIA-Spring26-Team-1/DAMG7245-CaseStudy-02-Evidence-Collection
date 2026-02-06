from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query

from app.services.evidence_store import EvidenceStore

router = APIRouter(prefix="/chunks")


@router.get("/")
def list_chunks(
    document_id: str = Query(..., description="Document ID"),
    limit: int = Query(default=200, ge=1, le=1000),
    offset: int = Query(default=0, ge=0),
):
    store = EvidenceStore()
    try:
        return store.list_chunks(document_id=document_id, limit=limit, offset=offset)
    finally:
        store.close()


@router.get("/{chunk_id}")
def get_chunk(chunk_id: str):
    store = EvidenceStore()
    try:
        row = store.get_chunk(chunk_id)
        if not row:
            raise HTTPException(status_code=404, detail="Chunk not found")
        return row
    finally:
        store.close()
