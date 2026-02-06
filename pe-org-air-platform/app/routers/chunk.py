from __future__ import annotations
from fastapi import APIRouter, Query
from app.services.evidence_store import EvidenceStore
router = APIRouter(prefix="/chunks")

@router.get("")
def list_chunks(
   document_id: str = Query(...),
   limit: int = Query(default=200, ge=1, le=1000),
):
   store = EvidenceStore()
   try:
       return store.list_chunks(document_id=document_id, limit=limit)
   finally:
       store.close()