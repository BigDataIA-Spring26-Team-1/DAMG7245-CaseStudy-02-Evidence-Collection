# app/routers/evidence.py
from __future__ import annotations
from fastapi import APIRouter, Query, HTTPException
from app.services.evidence_store import EvidenceStore
router = APIRouter(prefix="/evidence")

@router.get("/stats")
def stats():
   store = EvidenceStore()
   try:
       return store.evidence_stats()
   finally:
       store.close()

@router.get("/documents")
def list_documents(
   ticker: str | None = Query(default=None),
   company_id: str | None = Query(default=None),
   limit: int = Query(default=100, ge=1, le=500),
):
   store = EvidenceStore()
   try:
       return store.list_documents(ticker=ticker, company_id=company_id, limit=limit)
   finally:
       store.close()

@router.get("/documents/{document_id}")
def get_document(document_id: str):
   store = EvidenceStore()
   try:
       doc = store.get_document(document_id)
       if not doc:
           raise HTTPException(status_code=404, detail="Document not found")
       return doc
   finally:
       store.close()

@router.get("/documents/{document_id}/chunks")
def get_chunks(
   document_id: str,
   limit: int = Query(default=200, ge=1, le=1000),
):
   store = EvidenceStore()
   try:
       return store.list_chunks(document_id=document_id, limit=limit)
   finally:
       store.close()