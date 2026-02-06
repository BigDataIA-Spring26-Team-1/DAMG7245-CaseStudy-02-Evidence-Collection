from __future__ import annotations
 
from fastapi import FastAPI
 
from app.config import settings
from app.routers.health import router as health_router
from app.routers.companies import router as companies_router
from app.routers.assessments import router as assessments_router
 
# NEW routers (CS2)
from app.routers.documents import router as documents_router
from app.routers.signals import router as signals_router
from app.routers.evidence import router as evidence_router
from app.routers import chunk
 
 
app = FastAPI(title=settings.app_name)
 
# Health
app.include_router(health_router)
 
# Existing
app.include_router(companies_router, prefix=settings.api_prefix, tags=["companies"])
app.include_router(assessments_router, prefix=settings.api_prefix, tags=["assessments"])
 
# CS2 endpoints (match PDF)
app.include_router(documents_router, prefix=settings.api_prefix, tags=["documents"])
app.include_router(signals_router, prefix=settings.api_prefix, tags=["signals"])
app.include_router(evidence_router, prefix=settings.api_prefix, tags=["evidence"])

app.include_router(chunk.router, prefix=settings.api_prefix, tags=["chunks"])
app.include_router(chunk.router, prefix=settings.api_prefix, tags=["chunks"])
app.include_router(chunk.router, prefix=settings.api_prefix, tags=["chunks"])

app.include_router(chunk.router, prefix=settings.api_prefix, tags=["chunks"])