from fastapi import FastAPI
from app.config import settings
from app.routers.health import router as health_router
from app.routers.companies import router as companies_router
from app.routers.assessments import router as assessments_router
 
app = FastAPI(title=settings.app_name)
 
# Health endpoints
app.include_router(health_router)
 
# Versioned API endpoints
app.include_router(companies_router, prefix=settings.api_prefix)
app.include_router(assessments_router, prefix=settings.api_prefix)