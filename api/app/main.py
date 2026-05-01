from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from prometheus_fastapi_instrumentator import Instrumentator

from app.core.config import settings
from app.core.db import get_session, init_db
from app.api.v1 import router as v1_router

app = FastAPI(
    title="BrickProfit API",
    version="1.0.0",
    docs_url="/api/docs",
    redoc_url="/api/redoc",
    openapi_url="/api/openapi.json",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Prometheus metrics endpoint at /metrics
Instrumentator().instrument(app).expose(app)

app.include_router(v1_router, prefix="/api/v1")


@app.on_event("startup")
async def startup():
    init_db()


@app.get("/health", tags=["ops"])
def health():
    return {"status": "ok"}
