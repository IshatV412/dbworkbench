"""FastAPI application entry point.

Django ORM is bootstrapped on import so that all services can use
Django models, the atomic record_commit() function, and Fernet
decryption from ConnectionProfile.
"""

# Bootstrap Django ORM BEFORE any other app imports
import fastapi_backend.app.django_setup  # noqa: F401, E402

import logging
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from fastapi_backend.app.routes.query_routes import router as query_router
from fastapi_backend.app.routes.commit_routes import router as commit_router
from fastapi_backend.app.routes.anticommand_routes import router as anticommand_router
from fastapi_backend.app.routes.snapshot_routes import router as snapshot_router
from fastapi_backend.app.routes.rollback_routes import router as rollback_router

logger = logging.getLogger(__name__)

_cors_origins_env = os.getenv("BACKEND_CORS_ORIGINS", "").strip()
ALLOWED_ORIGINS = (
    [origin.strip() for origin in _cors_origins_env.split(",") if origin.strip()]
    or ["http://localhost:3000"]
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup / shutdown lifecycle.

    No connection pool to init — connections are created dynamically
    per-user via ConnectionProfile.  Django ORM is already bootstrapped.
    """
    logger.info("FastAPI started — Django ORM bootstrapped, dynamic user connections ready.")
    yield


app = FastAPI(
    title="WEAVE-DB API",
    description=(
        "Database version-control backend with commit tracking, "
        "inverse operations, configurable snapshotting, and rollback. "
        "All endpoints require JWT authentication (issued by Django)."
    ),
    version="0.3.0",
    lifespan=lifespan,
)

# CORS — allow the web UI frontend to call us
app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Routers
app.include_router(query_router)
app.include_router(commit_router)
app.include_router(anticommand_router)
app.include_router(snapshot_router)
app.include_router(rollback_router)


@app.get("/health")
def health():
    """Liveness check (no auth required)."""
    return {"status": "ok"}
