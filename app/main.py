"""Application entrypoint."""
from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI

from .config import ensure_dirs, get_settings
from .db import init_db
from .routers import archives, sessions, shares
from .service import recover_after_restart


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: create the volume layout, initialize the SQLite schema and
    # recover from any unclean shutdown before serving requests.
    settings = get_settings()
    ensure_dirs(settings)
    init_db(settings.db_path)
    recover_after_restart(settings)
    yield


app = FastAPI(
    title="Large File Archive API",
    version="1.0.0",
    description=(
        "Resumable chunked uploads with per-chunk SHA-256 verification, "
        "atomic merge, whole-file hash validation and content-addressed dedup."
    ),
    lifespan=lifespan,
)

app.include_router(sessions.router, prefix="/v1/sessions", tags=["sessions"])
app.include_router(archives.router, prefix="/v1/archives", tags=["archives"])
app.include_router(shares.router, prefix="/v1/shares", tags=["shares"])


@app.get("/healthz", tags=["meta"])
def healthz() -> dict:
    return {"status": "ok"}
