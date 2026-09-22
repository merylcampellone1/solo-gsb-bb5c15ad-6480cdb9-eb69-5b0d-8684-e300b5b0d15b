"""Archive endpoints: list, inspect, download, controlled shares."""
from __future__ import annotations

import re

from fastapi import APIRouter, HTTPException
from fastapi.responses import FileResponse

from ..config import get_settings
from ..db import db_conn
from ..schemas import ArchiveResponse, CreateShareRequest, ShareResponse
from ..storage import object_path
from .shares import create_share

router = APIRouter()

_SHA256_RE = re.compile(r"^[0-9a-fA-F]{64}$")


def _validate_sha(sha256: str) -> str:
    sha = sha256.lower()
    if not _SHA256_RE.fullmatch(sha):
        raise HTTPException(status_code=400, detail="sha256 must be 64 hex chars")
    return sha


def _get_archive(sha256: str):
    with db_conn() as conn:
        row = conn.execute(
            "SELECT * FROM archives WHERE sha256=?", (sha256,)
        ).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail="archive not found")
    return row


@router.get("", response_model=list[ArchiveResponse])
def list_archives() -> list[ArchiveResponse]:
    with db_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM archives ORDER BY created_at DESC"
        ).fetchall()
    return [
        ArchiveResponse(
            sha256=row["sha256"],
            file_name=row["file_name"],
            size=row["size"],
            ref_count=row["ref_count"],
            created_at=row["created_at"],
        )
        for row in rows
    ]


@router.get("/{sha256}", response_model=ArchiveResponse)
def get_archive(sha256: str) -> ArchiveResponse:
    row = _get_archive(_validate_sha(sha256))
    return ArchiveResponse(
        sha256=row["sha256"],
        file_name=row["file_name"],
        size=row["size"],
        ref_count=row["ref_count"],
        created_at=row["created_at"],
    )


@router.get("/{sha256}/download")
def download_archive(sha256: str) -> FileResponse:
    row = _get_archive(_validate_sha(sha256))
    path = object_path(get_settings(), row["sha256"])
    if not path.exists():
        raise HTTPException(status_code=500, detail="archive object missing on disk")
    return FileResponse(
        path,
        filename=row["file_name"],
        media_type="application/octet-stream",
    )


@router.post("/{sha256}/shares", response_model=ShareResponse, status_code=201)
def create_archive_share(sha256: str, payload: CreateShareRequest) -> ShareResponse:
    """Create a controlled download share.

    The opaque token is returned exactly once; only its SHA-256 is stored.
    Download it via ``GET /v1/shares/{token}/download`` and revoke via
    ``DELETE /v1/shares/{token}``.
    """
    return create_share(_validate_sha(sha256), payload)
