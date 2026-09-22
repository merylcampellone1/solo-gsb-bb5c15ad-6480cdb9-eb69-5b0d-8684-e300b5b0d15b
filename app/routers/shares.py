"""Controlled share endpoints: single-use token create, claim-and-download, revoke.

A share token is 32 bytes of CSPRNG randomness (URL-safe). Only the SHA-256 of
the token is persisted, so a database disclosure cannot reconstruct usable
tokens and the raw token is shown to the client exactly once.

Downloads are claimed with a single conditional ``UPDATE`` inside
``BEGIN IMMEDIATE``: every validity check (existence, revocation, expiry,
remaining quota) is part of the WHERE clause. A failed claim rolls back
without touching ``downloads_used``; a successful one atomically decrements the
remaining quota, so N concurrent downloads can never exceed ``max_downloads``.
"""
from __future__ import annotations

import hashlib
import secrets
from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, HTTPException
from fastapi.responses import FileResponse

from ..config import get_settings
from ..db import db_conn, immediate_tx
from ..schemas import (
    CreateShareRequest,
    RevokeShareResponse,
    ShareResponse,
    ShareStatusResponse,
)
from ..service import utcnow
from ..storage import object_path

router = APIRouter()


def hash_token(token: str) -> str:
    """One-way fingerprint stored in place of the token itself."""
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def create_share(sha256: str, payload: CreateShareRequest) -> ShareResponse:
    """Insert a share for an existing archive and return the raw token once."""
    token = secrets.token_urlsafe(32)
    now = datetime.now(timezone.utc)
    created_at = now.isoformat()
    expires_at = (now + timedelta(seconds=payload.expires_in)).isoformat()
    with db_conn() as conn, immediate_tx(conn):
        archive = conn.execute(
            "SELECT 1 FROM archives WHERE sha256=?", (sha256,)
        ).fetchone()
        if archive is None:
            raise HTTPException(status_code=404, detail="archive not found")
        conn.execute(
            "INSERT INTO shares (token_hash, archive_sha256, max_downloads,"
            " downloads_used, expires_at, revoked, created_at)"
            " VALUES (?,?,?,0,?,0,?)",
            (hash_token(token), sha256, payload.max_downloads, expires_at, created_at),
        )
    return ShareResponse(
        token=token,
        archive_sha256=sha256,
        expires_at=expires_at,
        max_downloads=payload.max_downloads,
        remaining_downloads=payload.max_downloads,
        created_at=created_at,
    )


def _share_state(row) -> str:
    if row["revoked"]:
        return "revoked"
    if row["downloads_used"] >= row["max_downloads"]:
        return "exhausted"
    if row["expires_at"] <= utcnow():
        return "expired"
    return "active"


def _raise_claim_error(row) -> None:
    """Map a non-claimable share to an explicit HTTP error."""
    if row["revoked"]:
        raise HTTPException(status_code=410, detail="share has been revoked")
    if row["expires_at"] <= utcnow():
        raise HTTPException(status_code=410, detail="share has expired")
    if row["downloads_used"] >= row["max_downloads"]:
        raise HTTPException(status_code=410, detail="share download limit reached")
    raise HTTPException(status_code=409, detail="share could not be claimed")


@router.get("/{token}", response_model=ShareStatusResponse)
def share_status(token: str) -> ShareStatusResponse:
    """Inspect a share (its counters and computed state) without downloading."""
    with db_conn() as conn:
        row = conn.execute(
            "SELECT * FROM shares WHERE token_hash=?", (hash_token(token),)
        ).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail="share not found")
    return ShareStatusResponse(
        archive_sha256=row["archive_sha256"],
        expires_at=row["expires_at"],
        max_downloads=row["max_downloads"],
        downloads_used=row["downloads_used"],
        remaining_downloads=row["max_downloads"] - row["downloads_used"],
        revoked=bool(row["revoked"]),
        state=_share_state(row),
        created_at=row["created_at"],
    )


@router.get("/{token}/download")
def share_download(token: str) -> FileResponse:
    """Atomically claim one download, then stream the archive.

    Validation and quota decrement happen together in one write transaction;
    if the share is unknown/revoked/expired/exhausted the transaction rolls
    back and no download is counted. The file is streamed only after commit.
    """
    token_hash = hash_token(token)
    now = utcnow()
    with db_conn() as conn, immediate_tx(conn):
        row = conn.execute(
            "SELECT * FROM shares WHERE token_hash=?", (token_hash,)
        ).fetchone()
        if row is None:
            raise HTTPException(status_code=404, detail="share not found")
        # Single atomic claim: all validity predicates live in the WHERE clause.
        cur = conn.execute(
            "UPDATE shares SET downloads_used=downloads_used+1"
            " WHERE token_hash=? AND revoked=0 AND expires_at > ?"
            "   AND downloads_used < max_downloads",
            (token_hash, now),
        )
        if cur.rowcount == 0:
            # Nothing was modified: re-read to report the exact reason (no count consumed).
            row = conn.execute(
                "SELECT * FROM shares WHERE token_hash=?", (token_hash,)
            ).fetchone()
            _raise_claim_error(row)
        claimed = conn.execute(
            "SELECT s.*, a.file_name AS file_name FROM shares s"
            " JOIN archives a ON a.sha256 = s.archive_sha256"
            " WHERE s.token_hash=?",
            (token_hash,),
        ).fetchone()
        remaining = claimed["max_downloads"] - claimed["downloads_used"]
        sha256 = claimed["archive_sha256"]
        file_name = claimed["file_name"]

    # Transaction committed: the download is counted even if streaming later fails.
    settings = get_settings()
    path = object_path(settings, sha256)
    if not path.exists():
        raise HTTPException(status_code=500, detail="archive object missing on disk")
    return FileResponse(
        path,
        filename=file_name,
        media_type="application/octet-stream",
        headers={"X-Share-Remaining-Downloads": str(remaining)},
    )


@router.delete("/{token}", response_model=RevokeShareResponse)
def revoke_share(token: str) -> RevokeShareResponse:
    """Revoke a share. Idempotent: revoking again reports ``revoked: false``."""
    with db_conn() as conn, immediate_tx(conn):
        row = conn.execute(
            "SELECT revoked FROM shares WHERE token_hash=?", (hash_token(token),)
        ).fetchone()
        if row is None:
            raise HTTPException(status_code=404, detail="share not found")
        already = bool(row["revoked"])
        if not already:
            conn.execute(
                "UPDATE shares SET revoked=1 WHERE token_hash=?", (hash_token(token),)
            )
    return RevokeShareResponse(revoked=not already)
