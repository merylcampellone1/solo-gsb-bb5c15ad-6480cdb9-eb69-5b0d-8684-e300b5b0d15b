"""Upload session endpoints: create, chunk upload, resume query, complete, cleanup."""
from __future__ import annotations

import hashlib
import os
import re
import shutil
import sqlite3
import uuid

from fastapi import APIRouter, Header, HTTPException, Request

from ..config import get_settings
from ..db import db_conn, immediate_tx
from ..schemas import (
    ChunkUploadResponse,
    CompleteResponse,
    CreateSessionRequest,
    DeleteSessionResponse,
    SessionResponse,
    SessionStatusResponse,
)
from ..service import expected_chunk_size, merge_chunks, reset_merge_to_active, utcnow
from ..storage import chunk_dir_for, chunk_path, new_tmp_path, object_path

router = APIRouter()

_SHA256_RE = re.compile(r"^[0-9a-fA-F]{64}$")


def _get_session(conn: sqlite3.Connection, session_id: str) -> sqlite3.Row:
    row = conn.execute("SELECT * FROM sessions WHERE id=?", (session_id,)).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail="session not found")
    return row


def _received_count(conn: sqlite3.Connection, session_id: str) -> int:
    return conn.execute(
        "SELECT COUNT(*) AS c FROM chunks WHERE session_id=?", (session_id,)
    ).fetchone()["c"]


@router.post("", response_model=SessionResponse, status_code=201)
def create_session(payload: CreateSessionRequest) -> SessionResponse:
    """Create an upload session.

    If an archive with the same whole-file SHA-256 already exists, the session
    is completed instantly and reuses that object (no upload needed).
    """
    session_id = uuid.uuid4().hex
    now = utcnow()
    with db_conn() as conn, immediate_tx(conn):
        existing = conn.execute(
            "SELECT sha256 FROM archives WHERE sha256=?", (payload.file_sha256,)
        ).fetchone()
        instant = existing is not None
        if instant:
            conn.execute(
                "UPDATE archives SET ref_count=ref_count+1 WHERE sha256=?",
                (payload.file_sha256,),
            )
        conn.execute(
            "INSERT INTO sessions (id, file_name, total_size, chunk_size, total_chunks,"
            " file_sha256, status, archive_sha256, created_at, updated_at, completed_at)"
            " VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            (
                session_id,
                payload.file_name,
                payload.total_size,
                payload.chunk_size,
                payload.total_chunks,
                payload.file_sha256,
                "completed" if instant else "active",
                payload.file_sha256 if instant else None,
                now,
                now,
                now if instant else None,
            ),
        )
    return SessionResponse(
        session_id=session_id,
        status="completed" if instant else "active",
        file_name=payload.file_name,
        total_size=payload.total_size,
        chunk_size=payload.chunk_size,
        total_chunks=payload.total_chunks,
        file_sha256=payload.file_sha256,
        instant=instant,
        archive_sha256=payload.file_sha256 if instant else None,
        created_at=now,
    )


@router.put("/{session_id}/chunks/{chunk_index}", response_model=ChunkUploadResponse)
async def upload_chunk(
    session_id: str,
    chunk_index: int,
    request: Request,
    x_chunk_sha256: str = Header(alias="X-Chunk-SHA256"),
) -> ChunkUploadResponse:
    """Upload one chunk (raw body, idempotent, any order).

    The `X-Chunk-SHA256` header is verified against the received bytes.
    Re-uploading a chunk that is already stored with the same hash is a no-op.
    """
    settings = get_settings()
    client_sha = x_chunk_sha256.lower()
    if not _SHA256_RE.fullmatch(client_sha):
        raise HTTPException(status_code=400, detail="X-Chunk-SHA256 must be 64 hex chars")

    with db_conn() as conn:
        session = _get_session(conn, session_id)
        if session["status"] != "active":
            raise HTTPException(
                status_code=409, detail=f"session is {session['status']}, not accepting chunks"
            )
        total_chunks = session["total_chunks"]
        if not 0 <= chunk_index < total_chunks:
            raise HTTPException(
                status_code=400,
                detail=f"chunk_index out of range [0, {total_chunks})",
            )
        expected_size = expected_chunk_size(
            session["total_size"], session["chunk_size"], total_chunks, chunk_index
        )
        stored = conn.execute(
            "SELECT sha256 FROM chunks WHERE session_id=? AND chunk_index=?",
            (session_id, chunk_index),
        ).fetchone()
        if stored is not None and stored["sha256"] == client_sha:
            # Duplicate upload of an identical chunk: acknowledge without rewriting.
            received = _received_count(conn, session_id)
            return ChunkUploadResponse(
                session_id=session_id,
                chunk_index=chunk_index,
                size=expected_size,
                sha256=client_sha,
                duplicate=True,
                received_chunks=received,
                total_chunks=total_chunks,
            )

    # Stream the body to a temp file while hashing; never trust Content-Length.
    tmp = new_tmp_path(settings)
    digest = hashlib.sha256()
    size = 0
    try:
        with open(tmp, "wb") as out:
            async for block in request.stream():
                out.write(block)
                digest.update(block)
                size += len(block)
                if size > expected_size:
                    raise HTTPException(
                        status_code=400,
                        detail=f"chunk exceeds expected size of {expected_size} bytes",
                    )
    except BaseException:
        tmp.unlink(missing_ok=True)
        raise

    if size != expected_size:
        tmp.unlink(missing_ok=True)
        raise HTTPException(
            status_code=400,
            detail=f"chunk size mismatch: expected {expected_size} bytes, got {size}",
        )
    if digest.hexdigest() != client_sha:
        tmp.unlink(missing_ok=True)
        raise HTTPException(
            status_code=422,
            detail="chunk SHA-256 mismatch: body does not match X-Chunk-SHA256",
        )

    dest = chunk_path(settings, session_id, chunk_index)
    dest.parent.mkdir(parents=True, exist_ok=True)
    os.replace(tmp, dest)  # atomic publish of the chunk file

    now = utcnow()
    with db_conn() as conn, immediate_tx(conn):
        row = conn.execute(
            "SELECT status FROM sessions WHERE id=?", (session_id,)
        ).fetchone()
        if row is None:
            dest.unlink(missing_ok=True)
            raise HTTPException(status_code=404, detail="session not found")
        if row["status"] != "active":
            dest.unlink(missing_ok=True)
            raise HTTPException(
                status_code=409, detail=f"session is {row['status']}, not accepting chunks"
            )
        conn.execute(
            "INSERT INTO chunks (session_id, chunk_index, size, sha256, uploaded_at)"
            " VALUES (?,?,?,?,?)"
            " ON CONFLICT(session_id, chunk_index) DO UPDATE SET"
            "   size=excluded.size, sha256=excluded.sha256, uploaded_at=excluded.uploaded_at",
            (session_id, chunk_index, size, client_sha, now),
        )
        conn.execute(
            "UPDATE sessions SET updated_at=? WHERE id=?", (now, session_id)
        )
        received = _received_count(conn, session_id)

    return ChunkUploadResponse(
        session_id=session_id,
        chunk_index=chunk_index,
        size=size,
        sha256=client_sha,
        duplicate=False,
        received_chunks=received,
        total_chunks=total_chunks,
    )


@router.get("/{session_id}", response_model=SessionStatusResponse)
def session_status(session_id: str) -> SessionStatusResponse:
    """Resume/breakpoint query: which chunks the server already has."""
    with db_conn() as conn:
        session = _get_session(conn, session_id)
        uploaded = [
            row["chunk_index"]
            for row in conn.execute(
                "SELECT chunk_index FROM chunks WHERE session_id=? ORDER BY chunk_index",
                (session_id,),
            )
        ]
    uploaded_set = set(uploaded)
    missing = [i for i in range(session["total_chunks"]) if i not in uploaded_set]
    return SessionStatusResponse(
        session_id=session["id"],
        status=session["status"],
        file_name=session["file_name"],
        total_size=session["total_size"],
        chunk_size=session["chunk_size"],
        total_chunks=session["total_chunks"],
        file_sha256=session["file_sha256"],
        received_chunks=len(uploaded),
        uploaded_chunks=uploaded,
        missing_chunks=missing,
        archive_sha256=session["archive_sha256"],
        last_error=session["last_error"],
        created_at=session["created_at"],
        updated_at=session["updated_at"],
        completed_at=session["completed_at"],
    )


@router.post("/{session_id}/complete", response_model=CompleteResponse)
def complete_session(session_id: str) -> CompleteResponse:
    """Merge all chunks and publish the archive (atomic, concurrency-safe).

    Only one request can win the active -> merging transition. The merge itself
    runs outside any DB transaction; the final publish (object rename + session
    update + chunk cleanup) is committed in a single transaction.
    """
    settings = get_settings()

    with db_conn() as conn, immediate_tx(conn):
        session = _get_session(conn, session_id)
        if session["status"] == "completed":
            # Idempotent retry of an already-finished complete.
            return CompleteResponse(
                session_id=session_id,
                status="completed",
                archive_sha256=session["archive_sha256"],
                size=session["total_size"],
                deduplicated=True,
            )
        if session["status"] != "active":
            raise HTTPException(status_code=409, detail="merge already in progress")
        received = _received_count(conn, session_id)
        if received < session["total_chunks"]:
            uploaded = {
                row["chunk_index"]
                for row in conn.execute(
                    "SELECT chunk_index FROM chunks WHERE session_id=?", (session_id,)
                )
            }
            missing = [i for i in range(session["total_chunks"]) if i not in uploaded]
            raise HTTPException(
                status_code=409,
                detail={
                    "error": "cannot complete: chunks are missing",
                    "received_chunks": received,
                    "missing_chunks": missing,
                },
            )
        # The gate: exactly one concurrent request flips active -> merging.
        conn.execute(
            "UPDATE sessions SET status='merging', updated_at=? WHERE id=? AND status='active'",
            (utcnow(), session_id),
        )
        total_chunks = session["total_chunks"]
        declared_sha = session["file_sha256"]
        file_name = session["file_name"]

    try:
        tmp, digest, size = merge_chunks(settings, session_id, total_chunks)
    except OSError as exc:
        reset_merge_to_active(session_id, f"merge I/O error: {exc}")
        raise HTTPException(status_code=500, detail=f"merge failed: {exc}") from exc

    if digest != declared_sha:
        tmp.unlink(missing_ok=True)
        reset_merge_to_active(
            session_id, f"whole-file hash mismatch: expected {declared_sha}, got {digest}"
        )
        raise HTTPException(
            status_code=422,
            detail={
                "error": "whole-file SHA-256 mismatch; session stays active, re-upload and retry",
                "expected": declared_sha,
                "actual": digest,
            },
        )

    # Publish: reuse an existing object when the content is already archived.
    with db_conn() as conn:
        already_archived = (
            conn.execute("SELECT 1 FROM archives WHERE sha256=?", (digest,)).fetchone()
            is not None
        )
    if already_archived:
        tmp.unlink(missing_ok=True)
    else:
        final = object_path(settings, digest)
        final.parent.mkdir(parents=True, exist_ok=True)
        os.replace(tmp, final)  # atomic, same volume as tmp/

    now = utcnow()
    with db_conn() as conn, immediate_tx(conn):
        conn.execute(
            "INSERT INTO archives (sha256, file_name, size, ref_count, created_at)"
            " VALUES (?,?,?,1,?)"
            " ON CONFLICT(sha256) DO UPDATE SET ref_count=ref_count+1",
            (digest, file_name, size, now),
        )
        conn.execute(
            "UPDATE sessions SET status='completed', archive_sha256=?, last_error=NULL,"
            " updated_at=?, completed_at=? WHERE id=?",
            (digest, now, now, session_id),
        )
        conn.execute("DELETE FROM chunks WHERE session_id=?", (session_id,))

    shutil.rmtree(chunk_dir_for(settings, session_id), ignore_errors=True)
    return CompleteResponse(
        session_id=session_id,
        status="completed",
        archive_sha256=digest,
        size=size,
        deduplicated=already_archived,
    )


@router.delete("/{session_id}", response_model=DeleteSessionResponse)
def delete_session(session_id: str) -> DeleteSessionResponse:
    """Clean up a session: drop staged chunks and release its archive reference.

    The underlying object is deleted only when the last referencing session
    is removed (reference counting keeps deduplicated files alive).
    """
    settings = get_settings()
    object_to_delete = None
    archive_released = False
    with db_conn() as conn, immediate_tx(conn):
        session = _get_session(conn, session_id)
        if session["status"] == "merging":
            raise HTTPException(status_code=409, detail="merge in progress, try again later")
        archive_sha = session["archive_sha256"]
        if archive_sha is not None:
            archive_released = True
            conn.execute(
                "UPDATE archives SET ref_count=ref_count-1 WHERE sha256=?", (archive_sha,)
            )
            remaining = conn.execute(
                "SELECT ref_count FROM archives WHERE sha256=?", (archive_sha,)
            ).fetchone()["ref_count"]
            if remaining == 0:
                conn.execute("DELETE FROM archives WHERE sha256=?", (archive_sha,))
                object_to_delete = object_path(settings, archive_sha)
        conn.execute("DELETE FROM sessions WHERE id=?", (session_id,))  # cascades chunks

    shutil.rmtree(chunk_dir_for(settings, session_id), ignore_errors=True)
    if object_to_delete is not None:
        object_to_delete.unlink(missing_ok=True)
        try:
            object_to_delete.parent.rmdir()  # drop the hash-prefix dir if now empty
        except OSError:
            pass
    return DeleteSessionResponse(
        session_id=session_id,
        deleted=True,
        archive_released=archive_released,
        archive_deleted=object_to_delete is not None,
    )
