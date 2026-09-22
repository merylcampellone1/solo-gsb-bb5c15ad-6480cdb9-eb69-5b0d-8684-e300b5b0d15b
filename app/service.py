"""Merge and recovery logic shared by the routers."""
from __future__ import annotations

import hashlib
import shutil
from datetime import datetime, timedelta, timezone
from pathlib import Path

from .config import Settings
from .db import db_conn, immediate_tx
from .storage import chunk_dir_for, chunk_path, new_tmp_path

_COPY_BUFFER = 1024 * 1024  # 1 MiB


def utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


def expected_chunk_size(total_size: int, chunk_size: int, total_chunks: int, index: int) -> int:
    """Every chunk is `chunk_size` bytes except the last one, which takes the remainder."""
    if index == total_chunks - 1:
        return total_size - chunk_size * (total_chunks - 1)
    return chunk_size


def merge_chunks(settings: Settings, session_id: str, total_chunks: int) -> tuple[Path, str, int]:
    """Concatenate all chunk files in index order into a temp file.

    Returns (tmp_path, sha256_hex, total_bytes). The caller owns the temp file.
    """
    tmp = new_tmp_path(settings)
    digest = hashlib.sha256()
    size = 0
    try:
        with open(tmp, "wb") as out:
            for index in range(total_chunks):
                part = chunk_path(settings, session_id, index)
                with open(part, "rb") as src:
                    while True:
                        buf = src.read(_COPY_BUFFER)
                        if not buf:
                            break
                        digest.update(buf)
                        out.write(buf)
                        size += len(buf)
            out.flush()
    except BaseException:
        tmp.unlink(missing_ok=True)
        raise
    return tmp, digest.hexdigest(), size


def reset_merge_to_active(session_id: str, error: str) -> None:
    """Best-effort rollback of the 'merging' state so the client can fix and retry."""
    with db_conn() as conn, immediate_tx(conn):
        conn.execute(
            "UPDATE sessions SET status='active', last_error=?, updated_at=? "
            "WHERE id=? AND status='merging'",
            (error, utcnow(), session_id),
        )


def recover_after_restart(settings: Settings) -> None:
    """Crash recovery, run once at startup.

    - A crash mid-merge leaves a session in 'merging'; no merge can be in flight
      right after startup, so flip those back to 'active' for a safe retry.
    - A crash after the reaper claimed a session (active -> reaping) but before
      it finished deleting the row/chunks leaves an interrupted reap: resume it
      by removing the claimed session and its staged chunks now.
    - Chunk directories without a matching session row are orphaned; remove them.
    """
    with db_conn() as conn, immediate_tx(conn):
        # Flip interrupted merges back, preserving updated_at: a session that was
        # already stale must stay eligible for idle reaping after recovery.
        conn.execute(
            "UPDATE sessions SET status='active' WHERE status='merging'"
        )
        claimed = [
            row["id"]
            for row in conn.execute("SELECT id FROM sessions WHERE status='reaping'")
        ]
        live = {row["id"] for row in conn.execute("SELECT id FROM sessions")}

    # Finish any interrupted reaps outside the short lock transaction: delete the
    # rows first (cascading the chunks rows), then sweep the staging directories.
    for session_id in claimed:
        _finalize_reaped_session(settings, session_id)

    if settings.chunk_dir.exists():
        for child in settings.chunk_dir.iterdir():
            if child.is_dir() and child.name not in live:
                shutil.rmtree(child, ignore_errors=True)


def reap_idle_sessions(settings: Settings, limit: int) -> tuple[list[str], int]:
    """Reclaim stale, still-active upload sessions and their staged chunks.

    Sessions whose last activity (``updated_at``) is older than the configured
    idle threshold are claimed with a single conditional state transition
    (active -> reaping) inside ``BEGIN IMMEDIATE``; hence a concurrent chunk
    upload or ``complete`` (which needs the session to stay active) and the
    reaper can never both win. Claimed sessions are deleted afterwards and
    their chunk directories removed. Completed sessions are never selected.

    Returns (reaped_session_ids, released_bytes); the byte count is the sum of
    the staged chunk sizes that were on disk for the claimed sessions.
    """
    cutoff = (
        datetime.now(timezone.utc)
        - timedelta(seconds=settings.session_idle_timeout_seconds)
    ).isoformat()

    with db_conn() as conn, immediate_tx(conn):
        candidates = [
            row["id"]
            for row in conn.execute(
                "SELECT id FROM sessions WHERE status='active' AND updated_at < ?"
                " ORDER BY updated_at ASC, id ASC LIMIT ?",
                (cutoff, limit),
            )
        ]
        if not candidates:
            return [], 0
        marks = ",".join("?" for _ in candidates)
        # The conditional claim: only still-active rows flip. Under the write
        # lock this matches every candidate, but the predicate is what makes the
        # transition safe regardless of lock scheduling.
        conn.execute(
            f"UPDATE sessions SET status='reaping', updated_at=? "
            f"WHERE status='active' AND id IN ({marks})",
            (utcnow(), *candidates),
        )
        claimed_rows = conn.execute(
            f"SELECT session_id, SUM(size) AS bytes FROM chunks "
            f"WHERE session_id IN ({marks}) GROUP BY session_id",
            candidates,
        ).fetchall()
        released = sum(row["bytes"] or 0 for row in claimed_rows)

    # Remove DB rows (chunks cascade) first, then the staging directories.
    for session_id in candidates:
        _finalize_reaped_session(settings, session_id)
    return candidates, released


def _finalize_reaped_session(settings: Settings, session_id: str) -> None:
    """Delete a claimed ('reaping') session's rows and staged chunk directory.

    The row is deleted (chunks cascade via the foreign key) before the directory
    is removed, so a crash at any point leaves either state the startup recovery
    knows how to finish; no active session can ever reference the path again.
    """
    with db_conn() as conn, immediate_tx(conn):
        conn.execute(
            "DELETE FROM sessions WHERE id=? AND status='reaping'", (session_id,)
        )
    shutil.rmtree(chunk_dir_for(settings, session_id), ignore_errors=True)
