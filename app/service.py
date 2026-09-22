"""Merge, session reaping and recovery logic shared by the routers."""
from __future__ import annotations

import hashlib
import shutil
import uuid
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


def _finish_reaped_session(
    settings: Settings,
    session_id: str,
    run_id: str,
    recovered: bool,
    finished_at: str,
) -> tuple[int, int]:
    """Complete one claimed reap.

    The claim (active -> reaping) is already durable and already blocks every
    chunk upload / complete by itself. We remove the chunk directory, then in a
    single write transaction: measure the claimed bytes, write the reap
    tombstone and delete the session plus its chunk rows. A crash at any point
    simply leaves the session in 'reaping' for startup recovery to resume with
    the byte accounting still intact.

    Returns (received_chunks, released_bytes).
    """
    # Filesystem cleanup happens with no transaction held (same as DELETE/complete).
    shutil.rmtree(chunk_dir_for(settings, session_id), ignore_errors=True)

    with db_conn() as conn, immediate_tx(conn):
        claimed = conn.execute(
            "SELECT * FROM sessions WHERE id=? AND status='reaping'", (session_id,)
        ).fetchone()
        if claimed is None:
            # Another worker (or a previous run) already finalized this session.
            tombstone = conn.execute(
                "SELECT received_chunks, released_bytes FROM reaped_sessions WHERE id=?",
                (session_id,),
            ).fetchone()
            if tombstone is None:
                raise RuntimeError(f"session {session_id} vanished while reaping")
            return tombstone["received_chunks"], tombstone["released_bytes"]
        stats = conn.execute(
            "SELECT COUNT(*) AS n, COALESCE(SUM(size), 0) AS bytes"
            " FROM chunks WHERE session_id=?",
            (session_id,),
        ).fetchone()
        received = stats["n"]
        released = stats["bytes"]
        conn.execute(
            "INSERT INTO reaped_sessions (id, file_name, total_size, chunk_size,"
            " total_chunks, file_sha256, received_chunks, released_bytes, reaped_at,"
            " run_id, recovered)"
            " VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            (
                claimed["id"],
                claimed["file_name"],
                claimed["total_size"],
                claimed["chunk_size"],
                claimed["total_chunks"],
                claimed["file_sha256"],
                received,
                released,
                finished_at,
                run_id,
                1 if recovered else 0,
            ),
        )
        conn.execute("DELETE FROM chunks WHERE session_id=?", (session_id,))
        conn.execute("DELETE FROM sessions WHERE id=? AND status='reaping'", (session_id,))
    return received, released


def _resume_interrupted_reaps(settings: Settings) -> tuple[list[tuple[str, int, int]], list[str]]:
    """Finish every session left in 'reaping' by a crash.

    Returns ((session_id, received_chunks, released_bytes), ...) and the ids of
    the original reap runs that were interrupted.
    """
    with db_conn() as conn:
        pending = [
            row["id"]
            for row in conn.execute(
                "SELECT id FROM sessions WHERE status='reaping' ORDER BY updated_at"
            )
        ]
        interrupted_runs = [
            row["run_id"]
            for row in conn.execute(
                "SELECT run_id FROM reap_runs WHERE finished_at IS NULL"
            )
        ]
    recovered: list[tuple[str, int, int]] = []
    if pending:
        # One synthetic recovery run owns all sessions resumed at this startup.
        run_id = "recover-" + uuid.uuid4().hex
        now = utcnow()
        with db_conn() as conn, immediate_tx(conn):
            conn.execute(
                "INSERT INTO reap_runs (run_id, started_at, finished_at, recovered,"
                " session_count, released_bytes) VALUES (?,?,?,1,0,0)",
                (run_id, now, now),
            )
        for session_id in pending:
            received, released = _finish_reaped_session(settings, session_id, run_id, True, now)
            recovered.append((session_id, received, released))
        total_bytes = sum(item[2] for item in recovered)
        with db_conn() as conn, immediate_tx(conn):
            conn.execute(
                "UPDATE reap_runs SET session_count=?, released_bytes=?"
                " WHERE run_id=?",
                (len(recovered), total_bytes, run_id),
            )
    if interrupted_runs:
        now = utcnow()
        with db_conn() as conn, immediate_tx(conn):
            for run_id in interrupted_runs:
                conn.execute(
                    "UPDATE reap_runs SET finished_at=COALESCE(finished_at, ?), recovered=1"
                    " WHERE run_id=?",
                    (now, run_id),
                )
    return recovered, interrupted_runs


def reap_idle_sessions(
    settings: Settings, limit: int
) -> tuple[str, list[tuple[str, int, int]], list[tuple[str, int, int]]]:
    """Reap up to ``limit`` active sessions idle past the configured threshold.

    Sessions are first claimed with a conditional ``active -> 'reaping'`` state
    transition inside a write transaction; that status alone gates every chunk
    upload and ``complete``, so only one side of a race can win. Only afterwards
    are the chunk directories removed and the sessions finalized (tombstone plus
    chunk/session row deletion), keeping the released-byte accounting crash-safe.
    Interrupted reaps are resumed first.

    Returns (run_id, reaped, recovered) where each list element is
    (session_id, received_chunks, released_bytes).
    """
    recovered, _ = _resume_interrupted_reaps(settings)

    run_id = uuid.uuid4().hex
    started_at = utcnow()
    with db_conn() as conn, immediate_tx(conn):
        conn.execute(
            "INSERT INTO reap_runs (run_id, started_at, recovered) VALUES (?, ?, 0)",
            (run_id, started_at),
        )

    cutoff = (datetime.now(timezone.utc) - timedelta(seconds=settings.session_idle_timeout_seconds)).isoformat()
    with db_conn() as conn, immediate_tx(conn):
        candidates = [
            row["id"]
            for row in conn.execute(
                "SELECT id FROM sessions WHERE status='active' AND updated_at <= ?"
                " ORDER BY updated_at LIMIT ?",
                (cutoff, limit),
            )
        ]
        claimed: list[str] = []
        for session_id in candidates:
            # The gate: only a session that is still active gets reaped. If a
            # concurrent chunk upload / complete moved it, rowcount is 0 and the
            # reaper skips it (exactly one side of the race wins).
            cur = conn.execute(
                "UPDATE sessions SET status='reaping', updated_at=? WHERE id=? AND status='active'",
                (utcnow(), session_id),
            )
            if cur.rowcount == 1:
                claimed.append(session_id)

    reaped: list[tuple[str, int, int]] = []
    finished_at = utcnow()
    for session_id in claimed:
        received, released = _finish_reaped_session(settings, session_id, run_id, False, finished_at)
        reaped.append((session_id, received, released))

    total_bytes = sum(item[2] for item in reaped)
    with db_conn() as conn, immediate_tx(conn):
        conn.execute(
            "UPDATE reap_runs SET finished_at=?, session_count=?, released_bytes=?"
            " WHERE run_id=?",
            (finished_at, len(reaped), total_bytes, run_id),
        )
    return run_id, reaped, recovered


def get_reaped_session(conn, session_id: str):
    """Look up the reap tombstone (used to answer 410 for already-reaped ids)."""
    return conn.execute(
        "SELECT * FROM reaped_sessions WHERE id=?", (session_id,)
    ).fetchone()


def recover_after_restart(settings: Settings) -> None:
    """Crash recovery, run once at startup.

    - Finish any reap interrupted by the crash: sessions stuck in 'reaping'
      have already been claimed (no chunk upload or complete can have touched
      them); remove their chunk directories and finalize their tombstones.
    - A crash mid-merge leaves a session in 'merging'; no merge can be in flight
      right after startup, so flip those back to 'active' for a safe retry.
    - Chunk directories without a matching session row are orphaned; remove them.
    """
    _resume_interrupted_reaps(settings)

    with db_conn() as conn, immediate_tx(conn):
        conn.execute(
            "UPDATE sessions SET status='active', updated_at=? WHERE status='merging'",
            (utcnow(),),
        )
        live = {row["id"] for row in conn.execute("SELECT id FROM sessions")}
    if settings.chunk_dir.exists():
        for child in settings.chunk_dir.iterdir():
            if child.is_dir() and child.name not in live:
                shutil.rmtree(child, ignore_errors=True)
