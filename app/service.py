"""Merge and recovery logic shared by the routers."""
from __future__ import annotations

import hashlib
import shutil
from datetime import datetime, timezone
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
    - Chunk directories without a matching session row are orphaned; remove them.
    """
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
