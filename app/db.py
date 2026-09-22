"""SQLite access layer.

Every operation opens a short-lived connection (WAL mode, busy timeout) so
concurrent requests are serialized safely by SQLite itself. Long-running file
I/O (chunk merge) never happens inside a transaction; only the small state
transitions do, using BEGIN IMMEDIATE to take the write lock up front.
"""
from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

SCHEMA = """
CREATE TABLE IF NOT EXISTS sessions (
    id             TEXT PRIMARY KEY,
    file_name      TEXT NOT NULL,
    total_size     INTEGER NOT NULL,
    chunk_size     INTEGER NOT NULL,
    total_chunks   INTEGER NOT NULL,
    file_sha256    TEXT NOT NULL,
    status         TEXT NOT NULL DEFAULT 'active'
                   CHECK (status IN ('active', 'merging', 'reaping', 'completed')),
    archive_sha256 TEXT,
    last_error     TEXT,
    created_at     TEXT NOT NULL,
    updated_at     TEXT NOT NULL,
    completed_at   TEXT
);
CREATE INDEX IF NOT EXISTS idx_sessions_status ON sessions(status);

CREATE TABLE IF NOT EXISTS chunks (
    session_id  TEXT NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
    chunk_index INTEGER NOT NULL,
    size        INTEGER NOT NULL,
    sha256      TEXT NOT NULL,
    uploaded_at TEXT NOT NULL,
    PRIMARY KEY (session_id, chunk_index)
);

CREATE TABLE IF NOT EXISTS archives (
    sha256     TEXT PRIMARY KEY,
    file_name  TEXT NOT NULL,
    size       INTEGER NOT NULL,
    ref_count  INTEGER NOT NULL DEFAULT 0 CHECK (ref_count >= 0),
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS shares (
    token_hash     TEXT PRIMARY KEY,
    archive_sha256 TEXT NOT NULL
                   REFERENCES archives(sha256) ON DELETE CASCADE,
    max_downloads  INTEGER NOT NULL CHECK (max_downloads >= 1),
    downloads_used INTEGER NOT NULL DEFAULT 0
                   CHECK (downloads_used >= 0 AND downloads_used <= max_downloads),
    expires_at     TEXT NOT NULL,
    revoked        INTEGER NOT NULL DEFAULT 0 CHECK (revoked IN (0, 1)),
    created_at     TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_shares_archive ON shares(archive_sha256);
"""


def connect(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path), timeout=30.0, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA busy_timeout=30000")
    return conn


@contextmanager
def db_conn() -> Iterator[sqlite3.Connection]:
    from .config import get_settings

    conn = connect(get_settings().db_path)
    try:
        yield conn
    finally:
        conn.close()


@contextmanager
def immediate_tx(conn: sqlite3.Connection) -> Iterator[sqlite3.Connection]:
    """Explicit read-write transaction; takes the DB write lock immediately."""
    conn.execute("BEGIN IMMEDIATE")
    try:
        yield conn
    except BaseException:
        conn.execute("ROLLBACK")
        raise
    else:
        conn.execute("COMMIT")


def _migrate_sessions_status(conn: sqlite3.Connection) -> None:
    """Rebuild the sessions table when its status CHECK predates the 'reaping' state.

    SQLite cannot alter a CHECK constraint in place, so the table is rebuilt the
    documented way (rename, recreate, copy, drop) with foreign keys temporarily
    off; no rows are ever in 'reaping' in an old database, so the new CHECK
    accepts every existing value.
    """
    row = conn.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name='sessions'"
    ).fetchone()
    if row is not None and "'reaping'" in (row["sql"] if isinstance(row, sqlite3.Row) else row[0]):
        return
    if row is None:
        return  # no sessions table yet; SCHEMA creates it with the current CHECK
    conn.execute("PRAGMA foreign_keys=OFF")
    try:
        conn.execute("BEGIN")
        try:
            conn.execute(
                """
                CREATE TABLE sessions_new (
                    id             TEXT PRIMARY KEY,
                    file_name      TEXT NOT NULL,
                    total_size     INTEGER NOT NULL,
                    chunk_size     INTEGER NOT NULL,
                    total_chunks   INTEGER NOT NULL,
                    file_sha256    TEXT NOT NULL,
                    status         TEXT NOT NULL DEFAULT 'active'
                                   CHECK (status IN ('active', 'merging', 'reaping', 'completed')),
                    archive_sha256 TEXT,
                    last_error     TEXT,
                    created_at     TEXT NOT NULL,
                    updated_at     TEXT NOT NULL,
                    completed_at   TEXT
                )
                """
            )
            conn.execute(
                "INSERT INTO sessions_new (id, file_name, total_size, chunk_size, total_chunks,"
                " file_sha256, status, archive_sha256, last_error, created_at, updated_at,"
                " completed_at) SELECT id, file_name, total_size, chunk_size, total_chunks,"
                " file_sha256, status, archive_sha256, last_error, created_at, updated_at,"
                " completed_at FROM sessions"
            )
            conn.execute("DROP TABLE sessions")
            conn.execute("ALTER TABLE sessions_new RENAME TO sessions")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_sessions_status ON sessions(status)")
        except BaseException:
            conn.execute("ROLLBACK")
            raise
        else:
            conn.execute("COMMIT")
    finally:
        conn.execute("PRAGMA foreign_keys=ON")
    violations = conn.execute("PRAGMA foreign_key_check").fetchall()
    if violations:
        raise RuntimeError(f"foreign key integrity violations after migration: {violations}")


def init_db(db_path: Path) -> None:
    conn = connect(db_path)
    try:
        _migrate_sessions_status(conn)
        conn.executescript(SCHEMA)
    finally:
        conn.close()
