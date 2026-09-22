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

CREATE TABLE IF NOT EXISTS reaped_sessions (
    id               TEXT PRIMARY KEY,
    file_name        TEXT NOT NULL,
    total_size       INTEGER NOT NULL,
    chunk_size       INTEGER NOT NULL,
    total_chunks     INTEGER NOT NULL,
    file_sha256      TEXT NOT NULL,
    received_chunks  INTEGER NOT NULL,
    released_bytes   INTEGER NOT NULL,
    reaped_at        TEXT NOT NULL,
    run_id           TEXT NOT NULL,
    recovered        INTEGER NOT NULL DEFAULT 0 CHECK (recovered IN (0, 1))
);

CREATE TABLE IF NOT EXISTS reap_runs (
    run_id         TEXT PRIMARY KEY,
    started_at     TEXT NOT NULL,
    finished_at    TEXT,
    recovered      INTEGER NOT NULL DEFAULT 0 CHECK (recovered IN (0, 1)),
    session_count  INTEGER NOT NULL DEFAULT 0,
    released_bytes INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_reaped_at ON reaped_sessions(reaped_at);
"""

# Rebuild the sessions table so its status CHECK also allows 'reaping'. This
# migrates databases created before the reaper existed; fresh databases are
# already created with the new CHECK and skip the rebuild entirely. The chunks
# table is never dropped: per the SQLite table-rebuild recipe, renaming the
# referenced table to sessions_new transparently rebinds chunks' foreign key
# (its schema SQL keeps the "sessions" name, resolved at rename time), so chunk
# rows are preserved with ON DELETE CASCADE intact.
_MIGRATE_STATUS_CHECK = """
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
);
INSERT INTO sessions_new (id, file_name, total_size, chunk_size, total_chunks,
    file_sha256, status, archive_sha256, last_error, created_at, updated_at, completed_at)
SELECT id, file_name, total_size, chunk_size, total_chunks,
    file_sha256, status, archive_sha256, last_error, created_at, updated_at, completed_at
FROM sessions;
DROP TABLE sessions;
ALTER TABLE sessions_new RENAME TO sessions;
CREATE INDEX idx_sessions_status ON sessions(status);
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


def init_db(db_path: Path) -> None:
    conn = connect(db_path)
    try:
        conn.executescript(SCHEMA)
        # Upgrade databases created before 'reaping' was a valid session state.
        # The table rebuild follows the SQLite recipe: foreign-key enforcement
        # is disabled so DROP TABLE does not cascade-delete the chunks rows
        # (their reference transparently follows the renamed parent table).
        create_sql = conn.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name='sessions'"
        ).fetchone()
        if create_sql is not None and "'reaping'" not in create_sql[0]:
            conn.execute("PRAGMA foreign_keys=OFF")
            try:
                conn.executescript(_MIGRATE_STATUS_CHECK)
                violations = conn.execute("PRAGMA foreign_key_check").fetchall()
                if violations:
                    raise RuntimeError(
                        f"schema migration left {len(violations)} foreign-key violations"
                    )
            finally:
                conn.execute("PRAGMA foreign_keys=ON")
    finally:
        conn.close()
