"""Tests for idle-session reaping: selection, atomic claim races and restart recovery."""
from __future__ import annotations

import sqlite3
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

from fastapi.testclient import TestClient

from .conftest import (
    create_session,
    gen,
    sha256,
    upload_all,
    upload_chunk,
)


# ---------------------------------------------------------------------------
# fixtures / helpers
# ---------------------------------------------------------------------------


def _settings():
    from app.config import get_settings

    return get_settings()


def _db_path() -> str:
    return str(_settings().db_path)


def _age_session(session_id: str, hours: int = 1) -> None:
    """Backdate updated_at so the session crosses the idle threshold."""
    old = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
    conn = sqlite3.connect(_db_path(), timeout=30.0)
    try:
        conn.execute(
            "UPDATE sessions SET updated_at=? WHERE id=?", (old, session_id)
        )
        conn.commit()
    finally:
        conn.close()


def _session_row(session_id: str):
    conn = sqlite3.connect(_db_path(), timeout=30.0)
    try:
        conn.row_factory = sqlite3.Row
        return conn.execute(
            "SELECT * FROM sessions WHERE id=?", (session_id,)
        ).fetchone()
    finally:
        conn.close()


def _reap(client: TestClient, limit: int = 100):
    resp = client.post("/v1/maintenance/reap-sessions", json={"limit": limit})
    assert resp.status_code == 200, resp.text
    return resp.json()


# ---------------------------------------------------------------------------
# selection / accounting
# ---------------------------------------------------------------------------


def test_reap_removes_idle_active_session_rows_chunks_and_dir(client, tmp_path):
    data = gen(150_000, b"reap")
    chunk_size = 50_000
    session = create_session(client, data, chunk_size)
    sid = session["session_id"]
    upload_chunk(client, sid, data, chunk_size, 0)
    upload_chunk(client, sid, data, chunk_size, 2)
    chunk_dir = tmp_path / "data" / "chunks" / sid
    assert chunk_dir.is_dir()
    assert len(list(chunk_dir.iterdir())) == 2

    # Fresh sessions are not reaped yet (idle threshold not reached).
    assert _reap(client)["reaped"] == 0

    _age_session(sid)
    result = _reap(client)
    assert result["reaped"] == 1
    assert result["reaped_ids"] == [sid]
    assert result["released_bytes"] == 2 * chunk_size
    assert result["limit"] == 100

    # Nothing may remain: no DB row, no chunks rows, no staging directory.
    assert _session_row(sid) is None
    assert not chunk_dir.exists()
    conn = sqlite3.connect(_db_path())
    try:
        assert conn.execute(
            "SELECT COUNT(*) FROM chunks WHERE session_id=?", (sid,)
        ).fetchone()[0] == 0
    finally:
        conn.close()

    # Requests for the reaped session are gone (404 after full removal).
    assert client.get(f"/v1/sessions/{sid}").status_code == 404
    assert client.post(f"/v1/sessions/{sid}/complete").status_code == 404
    assert client.delete(f"/v1/sessions/{sid}").status_code == 404


def test_reap_respects_limit_and_is_repeatable(client):
    sids = []
    for seed in (b"a", b"b", b"c", b"d"):
        data = gen(60_000, seed)
        sid = create_session(client, data, 30_000)["session_id"]
        upload_chunk(client, sid, data, 30_000, 0)
        _age_session(sid)
        sids.append(sid)

    first = _reap(client, limit=2)
    assert first["reaped"] == 2
    assert len(first["reaped_ids"]) == 2
    assert first["released_bytes"] == 60_000
    claimed = set(first["reaped_ids"])
    assert claimed <= set(sids)

    # The other two are untouched and still serviceable.
    remaining = [sid for sid in sids if sid not in claimed]
    assert len(remaining) == 2
    for sid in remaining:
        assert _session_row(sid)["status"] == "active"
        assert client.get(f"/v1/sessions/{sid}").status_code == 200

    second = _reap(client, limit=10)
    assert set(second["reaped_ids"]) == set(remaining)
    assert second["released_bytes"] == 60_000

    # Nothing left to reap.
    third = _reap(client, limit=10)
    assert third["reaped"] == 0
    assert third["reaped_ids"] == []
    assert third["released_bytes"] == 0


def test_reap_skips_recent_sessions_and_completed_sessions(client):
    # A recent active session must survive even when idle sessions are reaped.
    recent = gen(70_000, b"recent")
    recent_sid = create_session(client, recent, 35_000)["session_id"]
    upload_all(client, recent_sid, recent, 35_000)

    # A completed (published) session must NEVER be reaped, however old.
    published = gen(90_000, b"done")
    published_sid = create_session(client, published, 45_000)["session_id"]
    upload_all(client, published_sid, published, 45_000)
    assert client.post(f"/v1/sessions/{published_sid}/complete").status_code == 200
    _age_session(published_sid, hours=24 * 30)

    idle = gen(40_000, b"idle")
    idle_sid = create_session(client, idle, 20_000)["session_id"]
    upload_chunk(client, idle_sid, idle, 20_000, 0)
    _age_session(idle_sid)

    result = _reap(client, limit=50)
    assert result["reaped"] == 1
    assert result["reaped_ids"] == [idle_sid]

    recent_status = client.get(f"/v1/sessions/{recent_sid}").json()
    assert recent_status["status"] == "active"

    done_status = client.get(f"/v1/sessions/{published_sid}")
    assert done_status.status_code == 200
    assert done_status.json()["status"] == "completed"
    # The published archive is intact and downloadable.
    digest = done_status.json()["archive_sha256"]
    assert client.get(f"/v1/archives/{digest}/download").content == published


def test_chunk_upload_refreshes_idle_clock(client):
    data = gen(100_000, b"clock")
    sid = create_session(client, data, 50_000)["session_id"]
    _age_session(sid)

    # Activity on the session (a chunk landing) moves updated_at back to now,
    # so it must no longer qualify for reaping.
    upload_chunk(client, sid, data, 50_000, 0)
    assert _reap(client)["reaped"] == 0
    assert client.get(f"/v1/sessions/{sid}").status_code == 200


def test_reap_request_validation(client):
    assert (
        client.post("/v1/maintenance/reap-sessions", json={"limit": 0}).status_code
        == 422
    )
    assert (
        client.post("/v1/maintenance/reap-sessions", json={}).status_code == 422
    )
    assert (
        client.post(
            "/v1/maintenance/reap-sessions", json={"limit": -5}
        ).status_code
        == 422
    )


def test_reaped_session_requests_return_410_while_claimed(client, monkeypatch):
    """Between claim and finalization every session request must see 410 Gone."""
    from app import service
    from app.config import get_settings

    data = gen(80_000, b"claim")
    sid = create_session(client, data, 40_000)["session_id"]
    upload_all(client, sid, data, 40_000)
    _age_session(sid)

    seen = {"statuses": []}
    original = service._finalize_reaped_session

    def slow_finalize(settings, session_id):
        # The row is already deleted at this point; pause so an in-flight request
        # instead observes the claimed ('reaping') state by deferring deletion:
        # hook just before deletion via a small sleep around the real call.
        barrier.wait(timeout=5)
        return original(settings, session_id)

    barrier = threading.Event()
    monkeypatch.setattr(service, "_finalize_reaped_session", slow_finalize)

    def run_reap():
        seen["result"] = service.reap_idle_sessions(get_settings(), 10)

    worker = threading.Thread(target=run_reap)
    worker.start()

    # Wait until the claim transaction committed (status flipped to reaping)
    # but force that ordering by polling the DB directly.
    claimed = False
    for _ in range(100):
        row = _session_row(sid)
        if row is not None and row["status"] == "reaping":
            claimed = True
            break
        threading.Event().wait(0.01)
    assert claimed, "session never entered the claimed 'reaping' state"

    assert client.get(f"/v1/sessions/{sid}").status_code == 410
    assert client.post(f"/v1/sessions/{sid}/complete").status_code == 410
    assert client.delete(f"/v1/sessions/{sid}").status_code == 410
    piece = data[:40_000]
    blocked = client.put(
        f"/v1/sessions/{sid}/chunks/0",
        content=piece,
        headers={"X-Chunk-SHA256": sha256(piece)},
    )
    assert blocked.status_code == 410

    barrier.set()
    worker.join(timeout=5)
    assert not worker.is_alive()
    reaped_ids, released = seen["result"]
    assert reaped_ids == [sid]
    assert released == len(data)
    assert _session_row(sid) is None


# ---------------------------------------------------------------------------
# concurrency: only one side wins, nothing is left behind
# ---------------------------------------------------------------------------


def test_concurrent_complete_vs_reap_only_one_wins(client):
    data = gen(300_000, b"race-complete")
    chunk_size = 100_000
    sid = create_session(client, data, chunk_size)["session_id"]
    upload_all(client, sid, data, chunk_size)
    _age_session(sid)

    local = threading.local()

    def worker_client():
        c = getattr(local, "client", None)
        if c is None:
            c = TestClient(client.app)
            local.client = c
        return c

    outcomes = {"complete": [], "reap": []}
    lock = threading.Lock()

    def complete_once(_):
        resp = worker_client().post(f"/v1/sessions/{sid}/complete")
        with lock:
            outcomes["complete"].append(resp.status_code)

    def reap_once(_):
        resp = worker_client().post(
            "/v1/maintenance/reap-sessions", json={"limit": 1}
        )
        with lock:
            body = resp.json() if resp.status_code == 200 else None
            outcomes["reap"].append((resp.status_code, body))

    with ThreadPoolExecutor(max_workers=8) as pool:
        futures = [pool.submit(complete_once, i) for i in range(8)]
        futures += [pool.submit(reap_once, i) for i in range(8)]
        for future in futures:
            future.result(timeout=30)

    # No server errors in any outcome.
    assert all(code < 500 for code in outcomes["complete"])
    assert all(code < 500 for code, _ in outcomes["reap"])

    # Mutual exclusion: the session is either completed or gone, never both.
    row = _session_row(sid)
    digest = sha256(data)
    conn = sqlite3.connect(_db_path())
    try:
        archive = conn.execute(
            "SELECT ref_count FROM archives WHERE sha256=?", (digest,)
        ).fetchone()
    finally:
        conn.close()

    if row is not None:
        # complete won: the archive was published exactly once (ref_count 1);
        # 200s after the winning commit are the documented idempotent retries.
        assert row["status"] == "completed"
        assert archive is not None and archive[0] == 1
        assert any(code == 200 for code in outcomes["complete"])
        assert all(
            body is None or body["reaped"] == 0 for _, body in outcomes["reap"]
        )
        download = client.get(f"/v1/archives/{digest}/download")
        assert download.status_code == 200 and download.content == data
    else:
        # reap won: no complete succeeded, archive never created, dirs gone.
        assert all(code != 200 for code in outcomes["complete"])
        reapers = [body for code, body in outcomes["reap"] if code == 200]
        assert sum(body["reaped"] for body in reapers) == 1
        assert archive is None


def test_concurrent_chunk_upload_vs_reap_no_orphans(client):
    """Stress: uploads and reapers interleave; no DB rows or dirs dangle."""
    data = gen(250_000, b"race-upload")
    chunk_size = 50_000
    sessions = []
    for i in range(6):
        sid = create_session(client, data, chunk_size, name=f"r{i}.bin")[
            "session_id"
        ]
        _age_session(sid)
        sessions.append(sid)

    local = threading.local()

    def worker_client():
        c = getattr(local, "client", None)
        if c is None:
            c = TestClient(client.app)
            local.client = c
        return c

    codes = {"upload": [], "reap": []}
    lock = threading.Lock()

    def upload_attempt(args):
        sid, index = args
        piece = data[index * chunk_size : (index + 1) * chunk_size]
        resp = worker_client().put(
            f"/v1/sessions/{sid}/chunks/{index}",
            content=piece,
            headers={"X-Chunk-SHA256": sha256(piece)},
        )
        with lock:
            codes["upload"].append(resp.status_code)

    def reap_attempt(_):
        resp = worker_client().post(
            "/v1/maintenance/reap-sessions", json={"limit": 2}
        )
        with lock:
            codes["reap"].append(resp.status_code)

    attempts = [(sid, index) for sid in sessions for index in range(5)]
    with ThreadPoolExecutor(max_workers=12) as pool:
        futures = [pool.submit(upload_attempt, a) for a in attempts]
        futures += [pool.submit(reap_attempt, i) for i in range(15)]
        for future in futures:
            future.result(timeout=30)

    # Every upload ended in 200 (session alive) or 404/410 (reaper won);
    # no 409/500 from the state race.
    assert set(codes["upload"]) <= {200, 404, 410}
    assert codes["reap"] and all(c == 200 for c in codes["reap"])

    # Global invariants: no 'reaping' rows survive, chunks rows always have a
    # parent session, and every staging dir either matches a live session or is
    # gone (never an orphaned or half-deleted directory).
    from app.config import get_settings

    settings = get_settings()
    conn = sqlite3.connect(_db_path())
    try:
        stuck = conn.execute(
            "SELECT id FROM sessions WHERE status='reaping'"
        ).fetchall()
        assert stuck == []
        orphan_rows = conn.execute(
            "SELECT c.session_id FROM chunks c LEFT JOIN sessions s"
            " ON s.id = c.session_id WHERE s.id IS NULL"
        ).fetchall()
        assert orphan_rows == []
        live = {r[0] for r in conn.execute("SELECT id FROM sessions")}
    finally:
        conn.close()

    chunks_root = settings.chunk_dir
    if chunks_root.exists():
        for child in chunks_root.iterdir():
            assert child.name in live
            # A live active session's uploaded chunks must be coherent.
            for part in child.iterdir():
                assert part.suffix == ".part"


# ---------------------------------------------------------------------------
# restart recovery
# ---------------------------------------------------------------------------


def test_interrupted_reap_finished_on_restart(client, tmp_path):
    """A crash between the claim and the deletion resumes cleanup at startup."""
    data = gen(140_000, b"interrupted")
    chunk_size = 70_000
    sid = create_session(client, data, chunk_size)["session_id"]
    upload_all(client, sid, data, chunk_size)
    chunk_dir = tmp_path / "data" / "chunks" / sid
    assert chunk_dir.is_dir()

    # Simulate the exact post-crash state: session claimed, chunks still present.
    old = (datetime.now(timezone.utc) - timedelta(hours=2)).isoformat()
    conn = sqlite3.connect(_db_path())
    try:
        conn.execute(
            "UPDATE sessions SET status='reaping', updated_at=? WHERE id=?",
            (old, sid),
        )
        conn.commit()
    finally:
        conn.close()

    import os

    os.environ["DATA_DIR"] = str(tmp_path / "data")
    from app.config import get_settings

    get_settings.cache_clear()
    from app.main import app as app2

    with TestClient(app2) as restarted:
        # Startup recovery removed the claimed session, its chunks and its dir.
        assert restarted.get(f"/v1/sessions/{sid}").status_code == 404
        assert not chunk_dir.exists()
        followup = restarted.post(
            "/v1/maintenance/reap-sessions", json={"limit": 10}
        ).json()
        assert followup["reaped"] == 0
        conn = sqlite3.connect(_db_path())
        try:
            assert conn.execute(
                "SELECT COUNT(*) FROM sessions WHERE status='reaping'"
            ).fetchone()[0] == 0
            assert conn.execute(
                "SELECT COUNT(*) FROM chunks WHERE session_id=?", (sid,)
            ).fetchone()[0] == 0
        finally:
            conn.close()


def test_merging_interrupted_before_reap_recovers_and_is_then_reaped(
    client, tmp_path
):
    """Recovery order: stale 'merging' resets to active; a reap after restart
    reclaims it once it is idle — while completed sessions stay untouched."""
    data = gen(120_000, b"merging-restart")
    chunk_size = 60_000
    merging_sid = create_session(client, data, chunk_size)["session_id"]
    upload_all(client, merging_sid, data, chunk_size)

    published = gen(80_000, b"keep")
    pub_sid = create_session(client, published, 40_000)["session_id"]
    upload_all(client, pub_sid, published, 40_000)
    assert client.post(f"/v1/sessions/{pub_sid}/complete").status_code == 200
    digest = sha256(published)

    old = (datetime.now(timezone.utc) - timedelta(hours=3)).isoformat()
    conn = sqlite3.connect(_db_path())
    try:
        conn.execute(
            "UPDATE sessions SET status='merging', updated_at=? WHERE id=?",
            (old, merging_sid),
        )
        conn.commit()
    finally:
        conn.close()

    import os

    os.environ["DATA_DIR"] = str(tmp_path / "data")
    from app.config import get_settings

    get_settings.cache_clear()
    from app.main import app as app2

    with TestClient(app2) as restarted:
        row = restarted.get(f"/v1/sessions/{merging_sid}").json()
        assert row["status"] == "active"  # reset by recovery

        # It is already older than the default threshold; reaping finishes it.
        result = restarted.post(
            "/v1/maintenance/reap-sessions", json={"limit": 10}
        ).json()
        assert result["reaped_ids"] == [merging_sid]
        assert result["released_bytes"] == len(data)
        assert restarted.get(f"/v1/sessions/{merging_sid}").status_code == 404

        # The completed session and its archive survive.
        assert (
            restarted.get(f"/v1/sessions/{pub_sid}").json()["status"]
            == "completed"
        )
        assert restarted.get(f"/v1/archives/{digest}/download").content == published


def test_orphan_chunk_dir_from_interrupted_delete_swept_on_restart(
    client, tmp_path
):
    """A chunk dir with no session row is swept at startup (regression guard)."""
    data = gen(50_000, b"orphan")
    sid = create_session(client, data, 25_000)["session_id"]
    upload_all(client, sid, data, 25_000)
    chunk_dir = tmp_path / "data" / "chunks" / sid

    # Drop the row but leave the directory on disk, then restart.
    conn = sqlite3.connect(_db_path())
    try:
        conn.execute("DELETE FROM sessions WHERE id=?", (sid,))
        conn.commit()
    finally:
        conn.close()
    assert chunk_dir.is_dir()

    import os

    os.environ["DATA_DIR"] = str(tmp_path / "data")
    from app.config import get_settings

    get_settings.cache_clear()
    from app.main import app as app2

    with TestClient(app2):
        assert not chunk_dir.exists()


def test_legacy_database_migrates_to_reaping_status(tmp_path, monkeypatch):
    """A volume with the pre-reaper schema keeps its data and gains 'reaping'."""
    from app.db import connect, init_db

    monkeypatch.setenv("DATA_DIR", str(tmp_path / "legacy"))
    from app.config import get_settings

    get_settings.cache_clear()
    settings = get_settings()
    settings.data_dir.mkdir(parents=True, exist_ok=True)

    # Build a database with the ORIGINAL sessions CHECK (no 'reaping').
    conn = connect(settings.db_path)
    try:
        conn.execute(
            """
            CREATE TABLE sessions (
                id TEXT PRIMARY KEY, file_name TEXT NOT NULL,
                total_size INTEGER NOT NULL, chunk_size INTEGER NOT NULL,
                total_chunks INTEGER NOT NULL, file_sha256 TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'active'
                CHECK (status IN ('active', 'merging', 'completed')),
                archive_sha256 TEXT, last_error TEXT,
                created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
                completed_at TEXT
            )
            """
        )
        conn.execute(
            "CREATE TABLE chunks (session_id TEXT NOT NULL"
            " REFERENCES sessions(id) ON DELETE CASCADE, chunk_index INTEGER NOT NULL,"
            " size INTEGER NOT NULL, sha256 TEXT NOT NULL, uploaded_at TEXT NOT NULL,"
            " PRIMARY KEY (session_id, chunk_index))"
        )
        now = datetime.now(timezone.utc).isoformat()
        conn.execute(
            "INSERT INTO sessions (id, file_name, total_size, chunk_size,"
            " total_chunks, file_sha256, status, created_at, updated_at)"
            " VALUES ('sess-1','old.bin',10,10,1,?, 'active', ?, ?)",
            ("a" * 64, now, now),
        )
        conn.execute(
            "INSERT INTO chunks VALUES ('sess-1', 0, 10, ?, ?)",
            ("b" * 64, now),
        )
        conn.commit()
        # The old schema rejects 'reaping'.
        constraint_failed = False
        try:
            conn.execute(
                "UPDATE sessions SET status='reaping' WHERE id='sess-1'"
            )
            conn.commit()
        except sqlite3.IntegrityError:
            constraint_failed = True
            conn.rollback()
        assert constraint_failed
    finally:
        conn.close()

    # Startup migration preserves rows and widens the constraint.
    init_db(settings.db_path)
    conn = connect(settings.db_path)
    try:
        assert conn.execute("PRAGMA foreign_key_check").fetchall() == []
        row = conn.execute(
            "SELECT status, file_name FROM sessions WHERE id='sess-1'"
        ).fetchone()
        assert row[1] == "old.bin" and row[0] == "active"
        chunk = conn.execute(
            "SELECT size, sha256 FROM chunks WHERE session_id='sess-1'"
        ).fetchone()
        assert tuple(chunk) == (10, "b" * 64)
        conn.execute("UPDATE sessions SET status='reaping' WHERE id='sess-1'")
        # cascading delete still works through the rebuilt table
        conn.execute("DELETE FROM sessions WHERE id='sess-1'")
        conn.commit()
        assert conn.execute("SELECT COUNT(*) FROM chunks").fetchone()[0] == 0
    finally:
        conn.close()

    # Re-running init_db on the migrated DB is a no-op (idempotent startup).
    init_db(settings.db_path)
    get_settings.cache_clear()
