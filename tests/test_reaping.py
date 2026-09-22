"""Tests for idle-session reaping: threshold, atomic claims, races, restart."""
from __future__ import annotations

import sqlite3
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

import pytest
from fastapi.testclient import TestClient

from .conftest import (
    create_session,
    gen,
    publish_archive,
    sha256,
    upload_all,
    upload_chunk,
)


def _db_path(client: TestClient):
    from app.config import get_settings

    return get_settings().db_path


def _set_updated_at(db_path, session_id: str, when: datetime) -> None:
    conn = sqlite3.connect(str(db_path), timeout=30.0)
    try:
        conn.execute(
            "UPDATE sessions SET updated_at=? WHERE id=?",
            (when.isoformat(), session_id),
        )
        conn.commit()
    finally:
        conn.close()


def _set_status(db_path, session_id: str, status: str) -> None:
    conn = sqlite3.connect(str(db_path), timeout=30.0)
    try:
        conn.execute("UPDATE sessions SET status=? WHERE id=?", (status, session_id))
        conn.commit()
    finally:
        conn.close()


def _age(client: TestClient, session_id: str, seconds: float = 7200) -> None:
    _set_updated_at(
        _db_path(client),
        session_id,
        datetime.now(timezone.utc) - timedelta(seconds=seconds),
    )


def _reap(client: TestClient, limit: int = 100):
    resp = client.post("/v1/maintenance/reap-sessions", json={"limit": limit})
    assert resp.status_code == 200, resp.text
    return resp.json()


def test_reap_removes_only_idle_active_sessions_and_returns_bytes(client, tmp_path):
    data_old = gen(150_000, b"old")
    old = create_session(client, data_old, 50_000)
    upload_chunk(client, old["session_id"], data_old, 50_000, 0)
    upload_chunk(client, old["session_id"], data_old, 50_000, 1)
    _age(client, old["session_id"])

    # Fresh, busy session: must be left alone.
    data_new = gen(100_000, b"new")
    new = create_session(client, data_new, 50_000)
    upload_chunk(client, new["session_id"], data_new, 50_000, 0)

    # Idle session with no chunks at all: also reaped, releases zero bytes.
    empty = create_session(client, b"x" * 10, 10, name="empty.bin")
    _age(client, empty["session_id"])

    body = _reap(client, limit=100)
    ids = {item["session_id"] for item in body["reaped"]}
    assert ids == {old["session_id"], empty["session_id"]}
    assert body["reaped_count"] == 2
    assert body["idle_timeout_seconds"] == 3600
    assert body["limit"] == 100
    assert len(body["run_id"]) >= 16
    assert body["recovered"] is False

    by_id = {item["session_id"]: item for item in body["reaped"]}
    assert by_id[old["session_id"]]["received_chunks"] == 2
    assert by_id[old["session_id"]]["released_bytes"] == 100_000
    assert body["released_bytes"] == 100_000
    assert by_id[empty["session_id"]]["released_bytes"] == 0

    # Fresh session is intact: remaining chunk still uploads and completes.
    assert client.get(f"/v1/sessions/{new['session_id']}").status_code == 200
    upload_chunk(client, new["session_id"], data_new, 50_000, 1)
    assert client.post(f"/v1/sessions/{new['session_id']}/complete").status_code == 200

    # No DB rows and no chunk directory left behind for reaped sessions.
    conn = sqlite3.connect(str(_db_path(client)))
    try:
        for sid in ids:
            assert conn.execute(
                "SELECT COUNT(*) FROM sessions WHERE id=?", (sid,)
            ).fetchone()[0] == 0
            assert conn.execute(
                "SELECT COUNT(*) FROM chunks WHERE session_id=?", (sid,)
            ).fetchone()[0] == 0
            assert conn.execute(
                "SELECT COUNT(*) FROM reaped_sessions WHERE id=?", (sid,)
            ).fetchone()[0] == 1
    finally:
        conn.close()
    assert not (tmp_path / "data" / "chunks" / old["session_id"]).exists()
    assert not (tmp_path / "data" / "chunks" / empty["session_id"]).exists()


def test_reap_limit_takes_oldest_first_and_is_ordered(client):
    sids = []
    for i in range(3):
        s = create_session(client, gen(10_000 + i, f"s{i}".encode()), 10_000)
        sids.append(s["session_id"])
        _age(client, s["session_id"], seconds=7200 - i * 600)  # sid[0] oldest

    body = _reap(client, limit=2)
    assert body["reaped_count"] == 2
    assert [item["session_id"] for item in body["reaped"]] == sids[:2]
    # Third call reaps the rest; an empty run is a harmless no-op.
    assert _reap(client)["reaped_count"] == 1
    empty = _reap(client)
    assert empty["reaped"] == []
    assert empty["released_bytes"] == 0


def test_completed_sessions_are_never_reaped(client):
    data = gen(120_000, b"done")
    sha = publish_archive(client, data, chunk_size=60_000)

    # Even a long-completed session (updated_at in the past) stays completed.
    conn = sqlite3.connect(str(_db_path(client)))
    try:
        sid = conn.execute(
            "SELECT id FROM sessions WHERE archive_sha256=?", (sha,)
        ).fetchone()[0]
    finally:
        conn.close()
    _age(client, sid)

    body = _reap(client)
    assert body["reaped"] == []
    status = client.get(f"/v1/sessions/{sid}").json()
    assert status["status"] == "completed"
    assert client.get(f"/v1/archives/{sha}/download").content == data


def test_reaped_session_requests_get_410(client):
    s = create_session(client, gen(10_000, b"gone"), 10_000)
    sid = s["session_id"]
    upload_chunk(client, sid, gen(10_000, b"gone"), 10_000, 0)
    _age(client, sid)
    _reap(client)

    assert client.get(f"/v1/sessions/{sid}").status_code == 410
    assert client.delete(f"/v1/sessions/{sid}").status_code == 410
    assert client.post(f"/v1/sessions/{sid}/complete").status_code == 410
    resp = client.put(
        f"/v1/sessions/{sid}/chunks/0",
        content=b"x" * 10,
        headers={"X-Chunk-SHA256": sha256(b"x" * 10)},
    )
    assert resp.status_code == 410
    # Unknown ids keep their 404 (410 is reserved for reaped sessions).
    assert client.get("/v1/sessions/unknown-id").status_code == 404


def test_idle_threshold_boundary(client):
    # Just under the default 3600 s threshold: not reaped.
    recent = create_session(client, gen(10_000, b"recent"), 10_000)
    _set_updated_at(
        _db_path(client),
        recent["session_id"],
        datetime.now(timezone.utc) - timedelta(seconds=3599),
    )
    # Exactly at the threshold: reaped.
    edge = create_session(client, gen(10_000, b"edge"), 10_000)
    _set_updated_at(
        _db_path(client),
        edge["session_id"],
        datetime.now(timezone.utc) - timedelta(seconds=3600),
    )
    ids = {item["session_id"] for item in _reap(client)["reaped"]}
    assert edge["session_id"] in ids
    assert recent["session_id"] not in ids


def test_reap_validates_limit(client):
    assert client.post("/v1/maintenance/reap-sessions", json={"limit": 0}).status_code == 422
    assert client.post("/v1/maintenance/reap-sessions", json={"limit": 5000}).status_code == 422
    assert client.post("/v1/maintenance/reap-sessions", json={}).status_code == 422


def test_chunk_upload_activity_resets_idle_timer(client):
    data = gen(30_000, b"active-upload")
    s = create_session(client, data, 10_000)
    sid = s["session_id"]
    _age(client, sid, seconds=4000)  # would be stale...
    # ...but a chunk arrives, bumping updated_at inside the upload transaction.
    upload_chunk(client, sid, data, 10_000, 0)
    assert _reap(client)["reaped"] == []
    assert client.get(f"/v1/sessions/{sid}").status_code == 200


def test_concurrent_reap_and_complete_only_one_wins(client):
    """A complete running head-to-head with the reaper: never both succeed."""
    chunk_size = 60_000

    outcomes = {"completed": 0, "reaped_410": 0, "other": 0}
    lock = threading.Lock()

    def run(i):
        local = threading.local()
        worker = getattr(local, "client", None)
        if worker is None:
            worker = TestClient(client.app)
            local.client = worker

        # Unique content per iteration: workers are reused across iterations,
        # so identical bytes would trigger an instant complete on prior runs.
        data = gen(120_000, f"race-complete-{i}".encode())
        s = create_session(worker, data, chunk_size, name=f"race-{i}.bin")
        sid = s["session_id"]
        upload_all(worker, sid, data, chunk_size)
        _age(worker, sid)

        barrier = threading.Barrier(2)

        def complete():
            barrier.wait()
            return worker.post(f"/v1/sessions/{sid}/complete")

        def reap():
            barrier.wait()
            return worker.post("/v1/maintenance/reap-sessions", json={"limit": 100})

        with ThreadPoolExecutor(max_workers=2) as pool:
            f_comp = pool.submit(complete)
            f_reap = pool.submit(reap)
            comp = f_comp.result()
            reap_resp = f_reap.result()

        assert comp.status_code in (200, 410), comp.text
        with lock:
            if comp.status_code == 200:
                outcomes["completed"] += 1
                # Reaper must not have claimed the completed session.
                assert client.get(f"/v1/sessions/{sid}").json()["status"] == "completed"
            else:
                outcomes["reaped_410"] += 1
                assert "reap" in comp.json()["detail"]
            # Final invariant: nothing was left in 'reaping', archive download works
            # exactly for the sessions that completed.
            reaped_ids = {r["session_id"] for r in reap_resp.json()["reaped"]}
            if comp.status_code == 200:
                assert sid not in reaped_ids
            else:
                assert sid in reaped_ids
        return sid, comp.status_code, sha256(data)

    with ThreadPoolExecutor(max_workers=1) as outer:
        results = list(outer.map(run, range(24)))

    # Both outcomes must occur under contention; every iteration was consistent.
    assert outcomes["completed"] >= 1
    assert outcomes["reaped_410"] >= 1

    # Final invariant: nothing was left in 'reaping'.
    conn = sqlite3.connect(str(_db_path(client)))
    try:
        stuck = conn.execute(
            "SELECT COUNT(*) FROM sessions WHERE status='reaping'"
        ).fetchone()[0]
        dangling_chunks = conn.execute(
            "SELECT COUNT(*) FROM chunks c WHERE c.session_id IN"
            " (SELECT id FROM reaped_sessions)"
        ).fetchone()[0]
    finally:
        conn.close()
    assert stuck == 0
    assert dangling_chunks == 0
    for sid, code, digest in results:
        if code == 200:
            assert client.get(f"/v1/archives/{digest}/download").status_code == 200


def test_concurrent_reap_and_chunk_upload_only_one_wins(client, tmp_path):
    """A chunk upload committing head-to-head with the claim: never both win."""
    data = gen(60_000, b"race-chunk")
    chunk_size = 60_000

    def run(_):
        local = threading.local()
        worker = getattr(local, "client", None)
        if worker is None:
            worker = TestClient(client.app)
            local.client = worker
        s = create_session(worker, data, chunk_size)
        sid = s["session_id"]
        _age(worker, sid)

        barrier = threading.Barrier(2)

        def upload():
            barrier.wait()
            return upload_chunk(worker, sid, data, chunk_size, 0)

        def reap():
            barrier.wait()
            return worker.post("/v1/maintenance/reap-sessions", json={"limit": 100})

        with ThreadPoolExecutor(max_workers=2) as pool:
            f_up = pool.submit(upload)
            f_reap = pool.submit(reap)
            up = f_up.result()
            reaped = {r["session_id"] for r in f_reap.result().json()["reaped"]}

        assert up.status_code in (200, 410), up.text
        if up.status_code == 200:
            assert sid not in reaped  # upload refreshed/won; session lives
            status = client.get(f"/v1/sessions/{sid}").json()
            assert status["status"] == "active"
            assert status["received_chunks"] == 1
        else:
            assert sid in reaped  # reaper won
            assert client.get(f"/v1/sessions/{sid}").status_code == 410
            # The losing upload must not leave its file or directory behind.
            assert not (tmp_path / "data" / "chunks" / sid).exists()
        return sid, up.status_code

    with ThreadPoolExecutor(max_workers=1) as pool:
        results = list(pool.map(run, range(24)))

    codes = {code for _, code in results}
    assert codes <= {200, 410} and len(codes) == 2  # both outcomes were exercised

    conn = sqlite3.connect(str(_db_path(client)))
    try:
        assert conn.execute(
            "SELECT COUNT(*) FROM sessions WHERE status='reaping'"
        ).fetchone()[0] == 0
        # No chunk rows may reference a reaped (tombstoned) session.
        assert conn.execute(
            "SELECT COUNT(*) FROM chunks c JOIN reaped_sessions r"
            " ON r.id = c.session_id"
        ).fetchone()[0] == 0
    finally:
        conn.close()
    # No leftover chunk directories for reaped sessions anywhere on disk.
    conn = sqlite3.connect(str(_db_path(client)))
    try:
        reaped_ids = [r[0] for r in conn.execute("SELECT id FROM reaped_sessions")]
    finally:
        conn.close()
    for sid in reaped_ids:
        assert not (tmp_path / "data" / "chunks" / sid).exists()


def test_concurrent_reapers_never_double_reap(client):
    sids = []
    for i in range(20):
        s = create_session(client, gen(10_000, f"m{i}".encode()), 10_000)
        sids.append(s["session_id"])
        _age(client, s["session_id"])

    barrier = threading.Barrier(10)

    def run(_):
        local = threading.local()
        worker = getattr(local, "client", None)
        if worker is None:
            worker = TestClient(client.app)
            local.client = worker
        barrier.wait()
        return worker.post("/v1/maintenance/reap-sessions", json={"limit": 50}).json()

    with ThreadPoolExecutor(max_workers=10) as pool:
        bodies = list(pool.map(run, range(10)))

    seen: list[str] = []
    for body in bodies:
        seen.extend(item["session_id"] for item in body["reaped"])
    assert sorted(seen) == sorted(sids)  # every session exactly once
    conn = sqlite3.connect(str(_db_path(client)))
    try:
        assert conn.execute("SELECT COUNT(*) FROM reaped_sessions").fetchone()[0] == 20
        assert conn.execute(
            "SELECT COUNT(*) FROM sessions WHERE status='reaping'"
        ).fetchone()[0] == 0
    finally:
        conn.close()


def test_restart_resumes_interrupted_reap(client, tmp_path):
    """A crash between the claim and the finalize is finished on startup."""
    data = gen(100_000, b"crash")
    s = create_session(client, data, 50_000)
    sid = s["session_id"]
    upload_all(client, sid, data, 50_000)

    # Simulate an interrupted reap: row claimed ('reaping'), run unfinished,
    # but the chunk directory is still on disk.
    db = _db_path(client)
    _set_status(db, sid, "reaping")
    conn = sqlite3.connect(str(db))
    try:
        conn.execute(
            "INSERT INTO reap_runs (run_id, started_at, recovered) VALUES (?, ?, 0)",
            ("deadbeef" * 4, datetime.now(timezone.utc).isoformat()),
        )
        conn.commit()
    finally:
        conn.close()
    chunk_dir = tmp_path / "data" / "chunks" / sid
    assert chunk_dir.is_dir()
    # The in-flight app sees the claimed session as gone (410).
    assert client.get(f"/v1/sessions/{sid}").status_code == 410

    import os

    os.environ["DATA_DIR"] = str(tmp_path / "data")
    from app.config import get_settings

    get_settings.cache_clear()
    from app.main import app as app2

    with TestClient(app2) as restarted:
        # Startup recovery finalized the interrupted reap.
        assert not chunk_dir.exists()
        conn = sqlite3.connect(str(db))
        try:
            assert conn.execute(
                "SELECT COUNT(*) FROM sessions WHERE id=?", (sid,)
            ).fetchone()[0] == 0
            assert conn.execute(
                "SELECT COUNT(*) FROM chunks WHERE session_id=?", (sid,)
            ).fetchone()[0] == 0
            row = conn.execute(
                "SELECT received_chunks, released_bytes, recovered FROM reaped_sessions"
                " WHERE id=?",
                (sid,),
            ).fetchone()
        finally:
            conn.close()
        assert row == (2, 100_000, 1)
        assert restarted.get(f"/v1/sessions/{sid}").status_code == 410

        # The next reap is a no-op but reports the resumed count as zero now.
        body = restarted.post("/v1/maintenance/reap-sessions", json={"limit": 10}).json()
        assert body["reaped"] == []
        assert body["recovered"] is False
        assert body["recovered_count"] == 0

        # The interrupted original run was marked finished/recovered.
        conn = sqlite3.connect(str(db))
        try:
            run = conn.execute(
                "SELECT finished_at, recovered, session_count, released_bytes"
                " FROM reap_runs WHERE run_id=?",
                ("deadbeef" * 4,),
            ).fetchone()
        finally:
            conn.close()
        assert run[0] is not None and run[1] == 1


def test_reap_request_triggers_recovery_of_claimed_session(client, tmp_path):
    """Calling reap-sessions after a crash also resumes the stale claim."""
    data = gen(50_000, b"recover-on-call")
    s = create_session(client, data, 50_000)
    sid = s["session_id"]
    upload_all(client, sid, data, 50_000)
    _set_status(_db_path(client), sid, "reaping")
    chunk_dir = tmp_path / "data" / "chunks" / sid
    assert chunk_dir.is_dir()

    body = _reap(client)
    assert body["recovered"] is True
    assert body["recovered_count"] == 1
    assert body["recovered_bytes"] == 50_000
    assert body["reaped"] == []  # the stale session was recovered, not re-claimed
    assert not chunk_dir.exists()
    assert client.get(f"/v1/sessions/{sid}").status_code == 410


def test_idle_timeout_configurable_via_env(tmp_path, monkeypatch):
    monkeypatch.setenv("DATA_DIR", str(tmp_path / "data-env"))
    monkeypatch.setenv("SESSION_IDLE_TIMEOUT_SECONDS", "60")
    from app.config import get_settings

    get_settings.cache_clear()
    assert get_settings().session_idle_timeout_seconds == 60

    from fastapi.testclient import TestClient

    from app.main import app

    with TestClient(app) as local_client:
        s = create_session(local_client, gen(10_000, b"env"), 10_000)
        # 90 s old: stale under the 60 s custom threshold.
        _set_updated_at(
            get_settings().db_path,
            s["session_id"],
            datetime.now(timezone.utc) - timedelta(seconds=90),
        )
        body = local_client.post(
            "/v1/maintenance/reap-sessions", json={"limit": 10}
        ).json()
        assert body["idle_timeout_seconds"] == 60
        assert [r["session_id"] for r in body["reaped"]] == [s["session_id"]]
    get_settings.cache_clear()


def test_invalid_idle_timeout_env_rejected(tmp_path, monkeypatch):
    monkeypatch.setenv("DATA_DIR", str(tmp_path / "data-bad-env"))
    monkeypatch.setenv("SESSION_IDLE_TIMEOUT_SECONDS", "0")
    from app.config import get_settings

    get_settings.cache_clear()
    with pytest.raises(ValueError):
        get_settings()
    get_settings.cache_clear()


def test_init_db_migrates_legacy_database_without_reaping_status(tmp_path):
    """A database created before this feature migrates its sessions CHECK."""
    from app.db import connect, init_db

    db_path = tmp_path / "legacy" / "archive.db"
    db_path.parent.mkdir(parents=True)

    legacy = connect(db_path)
    try:
        legacy.executescript(
            """
            CREATE TABLE sessions (
                id TEXT PRIMARY KEY, file_name TEXT NOT NULL, total_size INTEGER NOT NULL,
                chunk_size INTEGER NOT NULL, total_chunks INTEGER NOT NULL, file_sha256 TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'active'
                  CHECK (status IN ('active','merging','completed')),
                archive_sha256 TEXT, last_error TEXT, created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL, completed_at TEXT);
            CREATE INDEX idx_sessions_status ON sessions(status);
            CREATE TABLE chunks (session_id TEXT NOT NULL
                REFERENCES sessions(id) ON DELETE CASCADE, chunk_index INTEGER NOT NULL,
                size INTEGER NOT NULL, sha256 TEXT NOT NULL, uploaded_at TEXT NOT NULL,
                PRIMARY KEY (session_id, chunk_index));
            CREATE TABLE archives (sha256 TEXT PRIMARY KEY, file_name TEXT NOT NULL,
                size INTEGER NOT NULL, ref_count INTEGER NOT NULL DEFAULT 0, created_at TEXT NOT NULL);
            CREATE TABLE shares (token_hash TEXT PRIMARY KEY, archive_sha256 TEXT NOT NULL
                REFERENCES archives(sha256) ON DELETE CASCADE, max_downloads INTEGER NOT NULL,
                downloads_used INTEGER NOT NULL DEFAULT 0, expires_at TEXT NOT NULL,
                revoked INTEGER NOT NULL DEFAULT 0, created_at TEXT NOT NULL);
            """
        )
        legacy.execute(
            "INSERT INTO sessions (id,file_name,total_size,chunk_size,total_chunks,"
            "file_sha256,status,created_at,updated_at)"
            " VALUES ('s1','f.bin',10,10,1,'a','active','t','t')"
        )
        legacy.execute("INSERT INTO chunks VALUES ('s1',0,10,'c','t')")
        legacy.commit()
    finally:
        legacy.close()

    init_db(db_path)  # must migrate in place, preserving rows

    conn = connect(db_path)
    try:
        # Data survived.
        assert conn.execute("SELECT status FROM sessions WHERE id='s1'").fetchone()[0] == "active"
        assert conn.execute("SELECT COUNT(*) FROM chunks").fetchone()[0] == 1
        # New status allowed; CHECK still enforced.
        conn.execute("BEGIN IMMEDIATE")
        conn.execute("UPDATE sessions SET status='reaping' WHERE id='s1'")
        conn.execute("COMMIT")
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                "INSERT INTO sessions (id,file_name,total_size,chunk_size,total_chunks,"
                "file_sha256,status,created_at,updated_at)"
                " VALUES ('s2','f',1,1,1,'b','bogus','t','t')"
            )
            conn.commit()
        # Cascade FK rebuilt correctly.
        conn.execute("DELETE FROM sessions WHERE id='s1'")
        conn.commit()
        assert conn.execute("SELECT COUNT(*) FROM chunks").fetchone()[0] == 0
        # New tables created.
        names = {
            r[0] for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
        assert {"reaped_sessions", "reap_runs"} <= names
    finally:
        conn.close()

    # Migration is idempotent: a second init must not rebuild/fail.
    init_db(db_path)
    conn = connect(db_path)
    try:
        assert conn.execute("SELECT COUNT(*) FROM sessions").fetchone()[0] == 0
    finally:
        conn.close()
