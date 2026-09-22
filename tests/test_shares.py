"""Tests for controlled shares: quota, expiry, revocation, concurrency, restart."""
from __future__ import annotations

import hashlib
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

from fastapi.testclient import TestClient

from .conftest import create_share, gen, publish_archive, sha256


def _db_path(client: TestClient):
    from app.config import get_settings

    return get_settings().db_path


def _set_expiry(db_path, token: str, when: datetime) -> None:
    """Poke the stored expiry directly (the server only keeps a token hash)."""
    token_hash = hashlib.sha256(token.encode()).hexdigest()
    conn = sqlite3.connect(str(db_path), timeout=30.0)
    try:
        conn.execute(
            "UPDATE shares SET expires_at=? WHERE token_hash=?",
            (when.isoformat(), token_hash),
        )
        conn.commit()
    finally:
        conn.close()


def test_create_share_returns_one_time_token_and_does_not_store_it(client):
    data = gen(100_000, b"share")
    sha = publish_archive(client, data)

    resp = client.post(
        f"/v1/archives/{sha}/shares",
        json={"expires_in": 3600, "max_downloads": 3},
    )
    assert resp.status_code == 201, resp.text
    body = resp.json()
    token = body["token"]
    assert token and body["token"] != sha
    assert body["remaining_downloads"] == 3
    assert body["max_downloads"] == 3
    assert body["archive_sha256"] == sha
    # expires_at is roughly one hour in the future
    expires = datetime.fromisoformat(body["expires_at"])
    assert timedelta(minutes=59) < expires - datetime.now(timezone.utc) < timedelta(minutes=61)

    # The raw token must never touch the database; only its hash.
    conn = sqlite3.connect(str(_db_path(client)))
    try:
        row = conn.execute("SELECT token_hash FROM shares").fetchone()
    finally:
        conn.close()
    assert row is not None
    assert row[0] == sha256(token.encode())
    assert token not in row[0]
    assert len(token) >= 40  # 32 bytes URL-safe


def test_create_share_for_missing_archive_404(client):
    missing = sha256(b"nothing")
    resp = client.post(
        f"/v1/archives/{missing}/shares",
        json={"expires_in": 60, "max_downloads": 1},
    )
    assert resp.status_code == 404
    assert resp.json()["detail"] == "archive not found"


def test_create_share_validates_parameters(client):
    data = gen(50_000, b"params")
    sha = publish_archive(client, data)
    url = f"/v1/archives/{sha}/shares"
    assert client.post(url, json={"expires_in": 0, "max_downloads": 1}).status_code == 422
    assert client.post(url, json={"expires_in": 60, "max_downloads": 0}).status_code == 422
    assert client.post(
        f"/v1/archives/{'z' * 64}/shares",
        json={"expires_in": 60, "max_downloads": 1},
    ).status_code == 400


def test_shared_download_streams_content_and_decrements_quota(client):
    data = gen(250_000, b"dl")
    sha = publish_archive(client, data)
    token = create_share(client, sha, max_downloads=2)["token"]

    first = client.get(f"/v1/shares/{token}/download")
    assert first.status_code == 200
    assert first.content == data
    assert first.headers["content-disposition"].endswith(".bin\"")
    assert first.headers["x-share-remaining-downloads"] == "1"

    status = client.get(f"/v1/shares/{token}").json()
    assert status["downloads_used"] == 1
    assert status["remaining_downloads"] == 1
    assert status["state"] == "active"

    second = client.get(f"/v1/shares/{token}/download")
    assert second.status_code == 200
    assert second.headers["x-share-remaining-downloads"] == "0"

    # Third attempt: exhausted, explicit error, counter not incremented.
    third = client.get(f"/v1/shares/{token}/download")
    assert third.status_code == 410
    assert "limit" in third.json()["detail"]
    assert client.get(f"/v1/shares/{token}").json()["downloads_used"] == 2


def test_expired_share_rejected_without_consuming_quota(client):
    data = gen(80_000, b"exp")
    sha = publish_archive(client, data)
    token = create_share(client, sha, max_downloads=5)["token"]

    _set_expiry(_db_path(client), token, datetime.now(timezone.utc) - timedelta(seconds=10))

    resp = client.get(f"/v1/shares/{token}/download")
    assert resp.status_code == 410
    assert "expired" in resp.json()["detail"]

    status = client.get(f"/v1/shares/{token}").json()
    assert status["state"] == "expired"
    assert status["downloads_used"] == 0  # failed validation never counts

    # Extend it back into the future: quota intact, download works.
    _set_expiry(_db_path(client), token, datetime.now(timezone.utc) + timedelta(hours=1))
    ok = client.get(f"/v1/shares/{token}/download")
    assert ok.status_code == 200
    assert ok.content == data


def test_unknown_and_garbage_token_404(client):
    assert client.get("/v1/shares/does-not-exist/download").status_code == 404
    assert client.delete("/v1/shares/does-not-exist").status_code == 404
    assert client.get("/v1/shares/does-not-exist").status_code == 404


def test_revoke_blocks_download_and_is_idempotent(client):
    data = gen(70_000, b"rev")
    sha = publish_archive(client, data)
    token = create_share(client, sha, max_downloads=3)["token"]

    first = client.delete(f"/v1/shares/{token}")
    assert first.status_code == 200
    assert first.json()["revoked"] is True

    again = client.delete(f"/v1/shares/{token}")
    assert again.status_code == 200
    assert again.json()["revoked"] is False  # already revoked -> idempotent

    blocked = client.get(f"/v1/shares/{token}/download")
    assert blocked.status_code == 410
    assert "revoked" in blocked.json()["detail"]

    status = client.get(f"/v1/shares/{token}").json()
    assert status["state"] == "revoked"
    assert status["downloads_used"] == 0  # no download was ever counted


def test_concurrent_downloads_respect_max_downloads(client):
    data = gen(300_000, b"race")
    sha = publish_archive(client, data)
    token = create_share(client, sha, max_downloads=7)["token"]

    import threading

    local = threading.local()

    def hit(_):
        # Each worker thread owns a client; shared clients are not safe across threads.
        worker = getattr(local, "client", None)
        if worker is None:
            worker = TestClient(client.app)
            local.client = worker
        resp = worker.get(f"/v1/shares/{token}/download")
        return resp.status_code, resp.content if resp.status_code == 200 else None

    with ThreadPoolExecutor(max_workers=16) as pool:
        results = list(pool.map(hit, range(40)))

    codes = [code for code, _ in results]
    assert codes.count(200) == 7
    assert codes.count(410) == 33
    for code, body in results:
        if code == 200:
            assert body == data

    status = client.get(f"/v1/shares/{token}").json()
    assert status["downloads_used"] == 7
    assert status["remaining_downloads"] == 0
    assert status["state"] == "exhausted"


def test_share_survives_restart_and_remaining_quota_persists(client, tmp_path):
    data = gen(180_000, b"restart")
    sha = publish_archive(client, data)
    token = create_share(client, sha, expires_in=3600, max_downloads=3)["token"]

    assert client.get(f"/v1/shares/{token}/download").status_code == 200

    # Simulate a process restart against the same DATA_DIR (fresh lifespan).
    import os

    os.environ["DATA_DIR"] = str(tmp_path / "data")
    from app.config import get_settings

    get_settings.cache_clear()
    from app.main import app as app2

    with TestClient(app2) as restarted:
        status = restarted.get(f"/v1/shares/{token}").json()
        assert status["downloads_used"] == 1
        assert status["remaining_downloads"] == 2
        assert status["state"] == "active"
        resp = restarted.get(f"/v1/shares/{token}/download")
        assert resp.status_code == 200
        assert resp.content == data

        # Revocation also persists.
        assert restarted.delete(f"/v1/shares/{token}").status_code == 200

    with TestClient(app2) as restarted_again:
        assert restarted_again.get(f"/v1/shares/{token}/download").status_code == 410
        assert restarted_again.get(f"/v1/shares/{token}").json()["state"] == "revoked"


def test_shares_table_created_on_startup_against_empty_db(tmp_path, monkeypatch):
    """A brand-new data dir gets the shares table via init_db (schema migration)."""
    from app.config import ensure_dirs, get_settings
    from app.db import connect, init_db

    monkeypatch.setenv("DATA_DIR", str(tmp_path / "fresh"))
    get_settings.cache_clear()
    settings = get_settings()
    ensure_dirs(settings)  # same order as the app lifespan
    init_db(settings.db_path)
    conn = connect(settings.db_path)
    try:
        names = {
            r[0]
            for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
    finally:
        conn.close()
    assert {"sessions", "chunks", "archives", "shares"} <= names


def test_deleting_archive_cascades_to_shares(client):
    data = gen(140_000, b"cascade")
    sha = publish_archive(client, data)
    # publish_archive leaves one completed session referencing the archive.
    token = create_share(client, sha)["token"]
    assert client.get(f"/v1/shares/{token}").status_code == 200

    # Releasing the last (only) archive reference removes the archive row;
    # the share must follow it (ON DELETE CASCADE) and now read as not found.
    conn = sqlite3.connect(str(_db_path(client)))
    try:
        sid = conn.execute(
            "SELECT id FROM sessions WHERE archive_sha256=?", (sha,)
        ).fetchone()[0]
    finally:
        conn.close()
    resp = client.delete(f"/v1/sessions/{sid}")
    assert resp.status_code == 200
    assert resp.json()["archive_deleted"] is True

    assert client.get(f"/v1/shares/{token}/download").status_code == 404
    assert client.get(f"/v1/shares/{token}").status_code == 404
