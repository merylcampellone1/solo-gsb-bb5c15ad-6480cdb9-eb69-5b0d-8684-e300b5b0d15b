"""Shared pytest fixtures/helpers (each test runs on a fresh data dir)."""
from __future__ import annotations

import hashlib

import pytest
from fastapi.testclient import TestClient


@pytest.fixture()
def client(tmp_path, monkeypatch):
    monkeypatch.setenv("DATA_DIR", str(tmp_path / "data"))
    from app.config import get_settings

    get_settings.cache_clear()
    from app.main import app

    with TestClient(app) as test_client:
        yield test_client
    get_settings.cache_clear()


def gen(size: int, seed: bytes) -> bytes:
    """Deterministic pseudo-random content."""
    out = b""
    counter = 0
    while len(out) < size:
        out += hashlib.sha256(seed + counter.to_bytes(4, "big")).digest()
        counter += 1
    return out[:size]


def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def create_session(client: TestClient, data: bytes, chunk_size: int, name: str = "blob.bin"):
    total = len(data)
    total_chunks = -(-total // chunk_size)
    resp = client.post(
        "/v1/sessions",
        json={
            "file_name": name,
            "total_size": total,
            "chunk_size": chunk_size,
            "total_chunks": total_chunks,
            "file_sha256": sha256(data),
        },
    )
    assert resp.status_code == 201, resp.text
    return resp.json()


def upload_chunk(client: TestClient, session_id: str, data: bytes, chunk_size: int, index: int):
    piece = data[index * chunk_size : (index + 1) * chunk_size]
    return client.put(
        f"/v1/sessions/{session_id}/chunks/{index}",
        content=piece,
        headers={"X-Chunk-SHA256": sha256(piece)},
    )


def upload_all(client: TestClient, session_id: str, data: bytes, chunk_size: int, order=None):
    total_chunks = -(-len(data) // chunk_size)
    for index in (order if order is not None else range(total_chunks)):
        resp = upload_chunk(client, session_id, data, chunk_size, index)
        assert resp.status_code == 200, resp.text


def publish_archive(client: TestClient, data: bytes, chunk_size: int = 100_000, name: str = "blob.bin") -> str:
    """Run the full upload flow and return the archive sha256."""
    session = create_session(client, data, chunk_size, name=name)
    upload_all(client, session["session_id"], data, chunk_size)
    resp = client.post(f"/v1/sessions/{session['session_id']}/complete")
    assert resp.status_code == 200, resp.text
    return resp.json()["archive_sha256"]


def create_share(client: TestClient, sha: str, expires_in: int = 3600, max_downloads: int = 1):
    resp = client.post(
        f"/v1/archives/{sha}/shares",
        json={"expires_in": expires_in, "max_downloads": max_downloads},
    )
    assert resp.status_code == 201, resp.text
    return resp.json()
