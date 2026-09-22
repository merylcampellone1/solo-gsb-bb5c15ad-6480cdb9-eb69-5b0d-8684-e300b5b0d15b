"""End-to-end tests for the archive API (each test runs on a fresh data dir)."""
from __future__ import annotations

from .conftest import (
    create_session,
    gen as _gen,
    publish_archive,
    sha256,
    upload_all,
    upload_chunk,
)


def test_full_flow_out_of_order_with_duplicate(client):
    data = _gen(1_000_003, b"flow")  # not a multiple of the chunk size
    chunk_size = 256 * 1024
    session = create_session(client, data, chunk_size)
    sid = session["session_id"]
    assert session["status"] == "active"
    assert session["instant"] is False

    # Out-of-order upload plus one duplicate re-upload.
    upload_all(client, sid, data, chunk_size, order=[2, 0, 3, 1])
    dup = upload_chunk(client, sid, data, chunk_size, 0)
    assert dup.status_code == 200
    assert dup.json()["duplicate"] is True

    status = client.get(f"/v1/sessions/{sid}").json()
    assert status["received_chunks"] == 4
    assert status["missing_chunks"] == []

    done = client.post(f"/v1/sessions/{sid}/complete")
    assert done.status_code == 200, done.text
    body = done.json()
    assert body["archive_sha256"] == sha256(data)
    assert body["size"] == len(data)
    assert body["deduplicated"] is False

    # Idempotent complete retry.
    again = client.post(f"/v1/sessions/{sid}/complete")
    assert again.status_code == 200

    download = client.get(f"/v1/archives/{sha256(data)}/download")
    assert download.status_code == 200
    assert download.content == data


def test_resume_query_reports_missing_chunks(client):
    data = _gen(300_000, b"resume")
    chunk_size = 100_000
    session = create_session(client, data, chunk_size)
    sid = session["session_id"]

    upload_chunk(client, sid, data, chunk_size, 1)
    status = client.get(f"/v1/sessions/{sid}").json()
    assert status["uploaded_chunks"] == [1]
    assert status["missing_chunks"] == [0, 2]

    # Completing with missing chunks is rejected and lists what is missing.
    resp = client.post(f"/v1/sessions/{sid}/complete")
    assert resp.status_code == 409
    assert resp.json()["detail"]["missing_chunks"] == [0, 2]


def test_chunk_hash_mismatch_rejected(client):
    data = _gen(100_000, b"badhash")
    session = create_session(client, data, 50_000)
    sid = session["session_id"]

    resp = client.put(
        f"/v1/sessions/{sid}/chunks/0",
        content=data[:50_000],
        headers={"X-Chunk-SHA256": sha256(b"not-the-right-data")},
    )
    assert resp.status_code == 422
    assert client.get(f"/v1/sessions/{sid}").json()["received_chunks"] == 0


def test_chunk_size_mismatch_rejected(client):
    data = _gen(100_000, b"badsize")
    session = create_session(client, data, 50_000)
    sid = session["session_id"]
    piece = data[:40_000]  # too short for chunk 0
    resp = client.put(
        f"/v1/sessions/{sid}/chunks/0",
        content=piece,
        headers={"X-Chunk-SHA256": sha256(piece)},
    )
    assert resp.status_code == 400


def test_chunk_index_out_of_range(client):
    data = _gen(100_000, b"range")
    session = create_session(client, data, 50_000)
    sid = session["session_id"]
    piece = b"x" * 10
    resp = client.put(
        f"/v1/sessions/{sid}/chunks/2",
        content=piece,
        headers={"X-Chunk-SHA256": sha256(piece)},
    )
    assert resp.status_code == 400


def test_whole_file_hash_mismatch_keeps_session_active(client):
    data = _gen(120_000, b"whoops")
    chunk_size = 60_000
    wrong_sha = sha256(b"different content")
    resp = client.post(
        "/v1/sessions",
        json={
            "file_name": "wrong.bin",
            "total_size": len(data),
            "chunk_size": chunk_size,
            "total_chunks": 2,
            "file_sha256": wrong_sha,
        },
    )
    sid = resp.json()["session_id"]
    upload_all(client, sid, data, chunk_size)

    done = client.post(f"/v1/sessions/{sid}/complete")
    assert done.status_code == 422
    detail = done.json()["detail"]
    assert detail["expected"] == wrong_sha
    assert detail["actual"] == sha256(data)

    status = client.get(f"/v1/sessions/{sid}").json()
    assert status["status"] == "active"  # client may fix and retry
    assert "hash mismatch" in status["last_error"]


def test_dedup_instant_session_and_refcount(client):
    data = _gen(200_000, b"dedup")
    chunk_size = 100_000

    first = create_session(client, data, chunk_size, name="first.bin")
    upload_all(client, first["session_id"], data, chunk_size)
    assert client.post(f"/v1/sessions/{first['session_id']}/complete").status_code == 200

    # Second upload of identical content completes instantly, no chunks needed.
    second = create_session(client, data, chunk_size, name="second.bin")
    assert second["instant"] is True
    assert second["status"] == "completed"

    archives = client.get("/v1/archives").json()
    assert len(archives) == 1
    assert archives[0]["ref_count"] == 2

    # Releasing one session keeps the object alive.
    resp = client.delete(f"/v1/sessions/{first['session_id']}")
    assert resp.json()["archive_deleted"] is False
    assert client.get(f"/v1/archives/{sha256(data)}").json()["ref_count"] == 1
    assert client.get(f"/v1/archives/{sha256(data)}/download").content == data

    # Releasing the last reference deletes the object.
    resp = client.delete(f"/v1/sessions/{second['session_id']}")
    assert resp.json()["archive_deleted"] is True
    assert client.get(f"/v1/archives/{sha256(data)}").status_code == 404


def test_delete_active_session_removes_staged_chunks(client, tmp_path):
    data = _gen(150_000, b"cleanup")
    chunk_size = 50_000
    session = create_session(client, data, chunk_size)
    sid = session["session_id"]
    upload_chunk(client, sid, data, chunk_size, 0)

    chunk_dir = tmp_path / "data" / "chunks" / sid
    assert chunk_dir.is_dir()

    resp = client.delete(f"/v1/sessions/{sid}")
    assert resp.status_code == 200
    assert resp.json()["archive_released"] is False
    assert not chunk_dir.exists()
    assert client.get(f"/v1/sessions/{sid}").status_code == 404


def test_session_validation_rejects_bad_chunk_math(client):
    resp = client.post(
        "/v1/sessions",
        json={
            "file_name": "x.bin",
            "total_size": 1000,
            "chunk_size": 300,
            "total_chunks": 3,  # ceil(1000/300) == 4
            "file_sha256": sha256(b"x"),
        },
    )
    assert resp.status_code == 422


def test_unknown_session_404(client):
    assert client.get("/v1/sessions/nope").status_code == 404
    assert client.post("/v1/sessions/nope/complete").status_code == 404
    assert client.delete("/v1/sessions/nope").status_code == 404


def test_legacy_download_endpoint_still_served(client):
    """The original unconstrained download endpoint remains available."""
    data = _gen(120_000, b"legacy")
    sha = publish_archive(client, data)
    first = client.get(f"/v1/archives/{sha}/download")
    assert first.status_code == 200
    assert first.content == data
    # No quota: repeatable forever.
    assert client.get(f"/v1/archives/{sha}/download").content == data
