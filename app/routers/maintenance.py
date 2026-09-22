"""Maintenance endpoints: idle session reaping."""
from __future__ import annotations

from fastapi import APIRouter

from ..config import get_settings
from ..schemas import ReapSessionsRequest, ReapSessionsResponse, ReapedSession
from ..service import reap_idle_sessions

router = APIRouter()


@router.post("/reap-sessions", response_model=ReapSessionsResponse)
def reap_sessions(payload: ReapSessionsRequest) -> ReapSessionsResponse:
    """Reclaim stale abandoned uploads.

    Reaps up to ``limit`` sessions that are still ``active`` but have been idle
    (no creation or chunk activity) longer than ``SESSION_IDLE_TIMEOUT_SECONDS``.
    Each session is first claimed atomically by an ``active -> reaping`` state
    transition, so a concurrent chunk upload or ``complete`` can never succeed
    alongside the reaper; requests for a reaped session afterwards get 410.
    If a previous process crashed mid-reap, its claimed sessions are finished
    first and reported as ``recovered``.
    """
    settings = get_settings()
    run_id, reaped, recovered = reap_idle_sessions(settings, payload.limit)
    return ReapSessionsResponse(
        run_id=run_id,
        idle_timeout_seconds=settings.session_idle_timeout_seconds,
        limit=payload.limit,
        reaped=[
            ReapedSession(session_id=sid, received_chunks=received, released_bytes=released)
            for sid, received, released in reaped
        ],
        reaped_count=len(reaped),
        released_bytes=sum(item[2] for item in reaped),
        recovered=bool(recovered),
        recovered_count=len(recovered),
        recovered_bytes=sum(item[2] for item in recovered),
    )
