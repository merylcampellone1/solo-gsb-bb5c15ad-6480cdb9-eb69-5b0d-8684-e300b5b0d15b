"""Maintenance endpoints: reclaim abandoned (idle) upload sessions."""
from __future__ import annotations

from fastapi import APIRouter

from ..config import get_settings
from ..schemas import ReapSessionsRequest, ReapSessionsResponse
from ..service import reap_idle_sessions

router = APIRouter()


@router.post("/reap-sessions", response_model=ReapSessionsResponse)
def reap_sessions(payload: ReapSessionsRequest) -> ReapSessionsResponse:
    """Reclaim active sessions idle longer than the configured threshold.

    Each candidate is claimed atomically (active -> reaping) before its row and
    staged chunk directory are deleted, so the reaper can never win against a
    concurrent chunk upload or ``complete``: exactly one side proceeds, and
    requests touching an already-claimed session get ``410 Gone``. Completed
    sessions are never reaped.
    """
    settings = get_settings()
    reaped_ids, released_bytes = reap_idle_sessions(settings, payload.limit)
    return ReapSessionsResponse(
        limit=payload.limit,
        reaped=len(reaped_ids),
        reaped_ids=reaped_ids,
        released_bytes=released_bytes,
    )
