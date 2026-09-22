"""Filesystem layout for chunks, merged objects and temp files.

    <DATA_DIR>/
      archive.db                  SQLite database
      chunks/<session>/<idx>.part per-session staging area for uploaded chunks
      objects/<aa>/<sha256>       content-addressed, deduplicated archives
      tmp/<uuid>.tmp              merge/upload temp files (same volume -> atomic rename)
"""
from __future__ import annotations

import uuid
from pathlib import Path

from .config import Settings

_CHUNK_NAME = "{index:08d}.part"


def chunk_dir_for(settings: Settings, session_id: str) -> Path:
    return settings.chunk_dir / session_id


def chunk_path(settings: Settings, session_id: str, index: int) -> Path:
    return chunk_dir_for(settings, session_id) / _CHUNK_NAME.format(index=index)


def object_path(settings: Settings, sha256: str) -> Path:
    return settings.object_dir / sha256[:2] / sha256


def new_tmp_path(settings: Settings) -> Path:
    return settings.tmp_dir / f"{uuid.uuid4().hex}.tmp"
