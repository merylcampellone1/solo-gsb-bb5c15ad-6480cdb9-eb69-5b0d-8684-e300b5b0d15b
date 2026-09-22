"""Runtime configuration, sourced from environment variables."""
from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

_DEFAULT_IDLE_TIMEOUT_SECONDS = 3600  # reap active sessions idle for one hour


@dataclass(frozen=True)
class Settings:
    data_dir: Path
    db_path: Path
    chunk_dir: Path
    object_dir: Path
    tmp_dir: Path
    session_idle_timeout_seconds: int


def _idle_timeout_from_env() -> int:
    raw = os.environ.get("SESSION_IDLE_TIMEOUT_SECONDS")
    if raw is None or raw == "":
        return _DEFAULT_IDLE_TIMEOUT_SECONDS
    try:
        value = int(raw)
    except ValueError as exc:
        raise ValueError("SESSION_IDLE_TIMEOUT_SECONDS must be an integer number of seconds") from exc
    if value < 1:
        raise ValueError("SESSION_IDLE_TIMEOUT_SECONDS must be >= 1")
    return value


@lru_cache
def get_settings() -> Settings:
    """Build settings from the environment (cached; clear cache in tests)."""
    data_dir = Path(os.environ.get("DATA_DIR", "./data")).resolve()
    return Settings(
        data_dir=data_dir,
        db_path=Path(os.environ.get("DB_PATH", str(data_dir / "archive.db"))),
        chunk_dir=data_dir / "chunks",
        object_dir=data_dir / "objects",
        tmp_dir=data_dir / "tmp",
        session_idle_timeout_seconds=_idle_timeout_from_env(),
    )


def ensure_dirs(settings: Settings) -> None:
    for path in (settings.data_dir, settings.chunk_dir, settings.object_dir, settings.tmp_dir):
        path.mkdir(parents=True, exist_ok=True)
