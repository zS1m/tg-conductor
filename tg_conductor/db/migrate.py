"""Programmatic Alembic runner used by the CLI and tests."""

from __future__ import annotations

import os
from pathlib import Path

from alembic import command
from alembic.config import Config


def _find_alembic_root() -> Path:
    """Locate the directory holding ``alembic.ini`` + ``alembic_migrations``.

    Historically this was ``Path(__file__).parents[2]`` — correct only when the
    package is imported from a source checkout (or an *editable* install, whose
    ``.pth`` points back at the checkout). Once the project is installed
    non-editable (e.g. baked into the Docker image's ``site-packages``), that
    path lands inside ``site-packages`` where no migration assets exist. So we
    probe candidate roots and pick the first that actually has ``alembic.ini``:

    1. ``$TG_CONDUCTOR_ALEMBIC_DIR`` — explicit override.
    2. ``parents[2]`` — source checkout / editable install (dev + tests).
    3. CWD — the container WORKDIR (``/app``), where the Dockerfile copies
       both ``alembic.ini`` and ``alembic_migrations``.
    """
    candidates: list[Path] = []
    env_dir = os.environ.get("TG_CONDUCTOR_ALEMBIC_DIR")
    if env_dir:
        candidates.append(Path(env_dir))
    candidates.append(Path(__file__).resolve().parents[2])
    candidates.append(Path.cwd())

    for root in candidates:
        if (root / "alembic.ini").is_file() and (root / "alembic_migrations").is_dir():
            return root

    raise RuntimeError(
        "Could not locate alembic.ini + alembic_migrations. Looked in: "
        + ", ".join(str(c) for c in candidates)
        + ". Set TG_CONDUCTOR_ALEMBIC_DIR to the directory containing them."
    )


def _build_config(database_url: str | None) -> Config:
    root = _find_alembic_root()
    cfg = Config(str(root / "alembic.ini"))
    cfg.set_main_option("script_location", str(root / "alembic_migrations"))
    if database_url is not None:
        cfg.set_main_option("sqlalchemy.url", database_url)
    return cfg


def _ensure_sqlite_parent_dir(database_url: str) -> None:
    prefix = "sqlite+aiosqlite:///"
    if database_url.startswith(prefix):
        Path(database_url[len(prefix) :]).parent.mkdir(parents=True, exist_ok=True)


def upgrade_head(database_url: str | None = None) -> None:
    """Run ``alembic upgrade head`` against the given URL (or settings default)."""
    if database_url is None:
        from tg_conductor.config.settings import get_settings

        database_url = get_settings().database_url
    _ensure_sqlite_parent_dir(database_url)
    cfg = _build_config(database_url)
    command.upgrade(cfg, "head")
