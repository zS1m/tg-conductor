# syntax=docker/dockerfile:1.7
#
# Multi-stage build for tg-conductor.
#
# Stage 1 (builder) — uses the official uv image to resolve + install the
# project into a private virtualenv. We copy ``pyproject.toml`` and
# ``uv.lock`` first so dependency installation caches across source-only
# code changes.
#
# Stage 2 (runtime) — python:3.13-slim with the venv copied in and the
# source tree on top. We do NOT install uv at runtime: ``uv run`` is a
# build-time convenience; production uses the venv's interpreter directly.
#
# Runs as a non-root user (uid 1000) per spec §18.1.
# Entrypoint runs ``alembic upgrade head`` then ``tg-conductor serve``
# (spec §18.2 — single-container deploy; ops can split into an init
# container by overriding the entrypoint).

# --------------------------------------------------------------- builder
FROM ghcr.io/astral-sh/uv:python3.13-bookworm-slim AS builder

ENV UV_LINK_MODE=copy \
    UV_COMPILE_BYTECODE=1 \
    UV_PROJECT_ENVIRONMENT=/opt/venv \
    UV_PYTHON_DOWNLOADS=never

WORKDIR /build

# tgcrypto (Kurigram 的 AES 加速扩展) 在 PyPI 只发到 cp311 的 wheel，
# Python 3.13 下没有预编译轮子，uv 必须从 sdist 现编。slim 镜像不带
# 编译器，所以这里装 build-essential。runtime 阶段只 copy 编译好的 venv，
# 不继承这层，镜像保持精简。
RUN apt-get update \
 && apt-get install -y --no-install-recommends build-essential \
 && rm -rf /var/lib/apt/lists/*

# Install deps first (cache-friendly layer).
COPY pyproject.toml uv.lock README.md ./
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-install-project --no-dev

# Now the source tree, then install the project itself into the venv.
# ``--no-editable``: by default ``uv sync`` installs the project editable —
# a ``.pth`` in the venv pointing back at this build-time source dir
# (``/build``). The runtime stage copies the venv to a fresh image where
# ``/build`` does not exist, so that link dangles and ``import tg_conductor``
# fails with ModuleNotFoundError even though the console script is present.
# ``--no-editable`` bakes the package into site-packages, which travels with
# the copied venv.
COPY tg_conductor ./tg_conductor
COPY alembic.ini ./
COPY alembic_migrations ./alembic_migrations
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-editable

# --------------------------------------------------------------- runtime
FROM python:3.13-slim-bookworm AS runtime

# System libs Kurigram + cryptography need at runtime. ``libssl`` ships
# with the base image; we only need libffi for cffi-backed wheels in
# some arch combinations.
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
        libffi8 \
 && rm -rf /var/lib/apt/lists/* \
 && groupadd --system --gid 1000 app \
 && useradd  --system --uid 1000 --gid app --home-dir /app --shell /bin/bash app \
 && mkdir -p /app/data /app/workflows \
 && chown -R app:app /app

ENV PATH="/opt/venv/bin:$PATH" \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    DATABASE_URL="sqlite+aiosqlite:///data/tg-conductor.sqlite3" \
    WORKFLOW_DIR="/app/workflows" \
    BIND_HOST="0.0.0.0" \
    BIND_PORT="8765"

WORKDIR /app

# Bring the resolved venv + source from the builder.
COPY --from=builder /opt/venv /opt/venv
COPY --from=builder /build/tg_conductor /app/tg_conductor
COPY --from=builder /build/alembic.ini /app/alembic.ini
COPY --from=builder /build/alembic_migrations /app/alembic_migrations
COPY pricing.yaml.example /app/pricing.yaml.example

USER app

EXPOSE 8765

# Run migrations then start the service. The lifespan inside
# ``tg-conductor serve`` also calls ``upgrade_head``; running it here
# is belt-and-suspenders so a misconfigured runtime fails fast before
# the HTTP listener binds.
ENTRYPOINT ["sh", "-c", "tg-conductor migrate && exec tg-conductor serve"]
