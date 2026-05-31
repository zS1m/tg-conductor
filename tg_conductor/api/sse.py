"""SSE wire-format helpers and history-row → bus-message converter.

We roll our own SSE framing rather than pull ``sse-starlette`` because the
contract is tiny: one ``data:`` line + a blank line per event, plus
``: keepalive`` comments for idle traffic. The event id (used as the
``id:`` line so a polite client can populate ``Last-Event-ID``) is the
``run_events.id`` primary key.
"""

from __future__ import annotations

import json
from typing import Any

from tg_conductor.runs.models import RunEventRow


def row_to_message(row: RunEventRow) -> dict[str, Any]:
    """Convert a DB row to the same dict shape used on the event bus.

    Keeps REST history and SSE live frames structurally identical so
    clients can deduplicate by ``seq`` across both.
    """
    assert row.id is not None
    return {
        "id": row.id,
        "owner_id": row.owner_id,
        "run_id": row.run_id,
        "seq": row.seq,
        "type": row.type,
        "message": row.message,
        "level": row.level,
        "attrs": row.attrs,
        "ts": row.ts.isoformat(),
    }


def format_event(msg: dict[str, Any]) -> bytes:
    """Encode one event as an SSE frame.

    ``id:`` carries the event-row primary key; ``data:`` carries the
    full JSON payload (clients tend to want fields beyond ``type``).
    """
    return (
        f"id: {msg['id']}\n"
        f"event: {msg['type']}\n"
        f"data: {json.dumps(msg, ensure_ascii=False)}\n\n"
    ).encode("utf-8")


KEEPALIVE_FRAME: bytes = b": keepalive\n\n"
