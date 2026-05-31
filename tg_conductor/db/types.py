"""SQLAlchemy types for Pydantic v2 interop (see design.md D13).

`PydanticJSONType` lets a SQLModel column hold a Pydantic model (including
discriminated unions). On INSERT/UPDATE the value is dumped via the supplied
``TypeAdapter`` in JSON mode; on SELECT it is round-tripped back through the
adapter, so an unknown discriminator tag raises ``ValidationError`` instead of
silently returning a raw ``dict``.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from pydantic import TypeAdapter
from sqlalchemy import JSON, DateTime
from sqlalchemy.engine import Dialect
from sqlalchemy.types import TypeDecorator


class UtcDateTime(TypeDecorator):
    """``DateTime(timezone=True)`` that always returns UTC-aware datetimes.

    aiosqlite + SQLite store ISO text and Python's default converter may
    return naive ``datetime`` even when the original was aware (especially
    for ``func.max``-aggregated values). This decorator normalizes on the
    way in (require tz-aware, convert to UTC) and on the way out (attach
    UTC if naive, else convert to UTC). Eliminates "can't compare offset-
    naive and offset-aware" downstream.
    """

    impl = DateTime
    cache_ok = True

    def __init__(self, **kw: Any) -> None:
        super().__init__(timezone=True, **kw)

    def process_bind_param(
        self, value: datetime | None, dialect: Dialect
    ) -> datetime | None:
        if value is None:
            return None
        if value.tzinfo is None:
            raise ValueError(
                "UtcDateTime requires tz-aware datetime; got naive {value!r}"
            )
        return value.astimezone(UTC)

    def process_result_value(
        self, value: datetime | None, dialect: Dialect
    ) -> datetime | None:
        if value is None:
            return None
        if value.tzinfo is None:
            return value.replace(tzinfo=UTC)
        return value.astimezone(UTC)


class PydanticJSONType(TypeDecorator):
    impl = JSON
    cache_ok = True

    def __init__(self, adapter: TypeAdapter, **kw: Any) -> None:
        super().__init__(**kw)
        self._adapter = adapter

    def process_bind_param(self, value: Any, dialect: Dialect) -> Any:
        if value is None:
            return None
        return self._adapter.dump_python(value, mode="json")

    def process_result_value(self, value: Any, dialect: Dialect) -> Any:
        if value is None:
            return None
        return self._adapter.validate_python(value)
