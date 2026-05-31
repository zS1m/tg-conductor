"""§3.2a — round-trip a Pydantic discriminated union through ``PydanticJSONType``.

Verifies:
* Writing a tagged model dumps to JSON via the bound TypeAdapter.
* Reading restores the concrete subclass, not a raw dict.
* An unknown discriminator tag in stored JSON raises ``ValidationError`` on
  read (not silent dict fallback).
"""

from __future__ import annotations

from typing import Annotated, Literal, Union

import pytest
from pydantic import BaseModel, Field, TypeAdapter, ValidationError
from sqlalchemy import Column, Integer, MetaData, Table, insert, select, update
from sqlalchemy.ext.asyncio import create_async_engine

from tg_conductor.db.types import PydanticJSONType


class _CronTrigger(BaseModel):
    type: Literal["cron"]
    expr: str


class _StartupTrigger(BaseModel):
    type: Literal["startup"]


_Trigger = Annotated[
    Union[_CronTrigger, _StartupTrigger],
    Field(discriminator="type"),
]
_TriggerAdapter: TypeAdapter[_Trigger] = TypeAdapter(_Trigger)


@pytest.fixture
def trigger_table() -> Table:
    md = MetaData()
    return Table(
        "trigger_test",
        md,
        Column("id", Integer, primary_key=True),
        Column("trig", PydanticJSONType(_TriggerAdapter), nullable=False),
    )


@pytest.mark.asyncio
async def test_round_trip_preserves_subclass(
    tmp_sqlite_url: str, trigger_table: Table
) -> None:
    engine = create_async_engine(tmp_sqlite_url, future=True)
    try:
        async with engine.begin() as conn:
            await conn.run_sync(trigger_table.metadata.create_all)
            await conn.execute(
                insert(trigger_table),
                [{"id": 1, "trig": _CronTrigger(type="cron", expr="* * * * *")}],
            )

        async with engine.connect() as conn:
            row = (await conn.execute(select(trigger_table))).one()
        assert isinstance(row.trig, _CronTrigger)
        assert row.trig.expr == "* * * * *"
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_unknown_discriminator_raises_on_read(
    tmp_sqlite_url: str, trigger_table: Table
) -> None:
    engine = create_async_engine(tmp_sqlite_url, future=True)
    try:
        async with engine.begin() as conn:
            await conn.run_sync(trigger_table.metadata.create_all)
            await conn.execute(
                insert(trigger_table),
                [{"id": 1, "trig": _StartupTrigger(type="startup")}],
            )
            # Smuggle in an unknown tag bypassing the TypeAdapter.
            await conn.execute(
                update(trigger_table)
                .where(trigger_table.c.id == 1)
                .values(trig={"type": "wat", "anything": 1}),
            )

        with pytest.raises(ValidationError):
            async with engine.connect() as conn:
                (await conn.execute(select(trigger_table))).one()
    finally:
        await engine.dispose()
