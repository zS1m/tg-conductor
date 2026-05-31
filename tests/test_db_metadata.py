"""Regression: importing ``tg_conductor.db`` registers every table.

A real user hit ``NoReferencedTableError: ... could not find table
'owners'`` during ``tg-conductor account login`` because the CLI's
import graph reached ``Account`` (FK → ``owners.id``) without ever
touching the ``Owner`` SQLModel. The fix lives in
``tg_conductor/db/__init__.py``, which side-effect-imports every
``table=True`` module so any entry point that uses the DB layer gets
the full metadata graph for free.

This test fails if a future ``table=True`` SQLModel is added without
also being listed in ``db/__init__.py``.
"""

from __future__ import annotations

import importlib

from sqlmodel import SQLModel

# spec: every owner_id FK in a business table points to ``owners.id`` —
# we use that as the canary because it's the most common cross-table
# reference and the failure mode the user actually observed.
_EXPECTED_TABLES = {
    "owners",
    "accounts",
    "workflows",
    "jobs",
    "runs",
    "run_events",
    "usage_events",
}


def test_importing_db_package_registers_every_table() -> None:
    # Re-import so we observe the package's side effects (in case an
    # earlier test already had its way with ``SQLModel.metadata``).
    importlib.import_module("tg_conductor.db")
    present = set(SQLModel.metadata.tables)
    missing = _EXPECTED_TABLES - present
    assert not missing, (
        f"these tables are not registered after importing tg_conductor.db: "
        f"{sorted(missing)}; add the model module to db/__init__.py"
    )


def test_account_owner_id_fk_resolves_to_owners_table() -> None:
    """The exact resolution path that ``CLI account login`` exercises."""
    importlib.import_module("tg_conductor.db")
    accounts = SQLModel.metadata.tables["accounts"]
    owner_id_col = accounts.c.owner_id
    assert owner_id_col.foreign_keys, "accounts.owner_id has no FK"
    fk = next(iter(owner_id_col.foreign_keys))
    # ``fk.column`` raises NoReferencedTableError if the target table is
    # missing — that's precisely the production error we're guarding.
    target = fk.column
    assert target.table.name == "owners"
    assert target.name == "id"
