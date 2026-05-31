"""DB package — importing this side-effect-registers every SQLModel table.

SQLAlchemy resolves foreign keys by *name* at flush time. If a process
boots into a code path that only imports e.g. ``Account`` (which has
``owner_id`` FK → ``owners.id``) but never imports ``Owner``, the
``owners`` table is missing from ``SQLModel.metadata`` and the next
``session.flush()`` raises::

    NoReferencedTableError: Foreign key associated with column
    'accounts.owner_id' could not find table 'owners' ...

Alembic dodges this by eagerly importing every model module in its
``env.py``. CLI commands (``account login`` / ``account list`` /
``migrate``) and the lifespan entrypoint need the same. Instead of
hand-listing imports at every entrypoint, we list them here once —
any caller doing ``from tg_conductor.db.engine import ...`` triggers
this package's ``__init__`` and the whole graph is registered.

Order doesn't matter for FK resolution; we keep it alphabetical for
diff stability.
"""

from __future__ import annotations

# Importing these modules is a side effect: each ``SQLModel`` subclass
# with ``table=True`` self-registers on ``SQLModel.metadata`` when its
# class body executes. ``noqa: F401`` keeps ruff quiet about the
# "unused" imports.
from tg_conductor.accounts import models as _accounts_models  # noqa: F401
from tg_conductor.ai import usage as _ai_usage_models  # noqa: F401
from tg_conductor.db import models as _db_models  # noqa: F401
from tg_conductor.runs import models as _runs_models  # noqa: F401
from tg_conductor.scheduler import models as _scheduler_models  # noqa: F401
from tg_conductor.workflows import models as _workflows_models  # noqa: F401
