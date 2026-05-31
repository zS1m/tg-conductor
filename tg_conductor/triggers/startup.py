"""Process-lifetime tracker for ``startup``-type triggers.

Spec invariant: a ``startup`` trigger fires *exactly once per process*;
``reload`` / SIGHUP do not re-fire it (config reloader 8.7 calls
:meth:`Reloader.reload` — that path must not consume from this tracker).

The tracker is shared by the scheduler (which checks ``consume`` at
service-startup) and by tests (which use ``reset_for_tests`` between
asserts).
"""

from __future__ import annotations


class StartupTracker:
    def __init__(self) -> None:
        self._fired = False

    @property
    def fired(self) -> bool:
        return self._fired

    def consume(self) -> bool:
        """Return True iff this is the first call; mark as fired afterwards."""
        if self._fired:
            return False
        self._fired = True
        return True

    def reset_for_tests(self) -> None:
        self._fired = False
