"""Backend-agnostic Telegram exceptions.

The throttle layer and business code only know these classes. Concrete
adapters (e.g. ``KurigramAdapter``) translate library-specific exceptions
into these wrappers, keeping the upstream surface swappable.
"""

from __future__ import annotations


class TGError(Exception):
    """Base for all tg_core-raised exceptions."""


class TGFloodWait(TGError):
    """Telegram asked us to back off. ``seconds`` is the server-suggested wait."""

    def __init__(self, seconds: float, operation: str = "") -> None:
        self.seconds = float(seconds)
        self.operation = operation
        super().__init__(f"FloodWait {self.seconds:.1f}s in {operation!r}")
