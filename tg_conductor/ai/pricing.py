"""Model pricing table + per-call ``cost_micros`` arithmetic.

spec ai-usage §"模型价格表":

* Prices live in an external YAML so OpenAI price changes don't require
  a code release. Default path ``./pricing.yaml``; override via the
  ``PRICING_FILE`` env var or the explicit constructor argument.
* Yuan-denominated, per-1M-tokens: ``input_per_1m_yuan`` /
  ``output_per_1m_yuan``. 1 元 = 1,000,000 µ元 so the integer
  ``cost_micros`` for a call is just ``ceil(tokens * per_1m_yuan)``
  per direction — no exchange-rate math at runtime (anyone wanting to
  express USD prices bakes the converted yuan number into the YAML at
  edit time).
* Unknown model → ``cost_micros=0`` + one WARNING log per model
  (rate-limited so a hot path with a typo doesn't flood logs).

Integer-only invariant: each call's cost is computed by ``math.ceil``
and stored as ``int``; aggregation is a SUM of ints. Float never
accumulates across calls.
"""

from __future__ import annotations

import logging
import math
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

log = logging.getLogger(__name__)


_DEFAULT_PRICING_PATH = Path("pricing.yaml")
_ENV_PRICING_PATH = "PRICING_FILE"


@dataclass(slots=True, frozen=True)
class ModelPrice:
    input_per_1m_yuan: float
    output_per_1m_yuan: float


class PricingTable:
    """In-memory price lookup loaded from a YAML file.

    Construct with :meth:`load_default` to pick up the env-configured
    path; tests can build one directly from a ``models`` dict.
    """

    def __init__(self, models: dict[str, ModelPrice]) -> None:
        self._models = models
        self._warned: set[str] = set()

    # ---------------------------------------------------------- loading

    @classmethod
    def load_default(cls, path: Path | None = None) -> PricingTable:
        """Load from ``path`` / ``PRICING_FILE`` env / ``./pricing.yaml``.

        Missing file is **not** fatal — boot succeeds with an empty table
        and every call ends up at ``cost_micros=0`` with a WARNING. This
        keeps fresh deployments runnable before the operator copies
        ``pricing.yaml.example``.
        """
        resolved = path or Path(
            os.environ.get(_ENV_PRICING_PATH, _DEFAULT_PRICING_PATH)
        )
        if not resolved.exists():
            log.warning(
                "pricing.file_missing path=%s — all AI calls will record "
                "cost_micros=0; copy pricing.yaml.example to %s to enable "
                "cost accounting",
                resolved,
                resolved,
            )
            return cls({})
        with resolved.open("r", encoding="utf-8") as fh:
            data = yaml.safe_load(fh) or {}
        return cls(_parse_models(data))

    # ---------------------------------------------------------- lookup

    def get(self, model: str) -> ModelPrice | None:
        return self._models.get(model)

    def cost_micros(
        self,
        *,
        model: str,
        prompt_tokens: int,
        completion_tokens: int,
    ) -> int:
        """spec ai-usage §"模型价格表" — integer ceil per direction.

        Unknown models log a WARNING (once per model) and return 0.
        """
        price = self._models.get(model)
        if price is None:
            if model not in self._warned:
                self._warned.add(model)
                log.warning(
                    "pricing.unknown_model model=%s cost_micros=0",
                    model,
                )
            return 0
        return math.ceil(prompt_tokens * price.input_per_1m_yuan) + math.ceil(
            completion_tokens * price.output_per_1m_yuan
        )


def _parse_models(data: dict[str, Any]) -> dict[str, ModelPrice]:
    models_raw = data.get("models") or {}
    parsed: dict[str, ModelPrice] = {}
    for name, body in models_raw.items():
        try:
            parsed[name] = ModelPrice(
                input_per_1m_yuan=float(body["input_per_1m_yuan"]),
                output_per_1m_yuan=float(body["output_per_1m_yuan"]),
            )
        except (KeyError, TypeError, ValueError) as exc:
            log.warning("pricing.invalid_entry model=%s error=%s", name, exc)
    return parsed
