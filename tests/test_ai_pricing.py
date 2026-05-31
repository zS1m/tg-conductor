"""§13.1 / §13.2 / §13.7 / §13.8 — model pricing table + cost_micros.

Spec scenarios covered:

* Known model: ``cost_micros = ceil(prompt * input_per_1m) + ceil(completion * output_per_1m)``
  (integer arithmetic, no float accumulation across calls).
* Unknown model: ``cost_micros == 0`` + WARNING emitted once.
* YAML loader picks up env-configured path; missing file is non-fatal.
* ``pricing.yaml.example`` parses cleanly and yields integer costs.
"""

from __future__ import annotations

import logging
from pathlib import Path

import pytest

from tg_conductor.ai.pricing import PricingTable


def _make_table() -> PricingTable:
    return PricingTable.load_default(Path("pricing.yaml.example"))


def test_known_model_uses_ceil_per_direction() -> None:
    table = PricingTable.load_default(Path("pricing.yaml.example"))
    # gpt-4o-mini: 1.05 in / 4.20 out per 1M tokens.
    # 1000 prompt → ceil(1000 * 1.05) = 1050 µ¥.
    # 500 completion → ceil(500 * 4.20) = 2100 µ¥. Sum: 3150.
    cost = table.cost_micros(
        model="gpt-4o-mini", prompt_tokens=1000, completion_tokens=500
    )
    assert cost == 3150


def test_ceil_rounds_up_fractional_micros() -> None:
    """1 token at a fractional yuan rate still produces an integer ≥ 1."""
    table = PricingTable.load_default(Path("pricing.yaml.example"))
    # 1 token * 1.05 = 1.05 → ceil → 2.
    cost = table.cost_micros(model="gpt-4o-mini", prompt_tokens=1, completion_tokens=0)
    assert cost == 2


def test_zero_tokens_returns_zero() -> None:
    table = PricingTable.load_default(Path("pricing.yaml.example"))
    assert (
        table.cost_micros(model="gpt-4o-mini", prompt_tokens=0, completion_tokens=0)
        == 0
    )


def test_unknown_model_returns_zero_and_logs_warning(
    caplog: pytest.LogCaptureFixture,
) -> None:
    table = PricingTable.load_default(Path("pricing.yaml.example"))
    with caplog.at_level(logging.WARNING, logger="tg_conductor.ai.pricing"):
        cost = table.cost_micros(
            model="totally-new-model", prompt_tokens=100, completion_tokens=100
        )
    assert cost == 0
    assert any(
        "pricing.unknown_model" in rec.getMessage()
        and "totally-new-model" in rec.getMessage()
        for rec in caplog.records
    )


def test_unknown_model_warns_only_once(
    caplog: pytest.LogCaptureFixture,
) -> None:
    table = PricingTable.load_default(Path("pricing.yaml.example"))
    with caplog.at_level(logging.WARNING, logger="tg_conductor.ai.pricing"):
        for _ in range(5):
            table.cost_micros(
                model="another-new-model", prompt_tokens=10, completion_tokens=10
            )
    unknown_lines = [r for r in caplog.records if "another-new-model" in r.getMessage()]
    assert len(unknown_lines) == 1


def test_missing_file_loads_empty_table_with_warning(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    missing = tmp_path / "no-such-pricing.yaml"
    with caplog.at_level(logging.WARNING, logger="tg_conductor.ai.pricing"):
        table = PricingTable.load_default(missing)
    assert table.get("gpt-4o-mini") is None
    assert any("pricing.file_missing" in rec.getMessage() for rec in caplog.records)


def test_invalid_entry_skipped_with_warning(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    path = tmp_path / "broken.yaml"
    path.write_text(
        "models:\n"
        "  good-model: {input_per_1m_yuan: 1.0, output_per_1m_yuan: 2.0}\n"
        "  bad-model: {input_per_1m_yuan: not-a-number}\n",
        encoding="utf-8",
    )
    with caplog.at_level(logging.WARNING, logger="tg_conductor.ai.pricing"):
        table = PricingTable.load_default(path)
    assert table.get("good-model") is not None
    assert table.get("bad-model") is None
    assert any("pricing.invalid_entry" in rec.getMessage() for rec in caplog.records)


def test_env_var_overrides_default_path(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    custom = tmp_path / "custom.yaml"
    custom.write_text(
        "models:\n  envmodel: {input_per_1m_yuan: 3.0, output_per_1m_yuan: 6.0}\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("PRICING_FILE", str(custom))
    table = PricingTable.load_default()
    assert (
        table.cost_micros(model="envmodel", prompt_tokens=100, completion_tokens=100)
        == 100 * 3 + 100 * 6
    )


def test_returns_int_not_float() -> None:
    table = _make_table()
    cost = table.cost_micros(
        model="gpt-4o-mini", prompt_tokens=37, completion_tokens=89
    )
    assert isinstance(cost, int)
