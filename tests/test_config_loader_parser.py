"""§8.1 / §8.2 / §8.9 — YAML scanner with collected error reporting."""

from __future__ import annotations

from pathlib import Path

import pytest

from tg_conductor.config_loader.parser import (
    LoadResult,
    load_workflows_from_dir,
)

VALID_YAML = """
name: daily-signin
account_id: 1
trigger:
  type: cron
  expression: "30 9 * * *"
action_plan:
  steps:
    - action: send_text
      chat_id: -1001234567890
      text: "签到"
"""

BAD_CRON_YAML = """
name: bad-cron
account_id: 1
trigger:
  type: cron
  expression: "not a cron"
action_plan:
  steps:
    - action: send_text
      chat_id: 1
      text: hi
"""

MUTEX_VIOLATION_YAML = """
name: mutex-bad
account_id: 1
trigger:
  type: startup
action_plan:
  steps:
    - action: send_text
      chat_id: 1
      text: x
  variants:
    - steps:
        - action: send_text
          chat_id: 1
          text: y
  pick_variant: random
"""

MULTI_WORKFLOW_LIST = """
- name: a
  account_id: 1
  trigger: { type: startup }
  action_plan:
    steps:
      - action: send_text
        chat_id: 1
        text: x
- name: b
  account_id: 1
  trigger: { type: startup }
  action_plan:
    steps:
      - action: send_text
        chat_id: 1
        text: y
"""


def _write(path: Path, body: str) -> None:
    path.write_text(body.strip() + "\n", encoding="utf-8")


def test_missing_directory_returns_empty(tmp_path: Path) -> None:
    result = load_workflows_from_dir(tmp_path / "does-not-exist")
    assert isinstance(result, LoadResult)
    assert result.workflows == []
    assert result.errors == []
    assert result.ok()


def test_ignores_non_yaml_files(tmp_path: Path) -> None:
    _write(tmp_path / "a.yaml", VALID_YAML)
    _write(tmp_path / "README.md", "# notes")
    _write(tmp_path / "config.toml", "[x]\ny=1")
    result = load_workflows_from_dir(tmp_path)
    assert [p.name for p, _ in result.workflows] == ["a.yaml"]
    assert result.errors == []


def test_loads_yaml_and_yml_extensions(tmp_path: Path) -> None:
    _write(tmp_path / "a.yaml", VALID_YAML)
    _write(
        tmp_path / "b.yml",
        VALID_YAML.replace("daily-signin", "daily-signin-2"),
    )
    result = load_workflows_from_dir(tmp_path)
    names = sorted(wf.name for _, wf in result.workflows)
    assert names == ["daily-signin", "daily-signin-2"]


def test_collects_errors_without_dropping_good_files(tmp_path: Path) -> None:
    """spec §8.9 scenario: 5 files, 1 broken → 4 emerge."""
    _write(tmp_path / "good_1.yaml", VALID_YAML)
    _write(
        tmp_path / "good_2.yaml",
        VALID_YAML.replace("daily-signin", "wf-2"),
    )
    _write(
        tmp_path / "good_3.yaml",
        VALID_YAML.replace("daily-signin", "wf-3"),
    )
    _write(
        tmp_path / "good_4.yaml",
        VALID_YAML.replace("daily-signin", "wf-4"),
    )
    _write(tmp_path / "bad.yaml", BAD_CRON_YAML)

    result = load_workflows_from_dir(tmp_path)

    good_names = sorted(wf.name for _, wf in result.workflows)
    assert good_names == ["daily-signin", "wf-2", "wf-3", "wf-4"]
    assert len(result.errors) >= 1
    [err] = [e for e in result.errors if e.path.name == "bad.yaml"]
    # Pydantic model_validators on discriminator-resolved classes land at
    # ``trigger.cron`` not ``trigger.cron.expression`` — anything mentioning
    # cron in either the field path or message is acceptable.
    assert "cron" in err.field_path or "cron" in err.message.lower()
    assert not result.ok()


def test_yaml_syntax_error_is_file_level(tmp_path: Path) -> None:
    _write(tmp_path / "broken.yaml", "name: ok\n  bad: [unclosed\n")
    result = load_workflows_from_dir(tmp_path)
    assert result.workflows == []
    [err] = result.errors
    assert err.field_path == ""
    assert "YAML parse error" in err.message


def test_empty_file_rejected(tmp_path: Path) -> None:
    _write(tmp_path / "empty.yaml", "")
    result = load_workflows_from_dir(tmp_path)
    assert result.workflows == []
    [err] = result.errors
    assert "empty" in err.message


def test_list_top_level_rejected_with_clear_message(tmp_path: Path) -> None:
    _write(tmp_path / "multi.yaml", MULTI_WORKFLOW_LIST)
    result = load_workflows_from_dir(tmp_path)
    assert result.workflows == []
    [err] = result.errors
    assert "exactly one Workflow" in err.message


def test_scalar_top_level_rejected(tmp_path: Path) -> None:
    _write(tmp_path / "scalar.yaml", "just a string")
    result = load_workflows_from_dir(tmp_path)
    assert result.workflows == []
    [err] = result.errors
    assert "mapping" in err.message


def test_validation_errors_carry_dotted_field_path(tmp_path: Path) -> None:
    _write(tmp_path / "mutex.yaml", MUTEX_VIOLATION_YAML)
    result = load_workflows_from_dir(tmp_path)
    assert result.workflows == []
    assert result.errors  # at least one
    # action_plan-level error from the steps/variants mutex
    assert any(
        "action_plan" in e.field_path or "mutually exclusive" in e.message
        for e in result.errors
    )


@pytest.mark.parametrize("count", [10])
def test_many_files_all_succeed(tmp_path: Path, count: int) -> None:
    for i in range(count):
        _write(
            tmp_path / f"wf_{i:02d}.yaml",
            VALID_YAML.replace("daily-signin", f"wf-{i:02d}"),
        )
    result = load_workflows_from_dir(tmp_path)
    assert len(result.workflows) == count
    assert result.errors == []
