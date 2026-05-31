"""§7.8 / §7.9 — Pydantic schema for Workflow / Trigger / ActionStep / ActionPlan.

Pure schema validation: no DB, no I/O. DB-level checks live in
``test_workflow_validate``.
"""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from tg_conductor.workflows.schema import (
    ActionPlan,
    ActionPlanAdapter,
    CountRange,
    CronTrigger,
    DelayRange,
    MessageMatchTrigger,
    SendDiceStep,
    SendTextStep,
    StartupTrigger,
    TimeWindowTrigger,
    TriggerAdapter,
    Workflow,
)

# ------------------------------------------------------------ trigger union (§7.8)


@pytest.mark.parametrize(
    "raw,expected_cls",
    [
        ({"type": "cron", "expression": "30 9 * * *"}, CronTrigger),
        (
            {
                "type": "time_window",
                "window": "10:00-23:00",
                "count": 8,
                "min_gap": "30m",
            },
            TimeWindowTrigger,
        ),
        (
            {
                "type": "message_match",
                "chat_id": -1001234567890,
                "text_pattern": "签到成功",
            },
            MessageMatchTrigger,
        ),
        ({"type": "startup"}, StartupTrigger),
    ],
)
def test_trigger_discriminator_round_trip(raw, expected_cls):
    parsed = TriggerAdapter.validate_python(raw)
    assert isinstance(parsed, expected_cls)
    dumped = TriggerAdapter.dump_python(parsed, mode="json")
    reparsed = TriggerAdapter.validate_python(dumped)
    assert isinstance(reparsed, expected_cls)
    assert reparsed == parsed


def test_unknown_trigger_type_rejected():
    with pytest.raises(ValidationError):
        TriggerAdapter.validate_python({"type": "moon_phase"})


def test_invalid_cron_expression_rejected():
    with pytest.raises(ValidationError, match="invalid cron"):
        CronTrigger(type="cron", expression="not-a-cron")


def test_time_window_count_can_be_range():
    t = TimeWindowTrigger(
        type="time_window",
        window="10:00-23:00",
        count=CountRange(min=8, max=9),
        min_gap="30m",
    )
    assert isinstance(t.count, CountRange)


def test_time_window_rejects_end_before_start():
    with pytest.raises(ValidationError, match="later than start"):
        TimeWindowTrigger(
            type="time_window",
            window="23:00-10:00",
            count=1,
            min_gap="1m",
        )


def test_time_window_rejects_malformed_min_gap():
    with pytest.raises(ValidationError, match="min_gap"):
        TimeWindowTrigger(
            type="time_window",
            window="10:00-11:00",
            count=1,
            min_gap="thirty minutes",
        )


def test_message_match_rejects_bad_regex():
    with pytest.raises(ValidationError, match="not a valid regex"):
        MessageMatchTrigger(type="message_match", chat_id=1, text_pattern="(unbalanced")


def test_message_match_rejects_str_chat_id():
    """String handles like '@channel' can never match incoming TG events.

    Pyrogram resolves chat references to numeric ids before they reach our
    handler. A str trigger ``chat_id`` would silently never fire.
    """
    with pytest.raises(ValidationError):
        MessageMatchTrigger.model_validate(
            {"type": "message_match", "chat_id": "@some_channel"}
        )


# ------------------------------------------------------------ action union (§7.9)


def _wrap_in_plan(step_dict: dict) -> dict:
    return {"steps": [step_dict]}


@pytest.mark.parametrize(
    "step",
    [
        {"action": "send_text", "chat_id": 1, "text": "hi"},
        {
            "action": "send_text",
            "chat_id": 1,
            "text_pool": ["a", "b", "c"],
            "pick_n": 2,
            "shuffle": True,
        },
        {"action": "send_dice", "chat_id": 1},
        {"action": "send_dice", "chat_id": 1, "emoji": "🎯"},
        {
            "action": "forward",
            "to_chat_id": -100,
            "source": "wait_for_step_1",
        },
        {
            "action": "click_button",
            "match": {"text": "签到"},
        },
        {
            "action": "click_button",
            "match": {"text_regex": "^签到.*$"},
        },
        {
            "action": "click_button",
            "match": {"ai_image_prompt": "选出图中的猫"},
        },
        {
            "action": "wait_for",
            "chat_id": 1,
            "text_pattern": "请点击下方",
        },
        {
            "action": "ai_reply",
            "to_chat_id": 1,
            "prompt_template": "Echo: {message_text}",
        },
    ],
)
def test_action_discriminator_round_trip(step):
    plan = ActionPlanAdapter.validate_python(_wrap_in_plan(step))
    dumped = ActionPlanAdapter.dump_python(plan, mode="json")
    reparsed = ActionPlanAdapter.validate_python(dumped)
    assert reparsed == plan
    assert reparsed.steps[0].action == step["action"]


def test_unknown_action_rejected():
    with pytest.raises(ValidationError):
        ActionPlanAdapter.validate_python(_wrap_in_plan({"action": "self_destruct"}))


# ------------------------------------------------------------ mutex / sanity


def test_send_text_text_and_pool_mutex():
    with pytest.raises(ValidationError, match="mutually exclusive"):
        SendTextStep(
            action="send_text",
            chat_id=1,
            text="x",
            text_pool=["y"],
            pick_n=1,
        )


def test_send_text_pool_requires_pick_n():
    with pytest.raises(ValidationError, match="pick_n"):
        SendTextStep(
            action="send_text",
            chat_id=1,
            text_pool=["a", "b"],
        )


def test_send_text_pick_n_exceeds_pool_rejected():
    with pytest.raises(ValidationError, match="exceeds text_pool size"):
        SendTextStep(
            action="send_text",
            chat_id=1,
            text_pool=["a", "b"],
            pick_n=5,
        )


def test_send_text_requires_either_text_or_pool():
    with pytest.raises(ValidationError, match="either text or text_pool"):
        SendTextStep(action="send_text", chat_id=1)


def test_send_dice_rejects_unknown_emoji():
    with pytest.raises(ValidationError, match="emoji"):
        SendDiceStep(action="send_dice", chat_id=1, emoji="🍎")


def test_click_button_match_exactly_one():
    with pytest.raises(ValidationError, match="exactly one"):
        ActionPlan(
            steps=[
                {
                    "action": "click_button",
                    "match": {"text": "a", "text_regex": "b"},
                }  # type: ignore[list-item]
            ]
        )


def test_action_plan_steps_and_variants_mutex():
    with pytest.raises(ValidationError, match="mutually exclusive"):
        ActionPlanAdapter.validate_python(
            {
                "steps": [{"action": "send_text", "chat_id": 1, "text": "x"}],
                "variants": [
                    {"steps": [{"action": "send_text", "chat_id": 1, "text": "y"}]}
                ],
                "pick_variant": "random",
            }
        )


def test_action_plan_empty_rejected():
    with pytest.raises(ValidationError, match="at least one"):
        ActionPlan()


def test_action_plan_variants_require_pick_variant():
    with pytest.raises(ValidationError, match="pick_variant is required"):
        ActionPlanAdapter.validate_python(
            {
                "variants": [
                    {"steps": [{"action": "send_text", "chat_id": 1, "text": "y"}]}
                ]
            }
        )


def test_action_plan_pick_variant_without_variants_rejected():
    with pytest.raises(ValidationError, match="only applies"):
        ActionPlanAdapter.validate_python(
            {
                "steps": [{"action": "send_text", "chat_id": 1, "text": "x"}],
                "pick_variant": "random",
            }
        )


def test_inter_step_delay_max_lt_min_rejected():
    with pytest.raises(ValidationError, match=">= min"):
        DelayRange(min=5.0, max=1.0)


def test_job_timeout_must_cover_max_step_timeout():
    """§11.14b precursor: refuse configs where job_timeout < max(step.timeout)."""
    with pytest.raises(ValidationError, match="job_timeout"):
        ActionPlanAdapter.validate_python(
            {
                "steps": [
                    {
                        "action": "wait_for",
                        "chat_id": 1,
                        "timeout": 120.0,
                    }
                ],
                "job_timeout": 60.0,
            }
        )


def test_job_timeout_uses_default_when_step_omits_explicit():
    """If a wait_for step has no timeout, default 600s applies — job_timeout must cover."""
    with pytest.raises(ValidationError, match="job_timeout"):
        ActionPlanAdapter.validate_python(
            {
                "steps": [{"action": "wait_for", "chat_id": 1}],
                "job_timeout": 60.0,
            }
        )


# ------------------------------------------------- delay_before (add-step-delay-before)


def test_delay_before_fixed_and_range_parse():
    plan = ActionPlanAdapter.validate_python(
        {
            "steps": [
                {"action": "send_text", "chat_id": 1, "text": "a", "delay_before": 30},
                {
                    "action": "send_text",
                    "chat_id": 1,
                    "text": "b",
                    "delay_before": {"min": 10, "max": 30},
                },
            ]
        }
    )
    assert plan.steps[0].delay_before == 30.0
    assert isinstance(plan.steps[1].delay_before, DelayRange)
    assert (plan.steps[1].delay_before.min, plan.steps[1].delay_before.max) == (10, 30)


def test_delay_before_negative_rejected():
    with pytest.raises(ValidationError, match="delay_before must be >= 0"):
        SendTextStep(action="send_text", chat_id=1, text="a", delay_before=-1)


def test_job_timeout_must_cover_step_timeout_plus_delay_before():
    # ai_reply default timeout 60 + delay_before 30 = 90 > job_timeout 80 → reject.
    with pytest.raises(ValidationError, match="job_timeout"):
        ActionPlanAdapter.validate_python(
            {
                "steps": [
                    {
                        "action": "ai_reply",
                        "to_chat_id": 1,
                        "prompt_template": "x",
                        "delay_before": 30,
                    }
                ],
                "job_timeout": 80.0,
            }
        )


def test_job_timeout_covers_step_timeout_plus_delay_before():
    # 60 + 30 = 90 <= job_timeout 100 → ok.
    plan = ActionPlanAdapter.validate_python(
        {
            "steps": [
                {
                    "action": "ai_reply",
                    "to_chat_id": 1,
                    "prompt_template": "x",
                    "delay_before": 30,
                }
            ],
            "job_timeout": 100.0,
        }
    )
    assert plan.job_timeout == 100.0


# ------------------------------------------------------------ Workflow envelope


def test_workflow_round_trip():
    raw = {
        "name": "daily-signin",
        "account_id": 1,
        "trigger": {"type": "cron", "expression": "30 9 * * *"},
        "action_plan": {
            "steps": [{"action": "send_text", "chat_id": -1, "text": "签到"}]
        },
    }
    wf = Workflow.model_validate(raw)
    assert wf.enabled is True
    dumped = wf.model_dump(mode="json")
    again = Workflow.model_validate(dumped)
    assert again == wf


def test_workflow_rejects_empty_name():
    with pytest.raises(ValidationError, match="name"):
        Workflow(
            name="",
            account_id=1,
            trigger=StartupTrigger(type="startup"),
            action_plan=ActionPlan(
                steps=[
                    {"action": "send_text", "chat_id": 1, "text": "x"}  # type: ignore[list-item]
                ]
            ),
        )


def test_workflow_rejects_unknown_top_level_field():
    with pytest.raises(ValidationError):
        Workflow.model_validate(
            {
                "name": "x",
                "account_id": 1,
                "trigger": {"type": "startup"},
                "action_plan": {
                    "steps": [{"action": "send_text", "chat_id": 1, "text": "x"}]
                },
                "extra_field": "no",
            }
        )
