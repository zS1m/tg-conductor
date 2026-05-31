"""``send_text`` action: literal text or pool sampling.

Pool semantics (spec §11.15):
1. ``pick_n`` ranges sample N first.
2. ``random.sample(pool, k=N)`` picks **without replacement**.
3. ``shuffle=True`` then permutes the picked subset; ``shuffle=False`` preserves
   pool order (sorted by index in the original pool).
4. Messages are sent one-by-one in result order.

The action emits a single ``action.send.text`` event carrying the final sent
texts + ids — useful for run history and SSE clients.
"""

from __future__ import annotations

import random
from typing import Any

from tg_conductor.actions.context import ActionContext
from tg_conductor.workflows.schema import CountRange, SendTextStep


async def execute(
    step: SendTextStep,
    ctx: ActionContext,
    *,
    rng: random.Random | None = None,
) -> None:
    if rng is None:
        rng = random.Random()

    texts, sample_info = _resolve_texts(step, rng)

    sent_ids: list[int] = []
    for text in texts:
        msg = await ctx.tg_client.send_text(step.chat_id, text)
        sent_ids.append(msg.id)

    attrs: dict[str, Any] = {
        "step_index": ctx.step_index,
        "chat_id": step.chat_id,
        "texts": texts,
        "sent_message_ids": sent_ids,
        **sample_info,
    }
    await ctx.emit(
        "action.send.text",
        attrs,
        message=f"sent {len(texts)} text(s) to {step.chat_id}",
    )


def _resolve_texts(
    step: SendTextStep, rng: random.Random
) -> tuple[list[str], dict[str, Any]]:
    if step.text_pool is None:
        assert step.text is not None  # schema enforces
        return [step.text], {}

    n = _sample_pick_n(step.pick_n, rng)
    n = min(n, len(step.text_pool))
    # ``random.sample`` is no-replacement; we sample indices so that we can
    # preserve pool order on ``shuffle=False``.
    indices = rng.sample(range(len(step.text_pool)), k=n)
    if step.shuffle:
        rng.shuffle(indices)
    else:
        indices.sort()
    picked = [step.text_pool[i] for i in indices]
    return picked, {
        "text_pool_size": len(step.text_pool),
        "pick_n": n,
        "shuffled": step.shuffle,
    }


def _sample_pick_n(pick_n: int | CountRange | None, rng: random.Random) -> int:
    if pick_n is None:
        return 1
    if isinstance(pick_n, int):
        return pick_n
    return rng.randint(pick_n.min, pick_n.max)
