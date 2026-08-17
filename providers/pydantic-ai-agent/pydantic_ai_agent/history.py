"""Asking souk for the conversation this run's input did not carry.

A provider is handed exactly what the *caller* sent for this run. An AG-UI
client resends its whole history every turn by convention, so an agent
reached that way sees ten turns; A2A's `message/send` carries one message,
so the same agent reached that way sees one. It cannot tell the difference
between a tenth turn and a first, and souk has held the thread all along.

This is not hypothetical in this repo's own demo market. Zahra's haggler
delegates to Yusuf's scribe over A2A, so the scribe receives a single
message every time — a fresh conversation as far as it can tell, on a
thread it has been talking on all afternoon.

**Opt-in per agent, and that is upstream's design rather than caution.**
Which context an agent wants is the agent's business: windows differ, costs
differ, some agents would rather summarise. souk holds the history and
declines to decide how much of it anyone needs, so `thread_history_limit`
is a number the agent's author picks.
"""

from __future__ import annotations

import logging
from typing import Any

from souk_agent_sdk.client import SoukQueryFailed

logger = logging.getLogger("pydantic_ai_agent")


class SoukAccess:
    """A one-slot holder for the link, filled after the provider is built.

    Not elegance — ordering. `SoukProvider`'s constructor takes the handles,
    and a handle's `run_stream` is what needs the link, so one of the two
    has to exist first and it cannot be the provider. The alternative is
    threading the provider through every closure after the fact, which is
    this with more steps.
    """

    def __init__(self) -> None:
        self.link: Any = None


async def with_thread_history(
    run_input: dict[str, Any], souk: SoukAccess, limit: int | None
) -> dict[str, Any]:
    """`run_input` with the thread's own messages merged in.

    Merged rather than replaced, and deduplicated by souk's message id.
    `thread_messages` overlaps `run_input["messages"]` on purpose — it
    returns the thread, *including* what this run was started with — and
    every message carries the id souk minted for it, so the overlap is
    exactly recognisable. Prepending blindly would show the agent its
    caller's turn twice.

    Order is history first, then anything from this run that souk has not
    stored yet. A caller's message is normally persisted before the run is
    offered, so that tail is usually empty; it is here because "usually"
    is not a thing to build on.

    A failed query is logged and the caller's own input is returned
    unchanged. An agent that cannot reach the history should still answer —
    degrading to what it was told is exactly what it did before this
    existed.
    """
    thread_id = run_input.get("threadId")
    if not limit or not thread_id or souk.link is None:
        return run_input
    try:
        history = await souk.link.thread_messages(thread_id, limit=limit)
    except SoukQueryFailed:
        logger.warning(
            "could not read thread %s; answering from this turn alone", thread_id, exc_info=True
        )
        return run_input

    seen = {m.get("id") for m in history if m.get("id")}
    tail = [m for m in (run_input.get("messages") or []) if m.get("id") not in seen]
    if not history:
        return run_input
    logger.info(
        "thread %s: %d message(s) from souk, %d not yet stored",
        thread_id,
        len(history),
        len(tail),
    )
    return {**run_input, "messages": [*history, *tail]}
