"""`with_thread_history` — merging what souk holds into what the caller sent.

Worth testing rather than eyeballing because the failure is quiet: get the
deduplication wrong and the agent sees the visitor's last message twice,
which reads as a person repeating themselves rather than as a bug.
"""

from __future__ import annotations

import pytest
from souk_agent_sdk.client import SoukQueryFailed

from pydantic_ai_agent.history import SoukAccess, with_thread_history


class FakeLink:
    def __init__(self, messages=None, raises: Exception | None = None) -> None:
        self.messages = messages or []
        self.raises = raises
        self.calls: list[tuple] = []

    async def thread_messages(self, thread_id: str, *, limit=None):
        self.calls.append((thread_id, limit))
        if self.raises is not None:
            raise self.raises
        return self.messages


def _access(link) -> SoukAccess:
    access = SoukAccess()
    access.link = link
    return access


def _input(*messages) -> dict:
    return {"threadId": "t1", "runId": "r1", "messages": list(messages)}


async def test_history_replaces_what_the_caller_resent():
    """souk's copy already contains this run's own message — it is stored
    before the run is offered — so the overlap must collapse, not stack."""
    caller = {"id": "msg_2", "role": "user", "content": "second"}
    link = FakeLink([{"id": "msg_1", "role": "user", "content": "first"}, caller])

    merged = await with_thread_history(_input(caller), _access(link), limit=20)

    assert [m["content"] for m in merged["messages"]] == ["first", "second"]


async def test_a_message_souk_has_not_stored_yet_is_kept_at_the_end():
    """Normally empty — a caller's message is persisted before the run is
    offered — but "normally" is not a thing to build on."""
    unsaved = {"id": "msg_new", "role": "user", "content": "not yet stored"}
    link = FakeLink([{"id": "msg_1", "role": "user", "content": "first"}])

    merged = await with_thread_history(_input(unsaved), _access(link), limit=20)

    assert [m["content"] for m in merged["messages"]] == ["first", "not yet stored"]


async def test_the_limit_is_passed_to_souk_rather_than_applied_here():
    """The parameter exists to keep the response frame bounded; trimming
    on return would have already put the whole thread on the wire."""
    link = FakeLink([{"id": "msg_1", "content": "first"}])

    await with_thread_history(_input(), _access(link), limit=5)

    assert link.calls == [("t1", 5)]


@pytest.mark.parametrize("limit", [None, 0])
async def test_no_limit_configured_asks_souk_nothing(limit):
    """Opt-in, and off means off: an agent that did not ask for history
    must not pay for a round trip on every run."""
    link = FakeLink([{"id": "msg_1", "content": "first"}])

    merged = await with_thread_history(_input(), _access(link), limit=limit)

    assert link.calls == []
    assert merged["messages"] == []


async def test_a_failed_query_answers_from_this_turn_alone():
    """An agent that cannot reach the history should still answer.
    Degrading to what it was told is exactly what it did before this
    existed — raising here would turn a missing convenience into a dead
    run."""
    caller = {"id": "msg_2", "content": "second"}
    link = FakeLink(raises=SoukQueryFailed("souk connection closed"))

    merged = await with_thread_history(_input(caller), _access(link), limit=20)

    assert merged["messages"] == [caller]


async def test_an_empty_thread_leaves_the_input_untouched():
    """`[]` is a real answer, and it must not blank out what the caller
    actually sent."""
    caller = {"id": "msg_2", "content": "second"}

    merged = await with_thread_history(_input(caller), _access(FakeLink([])), limit=20)

    assert merged["messages"] == [caller]


async def test_a_provider_with_no_link_yet_is_not_an_error():
    """`SoukAccess` is filled after the provider is constructed, so a run
    arriving in that window has no link. It answers from its input rather
    than raising."""
    caller = {"id": "msg_2", "content": "second"}

    merged = await with_thread_history(_input(caller), SoukAccess(), limit=20)

    assert merged["messages"] == [caller]
