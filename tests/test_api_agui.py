"""Covers the AG-UI HTTP surface: the optional POST /threads endpoint,
and that /agui/... runs mint a fresh thread automatically for an
unrecognized threadId rather than requiring POST /threads first (see
souk-no-forced-protocol-deviation) — real ag_ui.core.RunAgentInput shape
and all.

Every route takes the pair now. What that changed here is more than the
URLs: "offline" used to be arranged by backdating `last_seen_at` past a
heartbeat window, which is why half these tests reached into the `agents`
table to set up a fact about liveness. `online` is `is_serving`, so an
agent nobody attached is already offline and the setup is simply not
attaching one.
"""

from __future__ import annotations

import json

from souk import repo
from souk.models import AgentRef


def _run_input(thread_id: str, message: str = "hi") -> dict:
    """The real ag_ui.core.RunAgentInput wire shape — threadId/runId/
    state/messages/tools/context/forwardedProps all required by the real
    schema (see souk/models.py's module docstring for why souk no longer
    has its own looser version of this). runId is required by the schema
    but never actually used by souk — any placeholder satisfies it.
    """
    return {
        "threadId": thread_id,
        "runId": "ignored",
        "state": None,
        "messages": [{"id": "whatever", "role": "user", "content": message}],
        "tools": [],
        "context": [],
        "forwardedProps": None,
    }


def _run_started(sse_body: str) -> dict:
    """The stream's own RUN_STARTED event — the standard, in-band place a
    client learns the resolved threadId and runId (no custom X-Souk-*
    headers — see souk-no-forced-protocol-deviation).
    """
    for line in sse_body.splitlines():
        if line.startswith("data: "):
            event = json.loads(line[len("data: ") :])
            if event.get("type") == "RUN_STARTED":
                return event
    raise AssertionError(f"no RUN_STARTED event found in: {sse_body!r}")


async def test_create_thread_by_pair_returns_a_real_thread_id(client, register):
    served = await register("greeter")

    first = await client.post(f"/threads/{served.path()}")
    assert first.status_code == 200, first.text
    assert first.json()["thread_id"].startswith("thread_")

    second = await client.post(f"/threads/{served.path()}")
    assert second.json()["thread_id"] != first.json()["thread_id"]


async def test_create_thread_for_an_unregistered_agent_404s(client, register):
    served = await register("greeter")

    # A real provider, a name it never registered.
    assert (await client.post(f"/threads/{served.fingerprint}/nobody")).status_code == 404
    # A real name, a provider that does not exist.
    assert (await client.post(f"/threads/{'0' * 16}/greeter")).status_code == 404


async def test_agui_run_mints_a_fresh_thread_for_an_unrecognized_thread_id(client, register):
    """AG-UI's `threadId` is caller-minted and required by the schema —
    an id souk has never seen is a brand new conversation, not an error
    (unlike A2A's optional `contextId` — see test_api_a2a.py). Nobody is
    attached, purely so the run resolves immediately instead of streaming
    forever waiting for a provider — unrelated to what this test checks.
    """
    served = await register("greeter")

    resp = await client.post(f"/agui/{served.path()}", json=_run_input("thread_made_up"))

    assert resp.status_code == 200, resp.text
    real_thread_id = _run_started(resp.text)["threadId"]
    assert real_thread_id.startswith("thread_")
    assert real_thread_id != "thread_made_up"


async def test_agui_run_against_an_offline_agent_fails_fast(client, register):
    served = await register("translator")

    created = await client.post(f"/threads/{served.path()}")
    thread_id = created.json()["thread_id"]

    resp = await client.post(f"/agui/{served.path()}", json=_run_input(thread_id))

    assert resp.status_code == 200
    assert _run_started(resp.text)["threadId"] == thread_id
    assert "RUN_ERROR" in resp.text
    assert "offline" in resp.text


async def test_agui_run_reaches_an_attached_provider(client, serve):
    """The other half, and the one the old suite could not write: back then
    "online" was a timestamp, so a test could fake it by leaving
    `last_seen_at` fresh without anyone actually serving — and the run would
    then hang. Serving is now a live mapping, so there is nothing to fake:
    the provider is really there, and the stream really comes from it.
    """
    served = await serve(None, "greeter")

    resp = await client.post(f"/agui/{served.path()}", json=_run_input("thread_new"))

    assert resp.status_code == 200, resp.text
    types = [
        json.loads(line[len("data: ") :])["type"]
        for line in resp.text.splitlines()
        if line.startswith("data: ")
    ]
    assert types[0] == "RUN_STARTED"
    assert "TEXT_MESSAGE_CONTENT" in types
    assert types[-1] == "RUN_FINISHED"


async def _offline_run_with_metadata(client, served, metadata):
    """The fast-fail path resolves synchronously, still going through
    ensure_thread/create_run with the real metadata first (see
    api_agui._run_agent) — so it is enough to check what got persisted
    without needing a live provider.
    """
    body = _run_input("thread_made_up")
    body["metadata"] = metadata
    return await client.post(f"/agui/{served.path()}", json=body)


async def test_agui_run_with_valid_actor_chain_stores_verified_chain(client, session, register, new_identity):
    caller = new_identity()
    served = await register("greeter")
    subject = {"type": "user", "id": "employee_x"}
    chain = [caller.sign_chain_hop(subject)]

    resp = await _offline_run_with_metadata(client, served, {"actorChain": chain})
    assert resp.status_code == 200, resp.text
    run_id = _run_started(resp.text)["runId"]

    run = await repo.get_run(session, run_id)
    assert run.metadata["verifiedActorChain"]["subject"] == subject
    assert run.metadata["verifiedActorChain"]["actors"] == [
        {"publicKey": caller.public_key, "agentName": None}
    ]


async def test_agui_run_with_invalid_actor_chain_401s(client, register):
    served = await register("greeter")

    body = _run_input("thread_made_up")
    body["metadata"] = {"actorChain": ["not-a-real-jwt"]}

    resp = await client.post(f"/agui/{served.path()}", json=body)

    assert resp.status_code == 401


async def test_agui_run_without_actor_chain_is_unaffected(client, session, register):
    served = await register("greeter")

    resp = await _offline_run_with_metadata(client, served, {})
    assert resp.status_code == 200, resp.text
    run_id = _run_started(resp.text)["runId"]

    run = await repo.get_run(session, run_id)
    assert "verifiedActorChain" not in run.metadata


def test_build_forwarded_props_includes_caller_when_chain_verified():
    from souk.protocols.agui import build_forwarded_props

    subject = {"type": "user", "id": "employee_x"}
    actors = [{"publicKey": "abc", "agentName": None}]
    chain = ["hop0"]
    agent = AgentRef(provider_key="abc", name="greeter")

    result = build_forwarded_props(
        "test-signing-secret", "run_1", agent, {}, {"appSpecific": True}, subject, actors, chain
    )

    assert result == {
        "appSpecific": True,
        "caller": {"subject": subject, "actors": actors, "chain": chain},
    }


def test_build_forwarded_props_without_chain_or_kyok_passes_through_untouched():
    from souk.protocols.agui import build_forwarded_props

    agent = AgentRef(provider_key="abc", name="greeter")
    result = build_forwarded_props(
        "test-signing-secret", "run_1", agent, {}, {"appSpecific": True}
    )

    assert result == {"appSpecific": True}


async def test_a_thread_whose_pending_buffer_is_full_refuses_with_429(client, register, session, souk):
    """Upstream bounds a thread's pending-utterance buffer and refuses at
    the limit rather than accepting and expiring the message later. That
    refusal reaches this door, and which status it deserves is this
    layer's call: **429, not 409.**

    The distinction is not cosmetic. Every other refusal mapped to 409
    here means "your request conflicts with the state and will keep
    conflicting" — a de-listed agent, a thread owned by somebody else. A
    full queue means "right now" and nothing else: the identical request
    succeeds once the thread drains, which is precisely what 429 tells a
    caller and what 409 tells it not to bother doing.
    """
    served = await register("greeter")
    thread_id = await repo.create_thread(session, served.ref())
    limit = souk.settings.thread_queue_limit
    assert limit is not None, "this test is about the limit being set"
    # Filled through the repo rather than the door: these stand in for
    # utterances already waiting, and driving them through /agui would
    # start dispatching them.
    for _ in range(limit):
        await repo.create_run(session, thread_id, served.ref(), "ag-ui", {})
    await session.commit()

    resp = await client.post(f"/agui/{served.path()}", json=_run_input(thread_id))

    assert resp.status_code == 429, resp.text
    assert "not accepted" in resp.json()["detail"]
