"""The A2A HTTP surface: pair routing, and the offline fast-fail path.

Half of what this file used to cover no longer exists to cover. There were
three ways to reach an agent — by name, by id, by pair — and two tests here
existed only to hold them level: one asserted the name and id routes served
the same card, another asserted that a name collision 409'd with candidates
while the id routes still worked. Both are gone with the routes. What
replaces them is the property those tests were working around: the pair is
unambiguous, so two providers offering `greeter` need no disambiguation
scheme, and there is nothing for a caller to get wrong.
"""

from __future__ import annotations

from sqlalchemy import func, select

from souk import repo
from souk.schema import runs


async def _register(client, identity, name, **extra) -> str:
    """Register through the HTTP surface and hand back the URL prefix.

    Deliberately not the `register` fixture: this suite is about the routes,
    and `POST /agents/register` is one of them. The fingerprint comes from
    the identity rather than the response body — the roster carries it, but
    a test that reads it back from souk would pass even if the gateway put
    somebody else's agent in the URL it builds.
    """
    resp = await client.post("/agents/register", json=identity.register_body([{"name": name, **extra}]))
    assert resp.status_code == 201, resp.text
    return f"{identity.fingerprint}/{name}"


async def test_the_card_is_served_by_pair_and_says_where_the_rpc_is(client, new_identity):
    identity = new_identity()
    path = await _register(client, identity, "greeter", description="hi")

    resp = await client.get(f"/a2a/{path}/.well-known/agent-card.json")

    assert resp.status_code == 200, resp.text
    # v1.0 replaced the card's single `url` with a list of interfaces, each
    # stating its own binding and protocol version. Core no longer fills it
    # in — the gateway does, because only it knows where it serves.
    interface = resp.json()["supportedInterfaces"][0]
    assert interface["url"].endswith(f"/a2a/{path}/rpc")
    assert interface["protocolBinding"] == "JSONRPC"


async def test_the_full_public_key_addresses_the_same_agent_as_its_fingerprint(client, new_identity):
    """Core tells the two apart by length, so both work. The fingerprint is
    what this gateway puts in a URL; the key is what a caller holding the
    real thing already has, and making it re-derive a short form to be
    understood would be a gateway inventing an identity souk does not use."""
    identity = new_identity()
    await _register(client, identity, "greeter", description="hi")

    by_fingerprint = await client.get(f"/a2a/{identity.fingerprint}/greeter/.well-known/agent-card.json")
    by_key = await client.get(f"/a2a/{identity.public_key}/greeter/.well-known/agent-card.json")

    assert by_fingerprint.status_code == by_key.status_code == 200
    assert by_fingerprint.json() == by_key.json()


async def test_two_providers_may_offer_the_same_name_and_neither_shadows_the_other(
    client, new_identity
):
    """What the 409-with-candidates test used to be about. The collision is
    still real and still allowed; it is simply no longer addressable, so
    there is no winner to pick and no disambiguation to describe."""
    a, b = new_identity(), new_identity()
    path_a = await _register(client, a, "greeter", description="from a")
    path_b = await _register(client, b, "greeter", description="from b")

    card_a = await client.get(f"/a2a/{path_a}/.well-known/agent-card.json")
    card_b = await client.get(f"/a2a/{path_b}/.well-known/agent-card.json")

    assert card_a.status_code == card_b.status_code == 200
    assert card_a.json()["description"] == "from a"
    assert card_b.json()["description"] == "from b"


async def test_the_pre_v1_card_path_is_not_served(client, new_identity):
    """Only the current path. The old one was served for a while, answering
    with the *new* body — which has no top-level `url`, so a pre-v1 client
    found a card it could not use to locate the RPC endpoint. That is not an
    accommodation, and whether to offer a real one is a gateway decision."""
    path = await _register(client, new_identity(), "greeter", description="hi")

    resp = await client.get(f"/a2a/{path}/.well-known/agent.json")

    assert resp.status_code == 404


async def test_a_name_alone_no_longer_addresses_anything(client, new_identity):
    """The route is gone, not relaxed. Worth asserting rather than assuming:
    a bare `/a2a/greeter/rpc` matching nothing is the outcome, but so is it
    matching `/a2a/{provider}/{name}` with provider='greeter' — which would
    have been a 404 either way and told us nothing about which."""
    await _register(client, new_identity(), "greeter")

    assert (await client.get("/a2a/greeter/.well-known/agent-card.json")).status_code == 404
    assert (await client.post("/a2a/greeter/rpc", json={})).status_code == 404


async def test_offline_target_fails_fast_instead_of_queueing(client, register, session):
    """Registered but unattached: nobody is serving it, so the run must end
    rather than wait. `online` is `is_serving` now, so this needs no clock
    manipulation — the old version of this test aged `last_seen_at` by 120
    seconds to push the agent outside a heartbeat window that no longer
    exists."""
    served = await register("translator")

    thread_id = await repo.create_thread(session, served.ref())
    await session.commit()

    resp = await client.post(
        f"/a2a/{served.path()}/rpc",
        json={
            "jsonrpc": "2.0",
            "id": "1",
            "method": "tasks/send",
            "params": {
                "contextId": thread_id,
                "message": {"role": "user", "parts": [{"type": "text", "text": "hi"}]},
            },
        },
    )
    assert resp.status_code == 200, resp.text
    result = resp.json()["result"]
    assert result["status"]["state"] == "TASK_STATE_FAILED"

    run = (
        await session.execute(
            select(runs.c.status, runs.c.metadata).where(runs.c.run_id == result["id"])
        )
    ).mappings().first()
    assert run["status"] == "failed"
    assert run["metadata"]["failureReason"] == "agent_offline"


async def test_a2a_can_never_bypass_a_paused_run_even_with_a_resume_flag(
    client, register, session
):
    """Resuming over A2A is only the bound answer: a message whose
    `taskId` names the thread's paused task. A second tasks/send that
    tries the old metadata.resume=true convention instead must not touch
    the paused run — since upstream's utterance-queue change it becomes a
    *new* run, queued behind the thread's holder (one turn per thread),
    while the interrupt stays exactly where it was, waiting for whoever
    it was actually addressed to.
    """
    served = await register("approver")

    # Built directly via repo, not through a live tasks/send — that would
    # block draining a run nothing ever claims/finishes.
    thread_id = await repo.create_thread(session, served.ref())
    created = await repo.create_run(session, thread_id, served.ref(), "a2a", {})
    await repo.mark_run_status(
        session, created["run_id"], "input-required", metadata={"interrupts": [{"id": "int_1"}]}
    )
    await session.commit()

    second = await client.post(
        f"/a2a/{served.path()}/rpc",
        json={
            "jsonrpc": "2.0",
            "id": "2",
            "method": "tasks/send",
            "params": {
                "id": "task_b",
                "contextId": thread_id,
                "metadata": {"resume": True},
                "message": {"role": "user", "parts": [{"type": "text", "text": "approved"}]},
            },
        },
    )
    assert second.status_code == 200, second.text
    result = second.json()["result"]
    # A *new* run — never merged into the paused one. Nobody serves this
    # agent here, so the new run is recorded failed (agent_offline)
    # rather than queued; either way it is its own task, and the queueing
    # of a served sibling behind the thread's holder is upstream's suite's
    # to prove.
    assert result["id"] != created["run_id"]
    assert result["status"]["state"] == "TASK_STATE_FAILED"

    paused = (
        await session.execute(
            select(runs.c.status, runs.c.metadata).where(runs.c.run_id == created["run_id"])
        )
    ).mappings().first()
    assert paused["status"] == "input-required"
    assert paused["metadata"]["interrupts"] == [{"id": "int_1"}]

    run_count = (await session.execute(select(func.count()).select_from(runs))).scalar()
    assert run_count == 2
