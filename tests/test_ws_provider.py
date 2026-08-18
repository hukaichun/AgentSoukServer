"""WS /ws/provider — the socket a provider connects out on.

Half of what this file used to assert was about a claim loop that no longer
exists on either side. souk hands work over now: it finds whoever serves an
agent, offers each run, and the ack frame that comes back is the answer. So
the maxClaim budget test is gone (capacity is `maxConcurrentRuns`, declared
once at hello and enforced by souk's own bucket, not by a credit this
transport counts) and so is the long-poll promptness test (there is no poll
to be prompt about — an enqueued run is written to the socket).

What survives is the catalogue server-mode.md orders carried over, because
it is about the transport rather than the direction: no per-run state here,
a cancel reaching whichever socket the identity has open, and a dropped
socket ending nothing, with a reconnect reporting the rest. New: the
signed handshake, and the ack — including a *declined* one, which is the
only way a provider says "full" now that nothing counts for it.

Driven through the real ASGI app over httpx-ws, same event loop as the
souk fixture — the broker's queues are loop-bound, so a threaded test
client would be driving them cross-loop.
"""

from __future__ import annotations

import asyncio
import json
import time

import httpx
import pytest
from httpx_ws import WebSocketDisconnect, aconnect_ws
from httpx_ws.transport import ASGIWebSocketTransport

from souk import repo
from souk.config import CoreSettings
from souk.core import Souk
from souk.identity import verify_signature
from souk_server import ws_provider
from souk_server.handshake import souk_challenge_payload
from souk_server.server import create_app

from tests.conftest import DATABASE_URL

RECEIVE_TIMEOUT = 2.0


class _Socket:
    """One /ws/provider connection speaking the frame table directly."""

    def __init__(self, ws) -> None:
        self._ws = ws

    async def recv(self) -> dict:
        return json.loads(await self._ws.receive_text(timeout=RECEIVE_TIMEOUT))

    async def send(self, frame: dict) -> None:
        await self._ws.send_text(json.dumps(frame))

    async def take(self, run_id: str | None = None) -> dict:
        """Receive a run offer and accept it — the two halves are never
        apart in a real provider, and an offer left unacked stalls the
        broker for ACK_TIMEOUT_SECONDS."""
        frame = await self.recv()
        assert frame["type"] == "run", frame
        if run_id is not None:
            assert frame["runId"] == run_id
        await self.send({"type": "ack", "runId": frame["runId"], "accepted": True})
        return frame

    async def expect_nothing(self, seconds: float = 0.3) -> None:
        with pytest.raises(TimeoutError):
            await self._ws.receive_text(timeout=seconds)


def _provider_client(souk) -> httpx.AsyncClient:
    return httpx.AsyncClient(transport=ASGIWebSocketTransport(app=create_app(souk)))


def _connect(client: httpx.AsyncClient, **kwargs):
    return aconnect_ws("http://test/ws/provider", client, **kwargs)


async def _handshake(ws, identity, names: list[str], **hello_extra) -> _Socket:
    """All four frames, the way a real provider does them.

    The hello is serialized once and that exact text is what the proof
    signs a digest of — re-encoding the dict here would be the test
    agreeing with itself while disagreeing with the wire.
    """
    socket = _Socket(ws)
    hello = identity.hello(names, **hello_extra)
    hello_raw = json.dumps(hello)
    await ws.send_text(hello_raw)

    challenge = await socket.recv()
    assert challenge["type"] == "challenge", challenge
    await socket.send(identity.proof(hello_raw, hello["nonce"], challenge["nonce"]))

    assert (await socket.recv()) == {"type": "welcome"}
    return socket


async def _drain(souk, *run_ids: str) -> None:
    async with asyncio.timeout(2):
        while any(souk.broker.get(r) is not None for r in run_ids):
            await asyncio.sleep(0.01)


async def _claimed(souk, run_id: str) -> None:
    """Wait until souk has recorded the provider as holding this run.

    Sending the ack frame is not the same moment as souk processing it —
    the frame has a socket, a read loop and a future to cross first — and a
    test that treats them as one is testing its own timing. It matters for
    cancel specifically: a cancel arriving while `claimed_by` is still None
    is, correctly, a cancel of a run nobody has, and souk answers it without
    telling anybody.
    """
    async with asyncio.timeout(2):
        while (snapshot := souk.broker.get(run_id)) is None or snapshot.claimed_by is None:
            await asyncio.sleep(0.01)


# --- the signed handshake ---------------------------------------------------


async def test_a_mutual_challenge_response_opens_the_socket(souk, register):
    served = await register("greeter")
    async with _provider_client(souk) as client:
        async with _connect(client) as ws:
            await _handshake(ws, served.identity, ["greeter"])


async def test_souk_signs_the_provider_nonce_before_the_provider_signs_anything(souk, register):
    """The order is the security property, not an implementation detail: a
    provider must be able to walk away from a souk it does not recognise
    *before* producing anything worth stealing. So the challenge — souk's
    own key, and its signature over the nonce we chose — has to arrive
    while this side has still sent nothing but a nonce."""
    served = await register("greeter")
    async with _provider_client(souk) as client:
        async with _connect(client) as ws:
            socket = _Socket(ws)
            hello = served.identity.hello(["greeter"])
            await ws.send_text(json.dumps(hello))

            challenge = await socket.recv()

            assert challenge["type"] == "challenge"
            assert challenge["soukPublicKey"] == souk.identity_public_key
            assert verify_signature(
                challenge["soukPublicKey"],
                challenge["signature"],
                souk_challenge_payload(hello["nonce"], challenge["nonce"]),
            )


async def test_a_captured_handshake_does_not_open_a_second_socket(souk, register):
    """The defect this replaced, asserted as a property rather than
    assumed gone. Every frame of a complete, successful handshake is
    replayed verbatim onto a fresh connection — which is exactly what
    anything that observed one holds — and it must not attach.

    Under the old self-signed shape this passed: the provider composed its
    own statement, so a copy of it worked for whoever held it, for the
    whole freshness window.
    """
    served = await register("greeter")
    async with _provider_client(souk) as client:
        async with _connect(client) as ws:
            socket = _Socket(ws)
            hello_raw = json.dumps(served.identity.hello(["greeter"]))
            await ws.send_text(hello_raw)
            challenge = await socket.recv()
            proof = served.identity.proof(
                hello_raw, json.loads(hello_raw)["nonce"], challenge["nonce"]
            )
            await socket.send(proof)
            assert (await socket.recv()) == {"type": "welcome"}

        # Same hello, same proof, a new connection. souk chooses a new
        # nonce, so the recorded proof answers a question nobody asked.
        async with _connect(client) as ws:
            socket = _Socket(ws)
            await ws.send_text(hello_raw)
            replayed_challenge = await socket.recv()
            assert replayed_challenge["nonce"] != challenge["nonce"]
            await socket.send(proof)
            with pytest.raises(WebSocketDisconnect) as excinfo:
                await ws.receive_text(timeout=RECEIVE_TIMEOUT)
            assert excinfo.value.code == 1008


async def test_the_proof_is_bound_to_the_claims_the_hello_made(souk, register):
    """`sha256(hello)` is inside what the provider signs, so the claims
    cannot be edited in flight. Here the hello on the wire asks for two
    agents while the proof was computed over a hello asking for one —
    which is what a middlebox adding an agent name would produce."""
    served = await register("greeter", "translator")
    honest = served.identity.hello(["greeter"])
    honest_raw = json.dumps(honest)
    tampered_raw = json.dumps({**honest, "agentNames": ["greeter", "translator"]})

    async with _provider_client(souk) as client:
        async with _connect(client) as ws:
            socket = _Socket(ws)
            await ws.send_text(tampered_raw)
            challenge = await socket.recv()
            # Signed over the hello that was *not* sent.
            await socket.send(
                served.identity.proof(honest_raw, honest["nonce"], challenge["nonce"])
            )
            with pytest.raises(WebSocketDisconnect) as excinfo:
                await ws.receive_text(timeout=RECEIVE_TIMEOUT)
            assert excinfo.value.code == 1008


async def test_a_souk_signature_cannot_be_presented_as_a_provider_proof(souk, register):
    """What the `souk:`/`provider:` prefixes are for. Both sides sign the
    same two nonces; without the prefixes the two payloads would differ
    only by a trailing digest, and a souk that could be induced to sign
    would be handing out material for the other direction."""
    served = await register("greeter")
    async with _provider_client(souk) as client:
        async with _connect(client) as ws:
            socket = _Socket(ws)
            hello = served.identity.hello(["greeter"])
            await ws.send_text(json.dumps(hello))
            challenge = await socket.recv()
            await socket.send({"type": "proof", "signature": challenge["signature"]})
            with pytest.raises(WebSocketDisconnect) as excinfo:
                await ws.receive_text(timeout=RECEIVE_TIMEOUT)
            assert excinfo.value.code == 1008


async def test_a_souk_without_an_identity_says_so_rather_than_failing(register):
    """An unconfigured souk cannot prove itself, which is what every souk
    did before this existed. It reports that honestly — a null key, no
    signature — and leaves the decision to the provider, whose pin is the
    thing that turns it into a refusal."""
    unconfigured = Souk(CoreSettings(database_url=DATABASE_URL, token_signing_secret="x"))
    assert unconfigured.identity_public_key is None
    served = await register("greeter")
    async with httpx.AsyncClient(
        transport=ASGIWebSocketTransport(app=create_app(unconfigured))
    ) as client:
        async with _connect(client) as ws:
            socket = _Socket(ws)
            await ws.send_text(json.dumps(served.identity.hello(["greeter"])))
            challenge = await socket.recv()
            assert challenge["soukPublicKey"] is None
            assert challenge["signature"] is None


@pytest.mark.parametrize(
    "mangle",
    [
        pytest.param(lambda h: {k: v for k, v in h.items() if k != "version"}, id="no-version"),
        pytest.param(lambda h: {**h, "version": 99}, id="unsupported-version"),
        pytest.param(lambda h: {**h, "agentNames": []}, id="no-agent-names"),
        pytest.param(lambda h: {k: v for k, v in h.items() if k != "publicKey"}, id="no-public-key"),
        pytest.param(lambda h: {k: v for k, v in h.items() if k != "nonce"}, id="no-nonce"),
        pytest.param(lambda h: {"type": "event", "runId": "x"}, id="anything-before-hello"),
    ],
)
async def test_a_bad_hello_closes_the_socket_before_souk_signs_anything(souk, register, mangle):
    """Refused at the hello, which is *before* souk produces a signature —
    so a malformed frame can never make souk sign over bytes an attacker
    chose."""
    served = await register("greeter")
    async with _provider_client(souk) as client:
        async with _connect(client) as ws:
            await ws.send_text(json.dumps(mangle(served.identity.hello(["greeter"]))))
            with pytest.raises(WebSocketDisconnect) as excinfo:
                await ws.receive_text(timeout=RECEIVE_TIMEOUT)
            assert excinfo.value.code == 1008


async def test_the_old_two_frame_handshake_is_refused_by_name(souk, register):
    """A hard cutover, and the error says which side is behind. A provider
    on the old shape sends no `version` at all, so it cannot be told apart
    from a corrupt frame by anything except that absence — and a bare
    signature failure is what an attack looks like too, which would send
    whoever is debugging it somewhere unhelpful."""
    served = await register("greeter")
    async with _provider_client(souk) as client:
        async with _connect(client) as ws:
            await ws.send_text(
                json.dumps(
                    {
                        "type": "hello",
                        "publicKey": served.public_key,
                        "signature": "00" * 64,
                        "timestamp": int(time.time()),
                        "agentNames": ["greeter"],
                    }
                )
            )
            with pytest.raises(WebSocketDisconnect) as excinfo:
                await ws.receive_text(timeout=RECEIVE_TIMEOUT)
            assert excinfo.value.code == 1008
            assert "version" in excinfo.value.reason


@pytest.mark.parametrize(
    "bad_proof",
    [
        pytest.param({"type": "proof", "signature": "00" * 64}, id="does-not-verify"),
        pytest.param({"type": "proof"}, id="no-signature"),
        pytest.param({"type": "welcome"}, id="wrong-frame"),
    ],
)
async def test_a_bad_proof_closes_the_socket(souk, register, bad_proof):
    served = await register("greeter")
    async with _provider_client(souk) as client:
        async with _connect(client) as ws:
            socket = _Socket(ws)
            await ws.send_text(json.dumps(served.identity.hello(["greeter"])))
            await socket.recv()
            await socket.send(bad_proof)
            with pytest.raises(WebSocketDisconnect) as excinfo:
                await ws.receive_text(timeout=RECEIVE_TIMEOUT)
            assert excinfo.value.code == 1008


async def test_a_name_this_key_never_registered_is_refused_at_the_door(souk, register):
    """Registration is the prerequisite and core enforces it, so the socket
    closes here rather than the agent being advertised and served by
    nobody. The handshake itself is perfectly valid — the proof covers the
    names being claimed, which is exactly the claim being rejected."""
    served = await register("greeter")
    async with _provider_client(souk) as client:
        async with _connect(client) as ws:
            socket = _Socket(ws)
            hello = served.identity.hello(["greeter", "smuggled"])
            hello_raw = json.dumps(hello)
            await ws.send_text(hello_raw)
            challenge = await socket.recv()
            await socket.send(
                served.identity.proof(hello_raw, hello["nonce"], challenge["nonce"])
            )
            with pytest.raises(WebSocketDisconnect) as excinfo:
                await ws.receive_text(timeout=RECEIVE_TIMEOUT)
            assert excinfo.value.code == 1008


# --- runs over one socket ---------------------------------------------------


async def test_attaching_is_what_puts_an_agent_online(souk, register, client):
    served = await register("greeter")
    roster = (await client.get("/agents")).json()["agents"]
    assert [a["online"] for a in roster] == [False]

    async with _provider_client(souk) as ws_client:
        async with _connect(ws_client) as ws:
            await _handshake(ws, served.identity, ["greeter"])
            roster = (await client.get("/agents")).json()["agents"]
            assert [a["online"] for a in roster] == [True]

    # And a dropped socket takes it offline at once — no window to wait out.
    async with asyncio.timeout(2):
        while (await client.get("/agents")).json()["agents"][0]["online"]:
            await asyncio.sleep(0.01)


async def test_the_transport_keeps_no_state_per_run(souk, register):
    """Three runs offered over one socket; events go straight to core by
    runId. The transport's only per-run state is the acks it is waiting on,
    and once each run is accepted even that is empty.
    """
    served = await register("greeter")
    app = create_app(souk)
    async with httpx.AsyncClient(transport=ASGIWebSocketTransport(app=app)) as client:
        async with _connect(client) as ws:
            socket = await _handshake(ws, served.identity, ["greeter"])
            # Started *after* the handshake: with push delivery there is
            # nobody to offer a run to until somebody is attached.
            handles = [await souk.start_run(served.ref(), {"messages": []}) for _ in range(3)]
            streams = {h.run_id: h.events() for h in handles}
            run_ids = {h.run_id for h in handles}

            offered = {}
            for _ in range(3):
                frame = await socket.take()
                assert frame["agentName"] == "greeter"
                assert frame["input"]["runId"] == frame["runId"]
                offered[frame["runId"]] = frame
            assert set(offered) == run_ids

            threads = {h.run_id: h.thread_id for h in handles}
            for run_id in run_ids:
                await socket.send(
                    {
                        "type": "event",
                        "runId": run_id,
                        "event": {
                            "type": "RUN_STARTED",
                            "runId": run_id,
                            "threadId": threads[run_id],
                        },
                    }
                )
            for run_id in run_ids:
                async with asyncio.timeout(2):
                    assert (await anext(streams[run_id]))["type"] == "RUN_STARTED"

            for run_id in run_ids:
                await socket.send({"type": "finish", "runId": run_id})
            await _drain(souk, *run_ids)


async def test_a_reasoned_decline_fails_the_run_with_the_reason_recorded(souk, register):
    """The #18 wire half meeting AgentSouk#65's port: an ack that says no
    *with a reason* is a permanent refusal. souk fails the run, records
    the provider's words verbatim in failureReason, and does not offer it
    again — the run that used to sit `queued` forever while the reason
    lived in a log on somebody else's machine now says what happened,
    where the caller looks."""
    served = await register("greeter")
    async with _provider_client(souk) as client:
        async with _connect(client) as ws:
            socket = await _handshake(ws, served.identity, ["greeter"])
            handle = await souk.start_run(served.ref(), {"messages": []})

            first = await socket.recv()
            assert first["type"] == "run"
            await socket.send(
                {
                    "type": "ack",
                    "runId": first["runId"],
                    "accepted": False,
                    "reason": "input does not validate as RunAgentInput: probe",
                }
            )
            # Permanently refused: failed now, not re-offered on reconnect.
            async with asyncio.timeout(2):
                while (await souk.get_run(handle.run_id)).status != "failed":
                    await asyncio.sleep(0.01)
            await socket.expect_nothing()

        stored = await souk.get_run(handle.run_id)
        assert stored.metadata["failureReason"] == (
            "input does not validate as RunAgentInput: probe"
        )
        assert souk.broker.get(handle.run_id) is None

        async with _connect(client) as ws:
            socket = await _handshake(ws, served.identity, ["greeter"])
            await socket.expect_nothing()


async def test_a_declined_offer_costs_the_run_nothing(souk, register):
    """Saying no is how a full provider says so, and the run stays souk's.

    What it does *not* do is come straight back. souk waits for something
    to change before offering again — a run arriving, a provider
    registering, a run ending — because asking again immediately is asking
    a provider that just said no, with nothing about the answer having
    changed. Reconnecting is one of those changes, and it is the one a real
    provider makes, so that is what this drives.
    """
    served = await register("greeter")
    async with _provider_client(souk) as client:
        async with _connect(client) as ws:
            socket = await _handshake(ws, served.identity, ["greeter"])
            handle = await souk.start_run(served.ref(), {"messages": []})

            first = await socket.recv()
            assert first["type"] == "run"
            await socket.send({"type": "ack", "runId": first["runId"], "accepted": False})
            await socket.expect_nothing()

        # Declined, not failed: souk still holds it, unclaimed.
        snapshot = souk.broker.get(handle.run_id)
        assert snapshot is not None and snapshot.claimed_by is None
        assert (await souk.get_run(handle.run_id)).status == "queued"

        async with _connect(client) as ws:
            socket = await _handshake(ws, served.identity, ["greeter"])
            again = await socket.take(handle.run_id)
            assert again["runId"] == first["runId"]
            await socket.send({"type": "finish", "runId": handle.run_id})
            await _drain(souk, handle.run_id)


async def test_a_cancel_reaches_the_socket_the_identity_has_open(souk, register):
    served = await register("greeter")
    async with _provider_client(souk) as client:
        async with _connect(client) as ws:
            socket = await _handshake(ws, served.identity, ["greeter"])
            handle = await souk.start_run(served.ref(), {"messages": []})
            await socket.take(handle.run_id)
            await _claimed(souk, handle.run_id)

            souk.cancel_run(handle.run_id)
            assert (await socket.recv()) == {"type": "cancel", "runId": handle.run_id}

            # A request, not an order: the run is 'cancelling' until its
            # stream actually ends, and the outcome is read off what arrived.
            async with asyncio.timeout(2):
                while (await souk.get_run(handle.run_id)).status != "cancelling":
                    await asyncio.sleep(0.01)
            await socket.send({"type": "finish", "runId": handle.run_id})
            await _drain(souk, handle.run_id)
    assert (await souk.get_run(handle.run_id)).status == "cancelled"


@pytest.mark.parametrize("resume_with", ["events", "finish_only"])
async def test_a_dropped_socket_ends_nothing(souk, register, resume_with):
    """The property server-mode.md orders carried over: souk records no
    outcome at disconnect; a reconnect (fresh hello) reports the rest,
    including how the run ended."""
    served = await register("greeter")

    async with _provider_client(souk) as client:
        async with _connect(client) as ws:
            socket = await _handshake(ws, served.identity, ["greeter"])
            handle = await souk.start_run(served.ref(), {"messages": []})
            await socket.take(handle.run_id)
            await socket.send(
                {
                    "type": "event",
                    "runId": handle.run_id,
                    "event": {
                        "type": "RUN_STARTED",
                        "runId": handle.run_id,
                        "threadId": handle.thread_id,
                    },
                }
            )
            async with asyncio.timeout(2):
                while not await souk.get_run_events(handle.run_id):
                    await asyncio.sleep(0.01)
        # the socket drops mid-run

        assert souk.broker.get(handle.run_id) is not None
        assert (await souk.get_run(handle.run_id)).status == "running"

        async with _connect(client) as ws:
            socket = await _handshake(ws, served.identity, ["greeter"])
            if resume_with == "events":
                await socket.send(
                    {
                        "type": "event",
                        "runId": handle.run_id,
                        "event": {
                            "type": "RUN_FINISHED",
                            "runId": handle.run_id,
                            "threadId": handle.thread_id,
                        },
                    }
                )
                async with asyncio.timeout(2):
                    while not any(
                        e["type"] == "RUN_FINISHED"
                        for e in await souk.get_run_events(handle.run_id)
                    ):
                        await asyncio.sleep(0.01)
            await socket.send({"type": "finish", "runId": handle.run_id})
            await _drain(souk, handle.run_id)

    # RUN_FINISHED then stream-end is a completion; a bare stream-end is a
    # stop souk never asked for — a failure. Both read off what arrived.
    expected = "completed" if resume_with == "events" else "failed"
    assert (await souk.get_run(handle.run_id)).status == expected


async def test_max_concurrent_runs_is_declared_at_hello_and_souk_honours_it(souk, register):
    """Capacity is a fact about the provider, stated once, and souk sizes
    its own bucket from it — nothing here counts anything, which is exactly
    what the old in-transport claim budget was doing."""
    served = await register("greeter")
    async with _provider_client(souk) as client:
        async with _connect(client) as ws:
            socket = await _handshake(ws, served.identity, ["greeter"], maxConcurrentRuns=1)
            first = await souk.start_run(served.ref(), {"messages": []})
            second = await souk.start_run(served.ref(), {"messages": []})

            offered = await socket.take()
            # Capacity is spent: the second queued run is not offered…
            await socket.expect_nothing()
            # …until this one finishes and returns it.
            await socket.send({"type": "finish", "runId": offered["runId"]})
            next_run = await socket.take()
            assert {offered["runId"], next_run["runId"]} == {first.run_id, second.run_id}
            await socket.send({"type": "finish", "runId": next_run["runId"]})
            await _drain(souk, first.run_id, second.run_id)


async def test_a_run_enqueued_on_a_live_socket_is_offered_promptly(souk, register):
    """What the long-poll test became. There is no poll interval to beat
    any more — the offer is a write to a socket souk already holds."""
    served = await register("greeter")
    async with _provider_client(souk) as client:
        async with _connect(client) as ws:
            socket = await _handshake(ws, served.identity, ["greeter"])
            await asyncio.sleep(0.05)
            start = time.monotonic()
            handle = await souk.start_run(served.ref(), {"messages": []})
            await socket.take(handle.run_id)
            assert time.monotonic() - start < 2
            await socket.send({"type": "finish", "runId": handle.run_id})
            await _drain(souk, handle.run_id)


async def test_a_frame_for_a_run_this_identity_does_not_hold_gets_an_error_frame(souk, register):
    """Holding an authenticated socket is not holding the run: core's
    ownership check answers, and the rejection comes back as an error
    frame rather than a closed connection."""
    served = await register("greeter")
    async with _provider_client(souk) as client:
        async with _connect(client) as ws:
            socket = await _handshake(ws, served.identity, ["greeter"])
            await socket.send(
                {"type": "event", "runId": "run_nobody", "event": {"type": "RUN_STARTED"}}
            )
            frame = await socket.recv()
            assert frame["type"] == "error"
            assert frame["runId"] == "run_nobody"


# --- a registration that vanishes under a live socket -----------------------


async def test_a_provider_whose_agents_vanished_is_closed_so_it_re_registers(
    souk, register, client, monkeypatch
):
    """The failure PR #4 fixed, checked against the new design rather than
    assumed dead with the claim loop.

    It is not dead. souk validates registration once, at `attach_provider`,
    and the broker then holds the mapping in memory — so a registration that
    disappears underneath a live socket (a restored database, a de-listing, a
    souk redeployed against a fresh one) leaves the broker serving an agent
    souk's own roster no longer lists. Nothing routes to it, because
    `resolve_ref` cannot find it; nothing complains, because the socket is
    fine. A healthy container, an invisible agent, indefinitely — which is
    exactly the shape of the original incident.

    Closing is the repair, not a punishment: registration is what puts the
    name back, the SDK re-registers on every reconnect, so a provider told
    goodbye here returns listed.
    """
    monkeypatch.setattr(ws_provider, "OWNERSHIP_RECHECK_SECONDS", 0.05)
    served = await register("greeter")
    async with _provider_client(souk) as ws_client:
        async with _connect(ws_client) as ws:
            socket = await _handshake(ws, served.identity, ["greeter"])
            assert (await client.get("/agents")).json()["agents"][0]["online"] is True

            async with souk.engine.begin() as conn:
                await conn.exec_driver_sql("DELETE FROM agents")
            assert (await client.get("/agents")).json()["agents"] == []

            with pytest.raises(WebSocketDisconnect) as excinfo:
                await socket._ws.receive_text(timeout=RECEIVE_TIMEOUT)
            assert excinfo.value.code == 1008


# --- queries: the first frame on this wire that expects an answer -----------


async def _query(socket: _Socket, **params) -> dict:
    await socket.send(
        {
            "type": "query",
            "queryId": "q1",
            "method": "thread_messages",
            "params": params,
        }
    )
    frame = await socket.recv()
    assert frame["type"] == "queryResult", frame
    assert frame["queryId"] == "q1"
    return frame


async def test_a_provider_can_ask_for_the_history_its_run_input_never_carried(
    souk, register, client
):
    """The gap the query exists for. A provider sees exactly what the
    *caller* sent for its run: an AG-UI client resends its whole history
    every turn, A2A's `message/send` carries one message, and the same
    agent cannot tell a tenth turn from a first. souk has held the thread
    all along, and this is how it says so.
    """
    served = await register("greeter")
    thread_id = await souk.create_thread(served.ref())
    async with souk.session() as session:
        run = await repo.create_run(session, thread_id, served.ref(), "ag-ui", {})
        await repo.append_thread_messages(
            session,
            thread_id,
            run["run_id"],
            [{"role": "user", "content": "one"}, {"role": "assistant", "content": "two"}],
        )
        await session.commit()

    async with _provider_client(souk) as ws_client:
        async with _connect(ws_client) as ws:
            socket = await _handshake(ws, served.identity, ["greeter"])
            answer = await _query(socket, threadId=thread_id)

            assert [m["content"] for m in answer["result"]] == ["one", "two"]


async def test_limit_is_applied_by_souk_not_by_the_caller(souk, register):
    """The parameter exists to keep the response frame bounded. Applied on
    the way back it would bound nothing — a months-old thread would already
    have crossed the wire to be trimmed."""
    served = await register("greeter")
    thread_id = await souk.create_thread(served.ref())
    async with souk.session() as session:
        run = await repo.create_run(session, thread_id, served.ref(), "ag-ui", {})
        await repo.append_thread_messages(
            session,
            thread_id,
            run["run_id"],
            [{"role": "user", "content": str(i)} for i in range(6)],
        )
        await session.commit()

    async with _provider_client(souk) as ws_client:
        async with _connect(ws_client) as ws:
            socket = await _handshake(ws, served.identity, ["greeter"])
            answer = await _query(socket, threadId=thread_id, limit=2)

            # The most recent, because context is wanted from the recent end.
            assert [m["content"] for m in answer["result"]] == ["4", "5"]


async def test_a_provider_cannot_read_a_thread_that_is_not_its_own(souk, register):
    """Not in the upstream design, and added here. Thread ids are not
    guessable, but unguessable is not an authorization rule: a provider
    that served one run knows that thread id permanently, and would
    otherwise keep reading the conversation after being de-listed or after
    the agent moved to another stall.

    The refusal is the same as for a thread that does not exist — telling
    them apart would confirm a thread's existence to somebody who may not
    read it, which is the whole of what the check is for.
    """
    mine = await register("greeter")
    theirs = await register("greeter")
    their_thread = await souk.create_thread(theirs.ref())

    async with _provider_client(souk) as ws_client:
        async with _connect(ws_client) as ws:
            socket = await _handshake(ws, mine.identity, ["greeter"])

            refused = await _query(socket, threadId=their_thread)
            missing = await _query(socket, threadId="thread_does_not_exist")

            assert "result" not in refused
            assert refused["error"] == missing["error"]


@pytest.mark.parametrize(
    "params,expected",
    [
        pytest.param({}, "threadId", id="no-thread-id"),
        pytest.param({"threadId": "t", "limit": 0}, "limit", id="zero-limit"),
        pytest.param({"threadId": "t", "limit": "5"}, "limit", id="non-integer-limit"),
    ],
)
async def test_a_malformed_query_is_answered_not_dropped(souk, register, params, expected):
    """Answered on the same queryId, because the far side is waiting on
    exactly that: a query that gets no reply is a caller blocked until its
    timeout, for a mistake souk could see immediately."""
    served = await register("greeter")
    async with _provider_client(souk) as ws_client:
        async with _connect(ws_client) as ws:
            socket = await _handshake(ws, served.identity, ["greeter"])
            answer = await _query(socket, **params)
            assert expected in answer["error"]


async def test_an_unknown_query_method_is_refused_by_name(souk, register):
    served = await register("greeter")
    async with _provider_client(souk) as ws_client:
        async with _connect(ws_client) as ws:
            socket = await _handshake(ws, served.identity, ["greeter"])
            await socket.send({"type": "query", "queryId": "q9", "method": "list_agents"})
            frame = await socket.recv()
            assert frame["queryId"] == "q9"
            assert "list_agents" in frame["error"]


def test_the_wire_carries_every_query_the_link_declares():
    """`contract.LINK_QUERY_METHODS` is upstream's list of what a provider
    may ask. A method added there without a frame here would compile, pass
    every test, and fail at a provider — so this is the one place the two
    are compared."""
    from souk_provider_sdk.contract import LINK_QUERY_METHODS

    assert set(ws_provider.QUERY_METHODS) == set(LINK_QUERY_METHODS)
