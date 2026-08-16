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

from souk_server.server import create_app

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
    socket = _Socket(ws)
    await socket.send(identity.hello(names, **hello_extra))
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


async def test_a_signature_over_the_connect_payload_opens_the_socket(souk, register):
    served = await register("greeter")
    async with _provider_client(souk) as client:
        async with _connect(client) as ws:
            await _handshake(ws, served.identity, ["greeter"])


async def test_nothing_bearer_shaped_is_accepted_in_its_place(souk, register):
    """There is no token track any more, so the frame that used to be a
    complete, valid hello — a session token and a list of ids — is now just
    a hello with no signature in it."""
    served = await register("greeter")
    async with _provider_client(souk) as client:
        async with _connect(client) as ws:
            await ws.send_text(
                json.dumps({"type": "hello", "token": "looks-like-auth", "agentNames": ["greeter"]})
            )
            with pytest.raises(WebSocketDisconnect) as excinfo:
                await ws.receive_text(timeout=RECEIVE_TIMEOUT)
            assert excinfo.value.code == 1008


@pytest.mark.parametrize(
    "mangle",
    [
        pytest.param(lambda h: {**h, "signature": "00" * 64}, id="signature-does-not-verify"),
        pytest.param(lambda h: {**h, "timestamp": int(time.time()) - 86400}, id="stale-timestamp"),
        pytest.param(lambda h: {**h, "agentNames": []}, id="no-agent-names"),
        pytest.param(lambda h: {k: v for k, v in h.items() if k != "publicKey"}, id="no-public-key"),
        pytest.param(lambda h: {"type": "event", "runId": "x"}, id="anything-before-hello"),
    ],
)
async def test_a_bad_handshake_closes_the_socket(souk, register, mangle):
    served = await register("greeter")
    async with _provider_client(souk) as client:
        async with _connect(client) as ws:
            await ws.send_text(json.dumps(mangle(served.identity.hello(["greeter"]))))
            with pytest.raises(WebSocketDisconnect) as excinfo:
                await ws.receive_text(timeout=RECEIVE_TIMEOUT)
            assert excinfo.value.code == 1008


async def test_a_name_this_key_never_registered_is_refused_at_the_door(souk, register):
    """Registration is the prerequisite and core enforces it, so the socket
    closes here rather than the agent being advertised and served by
    nobody. The signature is perfectly valid — it covers the name being
    claimed, which is exactly the claim being rejected."""
    served = await register("greeter")
    async with _provider_client(souk) as client:
        async with _connect(client) as ws:
            await ws.send_text(json.dumps(served.identity.hello(["greeter", "smuggled"])))
            with pytest.raises(WebSocketDisconnect) as excinfo:
                await ws.receive_text(timeout=RECEIVE_TIMEOUT)
            assert excinfo.value.code == 1008


async def test_a_signature_for_one_name_set_does_not_open_a_socket_for_another(souk, register):
    """The names are inside what was signed, so a hello cannot claim a set
    the signature did not cover — even when every name in it is one this
    provider really registered."""
    served = await register("greeter", "translator")
    hello = served.identity.hello(["greeter"])
    async with _provider_client(souk) as client:
        async with _connect(client) as ws:
            await ws.send_text(json.dumps({**hello, "agentNames": ["greeter", "translator"]}))
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

            for run_id in run_ids:
                await socket.send(
                    {"type": "event", "runId": run_id, "event": {"type": "RUN_STARTED", "runId": run_id}}
                )
            for run_id in run_ids:
                async with asyncio.timeout(2):
                    assert (await anext(streams[run_id]))["type"] == "RUN_STARTED"

            for run_id in run_ids:
                await socket.send({"type": "finish", "runId": run_id})
            await _drain(souk, *run_ids)


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
                    "event": {"type": "RUN_STARTED", "runId": handle.run_id},
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
                        "event": {"type": "RUN_FINISHED", "runId": handle.run_id},
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
