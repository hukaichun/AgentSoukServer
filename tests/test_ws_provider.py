"""The WS /ws/provider relay (souk_server.ws_provider) — the properties the
gRPC transport was probed for, carried over, plus what the socket adds.

Same catalogue as the retired gRPC suite (tests/test_event_path_grpc.py,
deleted with the transport) by design: no per-run state in the transport,
a cancel reaching whichever socket the identity has open, and — the
property server-mode.md names as a test to carry over, not a hope — a
dropped socket ending nothing, with a reconnect reporting the rest.
New here: the hello handshake's dual-track auth, and the maxClaim budget
now enforced by the server-side claim loop (finish is the credit).

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
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from httpx_ws import WebSocketDisconnect, aconnect_ws
from httpx_ws.transport import ASGIWebSocketTransport

from souk.identity import registration_signing_payload
from souk_server.server import create_app

RECEIVE_TIMEOUT = 2.0


async def _register(souk, *names: str):
    key = Ed25519PrivateKey.generate()
    public_key = key.public_key().public_bytes_raw().hex()
    timestamp = int(time.time())
    registration = await souk.register_agents(
        public_key,
        key.sign(registration_signing_payload(list(names), timestamp)).hex(),
        timestamp,
        [{"name": n} for n in names],
    )
    return registration, public_key


class _Socket:
    """One /ws/provider connection speaking the frame table directly —
    the ws counterpart of the gRPC tests' _Session."""

    def __init__(self, ws) -> None:
        self._ws = ws

    async def recv(self) -> dict:
        return json.loads(await self._ws.receive_text(timeout=RECEIVE_TIMEOUT))

    async def send(self, frame: dict) -> None:
        await self._ws.send_text(json.dumps(frame))

    async def expect_nothing(self, seconds: float = 0.3) -> None:
        with pytest.raises(TimeoutError):
            await self._ws.receive_text(timeout=seconds)


def _provider_client(souk) -> httpx.AsyncClient:
    return httpx.AsyncClient(transport=ASGIWebSocketTransport(app=create_app(souk)))


def _connect(client: httpx.AsyncClient, **kwargs):
    return aconnect_ws("http://test/ws/provider", client, **kwargs)


async def _handshake(ws, token: str | None, agent_ids: list[str], **hello_extra) -> _Socket:
    socket = _Socket(ws)
    hello: dict = {"type": "hello", "agentIds": agent_ids, **hello_extra}
    if token is not None:
        hello["token"] = token
    await socket.send(hello)
    assert (await socket.recv()) == {"type": "welcome"}
    return socket


# --- handshake / dual-track auth -------------------------------------------


async def test_hello_track_authenticates_without_a_header(souk):
    registration, _ = await _register(souk, "greeter")
    async with _provider_client(souk) as client:
        async with _connect(client) as ws:
            await _handshake(ws, registration.session_token, list(registration.agent_ids.values()))


async def test_header_track_lets_hello_omit_the_token(souk):
    registration, _ = await _register(souk, "greeter")
    async with _provider_client(souk) as client:
        async with _connect(
            client, headers={"Authorization": f"Bearer {registration.session_token}"}
        ) as ws:
            await _handshake(ws, None, list(registration.agent_ids.values()))


@pytest.mark.parametrize(
    "hello",
    [
        {"type": "hello", "token": "not-a-token", "agentIds": []},  # invalid token
        {"type": "hello", "agentIds": []},  # no credential on either track
        {"type": "event", "runId": "x"},  # anything else before hello
    ],
)
async def test_a_bad_handshake_closes_the_socket(souk, hello):
    async with _provider_client(souk) as client:
        async with _connect(client) as ws:
            await ws.send_text(json.dumps(hello))
            with pytest.raises(WebSocketDisconnect) as excinfo:
                await ws.receive_text(timeout=RECEIVE_TIMEOUT)
            assert excinfo.value.code == 1008


async def test_a_header_token_and_a_different_hello_token_close_the_socket(souk):
    registration, _ = await _register(souk, "greeter")
    other, _ = await _register(souk, "other")
    async with _provider_client(souk) as client:
        async with _connect(
            client, headers={"Authorization": f"Bearer {registration.session_token}"}
        ) as ws:
            await ws.send_text(
                json.dumps({"type": "hello", "token": other.session_token, "agentIds": []})
            )
            with pytest.raises(WebSocketDisconnect) as excinfo:
                await ws.receive_text(timeout=RECEIVE_TIMEOUT)
            assert excinfo.value.code == 1008


# --- the worker loop over one socket ----------------------------------------


async def test_the_transport_keeps_no_state_per_run(souk):
    """Three runs pushed over one socket; events go straight to core by
    runId. The run frame carries the RunAgentInput — claiming is the
    hand-over, unchanged — and the transport's only registry (the cancel
    routing table) never learns a run_id.
    """
    registration, public_key = await _register(souk, "greeter")
    agent_id = registration.agent_ids["greeter"]
    handles = [await souk.start_run(agent_id, {"messages": []}) for _ in range(3)]
    streams = {h.run_id: h.events() for h in handles}
    run_ids = {h.run_id for h in handles}

    app = create_app(souk)
    async with httpx.AsyncClient(transport=ASGIWebSocketTransport(app=app)) as client:
        async with _connect(client) as ws:
            socket = await _handshake(ws, registration.session_token, [agent_id])
            claimed = {}
            for _ in range(3):
                frame = await socket.recv()
                assert frame["type"] == "run"
                assert frame["agentId"] == agent_id
                assert frame["input"]["runId"] == frame["runId"]
                claimed[frame["runId"]] = frame
            assert set(claimed) == run_ids

            for run_id in run_ids:
                await socket.send(
                    {"type": "event", "runId": run_id, "event": {"type": "RUN_STARTED", "runId": run_id}}
                )
            for run_id in run_ids:
                async with asyncio.timeout(2):
                    assert (await anext(streams[run_id]))["type"] == "RUN_STARTED"

            # No run_id anywhere in the transport's own state.
            state = repr(app.state.worker_sessions.__dict__)
            assert not any(run_id in state for run_id in run_ids)

            for run_id in run_ids:
                await socket.send({"type": "finish", "runId": run_id})
            async with asyncio.timeout(2):
                while any(souk.broker.get(r) is not None for r in run_ids):
                    await asyncio.sleep(0.01)


async def test_a_cancel_reaches_the_socket_the_identity_has_open(souk):
    registration, public_key = await _register(souk, "greeter")
    agent_id = registration.agent_ids["greeter"]
    handle = await souk.start_run(agent_id, {"messages": []})

    async with _provider_client(souk) as client:
        async with _connect(client) as ws:
            socket = await _handshake(ws, registration.session_token, [agent_id])
            run = await socket.recv()
            assert run["runId"] == handle.run_id

            souk.cancel_run(handle.run_id)
            frame = await socket.recv()
            assert frame == {"type": "cancel", "runId": handle.run_id}

            # A request, not an order: the run is 'cancelling' until its
            # stream actually ends, and the outcome is read off what arrived.
            async with asyncio.timeout(2):
                while (await souk.get_run(handle.run_id))["status"] != "cancelling":
                    await asyncio.sleep(0.01)
            await socket.send({"type": "finish", "runId": handle.run_id})
            async with asyncio.timeout(2):
                while souk.broker.get(handle.run_id) is not None:
                    await asyncio.sleep(0.01)
    assert (await souk.get_run(handle.run_id))["status"] == "cancelled"


@pytest.mark.parametrize("resume_with", ["events", "finish_only"])
async def test_a_dropped_socket_ends_nothing(souk, resume_with):
    """The property server-mode.md orders carried over: souk records no
    outcome at disconnect; a reconnect (fresh hello) reports the rest,
    including how the run ended."""
    registration, public_key = await _register(souk, "greeter")
    agent_id = registration.agent_ids["greeter"]
    handle = await souk.start_run(agent_id, {"messages": []})

    async with _provider_client(souk) as client:
        async with _connect(client) as ws:
            socket = await _handshake(ws, registration.session_token, [agent_id])
            run = await socket.recv()
            assert run["runId"] == handle.run_id
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
        assert (await souk.get_run(handle.run_id))["status"] == "running"

        async with _connect(client) as ws:
            socket = await _handshake(ws, registration.session_token, [agent_id])
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
            async with asyncio.timeout(2):
                while souk.broker.get(handle.run_id) is not None:
                    await asyncio.sleep(0.01)

    # RUN_FINISHED then stream-end is a completion; a bare stream-end is a
    # stop souk never asked for — a failure. Both read off what arrived.
    expected = "completed" if resume_with == "events" else "failed"
    assert (await souk.get_run(handle.run_id))["status"] == expected


async def test_max_claim_budget_and_finish_as_the_credit(souk):
    registration, public_key = await _register(souk, "greeter")
    agent_id = registration.agent_ids["greeter"]
    first = await souk.start_run(agent_id, {"messages": []})
    second = await souk.start_run(agent_id, {"messages": []})

    async with _provider_client(souk) as client:
        async with _connect(client) as ws:
            socket = await _handshake(
                ws, registration.session_token, [agent_id], maxClaim=1
            )
            run = await socket.recv()
            assert run["type"] == "run"
            # Budget exhausted: the second queued run is not pushed…
            await socket.expect_nothing()
            # …until finish returns the credit.
            await socket.send({"type": "finish", "runId": run["runId"]})
            next_run = await socket.recv()
            assert next_run["type"] == "run"
            assert {run["runId"], next_run["runId"]} == {first.run_id, second.run_id}
            await socket.send({"type": "finish", "runId": next_run["runId"]})
            async with asyncio.timeout(2):
                while souk.broker.get(next_run["runId"]) is not None:
                    await asyncio.sleep(0.01)


async def test_work_enqueued_mid_long_poll_is_pushed_promptly(souk):
    """The long-poll branch, now server-side: an idle socket learns about
    new work in roughly one wake, not after sitting out the full wait."""
    registration, _ = await _register(souk, "greeter")
    agent_id = registration.agent_ids["greeter"]

    async with _provider_client(souk) as client:
        async with _connect(client) as ws:
            socket = await _handshake(ws, registration.session_token, [agent_id])
            await asyncio.sleep(0.05)  # let the claim loop enter its wait
            start = time.monotonic()
            handle = await souk.start_run(agent_id, {"messages": []})
            frame = await socket.recv()
            assert frame["runId"] == handle.run_id
            assert time.monotonic() - start < 2
            await socket.send({"type": "finish", "runId": handle.run_id})
            async with asyncio.timeout(2):
                while souk.broker.get(handle.run_id) is not None:
                    await asyncio.sleep(0.01)


async def test_a_frame_for_a_run_this_identity_does_not_hold_gets_an_error_frame(souk):
    """Holding an authenticated socket is not holding the run: core's
    ownership check answers, and the rejection comes back as an error
    frame rather than a closed connection."""
    registration, _ = await _register(souk, "greeter")
    async with _provider_client(souk) as client:
        async with _connect(client) as ws:
            socket = await _handshake(ws, registration.session_token, [])
            await socket.send(
                {"type": "event", "runId": "run_nobody", "event": {"type": "RUN_STARTED"}}
            )
            frame = await socket.recv()
            assert frame["type"] == "error"
            assert frame["runId"] == "run_nobody"
