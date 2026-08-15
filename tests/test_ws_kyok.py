"""The WS /ws/kyok relay (souk_server.ws_kyok) — the poll/respond pair's
replacement, and the binding that pair never had.

The round trips mirror what tests/test_kyok_http.py probed over
poll/respond: a provider's /kyok/v1/chat/completions call answered by the
bridge, streaming and not, plus the error path. New here, and the reason
server-mode.md calls the socket a security fix: an answer is accepted only
on the connection its request was delivered to — a second authenticated
socket presenting a valid requestId is refused — and one socket serves
concurrent completions by requestId. The provider side of every test stays
plain HTTP; that endpoint is deliberately untouched.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import time

import httpx
import pytest
from httpx_ws import WebSocketDisconnect, aconnect_ws
from httpx_ws.transport import ASGIWebSocketTransport

from souk import repo
from souk.kyok import issue_kyok_token
from souk_server.server import create_app

RECEIVE_TIMEOUT = 2.0


def _kyok_headers(bearer: str, private_key, body: bytes) -> dict:
    timestamp = str(int(time.time()))
    payload = f"{bearer}:{timestamp}:{hashlib.sha256(body).hexdigest()}".encode()
    return {
        "Authorization": f"Bearer {bearer}",
        "X-Souk-Kyok-Timestamp": timestamp,
        "X-Souk-Kyok-Signature": private_key.sign(payload).hex(),
        "content-type": "application/json",
    }


def _chunk(content: str = "", role: str | None = None, finish_reason: str | None = None) -> dict:
    delta: dict = {}
    if role:
        delta["role"] = role
    if content:
        delta["content"] = content
    return {
        "id": "chatcmpl-1",
        "object": "chat.completion.chunk",
        "created": 1,
        "model": "kyok",
        "choices": [{"index": 0, "delta": delta, "finish_reason": finish_reason}],
    }


async def _register_agent(session, new_identity, name: str = "greeter"):
    identity = new_identity()
    agent_ids = await repo.register_agents(session, identity.public_key, [{"name": name}])
    await session.commit()
    return identity, agent_ids[name]


def _client(souk) -> httpx.AsyncClient:
    # One client, one app: ASGIWebSocketTransport falls through to plain
    # ASGITransport for HTTP, so the provider's completions POST and the
    # bridge's socket exercise the same instance.
    return httpx.AsyncClient(
        transport=ASGIWebSocketTransport(app=create_app(souk)), base_url="http://test"
    )


class _Bridge:
    """One /ws/kyok connection speaking the frame table directly."""

    def __init__(self, ws) -> None:
        self._ws = ws

    async def hello(self, session_id: str) -> None:
        await self._ws.send_text(json.dumps({"type": "hello", "sessionId": session_id}))
        assert (await self.recv()) == {"type": "welcome"}

    async def recv(self) -> dict:
        return json.loads(await self._ws.receive_text(timeout=RECEIVE_TIMEOUT))

    async def send(self, frame: dict) -> None:
        await self._ws.send_text(json.dumps(frame))

    async def answer(self, request_id: str, chunks: list[dict]) -> None:
        for chunk in chunks:
            await self.send({"type": "chunk", "requestId": request_id, "data": chunk})
        await self.send({"type": "done", "requestId": request_id})


# --- handshake ---------------------------------------------------------------


@pytest.mark.parametrize(
    "first_frame",
    [
        {"type": "hello"},  # no sessionId
        {"type": "hello", "sessionId": ""},
        {"type": "chunk", "requestId": "x"},  # anything else before hello
    ],
)
async def test_a_bad_handshake_closes_the_socket(souk, first_frame):
    async with _client(souk) as client:
        async with aconnect_ws("http://test/ws/kyok", client) as ws:
            await ws.send_text(json.dumps(first_frame))
            with pytest.raises(WebSocketDisconnect) as excinfo:
                await ws.receive_text(timeout=RECEIVE_TIMEOUT)
            assert excinfo.value.code == 1008


# --- round trips -------------------------------------------------------------


async def test_full_round_trip_non_streaming(session, souk, new_identity):
    identity, agent_id = await _register_agent(session, new_identity)
    run_id, session_id = "run_ws_nonstream", "sess_ws_nonstream"
    souk.broker.enqueue_run(run_id, agent_id, "thread_1", {}, "ag-ui")
    try:
        token = issue_kyok_token(run_id, session_id, agent_id, "test-signing-secret")
        body = json.dumps({"messages": [{"role": "user", "content": "hi"}]}).encode()

        async with _client(souk) as client:

            async def provider_call():
                resp = await client.post(
                    "/kyok/v1/chat/completions",
                    content=body,
                    headers=_kyok_headers(token, identity._key, body),
                )
                assert resp.status_code == 200, resp.text
                return resp.json()

            async def bridge_relay():
                async with aconnect_ws("http://test/ws/kyok", client) as ws:
                    bridge = _Bridge(ws)
                    await bridge.hello(session_id)
                    request = await bridge.recv()
                    assert request["type"] == "completionRequest"
                    assert request["payload"]["messages"][0]["content"] == "hi"
                    await bridge.answer(
                        request["requestId"],
                        [
                            _chunk(content="hello", role="assistant"),
                            _chunk(content=" world", finish_reason="stop"),
                        ],
                    )

            result, _ = await asyncio.gather(provider_call(), bridge_relay())
        assert result["choices"][0]["message"]["content"] == "hello world"
        assert result["choices"][0]["finish_reason"] == "stop"
    finally:
        souk.broker.forget(run_id)


async def test_full_round_trip_streaming(session, souk, new_identity):
    identity, agent_id = await _register_agent(session, new_identity)
    run_id, session_id = "run_ws_stream", "sess_ws_stream"
    souk.broker.enqueue_run(run_id, agent_id, "thread_1", {}, "ag-ui")
    try:
        token = issue_kyok_token(run_id, session_id, agent_id, "test-signing-secret")
        body = json.dumps({"messages": [], "stream": True}).encode()

        async with _client(souk) as client:

            async def provider_call():
                async with client.stream(
                    "POST",
                    "/kyok/v1/chat/completions",
                    content=body,
                    headers=_kyok_headers(token, identity._key, body),
                ) as resp:
                    assert resp.status_code == 200
                    return [line async for line in resp.aiter_lines() if line]

            async def bridge_relay():
                async with aconnect_ws("http://test/ws/kyok", client) as ws:
                    bridge = _Bridge(ws)
                    await bridge.hello(session_id)
                    request = await bridge.recv()
                    await bridge.answer(
                        request["requestId"],
                        [_chunk(content="hi", role="assistant", finish_reason="stop")],
                    )

            lines, _ = await asyncio.gather(provider_call(), bridge_relay())
        assert lines[-1] == "data: [DONE]"
        assert any("hi" in line for line in lines[:-1])
    finally:
        souk.broker.forget(run_id)


async def test_an_error_frame_fails_the_completion_fast(session, souk, new_identity):
    identity, agent_id = await _register_agent(session, new_identity)
    run_id, session_id = "run_ws_error", "sess_ws_error"
    souk.broker.enqueue_run(run_id, agent_id, "thread_1", {}, "ag-ui")
    try:
        token = issue_kyok_token(run_id, session_id, agent_id, "test-signing-secret")
        body = json.dumps({"messages": [], "stream": True}).encode()

        async with _client(souk) as client:

            async def provider_call():
                async with client.stream(
                    "POST",
                    "/kyok/v1/chat/completions",
                    content=body,
                    headers=_kyok_headers(token, identity._key, body),
                ) as resp:
                    return [line async for line in resp.aiter_lines() if line]

            async def bridge_relay():
                async with aconnect_ws("http://test/ws/kyok", client) as ws:
                    bridge = _Bridge(ws)
                    await bridge.hello(session_id)
                    request = await bridge.recv()
                    await bridge.send(
                        {
                            "type": "error",
                            "requestId": request["requestId"],
                            "message": "upstream LLM call failed",
                        }
                    )

            lines, _ = await asyncio.gather(provider_call(), bridge_relay())
        assert len(lines) == 1
        assert json.loads(lines[0].removeprefix("data: ")) == {"error": "upstream LLM call failed"}
    finally:
        souk.broker.forget(run_id)


async def test_one_socket_multiplexes_concurrent_completions(session, souk, new_identity):
    """requestId multiplexing — strictly better than poll_one's
    one-per-cycle handover: two completions in flight on one socket,
    answered out of order."""
    identity, agent_id = await _register_agent(session, new_identity)
    run_id, session_id = "run_ws_multiplex", "sess_ws_multiplex"
    souk.broker.enqueue_run(run_id, agent_id, "thread_1", {}, "ag-ui")
    try:
        token = issue_kyok_token(run_id, session_id, agent_id, "test-signing-secret")

        async with _client(souk) as client:

            async def provider_call(prompt: str) -> str:
                body = json.dumps({"messages": [{"role": "user", "content": prompt}]}).encode()
                resp = await client.post(
                    "/kyok/v1/chat/completions",
                    content=body,
                    headers=_kyok_headers(token, identity._key, body),
                )
                assert resp.status_code == 200, resp.text
                return resp.json()["choices"][0]["message"]["content"]

            async def bridge_relay():
                async with aconnect_ws("http://test/ws/kyok", client) as ws:
                    bridge = _Bridge(ws)
                    await bridge.hello(session_id)
                    first = await bridge.recv()
                    second = await bridge.recv()
                    # Answer in reverse order of arrival: each answer lands
                    # on its own completion, keyed by requestId.
                    for request in (second, first):
                        prompt = request["payload"]["messages"][0]["content"]
                        await bridge.answer(
                            request["requestId"],
                            [_chunk(content=f"re: {prompt}", role="assistant", finish_reason="stop")],
                        )

            first_answer, second_answer, _ = await asyncio.gather(
                provider_call("one"), provider_call("two"), bridge_relay()
            )
        assert first_answer == "re: one"
        assert second_answer == "re: two"
    finally:
        souk.broker.forget(run_id)


# --- the binding -------------------------------------------------------------


async def test_an_answer_is_only_accepted_on_the_socket_the_request_was_delivered_to(
    session, souk, new_identity
):
    """The security fix itself. A second connection — same session, so it
    would have passed any credential check the socket could make — presents
    a valid requestId it did not receive. Refused with an error frame, and
    the completion still gets its real answer from the socket that holds
    it: requestId is a multiplexing key within a connection, not a bearer
    capability."""
    identity, agent_id = await _register_agent(session, new_identity)
    run_id, session_id = "run_ws_binding", "sess_ws_binding"
    souk.broker.enqueue_run(run_id, agent_id, "thread_1", {}, "ag-ui")
    try:
        token = issue_kyok_token(run_id, session_id, agent_id, "test-signing-secret")
        body = json.dumps({"messages": [{"role": "user", "content": "hi"}]}).encode()

        async with _client(souk) as client:
            async with aconnect_ws("http://test/ws/kyok", client) as holder_ws:
                holder = _Bridge(holder_ws)
                await holder.hello(session_id)

                provider = asyncio.ensure_future(
                    client.post(
                        "/kyok/v1/chat/completions",
                        content=body,
                        headers=_kyok_headers(token, identity._key, body),
                    )
                )
                request = await holder.recv()
                request_id = request["requestId"]

                async with aconnect_ws("http://test/ws/kyok", client) as intruder_ws:
                    intruder = _Bridge(intruder_ws)
                    await intruder.hello(session_id)
                    await intruder.send(
                        {
                            "type": "chunk",
                            "requestId": request_id,
                            "data": _chunk(content="injected", role="assistant", finish_reason="stop"),
                        }
                    )
                    refusal = await intruder.recv()
                    assert refusal["type"] == "error"
                    assert refusal["requestId"] == request_id

                await holder.answer(
                    request_id, [_chunk(content="real", role="assistant", finish_reason="stop")]
                )
                resp = await provider
        assert resp.json()["choices"][0]["message"]["content"] == "real"
        assert "injected" not in resp.text
    finally:
        souk.broker.forget(run_id)


async def test_a_dropped_socket_fails_its_in_flight_completions_fast(
    session, souk, new_identity
):
    """A truncated answer must fail the completion, not complete it — and
    fail it now, not after the claim timeout."""
    identity, agent_id = await _register_agent(session, new_identity)
    run_id, session_id = "run_ws_dropped", "sess_ws_dropped"
    souk.broker.enqueue_run(run_id, agent_id, "thread_1", {}, "ag-ui")
    try:
        token = issue_kyok_token(run_id, session_id, agent_id, "test-signing-secret")
        body = json.dumps({"messages": [], "stream": True}).encode()

        async with _client(souk) as client:

            async def provider_call():
                async with client.stream(
                    "POST",
                    "/kyok/v1/chat/completions",
                    content=body,
                    headers=_kyok_headers(token, identity._key, body),
                ) as resp:
                    return [line async for line in resp.aiter_lines() if line]

            async def bridge_dies_mid_answer():
                async with aconnect_ws("http://test/ws/kyok", client) as ws:
                    bridge = _Bridge(ws)
                    await bridge.hello(session_id)
                    request = await bridge.recv()
                    await bridge.send(
                        {
                            "type": "chunk",
                            "requestId": request["requestId"],
                            "data": _chunk(content="half an ans", role="assistant"),
                        }
                    )
                # the socket drops with the answer unfinished

            async with asyncio.timeout(5):
                lines, _ = await asyncio.gather(provider_call(), bridge_dies_mid_answer())
        assert json.loads(lines[-1].removeprefix("data: ")) == {
            "error": "kyok bridge disconnected mid-response"
        }
    finally:
        souk.broker.forget(run_id)
