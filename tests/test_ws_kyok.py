"""The WS /ws/kyok relay (souk_server.ws_kyok) — the socket an LLM
provider serves completions over, since upstream made the answering party
a first-class provider kind instead of an anonymous session-keyed bridge.

The round trips mirror what the old bridge socket carried: a provider's
/kyok/v1/chat/completions call answered over the socket, streaming and
not, plus the error path and requestId multiplexing. What changed is who
is on the socket — an Ed25519-identified LLM provider that registered its
offerings and attached, through the same four-frame mutual handshake as
/ws/provider — and what the completionRequest frame carries: the run,
the proven calling agent, the addressed model, and the caller's context.

What deliberately did not change: an answer is accepted only on the
connection its request was delivered to. A second authenticated socket —
same identity, same offering, so it passes every credential check there
is — presenting a valid requestId it was never delivered is refused.

The agent-provider side of every test stays plain HTTP; that endpoint is
deliberately untouched.
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

from souk.kyok import KyokBinding, issue_kyok_token
from souk.models import LlmRef
from souk_server.handshake import HANDSHAKE_VERSION, new_nonce
from souk_server.server import create_app

from tests.conftest import TEST_SIGNING_SECRET, Identity

RECEIVE_TIMEOUT = 2.0


def _kyok_headers(bearer: str, private_key, body: bytes) -> dict:
    timestamp = str(int(time.time()))
    payload = f"souk-kyok-call:{bearer}:{timestamp}:{hashlib.sha256(body).hexdigest()}".encode()
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


async def _live(register, souk, run_id: str, llm: LlmRef, context=None):
    """A registered agent, a run the broker is dispatching, a KYOK binding
    to `llm`, and a token naming run and agent — the setup every round
    trip shares. The binding is written the way protocols/agui.py writes
    it at opt-in; these tests supply the run, not the AG-UI road in."""
    served = await register("greeter")
    souk.enqueue_run(run_id, served.ref(), "thread_1", {}, "ag-ui")
    souk.kyok_relay.bind_run(run_id, KyokBinding(llm_provider=llm, context=context))
    return served, issue_kyok_token(run_id, served.ref(), TEST_SIGNING_SECRET)


def _client(souk) -> httpx.AsyncClient:
    # One client, one app: ASGIWebSocketTransport falls through to plain
    # ASGITransport for HTTP, so the agent's completions POST and the LLM
    # provider's socket exercise the same instance.
    return httpx.AsyncClient(
        transport=ASGIWebSocketTransport(app=create_app(souk)), base_url="http://test"
    )


async def _register_llm(souk, names: list[str]) -> Identity:
    identity = Identity()
    signature, timestamp = identity.sign_llm_registration(names)
    await souk.register_llm_providers(identity.public_key, signature, timestamp, names)
    return identity


class _LlmSocket:
    """One /ws/kyok connection speaking the frame table directly, opening
    with the same mutual challenge-response as the provider socket."""

    def __init__(self, ws, identity: Identity) -> None:
        self._ws = ws
        self.identity = identity

    async def connect(self, model_names: list[str]) -> None:
        nonce = new_nonce()
        hello_raw = json.dumps(
            {
                "type": "hello",
                "version": HANDSHAKE_VERSION,
                "publicKey": self.identity.public_key,
                "modelNames": model_names,
                "nonce": nonce,
            }
        )
        await self._ws.send_text(hello_raw)
        challenge = await self.recv()
        assert challenge["type"] == "challenge"
        await self.send(self.identity.proof(hello_raw, nonce, challenge["nonce"]))
        assert (await self.recv()) == {"type": "welcome"}

    async def recv(self) -> dict:
        return json.loads(await self._ws.receive_text(timeout=RECEIVE_TIMEOUT))

    async def send(self, frame: dict) -> None:
        await self._ws.send_text(json.dumps(frame))

    async def answer(self, request_id: str, chunks: list[dict]) -> None:
        for chunk in chunks:
            await self.send({"type": "chunk", "requestId": request_id, "data": chunk})
        await self.send({"type": "done", "requestId": request_id})


# --- handshake and attach ----------------------------------------------------


@pytest.mark.parametrize(
    "first_frame",
    [
        {"type": "hello"},  # no version, no identity
        {"type": "hello", "version": HANDSHAKE_VERSION, "publicKey": "ab", "nonce": "n"},  # no modelNames
        {"type": "chunk", "requestId": "x"},  # anything else before hello
    ],
)
async def test_a_bad_hello_closes_the_socket(souk, first_frame):
    async with _client(souk) as client:
        async with aconnect_ws("http://test/ws/kyok", client) as ws:
            await ws.send_text(json.dumps(first_frame))
            with pytest.raises(WebSocketDisconnect) as excinfo:
                await ws.receive_text(timeout=RECEIVE_TIMEOUT)
            assert excinfo.value.code == 1008


async def test_attaching_unregistered_model_names_is_refused(souk):
    """Registration is the prerequisite, exactly as it is for agents —
    core refuses the attach, and the socket closes by name rather than
    serving an offering nobody registered."""
    identity = Identity()  # never registered anything
    async with _client(souk) as client:
        async with aconnect_ws("http://test/ws/kyok", client) as ws:
            socket = _LlmSocket(ws, identity)
            with pytest.raises((WebSocketDisconnect, AssertionError)):
                await socket.connect(["gpt-test"])
                await ws.receive_text(timeout=RECEIVE_TIMEOUT)


async def test_registration_over_http_then_attach(souk):
    """The whole LLM-provider arrival, over the wire a real one uses:
    POST /llm-providers/register with the SDK-signed payload, then the
    socket handshake, then attached — visible as the offering resolving."""
    identity = Identity()
    signature, timestamp = identity.sign_llm_registration(["gpt-test"])
    async with _client(souk) as client:
        resp = await client.post(
            "/llm-providers/register",
            json={
                "models": ["gpt-test"],
                "public_key": identity.public_key,
                "signature": signature,
                "timestamp": timestamp,
                "metadata": {"family": "test"},
            },
        )
        assert resp.status_code == 201, resp.text
        assert resp.json() == {"models": ["gpt-test"]}

        ref = LlmRef(provider_key=identity.public_key, name="gpt-test")

        async def roster_row() -> dict:
            resp = await client.get("/llm-providers")
            assert resp.status_code == 200
            (row,) = resp.json()["offerings"]
            return row

        # Registered but not attached: discoverable, and honestly offline —
        # the pre-flight glance a KYOK caller binds on.
        row = await roster_row()
        assert (row["provider_key"], row["name"]) == (identity.public_key, "gpt-test")
        assert row["metadata"] == {"family": "test"}
        assert row["online"] is False

        async with aconnect_ws("http://test/ws/kyok", client) as ws:
            await _LlmSocket(ws, identity).connect(["gpt-test"])
            assert souk.kyok_relay.serving(ref) is not None
            assert (await roster_row())["online"] is True
        # And detached the moment the socket is gone.
        async with asyncio.timeout(RECEIVE_TIMEOUT):
            while souk.kyok_relay.serving(ref) is not None:
                await asyncio.sleep(0.01)
        assert (await roster_row())["online"] is False


async def test_a_signed_deletion_removes_the_offering_and_serving_blocks_it(souk):
    """Roster symmetry's deletion half through this gateway: the mirror
    of upstream's delete_llm_offering, driven over the wire a throwaway
    bridge uses to clean up after itself. While attached, the delete is a
    409 — retiring a live offering means detaching first, same rule as
    agents; afterwards a signed order removes the row and the roster
    stops listing it."""
    from souk_llm_provider_sdk import sign_llm_deletion

    identity = await _register_llm(souk, ["gpt-test"])

    def order() -> dict:
        signature, timestamp = sign_llm_deletion(identity, "gpt-test")
        return {
            "name": "gpt-test",
            "public_key": identity.public_key,
            "signature": signature,
            "timestamp": timestamp,
        }

    async with _client(souk) as client:
        async with aconnect_ws("http://test/ws/kyok", client) as ws:
            await _LlmSocket(ws, identity).connect(["gpt-test"])
            refused = await client.request("DELETE", "/llm-providers", json=order())
            assert refused.status_code == 409

        ref = LlmRef(provider_key=identity.public_key, name="gpt-test")
        async with asyncio.timeout(RECEIVE_TIMEOUT):
            while souk.kyok_relay.serving(ref) is not None:
                await asyncio.sleep(0.01)

        # A registration signature is not a deletion order.
        wrong_signature, timestamp = identity.sign_llm_registration(["gpt-test"])
        forged = await client.request(
            "DELETE",
            "/llm-providers",
            json={
                "name": "gpt-test",
                "public_key": identity.public_key,
                "signature": wrong_signature,
                "timestamp": timestamp,
            },
        )
        assert forged.status_code == 401

        deleted = await client.request("DELETE", "/llm-providers", json=order())
        assert deleted.status_code == 204
        assert (await client.get("/llm-providers")).json() == {"offerings": []}
        # Gone means gone: a second order finds nothing.
        assert (
            await client.request("DELETE", "/llm-providers", json=order())
        ).status_code == 404


# --- round trips -------------------------------------------------------------


async def test_full_round_trip_non_streaming(souk, register):
    run_id = "run_ws_nonstream"
    llm_identity = await _register_llm(souk, ["gpt-test"])
    llm = LlmRef(provider_key=llm_identity.public_key, name="gpt-test")
    served, token = await _live(register, souk, run_id, llm, context={"voucher": "v1"})
    try:
        body = json.dumps({"messages": [{"role": "user", "content": "hi"}]}).encode()

        async with _client(souk) as client:
            # Attached before the agent calls: resolution is per call and
            # fails fast (503) on an unattached offering, by design.
            async with aconnect_ws("http://test/ws/kyok", client) as ws:
                socket = _LlmSocket(ws, llm_identity)
                await socket.connect(["gpt-test"])

                agent_call = asyncio.ensure_future(
                    client.post(
                        "/kyok/v1/chat/completions",
                        content=body,
                        headers=_kyok_headers(token, served.identity._key, body),
                    )
                )
                request = await socket.recv()
                assert request["type"] == "completionRequest"
                # The policy material keep-your-own-key.md promises the
                # LLM provider, on the frame itself.
                assert request["runId"] == run_id
                assert request["providerKey"] == served.public_key
                assert request["agentName"] == "greeter"
                assert request["llmName"] == "gpt-test"
                assert request["context"] == {"voucher": "v1"}
                assert request["payload"]["messages"][0]["content"] == "hi"
                await socket.answer(
                    request["requestId"],
                    [
                        _chunk(content="hello", role="assistant"),
                        _chunk(content=" world", finish_reason="stop"),
                    ],
                )
                resp = await agent_call
        assert resp.status_code == 200, resp.text
        result = resp.json()
        assert result["choices"][0]["message"]["content"] == "hello world"
        assert result["choices"][0]["finish_reason"] == "stop"
    finally:
        souk.broker.forget(run_id)


async def test_full_round_trip_streaming(souk, register):
    run_id = "run_ws_stream"
    llm_identity = await _register_llm(souk, ["gpt-test"])
    llm = LlmRef(provider_key=llm_identity.public_key, name="gpt-test")
    served, token = await _live(register, souk, run_id, llm)
    try:
        body = json.dumps({"messages": [], "stream": True}).encode()

        async with _client(souk) as client:
            async with aconnect_ws("http://test/ws/kyok", client) as ws:
                socket = _LlmSocket(ws, llm_identity)
                await socket.connect(["gpt-test"])

                async def agent_call():
                    async with client.stream(
                        "POST",
                        "/kyok/v1/chat/completions",
                        content=body,
                        headers=_kyok_headers(token, served.identity._key, body),
                    ) as resp:
                        assert resp.status_code == 200
                        return [line async for line in resp.aiter_lines() if line]

                async def llm_serves():
                    request = await socket.recv()
                    await socket.answer(
                        request["requestId"],
                        [_chunk(content="hi", role="assistant", finish_reason="stop")],
                    )

                lines, _ = await asyncio.gather(agent_call(), llm_serves())
        assert lines[-1] == "data: [DONE]"
        assert any("hi" in line for line in lines[:-1])
    finally:
        souk.broker.forget(run_id)


async def test_an_error_frame_fails_the_completion_fast(souk, register):
    run_id = "run_ws_error"
    llm_identity = await _register_llm(souk, ["gpt-test"])
    llm = LlmRef(provider_key=llm_identity.public_key, name="gpt-test")
    served, token = await _live(register, souk, run_id, llm)
    try:
        body = json.dumps({"messages": [], "stream": True}).encode()

        async with _client(souk) as client:
            async with aconnect_ws("http://test/ws/kyok", client) as ws:
                socket = _LlmSocket(ws, llm_identity)
                await socket.connect(["gpt-test"])

                async def agent_call():
                    async with client.stream(
                        "POST",
                        "/kyok/v1/chat/completions",
                        content=body,
                        headers=_kyok_headers(token, served.identity._key, body),
                    ) as resp:
                        return [line async for line in resp.aiter_lines() if line]

                async def llm_refuses():
                    request = await socket.recv()
                    await socket.send(
                        {
                            "type": "error",
                            "requestId": request["requestId"],
                            "message": "upstream LLM call failed",
                        }
                    )

                lines, _ = await asyncio.gather(agent_call(), llm_refuses())
        assert len(lines) == 1
        assert json.loads(lines[0].removeprefix("data: ")) == {
            "error": {"message": "upstream LLM call failed"}
        }
    finally:
        souk.broker.forget(run_id)


async def test_a_structured_refusal_reaches_the_agent_intact(souk, register):
    """The #63 envelope through this gateway, both response shapes: an
    error frame carrying a `refusal` dict arrives as the agent's error
    payload — data, not prose — in-stream for a streaming call, and on
    the 502 body for a non-streaming one. The vocabulary inside is the
    two roles' own; nothing on this path interprets it."""
    refusal = {"kind": "throttled", "retryAfter": 30}
    llm_identity = await _register_llm(souk, ["gpt-test"])
    llm = LlmRef(provider_key=llm_identity.public_key, name="gpt-test")

    for run_id, stream in (("run_refused_stream", True), ("run_refused_plain", False)):
        served, token = await _live(register, souk, run_id, llm)
        try:
            body = json.dumps({"messages": [], "stream": stream}).encode()
            async with _client(souk) as client:
                async with aconnect_ws("http://test/ws/kyok", client) as ws:
                    socket = _LlmSocket(ws, llm_identity)
                    await socket.connect(["gpt-test"])

                    async def agent_call():
                        if stream:
                            async with client.stream(
                                "POST",
                                "/kyok/v1/chat/completions",
                                content=body,
                                headers=_kyok_headers(token, served.identity._key, body),
                            ) as resp:
                                return [line async for line in resp.aiter_lines() if line]
                        return await client.post(
                            "/kyok/v1/chat/completions",
                            content=body,
                            headers=_kyok_headers(token, served.identity._key, body),
                        )

                    async def llm_refuses():
                        request = await socket.recv()
                        await socket.send(
                            {
                                "type": "error",
                                "requestId": request["requestId"],
                                "message": "refused by the LLM provider",
                                "refusal": refusal,
                            }
                        )

                    answer, _ = await asyncio.gather(agent_call(), llm_refuses())
            if stream:
                assert json.loads(answer[0].removeprefix("data: ")) == {"error": refusal}
            else:
                assert answer.status_code == 502
                assert answer.json()["error"] == refusal
        finally:
            souk.broker.forget(run_id)


async def test_one_socket_multiplexes_concurrent_completions(souk, register):
    """requestId multiplexing: two completions in flight on one socket,
    answered out of order, each answer landing on its own completion."""
    run_id = "run_ws_multiplex"
    llm_identity = await _register_llm(souk, ["gpt-test"])
    llm = LlmRef(provider_key=llm_identity.public_key, name="gpt-test")
    served, token = await _live(register, souk, run_id, llm)
    try:

        async with _client(souk) as client:
            async with aconnect_ws("http://test/ws/kyok", client) as ws:
                socket = _LlmSocket(ws, llm_identity)
                await socket.connect(["gpt-test"])

                async def agent_call(prompt: str) -> str:
                    body = json.dumps({"messages": [{"role": "user", "content": prompt}]}).encode()
                    resp = await client.post(
                        "/kyok/v1/chat/completions",
                        content=body,
                        headers=_kyok_headers(token, served.identity._key, body),
                    )
                    assert resp.status_code == 200, resp.text
                    return resp.json()["choices"][0]["message"]["content"]

                async def llm_serves():
                    first = await socket.recv()
                    second = await socket.recv()
                    # Answer in reverse order of arrival: each answer lands
                    # on its own completion, keyed by requestId.
                    for request in (second, first):
                        prompt = request["payload"]["messages"][0]["content"]
                        await socket.answer(
                            request["requestId"],
                            [_chunk(content=f"re: {prompt}", role="assistant", finish_reason="stop")],
                        )

                first_answer, second_answer, _ = await asyncio.gather(
                    agent_call("one"), agent_call("two"), llm_serves()
                )
        assert first_answer == "re: one"
        assert second_answer == "re: two"
    finally:
        souk.broker.forget(run_id)


# --- the binding -------------------------------------------------------------


async def test_an_answer_is_only_accepted_on_the_socket_the_request_was_delivered_to(
    souk, register
):
    """The security property carried over from the old socket, now proven
    against the strongest intruder the new model allows: the *same
    identity*, attached for the *same offering* — every credential check
    passes, and later completions would genuinely be its to serve. It
    presents a valid requestId it was not delivered, is refused with an
    error frame, and the completion still gets its real answer from the
    socket that holds it."""
    run_id = "run_ws_binding"
    llm_identity = await _register_llm(souk, ["gpt-test"])
    llm = LlmRef(provider_key=llm_identity.public_key, name="gpt-test")
    served, token = await _live(register, souk, run_id, llm)
    try:
        body = json.dumps({"messages": [{"role": "user", "content": "hi"}]}).encode()

        async with _client(souk) as client:
            async with aconnect_ws("http://test/ws/kyok", client) as holder_ws:
                holder = _LlmSocket(holder_ws, llm_identity)
                await holder.connect(["gpt-test"])

                agent_call = asyncio.ensure_future(
                    client.post(
                        "/kyok/v1/chat/completions",
                        content=body,
                        headers=_kyok_headers(token, served.identity._key, body),
                    )
                )
                request = await holder.recv()
                request_id = request["requestId"]

                async with aconnect_ws("http://test/ws/kyok", client) as intruder_ws:
                    intruder = _LlmSocket(intruder_ws, llm_identity)
                    await intruder.connect(["gpt-test"])
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
                resp = await agent_call
        assert resp.json()["choices"][0]["message"]["content"] == "real"
        assert "injected" not in resp.text
    finally:
        souk.broker.forget(run_id)


async def test_a_dropped_socket_fails_its_in_flight_completions_fast(souk, register):
    """A truncated answer must fail the completion, not complete it — and
    fail it now, not at the chunk-gap timeout."""
    run_id = "run_ws_dropped"
    llm_identity = await _register_llm(souk, ["gpt-test"])
    llm = LlmRef(provider_key=llm_identity.public_key, name="gpt-test")
    served, token = await _live(register, souk, run_id, llm)
    try:
        body = json.dumps({"messages": [], "stream": True}).encode()

        async with _client(souk) as client:

            async def agent_call():
                async with client.stream(
                    "POST",
                    "/kyok/v1/chat/completions",
                    content=body,
                    headers=_kyok_headers(token, served.identity._key, body),
                ) as resp:
                    return [line async for line in resp.aiter_lines() if line]

            async with aconnect_ws("http://test/ws/kyok", client) as ws:
                socket = _LlmSocket(ws, llm_identity)
                await socket.connect(["gpt-test"])
                call = asyncio.ensure_future(agent_call())
                request = await socket.recv()
                await socket.send(
                    {
                        "type": "chunk",
                        "requestId": request["requestId"],
                        "data": _chunk(content="half an ans", role="assistant"),
                    }
                )
            # the socket drops with the answer unfinished
            async with asyncio.timeout(5):
                lines = await call
        assert json.loads(lines[-1].removeprefix("data: ")) == {
            "error": {"message": "LLM provider disconnected mid-response"}
        }
    finally:
        souk.broker.forget(run_id)
