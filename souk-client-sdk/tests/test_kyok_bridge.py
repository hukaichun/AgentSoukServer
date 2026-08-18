"""Covers KyokBridge — the caller-side half of KYOK (Keep Your Own Key),
now an identified LLM provider rather than a session-keyed bridge. See
souk_client_sdk/kyok_bridge.py's own docstring for the transport (one
WebSocket to /ws/kyok, opened with the mutual challenge-response) and
docs/keep-your-own-key.md (in the souk repo) for the design.

Uses a stub /ws/kyok server speaking the gateway's frame protocol (no
real souk instance needed), and monkeypatches litellm.acompletion
directly rather than adding another mocking layer for it — litellm is
already a runtime dependency here.
"""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import json
from typing import Any

import litellm
import pytest
import websockets
from souk_llm_provider_sdk import llm_registration_payload

from souk_client_sdk.kyok_bridge import KyokBridge, _to_chunk_dict

RECEIVE_TIMEOUT = 2.0


# --- _to_chunk_dict: all three normalization paths -------------------------


class _ModelDumpChunk:
    def model_dump(self, mode: str = "python") -> dict:
        return {"via": "model_dump", "mode": mode}


class _DictMethodChunk:
    def dict(self) -> dict:
        return {"via": "dict_method"}


def test_to_chunk_dict_uses_model_dump_when_available():
    assert _to_chunk_dict(_ModelDumpChunk()) == {"via": "model_dump", "mode": "json"}


def test_to_chunk_dict_falls_back_to_dict_method():
    assert _to_chunk_dict(_DictMethodChunk()) == {"via": "dict_method"}


def test_to_chunk_dict_falls_back_to_plain_dict_conversion():
    assert _to_chunk_dict({"via": "plain_dict"}) == {"via": "plain_dict"}


# --- identity / registration / metadata -------------------------------------


def test_run_metadata_names_the_offering_and_carries_the_context():
    bridge = KyokBridge("http://souk.local", model="test-model", api_key="key", offering="my-llm")
    assert bridge.run_metadata({"voucher": "v1"}) == {
        "kyok": {
            "llmProvider": {
                "providerKey": bridge.identity.public_key,
                "name": "my-llm",
            },
            "context": {"voucher": "v1"},
        }
    }
    # No context → no context key, not a null one: souk treats the field
    # as opaque and absent is the honest shape for "nothing shared".
    assert "context" not in bridge.run_metadata()["kyok"]


async def test_every_connection_re_registers_first(monkeypatch):
    """The #16 fix: registration is part of each connection cycle, not a
    one-shot precondition — a souk whose database was reset gets the
    offering back on the next reconnect instead of refusing the attach
    forever."""
    calls = 0

    async def counting_register(self):
        nonlocal calls
        calls += 1

    monkeypatch.setattr(KyokBridge, "register", counting_register)
    async with StubGateway() as gateway:
        bridge, task = await _connected_bridge(gateway, stub_register=False)
        try:
            assert calls == 1
            # The connection drops; the reconnect cycle registers again.
            gateway.connected.clear()
            await gateway._conn.close()
            async with asyncio.timeout(RECEIVE_TIMEOUT + bridge.reconnect_delay):
                await gateway.connected.wait()
            assert calls == 2
        finally:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task


# --- the socket ------------------------------------------------------------


class StubGateway:
    """The server half of one /ws/kyok socket: walks the four-frame
    handshake (recording the hello and verifying the proof's shape),
    pushes what a test tells it to, records every frame the bridge sends."""

    def __init__(self) -> None:
        self.hello: dict | None = None
        self.hello_raw: str | None = None
        self.proof: dict | None = None
        self.frames: asyncio.Queue = asyncio.Queue()
        self.connected = asyncio.Event()
        self._conn = None

    async def __aenter__(self) -> "StubGateway":
        self._server = await websockets.serve(self._handler, "127.0.0.1", 0)
        self.port = self._server.sockets[0].getsockname()[1]
        return self

    async def __aexit__(self, *exc) -> None:
        self._server.close()
        await self._server.wait_closed()

    async def _handler(self, ws) -> None:
        self.hello_raw = await ws.recv()
        self.hello = json.loads(self.hello_raw)
        await ws.send(json.dumps({"type": "challenge", "soukPublicKey": None, "nonce": "n_souk"}))
        self.proof = json.loads(await ws.recv())
        await ws.send(json.dumps({"type": "welcome"}))
        self._conn = ws
        self.connected.set()
        async for raw in ws:
            self.frames.put_nowait(json.loads(raw))

    async def push(self, frame: dict) -> None:
        await self._conn.send(json.dumps(frame))

    async def next_frame(self) -> dict:
        async with asyncio.timeout(RECEIVE_TIMEOUT):
            return await self.frames.get()


async def _connected_bridge(gateway: StubGateway, *, stub_register: bool = True, **kwargs: Any):
    bridge = KyokBridge(
        f"http://127.0.0.1:{gateway.port}",
        model="test-model",
        api_key="key",
        reconnect_delay=0.05,
        **kwargs,
    )
    # The stub is ws-only; registration needs an HTTP souk. A test that
    # cares about register() patches it itself and passes stub_register=False.
    if stub_register:

        async def no_register():
            pass

        bridge.register = no_register
    task = asyncio.create_task(bridge.serve_forever())
    async with asyncio.timeout(RECEIVE_TIMEOUT):
        await gateway.connected.wait()
    return bridge, task


async def test_the_handshake_carries_identity_and_offering_and_proves_the_key():
    async with StubGateway() as gateway:
        bridge, task = await _connected_bridge(gateway, offering="my-llm")
        try:
            assert gateway.hello["type"] == "hello"
            assert gateway.hello["publicKey"] == bridge.identity.public_key
            assert gateway.hello["modelNames"] == ["my-llm"]
            # The proof signs the gateway's payload: both nonces and a
            # digest of the hello exactly as it went on the wire.
            digest = hashlib.sha256(gateway.hello_raw.encode()).hexdigest()
            payload = f"souk-auth:provider:{gateway.hello['nonce']}:n_souk:{digest}".encode()
            from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey

            Ed25519PublicKey.from_public_bytes(
                bytes.fromhex(bridge.identity.public_key)
            ).verify(bytes.fromhex(gateway.proof["signature"]), payload)
        finally:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task


def test_the_registration_payload_matches_the_sdk_statement():
    """The payload this bridge signs at register() is the SDK's, not a
    local restatement — one line, and it is what keeps this side from
    drifting when core's changes."""
    assert llm_registration_payload(["m"], 123) == b"souk-register-llm:m:123"


async def test_serving_is_the_whole_lifecycle_in_one_block():
    """#19: registered, attached before the body runs, torn down on exit."""
    async with StubGateway() as gateway:
        bridge = KyokBridge(
            f"http://127.0.0.1:{gateway.port}", model="m", api_key="k", reconnect_delay=0.05
        )

        async def no_register():
            pass

        bridge.register = no_register
        async with asyncio.timeout(RECEIVE_TIMEOUT):
            async with bridge.serving():
                # The block only opens attached — no race against the
                # socket for a run started here.
                assert bridge.attached.is_set()
        # And nothing is left serving after the block.
        assert not bridge.attached.is_set()


async def test_a_refusal_from_the_handler_travels_as_a_structured_error_frame():
    """The #63 envelope, end to end on this side: a handler raising
    CompletionRefused answers with its payload on the error frame, not
    prose — and the handler saw the whole DeliveredCompletion, which is
    the material its policy runs on."""
    from souk_llm_provider_sdk import CompletionRefused

    seen: dict = {}

    async def refusing_handler(delivered):
        seen["delivered"] = delivered
        raise CompletionRefused({"kind": "throttled", "retryAfter": 30})
        yield  # pragma: no cover - makes this an async generator

    async with StubGateway() as gateway:
        _bridge, task = await _connected_bridge(gateway, handler=refusing_handler)
        try:
            await gateway.push(
                {
                    "type": "completionRequest",
                    "requestId": "req_1",
                    "runId": "run_9",
                    "providerKey": "ab" * 32,
                    "agentName": "greeter",
                    "llmName": "kyok",
                    "context": {"voucher": "v1"},
                    "payload": {"messages": []},
                }
            )
            frame = await gateway.next_frame()
            assert frame["type"] == "error"
            assert frame["refusal"] == {"kind": "throttled", "retryAfter": 30}
            assert seen["delivered"].run_id == "run_9"
            assert seen["delivered"].agent_name == "greeter"
            assert seen["delivered"].context == {"voucher": "v1"}
        finally:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task


async def test_a_completion_request_streams_back_as_chunks_then_done(monkeypatch):
    captured: dict = {}

    async def fake_acompletion(**kwargs):
        captured.update(kwargs)

        async def gen():
            yield {"choices": [{"delta": {"role": "assistant", "content": "hi"}}]}
            yield {"choices": [{"delta": {"content": " there"}}]}

        return gen()

    monkeypatch.setattr(litellm, "acompletion", fake_acompletion)
    async with StubGateway() as gateway:
        _bridge, task = await _connected_bridge(gateway, api_base="http://llm.local")
        try:
            body = {
                "messages": [{"role": "user", "content": "hi"}],
                "tools": [{"type": "function"}],
                "temperature": 0.5,
            }
            await gateway.push({"type": "completionRequest", "requestId": "req_1", "payload": body})

            frames = [await gateway.next_frame() for _ in range(3)]
            assert [f["type"] for f in frames] == ["chunk", "chunk", "done"]
            assert all(f["requestId"] == "req_1" for f in frames)
            assert frames[0]["data"]["choices"][0]["delta"]["content"] == "hi"

            # The provider's whole request body reached litellm, on this
            # bridge's own key.
            assert captured["model"] == "test-model"
            assert captured["api_key"] == "key"
            assert captured["api_base"] == "http://llm.local"
            assert captured["messages"] == body["messages"]
            assert captured["tools"] == body["tools"]
            assert captured["temperature"] == 0.5
            assert captured["stream"] is True
        finally:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task


async def test_an_llm_failure_becomes_one_error_frame(monkeypatch):
    async def fake_acompletion(**kwargs):
        raise RuntimeError("upstream boom")

    monkeypatch.setattr(litellm, "acompletion", fake_acompletion)
    async with StubGateway() as gateway:
        _bridge, task = await _connected_bridge(gateway)
        try:
            await gateway.push(
                {"type": "completionRequest", "requestId": "req_1", "payload": {"messages": []}}
            )
            frame = await gateway.next_frame()
            assert frame == {"type": "error", "requestId": "req_1", "message": "upstream boom"}
        finally:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task


async def test_concurrent_completions_multiplex_on_one_socket(monkeypatch):
    """Two requests in flight at once; their chunks interleave by
    requestId — the property that made the socket strictly better than
    poll_one's one-per-cycle handover."""
    release = asyncio.Event()

    async def fake_acompletion(**kwargs):
        prompt = kwargs["messages"][0]["content"]

        async def gen():
            if prompt == "slow":
                await release.wait()
            yield {"choices": [{"delta": {"content": f"re: {prompt}"}}]}

        return gen()

    monkeypatch.setattr(litellm, "acompletion", fake_acompletion)
    async with StubGateway() as gateway:
        _bridge, task = await _connected_bridge(gateway)
        try:
            await gateway.push(
                {
                    "type": "completionRequest",
                    "requestId": "req_slow",
                    "payload": {"messages": [{"role": "user", "content": "slow"}]},
                }
            )
            await gateway.push(
                {
                    "type": "completionRequest",
                    "requestId": "req_fast",
                    "payload": {"messages": [{"role": "user", "content": "fast"}]},
                }
            )
            # The fast one answers while the slow one is still held open.
            first = await gateway.next_frame()
            assert first["requestId"] == "req_fast"
            release.set()
            rest = [await gateway.next_frame() for _ in range(3)]
            assert {"req_fast", "req_slow"} == {f["requestId"] for f in [first, *rest]}
        finally:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
