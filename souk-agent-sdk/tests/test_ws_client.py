"""SoukProvider's WebSocket transport (souk_agent_sdk.client) against a
stub gateway speaking the /ws/provider frame protocol — the table in the
gateway repo's docs/server-mode.md, which that repo authors and this SDK
implements. The stub is the server half of one socket: it runs the
four-frame handshake, pushes what a test tells it to, and records every
frame the provider sends back.

Registration is HTTP and separately covered; these tests drive
`_run_connection`, the transport under test.

**The stub signs.** It holds a souk identity and answers the provider's
nonce with a real signature, because the thing being tested on this side
is partly whether the provider *checks* that — and a stub that skipped it
would let a provider that never verified anything pass.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
from typing import Any

import pytest
import websockets
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from souk_agent_sdk.client import (
    AgentHandle,
    SoukIdentityMismatch,
    SoukProvider,
    SoukQueryFailed,
    souk_challenge_payload,
)

RECEIVE_TIMEOUT = 2.0


class StubGateway:
    """One souk, as a provider sees it.

    `identity=None` stands in for a souk with no key configured — which is
    a real deployment state, not a test shortcut, and the one a pinned
    provider must refuse.
    """

    def __init__(self, identity: Ed25519PrivateKey | None = "generate") -> None:
        if identity == "generate":
            identity = Ed25519PrivateKey.generate()
        self._identity = identity
        self.public_key = (
            identity.public_key().public_bytes_raw().hex() if identity is not None else None
        )
        self.hello: dict | None = None
        self.proof: dict | None = None
        self.souk_nonce: str | None = None
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
        self.hello = json.loads(await ws.recv())
        self.souk_nonce = "s" * 64
        payload = souk_challenge_payload(self.hello["nonce"], self.souk_nonce)
        await ws.send(
            json.dumps(
                {
                    "type": "challenge",
                    "soukPublicKey": self.public_key,
                    "nonce": self.souk_nonce,
                    "signature": (
                        self._identity.sign(payload).hex()
                        if self._identity is not None
                        else None
                    ),
                }
            )
        )
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


def _provider(gateway: StubGateway, handles: list[AgentHandle], **kwargs: Any) -> SoukProvider:
    return SoukProvider(f"http://127.0.0.1:{gateway.port}", handles, **kwargs)


async def _echo_run_stream(run_input: dict) -> Any:
    yield {"type": "RUN_STARTED", "runId": run_input["runId"]}
    yield {"type": "RUN_FINISHED", "runId": run_input["runId"]}


@contextlib.asynccontextmanager
async def _connected(gateway: StubGateway, provider: SoukProvider):
    """A connected provider whose runtime is running.

    `run_forever` starts the runtime and then loops on `_run_connection`;
    these tests drive the connection directly, so the start is theirs to
    do. Without it the socket comes up, the handshake completes, every
    offer is accepted — and nothing ever runs, which reads as a hang
    rather than as a missing call.
    """
    provider.runtime.start()
    conn = asyncio.create_task(provider._run_connection())
    try:
        async with asyncio.timeout(RECEIVE_TIMEOUT):
            await gateway.connected.wait()
        yield conn
    finally:
        conn.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await conn
        await provider.runtime.aclose(cancel_in_flight=True)


# --- the handshake ----------------------------------------------------------


async def test_hello_carries_the_claims_and_a_nonce_but_no_signature(tmp_path):
    """Nothing signed in frame one. The old shape put a self-composed
    signature here, which is precisely what made a captured hello worth
    replaying — this side now signs only after souk has answered."""
    async with StubGateway() as gateway:
        provider = _provider(
            gateway,
            [AgentHandle(name="echo", run_stream=_echo_run_stream)],
            max_concurrent_runs=2,
            identity_key_path=str(tmp_path / "k.key"),
        )
        async with _connected(gateway, provider):
            assert gateway.hello["type"] == "hello"
            assert gateway.hello["version"] == 1
            assert gateway.hello["publicKey"] == provider.public_key
            assert gateway.hello["agentNames"] == ["echo"]
            assert gateway.hello["maxConcurrentRuns"] == 2
            assert len(gateway.hello["nonce"]) == 64
            assert "signature" not in gateway.hello


async def test_the_proof_signs_both_nonces_and_the_hello_that_was_sent(tmp_path):
    """Verified by rebuilding the payload from the frames the stub
    actually received — so a provider that signed the right shape over the
    wrong bytes fails here rather than passing."""
    from souk_provider_sdk import verify_signature

    from souk_agent_sdk.client import provider_proof_payload

    async with StubGateway() as gateway:
        provider = _provider(
            gateway,
            [AgentHandle(name="echo", run_stream=_echo_run_stream)],
            identity_key_path=str(tmp_path / "k.key"),
        )
        async with _connected(gateway, provider):
            hello_raw = json.dumps(gateway.hello)
            assert verify_signature(
                provider.public_key,
                gateway.proof["signature"],
                provider_proof_payload(gateway.hello["nonce"], gateway.souk_nonce, hello_raw),
            )


async def test_a_pinned_provider_refuses_a_different_souk(tmp_path):
    """The case pinning exists for: something else answers the URL, holds
    a perfectly good key of its own, and signs correctly with it. Every
    cryptographic check passes; it is simply not the souk we meant."""
    async with StubGateway() as gateway:
        provider = _provider(
            gateway,
            [AgentHandle(name="echo", run_stream=_echo_run_stream)],
            identity_key_path=str(tmp_path / "k.key"),
            souk_public_key="ab" * 32,
        )
        with pytest.raises(SoukIdentityMismatch, match="not the"):
            await provider._run_connection()


async def test_a_pinned_provider_refuses_a_souk_with_no_identity(tmp_path):
    """A souk that cannot prove itself is not the souk we pinned. Saying
    "no identity configured" is honest, and it is still a no."""
    async with StubGateway(identity=None) as gateway:
        provider = _provider(
            gateway,
            [AgentHandle(name="echo", run_stream=_echo_run_stream)],
            identity_key_path=str(tmp_path / "k.key"),
            souk_public_key="ab" * 32,
        )
        with pytest.raises(SoukIdentityMismatch, match="no identity"):
            await provider._run_connection()


async def test_a_pinned_provider_accepts_the_souk_it_pinned(tmp_path):
    async with StubGateway() as gateway:
        provider = _provider(
            gateway,
            [AgentHandle(name="echo", run_stream=_echo_run_stream)],
            identity_key_path=str(tmp_path / "k.key"),
            souk_public_key=gateway.public_key,
        )
        async with _connected(gateway, provider):
            assert gateway.proof is not None


async def test_a_souk_that_presents_a_key_it_cannot_sign_with_is_refused(tmp_path):
    """A key in the frame is a claim, not a proof. This stub presents a
    real public key and signs with a different one — which is what
    anything relaying a genuine souk's advertised key, without its private
    half, would produce."""
    imposter = StubGateway()
    imposter.public_key = Ed25519PrivateKey.generate().public_key().public_bytes_raw().hex()
    async with imposter as gateway:
        provider = _provider(
            gateway,
            [AgentHandle(name="echo", run_stream=_echo_run_stream)],
            identity_key_path=str(tmp_path / "k.key"),
        )
        with pytest.raises(SoukIdentityMismatch, match="did not"):
            await provider._run_connection()


async def test_an_unpinned_provider_still_connects_to_a_souk_with_no_identity(tmp_path):
    """Today's deployments, unchanged: nothing to check and nothing
    claimed, so the provider proceeds. It warns, because a souk that meant
    to have an identity and does not is worth noticing."""
    async with StubGateway(identity=None) as gateway:
        provider = _provider(
            gateway,
            [AgentHandle(name="echo", run_stream=_echo_run_stream)],
            identity_key_path=str(tmp_path / "k.key"),
        )
        async with _connected(gateway, provider):
            assert gateway.proof is not None


# --- runs over the socket ---------------------------------------------------


async def test_a_pushed_run_comes_back_as_an_ack_then_events_then_finish(tmp_path):
    async with StubGateway() as gateway:
        provider = _provider(
            gateway,
            [AgentHandle(name="echo", run_stream=_echo_run_stream)],
            identity_key_path=str(tmp_path / "k.key"),
        )
        async with _connected(gateway, provider):
            await gateway.push(
                {
                    "type": "run",
                    "runId": "r1",
                    "threadId": "t1",
                    "agentName": "echo",
                    "input": {"runId": "r1", "threadId": "t1", "messages": []},
                }
            )
            frames = [await gateway.next_frame() for _ in range(4)]
            assert [f["type"] for f in frames] == ["ack", "event", "event", "finish"]
            assert frames[0] == {"type": "ack", "runId": "r1", "accepted": True}
            assert frames[1]["event"]["type"] == "RUN_STARTED"
            assert frames[2]["event"]["type"] == "RUN_FINISHED"
            assert all(f["runId"] == "r1" for f in frames)


async def test_a_cancel_interrupts_the_run_and_finish_still_goes_out(tmp_path):
    started = asyncio.Event()

    async def stuck_run_stream(run_input: dict) -> Any:
        yield {"type": "RUN_STARTED", "runId": run_input["runId"]}
        started.set()
        await asyncio.sleep(3600)  # a run that would never end on its own

    async with StubGateway() as gateway:
        provider = _provider(
            gateway,
            [AgentHandle(name="stuck", run_stream=stuck_run_stream)],
            identity_key_path=str(tmp_path / "k.key"),
        )
        async with _connected(gateway, provider):
            await gateway.push(
                {
                    "type": "run",
                    "runId": "r1",
                    "threadId": "t1",
                    "agentName": "stuck",
                    "input": {"runId": "r1"},
                }
            )
            assert (await gateway.next_frame())["type"] == "ack"
            assert (await gateway.next_frame())["type"] == "event"
            async with asyncio.timeout(RECEIVE_TIMEOUT):
                await started.wait()

            await gateway.push({"type": "cancel", "runId": "r1"})
            # Complying is the provider's choice, and this one complies:
            # the run's current await is interrupted, and finish — the
            # last word souk decides the outcome from — still goes out.
            assert (await gateway.next_frame()) == {"type": "finish", "runId": "r1"}


async def test_a_run_for_an_unknown_agent_is_declined_without_taking_the_socket_down(tmp_path):
    """Declining is a real answer now, so an agent this provider does not
    host produces `accepted: false` rather than silence. souk keeps the
    run, and this socket goes on serving."""
    async with StubGateway() as gateway:
        provider = _provider(
            gateway,
            [AgentHandle(name="echo", run_stream=_echo_run_stream)],
            identity_key_path=str(tmp_path / "k.key"),
        )
        async with _connected(gateway, provider):
            await gateway.push(
                {
                    "type": "run",
                    "runId": "r_alien",
                    "threadId": "t1",
                    "agentName": "not_ours",
                    "input": {},
                }
            )
            assert (await gateway.next_frame()) == {
                "type": "ack",
                "runId": "r_alien",
                "accepted": False,
            }
            await gateway.push(
                {
                    "type": "run",
                    "runId": "r2",
                    "threadId": "t1",
                    "agentName": "echo",
                    "input": {"runId": "r2"},
                }
            )
            frames = [await gateway.next_frame() for _ in range(4)]
            assert all(f["runId"] == "r2" for f in frames)


async def test_a_dropped_socket_does_not_end_the_run_and_its_frames_flush_on_the_next(tmp_path):
    """The SDK half of "a dropped socket ends nothing".

    Two things make it true, and the second is new. The outbound queue is
    not per-connection — a run is addressed by runId, not by the socket it
    arrived on — so frames a dead socket failed to carry go out on the
    next one. And the *runtime* is no longer tied to the connection
    either: the agent keeps running while there is no socket at all, and
    finishes into the queue.

    That second half changed with the inversion. The transport used to own
    the loop, so losing the socket cancelled the work; now the loop is the
    provider's and the socket is only its carrier. Which is why this test
    releases the run *after* the drop — under the old shape there would
    have been nothing left alive to release.
    """
    release = asyncio.Event()

    async def two_phase_run_stream(run_input: dict) -> Any:
        yield {"type": "RUN_STARTED", "runId": run_input["runId"]}
        await release.wait()
        yield {"type": "RUN_FINISHED", "runId": run_input["runId"]}

    async with StubGateway() as gateway:
        provider = _provider(
            gateway,
            [AgentHandle(name="twophase", run_stream=two_phase_run_stream)],
            identity_key_path=str(tmp_path / "k.key"),
        )
        provider.runtime.start()
        conn = asyncio.create_task(provider._run_connection())
        async with asyncio.timeout(RECEIVE_TIMEOUT):
            await gateway.connected.wait()
        await gateway.push(
            {
                "type": "run",
                "runId": "r1",
                "threadId": "t1",
                "agentName": "twophase",
                "input": {"runId": "r1"},
            }
        )
        assert (await gateway.next_frame())["type"] == "ack"
        assert (await gateway.next_frame())["type"] == "event"

        # The socket drops mid-run, with the agent parked on `release`.
        await gateway._conn.close()
        with contextlib.suppress(Exception):
            async with asyncio.timeout(RECEIVE_TIMEOUT):
                await conn
        gateway.connected.clear()

        # A fresh connection — run_forever would do exactly this. The
        # handshake runs again in full: a reconnect is a new connection,
        # with a new nonce from each side.
        conn2 = asyncio.create_task(provider._run_connection())
        try:
            async with asyncio.timeout(RECEIVE_TIMEOUT):
                await gateway.connected.wait()
            release.set()
            frames = [await gateway.next_frame() for _ in range(2)]
            assert frames[0]["event"]["type"] == "RUN_FINISHED"
            assert frames[1] == {"type": "finish", "runId": "r1"}
        finally:
            conn2.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await conn2
            await provider.runtime.aclose(cancel_in_flight=True)


# --- queries: request/response over a one-way wire --------------------------


async def test_a_query_goes_out_with_a_correlation_id_and_its_answer_comes_back(tmp_path):
    """The first thing on this wire that expects a reply. Everything else a
    provider sends is fire-and-forget, which is why the queryId exists at
    all: several questions may be outstanding on one socket, and an answer
    has to find the caller that asked."""
    async with StubGateway() as gateway:
        provider = _provider(
            gateway,
            [AgentHandle(name="echo", run_stream=_echo_run_stream)],
            identity_key_path=str(tmp_path / "k.key"),
        )
        async with _connected(gateway, provider):
            asked = asyncio.create_task(provider.thread_messages("t1", limit=3))

            query = await gateway.next_frame()
            assert query["type"] == "query"
            assert query["method"] == "thread_messages"
            assert query["params"] == {"threadId": "t1", "limit": 3}
            assert query["queryId"]

            await gateway.push(
                {
                    "type": "queryResult",
                    "queryId": query["queryId"],
                    "result": [{"role": "user", "content": "hi"}],
                }
            )
            assert await asyncio.wait_for(asked, RECEIVE_TIMEOUT) == [
                {"role": "user", "content": "hi"}
            ]


async def test_two_queries_in_flight_get_their_own_answers(tmp_path):
    """Answered out of order, on purpose: correlation is the point of the
    id, and a queue would have served the wrong caller."""
    async with StubGateway() as gateway:
        provider = _provider(
            gateway,
            [AgentHandle(name="echo", run_stream=_echo_run_stream)],
            identity_key_path=str(tmp_path / "k.key"),
        )
        async with _connected(gateway, provider):
            first = asyncio.create_task(provider.thread_messages("t1"))
            second = asyncio.create_task(provider.thread_messages("t2"))
            q1 = await gateway.next_frame()
            q2 = await gateway.next_frame()

            for query in (q2, q1):
                await gateway.push(
                    {
                        "type": "queryResult",
                        "queryId": query["queryId"],
                        "result": [{"thread": query["params"]["threadId"]}],
                    }
                )

            assert await asyncio.wait_for(first, RECEIVE_TIMEOUT) == [{"thread": "t1"}]
            assert await asyncio.wait_for(second, RECEIVE_TIMEOUT) == [{"thread": "t2"}]


async def test_an_error_answer_raises_rather_than_returning_nothing(tmp_path):
    """`[]` is a real answer — a thread with nothing in it — so a failure
    that returned it would have an agent summarise an empty history as if
    it were the conversation."""
    async with StubGateway() as gateway:
        provider = _provider(
            gateway,
            [AgentHandle(name="echo", run_stream=_echo_run_stream)],
            identity_key_path=str(tmp_path / "k.key"),
        )
        async with _connected(gateway, provider):
            asked = asyncio.create_task(provider.thread_messages("t_not_mine"))
            query = await gateway.next_frame()
            await gateway.push(
                {
                    "type": "queryResult",
                    "queryId": query["queryId"],
                    "error": "no such thread for this provider",
                }
            )
            with pytest.raises(SoukQueryFailed, match="no such thread"):
                await asyncio.wait_for(asked, RECEIVE_TIMEOUT)


async def test_a_socket_that_dies_fails_its_outstanding_queries_at_once(tmp_path):
    """Not left to time out. The answer is already known, and a caller
    waiting the full timeout for a certainty is only a slower failure —
    and unlike a run, a query is not retried on the next connection: the
    agent asked mid-run, and whether it still wants the answer is the
    agent's to decide."""
    async with StubGateway() as gateway:
        provider = _provider(
            gateway,
            [AgentHandle(name="echo", run_stream=_echo_run_stream)],
            identity_key_path=str(tmp_path / "k.key"),
        )
        provider.runtime.start()
        conn = asyncio.create_task(provider._run_connection())
        try:
            async with asyncio.timeout(RECEIVE_TIMEOUT):
                await gateway.connected.wait()
            asked = asyncio.create_task(provider.thread_messages("t1"))
            await gateway.next_frame()

            await gateway._conn.close()

            with pytest.raises(SoukQueryFailed, match="closed"):
                await asyncio.wait_for(asked, RECEIVE_TIMEOUT)
        finally:
            conn.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await conn
            await provider.runtime.aclose(cancel_in_flight=True)


async def test_an_answer_to_a_question_nobody_is_waiting_on_is_dropped(tmp_path):
    """A late reply — its caller timed out, or its socket already failed
    it — must not take the connection down with it."""
    async with StubGateway() as gateway:
        provider = _provider(
            gateway,
            [AgentHandle(name="echo", run_stream=_echo_run_stream)],
            identity_key_path=str(tmp_path / "k.key"),
        )
        async with _connected(gateway, provider):
            await gateway.push(
                {"type": "queryResult", "queryId": "never-asked", "result": []}
            )
            # Still serving.
            await gateway.push(
                {
                    "type": "run",
                    "runId": "r1",
                    "threadId": "t1",
                    "agentName": "echo",
                    "input": {"runId": "r1"},
                }
            )
            assert (await gateway.next_frame())["type"] == "ack"
