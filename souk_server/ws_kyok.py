"""WS /ws/kyok: the socket an LLM provider connects out on.

The party on the far end changed, and this file changed with it. This
socket used to carry an anonymous "bridge" that rendezvoused with souk
over a caller-minted sessionId — the only actor in the system with no
identity, which upstream's KYOK redesign names as the root of every
failure that design had (AgentSouk/docs/keep-your-own-key.md, "History").
The answering party is now an **LLM provider**: a first-class provider
kind with the same Ed25519 identity machinery as an agent provider. It
registers model offerings (`POST /llm-providers/register`, payload prefix
`souk-register-llm`), then connects here and attaches as the live server
for the offerings it names — `attach_llm_provider`, the mirror of
`attach_provider` rule for rule, registration enforced the same way.

So this socket now opens exactly like `/ws/provider`: the same four-frame
mutual challenge-response (see handshake.py), with `modelNames` in the
hello where the provider socket says `agentNames`. The signed digest of
the hello binds the claimed names; fresh nonces on both sides make a
recorded exchange worthless.

What flows afterwards is the completion relay, inverted from the old
poll: core resolves a run's binding to an attached link per call
(`KyokAdapter.complete`) and calls `complete()` on it; this file writes
that request down the socket as a `completionRequest` frame and feeds
`chunk`/`done`/`error` frames back as the `ChatCompletionChunk` stream
core is iterating. One socket serves concurrent completions, multiplexed
by `requestId`.

What survived from the old socket, because it was the security fix worth
keeping: **an answer is accepted only on the connection its request was
delivered to.** Membership in this connection's in-flight table — not
anything a frame carries — is what authorizes an answer, so a requestId
is a multiplexing key within the connection that received it, never a
bearer capability on an open endpoint.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from collections.abc import AsyncIterator
from typing import TYPE_CHECKING, Any

from fastapi import APIRouter, WebSocket
from openai.types.chat import ChatCompletionChunk

from souk.errors import LlmProviderNotFound
from souk.ids import new_id
from souk_llm_provider_sdk import CONNECTED_LLM_PROVIDER_ATTRS
from souk_server.handshake import HANDSHAKE_VERSION
from souk_server.ws_common import (
    POLICY_VIOLATION,
    parse_frame,
    receive_hello,
    write_loop,
)
from souk_server.ws_provider import prove_and_verify

if TYPE_CHECKING:
    from souk.core import Souk
    from souk.kyok import CompletionRequest

logger = logging.getLogger("souk.ws_kyok")

router = APIRouter()

# The longest a completion waits for the *next* frame of its answer. Not a
# per-completion deadline — a long generation streams for as long as it
# streams — but a gap this long means the provider is gone in a way the
# socket has not noticed, and the agent's HTTP call must fail rather than
# hang on it.
CHUNK_GAP_TIMEOUT_SECONDS = 120.0

# Sentinel closing one completion's answer queue.
_DONE = object()


class SocketLLMProvider:
    """`souk.kyok.ConnectedLLMProvider` with a WebSocket underneath.

    Duck-typed against core's protocol, like `SocketProvider` beside it,
    and asserted against upstream's `CONNECTED_LLM_PROVIDER_ATTRS` in the
    constructor for the same reason: an attribute core expects but this
    forgets would attach fine and fail inside the relay, three layers
    from the cause.

    Holds one queue per in-flight completion, keyed by the requestId this
    side minted. That table is connection-scoped on purpose — it *is* the
    binding described in the module docstring.
    """

    def __init__(self, public_key: str, outbound: asyncio.Queue) -> None:
        missing = sorted(
            a for a in CONNECTED_LLM_PROVIDER_ATTRS if not hasattr(type(self), a)
        )
        if missing:
            raise TypeError(
                f"{type(self).__name__} is not a ConnectedLLMProvider: missing {missing}"
            )
        self._public_key = public_key
        self._outbound = outbound
        self._answers: dict[str, asyncio.Queue[Any]] = {}

    @property
    def public_key(self) -> str:
        return self._public_key

    def complete(self, request: "CompletionRequest") -> AsyncIterator[ChatCompletionChunk]:
        """Write `request` to the wire and return the stream of its answer.

        The frame goes out here, not in the generator, so the request is
        on the wire the moment core holds the iterator — before anything
        awaits it. Field names are this frame's own mapping from core's
        `CompletionRequest`, confined to this one place the same way
        `SocketProvider.deliver` confines the run frame's.
        """
        request_id = new_id("kyokreq")
        queue: asyncio.Queue[Any] = asyncio.Queue()
        self._answers[request_id] = queue
        self._outbound.put_nowait(
            {
                "type": "completionRequest",
                "requestId": request_id,
                "runId": request.run_id,
                "providerKey": request.agent.provider_key,
                "agentName": request.agent.name,
                "llmName": request.llm_name,
                "context": request.context,
                "actorChain": request.actor_chain,
                "payload": request.body,
            }
        )
        return self._answer_stream(request_id, queue)

    async def _answer_stream(
        self, request_id: str, queue: asyncio.Queue[Any]
    ) -> AsyncIterator[ChatCompletionChunk]:
        """One completion's answer, frame by frame. Raising is how this
        side fails the completion — core's `CompletionRelay` turns it into
        a 502 or an in-band error, so nothing here needs to know which
        shape the caller asked for. A chunk that is not a valid
        `ChatCompletionChunk` fails the same way: what an LLM provider
        returns is untrusted input, and core relays what this yields
        as-is.
        """
        try:
            while True:
                try:
                    item = await asyncio.wait_for(queue.get(), CHUNK_GAP_TIMEOUT_SECONDS)
                except asyncio.TimeoutError:
                    raise RuntimeError(
                        "LLM provider stopped answering mid-completion"
                    ) from None
                if item is _DONE:
                    return
                if isinstance(item, Exception):
                    raise item
                yield ChatCompletionChunk.model_validate(item)
        finally:
            self._answers.pop(request_id, None)

    def feed(self, request_id: str, item: Any) -> bool:
        """Route one inbound frame's payload to its completion. False if
        this connection was never delivered `request_id` (or it is over) —
        the refusal that used to be an open door."""
        queue = self._answers.get(request_id)
        if queue is None:
            return False
        queue.put_nowait(item)
        return True

    def fail_pending(self) -> None:
        """The socket is gone: a truncated answer must fail its
        completion, not complete it, and fail it now rather than at the
        gap timeout."""
        for queue in self._answers.values():
            queue.put_nowait(RuntimeError("LLM provider disconnected mid-response"))
        self._answers.clear()


def _hello_error(hello: dict[str, Any]) -> str | None:
    """What an LLM provider's hello must carry to be worth challenging.

    Same checks and same ordering rationale as the provider socket's:
    version first, by name; nothing signed until the frame is worth it.
    """
    version = hello.get("version")
    if version != HANDSHAKE_VERSION:
        if version is None:
            return (
                "hello has no version: this souk speaks handshake "
                f"v{HANDSHAKE_VERSION}, a mutual challenge-response"
            )
        return f"unsupported handshake version {version!r}; this souk speaks v{HANDSHAKE_VERSION}"
    if not isinstance(hello.get("publicKey"), str):
        return "hello needs a publicKey"
    if not isinstance(hello.get("nonce"), str) or not hello["nonce"]:
        return "hello needs a nonce"
    names = hello.get("modelNames")
    if not (isinstance(names, list) and names and all(isinstance(n, str) for n in names)):
        return "modelNames must be a non-empty list of strings"
    return None


@router.websocket("/ws/kyok")
async def kyok_socket(websocket: WebSocket) -> None:
    souk: "Souk" = websocket.app.state.souk

    await websocket.accept()
    received = await receive_hello(websocket)
    if received is None:
        return
    hello, hello_raw = received

    problem = _hello_error(hello)
    if problem:
        await websocket.close(code=POLICY_VIOLATION, reason=problem)
        return

    if not await prove_and_verify(websocket, souk, hello, hello_raw):
        return

    public_key = hello["publicKey"]
    model_names = hello["modelNames"]

    outbound: asyncio.Queue = asyncio.Queue()
    link = SocketLLMProvider(public_key, outbound)
    # Before attaching, for the same reason the provider socket queues its
    # welcome first: attaching makes this link resolvable, and a
    # completion could be delivered inside `attach_llm_provider`'s own
    # awaits. Nothing is written until the writer task starts, so a failed
    # attach still closes without ever sending this.
    outbound.put_nowait({"type": "welcome"})
    try:
        # Registration is the prerequisite and core enforces it — a model
        # name this key never registered is refused here.
        await souk.attach_llm_provider(link, model_names)
    except (LlmProviderNotFound, ValueError) as e:
        await websocket.close(code=POLICY_VIOLATION, reason=str(e))
        return

    writer = asyncio.create_task(write_loop(websocket, outbound))
    try:
        while True:
            message = await websocket.receive()
            if message["type"] == "websocket.disconnect":
                break
            frame = parse_frame(message)
            if frame is None:
                outbound.put_nowait({"type": "error", "message": "unparseable frame"})
                continue
            ftype = frame.get("type")
            request_id = frame.get("requestId")
            if ftype not in ("chunk", "done", "error"):
                outbound.put_nowait(
                    {"type": "error", "message": f"unknown frame type {ftype!r}"}
                )
                continue
            if not isinstance(request_id, str):
                outbound.put_nowait(
                    {"type": "error", "message": "frame needs a requestId"}
                )
                continue
            if ftype == "chunk":
                data = frame.get("data")
                if not isinstance(data, dict):
                    outbound.put_nowait(
                        {
                            "type": "error",
                            "requestId": request_id,
                            "message": "chunk data must be an object",
                        }
                    )
                    continue
                accepted = link.feed(request_id, data)
            elif ftype == "done":
                accepted = link.feed(request_id, _DONE)
            else:  # error: the provider failing fast beats the gap timeout
                accepted = link.feed(
                    request_id,
                    RuntimeError(
                        frame.get("message") or "LLM provider reported an error"
                    ),
                )
            if not accepted:
                # Not delivered on this socket, or already over — see the
                # module docstring for why membership is the check.
                outbound.put_nowait(
                    {
                        "type": "error",
                        "requestId": request_id,
                        "message": "no such in-flight completion on this connection",
                    }
                )
    finally:
        souk.detach_llm_provider(public_key)
        link.fail_pending()
        writer.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await writer
