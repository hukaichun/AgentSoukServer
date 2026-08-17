"""WS /ws/kyok: the completion relay, per docs/server-mode.md.

Replaces the `GET /kyok/poll` + `POST /kyok/respond/{id}` pair. The
provider-facing `POST /kyok/v1/chat/completions` endpoint is untouched —
an OpenAI-compatible URL is the whole point of that side.

What KYOK *means* — the two-part authorization on the provider side, the
relay queues, reassembly — stays in core (souk/protocols/kyok.py). This
file frames it: it pushes queued completion requests down the socket and
feeds answer frames back through the same `KyokAdapter.respond` the HTTP
endpoint used, as the NDJSON lines that call already speaks.

The `sessionId` in `hello` is a routing key, the same one `/kyok/poll`
took — souk neither mints nor verifies it (see souk/kyok.py), because
souk has no caller identity to bind it to; *who* may present a session is
the deployment's business, enforced at the edge (pure ASGI middleware,
before accept). What this socket adds is the binding the HTTP pair never
had: answer frames are accepted only for requests *delivered on this
socket*, so a `request_id` stops being a bearer capability on an open
endpoint and becomes a multiplexing key within the connection that
received it. Sockets sharing a session coexist, completion requests going
to whichever polls first — the same race the HTTP poll had; tightening
that to one-bridge-per-session waits on a real bridge credential, which
is a caller-identity question souk deliberately doesn't own.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from typing import TYPE_CHECKING, Any

from fastapi import APIRouter, WebSocket

from souk.errors import KyokRejected
from souk.protocols.kyok import KyokAdapter
from souk_server.ws_common import (
    INTERNAL_ERROR,
    POLICY_VIOLATION,
    close_frame,
    parse_frame,
    receive_hello,
    write_loop,
)

if TYPE_CHECKING:
    from souk.core import Souk

logger = logging.getLogger("souk.ws_kyok")

router = APIRouter()

# One cycle of the server-side poll loop — how long each poll waits for a
# completion to be queued before coming back empty and going again.
POLL_WAIT_SECONDS = 25.0


class _Relay:
    """One in-flight completion's answer path: the same
    `KyokAdapter.respond` call the HTTP endpoint made, fed frame by frame
    instead of by a request body. `respond` takes chunks and already
    understands `{"error": ...}` as "fail this completion", so nothing
    about the relay's semantics — incremental consumption, the done
    sentinel, error short-circuit — is reimplemented here.

    Chunks, not NDJSON. This used to serialise each frame and hand
    `respond` the bytes so it could parse them straight back, with no
    network in between: framing invented in order to be undone one call
    away. Core changed the port and the encoding drops out.
    """

    def __init__(self, souk: "Souk", request_id: str) -> None:
        self.request_id = request_id
        self._queue: asyncio.Queue[dict[str, Any] | None] = asyncio.Queue()
        self._task = asyncio.create_task(self._run(souk))

    async def _run(self, souk: "Souk") -> None:
        try:
            await KyokAdapter(souk).respond(self.request_id, self._chunks())
        except KyokRejected as e:
            # The completion is already gone — timed out waiting (see
            # CLAIM_TIMEOUT_SECONDS) or abandoned. Nothing to route the
            # answer to; the bridge finds out when its next frame for this
            # request_id gets an error frame (alive() below).
            logger.info("kyok ws relay %s: %s", self.request_id, e)

    async def _chunks(self):
        while (chunk := await self._queue.get()) is not None:
            yield chunk

    def alive(self) -> bool:
        return not self._task.done()

    def feed(self, message: dict[str, Any]) -> None:
        self._queue.put_nowait(message)

    def finish(self) -> None:
        self._queue.put_nowait(None)

    async def abandon(self) -> None:
        """The socket died mid-answer. A truncated answer must fail the
        completion, not complete it — feed the error chunk `respond`
        treats as exactly that, then let the task drain."""
        if self.alive():
            self.feed({"error": "kyok bridge disconnected mid-response"})
            self.finish()
        with contextlib.suppress(Exception):
            await self._task


async def _poll_loop(
    souk: "Souk", session_id: str, relays: dict[str, _Relay], outbound: asyncio.Queue
) -> None:
    """The server polling on the bridge's behalf — what `poll` returned is
    pushed instead. One socket serves concurrent completions: each gets
    its own relay, keyed by requestId, and the loop goes straight back to
    polling while answers stream in."""
    adapter = KyokAdapter(souk)
    while True:
        try:
            item = await adapter.poll(session_id, POLL_WAIT_SECONDS)
        except Exception:
            logger.exception("kyok poll loop for session %s failed", session_id)
            outbound.put_nowait(close_frame(INTERNAL_ERROR, "poll loop failed"))
            return
        if item is None:
            continue
        request_id = item["requestId"]
        relays[request_id] = _Relay(souk, request_id)
        outbound.put_nowait(
            {"type": "completionRequest", "requestId": request_id, "payload": item["body"]}
        )


@router.websocket("/ws/kyok")
async def kyok_socket(websocket: WebSocket) -> None:
    souk: "Souk" = websocket.app.state.souk

    await websocket.accept()
    received = await receive_hello(websocket)
    hello = received[0] if received else None
    if hello is None:
        return
    session_id = hello.get("sessionId")
    if not (isinstance(session_id, str) and session_id):
        await websocket.close(code=POLICY_VIOLATION, reason="hello must carry a sessionId")
        return

    outbound: asyncio.Queue = asyncio.Queue()
    # requestIds delivered on *this* socket, each with its live answer
    # path. This set is the binding described in the module docstring —
    # membership, not any credential a frame carries, is what authorizes
    # an answer.
    relays: dict[str, _Relay] = {}
    outbound.put_nowait({"type": "welcome"})

    writer = asyncio.create_task(write_loop(websocket, outbound))
    poller = asyncio.create_task(_poll_loop(souk, session_id, relays, outbound))
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
            relay = relays.get(request_id) if isinstance(request_id, str) else None
            if relay is None or not relay.alive():
                # Not delivered on this socket (or already over): the one
                # rejection that used to be an open door — see module
                # docstring.
                outbound.put_nowait(
                    {
                        "type": "error",
                        "requestId": request_id,
                        "message": "no such in-flight completion on this connection",
                    }
                )
                continue
            if ftype == "chunk":
                data = frame.get("data")
                if not isinstance(data, dict):
                    outbound.put_nowait(
                        {"type": "error", "requestId": request_id, "message": "chunk data must be an object"}
                    )
                    continue
                relay.feed(data)
            elif ftype == "done":
                relay.finish()
                del relays[request_id]
            else:  # error: the bridge failing fast beats the timeout
                relay.feed({"error": frame.get("message") or "kyok bridge reported an error"})
                relay.finish()
                del relays[request_id]
    finally:
        poller.cancel()
        writer.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await poller
        with contextlib.suppress(asyncio.CancelledError):
            await writer
        for relay in list(relays.values()):
            await relay.abandon()
