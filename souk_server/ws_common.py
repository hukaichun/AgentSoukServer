"""What the two WebSocket relays share: frame plumbing, not semantics.

Both sockets (`/ws/provider`, `/ws/kyok`) speak JSON text frames, open
with a `hello` that must arrive promptly, route every outbound frame —
including the close itself — through one writer task, and answer a bad
inbound frame with an `error` frame rather than a teardown. That much is
carrier, identical on both; everything each socket *means* stays in its
own module.
"""

from __future__ import annotations

import asyncio
import json
from typing import Any

from fastapi import WebSocket

# How long a fresh connection may sit silent before `hello` arrives. The
# hello track exists for browser clients that cannot set headers; a socket
# that connects and says nothing is not one of those, it is a leak.
HELLO_TIMEOUT_SECONDS = 5.0

# Close codes: 1008 (policy violation) for anything about credentials or
# the handshake, 1011 for a server-side failure the client didn't cause.
POLICY_VIOLATION = 1008
INTERNAL_ERROR = 1011

# Internal frame type routing a close through the writer task — never
# sent on the wire. Serialized like every other outbound frame so a close
# can't interleave with a half-written message.
CLOSE = "_close"


def close_frame(code: int, reason: str) -> dict[str, Any]:
    return {"type": CLOSE, "code": code, "reason": reason}


def parse_frame(message: dict[str, Any]) -> dict[str, Any] | None:
    """The JSON object in one raw ASGI receive message, or None if it
    isn't one (binary, unparseable, or not an object)."""
    text = message.get("text")
    if text is None:
        return None
    try:
        frame = json.loads(text)
    except json.JSONDecodeError:
        return None
    return frame if isinstance(frame, dict) else None


async def receive_hello(websocket: WebSocket) -> tuple[dict[str, Any], str] | None:
    """The handshake's server half, after accept: the first frame, which
    must be a prompt, well-formed `hello`. Returns `(frame, raw_text)`, or
    None after closing the socket — anything else before hello closes it
    (per docs/server-mode.md), so a caller only ever proceeds or returns.

    The raw text comes back beside the parsed frame because `/ws/provider`
    signs a digest of it. Re-serializing the parsed dict would not do:
    key order, separators and unicode escaping are all free choices, so
    two JSON encoders agreeing on the *value* can disagree on the bytes,
    and the signature would fail for reasons neither side could see. What
    was sent is the only thing both sides can hash.
    """
    try:
        message = await asyncio.wait_for(websocket.receive(), timeout=HELLO_TIMEOUT_SECONDS)
    except asyncio.TimeoutError:
        await websocket.close(code=POLICY_VIOLATION, reason="no hello frame")
        return None
    if message["type"] == "websocket.disconnect":
        return None
    hello = parse_frame(message)
    if hello is None or hello.get("type") != "hello":
        await websocket.close(code=POLICY_VIOLATION, reason="first frame must be hello")
        return None
    return hello, message["text"]


async def receive_frame(websocket: WebSocket, timeout: float = HELLO_TIMEOUT_SECONDS):
    """One more frame during a handshake, with the same deadline.

    Returns the parsed frame, or None if the socket closed or sent
    something unparseable — the caller decides what a missing frame means,
    because mid-handshake that differs by which frame was expected.
    """
    try:
        message = await asyncio.wait_for(websocket.receive(), timeout=timeout)
    except asyncio.TimeoutError:
        return None
    if message["type"] == "websocket.disconnect":
        return None
    return parse_frame(message)


async def write_loop(websocket: WebSocket, outbound: asyncio.Queue) -> None:
    """Single writer serializing every outbound frame — pushes, error
    replies, and the close itself — because concurrent sends on one ASGI
    websocket are not safe to interleave."""
    while True:
        frame = await outbound.get()
        if frame.get("type") == CLOSE:
            await websocket.close(code=frame["code"], reason=frame["reason"])
            return
        await websocket.send_text(json.dumps(frame))
