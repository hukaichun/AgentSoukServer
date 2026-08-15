"""WS /ws/provider: the worker relay, per docs/server-mode.md.

This file is transport and nothing else — the third carrier for the same
three-call worker port (claim_work / report_event / finish_run) plus a
cancel push, after in-process and gRPC. What a run *means* lives in core;
this file frames JSON text messages onto one duplex socket and hands
everything else to `Souk`.

The inversion the gRPC servicer performed is kept, but moves further in:
under gRPC the *SDK* drove PollForWork; here the server drives the claim
loop on the worker's behalf, pushing what it claims. The worker's whole
vocabulary is the frame table in docs/server-mode.md — `hello` in, then
`run`/`cancel`/`error` down and `event`/`finish` up.

Nothing here keeps per-run state, for the same reason the gRPC file
didn't: every frame names its run, core holds the only routing table, and
a dropped socket therefore ends nothing — the worker reconnects (a fresh
`hello`) and reports the rest. The one piece of connection state that
does exist, the in-flight claim budget, is deliberately per-socket: it is
flow control for *this* connection's pushes, not an account of what the
worker holds.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from collections import defaultdict
from functools import partial
from typing import TYPE_CHECKING, Any

from fastapi import APIRouter, WebSocket

from souk.errors import InvalidRegistration
from souk.identity import verify_session_token
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

logger = logging.getLogger("souk.ws_provider")

router = APIRouter()

# One cycle of the server-side claim loop: how long each claim_work call
# long-polls before coming back empty. Also the liveness heartbeat — every
# cycle marks this worker's agents seen, exactly as PollForWork did.
CLAIM_WAIT_SECONDS = 25.0


class WorkerSessions:
    """Which connected providers souk can currently reach, by public key.

    Transplanted from the gRPC servicer, and for the same single message —
    "please stop run X" — because a cancel has to reach a *connection*, and
    the claim that created the obligation may have happened on a socket
    that is gone by the time souk asks. Keyed by identity rather than by
    connection: both sockets of a provider running two processes (or one
    briefly holding two across a reconnect) are the same provider, either
    may hold the run, and asking both is harmless — a socket without the
    run ignores the frame (the SDK drops frames for unknown run_ids).
    """

    def __init__(self) -> None:
        self._outbound: dict[str, set[asyncio.Queue]] = defaultdict(set)

    def add(self, public_key: str, outbound: asyncio.Queue) -> None:
        self._outbound[public_key].add(outbound)

    def remove(self, public_key: str, outbound: asyncio.Queue) -> None:
        self._outbound[public_key].discard(outbound)
        if not self._outbound[public_key]:
            del self._outbound[public_key]

    def notify_cancel(self, public_key: str, run_id: str) -> None:
        """Queue souk's cancel request for the wire. Best-effort by design:
        returning says the request was queued, never that anything stopped —
        the outcome is decided by what the run's stream does next."""
        queues = self._outbound.get(public_key)
        if not queues:
            logger.info("cancel for run %s: no open socket for %s", run_id, public_key)
            return
        for outbound in queues:
            outbound.put_nowait({"type": "cancel", "runId": run_id})


def _bearer(websocket: WebSocket) -> str:
    header = websocket.headers.get("authorization", "")
    return header[len("Bearer ") :] if header.startswith("Bearer ") else header


async def _claim_loop(
    souk: "Souk",
    sessions: WorkerSessions,
    token: str,
    public_key: str,
    agent_ids: list[str],
    max_claim: int | None,
    in_flight: set[str],
    credit: asyncio.Event,
    outbound: asyncio.Queue,
) -> None:
    """The server driving claim_work on the worker's behalf. Flow control
    is the `maxClaim` budget, enforced where it always was — further runs
    are claimed only while in-flight (claimed − finished, on this socket)
    is under it. No credit frames; `finish` is the credit (the reader sets
    `credit` when one arrives).
    """
    while True:
        remaining = None
        if max_claim is not None:
            credit.clear()
            # Re-check after clear, not before: a finish landing between
            # the check and the clear must not be lost with the event.
            remaining = max_claim - len(in_flight)
            if remaining <= 0:
                await credit.wait()
                continue
        try:
            runs = await souk.claim_work(
                token,
                agent_ids,
                max_claim=remaining,
                wait_seconds=CLAIM_WAIT_SECONDS,
                on_cancel=partial(sessions.notify_cancel, public_key),
            )
        except InvalidRegistration as e:
            # The session token aged out under a long-lived socket. Close
            # with policy — the SDK's reconnect re-registers, which is how
            # tokens were always refreshed.
            outbound.put_nowait(close_frame(POLICY_VIOLATION, str(e)))
            return
        except Exception:
            logger.exception("claim loop for %s failed", public_key)
            outbound.put_nowait(close_frame(INTERNAL_ERROR, "claim loop failed"))
            return
        for run in runs:
            in_flight.add(run.run_id)
            outbound.put_nowait(
                {
                    "type": "run",
                    "runId": run.run_id,
                    "threadId": run.thread_id,
                    "agentId": run.agent_id,
                    "input": run.run_input,
                }
            )


@router.websocket("/ws/provider")
async def provider_socket(websocket: WebSocket) -> None:
    souk: "Souk" = websocket.app.state.souk
    sessions: WorkerSessions = websocket.app.state.worker_sessions

    # Dual-track auth (docs/server-mode.md): a header when the client can
    # set one — which is also what lets an edge gate the handshake before
    # accepting — the first frame when it can't. Accepting first is the
    # hello track's cost; an embedder wanting pre-accept rejection puts a
    # pure ASGI middleware in front (BaseHTTPMiddleware never sees
    # websocket scopes).
    await websocket.accept()
    hello = await receive_hello(websocket)
    if hello is None:
        return

    header_token = _bearer(websocket)
    hello_token = hello.get("token") or ""
    if header_token and hello_token and header_token != hello_token:
        await websocket.close(code=POLICY_VIOLATION, reason="header and hello tokens differ")
        return
    token = hello_token or header_token
    public_key = verify_session_token(token, souk.settings.token_signing_secret) if token else None
    if public_key is None:
        await websocket.close(code=POLICY_VIOLATION, reason="missing or invalid session token")
        return

    agent_ids = hello.get("agentIds") or []
    max_claim = hello.get("maxClaim")
    if not (isinstance(agent_ids, list) and all(isinstance(a, str) for a in agent_ids)):
        await websocket.close(code=POLICY_VIOLATION, reason="agentIds must be a list of strings")
        return
    if max_claim is not None and not isinstance(max_claim, int):
        await websocket.close(code=POLICY_VIOLATION, reason="maxClaim must be an integer")
        return

    outbound: asyncio.Queue = asyncio.Queue()
    in_flight: set[str] = set()
    credit = asyncio.Event()
    sessions.add(public_key, outbound)
    outbound.put_nowait({"type": "welcome"})

    writer = asyncio.create_task(write_loop(websocket, outbound))
    claimer = asyncio.create_task(
        _claim_loop(
            souk, sessions, token, public_key, agent_ids, max_claim, in_flight, credit, outbound
        )
    )
    try:
        while True:
            frame = await websocket.receive()
            if frame["type"] == "websocket.disconnect":
                break
            parsed = await _parse_worker_frame(frame)
            if parsed is None:
                outbound.put_nowait({"type": "error", "message": "unparseable frame"})
                continue
            ftype, run_id, event = parsed
            if ftype == "event":
                if run_id is None or not souk.report_event(run_id, event, claimed_by=public_key):
                    outbound.put_nowait(
                        {"type": "error", "runId": run_id, "message": "event rejected: unknown run or not the claimer"}
                    )
            elif ftype == "finish":
                if run_id is None or not souk.finish_run(run_id, claimed_by=public_key):
                    outbound.put_nowait(
                        {"type": "error", "runId": run_id, "message": "finish rejected: unknown run or not the claimer"}
                    )
                # The budget only ever tracked this socket's claims: a
                # finish for a run claimed on a previous connection is
                # forwarded above but is not this socket's credit.
                if run_id in in_flight:
                    in_flight.discard(run_id)
                    credit.set()
            else:
                outbound.put_nowait(
                    {"type": "error", "message": f"unknown frame type {ftype!r}"}
                )
    finally:
        claimer.cancel()
        writer.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await claimer
        with contextlib.suppress(asyncio.CancelledError):
            await writer
        sessions.remove(public_key, outbound)
        # Deliberately nothing else: this socket going away is not evidence
        # that the runs it carried have ended. The worker may reconnect and
        # report them; one that is truly gone is caught by the stall sweep.


async def _parse_worker_frame(message: dict[str, Any]) -> tuple[str, str | None, Any] | None:
    """(type, runId, event) off one raw ASGI message, or None if it isn't
    JSON text. Split out so the reader loop above stays about semantics."""
    frame = parse_frame(message)
    if frame is None:
        return None
    ftype = frame.get("type")
    if not isinstance(ftype, str):
        return None
    run_id = frame.get("runId")
    return ftype, run_id if isinstance(run_id, str) else None, frame.get("event")
