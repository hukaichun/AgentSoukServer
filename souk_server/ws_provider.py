"""WS /ws/provider: the socket a provider connects out on.

This file is transport and nothing else. souk states the provider
contract itself now (`souk_provider_sdk`), and `broker.ConnectedProvider`
is the whole of what souk needs to know about anybody: who you are, how
to hand you a run, how to ask you to stop one. `SocketProvider` below is
that, with a WebSocket underneath — the gateway's half of a contract
whose other half is `souk_provider_sdk.ProviderRuntime`, running behind
the same socket in souk-agent-sdk.

**souk hands work over; it does not ask for it.** There is no claim loop
here any more, on either side: the broker finds whoever serves an agent
and offers each run, `deliver` writes it to the wire, and the ack frame
that comes back is the return value. Declining is how a full provider
says so, and souk keeps the run.

Two things the inversion deleted rather than moved. Liveness is no longer
a heartbeat — `online` is `is_serving`, so being attached *is* being
online and a socket that drops takes its agents offline at once. And
there is no session token: the hello frame is signed by the provider's
own key, so nothing bearer-shaped exists to leak or to expire underneath
a long-lived connection.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from typing import TYPE_CHECKING, Any

from fastapi import APIRouter, WebSocket

from souk.errors import AgentNotFound
from souk.identity import is_timestamp_fresh, verify_signature
from souk_provider_sdk import SoukConnection
from souk_server.ws_common import (
    POLICY_VIOLATION,
    parse_frame,
    receive_hello,
    write_loop,
)

if TYPE_CHECKING:
    from souk.core import Souk

logger = logging.getLogger("souk.ws_provider")

router = APIRouter()

# A backstop, and deliberately longer than the deadline that actually
# governs. souk wraps every offer in `RunBroker.deliver_timeout_seconds`
# (5s) because it has one delivery loop and an offer that never returns
# stops dispatch for everybody — so souk gives up first, and this never
# fires in a souk that sets a deadline. Shortening it to "win" would put
# the same policy in two places and let them drift; deleting it would
# leave a wait with no bound at all if souk ever offers without one.
ACK_TIMEOUT_SECONDS = 30.0


def connect_signing_payload(public_key: str, agent_names: list[str], timestamp: int) -> bytes:
    """What a provider signs to open a socket.

    Deliberately *not* `souk.identity.registration_signing_payload`, even
    though it covers the same facts: reusing it would make a captured
    registration signature replayable as a connection, and within the
    freshness window that is someone else's runs delivered to you. The
    prefix is what keeps the two apart.

    The format is this gateway's, not core's — signing a socket open is a
    serving act, and souk supplies only the primitives it is built from
    (`verify_signature`, `is_timestamp_fresh`).
    """
    names = ",".join(sorted(agent_names))
    return f"souk-provider-connect:{public_key}:{names}:{timestamp}".encode()


class SocketProvider(SoukConnection):
    """`broker.ConnectedProvider` with a socket underneath.

    Subclasses the SDK's base rather than duck-typing the four members,
    which is the point of that base existing: souk sizes a capacity
    bucket from `max_concurrent_runs`, and a connection that forgets it
    attaches perfectly well and then fails inside the broker. Here it
    fails at construction instead.

    Holds no per-run state beyond the acks it is waiting on: every frame
    names its run, and souk keeps the only routing table.
    """

    def __init__(
        self, public_key: str, outbound: asyncio.Queue, max_concurrent_runs: int | None
    ) -> None:
        self._public_key = public_key
        self._max_concurrent_runs = max_concurrent_runs
        self._outbound = outbound
        self._acks: dict[str, asyncio.Future[bool]] = {}

    @property
    def public_key(self) -> str:
        return self._public_key

    @property
    def max_concurrent_runs(self) -> int | None:
        return self._max_concurrent_runs

    async def offer(self, run: Any) -> bool:
        """Write this run to the wire and wait for the answer.

        `offer` rather than `deliver`: the base class does the mapping
        from souk's `ClaimedRun` to the SDK's `DeliveredRun`, which is
        the one place either side's field names are allowed to appear.
        A transport that overrode `deliver` would be naming souk's shape
        again — which is exactly how the first provider to be handed a
        run died, on `input_json`.

        Answering late is the same as declining, whichever deadline ran
        out: the ack arrives for a run nobody is waiting on any more, and
        `ack` drops it. souk keeps the run either way.
        """
        loop = asyncio.get_running_loop()
        waiter: asyncio.Future[bool] = loop.create_future()
        self._acks[run.run_id] = waiter
        self._outbound.put_nowait(
            {
                "type": "run",
                "runId": run.run_id,
                "threadId": run.thread_id,
                "agentName": run.agent_name,
                "input": run.run_input,
            }
        )
        try:
            return await asyncio.wait_for(waiter, timeout=ACK_TIMEOUT_SECONDS)
        except asyncio.TimeoutError:
            logger.warning(
                "provider %s did not answer the offer of run %s in %.0fs — treating as declined",
                self.public_key,
                run.run_id,
                ACK_TIMEOUT_SECONDS,
            )
            return False
        finally:
            self._acks.pop(run.run_id, None)

    def ack(self, run_id: str, accepted: bool) -> None:
        waiter = self._acks.get(run_id)
        if waiter is not None and not waiter.done():
            waiter.set_result(accepted)

    def cancel(self, run_id: str) -> None:
        """Ask, and do not wait. souk decides the outcome from what the
        run's stream does next, not from anything this returns."""
        self._outbound.put_nowait({"type": "cancel", "runId": run_id})

    def fail_pending(self) -> None:
        """The socket is gone: nothing can answer these offers."""
        for waiter in self._acks.values():
            if not waiter.done():
                waiter.set_result(False)
        self._acks.clear()


def _bearer_free_hello_error(hello: dict[str, Any]) -> str | None:
    if not isinstance(hello.get("publicKey"), str):
        return "hello needs a publicKey"
    if not isinstance(hello.get("signature"), str):
        return "hello needs a signature"
    if not isinstance(hello.get("timestamp"), int):
        return "hello needs an integer timestamp"
    names = hello.get("agentNames")
    if not (isinstance(names, list) and names and all(isinstance(n, str) for n in names)):
        return "agentNames must be a non-empty list of strings"
    max_runs = hello.get("maxConcurrentRuns")
    if max_runs is not None and not isinstance(max_runs, int):
        return "maxConcurrentRuns must be an integer"
    return None


@router.websocket("/ws/provider")
async def provider_socket(websocket: WebSocket) -> None:
    souk: "Souk" = websocket.app.state.souk

    await websocket.accept()
    hello = await receive_hello(websocket)
    if hello is None:
        return

    problem = _bearer_free_hello_error(hello)
    if problem:
        await websocket.close(code=POLICY_VIOLATION, reason=problem)
        return

    public_key = hello["publicKey"]
    agent_names = hello["agentNames"]
    timestamp = hello["timestamp"]
    if not is_timestamp_fresh(timestamp):
        await websocket.close(code=POLICY_VIOLATION, reason="stale connect timestamp")
        return
    if not verify_signature(
        public_key, hello["signature"], connect_signing_payload(public_key, agent_names, timestamp)
    ):
        await websocket.close(code=POLICY_VIOLATION, reason="connect signature does not verify")
        return

    outbound: asyncio.Queue = asyncio.Queue()
    provider = SocketProvider(public_key, outbound, hello.get("maxConcurrentRuns"))
    # Queued *before* attaching, and that is not cosmetic. Attaching is what
    # makes this provider reachable, and the broker starts offering the
    # moment it is — inside `attach_provider`'s own awaits. Queue the welcome
    # afterwards and a provider with queued work receives a `run` frame as
    # the first thing after its hello, which is not the handshake either
    # side's docs describe: souk-agent-sdk reads exactly one frame, sees a
    # run where it expected a welcome, raises, and reconnects into the same
    # race forever.
    #
    # Nothing is written yet — the writer task starts below — so a failed
    # attach still closes without ever sending this.
    outbound.put_nowait({"type": "welcome"})
    try:
        # Registration is the prerequisite, and core enforces it: a name
        # this key never registered is refused here rather than being
        # served by nobody later.
        await souk.attach_provider(provider, agent_names)
    except (AgentNotFound, ValueError) as e:
        await websocket.close(code=POLICY_VIOLATION, reason=str(e))
        return

    writer = asyncio.create_task(write_loop(websocket, outbound))
    try:
        while True:
            frame = await websocket.receive()
            if frame["type"] == "websocket.disconnect":
                break
            parsed = parse_frame(frame)
            if parsed is None:
                outbound.put_nowait({"type": "error", "message": "unparseable frame"})
                continue
            kind = parsed.get("type")
            run_id = parsed.get("runId")
            if kind == "ack":
                provider.ack(run_id, bool(parsed.get("accepted", True)))
            elif kind == "event":
                if not souk.report_event(run_id, parsed.get("event"), claimed_by=public_key):
                    outbound.put_nowait(
                        {"type": "error", "runId": run_id, "message": "event refused"}
                    )
            elif kind == "finish":
                if not souk.finish_run(run_id, claimed_by=public_key):
                    outbound.put_nowait(
                        {"type": "error", "runId": run_id, "message": "finish refused"}
                    )
            else:
                outbound.put_nowait({"type": "error", "message": f"unexpected frame {kind!r}"})
    finally:
        provider.fail_pending()
        # Detaching is what takes these agents offline, immediately —
        # `online` is `is_serving`, so there is no window where souk still
        # advertises an agent whose socket has gone.
        await souk.detach_provider(public_key)
        writer.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await writer
