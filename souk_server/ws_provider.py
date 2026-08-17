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
there is no session token: nothing bearer-shaped exists to leak or to
expire underneath a long-lived connection.

Opening the socket is a mutual challenge-response — four frames, both
sides signing bytes the other chose. See `handshake.py` for the payloads
and for what the self-signed assertion it replaced could not do.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
from typing import TYPE_CHECKING, Any

from fastapi import APIRouter, WebSocket

from souk.errors import AgentNotFound
from souk.identity import verify_signature
from souk.models import AgentRef
from souk_provider_sdk import SoukConnection
from souk_server.handshake import (
    HANDSHAKE_VERSION,
    new_nonce,
    provider_proof_payload,
    souk_challenge_payload,
)
from souk_server.ws_common import (
    POLICY_VIOLATION,
    close_frame,
    parse_frame,
    receive_frame,
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

# How often a live socket re-asks whether souk still lists the agents it
# attached for. The condition it catches is rare and permanent, so
# noticing it a minute late costs nothing — see `_watch_registration`.
OWNERSHIP_RECHECK_SECONDS = 120.0


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


async def _watch_registration(
    souk: "Souk", public_key: str, agent_names: list[str], outbound: asyncio.Queue
) -> None:
    """Close this socket if souk stops listing every agent it serves.

    souk checks registration once, when the provider attaches, and the
    broker then holds the mapping in memory. So a registration that
    disappears *underneath* a live socket — a restored database, a
    de-listing, a souk redeployed against a fresh one — leaves the broker
    serving an agent souk's own roster no longer has. Nothing can route to
    it, because addressing it needs a row that is gone; nothing complains,
    because the socket is fine. A healthy container and an invisible agent,
    indefinitely.

    That is the same failure the retired claim loop had, and it survived
    the inversion rather than being fixed by it: `claim_work` used to
    filter unowned ids and carry on, which was correct in itself and left
    a worker looping forever against agents that no longer existed.
    Observed once already — a database restored under a running provider
    left it invisible for half an hour, with one server-side warning per
    cycle and nothing at all on its own side.

    Closing is the repair, not a punishment. Registration is what puts the
    name back and the SDK re-registers on every reconnect, so a provider
    told goodbye here comes back listed. Only when *every* name has gone:
    losing one of several is a de-listing somebody meant, and the socket
    still has work to do for the rest.

    Asked per name through `get_agent`, which takes the pair and therefore
    answers the ownership half by itself. It could not before — an
    `agent_id` did not carry its owner, so the old version of this had to
    scan the whole roster to ask the same question.
    """
    refs = [AgentRef(provider_key=public_key, name=name) for name in agent_names]
    while True:
        await asyncio.sleep(OWNERSHIP_RECHECK_SECONDS)
        listed = [ref for ref in refs if await souk.get_agent(ref) is not None]
        if listed:
            continue
        logger.warning(
            "provider %s is attached for agent(s) souk no longer lists (%s) — "
            "closing so its reconnect re-registers",
            public_key[:16],
            agent_names,
        )
        outbound.put_nowait(
            close_frame(POLICY_VIOLATION, "souk no longer lists these agents; re-register")
        )
        return


def _hello_error(hello: dict[str, Any]) -> str | None:
    """What a hello must carry to be worth challenging.

    Checked before souk signs anything, so an unparseable frame cannot
    make it produce a signature over attacker-chosen bytes. The version is
    first because a provider on the old two-frame shape has no `version`
    at all, and saying so by name is far more use than the bad-signature
    error it would otherwise get — which is what an attack looks like too.
    """
    version = hello.get("version")
    if version != HANDSHAKE_VERSION:
        if version is None:
            return (
                "hello has no version: this souk speaks handshake "
                f"v{HANDSHAKE_VERSION}, a mutual challenge-response. Upgrade "
                "souk-agent-sdk."
            )
        return f"unsupported handshake version {version!r}; this souk speaks v{HANDSHAKE_VERSION}"
    if not isinstance(hello.get("publicKey"), str):
        return "hello needs a publicKey"
    if not isinstance(hello.get("nonce"), str) or not hello["nonce"]:
        return "hello needs a nonce"
    names = hello.get("agentNames")
    if not (isinstance(names, list) and names and all(isinstance(n, str) for n in names)):
        return "agentNames must be a non-empty list of strings"
    max_runs = hello.get("maxConcurrentRuns")
    if max_runs is not None and not isinstance(max_runs, int):
        return "maxConcurrentRuns must be an integer"
    return None


async def _prove_and_verify(
    websocket: WebSocket, souk: "Souk", hello: dict[str, Any], hello_raw: str
) -> bool:
    """Frames two and three: souk answers the provider's nonce, then checks
    the provider's answer to its own. True if the provider proved itself.

    Closes the socket itself on every failure — there is exactly one way
    past this function, which is what keeps a half-authenticated
    connection from existing.

    souk signs first, and that ordering is the point of the exchange
    rather than an accident of it: a provider must be able to walk away
    from a souk it does not recognise *before* it has produced anything
    worth stealing. Signing second would mean handing a credential to
    whatever answered the URL and only then asking who it was.

    A souk with no identity configured cannot sign, and says so by sending
    a challenge with `soukPublicKey: null` rather than by failing. That is
    an honest report of today's deployment, and it is the provider's to
    act on — one that pinned a key refuses; one that pinned nothing is no
    worse off than it was before this existed.
    """
    souk_nonce = new_nonce()
    provider_nonce = hello["nonce"]
    souk_public_key = souk.identity_public_key
    challenge: dict[str, Any] = {
        "type": "challenge",
        "soukPublicKey": souk_public_key,
        "nonce": souk_nonce,
        "signature": (
            souk.sign(souk_challenge_payload(provider_nonce, souk_nonce))
            if souk_public_key is not None
            else None
        ),
    }
    if souk_public_key is None:
        logger.warning(
            "this souk has no identity, so provider %s cannot tell it from any other — "
            "set SOUK_IDENTITY_PRIVATE_KEY",
            hello["publicKey"][:16],
        )
    await websocket.send_text(json.dumps(challenge))

    proof = await receive_frame(websocket)
    if proof is None or proof.get("type") != "proof":
        await websocket.close(code=POLICY_VIOLATION, reason="expected a proof frame")
        return False
    signature = proof.get("signature")
    if not isinstance(signature, str):
        await websocket.close(code=POLICY_VIOLATION, reason="proof needs a signature")
        return False
    if not verify_signature(
        hello["publicKey"],
        signature,
        provider_proof_payload(provider_nonce, souk_nonce, hello_raw),
    ):
        await websocket.close(code=POLICY_VIOLATION, reason="proof does not verify")
        return False
    return True


@router.websocket("/ws/provider")
async def provider_socket(websocket: WebSocket) -> None:
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

    if not await _prove_and_verify(websocket, souk, hello, hello_raw):
        return

    public_key = hello["publicKey"]
    agent_names = hello["agentNames"]

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
    watcher = asyncio.create_task(
        _watch_registration(souk, public_key, agent_names, outbound)
    )
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
        for task in (watcher, writer):
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
