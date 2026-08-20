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

from souk.errors import AgentNotFound, InvalidRegistration
from souk.models import AgentRef
from pydantic import ValidationError
from souk_provider_sdk import CONNECTED_PROVIDER_ATTRS, DeliveredRun, Refusal
from souk_provider_sdk.contract import LINK_QUERY_METHODS
from souk_server.handshake import HANDSHAKE_VERSION, souk_connect_payload
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

# What a provider may ask souk, and it is deliberately short. Upstream's
# `contract.LINK_QUERY_METHODS` states the rule: this is not a mirror of
# souk's API, because every method admitted here is one more frame type
# every transport has to carry. Read from upstream rather than retyped, so
# a method added there without a frame here fails a test instead of a
# provider.
QUERY_METHODS = frozenset(LINK_QUERY_METHODS)

# How often a live socket re-asks whether souk still lists the agents it
# attached for. The condition it catches is rare and permanent, so
# noticing it a minute late costs nothing — see `_watch_registration`.
OWNERSHIP_RECHECK_SECONDS = 120.0

# What this socket accepts after the handshake. The dispatch below reads
# it, and docs/wire-vectors.json publishes it — one set, asserted equal in
# tests, so a frame type added in code without a vectors row goes red.
INBOUND_FRAME_TYPES = frozenset({"ack", "event", "finish", "query"})


class SocketProvider:
    """`broker.ConnectedProvider` with a socket underneath.

    **Not a `SoukLink`,** and upstream's own docstring says so. A link is
    one provider joined to one souk, both directions in one object; this
    lives on souk's side, holds an outbound queue and no runtime, and only
    ever carries work *outward*. The object opposite it — the socket
    client in souk-agent-sdk — is the one that subclasses `SoukLink`,
    because it really does own both halves of the connection.

    So the four members below are duck-typed against souk's own
    `ConnectedProvider` protocol rather than inherited. That loses the
    fails-at-construction property a base class gave, which is why the
    constructor asserts against `CONNECTED_PROVIDER_ATTRS` instead: souk
    sizes a capacity bucket from `max_concurrent_runs`, and a connection
    that forgets it attaches perfectly well and then fails inside the
    broker, three layers from the cause.

    Holds no per-run state beyond the acks it is waiting on: every frame
    names its run, and souk keeps the only routing table.
    """

    def __init__(
        self, public_key: str, outbound: asyncio.Queue, max_concurrent_runs: int | None
    ) -> None:
        # Against the class, not the instance: `public_key` and
        # `max_concurrent_runs` are properties, so asking `self` would run
        # their getters against fields this constructor has not assigned
        # yet and report every one of them missing.
        missing = sorted(a for a in CONNECTED_PROVIDER_ATTRS if not hasattr(type(self), a))
        if missing:
            raise TypeError(f"{type(self).__name__} is not a ConnectedProvider: missing {missing}")
        self._public_key = public_key
        self._max_concurrent_runs = max_concurrent_runs
        self._outbound = outbound
        self._acks: dict[str, asyncio.Future[bool | Refusal]] = {}

    @property
    def public_key(self) -> str:
        return self._public_key

    @property
    def max_concurrent_runs(self) -> int | None:
        return self._max_concurrent_runs

    async def deliver(self, run: Any) -> bool | Refusal:
        """Write this run to the wire and wait for the answer.

        The frame is `{"type": "run"}` plus the wire form upstream
        declares — `DeliveredRun.from_claimed(...).model_dump(by_alias=
        True)` — so this gateway no longer hand-writes the mapping and the
        far side rebuilds with `model_validate` instead of picking fields.
        `from_claimed` also owns the validation rule: input that does not
        parse as `RunAgentInput` is a permanent `Refusal`, answered here
        without ever touching the wire (souk built the input, so this
        firing means a core bug or a version skew — either way permanent).

        Answering late is the same as declining, whichever deadline ran
        out: the ack arrives for a run nobody is waiting on any more, and
        `ack` drops it. souk keeps the run either way.
        """
        try:
            delivered = DeliveredRun.from_claimed(run)
        except ValidationError as e:
            return Refusal(f"input does not validate as RunAgentInput: {e}")
        loop = asyncio.get_running_loop()
        waiter: asyncio.Future[bool | Refusal] = loop.create_future()
        self._acks[run.run_id] = waiter
        self._outbound.put_nowait(
            {"type": "run", **delivered.model_dump(by_alias=True, mode="json")}
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

    def ack(self, run_id: str, accepted: bool, reason: str | None = None) -> None:
        """A declined ack carrying a `reason` is a *permanent* refusal — the
        provider saying re-offering can never succeed (an input that does
        not parse, most importantly). souk fails the run with the reason
        recorded verbatim and stops re-offering; a bare decline stays what
        it always was, \"full right now\". The wire says so with one
        optional field because the port says so with one optional type
        (`souk_provider_sdk.Refusal`, read duck-typed by the broker)."""
        waiter = self._acks.get(run_id)
        if waiter is not None and not waiter.done():
            if not accepted and isinstance(reason, str) and reason:
                waiter.set_result(Refusal(reason))
            else:
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


async def _answer_query(
    souk: "Souk", public_key: str, parsed: dict[str, Any], outbound: asyncio.Queue
) -> None:
    """One `query` frame, answered on the same socket by `queryId`.

    The first thing on this wire that is not fire-and-forget, and the
    reason it exists is a real gap rather than convenience: a provider
    sees exactly what the *caller* sent for its run and nothing more. An
    AG-UI client resends its whole history every turn by convention;
    A2A's `message/send` carries one message. The same agent, unchanged,
    cannot tell a tenth turn from a first — and souk has held the thread
    the whole time.

    **`limit` is applied here, not by the caller.** The parameter exists
    to keep the response frame bounded; trimming after receiving would
    bound nothing and put a months-old thread on the wire to do it.

    **A provider may only read threads for agents it serves**, which the
    upstream design did not call for and this adds. Thread ids are not
    guessable, but "not guessable" is not an authorization rule: a
    provider that served one run knows that thread id permanently, and
    would otherwise keep reading the conversation after being de-listed,
    or after the agent moved to somebody else's stall. The thread names
    its agent, and an agent is `(provider_key, name)`, so the check is a
    comparison souk can already make.
    """
    query_id = parsed.get("queryId")
    method = parsed.get("method")
    params = parsed.get("params") or {}

    def answer(**fields: Any) -> None:
        outbound.put_nowait({"type": "queryResult", "queryId": query_id, **fields})

    if not isinstance(query_id, str) or not query_id:
        outbound.put_nowait({"type": "error", "message": "query needs a queryId"})
        return
    if method not in QUERY_METHODS:
        answer(error=f"unknown query method {method!r}")
        return

    thread_id = params.get("threadId")
    limit = params.get("limit")
    if not isinstance(thread_id, str) or not thread_id:
        answer(error="thread_messages needs a threadId")
        return
    if limit is not None and not (isinstance(limit, int) and limit > 0):
        answer(error="limit must be a positive integer")
        return

    thread = await souk.get_thread(thread_id)
    if thread is None or thread["provider_key"] != public_key:
        # One answer for "no such thread" and "not yours", deliberately:
        # telling them apart would confirm a thread exists to somebody who
        # may not read it, which is the whole of what the check is for.
        answer(error="no such thread for this provider")
        return

    messages = await souk.get_thread_messages(thread_id)
    answer(result=messages[-limit:] if limit is not None else messages)


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


async def collect_connect_proof(
    websocket: WebSocket, souk: "Souk", hello: dict[str, Any]
) -> tuple[str, str] | None:
    """Frames two and three: souk answers the provider's nonce, then
    collects the provider's answer to its own. Returns `(challenge,
    proof)` for attach to verify, or None after closing the socket.

    The gateway stopped being the verifier here — that moved into core's
    attach, which is where runs change hands and therefore where the
    proof belongs (upstream's attach-proof change; in-process links are
    challenged the same way now, so this transport is no longer the only
    authenticated road in). What stays this side's: relaying the
    challenge core minted (`issue_connect_challenge` — single-use,
    freshness-bounded, so a recorded proof answers a question nobody is
    asking), and souk's own signature over `souk-connect-souk:...`, which
    core cannot send because core has no wire.

    souk signs first, and that ordering is the point of the exchange
    rather than an accident of it: a provider must be able to walk away
    from a souk it does not recognise *before* it has produced anything
    worth stealing. A souk with no identity configured cannot sign, and
    says so by sending `soukPublicKey: null` rather than by failing —
    honest, and the provider's pin is what turns it into a refusal.

    The challenge frame's `soukPublicKey` does double duty since v3: it
    is also the recipient the provider binds into its proof — core
    verifies against a payload built with its *own* key, so a proof this
    souk coaxed out cannot be relayed to attach at another. A provider
    facing an identity-less souk binds the empty string, matching what
    core builds when `identity_public_key` is None.

    Attach returns souk's answering signature over the same
    `souk-connect-souk:` bytes signed here — for transports where the
    proof arrives before souk has spoken. This transport already said it
    in the challenge frame (deterministic Ed25519: same key, same bytes,
    same signature), so the sockets discard the return rather than
    sending it twice.
    """
    souk_nonce = souk.issue_connect_challenge()
    provider_nonce = hello["nonce"]
    souk_public_key = souk.identity_public_key
    challenge: dict[str, Any] = {
        "type": "challenge",
        "soukPublicKey": souk_public_key,
        "nonce": souk_nonce,
        "signature": (
            souk.sign(souk_connect_payload(souk_nonce, provider_nonce))
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
        return None
    signature = proof.get("signature")
    if not isinstance(signature, str):
        await websocket.close(code=POLICY_VIOLATION, reason="proof needs a signature")
        return None
    return souk_nonce, signature


@router.websocket("/ws/provider")
async def provider_socket(websocket: WebSocket) -> None:
    souk: "Souk" = websocket.app.state.souk

    await websocket.accept()
    received = await receive_hello(websocket)
    if received is None:
        return
    hello, _hello_raw = received

    problem = _hello_error(hello)
    if problem:
        await websocket.close(code=POLICY_VIOLATION, reason=problem)
        return

    exchange = await collect_connect_proof(websocket, souk, hello)
    if exchange is None:
        return
    challenge, proof = exchange

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
        # Registration and the connect proof are both core's to enforce:
        # a name this key never registered, or a proof that does not
        # answer the live challenge, is refused here rather than served.
        await souk.attach_provider(
            provider,
            agent_names,
            challenge=challenge,
            provider_nonce=hello["nonce"],
            proof=proof,
        )
    except (AgentNotFound, InvalidRegistration, ValueError) as e:
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
            if kind not in INBOUND_FRAME_TYPES:
                outbound.put_nowait({"type": "error", "message": f"unexpected frame {kind!r}"})
            elif kind == "ack":
                provider.ack(
                    run_id,
                    bool(parsed.get("accepted", True)),
                    parsed.get("reason"),
                )
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
            elif kind == "query":
                # Spawned rather than awaited: a query hits the database,
                # and awaiting it here would stop this socket reading —
                # including the acks and events of every run in flight on
                # it — for the length of that read.
                asyncio.create_task(_answer_query(souk, public_key, parsed, outbound))
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
