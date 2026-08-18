"""The WebSocket a provider reaches souk on — transport, and nothing else.

What a provider *is* now comes from `souk_provider_sdk`: the identity and
what it signs, the port an agent satisfies, and the loop that runs the
work. This module is the carrier for exactly that, and the split is the
point — `ProviderRuntime` never learns it is on a wire, and this file
never learns what an agent does.

Two frames carry the hand-over, because souk hands work over rather than
being asked for it: a `run` arrives, and the `ack` this side sends back
is the runtime's own answer to whether it took it. Declining is how a
full provider says so, and the run stays souk's problem — which is the
only channel capacity has, and why nothing here counts anything.

Nothing bearer-shaped is involved, and nothing replayable either. The
socket opens with a mutual challenge-response: each side signs a nonce the
other chose, so a recorded handshake is worth nothing to whoever recorded
it, and the provider learns whether the thing answering the URL is the
souk it meant. Four frames — hello, challenge, proof, welcome — described
in `souk_server/handshake.py`, which is the spec both halves are written
against.

Pass `souk_public_key` to say which souk this provider will talk to. Left
unset, the provider still verifies that whatever answered holds the key it
presents, but not that it is the right key — enough to notice a broken
souk, not enough to notice a substituted one.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import secrets
import ssl
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit, urlunsplit

import httpx
import websockets
from pydantic import ValidationError
from souk_provider_sdk import (
    AgentHandle,
    DeliveredRun,
    HandleProvider,
    ProviderIdentity,
    ProviderRuntime,
    SoukLink,
    new_nonce,
    souk_connect_payload,
    verify_signature,
)


logger = logging.getLogger("souk_agent_sdk")

# How long to wait for `welcome` before giving up and reconnecting.
WELCOME_TIMEOUT_SECONDS = 10.0


# The handshake this side speaks. The frame choreography must match the
# gateway's `souk_server.handshake`; the *bytes signed* come from
# souk_provider_sdk's link-open family now (`provider_connect_payload` /
# `souk_connect_payload`), so this package no longer restates any payload
# — v2 is exactly that migration, and the digest-of-the-hello subtlety
# went with it.
HANDSHAKE_VERSION = 2

# How long an agent waits for souk to answer a question. Generous: it is a
# database read on the far side of a socket, and the failure it guards is
# a lost frame, not a slow one.
QUERY_TIMEOUT_SECONDS = 30.0


class SoukIdentityMismatch(Exception):
    """The souk at this URL is not the one this provider was told to trust.

    Raised rather than logged. The whole value of pinning a key is that a
    substituted souk is refused, and a provider that carried on after
    noticing would be pinning nothing.
    """


class SoukQueryFailed(Exception):
    """A question this provider asked souk did not come back.

    Raised rather than answered with an empty list, and the distinction is
    not pedantic: `thread_messages` returning `[]` is a real answer — a
    thread with nothing in it — and a caller that cannot tell that from
    "the socket died" will summarise an empty history as if it were the
    conversation.
    """


class SoukProvider(SoukLink):
    """One identity, its agents, and the socket between them and souk.

    A `SoukLink`, because over a wire this object genuinely is both
    directions: run frames arrive on the same socket that event frames
    leave by. The gateway's own `SocketProvider` is deliberately *not* one
    — it holds no runtime and only carries work outward.

    Constructing this attaches it to the runtime, so it must exist before
    the runtime is given work: events produced while the runtime has no
    link are dropped by design.
    """

    def __init__(
        self,
        souk_http_url: str,
        agents: list[AgentHandle],
        reconnect_delay: float = 2.0,
        max_concurrent_runs: int | None = None,
        identity_key_path: str = "souk_identity.key",
        ca_cert_path: str | None = None,
        provider_name: str | None = None,
        souk_public_key: str | None = None,
    ) -> None:
        # One URL is the whole address: registration posts to it, and the
        # socket is the same listener with the scheme swapped.
        self.souk_http_url = souk_http_url.rstrip("/")
        self.reconnect_delay = reconnect_delay
        self.ca_cert_path = ca_cert_path
        self.provider_name = provider_name
        # Which souk this provider will talk to, as a hex Ed25519 public
        # key. None means "whichever answers the URL" — see
        # `_check_souk_identity` for exactly what is and is not checked
        # then.
        self.souk_public_key = souk_public_key
        # `load_or_create` does not make the directory, and a provider's
        # key path is very often one it owns alone (`/data/…` on a fresh
        # volume, `./keys/…` in a checkout) — so this is the one thing
        # done before handing the path over.
        Path(identity_key_path).parent.mkdir(parents=True, exist_ok=True)
        self.identity = ProviderIdentity.load_or_create(identity_key_path)
        self.agents = {agent.name: agent for agent in agents}
        self._outbound: asyncio.Queue = asyncio.Queue()
        # Questions asked and not yet answered, by queryId. The only
        # per-request state on this side, and the reason the wire needs a
        # correlation id at all: everything else here is fire-and-forget.
        self._pending: dict[str, asyncio.Future] = {}
        # The provider's own loop, which knows nothing about any of this.
        # It reports through `self` — the link — and setting that is this
        # constructor's job, not the runtime's.
        self.runtime = ProviderRuntime(
            self.identity,
            HandleProvider(list(agents)),
            max_concurrent_runs=max_concurrent_runs,
        )
        self.runtime.link = self

    # ---- souk → provider

    @property
    def public_key(self) -> str:
        return self.identity.public_key

    @property
    def max_concurrent_runs(self) -> int | None:
        return self.runtime.max_concurrent_runs

    async def offer(self, run: DeliveredRun) -> bool:
        """souk offers a run. Never called on this side — a socket
        provider is offered work by a `run` frame, which `_offer` below
        turns into `runtime.deliver`. It exists because `SoukLink` names
        both directions and this is the half a wire routes differently."""
        return await self.runtime.deliver(run)

    def cancel(self, run_id: str) -> None:
        """souk is asking for a run to stop. A request, and this provider
        complies — the runtime cancels the task, which is the only way to
        interrupt an arbitrary async generator. Reached from the `cancel`
        frame; souk never calls it directly on this side of a wire."""
        self.runtime.cancel(run_id)

    # ---- provider → souk

    async def report_event(self, run_id: str, event: Any) -> None:
        self._outbound.put_nowait({"type": "event", "runId": run_id, "event": event})

    async def finish_run(self, run_id: str) -> None:
        self._outbound.put_nowait({"type": "finish", "runId": run_id})

    async def thread_messages(
        self, thread_id: str, *, limit: int | None = None
    ) -> list[dict[str, Any]]:
        """This thread's messages, oldest first — the one thing a provider
        cannot work out for itself.

        What arrives in `run_input` is exactly what the *caller* sent for
        this run: an AG-UI client resends its whole history every turn,
        while A2A's `message/send` carries one message. The same agent
        cannot tell a tenth turn from a first, and souk has held the
        thread all along.

        `limit` is sent rather than applied on return. The parameter
        exists to keep the response frame bounded, and trimming after
        receiving would have already put a months-old thread on the wire.
        """
        query_id = secrets.token_hex(8)
        loop = asyncio.get_running_loop()
        waiter: asyncio.Future = loop.create_future()
        self._pending[query_id] = waiter
        self._outbound.put_nowait(
            {
                "type": "query",
                "queryId": query_id,
                "method": "thread_messages",
                "params": {"threadId": thread_id, "limit": limit},
            }
        )
        try:
            return await asyncio.wait_for(waiter, timeout=QUERY_TIMEOUT_SECONDS)
        except asyncio.TimeoutError:
            raise SoukQueryFailed(
                f"souk did not answer thread_messages({thread_id}) in "
                f"{QUERY_TIMEOUT_SECONDS:.0f}s"
            ) from None
        finally:
            self._pending.pop(query_id, None)

    def _resolve_query(self, frame: dict[str, Any]) -> None:
        waiter = self._pending.get(frame.get("queryId"))
        if waiter is None or waiter.done():
            # An answer to a question nobody is waiting on — the query
            # timed out, or its socket died and it was already failed.
            # Dropping it is right: the caller has had its answer.
            return
        if frame.get("error") is not None:
            waiter.set_exception(SoukQueryFailed(str(frame["error"])))
        else:
            waiter.set_result(frame.get("result") or [])

    def _fail_pending_queries(self, reason: str) -> None:
        """The socket is gone: nothing can answer these.

        Failed rather than left to time out, because the answer is already
        known and an agent waiting the full timeout for a certainty is
        just a slower failure. Not retried on the next connection either:
        the agent asked in the middle of a run, and whether that run still
        wants the answer is the agent's to decide, not this queue's.
        """
        for waiter in self._pending.values():
            if not waiter.done():
                waiter.set_exception(SoukQueryFailed(reason))
        self._pending.clear()

    @property
    def _ws_url(self) -> str:
        scheme, netloc, path, query, fragment = urlsplit(self.souk_http_url)
        ws_scheme = "wss" if scheme == "https" else "ws"
        return urlunsplit((ws_scheme, netloc, f"{path.rstrip('/')}/ws/provider", query, fragment))

    async def register(self) -> None:
        """Prove this identity holds its key, and say what it offers.

        Nothing comes back that has to be kept: souk mints no ids, so this
        provider already knows everything it needs — its key, and the names
        it chose.
        """
        signature, timestamp = self.identity.sign_registration(sorted(self.agents))
        body: dict[str, Any] = {
            "agents": [agent.as_registration() for agent in self.agents.values()],
            "public_key": self.public_key,
            "signature": signature,
            "timestamp": timestamp,
        }
        if self.provider_name is not None:
            body["provider_name"] = self.provider_name
        async with httpx.AsyncClient(timeout=30.0, verify=self.ca_cert_path or True) as client:
            response = await client.post(f"{self.souk_http_url}/agents/register", json=body)
            response.raise_for_status()
        logger.info("registered %d agent(s) as provider %s", len(self.agents), self.public_key)

    async def run_forever(self) -> None:
        """Stay connected, reconnecting on anything that is not shutdown."""
        self.runtime.start()
        try:
            while True:
                try:
                    await self.register()
                    await self._run_connection()
                except asyncio.CancelledError:
                    raise
                except Exception:
                    logger.exception(
                        "souk connection lost; reconnecting in %.1fs", self.reconnect_delay
                    )
                await asyncio.sleep(self.reconnect_delay)
        finally:
            await self.runtime.aclose()

    async def _handshake(self, ws) -> None:
        """Four frames, and this side signs nothing until it has decided
        who it is talking to.

        The order is the point: souk answers our nonce first, so a provider
        can walk away from a souk it does not recognise *before* producing
        anything worth stealing. The old shape signed first and asked
        nobody, which is how a provider induced to connect to the wrong URL
        handed a working credential to whatever picked up.
        """
        provider_nonce = new_nonce()
        names = sorted(self.agents)
        await ws.send(
            json.dumps(
                {
                    "type": "hello",
                    "version": HANDSHAKE_VERSION,
                    "publicKey": self.public_key,
                    "agentNames": names,
                    "maxConcurrentRuns": self.runtime.max_concurrent_runs,
                    "nonce": provider_nonce,
                }
            )
        )

        challenge = json.loads(await asyncio.wait_for(ws.recv(), WELCOME_TIMEOUT_SECONDS))
        if challenge.get("type") != "challenge":
            raise RuntimeError(f"expected challenge, got {challenge!r}")
        souk_nonce = challenge.get("nonce")
        if not isinstance(souk_nonce, str) or not souk_nonce:
            raise RuntimeError("challenge carried no nonce")
        self._check_souk_identity(challenge, provider_nonce, souk_nonce)

        await ws.send(
            json.dumps(
                {
                    "type": "proof",
                    # The SDK's own statement of what a provider signs to
                    # open a link — no local payload, no hello digest.
                    "signature": self.identity.sign_connect(
                        souk_nonce, provider_nonce, names
                    ),
                }
            )
        )

        welcome = json.loads(await asyncio.wait_for(ws.recv(), WELCOME_TIMEOUT_SECONDS))
        if welcome.get("type") != "welcome":
            raise RuntimeError(f"expected welcome, got {welcome!r}")

    def _check_souk_identity(self, challenge: dict, provider_nonce: str, souk_nonce: str) -> None:
        """Is the thing answering this URL the souk we meant?

        Three states, and the difference between the last two is the whole
        reason `souk_public_key` exists:

        - **A souk with no identity.** It says so — `soukPublicKey: null` —
          which is honest and is what every souk did before this existed.
          Refused if we pinned a key, since a souk that cannot prove itself
          is not the one we pinned. Otherwise a warning: we are no worse
          off than before, and silence would hide a deployment that meant
          to configure one.
        - **A key we did not pin.** The signature is still checked, which
          proves whoever answered holds the key it presented — enough to
          notice a broken souk, not enough to notice a substituted one.
          Logged with the fingerprint so it can be eyeballed, and so the
          value to pin is in reach.
        - **A key we pinned.** Must match, and must sign. This is the case
          that makes a substituted souk fail instead of being trusted.

        TOFU — pinning whatever appears on first connect — is deliberately
        not built. It reads as free safety and is not: souk's key is
        provisioned, so any deployment that rotates or regenerates it jams
        every provider at once, and the recovery is to go and clear a pin
        on each of them. A configured key costs one line and has no such
        state.
        """
        souk_key = challenge.get("soukPublicKey")
        signature = challenge.get("signature")

        if souk_key is None:
            if self.souk_public_key is not None:
                raise SoukIdentityMismatch(
                    f"{self.souk_http_url} has no identity configured, so it cannot be the "
                    f"souk pinned as {self.souk_public_key[:16]}…"
                )
            logger.warning(
                "souk at %s has no identity, so this provider cannot tell it from any "
                "other souk at that URL",
                self.souk_http_url,
            )
            return

        if not isinstance(signature, str) or not verify_signature(
            souk_key, signature, souk_connect_payload(souk_nonce, provider_nonce)
        ):
            raise SoukIdentityMismatch(
                f"{self.souk_http_url} presented public key {souk_key[:16]}… but did not "
                "sign our nonce with it"
            )

        if self.souk_public_key is None:
            logger.warning(
                "trusting unverified souk identity %s at %s — pass souk_public_key to pin it",
                souk_key[:16],
                self.souk_http_url,
            )
        elif souk_key != self.souk_public_key:
            raise SoukIdentityMismatch(
                f"{self.souk_http_url} is souk {souk_key[:16]}…, not the "
                f"{self.souk_public_key[:16]}… this provider was told to trust"
            )

    async def _run_connection(self) -> None:
        ssl_context: ssl.SSLContext | None = None
        if self._ws_url.startswith("wss") and self.ca_cert_path:
            ssl_context = ssl.create_default_context(cafile=self.ca_cert_path)

        async with websockets.connect(self._ws_url, ssl=ssl_context) as ws:
            await self._handshake(ws)

            writer = asyncio.create_task(self._write_loop(ws))
            try:
                async for raw in ws:
                    frame = json.loads(raw)
                    kind = frame.get("type")
                    if kind == "run":
                        # The ack is the runtime's answer, not this file's
                        # opinion: it takes the run or it is full.
                        await self._offer(frame)
                    elif kind == "queryResult":
                        self._resolve_query(frame)
                    elif kind == "cancel":
                        self.cancel(frame.get("runId"))
                    elif kind == "error":
                        logger.warning("souk rejected a frame: %s", frame)
                    else:
                        logger.warning("unexpected frame from souk, ignoring: %s", frame)
            finally:
                # Queries die with the socket; runs do not. A run is
                # addressed by runId and its frames go out on whatever
                # connection is next, but a question was asked of *this*
                # connection and nothing will ever answer it.
                self._fail_pending_queries("souk connection closed")
                writer.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await writer

    async def _offer(self, frame: dict[str, Any]) -> None:
        run_id = frame.get("runId", "")
        agent_name = frame.get("agentName", "")
        # A name this provider does not host is declined here rather than
        # passed on. The runtime cannot answer it — `deliver` is a capacity
        # question and knows nothing about names — so accepting would start
        # a run that raises KeyError on its first step, report a bare
        # stream end, and have souk record it as *failed*. Declining leaves
        # it queued for a provider that does host the name, which is the
        # difference between "not me" and "broken" — and why this stays a
        # bare decline while a validation failure below carries a reason.
        #
        # souk should never send one: it offers a run only to a provider
        # attached for that agent. This is the answer for when it does.
        if agent_name not in self.agents:
            logger.warning(
                "declining run %s: this provider does not serve %r", run_id, agent_name
            )
            self._outbound.put_nowait({"type": "ack", "runId": run_id, "accepted": False})
            return
        # The frame *is* the declared envelope now — `DeliveredRun.
        # model_dump(by_alias=True)` on souk's side, rebuilt here with
        # `model_validate` (AgentSouk#74's answer). No field mapping on
        # either end; a frame that does not validate is a permanent
        # refusal, because the same bytes re-offered can never do better —
        # the rule `DeliveredRun.from_claimed` states in-process, met at
        # this transport's rebuild step.
        try:
            delivered = DeliveredRun.model_validate(frame)
        except ValidationError as e:
            reason = f"frame does not validate as a DeliveredRun: {e}"
            logger.warning("refusing run %s: %s", run_id, reason)
            self._outbound.put_nowait(
                {"type": "ack", "runId": run_id, "accepted": False, "reason": reason}
            )
            return
        accepted = await self.offer(delivered)
        self._outbound.put_nowait(
            {"type": "ack", "runId": run_id, "accepted": bool(accepted)}
        )

    async def _write_loop(self, ws) -> None:
        while True:
            frame = await self._outbound.get()
            await ws.send(json.dumps(frame))
