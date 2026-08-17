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
import hashlib
import json
import logging
import secrets
import ssl
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit, urlunsplit

import httpx
import websockets
from souk_provider_sdk import (
    AgentHandle,
    DeliveredRun,
    HandleProvider,
    ProviderIdentity,
    ProviderRuntime,
)

from souk_agent_sdk.identity import verify_signature

logger = logging.getLogger("souk_agent_sdk")

# How long to wait for `welcome` before giving up and reconnecting.
WELCOME_TIMEOUT_SECONDS = 10.0


# The handshake this side speaks. Must match the gateway's
# `souk_server.handshake` — the two are stated separately rather than
# shared because this package must not import the gateway, and a mismatch
# is loud: the handshake fails on the first connection, in tests, not
# quietly later.
HANDSHAKE_VERSION = 1
NONCE_BYTES = 32


def souk_challenge_payload(provider_nonce: str, souk_nonce: str) -> bytes:
    """What souk signs, and this side verifies to know who answered."""
    return f"souk-auth:souk:{provider_nonce}:{souk_nonce}".encode()


def provider_proof_payload(provider_nonce: str, souk_nonce: str, hello_raw: str) -> bytes:
    """What this side signs, over bytes souk partly chose.

    `hello_raw` is the exact text sent, hashed rather than re-serialized:
    key order and separators are free choices in JSON, so re-encoding the
    same values can produce different bytes and a signature that fails for
    a reason neither side can see.
    """
    hello_digest = hashlib.sha256(hello_raw.encode()).hexdigest()
    return f"souk-auth:provider:{provider_nonce}:{souk_nonce}:{hello_digest}".encode()


class SoukIdentityMismatch(Exception):
    """The souk at this URL is not the one this provider was told to trust.

    Raised rather than logged. The whole value of pinning a key is that a
    substituted souk is refused, and a provider that carried on after
    noticing would be pinning nothing.
    """


class SoukProvider:
    """One identity, its agents, and the socket between them and souk."""

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
        # The provider's own loop, which knows nothing about any of this.
        # Results leave through the two callbacks: it hands back a run_id
        # and an event, and turning those into frames is this file's whole
        # remaining job.
        self.runtime = ProviderRuntime(
            self.identity,
            HandleProvider(list(agents)),
            on_event=self._on_event,
            on_finish=self._on_finish,
            max_concurrent_runs=max_concurrent_runs,
        )

    @property
    def public_key(self) -> str:
        return self.identity.public_key

    def _on_event(self, run_id: str, event: Any) -> None:
        self._outbound.put_nowait({"type": "event", "runId": run_id, "event": event})

    def _on_finish(self, run_id: str) -> None:
        self._outbound.put_nowait({"type": "finish", "runId": run_id})

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
        provider_nonce = secrets.token_hex(NONCE_BYTES)
        hello_raw = json.dumps(
            {
                "type": "hello",
                "version": HANDSHAKE_VERSION,
                "publicKey": self.public_key,
                "agentNames": sorted(self.agents),
                "maxConcurrentRuns": self.runtime.max_concurrent_runs,
                "nonce": provider_nonce,
            }
        )
        # The exact text is kept, not the dict: the proof signs a digest of
        # what went on the wire, and re-serializing could produce different
        # bytes for the same values.
        await ws.send(hello_raw)

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
                    "signature": self.identity.sign(
                        provider_proof_payload(provider_nonce, souk_nonce, hello_raw)
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
            souk_key, signature, souk_challenge_payload(provider_nonce, souk_nonce)
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
                    elif kind == "cancel":
                        self.runtime.cancel(frame.get("runId"))
                    elif kind == "error":
                        logger.warning("souk rejected a frame: %s", frame)
                    else:
                        logger.warning("unexpected frame from souk, ignoring: %s", frame)
            finally:
                writer.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await writer

    async def _offer(self, frame: dict[str, Any]) -> None:
        run = DeliveredRun(
            run_id=frame["runId"],
            agent_name=frame.get("agentName", ""),
            run_input=frame.get("input") or {},
            thread_id=frame.get("threadId"),
        )
        # A name this provider does not host is declined here rather than
        # passed on. The runtime cannot answer it — `deliver` is a capacity
        # question and knows nothing about names — so accepting would start
        # a run that raises KeyError on its first step, report a bare
        # stream end, and have souk record it as *failed*. Declining leaves
        # it queued for a provider that does host the name, which is the
        # difference between "not me" and "broken".
        #
        # souk should never send one: it offers a run only to a provider
        # attached for that agent. This is the answer for when it does.
        if run.agent_name not in self.agents:
            logger.warning(
                "declining run %s: this provider does not serve %r",
                run.run_id,
                run.agent_name,
            )
            self._outbound.put_nowait({"type": "ack", "runId": run.run_id, "accepted": False})
            return
        accepted = await self.runtime.deliver(run)
        self._outbound.put_nowait(
            {"type": "ack", "runId": run.run_id, "accepted": accepted}
        )

    async def _write_loop(self, ws) -> None:
        while True:
            frame = await self._outbound.get()
            await ws.send(json.dumps(frame))
