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

Nothing bearer-shaped is involved. The socket is opened with a signature
from the provider's own key over `souk-provider-connect:…`, so there is
no token to leak, and none to expire underneath a long-lived connection.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import ssl
import time
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

from souk_agent_sdk.identity import load_or_create_identity

logger = logging.getLogger("souk_agent_sdk")

# How long to wait for `welcome` before giving up and reconnecting.
WELCOME_TIMEOUT_SECONDS = 10.0


def connect_signing_payload(public_key: str, agent_names: list[str], timestamp: int) -> bytes:
    """What this side signs to open a socket.

    Must match the gateway's `souk_server.ws_provider.connect_signing_payload`
    byte for byte. Deliberately distinct from the registration payload —
    reusing that would make a captured registration signature replayable as
    a connection, which inside the freshness window is somebody else's runs
    delivered here.
    """
    names = ",".join(sorted(agent_names))
    return f"souk-provider-connect:{public_key}:{names}:{timestamp}".encode()


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
    ) -> None:
        # One URL is the whole address: registration posts to it, and the
        # socket is the same listener with the scheme swapped.
        self.souk_http_url = souk_http_url.rstrip("/")
        self.reconnect_delay = reconnect_delay
        self.ca_cert_path = ca_cert_path
        self.provider_name = provider_name
        # The key is loaded here rather than through
        # `ProviderIdentity.load_or_create` because this transport has to
        # sign something upstream does not define: the connect payload
        # below. `ProviderIdentity` signs registrations, deletions and
        # delegation hops — every payload souk itself verifies — and
        # exposes no general `sign`, which is right for a package that
        # names no transport and awkward for one that is a transport.
        # Constructing it from a key we already hold costs nothing and
        # reaches into nothing private. See AgentSouk#43.
        self._private_key = load_or_create_identity(identity_key_path)
        self.identity = ProviderIdentity(self._private_key)
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

    async def _run_connection(self) -> None:
        ssl_context: ssl.SSLContext | None = None
        if self._ws_url.startswith("wss") and self.ca_cert_path:
            ssl_context = ssl.create_default_context(cafile=self.ca_cert_path)

        names = sorted(self.agents)
        timestamp = int(time.time())
        signature = self._private_key.sign(
            connect_signing_payload(self.public_key, names, timestamp)
        ).hex()

        async with websockets.connect(self._ws_url, ssl=ssl_context) as ws:
            await ws.send(
                json.dumps(
                    {
                        "type": "hello",
                        "publicKey": self.public_key,
                        "signature": signature,
                        "timestamp": timestamp,
                        "agentNames": names,
                        "maxConcurrentRuns": self.runtime.max_concurrent_runs,
                    }
                )
            )
            welcome = json.loads(await asyncio.wait_for(ws.recv(), WELCOME_TIMEOUT_SECONDS))
            if welcome.get("type") != "welcome":
                raise RuntimeError(f"expected welcome, got {welcome!r}")

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
        accepted = await self.runtime.deliver(run)
        self._outbound.put_nowait(
            {"type": "ack", "runId": run.run_id, "accepted": accepted}
        )

    async def _write_loop(self, ws) -> None:
        while True:
            frame = await self._outbound.get()
            await ws.send(json.dumps(frame))
