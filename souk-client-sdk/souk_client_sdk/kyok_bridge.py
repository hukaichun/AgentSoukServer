"""Optional Keep Your Own Key bridge: lets a souk-client-sdk caller pay
for a run's LLM usage with their own key instead of leaving that to the
agent provider. Purely additive — a caller that never touches this module
is simply not offering KYOK; `SoukClient.run()` alone is a complete,
ordinary caller either way. See docs/keep-your-own-key.md in the souk
repo for the design, and the gateway repo's docs/server-mode.md for the
wire protocol this speaks.

**This is an LLM provider now.** Upstream retired the anonymous
session-keyed bridge — the one actor in the system with no identity, and
the root of that design's failures — and made the answering party a
first-class provider kind. So this bridge holds an Ed25519 keypair
(`souk_llm_provider_sdk.ProviderIdentity`), registers a model offering
under it (`register()`, payload prefix `souk-register-llm`), and opens
`/ws/kyok` with the same four-frame mutual challenge-response an agent
provider uses. The caller then opts a run in by naming the offering —
`run_metadata()` builds exactly that — instead of minting a session id.

The transport after the handshake is what it always was: one WebSocket,
completion requests pushed down it, the real LLM's chunks streamed back
as frames multiplexed by requestId. An answer is only accepted on the
socket its request was delivered to, so a reconnect starts fresh:
completions in flight on a dead socket are failed by souk immediately
rather than retried here.

**Experimental**, same status as before: no state survives a crash; if
this process dies mid-run the run's provider sees errors, with no
retry/resume path on either end.

Uses litellm (https://github.com/BerriAI/litellm) to actually call the
real LLM, so this bridge isn't tied to one provider — model strings are
litellm's own ("anthropic/claude-...", "gemini/...", "openai/...", a
custom OpenAI-compatible `api_base`, ...) and its streaming chunks are
already OpenAI-shaped, which is exactly what souk's relay expects: no
translation layer needed here, just forward what litellm already gives us.
"""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import json
import logging
import secrets
from typing import Any
from urllib.parse import urlsplit, urlunsplit

import httpx
import litellm
import websockets
from souk_llm_provider_sdk import ProviderIdentity, sign_llm_registration

logger = logging.getLogger("souk_client_sdk.kyok_bridge")

WELCOME_TIMEOUT_SECONDS = 10.0

# The gateway's handshake, restated: this package must not import the
# gateway, and a mismatch fails loudly on the first connection. Keep in
# lockstep with souk_server/handshake.py — same rule souk-agent-sdk
# follows for the same four frames.
HANDSHAKE_VERSION = 1
NONCE_BYTES = 32


def provider_proof_payload(provider_nonce: str, souk_nonce: str, hello_raw: str) -> bytes:
    hello_digest = hashlib.sha256(hello_raw.encode()).hexdigest()
    return f"souk-auth:provider:{provider_nonce}:{souk_nonce}:{hello_digest}".encode()


class KyokBridge:
    """One KYOK LLM provider serving one model offering with the caller's
    own key. Typical use: `await bridge.register()`, pass
    `metadata=bridge.run_metadata()` to `SoukClient.run()`, and run
    `serve_forever()` as a background task alongside consuming that run's
    event stream — cancel it once the run finishes.

    `offering` is the model name callers address —
    `(identity.public_key, offering)` is the offering exactly as
    `(provider_key, name)` is an agent. `model`/`api_key`/`api_base` are
    what this bridge actually calls with, litellm-side, and souk never
    sees them.
    """

    def __init__(
        self,
        souk_http_url: str,
        model: str,
        api_key: str,
        *,
        api_base: str | None = None,
        offering: str = "kyok",
        identity: ProviderIdentity | None = None,
        reconnect_delay: float = 2.0,
    ) -> None:
        self.souk_http_url = souk_http_url.rstrip("/")
        self.model = model
        self.api_key = api_key
        self.api_base = api_base
        self.offering = offering
        # Ephemeral by default: a personal bridge's identity only needs to
        # outlive its runs. Pass a persisted one to keep a stable
        # provider_key across restarts.
        self.identity = identity or ProviderIdentity.generate()
        self.reconnect_delay = reconnect_delay
        self.registered = False

    @property
    def _ws_url(self) -> str:
        scheme, netloc, path, _query, _fragment = urlsplit(self.souk_http_url)
        ws_scheme = "wss" if scheme == "https" else "ws"
        return urlunsplit((ws_scheme, netloc, path.rstrip("/") + "/ws/kyok", "", ""))

    async def register(self) -> None:
        """Register this bridge's offering with souk, signed by its own
        key — the prerequisite the socket's attach enforces. Idempotent;
        re-registering refreshes the record."""
        signature, timestamp = sign_llm_registration(self.identity, [self.offering])
        async with httpx.AsyncClient() as client:
            resp = await client.post(
                f"{self.souk_http_url}/llm-providers/register",
                json={
                    "models": [self.offering],
                    "public_key": self.identity.public_key,
                    "signature": signature,
                    "timestamp": timestamp,
                },
            )
            resp.raise_for_status()
        self.registered = True

    def run_metadata(self, context: Any = None) -> dict[str, Any]:
        """The `metadata` a caller passes to opt a run into this bridge:
        names the offering, and carries `context` — opaque to souk,
        stripped before anything persists, delivered back to this bridge
        on every completion the run (and its delegation tree) makes."""
        kyok: dict[str, Any] = {
            "llmProvider": {
                "providerKey": self.identity.public_key,
                "name": self.offering,
            }
        }
        if context is not None:
            kyok["context"] = context
        return {"kyok": kyok}

    async def serve_forever(self) -> None:
        """Holds the `/ws/kyok` socket and serves every completion souk
        pushes down it — calls the real LLM via litellm using this
        bridge's own api_key, and streams the response back as frames.
        Reconnects on a drop. Runs until cancelled; intended to be wrapped
        in `asyncio.create_task` alongside the run it's serving, not
        awaited to completion (a KYOK bridge has no natural end of its own
        — the run it's serving does).
        """
        assert self.registered, "call register() before serve_forever()"
        while True:
            try:
                await self._serve_connection()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception(
                    "kyok bridge connection lost; reconnecting in %.1fs", self.reconnect_delay
                )
            await asyncio.sleep(self.reconnect_delay)

    async def _handshake(self, ws) -> None:
        nonce = secrets.token_hex(NONCE_BYTES)
        hello_raw = json.dumps(
            {
                "type": "hello",
                "version": HANDSHAKE_VERSION,
                "publicKey": self.identity.public_key,
                "modelNames": [self.offering],
                "nonce": nonce,
            }
        )
        await ws.send(hello_raw)
        challenge = json.loads(await asyncio.wait_for(ws.recv(), WELCOME_TIMEOUT_SECONDS))
        if challenge.get("type") != "challenge":
            raise RuntimeError(f"expected challenge, got {challenge!r}")
        await ws.send(
            json.dumps(
                {
                    "type": "proof",
                    "signature": self.identity.sign(
                        provider_proof_payload(nonce, challenge["nonce"], hello_raw)
                    ),
                }
            )
        )
        welcome = json.loads(await asyncio.wait_for(ws.recv(), WELCOME_TIMEOUT_SECONDS))
        if welcome.get("type") != "welcome":
            raise RuntimeError(f"expected welcome, got {welcome!r}")

    async def _serve_connection(self) -> None:
        async with websockets.connect(self._ws_url) as ws:
            await self._handshake(ws)

            # Single writer: concurrent completions queue frames here
            # rather than interleaving sends on the socket directly.
            outbound: asyncio.Queue = asyncio.Queue()
            writer = asyncio.create_task(self._write_loop(ws, outbound))
            in_flight: set[asyncio.Task] = set()
            try:
                async for raw in ws:
                    frame = json.loads(raw)
                    kind = frame.get("type")
                    if kind == "completionRequest":
                        task = asyncio.create_task(
                            self._serve_one(outbound, frame["requestId"], frame["payload"])
                        )
                        in_flight.add(task)
                        task.add_done_callback(in_flight.discard)
                    elif kind == "error":
                        logger.warning("souk rejected a frame: %s", frame)
                    else:
                        logger.warning("unexpected frame from souk, ignoring: %s", frame)
            finally:
                writer.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await writer
                # Answers are only accepted on the socket their request
                # arrived on, and this one is gone — souk is already
                # failing these completions. Stop paying the LLM for
                # output with nowhere to go.
                for task in list(in_flight):
                    task.cancel()

    async def _write_loop(self, ws, outbound: asyncio.Queue) -> None:
        while True:
            await ws.send(json.dumps(await outbound.get()))

    async def _serve_one(self, outbound: asyncio.Queue, request_id: str, body: dict[str, Any]) -> None:
        """One completion: stream the real LLM's chunks back as `chunk`
        frames, then `done` — or one `error` frame if the call fails
        outright, so the waiting provider fails fast instead of timing
        out."""
        try:
            stream = await litellm.acompletion(
                model=self.model,
                api_key=self.api_key,
                api_base=self.api_base,
                messages=body.get("messages", []),
                tools=body.get("tools") or None,
                temperature=body.get("temperature"),
                stream=True,
            )
            async for chunk in stream:
                outbound.put_nowait(
                    {"type": "chunk", "requestId": request_id, "data": _to_chunk_dict(chunk)}
                )
            outbound.put_nowait({"type": "done", "requestId": request_id})
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.exception("kyok bridge: LLM call failed for request_id=%s", request_id)
            outbound.put_nowait({"type": "error", "requestId": request_id, "message": str(e)})


def _to_chunk_dict(chunk: Any) -> dict:
    """One litellm streaming chunk as the plain dict souk relays — litellm
    returns pydantic-ish objects whose serialization surface varies by
    version, hence the three paths."""
    if hasattr(chunk, "model_dump"):
        return chunk.model_dump(mode="json")
    if hasattr(chunk, "dict"):
        return chunk.dict()
    return dict(chunk)
