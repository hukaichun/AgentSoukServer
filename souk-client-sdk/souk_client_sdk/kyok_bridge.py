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
import json
import logging
from typing import Any
from urllib.parse import urlsplit, urlunsplit

import httpx
import litellm
import websockets
from pydantic import ValidationError
from souk_llm_provider_sdk import (
    CompletionHandler,
    CompletionRefused,
    DeliveredCompletion,
    ProviderIdentity,
    sign_llm_deletion,
    sign_llm_registration,
)
from souk_provider_sdk import new_nonce

logger = logging.getLogger("souk_client_sdk.kyok_bridge")

WELCOME_TIMEOUT_SECONDS = 10.0

# The frame choreography matches souk_server/handshake.py; the bytes
# signed are souk_provider_sdk's link-open family (`sign_connect`), so
# this package no longer restates any payload. v2 is that migration.
HANDSHAKE_VERSION = 2


class KyokBridge:
    """One KYOK LLM provider serving one model offering with the caller's
    own key. Typical use is one block:

    ```python
    async with bridge.serving():
        async for event in client.run(agent, msg,
                                      metadata=bridge.run_metadata(ctx)):
            ...
    ```

    (`register()` + `serve_forever()` remain callable separately for a
    long-lived bridge that outlives any single run.)

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
        handler: CompletionHandler | None = None,
    ) -> None:
        self.souk_http_url = souk_http_url.rstrip("/")
        self.model = model
        self.api_key = api_key
        self.api_base = api_base
        self.offering = offering
        # Ephemeral by default: a personal bridge's identity only needs to
        # outlive its runs. Pass a persisted one to keep a stable
        # provider_key across restarts. Remembered which, because it
        # decides cleanup: an ephemeral key can never come back, so the
        # offering registered under it is roster garbage the moment this
        # process exits — `serving()` deletes it on the way out. A
        # persisted identity keeps its registration, same as an agent
        # provider between connections.
        self._ephemeral = identity is None
        self.identity = identity or ProviderIdentity.generate()
        self.reconnect_delay = reconnect_delay
        # The interposition point the library guarantees (see the
        # souk-llm-provider-sdk README and AgentSouk#26's resolution):
        # every completion passes through this before any money moves.
        # None means the default litellm call with this bridge's own key;
        # a caller enforcing policy — a spend ceiling, a model allow-list,
        # refusing a delegation chain it doesn't recognise — wraps or
        # replaces it, and may raise `CompletionRefused` to answer with a
        # structured refusal that reaches the calling agent intact.
        self.handler = handler
        # Set while a connection is attached (welcome received), cleared
        # when it drops. `serving()` waits on it; polling code may read it.
        self.attached = asyncio.Event()

    @property
    def _ws_url(self) -> str:
        scheme, netloc, path, _query, _fragment = urlsplit(self.souk_http_url)
        ws_scheme = "wss" if scheme == "https" else "ws"
        return urlunsplit((ws_scheme, netloc, path.rstrip("/") + "/ws/kyok", "", ""))

    async def register(self) -> None:
        """Register this bridge's offering with souk, signed by its own
        key — the prerequisite the socket's attach enforces. Idempotent
        (the upsert refreshes the record), and `serve_forever` re-runs it
        before every connection, so a souk whose database was reset gets
        the registration back on the next reconnect instead of refusing
        the attach forever — the same self-healing the agent SDK has
        always had (AgentSoukServer#16)."""
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

    async def deregister(self) -> None:
        """Delete this bridge's offering from souk's roster, signed by its
        own key — `register()`'s mirror (payload prefix `souk-delete-llm`).

        Best-effort by design: souk refuses (409) while the offering is
        attached or a run is still bound to it, and a bridge tearing down
        has nothing useful to do about either — so refusals and transport
        failures are logged, not raised. A stale row's only cost is roster
        noise; crashing a clean shutdown over it would cost more.
        """
        signature, timestamp = sign_llm_deletion(self.identity, self.offering)
        try:
            async with httpx.AsyncClient() as client:
                resp = await client.request(
                    "DELETE",
                    f"{self.souk_http_url}/llm-providers",
                    json={
                        "name": self.offering,
                        "public_key": self.identity.public_key,
                        "signature": signature,
                        "timestamp": timestamp,
                    },
                )
            if resp.status_code not in (204, 404):
                logger.warning(
                    "kyok bridge could not deregister %r: %s %s",
                    self.offering,
                    resp.status_code,
                    resp.text,
                )
        except Exception:
            logger.warning("kyok bridge could not deregister %r", self.offering, exc_info=True)

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
        pushes down it — through `handler` if one was given, else calling
        the real LLM via litellm with this bridge's own api_key — and
        streams the response back as frames. Registers before every
        connection and reconnects on a drop. Runs until cancelled;
        `serving()` below wraps the whole lifecycle when the bridge lives
        alongside the run it serves.
        """
        while True:
            try:
                await self.register()
                await self._serve_connection()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception(
                    "kyok bridge connection lost; reconnecting in %.1fs", self.reconnect_delay
                )
            await asyncio.sleep(self.reconnect_delay)

    @contextlib.asynccontextmanager
    async def serving(self):
        """The whole lifecycle as one block: registered, attached, torn
        down — so \"run a KYOK-backed run\" stops being four manual steps
        with three ways to sequence them wrong.

        ```python
        async with bridge.serving():
            async for event in client.run(agent, msg,
                                          metadata=bridge.run_metadata(ctx)):
                ...
        ```

        Yields once the first attach is confirmed (souk's welcome), so a
        run started inside the block can't race the socket and eat a 503
        on its first completion. On exit the serve task is cancelled and
        awaited; completions still in flight die with it, which is the
        crash-behavior this bridge has always documented.
        """
        task = asyncio.create_task(self.serve_forever())
        try:
            waiter = asyncio.create_task(self.attached.wait())
            done, _ = await asyncio.wait(
                {task, waiter}, return_when=asyncio.FIRST_COMPLETED
            )
            if task in done:
                # serve_forever only ends by raising; surface that instead
                # of yielding a bridge that isn't there.
                waiter.cancel()
                raise RuntimeError("kyok bridge failed before attaching") from task.exception()
            yield self
        finally:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
            # After the socket is down, so the attach no longer blocks the
            # delete. Only for an identity this bridge minted itself — a
            # persisted one keeps its registration between runs.
            if self._ephemeral:
                await self.deregister()

    async def _handshake(self, ws) -> None:
        nonce = new_nonce()
        await ws.send(
            json.dumps(
                {
                    "type": "hello",
                    "version": HANDSHAKE_VERSION,
                    "publicKey": self.identity.public_key,
                    "modelNames": [self.offering],
                    "nonce": nonce,
                }
            )
        )
        challenge = json.loads(await asyncio.wait_for(ws.recv(), WELCOME_TIMEOUT_SECONDS))
        if challenge.get("type") != "challenge":
            raise RuntimeError(f"expected challenge, got {challenge!r}")
        await ws.send(
            json.dumps(
                {
                    "type": "proof",
                    # The SDK's statement of the link-open proof — no local
                    # payload, no hello digest.
                    "signature": self.identity.sign_connect(
                        challenge["nonce"], nonce, [self.offering]
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
            self.attached.set()

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
                            self._serve_one(outbound, frame["requestId"], frame)
                        )
                        in_flight.add(task)
                        task.add_done_callback(in_flight.discard)
                    elif kind == "error":
                        logger.warning("souk rejected a frame: %s", frame)
                    else:
                        logger.warning("unexpected frame from souk, ignoring: %s", frame)
            finally:
                self.attached.clear()
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

    async def _serve_one(self, outbound: asyncio.Queue, request_id: str, frame: dict[str, Any]) -> None:
        """One completion: through the handler (the interposition point),
        streaming its chunks back as `chunk` frames, then `done` — or one
        `error` frame if the call fails, so the waiting provider fails
        fast instead of timing out. A `CompletionRefused` raised by the
        handler puts its structured payload on the error frame, and souk
        relays it to the calling agent intact."""
        # The frame is the declared envelope (`DeliveredCompletion.
        # model_dump(by_alias=True)` on souk's side) plus type/requestId;
        # rebuilding is one validate, not a field mapping.
        try:
            delivered = DeliveredCompletion.model_validate(frame)
        except ValidationError as e:
            logger.warning("kyok bridge: malformed completionRequest %s: %s", request_id, e)
            outbound.put_nowait(
                {"type": "error", "requestId": request_id, "message": "malformed completionRequest"}
            )
            return
        try:
            stream = self.handler(delivered) if self.handler else self._call_llm(delivered)
            async for chunk in stream:
                outbound.put_nowait(
                    {"type": "chunk", "requestId": request_id, "data": _to_chunk_dict(chunk)}
                )
            outbound.put_nowait({"type": "done", "requestId": request_id})
        except asyncio.CancelledError:
            raise
        except CompletionRefused as e:
            logger.info("kyok bridge refused request_id=%s: %s", request_id, e.refusal)
            outbound.put_nowait(
                {
                    "type": "error",
                    "requestId": request_id,
                    "message": "refused by the LLM provider",
                    "refusal": e.refusal,
                }
            )
        except Exception as e:
            logger.exception("kyok bridge: LLM call failed for request_id=%s", request_id)
            outbound.put_nowait({"type": "error", "requestId": request_id, "message": str(e)})

    async def _call_llm(self, delivered: DeliveredCompletion):
        """The default handler: the real LLM via litellm, on this
        bridge's own key. Async generator, so a wrapping handler can
        iterate it after enforcing its own policy."""
        body = delivered.body
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
            yield chunk


def _to_chunk_dict(chunk: Any) -> dict:
    """One litellm streaming chunk as the plain dict souk relays — litellm
    returns pydantic-ish objects whose serialization surface varies by
    version, hence the three paths."""
    if hasattr(chunk, "model_dump"):
        return chunk.model_dump(mode="json")
    if hasattr(chunk, "dict"):
        return chunk.dict()
    return dict(chunk)
