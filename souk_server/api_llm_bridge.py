"""KYOK HTTP surface: one route only.

What KYOK *means* — the two-part authorization, the relay between a provider
and whoever is paying for its inference, reassembling streamed chunks — lives
in souk/protocols/kyok.py, in core. This file lifts headers off the request,
frames the result as SSE or JSON, and maps `KyokRejected` onto its status.

- `POST /kyok/v1/chat/completions`: what a provider's OpenAI-compatible model
  client actually calls — it looks exactly like a real OpenAI-compatible
  host, which is the whole point of this side. Queues the request for the
  caller's bridge, then blocks *this HTTP call* while relaying the answer
  back.

The answering side of the relay is `WS /ws/kyok` (souk_server/ws_kyok.py):
the LLM provider the run's binding names, attached over a socket, with
answers accepted only on the connection each request was delivered to. See
docs/server-mode.md for the frames and docs/keep-your-own-key.md upstream
for why the answering party is an identified provider now, not an
anonymous bridge.

See docs/keep-your-own-key.md for the full picture.
"""

from __future__ import annotations

import logging

from fastapi import APIRouter, Depends, Request
from fastapi.responses import JSONResponse, StreamingResponse

from souk.core import Souk
from souk_server.deps import get_souk
from souk.errors import KyokRejected
from souk.protocols.kyok import KyokAdapter

logger = logging.getLogger("souk.api_llm_bridge")

router = APIRouter()


def _bearer(request: Request) -> str:
    header = request.headers.get("authorization", "")
    return header[len("Bearer ") :] if header.startswith("Bearer ") else header


@router.post("/kyok/v1/chat/completions", response_model=None)
async def chat_completions(
    request: Request, souk: Souk = Depends(get_souk)
) -> StreamingResponse | JSONResponse:
    relay = await KyokAdapter(souk).complete(
        _bearer(request),
        await request.body(),
        timestamp=request.headers.get("x-souk-kyok-timestamp", ""),
        signature=request.headers.get("x-souk-kyok-signature", ""),
    )
    if not relay.stream_requested:
        # `collapsed` returns the openai package's `ChatCompletion` model —
        # dumped in json mode so what goes on the wire is exactly its
        # serialization, not the dict a model happens to be underneath.
        return JSONResponse((await relay.collapsed()).model_dump(mode="json"))

    async def sse():
        # A rejection here surfaces mid-stream: the response has already
        # started, so there is no status left to change and the caller sees
        # the stream simply end.
        try:
            async for data in relay.encode():
                yield f"data: {data}\n\n"
        except KyokRejected as e:
            logger.warning("kyok completion %s ended early: %s", relay.request_id, e)

    return StreamingResponse(sse(), media_type="text/event-stream")
