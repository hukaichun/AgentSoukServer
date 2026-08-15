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

The bridge's side of the relay — where `poll` and `respond/{request_id}`
used to be — is `WS /ws/kyok` (souk_server/ws_kyok.py): the same queues,
pushed down one socket instead of polled, with answers accepted only on the
connection each request was delivered to. See docs/server-mode.md for why
that replacement is a security fix and not only a transport swap.

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
        return JSONResponse(await relay.collapsed())

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
