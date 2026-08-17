"""A2A HTTP surface: routes only.

What A2A *means* — Task.id being souk's run_id, contextId being thread_id,
what tasks/send does when a session already has a live run — lives in
souk/protocols/a2a.py, in core. This file parses requests and frames results
as JSON or SSE; domain errors are translated once for the whole app (see
souk.deps.install_error_handlers), not per route.

Agent Cards are served under a per-agent path prefix rather than at the
origin root — a deliberate deviation from A2A's single-agent-per-origin
assumption, since one souk fronts many agents at one origin.

One way to address an agent: `/a2a/{provider}/{name}/...`. An agent *is*
`(provider_key, name)`, so addressing it takes both and takes nothing souk
minted; `provider` may be the full public key or its 16-hex fingerprint,
which core tells apart by length.

There was a second, by-name route, kept because a bare name is what a human
types. It is gone rather than deprecated. A name is not unique — two
identities may both register `translator` (see repo.register_agents) — so
that route had to either pick a winner or refuse, and picking a winner is
how a caller reaches an agent it never meant to reach. Resolving a name is
still easy and still supported; it is `list_agents`, done once by whoever
holds the name, and then the pair is what goes on the wire.
"""

from __future__ import annotations

from a2a.utils.constants import AGENT_CARD_WELL_KNOWN_PATH

from souk.identity import provider_fingerprint
from fastapi import APIRouter, Depends, Request
from sse_starlette.sse import EventSourceResponse

from souk.models import AgentRef
from souk_server.config import ServingSettings
from souk.core import Souk
from souk_server.deps import get_serving_settings, get_souk, resolve_ref
from souk.protocols.a2a import A2AAdapter, A2AStream, ServedInterface

router = APIRouter()


def _adapter(souk: Souk) -> A2AAdapter:
    return A2AAdapter(souk)


def _interfaces(agent: AgentRef, serving: ServingSettings) -> list[ServedInterface]:
    """Where this gateway actually serves that agent.

    Core stopped naming URLs, which is right: it had been interpolating a
    route layout on behalf of every gateway that would ever serve it. The
    layout below is this repo's — `/a2a/{fingerprint}/{name}/rpc` — and
    saying so here is the whole of what changed.
    """
    base = serving.public_http_url.rstrip("/")
    return [
        ServedInterface(
            url=f"{base}/a2a/{provider_fingerprint(agent.provider_key)}/{agent.name}/rpc",
            binding="JSONRPC",
        )
    ]


# The path comes from a2a.utils.constants rather than being typed here, for
# the same reason every other A2A string in souk does: v1.0 moved it (from
# `/.well-known/agent.json`), and souk should learn that from the package
# rather than from a client failing against it.
#
# Only this one is served. A route for the older path existed briefly, on the
# reasoning that a card is what a client finds souk *with* — but it answered
# the old URL with the *new* body, which has no top-level `url` and no
# `preferredTransport`, so a pre-v1 client found a card it could not use to
# locate the RPC endpoint. Half an accommodation is not one, and the two that
# do work — every older method name, and every older part spelling — work end
# to end (see souk/tests/test_a2a_spec_methods.py). Whether to serve the
# legacy path at all, and if so whether to answer it with a legacy-shaped
# body, is a gateway decision and belongs downstream.
@router.get("/a2a/{provider}/{name}" + AGENT_CARD_WELL_KNOWN_PATH)
async def agent_card_by_pair(
    provider: str,
    name: str,
    souk: Souk = Depends(get_souk),
    serving: ServingSettings = Depends(get_serving_settings),
) -> dict:
    agent = await resolve_ref(souk, provider, name)
    return await _adapter(souk).agent_card(agent, _interfaces(agent, serving))


async def _rpc(adapter: A2AAdapter, agent: AgentRef, request: Request):
    result = await adapter.handle_rpc(agent, await request.json())
    if not isinstance(result, A2AStream):
        return result

    async def stream():
        async for data in result.encode():
            yield {"event": "message", "data": data}

    return EventSourceResponse(stream())


@router.post("/a2a/{provider}/{name}/rpc")
async def rpc_by_pair(
    provider: str,
    name: str,
    request: Request,
    souk: Souk = Depends(get_souk),
):
    return await _rpc(_adapter(souk), await resolve_ref(souk, provider, name), request)
