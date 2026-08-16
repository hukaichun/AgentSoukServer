"""Registration and roster HTTP surface: routes only.

Verifying that a registration really holds the key it claims is domain, not
HTTP — the same act for a provider across a network and one in this process —
so it lives on `Souk` (see `Souk.register_agents`). This file only parses the
request; a rejection becomes a 401 through the app-wide handler.
"""

from fastapi import APIRouter, Depends

from souk.core import Souk
from souk.identity import provider_fingerprint
from souk_server.deps import get_souk
from souk_server.models import AgentRosterEntry, RegisterBatchRequest, RegisterBatchResponse, RosterResponse

router = APIRouter()


@router.post("/agents/register", status_code=201)
async def register_agents(
    body: RegisterBatchRequest, souk: Souk = Depends(get_souk)
) -> RegisterBatchResponse:
    registration = await souk.register_agents(
        body.public_key,
        body.signature,
        body.timestamp,
        [agent.model_dump() for agent in body.agents],
        provider_name=body.provider_name,
    )

    return RegisterBatchResponse(agents=await _roster(souk))


@router.get("/agents")
async def list_agents(souk: Souk = Depends(get_souk)) -> RosterResponse:
    return RosterResponse(agents=await _roster(souk))


async def _roster(souk: Souk) -> list[AgentRosterEntry]:
    """The roster, with the fingerprint this gateway addresses agents by
    filled in beside the key it is derived from."""
    return [
        AgentRosterEntry(
            **summary.model_dump(),
            fingerprint=provider_fingerprint(summary.provider_key),
        )
        for summary in await souk.list_agents()
    ]
