"""The souk's docent: an MCP server that shows a visitor around the market.

Not an admin surface, and deliberately not a way to *use* an agent. This
answers the questions a guide answers — who has a stall today, what does
each one do, which one do I want — and then gives directions: the A2A
endpoint where the actual conversation happens. Calling an agent is A2A's
job, which souk already serves without deviation, so wrapping `start_run`
in an MCP tool would build a second, lossier invocation path beside a
standard one (see docs/server-mode.md's MCP section).

Everything here is read-only. There is no tool that changes anything, and
nothing about registration, identity, KYOK or runs is exposed: a visitor
does not administer the market.

Two layers on purpose, mirroring how the protocol adapters in core are
built. `describe_*` below are pure functions from souk's models to the
shapes a docent hands out — no MCP types, no I/O — and the `MCPServer`
underneath is the binding. If a second consumer of the mapping ever
appears (the design note names promotion to core as the trigger), the
pure half moves and the binding stays.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from a2a.utils.constants import AGENT_CARD_WELL_KNOWN_PATH
from mcp.server import MCPServer
from mcp.types import ToolAnnotations

from souk.core import Souk
from souk.models import AgentSummary

INSTRUCTIONS = """\
You are looking at an Agent Souk: an open marketplace where independent \
providers list agents anyone can talk to. Use these tools to find out who \
is here and what they do.

You cannot run an agent through this server, and should not try. Every \
answer includes an `a2a_endpoint` — that is where the conversation \
actually happens, over A2A JSON-RPC. Hand it to whoever asked, or use an \
A2A client with it.

`online` is a live judgement, not a stored flag: it means the agent has \
checked in recently. An offline agent is still listed — its stall is here, \
its keeper stepped away — so say so rather than pretending it is gone.\
"""

# Declared, not just true: a client that surfaces tool safety to a user (or
# auto-approves) reads these rather than the docstrings.
READ_ONLY = ToolAnnotations(read_only_hint=True, open_world_hint=False)


def _seen_ago(last_seen_at: datetime) -> str:
    """How long since this agent checked in, in words a guide would use.

    Reported alongside `online` rather than instead of it, because `online`
    is derived from this same timestamp at query time (see AgentSummary):
    handing back only the boolean loses the difference between "away for a
    minute" and "has not been seen in a week", which is exactly what a
    visitor deciding whether to wait wants to know.
    """
    seen = last_seen_at if last_seen_at.tzinfo else last_seen_at.replace(tzinfo=UTC)
    seconds = max(0, int((datetime.now(UTC) - seen).total_seconds()))
    if seconds < 90:
        return f"{seconds}s ago"
    if seconds < 5400:
        return f"{seconds // 60}m ago"
    if seconds < 172800:
        return f"{seconds // 3600}h ago"
    return f"{seconds // 86400}d ago"


def describe_agent(agent: AgentSummary, public_base_url: str) -> dict[str, Any]:
    """One stall, as the docent describes it — including where to go."""
    base = public_base_url.rstrip("/")
    return {
        "agent_id": agent.agent_id,
        "name": agent.name,
        "description": agent.description,
        "skills": agent.skills,
        "online": agent.online,
        "last_seen": _seen_ago(agent.last_seen_at),
        "listed_since": agent.joined_at.isoformat(),
        # The stall keeper. The key is the identity souk actually verifies;
        # provider_name is a self-chosen storefront label and may be absent,
        # which is why both are here and only the key is ever authoritative.
        "provider": {
            "public_key": agent.public_key,
            "storefront_name": agent.provider_name,
        },
        # Directions, always. The id route rather than the name route: names
        # are not unique across providers (see api_a2a's module docstring),
        # so this is the address that cannot resolve to somebody else.
        "a2a_endpoint": f"{base}/a2a/id/{agent.agent_id}/rpc",
        "agent_card_url": f"{base}/a2a/id/{agent.agent_id}{AGENT_CARD_WELL_KNOWN_PATH}",
    }


def describe_stall(
    public_key: str, agents: list[AgentSummary], public_base_url: str
) -> dict[str, Any]:
    """One provider and everything it offers.

    A stall, not a list of unrelated agents: souk groups by provider because
    the key *is* the identity it verifies, and one identity serving several
    agents is the ordinary case. `storefront_name` is a self-chosen label and
    may be absent or shared; only the key identifies a stall.
    """
    return {
        "provider_key": public_key,
        "storefront_name": next((a.provider_name for a in agents if a.provider_name), None),
        "agent_count": len(agents),
        "online_count": sum(1 for a in agents if a.online),
        "offers": [a.name for a in agents],
        "agents": [describe_agent(a, public_base_url) for a in agents],
    }


def describe_market(agents: list[AgentSummary], public_base_url: str) -> dict[str, Any]:
    """The market at a glance: every stall, grouped by who keeps it.

    Grouped rather than flat because that is the shape every consumer wants
    anyway — souk's own roster groups by provider, and so does every
    frontend built on it — and because a flat list makes each of them
    re-derive the grouping from a field they might drop.
    """
    stalls: dict[str, list[AgentSummary]] = {}
    for agent in agents:
        stalls.setdefault(agent.public_key, []).append(agent)
    return {
        "agent_count": len(agents),
        "online_count": sum(1 for a in agents if a.online),
        "provider_count": len(stalls),
        "providers": [
            describe_stall(key, members, public_base_url) for key, members in stalls.items()
        ],
    }


def _matches(agent: AgentSummary, needle: str) -> bool:
    """Substring match over everything an agent says about itself.

    Deliberately in Python over the whole roster rather than a query: a
    souk's roster is a market's worth of stalls, not a log, and souk has no
    search index. If a deployment ever outgrows this, that is the moment a
    core query earns its place — not before (issue AgentSouk#31 records the
    same reasoning for enumeration).
    """
    haystack = [agent.name, agent.description]
    for skill in agent.skills:
        if isinstance(skill, dict):
            haystack += [str(skill.get("name", "")), str(skill.get("description", ""))]
            haystack += [str(tag) for tag in skill.get("tags", []) or []]
    return needle in " ".join(haystack).lower()


def create_docent(souk: Souk, public_base_url: str) -> MCPServer:
    """The MCP server. `public_base_url` is what callers reach this souk at
    — the same value `A2AAdapter` is given, and for the same reason: core
    must not know what it is called on a network, so the directions this
    hands out are the serving layer's to supply.
    """
    docent = MCPServer(
        name="souk-docent",
        title="Agent Souk docent",
        instructions=INSTRUCTIONS,
        version="0.1.0",
    )

    @docent.tool(
        title="Browse the souk",
        description=(
            "Everyone in this souk right now, grouped by the provider who "
            "keeps each stall. Start here when asked what is available."
        ),
        annotations=READ_ONLY,
    )
    async def browse_souk() -> dict[str, Any]:
        return describe_market(await souk.list_agents(), public_base_url)

    @docent.tool(
        title="Search for an agent",
        description=(
            "Find agents whose name, description or skills mention "
            "something. Use it when asked for help with a task rather than "
            "for a specific agent by name."
        ),
        annotations=READ_ONLY,
    )
    async def search_agents(query: str) -> dict[str, Any]:
        needle = query.strip().lower()
        agents = await souk.list_agents()
        found = [a for a in agents if _matches(a, needle)] if needle else agents
        return {
            "query": query,
            "match_count": len(found),
            "searched_count": len(agents),
            "agents": [describe_agent(a, public_base_url) for a in found],
        }

    @docent.tool(
        name="describe_agent",
        title="Describe one agent",
        description=(
            "Everything this souk knows about one agent, addressed by its "
            "agent_id or its display name, and where to talk to it."
        ),
        annotations=READ_ONLY,
    )
    async def describe_agent_tool(name_or_id: str) -> dict[str, Any]:
        agents = await souk.list_agents()
        by_id = [a for a in agents if a.agent_id == name_or_id]
        if by_id:
            return describe_agent(by_id[0], public_base_url)

        by_name = [a for a in agents if a.name == name_or_id]
        if not by_name:
            return {
                "found": False,
                "asked_for": name_or_id,
                "hint": "No listed agent by that id or name — browse_souk shows who is here.",
            }
        if len(by_name) > 1:
            # Names are not unique: several identities may register the same
            # one. souk's own name route 409s rather than picking a winner,
            # and a docent that guessed would send a visitor to the wrong
            # stall — so hand back the candidates and let the asker choose.
            return {
                "found": False,
                "asked_for": name_or_id,
                "reason": "ambiguous_name",
                "hint": "Several providers list this name. Ask again with one of these agent_ids.",
                "candidates": [describe_agent(a, public_base_url) for a in by_name],
            }
        return describe_agent(by_name[0], public_base_url)

    @docent.tool(
        name="describe_stall",
        title="Describe one stall",
        description=(
            "One provider and everything it offers, addressed by its "
            "provider key. Use it when asked about a stall rather than "
            "about a single agent."
        ),
        annotations=READ_ONLY,
    )
    async def describe_stall_tool(provider_key: str) -> dict[str, Any]:
        members = [a for a in await souk.list_agents() if a.public_key == provider_key]
        if not members:
            return {
                "found": False,
                "asked_for": provider_key,
                "hint": "No stall by that provider key — browse_souk lists every stall here.",
            }
        return describe_stall(provider_key, members, public_base_url)

    @docent.resource(
        "souk://providers",
        title="The souk's stalls",
        description="Every provider here, and what each one offers.",
        mime_type="application/json",
    )
    async def stalls() -> dict[str, Any]:
        return describe_market(await souk.list_agents(), public_base_url)

    @docent.resource(
        "souk://agents",
        title="The souk's roster",
        description="Every listed agent, each carrying the provider that keeps it.",
        mime_type="application/json",
    )
    async def roster() -> dict[str, Any]:
        agents = await souk.list_agents()
        return {
            "agent_count": len(agents),
            "online_count": sum(1 for a in agents if a.online),
            "agents": [describe_agent(a, public_base_url) for a in agents],
        }

    @docent.resource(
        "souk://agent/{agent_id}",
        title="One agent",
        description="One listed agent, and where to reach it.",
        mime_type="application/json",
    )
    async def agent_resource(agent_id: str) -> dict[str, Any]:
        for agent in await souk.list_agents():
            if agent.agent_id == agent_id:
                return describe_agent(agent, public_base_url)
        return {"found": False, "agent_id": agent_id}

    return docent
