"""Turning a sub-agent's *name* into its address, once, at call time.

A config file names a sub-agent — `translator` — because that is what a
person writing one knows. An address is `(provider, name)`, and the
provider half is a key this config cannot contain: it is the callee's own
Ed25519 identity, which does not exist until that provider first starts
and writes its key file. There is no way to write the finished URL down
ahead of time, and that is not an oversight of this repo's — it is what
"an agent is the pair" means for anyone holding only a name.

So the name is resolved against the roster, which is the same thing the
gateway's deleted by-name routes were doing. The difference is where: here
the caller sees the ambiguity and can be told to answer it, instead of a
route picking a winner on its behalf.

**Lazily, on first call, not at startup.** A sub-agent is very often a
sibling container that has not registered yet when this one boots, and a
resolver that ran at startup would turn a delegation edge into a boot
order — one that compose cannot express, since `depends_on` waits for a
container, not for a registration. Resolving when the tool is actually
called moves the requirement to the moment it is genuinely true: you
cannot delegate to somebody who is not there.

The answer is cached for the life of the process. A provider's key is
stable across its restarts (it is persisted), so a resolved address does
not go stale — and an agent that is merely offline keeps its roster row,
so nothing here needs to re-resolve to notice.
"""

from __future__ import annotations

import httpx

from pydantic_ai_agent.config import SubAgentConfig


class SubAgentUnresolvable(Exception):
    """Said in words a model can relay: these end up in front of one."""


async def _roster(souk_http_url: str) -> list[dict]:
    async with httpx.AsyncClient(timeout=15.0) as client:
        resp = await client.get(f"{souk_http_url.rstrip('/')}/agents")
        resp.raise_for_status()
    return resp.json()["agents"]


async def resolve_a2a_url(sub: SubAgentConfig, souk_http_url: str) -> str:
    """The A2A JSON-RPC endpoint for this sub-agent.

    An explicit `a2a_url` wins and is not looked up at all — that is the
    escape hatch for an agent on a *different* souk, or one reached through
    something other than this gateway.
    """
    if sub.a2a_url:
        return sub.a2a_url

    wanted = sub.agent or sub.name
    base = souk_http_url.rstrip("/")
    candidates = [
        row
        for row in await _roster(base)
        if row["name"] == wanted
        and (not sub.provider or sub.provider in (row["provider_key"], row["fingerprint"]))
    ]
    if not candidates:
        where = f" under provider '{sub.provider}'" if sub.provider else ""
        raise SubAgentUnresolvable(
            f"no agent named '{wanted}'{where} is listed on this souk — "
            "it may not have registered yet"
        )
    if len(candidates) > 1:
        stalls = ", ".join(
            f"{row.get('provider_name') or 'unnamed'} ({row['fingerprint']})" for row in candidates
        )
        raise SubAgentUnresolvable(
            f"'{wanted}' is offered by {len(candidates)} providers: {stalls}. "
            f"Set `provider:` on this sub_agent to say which one is meant."
        )
    row = candidates[0]
    return f"{base}/a2a/{row['fingerprint']}/{row['name']}/rpc"


class ResolvedURL:
    """One sub-agent's address, resolved at most once per process.

    Failures are not cached: a name that was not listed yet is very likely
    listed a minute later, and caching "no" would make a startup race
    permanent for the life of the container.
    """

    def __init__(self, sub: SubAgentConfig, souk_http_url: str) -> None:
        self._sub = sub
        self._souk_http_url = souk_http_url
        self._url: str | None = None

    async def get(self) -> str:
        if self._url is None:
            self._url = await resolve_a2a_url(self._sub, self._souk_http_url)
        return self._url
