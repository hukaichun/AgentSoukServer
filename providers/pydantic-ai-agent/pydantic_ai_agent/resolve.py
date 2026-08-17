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

from dataclasses import dataclass

import httpx

from pydantic_ai_agent.config import SubAgentConfig


class SubAgentUnresolvable(Exception):
    """Said in words a model can relay: these end up in front of one."""


@dataclass(frozen=True)
class Address:
    """Where a sub-agent is, and who it is.

    Both halves, deliberately, because the answer is consumed twice: the
    URL to call, and the identity to *say* — a delegation is reported live
    on the caller's own event stream, and a report naming only a name
    cannot be told apart from a report about somebody else's agent of the
    same name. Returning a bare URL was how that got lost the first time.

    `provider` is None only for an explicit `a2a_url` that does not look
    like one of this gateway's pair routes — an agent on another souk, or
    behind something else. Unknown is said as None rather than guessed.
    """

    url: str
    provider: str | None = None
    provider_key: str | None = None
    agent_name: str | None = None


async def _roster(souk_http_url: str) -> list[dict]:
    async with httpx.AsyncClient(timeout=15.0) as client:
        resp = await client.get(f"{souk_http_url.rstrip('/')}/agents")
        resp.raise_for_status()
    return resp.json()["agents"]


def _address_from_url(url: str) -> Address:
    """Read the pair back out of an explicit URL, when it has one.

    A `.../a2a/{provider}/{name}/rpc` written by hand names an agent just
    as well as a resolved one does, and a caller who wrote it should not
    lose the identity in their own progress events for having been
    explicit. Anything else keeps its URL and admits it knows no pair.
    """
    parts = url.rstrip("/").split("/")
    if len(parts) >= 4 and parts[-1] == "rpc" and parts[-4] == "a2a":
        return Address(url=url, provider=parts[-3], agent_name=parts[-2])
    return Address(url=url)


async def resolve_address(sub: SubAgentConfig, souk_http_url: str) -> Address:
    """Where this sub-agent is, and who it is.

    An explicit `a2a_url` wins and is not looked up at all — that is the
    escape hatch for an agent on a *different* souk, or one reached through
    something other than this gateway.
    """
    if sub.a2a_url:
        return _address_from_url(sub.a2a_url)

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
    return Address(
        url=f"{base}/a2a/{row['fingerprint']}/{row['name']}/rpc",
        provider=row["fingerprint"],
        provider_key=row["provider_key"],
        agent_name=row["name"],
    )


class ResolvedAddress:
    """One sub-agent's address, resolved at most once per process.

    Failures are not cached: a name that was not listed yet is very likely
    listed a minute later, and caching "no" would make a startup race
    permanent for the life of the container.
    """

    def __init__(self, sub: SubAgentConfig, souk_http_url: str) -> None:
        self._sub = sub
        self._souk_http_url = souk_http_url
        self._address: Address | None = None

    async def get(self) -> Address:
        if self._address is None:
            self._address = await resolve_address(self._sub, self._souk_http_url)
        return self._address
