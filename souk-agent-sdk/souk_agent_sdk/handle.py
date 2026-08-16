"""One agent, as a provider declares it to souk.

`souk_provider_sdk.AgentHandle` is the base and states what an agent *is*:
a name, a description, and a stream of AG-UI events. This adds back the
two fields a *registration* carries that the base one stopped sending —
`agent_card_extra` and `metadata` — because souk still reads both
(`repo.register_agents` builds the agent card from name + description +
`agent_card_extra`, and stores `metadata` beside it), and the base
`as_registration` no longer offers anywhere to put them.

**This is not a preference.** `agent_card_extra` is the only route skills
take into souk, and skills are what the discovery surface searches: an
agent with none is findable only by someone who already knows its name.
That failure has happened here once already, from the opposite direction —
`AgentHandle` had the field, `AgentConfig` did not, and every agent
registered through the runner was invisible to the docent. It was found by
asking the docent to find itself. Losing the field on the SDK side
produces the identical symptom: registration succeeds, the agent appears,
and it is unsearchable.

Upstream is the right home for this — see AgentSouk#46. Until it lands,
this subclass is what keeps the gateway's own docent able to see the
market it is standing in.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from souk_provider_sdk import AgentHandle as _BaseAgentHandle


@dataclass
class AgentHandle(_BaseAgentHandle):
    """`souk_provider_sdk.AgentHandle` plus everything a registration says.

    A drop-in: same constructor, same `run_stream` contract, and
    `HandleProvider`/`ProviderRuntime` take it unchanged, since neither
    looks at anything below.
    """

    # Merged verbatim into the published A2A Agent Card. `skills` lives
    # here — souk drops any registration key it does not name, silently,
    # so a top-level `skills=` would register an agent with none.
    agent_card_extra: dict[str, Any] = field(default_factory=dict)
    # souk-internal, never exposed on the card (see agents.metadata in
    # souk/schema.py). Not interpreted by souk.
    metadata: dict[str, Any] = field(default_factory=dict)

    def as_registration(self) -> dict[str, Any]:
        registration = super().as_registration()
        if self.agent_card_extra:
            registration["agent_card_extra"] = self.agent_card_extra
        if self.metadata:
            registration["metadata"] = self.metadata
        return registration
