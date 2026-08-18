"""Pydantic schemas for souk's HTTP surface.

`RunAgentInput` itself is deliberately *not* defined here — the inbound
`/agui/{name}` request body uses `ag_ui.core.RunAgentInput`, the real
AG-UI schema, directly (see api_agui.py). A separate, souk-flavored
reimplementation of the same model used to live here, which meant two
different types with the same name, only one of which was the real
protocol — and the souk-only one was missing `tools`/`state`/`context`
entirely, fields a real caller may legitimately want to send. There is
nothing left to duplicate.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class AgentRegistration(BaseModel):
    name: str = Field(min_length=1, max_length=128, pattern=r"^[A-Za-z0-9_-]+$")
    description: str = ""
    agent_card_extra: dict[str, Any] = Field(default_factory=dict)
    # souk-internal, not exposed via the public A2A Agent Card — see
    # agents.metadata in souk/schema.py.
    metadata: dict[str, Any] = Field(default_factory=dict)


class RegisterBatchRequest(BaseModel):
    agents: list[AgentRegistration]
    # Optional storefront label for this public_key, shown when
    # souk-directory groups agents by provider — see souk/schema.py's
    # providers table. Purely descriptive (like AgentRegistration.
    # description below), so — like description — it's not part of what
    # registration_signing_payload covers; only which names are being
    # claimed needs to be tamper-proof.
    provider_name: str | None = Field(default=None, max_length=128)
    # Ed25519 public key (hex) this provider's identity is backed by, and
    # a signature (hex) over souk.identity.registration_signing_payload —
    # proves possession of the matching private key. See
    # souk_agent_sdk.identity for how a provider generates/persists one.
    # First registration of a name binds it to this key; later attempts
    # to register the same name with a different key are rejected (see
    # repo.register_agents) — this is the whole of souk's provider
    # identity model: no signup flow, the keypair *is* the identity.
    public_key: str
    signature: str
    # Unix timestamp (seconds) included in what was signed — souk rejects
    # anything outside souk.identity.SIGNATURE_FRESHNESS_WINDOW_SECONDS of
    # its own clock, so a captured signed request can't be replayed
    # indefinitely to keep minting fresh session tokens.
    timestamp: int


class AgentRosterEntry(BaseModel):
    """One roster row on the wire.

    An agent *is* `(provider_key, name)` now — souk mints no id for anyone
    to hold — so the pair is what a caller addresses it by. `fingerprint`
    is the same identity in 16 hex, which is what this gateway puts in a
    URL; it is derived, never authoritative, and `provider_key` is the
    thing to compare.
    """

    model_config = ConfigDict(from_attributes=True)

    provider_key: str
    fingerprint: str
    name: str
    description: str = ""
    skills: list[dict[str, Any]] = Field(default_factory=list)
    joined_at: datetime
    last_seen_at: datetime
    online: bool
    # The optional storefront label for that key (souk.schema's providers
    # table), None if this provider never set one.
    provider_name: str | None = None


class RosterResponse(BaseModel):
    agents: list[AgentRosterEntry]


class RegisterBatchResponse(RosterResponse):
    """What a provider gets back for proving who it is.

    No session token: souk stopped issuing one when work stopped being
    claimed, and this gateway's socket is authenticated by a signature
    from the same key instead (see ws_provider.connect_signing_payload) —
    so there is nothing bearer-shaped to leak or to expire under a
    long-lived connection.

    No ids either, for the reason core gives: a provider that held ids
    souk minted could be cut off from its own work by a database it never
    saw replaced. It already knows its key and the names it chose, and
    that pair is the agent.
    """


class LlmRegisterRequest(BaseModel):
    """Registration for the KYOK side's party: an LLM provider declaring
    model offerings. Same Ed25519 machinery as agent registration, under
    its own payload prefix (`souk-register-llm`) so neither signature can
    be replayed as the other.

    Names are freer than agent names on purpose — a model offering is
    addressed inside frames and metadata, never in a URL path, and real
    model names carry dots ("gpt-4.1").
    """

    models: list[str] = Field(min_length=1)
    # Free-form description of the offerings (pricing hints, model family,
    # whatever the provider wants a directory to show). Stored verbatim.
    metadata: dict[str, Any] = Field(default_factory=dict)
    public_key: str
    signature: str
    timestamp: int


class LlmRegisterResponse(BaseModel):
    models: list[str]


class LlmOfferingEntry(BaseModel):
    """One LLM offering on the wire — `AgentRosterEntry`'s mirror.

    `online` is the pre-flight answer a KYOK caller had no way to ask
    before binding a run: whether the offering it is about to name is
    attached *right now* (liveness stays a per-call fact after that —
    this is a glance, not a reservation).
    """

    model_config = ConfigDict(from_attributes=True)

    provider_key: str
    fingerprint: str
    name: str
    metadata: dict[str, Any] = Field(default_factory=dict)
    joined_at: datetime
    last_seen_at: datetime
    online: bool
    provider_name: str | None = None


class LlmRosterResponse(BaseModel):
    offerings: list[LlmOfferingEntry]


class CreateThreadRequest(BaseModel):
    # The agent comes from the URL path (POST /threads/{provider}/{name})
    # — this body is just the optional extras.
    metadata: dict[str, Any] = Field(default_factory=dict)


class CreateThreadResponse(BaseModel):
    thread_id: str


class LivenessResponse(BaseModel):
    """`/healthz`. Nothing about souk's dependencies belongs here — the
    response existing is the answer."""

    status: str


class HealthResponse(BaseModel):
    """`/readyz`. Facts, so an operator reading a 503 does not have to go
    and find out which of them was false. Carries no connection string and
    no driver message — see souk.core.Health, and note that this endpoint is
    unauthenticated because a probe cannot hold a credential.
    """

    ready: bool
    database: bool
    # The exception's type name, never its message.
    database_error: str | None = None
    schema_revision: str | None = None
    expected_schema_revision: str
    background_running: bool
