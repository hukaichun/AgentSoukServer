"""Test fixtures for the gateway's test suite.

Deliberately a copy of souk/tests/conftest.py rather than an import of it:
these are two independent distributions (see CONTRIBUTING.md's "no shared
workspace"), and a test-only dependency from the gateway back into souk's
test package would be the first thread of exactly the coupling the split
exists to remove. What is shared is a database and a schema, not fixtures.

Two things are the gateway's own. `client` is an ASGI client over
`create_app`, which is the whole reason these tests live here. And
`serve`/`register` hand back the `AgentRef` *and* the fingerprint, because
an agent is `(provider_key, name)` now and this layer puts the short form
of that pair in a URL — a test that talks to a route needs both halves.

`Identity` subclasses `souk_provider_sdk.ProviderIdentity` rather than
reimplementing the signing against `cryptography`, which the old copy did
because souk did not depend on the SDK. This gateway does — `ws_provider`
builds on `SoukConnection` — so reimplementing what a provider signs would
mean the tests could agree with themselves while disagreeing with every
real provider.

Runs against SQLite by default — zero configuration, no database to stand
up first. The same suite runs against Postgres by exporting SOUK_DATABASE_URL
(a `postgresql+psycopg://…` DSN) before invoking pytest; souk's schema and
queries are dialect-neutral (see souk/schema.py, souk/repo.py), so both
backends exercise the same semantics. See CONTRIBUTING.md for the Postgres
setup.

Settings are constructed explicitly here (see souk/core.py) rather than
arranged in `os.environ` before the first souk import — that ordering
constraint is exactly what injecting settings removed.

Tests aren't wrapped in a rolled-back transaction: souk.repo's functions
call session.commit() internally throughout (e.g. register_agents,
create_run), so a single outer transaction can't cleanly contain a whole
test. The schema is applied once per session via Alembic (the same
`alembic upgrade head` a real deployment runs), and rows are cleared
between tests.
"""

from __future__ import annotations

import os
import tempfile
from collections.abc import AsyncIterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest
from alembic import command
from alembic.config import Config
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from httpx import ASGITransport, AsyncClient
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import AsyncSession

from souk.config import CoreSettings
from souk.core import Souk
from souk.identity import provider_fingerprint
from souk.models import AgentRef
from souk_provider_sdk import InProcessLink, ProviderIdentity, ProviderRuntime
from souk_server.handshake import HANDSHAKE_VERSION, new_nonce
from souk_server.server import create_app

ALEMBIC_INI = (
    Path(__file__).resolve().parent.parent / "AgentSouk" / "souk" / "alembic.ini"
)

TEST_SIGNING_SECRET = "test-signing-secret"

# Postgres when a DSN is exported, a throwaway SQLite file otherwise.
DATABASE_URL = os.environ.get(
    "SOUK_DATABASE_URL", f"sqlite+aiosqlite:///{Path(tempfile.gettempdir()) / 'souk_pytest.db'}"
)

# FK-safe teardown order (children before parents) for the SQLite path,
# where there's no TRUNCATE ... CASCADE. Postgres uses TRUNCATE directly.
_TABLES_CHILD_FIRST = (
    "run_events",
    "thread_messages",
    "runs",
    "threads",
    "agents",
    "llm_providers",
    "providers",
)


# A fixed key rather than a generated one: a test that asserts what a
# provider pinned needs the same souk to be the same souk across runs, and
# generating one per session would make "is this the souk I meant" a
# question with no stable answer to write down.
TEST_SOUK_IDENTITY = "11" * 32


@pytest.fixture(scope="session")
def settings() -> CoreSettings:
    return CoreSettings(
        database_url=DATABASE_URL,
        token_signing_secret=TEST_SIGNING_SECRET,
        identity_private_key=TEST_SOUK_IDENTITY,
    )


@pytest.fixture(scope="session")
def souk(settings: CoreSettings) -> Souk:
    return Souk(settings)


@pytest.fixture(scope="session", autouse=True)
def _schema(settings: CoreSettings) -> None:
    # Start each SQLite run from a clean file so a schema change between
    # runs can't leave a stale table lying around (Postgres relies on the
    # migration + per-test cleanup instead — its DB isn't disposable here).
    url = make_url(settings.database_url)
    if url.get_backend_name() == "sqlite" and url.database:
        for suffix in ("", "-wal", "-shm"):
            Path(url.database + suffix).unlink(missing_ok=True)
    # alembic/env.py reads SOUK_DATABASE_URL from the environment (it
    # deliberately doesn't import souk.config), so migrations still go
    # through the environment even though the app itself no longer does.
    os.environ["SOUK_DATABASE_URL"] = settings.database_url
    command.upgrade(Config(str(ALEMBIC_INI)), "head")


@pytest.fixture(autouse=True)
async def _dispatching(souk: Souk) -> AsyncIterator[None]:
    """The broker's dispatch loop, for every test.

    `create_app`'s lifespan is what starts it in a real process, and
    `ASGITransport` does not run lifespans — so without this a run reaches
    the broker and simply sits there, which reads as a hang rather than as
    a missing fixture.
    """
    souk.broker.start()
    try:
        yield
    finally:
        souk.broker.stop()


@pytest.fixture(autouse=True)
async def _clean_db(souk: Souk) -> AsyncIterator[None]:
    is_postgres = souk.engine.sync_engine.dialect.name == "postgresql"
    async with souk.engine.begin() as conn:
        if is_postgres:
            await conn.exec_driver_sql(
                "TRUNCATE providers, agents, llm_providers, threads, runs, thread_messages, "
                "run_events RESTART IDENTITY CASCADE"
            )
        else:
            for table in _TABLES_CHILD_FIRST:
                await conn.exec_driver_sql(f"DELETE FROM {table}")
    yield


@pytest.fixture
async def session(souk: Souk) -> AsyncIterator[AsyncSession]:
    async with souk.session() as s:
        yield s


@pytest.fixture
async def client(souk: Souk) -> AsyncIterator[AsyncClient]:
    transport = ASGITransport(app=create_app(souk))
    async with AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


class Identity(ProviderIdentity):
    """A throwaway keypair that signs the way a real provider does.

    Everything souk verifies is `ProviderIdentity`'s. `sign_connect` is the
    exception and belongs here: opening a socket is a serving act, so the
    payload is this gateway's (see ws_provider.connect_signing_payload) and
    upstream neither defines it nor exposes a general `sign` to build it
    with — the same gap souk-agent-sdk works around, and AgentSouk#43.
    """

    def __init__(self) -> None:
        self._key = Ed25519PrivateKey.generate()
        super().__init__(self._key)

    @property
    def fingerprint(self) -> str:
        return provider_fingerprint(self.public_key)

    def sign_chain_hop(self, subject: dict, prev_token: str | None = None, exp_offset: int = 300) -> str:
        """One actor-chain hop. `exp_offset` can be negative to build an
        already-expired one, for souk.identity.verify_actor_chain's per-hop
        exp handling."""
        return self.sign_hop(subject, prev_token, ttl=exp_offset)

    def register_body(self, agents: list[dict], **extra) -> dict:
        signature, timestamp = self.sign_registration([a["name"] for a in agents])
        return {
            "public_key": self.public_key,
            "signature": signature,
            "timestamp": timestamp,
            "agents": agents,
            **extra,
        }

    def sign_llm_registration(self, names: list[str]) -> tuple[str, int]:
        """Signature+timestamp for registering LLM offerings — through the
        SDK an actual LLM provider ships, for the shipped-signer reason:
        this suite's hand-written payloads catch core drifting, and this
        catches the SDK drifting from both."""
        from souk_llm_provider_sdk import sign_llm_registration

        return sign_llm_registration(self, names)

    def hello(self, names: list[str], **extra) -> dict:
        """Frame one. No signature in it — the proof comes later, over a
        nonce souk chooses, which is the whole of what changed."""
        return {
            "type": "hello",
            "version": HANDSHAKE_VERSION,
            "publicKey": self.public_key,
            "agentNames": sorted(names),
            "nonce": new_nonce(),
            **extra,
        }

    def proof(self, names: list[str], provider_nonce: str, souk_nonce: str) -> dict:
        """Frame three — the SDK's `sign_connect`, which is the whole point
        of v2: no local payload, no hello digest, the names bound in."""
        return {
            "type": "proof",
            "signature": self.sign_connect(souk_nonce, provider_nonce, names),
        }


@pytest.fixture
def new_identity() -> type[Identity]:
    return Identity


@dataclass
class Served:
    """What `serve`/`register` hand back: everything a test needs to talk
    about the provider it just stood up, including how to address it.

    `ref` is what souk takes, `fingerprint` is what goes in a URL — the same
    identity in two forms, and a test that has to derive one from the other
    is a test that has taken a position on which is authoritative.
    """

    identity: Identity
    provider: Any
    runtime: ProviderRuntime | None
    names: tuple[str, ...]

    @property
    def public_key(self) -> str:
        return self.identity.public_key

    @property
    def fingerprint(self) -> str:
        return self.identity.fingerprint

    def ref(self, name: str | None = None) -> AgentRef:
        return AgentRef(provider_key=self.public_key, name=name or self.names[0])

    def path(self, name: str | None = None) -> str:
        """The `{provider}/{name}` half of every route this gateway serves."""
        return f"{self.fingerprint}/{name or self.names[0]}"


@pytest.fixture
async def attach(souk: Souk):
    """Attach a provider the way a real one arrives: the SDK's runtime, with
    an adapter in front of it that souk can hand a run to.

    Every runtime is stopped when the test ends. The `souk` fixture is
    session-scoped, so one left running stays registered with the broker and
    takes the next test's runs.
    """
    started: list[ProviderRuntime] = []

    async def _attach(identity: ProviderIdentity, provider, names, **kwargs) -> ProviderRuntime:
        runtime = ProviderRuntime(identity, provider, **kwargs)
        started.append(runtime)
        runtime.start()
        # Constructing the link is what joins it to the runtime, so it has
        # to happen before any work arrives — a runtime with no link drops
        # its output silently.
        await souk.attach_provider(InProcessLink(souk, runtime), list(names))
        return runtime

    yield _attach
    for runtime in started:
        await runtime.aclose(cancel_in_flight=True)


class EchoAgent:
    """A provider that answers with one short message and remembers who
    called it."""

    def __init__(self) -> None:
        self.seen_caller: dict | None = None

    async def run_stream(self, agent_name: str, run_input):
        self.seen_caller = (run_input.forwarded_props or {}).get("caller")
        ids = {"threadId": run_input.thread_id, "runId": run_input.run_id}
        yield {"type": "RUN_STARTED", **ids}
        yield {"type": "TEXT_MESSAGE_START", "messageId": "m1", "role": "assistant"}
        yield {"type": "TEXT_MESSAGE_CONTENT", "messageId": "m1", "delta": "done"}
        yield {"type": "TEXT_MESSAGE_END", "messageId": "m1"}
        yield {"type": "RUN_FINISHED", **ids}


@pytest.fixture
async def register(souk: Souk):
    """Register agents without attaching anything — for the cases that are
    about souk knowing a name, not about anybody serving it. Which is most
    of this suite: an offline agent is a state the gateway has routes for.
    """

    async def _register(*names: str, **agent_extra) -> Served:
        names = names or ("agent",)
        identity = Identity()
        signature, timestamp = identity.sign_registration(list(names))
        await souk.register_agents(
            identity.public_key,
            signature,
            timestamp,
            [{"name": n, **agent_extra} for n in names],
        )
        return Served(identity, None, None, tuple(names))

    return _register


@pytest.fixture
async def serve(souk: Souk, attach, register):
    """Register a provider's agents and attach it, in one step.

    Both halves, because they are always done together and neither is
    optional: registration is what makes the names souk's to serve, and
    attaching is what makes them reachable.
    """

    async def _serve(provider=None, *names: str, **kwargs) -> Served:
        provider = EchoAgent() if provider is None else provider
        served = await register(*names)
        runtime = await attach(served.identity, provider, served.names, **kwargs)
        return Served(served.identity, provider, runtime, served.names)

    return _serve
