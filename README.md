# AgentSoukServer

The reference [Agent Souk](https://github.com/hukaichun/AgentSouk) gateway:
AG-UI + A2A over HTTP (FastAPI), and the gRPC relay that providers connect
out to for NAT traversal. Extracted from AgentSouk's `souk-server`
subproject; souk core (the network-free library) is consumed from the
`AgentSouk` git submodule via a `uv` path dependency.

This package owns every I/O decision — which framework, which port, which
TLS story. Core knows a database and nothing else; see
[`AgentSouk/docs/library-architecture.md`](AgentSouk/docs/library-architecture.md)
for the design and the core/serving boundary this split protects.

## Setup

The submodule is required — without it there is no `souk` to resolve:

```bash
git clone --recurse-submodules git@github.com:hukaichun/AgentSoukServer.git
cd AgentSoukServer
# (already cloned without it? git submodule update --init)
```

Quickstart:

```bash
uv sync --group dev
uv run bash AgentSouk/scripts/gen_proto.sh souk_server/grpc_gen   # gRPC stubs
uv run pytest
SOUK_TOKEN_SIGNING_SECRET=dev uv run souk-server                  # :8000 HTTP, :50051 gRPC
```

Always pass the explicit output path to `gen_proto.sh` — its no-arg default
targets the AgentSouk repo's own layout.

## Configuration

All via `SOUK_*` environment variables — see [.env.example](.env.example)
(documentation only; nothing auto-loads it). Unset `SOUK_DATABASE_URL`
means zero-config SQLite at `./souk.db`; real deployments use Postgres
(`postgresql+psycopg://…`) and must set `SOUK_TOKEN_SIGNING_SECRET`.

## Docker

```bash
docker compose up --build   # paradedb + one-shot alembic migrate + the gateway
```

Schema migration is a separate one-shot service (`souk-migrate`), not a
server-startup step — deployments run DDL with different credentials than
the DML-only role the server needs.

## Tests

SQLite by default; the same suite runs against Postgres by exporting a DSN
(dialect bugs only appear on one side — run both):

```bash
uv run pytest
docker compose up paradedb -d
SOUK_DATABASE_URL=postgresql+psycopg://souk:souk@localhost:5433/souk uv run pytest
```

A green suite does not import `souk_server/server.py`; after broad edits
also verify the app builds:

```bash
uv run python -c "from souk.config import CoreSettings; from souk.core import Souk; from souk_server.server import create_app; create_app(Souk(CoreSettings(token_signing_secret='x'))); print('app builds')"
```
