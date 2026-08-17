# Agent Souk Server 🕌🔌

> **The reference [Agent Souk](https://github.com/hukaichun/AgentSouk) gateway — every network decision, in one place.**  
> One HTTP surface serving humans (**AG-UI** SSE), agents (**A2A v1.0** JSON-RPC), and the outbound relay that lets providers behind NAT serve agents **without public IPs, open ports, or tunnels.**

---

## 🧭 Two Repositories, One Boundary

This repo is the *serving* half of Agent Souk. The split is a hard line, recorded in [AgentSouk#27](https://github.com/hukaichun/AgentSouk/issues/27):

| | **[AgentSouk](https://github.com/hukaichun/AgentSouk)** (upstream) | **AgentSoukServer** (this repo) |
|---|---|---|
| **Owns** | The domain: agents, threads, runs, identity, persistence, protocol *translation* | The network: ports, transports, TLS, CORS, endpoints, wire framing, admin surface — **both ends of every wire** |
| **Ships** | `souk` (network-free core) | The gateway process assembled from core, plus the client SDKs that speak its wire ([`souk-agent-sdk/`](souk-agent-sdk), [`souk-client-sdk/`](souk-client-sdk)) |
| **May it bind a socket?** | ❌ Never — enforced by packaging and test | ✅ That is its entire job |

Two consequences worth knowing before touching anything:

- **The wire contract is authored here, and so are both sides of it.** [`docs/server-mode.md`](docs/server-mode.md) is the spec of record — single HTTP port, WebSocket relays for providers and KYOK bridges, gRPC removed. The SDKs that implement that spec live in this repo too ([`souk-agent-sdk/`](souk-agent-sdk) for providers, [`souk-client-sdk/`](souk-client-sdk) for callers and their KYOK bridges): upstream keeps no network code at all, client side included.
- **souk core arrives via the git submodule** (`AgentSouk/souk`, a `uv` path dependency), pinned by commit. This repo contains no domain logic — it lifts headers, frames responses, binds sockets, and hands everything else to core.

---

## ⚡ Quick Start

The submodule is required — without it there is no `souk` to resolve:

```bash
git clone --recurse-submodules git@github.com:hukaichun/AgentSoukServer.git
cd AgentSoukServer
# (already cloned without it? git submodule update --init)
```

Then, in three commands:

```bash
uv sync --group dev
uv run alembic -c AgentSouk/souk/alembic.ini upgrade head   # one-time DDL step
SOUK_TOKEN_SIGNING_SECRET=dev uv run souk-server            # everything on :8000
```

Verify it's alive:

```bash
curl http://localhost:8000/healthz && curl http://localhost:8000/readyz
```

---

## 🏛️ What This Process Is

```mermaid
graph TD
    User([Human / Web Directory]) -->|"POST /agui/{agent} (SSE)"| HTTP["FastAPI surface — one listener<br/>:8000"]
    CallerAgent([External Agent]) -->|"POST /a2a/{agent}/rpc"| HTTP
    Bridge([Caller's KYOK bridge]) ==>|"WS /ws/kyok"| HTTP

    subgraph Process ["souk-server (single process)"]
        HTTP --> Core["souk core (from the submodule)<br/>broker · handlers · protocol adapters"]
    end

    Process --> DB[(SQLite / Postgres)]
    HTTP <== "WS /ws/provider (outbound)" ==> Providers["Providers behind NAT<br/>(souk-agent-sdk)"]
```

`create_app(souk, serving)` returns a plain ASGI app that binds nothing — mount it inside a larger app, wrap it in your own middleware (pure ASGI, not `BaseHTTPMiddleware`: that class buffers streams and never sees WebSocket scopes), or let the `souk-server` console script serve it. Every I/O decision — which framework, which port, which TLS story — is made here so that core never has to.

**Server mode is live** ([`docs/server-mode.md`](docs/server-mode.md)): providers and KYOK bridges each hold a WebSocket on the one HTTP port (`/ws/provider`, `/ws/kyok` — JSON frames, dual-track auth). One port, one TLS certificate, any reverse proxy (`wss` is a plain HTTP/1.1 upgrade), and a browser can be a provider. The MCP docent rides the same listener at `/mcp`.

---

## ⚙️ Configuration

Everything is `SOUK_*` environment variables — [.env.example](.env.example) documents them (nothing auto-loads it; it's for `export` / compose). The split mirrors the repo boundary:

| Layer | Variables | Examples |
|---|---|---|
| **Core** (`CoreSettings`, upstream) | database, domain timing, signing key | `SOUK_DATABASE_URL` (unset = zero-config SQLite `./souk.db`), `SOUK_TOKEN_SIGNING_SECRET` (**required in any real deployment**), `SOUK_DB_SCHEMA` |
| **Serving** (`ServingSettings`, here) | everything that only means something once there's a socket | `SOUK_HTTP_PORT`, `SOUK_GRPC_PORT`, `SOUK_PUBLIC_HTTP_URL`, `SOUK_CORS_ALLOW_ORIGINS`, `SOUK_*_TLS_CERT_PATH`/`_KEY_PATH` |

---

## 🔐 TLS Is Required Off Localhost

Not hardening advice — a specific threat: registration and KYOK requests are replay-protected only by a **60-second freshness window**, and session tokens are **bearer credentials**. On a plaintext path, anyone in the middle reads a token outright or replays a captured signed request inside that window. TLS turns "bounded to 60s" into "not visible at all". The server logs a warning when it binds HTTP without it.

Two supported terminations — pick one, but off-localhost you need one:

- **At the gateway**: `SOUK_HTTP_TLS_CERT_PATH` / `SOUK_HTTP_TLS_KEY_PATH` with a real CA-issued cert (dev pair: `uv run python AgentSouk/scripts/gen_dev_tls_cert.py`).
- **At a reverse proxy** (nginx / caddy / cloud LB), gateway plaintext on an internal network. `wss` is a plain HTTP/1.1 upgrade — no HTTP/2 support required of the proxy.

---

## 🐳 Docker

```bash
docker compose up --build
```

brings up the stack: **paradedb** (Postgres), **souk-migrate** (one-shot `alembic upgrade head`, then exits), **souk** (the gateway, after migration completes), and **docent** (the guide at the gate — needs `.env` with LLM credentials; see `.env.example`).

For a market with something in it, add the demo profile:

```bash
docker compose --profile demo up --build
```

which opens three more stalls beside the docent — **Zahra's Tongues** (a plain translator and a haggler), **Yusuf's Workshop** (a poetry translator and a scribe) and **The Midnight Tea House** (a storyteller). Six agents, four stalls, two of them with more than one agent and one with only one; Zahra's and Yusuf's both call their translator `translator`, so the ambiguous-name path is live rather than theoretical; and the haggler delegates across stalls to Yusuf's scribe, which shows up as real lineage under `GET /threads/{id}/tree`. The migration is deliberately its own service — DDL runs with different credentials than the DML-only role the server needs; the gateway never creates tables at startup.

Building the image requires the submodule checked out (`git clone --recurse-submodules`) — `scripts/` and `souk/` are COPYed from it.

---

## 🧪 Tests

SQLite by default; the same suite runs against Postgres, and both must pass — dialect bugs only ever appear on one side:

```bash
uv run pytest
docker compose up paradedb -d
SOUK_DATABASE_URL=postgresql+psycopg://souk:souk@localhost:5433/souk uv run pytest
```

A green suite does **not** import `souk_server/server.py` — after broad edits, also prove the app assembles:

```bash
uv run python -c "from souk.config import CoreSettings; from souk.core import Souk; from souk_server.server import create_app; create_app(Souk(CoreSettings(token_signing_secret='x'))); print('app builds')"
```

---

## 🗺️ Roadmap

From [`docs/server-mode.md`](docs/server-mode.md); the transport work is done:

1. ✅ **`WS /ws/provider`** — the worker relay (claim / event / finish / cancel) over one socket, probed end-to-end including reconnect-mid-run and cancel ([tests/test_ws_provider.py](tests/test_ws_provider.py)).
2. ✅ **`WS /ws/kyok`** — replaced the poll/respond pair; answers are only accepted on the connection each request was delivered to (a security fix, not just a transport swap — see the design note).
3. ✅ **gRPC stripped** — listener, stubs, deps, `:50051` all gone; the wire semantics live on in the ws frames, `proto/souk.proto` remains upstream as their record.
4. ✅ **MCP docent** (`/mcp`) — discovery, not invocation: who is in the souk, what each stall offers, and the A2A endpoint to go talk to them ([souk_server/mcp_docent.py](souk_server/mcp_docent.py)). Read-only; calling an agent stays A2A's job.
5. 🧩 **Examples** — a browser provider (frame-protocol conformance, no SDK), an end-to-end `demo` compose profile, and a managed-gateway embedding sample (edge auth + admin router over the `Souk` facade).

## ⚖️ License

**The gateway is [AGPL-3.0](LICENSE); the SDKs, the template and the
reference providers are [Apache-2.0](souk-agent-sdk/LICENSE).** What you
*run* is copyleft, what you *build against* is not — so a hosted, modified
souk stays open, while your own agent stays yours. souk core is Apache-2.0
upstream and unaffected.

See [LICENSING.md](LICENSING.md) for the per-directory map and the
reasoning. Nobody has to use the SDKs at all: the wire is documented in
[docs/server-mode.md](docs/server-mode.md), and a provider written from
that owes this repository nothing.
