# Server mode: one port, WebSocket relay

Status: **implemented** (`souk_server/ws_provider.py`,
`souk_server/ws_kyok.py`), gRPC removed. Defines what this gateway
serves and over which transports. Supersedes the inherited HTTP+gRPC
split.

Nothing here is constrained by compatibility: souk is unreleased, this
gateway has no deployments, and the gRPC SDK has no users. This is the
cheapest possible moment to pick the final shape, so we pick it outright
rather than staging a migration.

## The decision

One listener. Everything — callers, providers, KYOK bridges — arrives on
a single HTTP port. Outbound-claim stays (souk never connects to anyone;
that is the architecture, forced by NAT topology, and it is not up for
revision). What changes is the *carrier* for the two claim-based edges:
a persistent WebSocket each, replacing gRPC entirely.

| Who | Surface | Transport | Status |
|---|---|---|---|
| Callers | AG-UI (`/agui/*`, `/threads/*`), A2A (`/a2a/*`), registry (`/agents*`), health | HTTP + SSE | exists, unchanged |
| Callers | MCP (the docent) | streamable HTTP at `/mcp`, same listener | built; see "MCP: the docent" below |
| Providers | work relay | `WS /ws/provider` | built |
| KYOK bridge | completion relay | `WS /ws/kyok` | built |
| Provider's model client | `POST /kyok/v1/chat/completions` | HTTP (OpenAI-compatible by definition) | exists, unchanged |

gRPC is **removed**, not demoted to an option: `grpc_server.py`,
`grpc_gen/`, the proto generation step, the `grpcio`/`protobuf`
dependencies, port 50051, and the Dockerfile's stub-gen step all go. A
transport with zero users is not an option worth maintaining; the wire
*semantics* it carried are kept (below), because those were the hard-won
part — `proto/souk.proto` remains upstream as the record of them.

What one port buys: one TLS certificate, one load-balancer rule, no
HTTP/2 requirement on proxies (wss is an HTTP/1.1 upgrade), and a
browser can be a provider — which is the audience that makes WebSocket
the right default rather than a fallback.

Core is untouched throughout. The worker inversion reduced the
provider wire to three calls (`claim_work` / `report_event` /
`finish_run`) plus a cancel push; a transport is just a carrier for
that port, and this is the third carrier after in-process and gRPC.
The KYOK edge swaps the same way because `LLMBridge` is likewise a
transport-free port; only this repo's serving layer changes.

## Provider relay: `WS /ws/provider`

Frames are JSON text messages, camelCase — matching the AG-UI/A2A wire
style, readable in devtools, and free for the browser providers that
justify ws in the first place. The semantics mirror `proto/souk.proto`'s
PollForWork/AgentSession, minus what a single duplex socket makes
redundant.

**Auth is dual-track: an `Authorization` header when the client can set
one, the first frame when it can't.** Applies to both sockets (`/ws/
provider` and `/ws/kyok`). Tokens never go in query strings — those leak
into access logs.

- **Header track.** A non-browser client sends `Authorization: Bearer
  <token>` on the handshake request. This exists for managed
  deployments: an edge middleware (or reverse proxy) can then gate the
  connection *before* it is accepted, which frame-level auth can never
  offer — at handshake time a hello-only design carries no credential
  for the edge to check. Note for embedders: Starlette's
  `BaseHTTPMiddleware` never sees WebSocket scopes; edge gating must be
  pure ASGI middleware.
- **Hello track.** The browser WebSocket API cannot set headers, so a
  handshake without the header is accepted pending, and the server
  waits (briefly) for the credential as the first frame:

```json
{"type": "hello", "token": "<session JWT>", "agentIds": ["..."], "maxClaim": 2}
```

If the header was present and valid, `hello` still arrives first (it
carries `agentIds`/`maxClaim`, which are not auth) but may omit `token`.
A `hello` token alongside a header token must match, else close.

Either way the token is verified exactly as PollForWork verified it —
the token is the identity (its `public_key`), `agentIds` must be ones
that key registered. Reply is `{"type": "welcome"}` or a close with a
policy code. Anything else before `hello`, an invalid token, or no
credential on either track, closes the socket. Concretely: `hello` must
arrive within 5 seconds of the handshake; every handshake refusal closes
with 1008 (policy violation) and a reason string; 1011 is reserved for
server-side failure the client didn't cause — including the session
token expiring under a long-lived socket, which closes with 1008 so the
client's ordinary reconnect (which re-registers, refreshing the token)
is the recovery path.

After `hello`, the server drives the claim loop on the worker's behalf —
the same inversion the gRPC servicer performed: it calls
`souk.claim_work(token, agentIds, max_claim=…)` with `on_cancel` wired to
this socket, and pushes what it claims:

| direction | frame | carries |
|---|---|---|
| ↓ | `{"type": "run", "runId", "threadId", "agentId", "input"}` | a claimed run **with its RunAgentInput** — claiming is the hand-over, unchanged. `agentId` rides along because the worker routes by it and RunAgentInput does not name it |
| ↑ | `{"type": "event", "runId", "event"}` | one AG-UI event; authorized against `Run.claimed_by`, unchanged |
| ↑ | `{"type": "finish", "runId"}` | that run's stream ended (was `end_of_stream`) |
| ↓ | `{"type": "cancel", "runId"}` | a request, not an order — outcome decided when the stream ends, unchanged |
| ↓ | `{"type": "error", "message", "runId"?}` | server-side rejection of a frame (bad runId, not the claimer) |

Flow control is the `maxClaim` budget, enforced where it always was: the
server claims further runs only while in-flight (claimed − finished) is
under the budget. No credit frames; `finish` is the credit.

Liveness: claiming marked a provider seen, and still does — the server's
claim loop touches liveness exactly as PollForWork did. WebSocket
ping/pong keeps intermediaries from reaping idle sockets but is not the
liveness signal.

**A dropped socket ends nothing.** Events are addressed by `runId`, so a
worker reconnects (a fresh `hello`) and reports the rest, including how
runs ended. souk records nothing at disconnect; a worker that is truly
gone is caught by the stall sweep. This property was probed and kept
under gRPC and must be preserved: reconnect-and-finish is a test to
carry over, not a hope.

At-least-once delivery (an ack per event) remains expressible and
remains unbuilt — same status as under gRPC. The `reserved 5` lesson
travels as words here: a retired frame type's name is never reused.

## KYOK relay: `WS /ws/kyok`

Replaces the `GET /kyok/poll` + `POST /kyok/respond/{id}` pair. The
provider-facing `POST /kyok/v1/chat/completions` endpoint is untouched —
an OpenAI-compatible URL is the whole point of that side.

The socket opens with `{"type": "hello", "sessionId": "<session id>"}`.
`sessionId` is a **routing key, not a credential** — the same
caller-minted, souk-opaque string `poll` took (souk neither mints nor
verifies it; see souk/kyok.py). souk has no caller identity to bind a
bridge credential to, deliberately: *who* may present a session is the
deployment's business, enforced at the edge (pure ASGI middleware, before
accept — the header track exists for exactly this). What the socket
still buys over the query string: `sessionId` no longer appears in any
URL, so it stops leaking into access logs — the mistake the old
`/kyok/poll?sessionId=…` was making against this document's own rule.

| direction | frame | carries |
|---|---|---|
| ↓ | `{"type": "completionRequest", "requestId", "payload"}` | what `poll` returned, pushed instead of polled |
| ↑ | `{"type": "chunk", "requestId", "data"}` | one chunk of the bridge's LLM response (was a line of the `respond` NDJSON stream) |
| ↑ | `{"type": "done", "requestId"}` | end of that response (was the `_DONE` sentinel / EOF) |
| ↑ | `{"type": "error", "requestId", "message"}` | bridge-side failure, so the waiting completion can fail fast instead of timing out |
| ↓ | `{"type": "error", "requestId"?, "message"}` | server-side rejection of a frame (unknown type, or a `requestId` not in flight on this connection) — answered, not a teardown, same as the provider socket |

`requestId` multiplexing means one bridge socket serves concurrent
completions — strictly better than `poll_one`'s one-per-cycle handover.

Connection semantics, decided with the implementation:

- **Sockets sharing a `sessionId` coexist**, each completion request
  going to whichever polls first — the race the HTTP poll already had.
  Pretending to enforce one-bridge-per-session would be ritual, not
  security: without a real bridge credential, "the bridge" *is* whoever
  presents the sessionId. Tightening this waits on a caller-identity
  primitive, which souk deliberately doesn't own.
- **An answer is accepted only on the connection its request was
  delivered to** (see the security note below).
- **A socket dropping mid-answer fails its in-flight completions
  immediately** (`{"error": …}` to the waiting provider) — a truncated
  answer must never pass as a complete one, and failing now beats the
  claim timeout. Requests delivered but unanswered when a socket dies
  are not re-queued; they fail the same way a crashed poll-era bridge
  failed them.

Provider and KYOK stay **two endpoints**, not one multiplexed socket:
different identity (provider key vs. caller session), different
lifetime (long-lived vs. per-session), different frames. Merging them
buys one route at the cost of role-dispatch on every frame.

### Why the socket is a security fix, not only a transport swap

The HTTP KYOK surface leaks a check the socket closes structurally, so
the migration is worth doing for that reason alone — not just port
consolidation.

**`POST /kyok/respond/{request_id}` has no authentication of its own.**
It reads the request body and streams whatever arrives into the pending
completion's queue (`KyokAdapter.respond` → `pending.response_queue`),
guarded by nothing but the `request_id` being unguessable — a
96-bit random `kyokreq_…` string (`souk.ids.new_id`). That is a
capability-string model: hold the string, drive the completion. It is
cryptographically hard to guess, but the string is not treated as a
secret everywhere it travels — `respond` logs it (`kyok respond %s:
dropping malformed line`), and, decisively, **nothing binds the
responder to the session that was handed the request.** Whoever presents
a valid `request_id` can supply the "LLM" answer for it, and that answer
is content the provider's agent then acts on — if that agent holds a
tool with side effects, injected completion output is injected tool
input.

The WebSocket removes the naked endpoint. After migration:

- `chunk` / `done` / `error` frames are accepted **only for requests
  delivered on that same connection** — the server knows exactly which
  `requestId`s it pushed down each socket, and membership in that set,
  not anything a frame carries, is what authorizes an answer. This is
  the binding the capability string never had, and it is deliberately
  connection-scoped state living in the serving layer: core has no
  concept of a connection, and has nothing of its own to verify on the
  bridge side (see the hello note above). `request_id` reverts to what
  it should have been all along: a multiplexing key *within* the
  connection that received it, not a bearer capability on an open route.
- There is no `request_id` in a URL or a log line to leak, and no
  unauthenticated route to replay it against.

This does not change what a *legitimate* bridge is trusted to do — souk
still does not validate the LLM output a bridge returns (see "Scope /
limitations" in `keep-your-own-key.md`; a caller manipulating its own
run's completions mostly harms itself, and a provider must treat KYOK
output as untrusted input regardless). It closes the gap where someone
who is *not* the bridge could answer at all.

**Out of scope here, tracked upstream:** two KYOK hazards live in core
(`souk/kyok.py`) and are unaffected by which transport this repo serves,
so they are fixed in AgentSouk, not here:

- unauthenticated `/kyok/poll` growing `KyokBridge`'s registries without
  bound (`defaultdict` read-inserts) — [AgentSouk#25](https://github.com/hukaichun/AgentSouk/issues/25).
- no per-run spend ceiling: a live run can drive unlimited completions of
  any size and model against the caller's key. The structural defense is
  that the bridge is the caller's own code and every request passes
  through it before money moves — so the fix is a default ceiling in
  `souk-client-sdk`'s bridge (per-run request/token caps, model
  allow-list), not a souk-side rule souk can't price. Tracked upstream.

## MCP: the docent

MCP is served on the same HTTP listener, as an adapter **in this
repo**, not in core: the official SDK drags transport dependencies core
is forbidden to have, and MCP has no in-process consumer — its only rung
is the wire, so the "protocol translation is core" rule does not bind
it. Written as two layers (pure mapping over souk's types + SDK
binding) so the mapping can be promoted to core if a second consumer
ever appears.

**Scope: discovery, not invocation.** Talking to an agent is A2A's
job — souk already serves cards and JSON-RPC for exactly that, and
wrapping `start_run` in an MCP tool would build a second, lossier
invocation path beside a standard one (an earlier draft of this section
did exactly that, and it was scoped out on review). What MCP adds is
the thing A2A assumes you already did: *knowing what is in this souk.*
MCP hands out the map; A2A does the walking.

**Built** as `souk_server/mcp_docent.py`, mounted at `/mcp` on the same
listener (`tests/test_mcp_docent.py`; probed end to end with a real MCP
client over a real socket). The audience is a **docent** — the guide who
walks a visitor through the market — not an operator, which is what
fixes the surface below at "who is here, what do they do, where do I go".

- **Tools** (read-only, `read_only_hint` declared): `browse_souk` (the
  market grouped by stall), `search_agents(query)` (over name,
  description, skills and tags), `describe_agent(name_or_id)`,
  `describe_stall(provider_key)`. Tools rather than resources alone
  because most MCP clients wield tools far more readily.
- **Resources.** `souk://providers` (stall-shaped) and `souk://agents`
  (flat, each record still carrying its provider), plus the
  `souk://agent/{agent_id}` template.
- **Every answer carries directions and a provider key.** The
  `a2a_endpoint` uses the `/a2a/id/{agent_id}` route, never the name
  route: display names are not unique across providers, and a direction
  that sometimes 409s is not a direction. The provider's `public_key`
  rides on every agent record because that is what makes an answer
  *placeable* — souk-directory groups by stall, and the AI-town layout
  derives a stall's map coordinate by hashing that key, so an agent named
  without its provider cannot be pointed at.
- **Ambiguity is handed back, not guessed.** A duplicate display name
  returns the candidates with their ids rather than picking one — the
  same refusal souk's own name route makes.
- **`online` never travels alone.** Each record pairs it with
  `last_seen` in words ("40s ago", "3d ago"), because the boolean cannot
  separate "stepped away" from "gone for a week" and that is the
  difference a visitor deciding whether to wait is asking about.
- **Notifications.** Not built. If built, necessarily two-track:
  `souk.on_change` fires for registrations and de-listings, but an agent
  going stale fires *nothing* — `online` is derived from `last_seen_at`
  against a window at query time, so there is no instant to fire on
  (souk/changes.py records this deliberately). A directory that
  advertises live updates off `on_change` alone would miss exactly the
  transition its users care most about; pair the hook with a slow poll,
  or just poll. Not load-bearing either way.
- **Not exposed:** invocation (A2A's job), registration/identity
  (provider business), KYOK (bridge business), threads/runs (run
  observation is a different feature with a different audience — add
  it later if wanted, deliberately absent now), admin (deployment
  policy — the managed-gateway example's job).

Core serves all of it from `list_agents` alone
([AgentSouk#31](https://github.com/hukaichun/AgentSouk/issues/31), now
closed: typed query models landed, enumeration was withdrawn as
unneeded). Search filters that roster in Python rather than querying —
a market's worth of stalls is not a log, and if a deployment ever
outgrows it, that is when a core query earns its place.

**The docent is also a stall.**
`providers/pydantic-ai-agent/config.docent.yaml` runs it as an ordinary
provider — its own key, a row in the roster, runs claimed over
`/ws/provider` — reaching the market through `/mcp` as a real MCP
client rather than through `GET /agents`. Two things that buys: every
frontend gets a guide instead of each one building its own, and the
surface above acquires a consumer, so a question `/mcp` cannot answer
shows up as a guide that cannot answer it, in a running process rather
than in review. (`souk_tools.py` — the same capability as plain
function tools — named this exact moment as when to prefer a real MCP
server, and stays off in that config so the model has one way to ask,
not two.) It runs unthrottled on purpose: backpressure is a feature at a
working stall and a bad front door at the gate.

**The one question the docent cannot answer: "are they busy right
now?"** Capacity is per-stall in souk's model (`maxClaim` is a
provider's budget across everything it hosts), and the roster carries
nothing about it — so "you'll have to wait, they're serving someone",
one of the few genuinely market-shaped answers a guide could give, is
unavailable. Note where the data actually is before reaching upstream
for it: this gateway knows each connected worker's `maxClaim` (it
arrives in the hello frame) and how many runs it has in flight (it
drives the claim loop), so the honest version is per-process serving
state, not a core projection — and it would read as authoritative while
being blind to workers connected to another replica.

## Where examples live

Split by what an example teaches, not by where it happens to run:

| example | teaches | lives | why |
|---|---|---|---|
| `agent-template`, python providers | writing a provider against **souk-agent-sdk** | upstream (AgentSouk), for now | written when the SDK lived there; with the SDKs relocated here, whether they follow or stay as path-dep consumers is settled in upstream's half of the move |
| end-to-end demo (gateway + agent, one `docker compose` command) | what the whole system looks like running | **this repo**, a `demo` compose profile | after souk-server leaves upstream, only this repo has a gateway to demo against; build contexts point into the submodule (`AgentSouk/agent-template/…`) so the example code keeps one home |
| browser provider (single HTML file speaking `/ws/provider`) | the frame protocol in this document, directly — no SDK | **this repo**, `examples/` | the frames are authored here, so their conformance demo belongs here; it is also the living proof of the claim that justified ws — a browser can be a provider |
| managed-gateway embedding (`create_app` wrapped in edge auth + an admin router over the `Souk` facade) | how a deployment adds management without this repo shipping policy | **this repo**, `examples/` | the embedding surface (`create_app`, `app.state.souk`) is this repo's contract |

This settles the one judgment call flagged in the extraction plan's
Phase 2 checklist: upstream's compose drops its `agent-demo`-style
services and slims to library development (database + tests); the demo
role moves here wholesale.

## What this removes from this repo

- `souk_server/grpc_server.py`, `souk_server/grpc_gen/`
- `grpcio`, `protobuf` runtime deps; `grpcio-tools` dev dep
- the `gen_proto.sh` invocation (README, Dockerfile) and the stub-gen
  Docker layer
- port 50051 everywhere: `ServingSettings.grpc_*` fields, Dockerfile
  `EXPOSE`, compose ports, TLS cert pair for gRPC
- adds: `websockets` (or uvicorn's built-in ws support — FastAPI's
  `WebSocket` route type needs no new top-level dependency)

The SDKs (`souk-agent-sdk/`, `souk-client-sdk/`) implement the matching
ws transports and live **in this repo** — relocated from upstream when
they were rewritten, because a wire client is network code and upstream
keeps none: this repo owns both ends of every wire it defines. Their
removal from AgentSouk (with `agent-template`/`providers/*` re-pointed at
the new home) is upstream's half of the move.

## Build order

1. `WS /ws/provider` + a probe provider driving a real run end-to-end
   (including reconnect-mid-run and cancel — the two cases reading code
   gets wrong; see upstream CLAUDE.md).
2. `WS /ws/kyok` + the completions relay against it; delete
   `poll`/`respond`.
3. Strip gRPC (the removal list above); `ServingSettings` loses its
   `grpc_*` fields.
4. SDK ws transports — done, and the SDKs moved into this repo with
   them (upstream keeps no network code; see "What this removes").
   Remaining upstream: delete the old SDK directories and re-point
   their consumers.
