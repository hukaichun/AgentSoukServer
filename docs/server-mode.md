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
| Callers | MCP | streamable HTTP, same listener | later; see "MCP" below |
| Providers | work relay | `WS /ws/provider` | to build |
| KYOK bridge | completion relay | `WS /ws/kyok` | to build |
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

## MCP (recorded decision, separate work)

MCP lands on the same HTTP listener later, as an adapter **in this
repo**, not in core: the official SDK drags transport dependencies core
is forbidden to have, and MCP has no in-process consumer — its only rung
is the wire, so the "protocol translation is core" rule does not bind
it. Written as two layers (pure mapping over souk's types + SDK
binding) so the mapping can be promoted to core if a second consumer
ever appears. Mapping sketch to design properly at build time: agent →
tool, run events → progress notifications, `input-required` →
elicitation.

## Where examples live

Split by what an example teaches, not by where it happens to run:

| example | teaches | lives | why |
|---|---|---|---|
| `agent-template`, python providers | writing a provider against **souk-agent-sdk** | upstream (AgentSouk) | they are the SDK's teaching material and a provider is gateway-agnostic — it connects to any souk |
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

Upstream (`souk-agent-sdk`, `souk-client-sdk`) needs ws transports to
match — freely rewritable for the same no-users reason. Out of scope
here; belongs with the Phase 2 upstream cleanup.

## Build order

1. `WS /ws/provider` + a probe provider driving a real run end-to-end
   (including reconnect-mid-run and cancel — the two cases reading code
   gets wrong; see upstream CLAUDE.md).
2. `WS /ws/kyok` + the completions relay against it; delete
   `poll`/`respond`.
3. Strip gRPC (the removal list above); `ServingSettings` loses its
   `grpc_*` fields.
4. Upstream: SDK ws transports, alongside the already-planned removal
   of `souk-server/` from AgentSouk.
