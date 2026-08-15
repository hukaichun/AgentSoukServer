# Server mode: one port, WebSocket relay

Status: **design, not yet implemented.** Defines what this gateway serves
and over which transports. Supersedes the inherited HTTP+gRPC split.

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

**Auth is the first frame, not a header.** The browser WebSocket API
cannot set headers, and tokens in query strings leak into access logs.
The server waits (briefly) for:

```json
{"type": "hello", "token": "<session JWT>", "agentIds": ["..."], "maxClaim": 2}
```

verified exactly as PollForWork verified it — the token is the identity
(its `public_key`), `agentIds` must be ones that key registered. Reply is
`{"type": "welcome"}` or a close with a policy code. Anything else before
`hello`, or an invalid token, closes the socket.

After `hello`, the server drives the claim loop on the worker's behalf —
the same inversion the gRPC servicer performed: it calls
`souk.claim_work(token, agentIds, max_claim=…)` with `on_cancel` wired to
this socket, and pushes what it claims:

| direction | frame | carries |
|---|---|---|
| ↓ | `{"type": "run", "runId", "threadId", "input"}` | a claimed run **with its RunAgentInput** — claiming is the hand-over, unchanged |
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

One socket per caller session, hello-authenticated like the provider
socket (`{"type": "hello", "token": "<kyok token>"}` — the same bearer
`respond` verifies today, carrying the KYOK HMAC material the adapter
already checks):

| direction | frame | carries |
|---|---|---|
| ↓ | `{"type": "completionRequest", "requestId", "payload"}` | what `poll` returned, pushed instead of polled |
| ↑ | `{"type": "chunk", "requestId", "data"}` | one chunk of the bridge's LLM response (was a line of the `respond` NDJSON stream) |
| ↑ | `{"type": "done", "requestId"}` | end of that response (was the `_DONE` sentinel / EOF) |
| ↑ | `{"type": "error", "requestId", "message"}` | bridge-side failure, so the waiting completion can fail fast instead of timing out |

`requestId` multiplexing means one bridge socket serves concurrent
completions — strictly better than `poll_one`'s one-per-cycle handover.

Provider and KYOK stay **two endpoints**, not one multiplexed socket:
different identity (provider key vs. caller session), different
lifetime (long-lived vs. per-session), different frames. Merging them
buys one route at the cost of role-dispatch on every frame.

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
