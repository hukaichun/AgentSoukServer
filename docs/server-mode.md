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
| LLM providers (KYOK) | registration + roster + deletion (`/llm-providers*`), completion relay | HTTP + `WS /ws/kyok` | built |
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

Core is untouched throughout. The provider port is core's to state, and
it has since been inverted again — souk offers a run and the provider
answers, rather than the provider asking for work — so what a transport
carries today is `broker.ConnectedProvider`: who you are, how much you
will take at once, how to hand you a run, how to ask you to stop one. A
transport is just a carrier for that port, and this is the third carrier
after in-process and gRPC. The KYOK edge swaps the same way because
`LLMBridge` is likewise a transport-free port; only this repo's serving
layer changes.

## Addressing: an agent is `(provider, name)`

Every surface in that table takes both halves, and none takes anything
souk minted. There is no `agent_id` — core stopped minting one, because a
provider holding identifiers only souk could issue lost its whole
vocabulary whenever the database was replaced, with no way to rebuild it.

```
/a2a/{provider}/{name}/rpc      /a2a/{provider}/{name}/.well-known/agent-card.json
/agui/{provider}/{name}         /threads/{provider}/{name}
```

`{provider}` is the provider's Ed25519 public key or its 16-hex
fingerprint (`sha256(key)[:16]`); core tells them apart by length. This
gateway puts the fingerprint in URLs and the roster carries both, with
`provider_key` the one to compare — the fingerprint is derived from it and
never authoritative.

**The by-name routes are deleted, not deprecated.** A display name is not
unique: two identities may both register `translator`, and that is
allowed. A route taking a bare name therefore had to guess or refuse, and
guessing is how a caller reaches an agent it never meant to reach.

Resolving a name is still ordinary, and still supported — it is
`GET /agents`, done once by whoever holds the name, after which the pair
is what goes on the wire. Both SDKs do exactly this (`SoukClient.resolve`,
and the demo providers' sub-agent resolver), and both surface ambiguity to
their caller rather than picking. The difference from the old route is
only *where* the choice is made: somewhere the asker can answer it.

That is also why an address cannot be written into a config file. The
provider half is the callee's own key fingerprint, which does not exist
until that provider has started once and written its key — so a static
`a2a_url` for a sibling agent is unwriteable in principle, not merely
inconvenient. Providers resolve lazily, on first delegation, so a
delegation edge does not become a boot order that `depends_on` cannot
express.

## Provider relay: `WS /ws/provider`

Frames are JSON text messages, camelCase — matching the AG-UI/A2A wire
style, readable in devtools, and free for the browser providers that
justify ws in the first place. The semantics mirror `proto/souk.proto`'s
PollForWork/AgentSession, minus what a single duplex socket makes
redundant.

**souk hands work over; it does not wait to be asked for it.** There is
no claim loop on either side. souk's broker knows which provider serves
which agent, offers each run to it, and the answer comes back as a frame.
The socket carries the offer; `souk_provider_sdk.ProviderRuntime`, on the
far side, decides.

### Opening a socket: a mutual challenge-response

Four frames, two round trips, and **each side signs bytes the other
chose**. The payloads live in `souk_server/handshake.py`, which is the
spec both halves are written against; the SDK states them separately
rather than importing them, because a provider must not need the gateway
installed to be a provider.

```
provider → hello      { version, publicKey, agentNames,
                        maxConcurrentRuns, nonce }
souk     → challenge  { soukPublicKey, nonce, signature }
provider → proof      { signature }
souk     → welcome    { }
```

```
sig_s = souk.sign(     b"souk-auth:souk:"     + nonce_p + b":" + nonce_s )
sig_p = identity.sign( b"souk-auth:provider:" + nonce_p + b":" + nonce_s
                       + b":" + sha256(hello_raw) )
```

**What this replaced.** A provider used to open a socket by signing a
statement it composed itself —
`souk-provider-connect:{key}:{names}:{timestamp}` — in which the verifier
chose nothing: no nonce, no server identity, no binding to the connection,
only a timestamp checked against a 60-second window. Anyone who observed
that signature could replay it and attach as that provider. The old
docstring reasoned carefully about the neighbouring case — that reusing
the *registration* payload would let a captured registration be replayed
as a connection, hence the differing prefixes — and stopped one step
short. A prefix stops a signature being replayed as a different kind of
thing; it does nothing about a connection signature replayed as a
connection, which needs no change of use at all.

Why each piece is there:

- **Both nonces in both signatures.** Each side contributes freshness, so
  a recorded exchange is worth nothing to whoever recorded it.
- **`souk:` / `provider:` prefixes.** Neither signature can be presented
  as the other. Without them the two payloads would differ only by a
  trailing digest.
- **`sha256(hello_raw)`.** Binds the claims: `agentNames` and
  `maxConcurrentRuns` cannot be altered in flight. A digest rather than a
  re-send, because `hello` goes out before `nonce_s` exists — and a
  digest of *the bytes actually sent*, since key order and separators are
  free choices in JSON and re-encoding the same values can produce
  different bytes.
- **souk signs first.** A provider must be able to walk away from a souk
  it does not recognise *before* producing anything worth stealing.
  Signing second would mean handing a credential to whatever answered the
  URL and only then asking who it was.

`sig_s` is the half souk never had: until now it proved nothing to
anybody, so a provider connected to a URL and trusted whatever picked up.
A souk with no `SOUK_IDENTITY_PRIVATE_KEY` says so — `soukPublicKey:
null`, no signature — rather than failing, which is an honest report of
today's deployments; a provider that pinned a key refuses it, and one
that pinned nothing is no worse off than before.

**Provider-side trust.** `SoukProvider(souk_public_key=…)` pins one souk,
and that is the recommended shape: the provider already receives the souk
URL from somewhere, and the same channel carries a fingerprint. Unset, the
provider still checks that whoever answered holds the key it presents —
enough to notice a broken souk, not enough to notice a substituted one —
and logs the fingerprint so the value to pin is in reach.

TOFU is deliberately **not** built. It reads as free safety and is not:
souk's key is provisioned, so any deployment that rotates or regenerates
it jams every provider at once, with the recovery being to go and clear a
pin on each. A configured key costs one line and has no such state.

**Channel binding is out — decided, not overlooked.** It is the standard
answer to a relay, and unusable here: a Zscaler-class proxy terminates and
re-originates TLS by design, so the two sides never derive the same value
and the check fails every time. Enforcing it would not harden the
deployment; it would lock out every enterprise running one, which is the
deployment this exists for. It was also never the fix — the defect is a
*stealable* credential, and challenge-response closes that with an
intercepting proxy in the path:

| | before | after |
|---|---|---|
| see the traffic | yes | yes — it terminates TLS, that is its job |
| capture the credential, connect later as that provider | **yes** | **no** — cannot answer a fresh nonce |
| tamper with frames on a live connection | yes | yes |

The bottom row stays open, deliberately: run inputs and events are not
individually signed, an intercepting proxy is in the trust model by
construction (the enterprise installed it and pushed its CA), and signing
every frame is a large cost against a threat the operator chose.

**`version` is in `hello`, and there is no compatibility branch.** A hard
cutover: the old shape carried no version field, so it cannot be accepted
and told apart from a corrupt frame, and its absence is what the refusal
names — a bare signature failure is what an attack looks like too, and
would send whoever is debugging it somewhere unhelpful. Dual-shape
acceptance was considered and skipped because every provider that exists
is in this repo behind one SDK; the field is there so the *next* change
has something to select on, which is when it earns its keep.

**`welcome` is queued before attaching**, and that ordering is
load-bearing: attaching is what makes the provider reachable, and the
broker begins offering inside `attach_provider`'s own awaits — so queueing
it after lets a provider with work already waiting receive a `run` frame
as the first thing after its proof. A client reading exactly one frame
there raises and reconnects into the same race forever. Every handshake
refusal closes with 1008 (policy violation) and a reason string; 1011
stays reserved for server-side failure the client didn't cause.

### Once attached

| direction | frame | carries |
|---|---|---|
| ↓ | `{"type": "run", "runId", "threadId", "agentName", "input"}` | an **offer**, with its RunAgentInput. `agentName` rides along because the provider routes by it and RunAgentInput does not name it |
| ↑ | `{"type": "ack", "runId", "accepted", "reason"?}` | whether this provider took it. A bare `accepted: false` is how a full one says so — transient, souk re-offers later. `reason` makes the decline *permanent* (an input that does not parse): souk fails the run with the provider's words recorded verbatim in `failureReason` and stops re-offering. souk invents no reason vocabulary; the string is the provider's own |
| ↑ | `{"type": "event", "runId", "event"}` | one AG-UI event; authorized against `Run.claimed_by` |
| ↑ | `{"type": "finish", "runId"}` | that run's stream ended |
| ↓ | `{"type": "cancel", "runId"}` | a request, not an order — outcome decided when the stream ends |
| ↑ | `{"type": "query", "queryId", "method", "params"}` | a question about the work souk gave this provider |
| ↓ | `{"type": "queryResult", "queryId", "result"?, "error"?}` | its answer, correlated by `queryId` |
| ↓ | `{"type": "error", "message", "runId"?}` | server-side rejection of a frame (bad runId, not the holder) |

### Queries: the one thing here that expects an answer

Every other frame is fire-and-forget. `query` is not, and it is worth
being explicit about why it earns the machinery — a correlation id, a
pending map, a timeout, and a rule for a socket that dies mid-question.

**A provider sees exactly what the caller sent for its run, and nothing
more.** An AG-UI client resends its whole history every turn by
convention; A2A's `message/send` carries one message. The same agent,
unchanged, cannot tell a tenth turn from a first — and souk has held the
thread the whole time. `souk_provider_sdk.SoukLink.thread_messages` is
the question, and this is how it crosses a wire.

```json
↑ {"type": "query", "queryId": "9f3c…", "method": "thread_messages",
   "params": {"threadId": "thread_…", "limit": 20}}
↓ {"type": "queryResult", "queryId": "9f3c…", "result": [ …messages… ]}
```

- **`limit` is applied by souk**, not by the caller on return. The
  parameter exists to keep the response frame bounded; trimming after
  receiving would bound nothing and put a months-old thread on the wire to
  do it.
- **A provider may only read threads for agents it serves.** Not in the
  upstream design and added here. Thread ids are not guessable, but
  unguessable is not an authorization rule: a provider that served one run
  knows that thread id permanently, and would otherwise keep reading the
  conversation after being de-listed, or after the agent moved to another
  stall. A thread names its agent and an agent is `(provider_key, name)`,
  so souk can already make the comparison. "Not yours" and "no such
  thread" get the *same* answer — telling them apart would confirm a
  thread's existence to somebody who may not read it.
- **A malformed query is answered, not dropped.** The far side is waiting
  on that `queryId`; silence costs it the full timeout for a mistake souk
  could see at once.
- **A dead socket fails its outstanding queries immediately**, rather than
  leaving them to time out. Unlike a run — which is addressed by `runId`
  and whose frames go out on whatever connection is next — a question was
  asked of *this* connection and nothing will ever answer it. It is not
  retried on reconnect either: the agent asked mid-run, and whether it
  still wants the answer is the agent's to decide.
- **What may be asked is deliberately short.** Upstream's
  `contract.LINK_QUERY_METHODS` states the rule — this is not a mirror of
  souk's API, because every method admitted is one more frame type every
  transport must carry. The gateway reads that set rather than retyping
  it, so a method added upstream without a frame here fails a test instead
  of a provider.

Adding these frames does **not** bump `version`. They are additive: a
provider that never asks is unaffected, and an older gateway answers an
unknown frame type with `error`. The version selects the *handshake*,
which is the part that genuinely cannot interoperate across shapes.

### Which object is which

`SoukLink` is one provider joined to one souk — both directions, one
object — and the socket client in souk-agent-sdk is one, because over a
wire that is literally true: run frames arrive on the same socket event
frames leave by.

The gateway's `SocketProvider` is **not** one, and upstream's own docstring
says so. It sits on souk's side, holds an outbound queue and no runtime,
and only carries work outward. It satisfies souk's `ConnectedProvider`
protocol structurally, and checks itself against
`contract.CONNECTED_PROVIDER_ATTRS` at construction — because souk sizes a
capacity bucket from `max_concurrent_runs`, and a connection that forgets
it attaches perfectly well and then fails inside the broker, three layers
from the cause.

A declined offer costs the run nothing: it stays queued and is offered
again when something changes — a run arriving, a provider registering, one
of this provider's runs ending. Not immediately, deliberately: asking
again at once is asking a provider that just said no, with nothing about
the answer having changed.

Flow control is `maxConcurrentRuns`, declared once at hello. souk keeps a
bucket that size and offers nothing while it is empty. No credit frames
and no counting in the transport — the number is a fact about the
provider, and souk sees for itself when a run ends.

Two deadlines apply to an offer, and only one governs. souk wraps every
offer in `RunBroker.deliver_timeout_seconds` (5s) because it has a single
delivery loop and an offer that never returns stops dispatch for
everybody. The gateway's own `ACK_TIMEOUT_SECONDS` is longer and is a
backstop for a souk that offers without a deadline. Answering after
either has run out is the same as declining: the ack arrives for a run
nobody is waiting on, and is dropped.

**Liveness is not a heartbeat.** `online` is `is_serving` — souk holds a
live provider mapping for that agent or it does not — so attaching *is*
being online, and a dropped socket takes its agents offline in the same
instant. There is no window to age out of and no `last_seen_at` clock to
read. Because a provider connects once for every agent it serves, its
agents go online and offline together, by construction. `last_seen_at` is
still recorded and still worth reading, but it now answers a different
question: how long since anybody was here, which `online` no longer says
anything about. WebSocket ping/pong keeps intermediaries from reaping
idle sockets and is not the liveness signal.

**A dropped socket ends nothing.** Events are addressed by `runId`, so a
provider reconnects (a fresh `hello`) and reports the rest, including how
runs ended. souk records nothing at disconnect; one that is truly gone is
caught by the stall sweep. This property was probed and kept under gRPC
and must be preserved: reconnect-and-finish is a test to carry over, not
a hope.

At-least-once delivery (an ack per *event*) remains expressible and
remains unbuilt — the `ack` frame above answers an offer, not an event.
The `reserved 5` lesson travels as words here: a retired frame type's
name is never reused.

## KYOK relay: `WS /ws/kyok`

The socket an **LLM provider** connects out on — the party upstream's
KYOK redesign made first-class (`AgentSouk/docs/keep-your-own-key.md`).
The agent-provider-facing `POST /kyok/v1/chat/completions` endpoint is
untouched — an OpenAI-compatible URL is the whole point of that side.

This section previously described a different wire: an anonymous
"bridge" that rendezvoused with souk over a caller-minted `sessionId`,
with a paragraph of apology for everything a routing key that is secretly
a credential cannot do (who may open a session, whether two sockets are
the same party, what a token may safely carry). Upstream's answer was not
a better session id but an identity: the party answering completions
registers Ed25519 offerings like any provider and attaches like any
provider, and every one of those questions became answerable. The
`session_routing_key` fix this document used to describe — souk hashing
the session id before putting it in the token — is gone along with the
session id itself: a KYOK token now carries `{runId, providerKey,
agentName, exp}` and nothing caller-side at all.

The arrival is the agent provider's, rule for rule:

1. **register** — `POST /llm-providers/register` with
   `{models, metadata?, public_key, signature, timestamp}`, the signature
   over upstream's `souk-register-llm` payload
   (`souk_llm_provider_sdk.sign_llm_registration` builds it). Names are
   deliberately not exclusive across identities: two providers both
   offering `gpt4` is normal, and an offering is `(provider_key, name)`
   exactly as an agent is.
2. **attach** — this socket, opened with the same four-frame mutual
   challenge-response as `/ws/provider` (same `handshake.py` payloads,
   same version), the hello carrying `modelNames` where the provider
   socket says `agentNames`. Core refuses an attach for a name this key
   never registered; a socket that drops takes its offerings offline in
   the same instant, and a re-attach mid-run just works because a run's
   binding names the offering, not the connection.

A caller opts a run in with
`metadata: {"kyok": {"llmProvider": {"providerKey", "name"}, "context"}}`
— no extra connection, no SDK required. souk binds the run at start,
strips `context` from everything it persists, and resolves
binding → attached link per completion call; not attached is a fast 503,
the same shape as an offline agent (`souk-client-sdk`'s `KyokBridge` is
the reference LLM provider and builds that metadata via
`run_metadata()`).

| direction | frame | carries |
|---|---|---|
| ↓ | `{"type": "completionRequest", "requestId", "runId", "providerKey", "agentName", "llmName", "context", "actorChain", "payload"}` | core's `CompletionRequest`, camelCased: the run, the *proven* calling agent, which of this provider's models was addressed, the caller's opaque context, the delegation chain, and the OpenAI-shaped body |
| ↑ | `{"type": "chunk", "requestId", "data"}` | one OpenAI `chat.completion.chunk`; validated on souk's side, an invalid one fails the completion |
| ↑ | `{"type": "done", "requestId"}` | end of that response |
| ↑ | `{"type": "error", "requestId", "message", "refusal"?}` | provider-side failure or refusal, so the waiting completion fails fast instead of timing out — policy (throttling, billing, refusing a chain it does not recognise) is the LLM provider's, and this frame is how it says no. `refusal` is a structured payload relayed to the calling agent *intact* (in-stream as the `{"error": ...}` value, or as `error` on the non-streaming 502 body) — the envelope souk guarantees; the vocabulary inside is the two roles' own |
| ↓ | `{"type": "error", "requestId"?, "message"}` | server-side rejection of a frame (unknown type, or a `requestId` not in flight on this connection) — answered, not a teardown, same as the provider socket |

`requestId` multiplexing means one socket serves concurrent completions.
A gap of `CHUNK_GAP_TIMEOUT_SECONDS` (120s) between frames of one answer
fails that completion — not a per-completion deadline, a
provider-is-gone detector for the case the socket has not noticed.

Connection semantics, carried over or sharpened:

- **An answer is accepted only on the connection its request was
  delivered to.** This survived the redesign because it was the security
  fix worth keeping, and it now holds against a *stronger* intruder than
  the old socket ever faced: a second connection with the same identity,
  attached for the same offering — every credential check passes — is
  still refused an in-flight requestId it was not delivered.
  `tests/test_ws_kyok.py` drives exactly that. Membership in the
  connection's in-flight table, not anything a frame carries, is what
  authorizes an answer; a requestId is a multiplexing key within the
  connection that received it, never a bearer capability on an open
  route.
- **A socket dropping mid-answer fails its in-flight completions
  immediately** — a truncated answer must never pass as a complete one.
  Requests delivered but unanswered when a socket dies are not re-queued.
- **Two sockets, one identity, one offering**: the later attach takes
  over the offering for future completions (core's relay maps each
  offering to one live link). In-flight answers stay bound to their own
  socket, per the rule above.

Provider and KYOK stay **two endpoints**, not one multiplexed socket:
one carries runs for agents, the other completions for model offerings —
different roster, different frames, and one identity may hold both at
once. Merging them buys one route at the cost of role-dispatch on every
frame.

What souk still deliberately does not do: validate the LLM output a
provider returns (a provider must treat KYOK output as untrusted input
regardless — see "Scope / limitations" in `keep-your-own-key.md`), or
impose a spend ceiling. The ceiling belongs to the LLM provider, which
is now an identified party with the material to enforce one — the run
id, the proven calling agent, the caller's context and the delegation
chain arrive on every `completionRequest` frame (AgentSouk#26).

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
  description, skills and tags), `describe_agent(name, provider="")`,
  `describe_stall(provider_key)`. Tools rather than resources alone
  because most MCP clients wield tools far more readily.

  `browse_souk` takes a required `only_online` rather than no arguments
  at all, and that is a workaround, not a design: a live model called the
  zero-argument version with `{}""` — invalid JSON — retried identically,
  and killed the run, while every tool here that takes a parameter was
  called cleanly. Optional was not enough; the model still chose to send
  nothing, and sending nothing is what it does badly.
- **Resources.** `souk://providers` (stall-shaped) and `souk://agents`
  (flat, each record still carrying its provider), plus the
  `souk://agent/{provider}/{name}` template.
- **Every answer carries directions and a provider key.** The
  `a2a_endpoint` is the pair route, `/a2a/{fingerprint}/{name}/rpc`,
  because that is the only kind of address there is: a display name is
  not unique across providers, so a direction built from one leads
  somewhere only by luck. The provider's `public_key` rides on every
  agent record beside the fingerprint, because that is what makes an
  answer *placeable* — souk-directory groups by stall, and the AI-town
  layout derives a stall's map coordinate by hashing that key. The
  fingerprint is derived from the key and is never the thing to compare.
- **Ambiguity is handed back, not guessed.** A duplicate display name
  returns the candidates, each with its own provider and address, rather
  than picking one. There is no route left that could pick one, which is
  the point: the refusal moved to where the asker can answer it.
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

## Serving state stays out of core's database

**Today the gateway persists nothing.** Its only database access is
`deps.get_session`, which borrows core's session and hands it to a route;
every table in the deployment belongs to `souk`, is created by
`souk/alembic/`, and is migrated by the one-shot `souk-migrate` service.
That is not an accident to preserve by luck — it is the state this rule
protects.

**When serving *does* need persistence, it is isolated from core's, and
the isolation is structural rather than a naming convention.** Whatever
it turns out to be — edge-auth records, rate limits, admin state, an MCP
event store for resumable streams — it does not get a table in core's
schema and never gets a revision in `souk/alembic/`.

Three reasons, in the order they bite:

1. **A shared migration chain merges the two repos.** `souk/alembic/` is
   upstream's, versioned with core. A gateway table added there makes
   `alembic upgrade head` from souk responsible for serving state, and
   makes this repo's schema a function of the submodule pin.
2. **It would break core's own readiness answer.** `Souk.health` compares
   `alembic_version` against `EXPECTED_SCHEMA_REVISION`, a literal in
   `souk/db_schema.py`. A chain carrying gateway revisions would move
   past what core expects, and core would report a database it is
   perfectly able to serve as not ready.
3. **The DDL/DML split is already load-bearing here.** `souk-migrate`
   exists so DDL runs with credentials the server itself never holds
   (README). Serving tables mean a *second* migrate step with the same
   split, not a merged one.

**Follow upstream's mechanism rather than inventing one** (see
`AgentSouk/souk/souk/db_schema.py` and `souk/alembic/env.py`): a schema
namespace read from the environment, quoted in exactly one place, ignored
on SQLite (which has no schema namespace at all), with a dependency-free
module holding the constants so both the app and its `env.py` can import
them without dragging in required settings. The serving version is the
same shape under its own names — a `SOUK_SERVER_DB_SCHEMA`, an
`alembic/` in this repo, its own expected-revision check.

Whether the two live in one database under separate schemas, or in two
databases entirely, is a deployment choice and both must keep working —
which sets the real test, and it is not the schema name:

> **No code path may put core state and serving state in one
> transaction.**

A shared session or a single `begin()` spanning both makes them one
database in practice however they are namespaced, and forecloses the
split deployment silently. Serving persistence therefore gets its own
engine and sessionmaker, not `souk.session()`.

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
