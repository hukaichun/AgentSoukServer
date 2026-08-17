# Provider examples

souk doesn't run any agent logic itself — every actual agent is a "provider"
that connects out to a souk and speaks the gateway's WebSocket frame
protocol (authored in AgentSoukServer's `docs/server-mode.md`).
`souk_agent_sdk` is a convenience client for that contract, not the contract
itself: any implementation of those frames, in any language, is an equally
valid provider.

- **`/agent-template`** (repo root, not under here) — the minimal reference:
  the smallest possible `souk_agent_sdk.AgentHandle` implementation, no
  framework attached. Start here to understand the contract, or copy it as
  the seed for a provider written from scratch.
- **`pydantic-ai-agent/`** — a fuller example: YAML-configured agents backed
  by [pydantic-ai](https://ai.pydantic.dev), with MCP tool support and
  sub-agent delegation over A2A.
- **`openai-compat-agent/`** — wraps any OpenAI-compatible chat endpoint
  (a raw LLM, or a full agent that just happens to speak that wire format,
  e.g. [Hermes Agent](https://hermes-agent.nousresearch.com/)'s own API
  server) as a souk provider, with zero prompting/orchestration of its
  own. Optional, off by default — see its own README for how to bring it
  up (`docker compose --profile hermes-demo up`).

Add new provider examples as siblings of `pydantic-ai-agent/` here (e.g. a
different LLM framework, a non-Python implementation, a HITL-approval demo)
rather than growing `/agent-template` — that one stays deliberately minimal.

## Pinning a sub-agent delegation target

A sub-agent is declared by name — `- name: scribe` — and the address is
resolved from the roster on first delegation. The finished URL cannot be
written in a config file: it contains the *callee's* key fingerprint, which
does not exist until that provider has started once and written its key.

Names are not unique or reserved: any identity may register any name (see
souk's README, "Provider identity"), and the demo market has two stalls
each keeping a `translator` on purpose. When a name resolves to more than
one, the delegation is **refused, naming both stalls** — never guessed,
because guessing is how a caller reaches an agent it never meant to. Say
which one with `provider:`, taking the fingerprint or the full public key
from any roster row:

```yaml
sub_agents:
  - name: translator          # the tool the model sees: call_translator
    agent: translator         # who it reaches, if that differs
    provider: 77e2e50fded4ff48
```

`a2a_url:` remains as an escape hatch for a complete URL used verbatim and
never looked up — an agent on a *different* souk, or one reached through
something other than this gateway.
