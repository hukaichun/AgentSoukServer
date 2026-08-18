# Threat model: the agent that dials out

Status: **design record**, not implemented behaviour. This documents a
danger the architecture invites, the one defensive property this repo
actually holds against it, and the boundary that must not be crossed
silently. It exists because a reviewer looked at `providers/pod-probe-agent`
and asked the right question — *is this thing safe?* — and the honest answer
needed more room than a code comment.

Read `docs/server-mode.md` first for the wire; this assumes it.

## The uncomfortable shape

Every provider here **dials out** to a souk and then serves a loop: it
waits for a run, answers it, waits again. That shape is forced by NAT
topology (souk never connects in — see server-mode.md) and it is correct.
It is also, drawn without the labels, the shape of a remote-access trojan:
a program on a host that connects out to a server it does not control the
placement of, sits in a loop, and does what the operator on the other end
directs. Outbound-only, no inbound port, blends with normal egress. The
resemblance is not a flaw in the design; it is intrinsic to *any*
call-home agent, ours and Datadog's and Teleport's alike.

What turns the resemblance into a weapon is one change: **give the model
the tools directly, and let them write.** `providers/pod-probe-agent`
deliberately does not — its README says handing the model the tools is
"a later, larger step … out of scope on purpose" — but that is one edit of
one file away. Cross it and the remaining skeleton (a `scratch` static
binary, outbound-only, auto-registering under the pod's own name, borrowing
the caller's model credential through KYOK so it carries none of its own) is
a more complete implant framework than most that get names. souk becomes
the C2.

This is **dual-use**, and stating it plainly is the point. The skeleton
that makes a good read-only probe makes a good implant. The difference is
never the architecture; it is what the actor can *do*, whose consent put it
there, and which direction every design decision was made in.

## This is not novel, and that is load-bearing

The pattern above is documented. It is not a discovery, and nothing in this
repo discloses a capability the offensive-security world lacks:

- OWASP's **LLM06: Excessive Agency** (GenAI Security Project, 2025) names
  the whole failure class — excessive functionality, permissions, autonomy
  — and its mitigation is already "treat the model as an untrusted decision
  layer and keep enforcement in external systems."
- Cloud Security Alliance's **Agentic C2: AI Agents as Command-and-Control
  Infrastructure** (2026) analyses exactly the call-home-agent-as-C2 shape.
- **Hiding in the AI Traffic: Abusing MCP for LLM-Powered Agentic Red
  Teaming** (arXiv:2511.15998, 2025) states the defensive core almost
  verbatim to what follows here — "you cannot abuse what you cannot invoke,"
  read-only constraints against exfiltration, default-deny egress,
  controls at the deployment layer over behavioural detection of the model.

The consequence: there is nothing here to *report* to a CERT or a vendor.
Reporting is for an observed incident, a specific product's un-mitigable
flaw, or a genuinely new capability — and "a call-home agent can be
weaponised" is a thirty-year-old category with a name. What this repo can
add to the public record is the *other* half: a working, defensive-by-design
instance of the pattern. The literature argues what the safe shape is; it is
short on a concrete agent that *is* that shape. `pod-probe-agent` is one.

## Four doors, and where each one gives

The instinct that this is undefendable comes from looking at the wrong
layer. Defence does not happen at "can I out-think the model's prompts" —
that layer genuinely cannot be held and should not be tried. It happens at
the layers the model does not touch: getting onto the host, executing there,
and reaching the network. There are four, and honesty requires naming the
precondition under which each fails, because against a capable attacker most
of them do.

| Door | What it stops | Collapses when |
|---|---|---|
| **Landing & execution** | an unknown binary getting onto the host and running | rarely — this is the thick one; unsigned, unknown, rare binaries are exactly what endpoint controls and allowlisting exist to catch |
| **Egress control** | the loop reaching *any* C2 | the network is not default-deny — and most enterprise networks allow broad outbound to :443 |
| **Action / DLP detection** | malicious actions and sensitive data leaving | the actor only reads (`ls`, `cat` are indistinguishable from an admin's `ssh`) and exfiltrates inside legitimate-looking traffic |
| **Model-provider abuse detection** | the model provider seeing itself used to plan an attack | the attacker self-hosts the model or relays through their own souk — then there is no third party in the loop |

Two things follow that a defender must internalise:

**Egress control is an allowlist, not a blocklist.** The reflex is "can the
network catch *souk*?" — and that reflex loses, because an attacker's souk
can sit on any IP. The mechanism that works is default-deny: permit a small
set of known destinations, refuse everything else. Then the attacker's souk
fails not because it was identified but because it was never permitted; that
souk being open-source and rehostable anywhere is irrelevant. `SOUK_PUBLIC_KEY`
(pin the one souk this provider trusts) plus a NetworkPolicy allowing only
that destination is the deployable form — and it is off by default, which
puts it in the same box as TLS-off and CORS-`*`: correct but unarmed until a
deployment arms it.

**Doors 2 and 3 are coupled.** Reading is invisible at the action layer; the
theft only becomes observable when the data *leaves*. So DLP's catch depends
on egress being watched or constrained — open the egress door and the action
door weakens with it. Against a capable attacker (permissive network,
self-hosted model) the four collapse toward one: landing and execution. Which
is the honest reason the delivery step — "get the loosened binary onto
someone else's host" — is where this stops being a design conversation and
becomes a crime, and is the part no artifact here assists.

## The move that does not collapse: remove the capability

Detection is probabilistic and, as the table shows, gives way under pressure.
The one thing that does not is **making the actor incapable**. You cannot
reliably detect a `cat .env` — so the durable answer is not to detect it
harder but to ensure the actor cannot open `.env` at all. Detection is a
guess that fails; a capability the binary does not have is a certainty that
does not.

This is why `pod-probe-agent` is shaped the way it is, and every one of its
constraints is a deliberate application of this principle, not caution:

- **The answer path reads no file contents.** `brain.go` gathers a fixed set
  of facts — names, sizes, modification times, process names, an environment
  summary — and never the bytes of a file. `readFile` exists in `probe.go`
  but the answer path does not call it. The theft primitive is simply absent.
- **Secret-shaped environment values are redacted** (`probe.go`, `looksSecret`)
  before anything leaves the pod.
- **The model is kept out of the control loop** (`brain.go`): deterministic
  code gathers the facts *first*, and only then may a model interpret them.
  The model never chooses what to read or walk, so a prompt-injected model
  still cannot make the agent touch anything the deterministic pass did not
  already hold. The security property survives the model being wrong or
  subverted — which is the only kind of property worth having, because the
  model *will* eventually be wrong or subverted.
- **The image is `scratch`**: no shell, no second binary, nothing in it to
  turn into a foothold.

None of this relies on the agent being trusted, on detection firing, or on
any AI's judgement holding — including the judgement of whatever assistant
helped write it. That is the test a control must pass: it must still hold
when the smart thing in the loop is fooled.

## The invariant this repo must keep

The read-only property currently lives in one provider's `brain.go`. That is
where it is *implemented*, but it must not be the only place it is *enforced*
— a different provider binary discards it in one edit, and the whole point is
that the pattern is one edit from dangerous.

**A provider that can perform state-changing actions on behalf of a model
must not be reachable without an explicit, out-of-band authorization
recorded at the gateway.** Not a self-declaration in the binary; a fact souk
knows and a caller cannot conjure. This mirrors an invariant the repo already
keeps deliberately — the docent gives directions and *runs nothing*, enforced
at the gateway with a test asserting its tool list (CLAUDE.md, `docs/server-mode.md`).
The same shape applies here: dangerous verbs are gated at the boundary, not
left to each implementation's discipline.

Making this real is future work, and it has a natural home. Capability level
should be a claim a provider registers and souk records — and skills already
reach souk only through `agent_card_extra` (`repo.register_agents` builds the
card from name + description + `agent_card_extra` and drops everything else),
so that channel is the place to carry it. Enforcement cannot inspect a lying
binary — souk cannot see what Go a provider compiled — but it changes the
game regardless: read-only agents are plentiful and write-capable ones rare
and conspicuous; a write-capable agent appearing on the roster becomes a
*detectable event*; and the next person who wants to loosen tool use meets a
gate to walk through instead of a comment to delete.

## What to do with this, concretely

For a deployment:

- Arm the controls that ship off: `SOUK_PUBLIC_KEY` pinned, NetworkPolicy
  default-deny to the one souk, TLS on, CORS and roster access tightened.
  These are the difference between "correct but unarmed" and "armed."
- Treat the AI/model egress endpoint as a high-value one to watch: automated,
  regular call-out patterns from a host process are a signal even when the
  destination is reputable.

For anyone building a new provider:

- Default to read-only. Keep the model out of the control loop. Remove the
  capability rather than plan to detect its abuse. If a provider must write,
  it crosses the invariant above and needs the gateway authorization, not a
  README note.

## Sources

- OWASP GenAI Security Project — *LLM06:2025 Excessive Agency*.
- Cloud Security Alliance — *Agentic C2: AI Agents as Command-and-Control
  Infrastructure* (2026).
- *Hiding in the AI Traffic: Abusing MCP for LLM-Powered Agentic Red Teaming*,
  arXiv:2511.15998 (2025).
- *Forewarned is Forearmed: A Survey on Large Language Model-based Agents in
  Autonomous Cyberattacks*, arXiv:2505.12786 (2025).
