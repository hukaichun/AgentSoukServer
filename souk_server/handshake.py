"""What each side signs to open a provider socket.

The gateway's, not core's: proving identity as a connection opens is a
serving act, so souk supplies only the primitives (`Souk.sign`,
`ProviderIdentity.sign`, `verify_signature`) and the strings are here.

**What this replaced, and why.** A provider used to open a socket by
signing a statement it composed itself —
`souk-provider-connect:{key}:{names}:{timestamp}` — in which the verifier
chose nothing. No nonce, no server identity, no binding to the connection:
only a timestamp, checked against a 60-second freshness window. So anyone
who observed that signature could replay it and attach as that provider,
and receive that provider's runs.

The old docstring reasoned carefully about the neighbouring case — that
reusing the *registration* payload would let a captured registration be
replayed as a connection, which is why the prefixes differ — and stopped
one step short. A prefix stops a signature being replayed as a different
kind of thing. It does nothing about a connection signature being replayed
as a connection, which needs no change of use at all.

Who could observe it: anything terminating TLS (a corporate proxy, a load
balancer), a log that captured frames, and — the sharp one — any souk a
provider could be induced to connect to, since the provider offered this
credential to whatever answered the URL.

    provider → hello      { version, publicKey, agentNames,
                            maxConcurrentRuns, nonce }
    souk     → challenge  { soukPublicKey, nonce, signature }
    provider → proof      { signature }
    souk     → welcome    { }

Both nonces appear in both signatures, so each side contributes freshness
and a recorded exchange is worth nothing. The `souk:`/`provider:` prefixes
mean neither signature can be presented as the other. And `sha256(hello)`
binds the claims: `agentNames` and `maxConcurrentRuns` cannot be altered
in flight. A digest rather than a re-send, because `hello` goes out before
`nonce_s` exists — and a digest of *the bytes actually sent*, not of a
re-serialization, since two JSON encoders can agree on a value and differ
on its encoding.

**Channel binding is out, decided rather than overlooked.** It is the
standard answer to a relay — sign something derived from the TLS session,
which a relay cannot reproduce — and it is unusable here. A Zscaler-class
proxy terminates and re-originates TLS by design, so the two sides never
derive the same value and the check fails every time. Enforcing it would
not harden the deployment; it would lock out every enterprise running one,
which is the deployment this exists for.

It was also never what fixes the defect. Challenge-response closes the
stealable-credential hole *with* an intercepting proxy in the path: the
proxy still sees the traffic, and still cannot answer a fresh nonce
without the private key. What stays open is tampering on a live
connection, deliberately — run inputs and events are not individually
signed, and an intercepting proxy is in the trust model by construction,
since the enterprise installed it and pushed its CA. See
AgentSoukServer#10.
"""

from __future__ import annotations

import hashlib
import secrets

# The handshake version a provider must declare. Bumped when the frames or
# the signed bytes change, so a mismatch is refused by name instead of
# failing as a bad signature — which is the same symptom as an attack and
# would send whoever is debugging it somewhere unhelpful.
#
# There is no compatibility branch for version 1's predecessor. The old
# shape had no version field at all, so it cannot be accepted and told
# apart from a corrupt frame; and every provider that exists is in this
# repo, behind one SDK. Its absence is what the error message names.
HANDSHAKE_VERSION = 1

# 32 bytes. Long enough that a nonce never repeats by accident, which is
# the only property required of it — it is never secret and never stored.
NONCE_BYTES = 32


def new_nonce() -> str:
    return secrets.token_hex(NONCE_BYTES)


def souk_challenge_payload(provider_nonce: str, souk_nonce: str) -> bytes:
    """What souk signs, so a provider can tell this souk from another.

    Everything else on this wire is one-directional: a provider proves who
    it is and souk proves nothing, so a provider connected to a URL and
    trusted whatever answered. This is the other half.
    """
    return f"souk-auth:souk:{provider_nonce}:{souk_nonce}".encode()


def provider_proof_payload(provider_nonce: str, souk_nonce: str, hello_raw: str) -> bytes:
    """What a provider signs to prove it holds its key, *now*.

    `hello_raw` is the exact text of the hello frame as it went on the
    wire — hashed rather than re-serialized, and hashed rather than
    re-sent, for the reasons in the module docstring.
    """
    hello_digest = hashlib.sha256(hello_raw.encode()).hexdigest()
    return f"souk-auth:provider:{provider_nonce}:{souk_nonce}:{hello_digest}".encode()
