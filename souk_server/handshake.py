"""What each side signs to open a provider or LLM-provider socket.

The payloads are upstream's now — `souk_provider_sdk.identity` publishes
the link-open family (`souk-connect-provider:{souk_public_key}:
{souk_nonce}:{provider_nonce}:{sorted names}` and
`souk-connect-souk:{souk_nonce}:{provider_nonce}`), vectored in
AgentSouk/docs/contract-vectors.json — and this module only
re-exports them beside the version number and the frame choreography,
which remain serving decisions. Three packages used to restate these
bytes because none could import another (the gateway is AGPL, the SDKs
Apache); the SDK was the shared home all three could already import, and
upstream putting the family there is what dissolved the triplication.

**What v2 changed, and what it deliberately gave up.** Version 1's proof
signed `sha256(hello_raw)` — the exact bytes of the hello frame — binding
every claim in it, `maxConcurrentRuns` included. The connect family binds
the sorted names instead, and nothing else. That loses the binding on
`maxConcurrentRuns`, and we take the trade knowingly: the names are the
authorization-relevant claim (which agents or offerings this key attaches
for), while tampering with a live connection's other fields was already
outside the threat model — an intercepting proxy is trusted by
construction here (see version 1's notes, kept in git history, and
AgentSoukServer#10). In exchange, all three implementations drop the
digest-of-the-bytes-actually-sent subtlety, which was the easiest thing
on this wire to get wrong.

The choreography is unchanged — four frames, both sides signing bytes the
other chose:

    provider → hello      { version, publicKey, agentNames|modelNames,
                            maxConcurrentRuns?, nonce }
    souk     → challenge  { soukPublicKey, nonce, signature }
    provider → proof      { signature }
    souk     → welcome    { }

souk still signs first: a provider must be able to walk away from a souk
it does not recognise before producing anything worth stealing. A souk
with no identity configured still says so honestly (`soukPublicKey:
null`) rather than failing.
"""

from __future__ import annotations

# Re-exported so the sockets keep one import site for handshake material.
# These are upstream's statements; the gateway adds nothing to them.
from souk_provider_sdk import (  # noqa: F401
    new_nonce,
    provider_connect_payload,
    souk_connect_payload,
)

# The handshake version a provider must declare. Bumped when the frames or
# the signed bytes change, so a mismatch is refused by name instead of
# failing as a bad signature — which is the same symptom as an attack and
# would send whoever is debugging it somewhere unhelpful.
#
# v2: the signed payloads moved from this gateway's souk-auth family
# (hello-digest binding) to upstream's souk-connect family (sorted-names
# binding). v1 partners fail here by name, not by signature.
#
# v3: the provider's proof binds the recipient souk's public key — the
# first field of `souk-connect-provider:` is the key from the challenge
# frame (empty string for a souk with no identity) — so a proof coaxed
# out by one souk cannot be relayed to attach at another. Upstream made
# the proof unconditional in the same stroke: core's attach refuses a
# proofless connection outright, with no setting to say otherwise.
HANDSHAKE_VERSION = 3
