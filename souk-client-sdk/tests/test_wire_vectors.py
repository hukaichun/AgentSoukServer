"""The KYOK bridge's restatement of the handshake, against the published
bytes (docs/wire-vectors.json at the repo root) — the same drift guard
souk-agent-sdk carries, for the same reason: this package must not import
the gateway, so its copy of the payloads answers to the vector file
instead. The bridge only ever signs the provider side, so that is the
half it restates and the half checked here.
"""

from __future__ import annotations

import json
from pathlib import Path

from souk_client_sdk.kyok_bridge import HANDSHAKE_VERSION, provider_proof_payload

VECTORS = json.loads(
    (Path(__file__).parent.parent.parent / "docs" / "wire-vectors.json").read_text()
)


def test_this_side_builds_the_published_proof_payload():
    (proof,) = [v for v in VECTORS["vectors"] if v["kind"] == "provider-proof"]
    inputs = proof["inputs"]
    built = provider_proof_payload(
        inputs["provider_nonce"], inputs["souk_nonce"], inputs["hello_raw"]
    )
    assert built == proof["payload_utf8"].encode()


def test_the_published_version_is_the_spoken_one():
    assert VECTORS["handshake_version"] == HANDSHAKE_VERSION
