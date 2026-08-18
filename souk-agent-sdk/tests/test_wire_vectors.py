"""This package's restatement of the handshake, against the published
bytes (docs/wire-vectors.json at the repo root).

The restatement is deliberate — this package must not import the gateway
— and this test is what makes it safe: both sides now agree with one
published vector file instead of only with each other on the first
connection.
"""

from __future__ import annotations

import json
from pathlib import Path

from souk_agent_sdk.client import (
    HANDSHAKE_VERSION,
    provider_proof_payload,
    souk_challenge_payload,
)

VECTORS = json.loads(
    (Path(__file__).parent.parent.parent / "docs" / "wire-vectors.json").read_text()
)


def test_this_side_builds_the_published_payloads():
    for vector in VECTORS["vectors"]:
        inputs = vector["inputs"]
        if vector["kind"] == "souk-challenge":
            built = souk_challenge_payload(inputs["provider_nonce"], inputs["souk_nonce"])
        elif vector["kind"] == "provider-proof":
            built = provider_proof_payload(
                inputs["provider_nonce"], inputs["souk_nonce"], inputs["hello_raw"]
            )
        else:
            raise AssertionError(f"unknown vector kind {vector['kind']!r}")
        assert built == vector["payload_utf8"].encode(), vector["kind"]


def test_the_published_version_is_the_spoken_one():
    assert VECTORS["handshake_version"] == HANDSHAKE_VERSION
