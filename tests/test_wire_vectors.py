"""docs/wire-vectors.json, consumed by the side that authors it.

Upstream's contract-vectors.json publishes the payloads souk core
verifies; this file publishes the payloads *this gateway* authors — the
mutual handshake — plus each socket's inbound frame vocabulary. Three
suites consume it (this one, souk-agent-sdk's, souk-client-sdk's), which
is what keeps the three deliberate restatements of the handshake from
drifting: they no longer only agree with themselves, they all agree with
one published set of bytes an external implementation can replay.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from souk.identity import verify_signature
from souk_server import ws_kyok, ws_provider
from souk_server.handshake import (
    HANDSHAKE_VERSION,
    provider_proof_payload,
    souk_challenge_payload,
)

VECTORS = json.loads((Path(__file__).parent.parent / "docs" / "wire-vectors.json").read_text())


def _key(name: str) -> Ed25519PrivateKey:
    key = Ed25519PrivateKey.from_private_bytes(
        bytes.fromhex(VECTORS[name]["private_key_hex"])
    )
    assert (
        key.public_key().public_bytes_raw().hex() == VECTORS[name]["public_key_hex"]
    ), name
    return key


BUILDERS = {
    "souk-challenge": lambda i: souk_challenge_payload(i["provider_nonce"], i["souk_nonce"]),
    "provider-proof": lambda i: provider_proof_payload(
        i["provider_nonce"], i["souk_nonce"], i["hello_raw"]
    ),
}


def test_this_side_reproduces_every_vector():
    keys = {name: _key(name) for name in ("provider_test_key", "souk_test_key")}
    covered = 0
    for vector in VECTORS["vectors"]:
        builder = BUILDERS.get(vector["kind"])
        assert builder is not None, vector["kind"]
        covered += 1
        payload = builder(vector["inputs"])
        assert payload == vector["payload_utf8"].encode(), vector["kind"]
        key = keys[vector["signed_by"]]
        assert key.sign(payload).hex() == vector["signature_hex"], vector["kind"]
        assert verify_signature(
            VECTORS[vector["signed_by"]]["public_key_hex"],
            vector["signature_hex"],
            payload,
        )
    assert covered == len(BUILDERS)


def test_the_hello_digest_is_over_the_published_bytes():
    (proof,) = [v for v in VECTORS["vectors"] if v["kind"] == "provider-proof"]
    assert (
        hashlib.sha256(proof["inputs"]["hello_raw"].encode()).hexdigest()
        == proof["inputs"]["hello_sha256_hex"]
    )


def test_the_published_version_is_the_spoken_one():
    assert VECTORS["handshake_version"] == HANDSHAKE_VERSION


def test_the_frame_vocabulary_is_the_dispatched_one():
    assert set(VECTORS["frames"]["provider_socket_inbound"]) == ws_provider.INBOUND_FRAME_TYPES
    assert set(VECTORS["frames"]["kyok_socket_inbound"]) == ws_kyok.INBOUND_FRAME_TYPES
