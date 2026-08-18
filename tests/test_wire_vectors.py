"""docs/wire-vectors.json, consumed by the side that authors it.

Slimmer than it was, on purpose: the signed payloads this file used to
vector (the souk-auth handshake family) were replaced in v2 by upstream's
link-open family, vectored in AgentSouk/docs/contract-vectors.json — the
authority followed the bytes. What stays this repo's to publish and pin:
the handshake version, and each socket's frame vocabulary. The other two
consumers (souk-agent-sdk, souk-client-sdk) check the same file, so the
three implementations of the choreography still answer to one statement.
"""

from __future__ import annotations

import json
from pathlib import Path

import souk_provider_sdk

from souk_server import handshake, ws_kyok, ws_provider

VECTORS = json.loads((Path(__file__).parent.parent / "docs" / "wire-vectors.json").read_text())


def test_the_published_version_is_the_spoken_one():
    assert VECTORS["handshake_version"] == handshake.HANDSHAKE_VERSION


def test_the_handshake_payloads_are_upstreams_not_restatements():
    """v2's whole point: the gateway signs and verifies exactly the bytes
    souk_provider_sdk states, re-exported rather than copied — so the
    vectors for them live upstream and cannot drift from this side."""
    assert handshake.provider_connect_payload is souk_provider_sdk.provider_connect_payload
    assert handshake.souk_connect_payload is souk_provider_sdk.souk_connect_payload


def test_the_frame_vocabulary_is_the_dispatched_one():
    assert set(VECTORS["frames"]["provider_socket_inbound"]) == ws_provider.INBOUND_FRAME_TYPES
    assert set(VECTORS["frames"]["kyok_socket_inbound"]) == ws_kyok.INBOUND_FRAME_TYPES
