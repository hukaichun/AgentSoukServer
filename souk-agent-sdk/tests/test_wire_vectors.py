"""This package against the published wire statement (docs/wire-vectors.json
at the repo root).

The payload restatement this test used to guard is gone — v2 signs
upstream's link-open family, imported from souk_provider_sdk rather than
copied, so the byte-level vectors live upstream and this side has nothing
of its own to drift. What remains local, and checked here: the handshake
version this client speaks.
"""

from __future__ import annotations

import json
from pathlib import Path

from souk_agent_sdk.client import HANDSHAKE_VERSION

VECTORS = json.loads(
    (Path(__file__).parent.parent.parent / "docs" / "wire-vectors.json").read_text()
)


def test_the_published_version_is_the_spoken_one():
    assert VECTORS["handshake_version"] == HANDSHAKE_VERSION
