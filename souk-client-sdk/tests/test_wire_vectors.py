"""The KYOK bridge against the published wire statement
(docs/wire-vectors.json at the repo root).

Like souk-agent-sdk's twin of this file, the payload half is gone: v2
signs upstream's link-open family via `ProviderIdentity.sign_connect`,
so the bridge restates no bytes and the vectors for them live upstream.
The handshake version is what remains local and checked.
"""

from __future__ import annotations

import json
from pathlib import Path

from souk_client_sdk.kyok_bridge import HANDSHAKE_VERSION

VECTORS = json.loads(
    (Path(__file__).parent.parent.parent / "docs" / "wire-vectors.json").read_text()
)


def test_the_published_version_is_the_spoken_one():
    assert VECTORS["handshake_version"] == HANDSHAKE_VERSION
