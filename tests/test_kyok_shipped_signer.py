"""The object a real provider actually attaches to its model client, against
the real app.

Every other KYOK test in this repo and in core writes the signing payload out
by hand, and that is deliberate — a shared helper would agree with itself, so
a change to core's payload has to show up as two independent statements
disagreeing. What that independence cannot catch is `KyokSigningAuth` drifting
from *both* of them: it is the one implementation nobody's hand-written copy
stands in for, and it is the one every provider ships.

Which had already happened. Core added an operation prefix to the call
payload, and this file is what would have gone red on the same commit rather
than a provider getting a 401.
"""

from __future__ import annotations

import json

import httpx
from souk.kyok import issue_kyok_token
from souk_agent_sdk.kyok_auth import KyokSigningAuth

from souk_server.server import create_app


async def test_the_signer_a_provider_ships_is_accepted_by_this_gateway(souk, register):
    """No hand-written headers anywhere in this test. The token comes from
    core, the signature from the SDK, and the verification from the gateway —
    three separate statements of the same payload, meeting for the first
    time."""
    served = await register("greeter")
    run_id = "run_shipped_signer"
    souk.enqueue_run(run_id, served.ref(), "thread_1", {}, "ag-ui")
    token = issue_kyok_token(run_id, served.ref(), "test-signing-secret")
    try:
        transport = httpx.ASGITransport(app=create_app(souk))
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.post(
                "/kyok/v1/chat/completions",
                content=json.dumps({"messages": []}).encode(),
                headers={"Authorization": f"Bearer {token}", "content-type": "application/json"},
                auth=KyokSigningAuth(served.identity._key),
            )

        # 503 is the *pass*: authorization succeeded, and the call moved on
        # to resolving a KYOK binding this test never made. A signature the
        # gateway rejected would be 401, which is the failure being guarded.
        assert resp.status_code == 503, resp.text
    finally:
        souk.broker.forget(run_id)
