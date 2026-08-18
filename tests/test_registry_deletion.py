"""DELETE /agents — the deletion half of the agent roster over HTTP,
added alongside the offering deletion for roster symmetry (the KYOK
side's test lives with its socket in test_ws_kyok.py).

Core owns every refusal rule (`Souk.delete_agent`); this suite drives the
route: the signed order works, the wrong signature is a 401 not a 404
(don't confirm existence to somebody who can't sign for it), and in-use
refusals surface as 409 through the app-wide handler.
"""

from __future__ import annotations


def _order(served, name: str | None = None) -> dict:
    name = name or served.names[0]
    signature, timestamp = served.identity.sign_deletion(name)
    return {
        "name": name,
        "public_key": served.public_key,
        "signature": signature,
        "timestamp": timestamp,
    }


async def test_a_signed_deletion_removes_the_agent(client, register):
    served = await register("retiree")
    resp = await client.request("DELETE", "/agents", json=_order(served))
    assert resp.status_code == 204
    roster = (await client.get("/agents")).json()["agents"]
    assert roster == []
    # Gone means gone.
    assert (await client.request("DELETE", "/agents", json=_order(served))).status_code == 404


async def test_a_served_agent_refuses_deletion(client, serve):
    served = await serve(None, "busy")
    resp = await client.request("DELETE", "/agents", json=_order(served))
    assert resp.status_code == 409
    assert len((await client.get("/agents")).json()["agents"]) == 1


async def test_a_registration_signature_is_not_a_deletion_order(client, register):
    served = await register("safe")
    signature, timestamp = served.identity.sign_registration(["safe"])
    resp = await client.request(
        "DELETE",
        "/agents",
        json={
            "name": "safe",
            "public_key": served.public_key,
            "signature": signature,
            "timestamp": timestamp,
        },
    )
    assert resp.status_code == 401
