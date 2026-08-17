"""KYOK's one remaining HTTP route: `POST /kyok/v1/chat/completions`
(souk_server.api_llm_bridge) — the provider-facing, OpenAI-compatible
side, and every way its two-part authorization refuses a call. The
bridge's side of the relay is `WS /ws/kyok`; its round trips live in
tests/test_ws_kyok.py. See docs/keep-your-own-key.md for the full design.

A KYOK token names `(provider_key, agent_name)` now rather than an
`agent_id`, and the signature it demands is checked against
`token.agent.provider_key` — the same key that registered the name. That
is a tightening, not a rename: an id was a value souk minted and handed
out, so it said only that whoever held it had once been told it, while the
pair names an identity that must produce a signature to be believed.
"""

from __future__ import annotations

import hashlib
import json
import time

from souk.kyok import issue_kyok_token
from souk.models import AgentRef


def _kyok_headers(bearer: str, private_key, body: bytes) -> dict:
    """Mirrors souk_agent_sdk.KyokSigningAuth.auth_flow exactly (see
    docs/keep-your-own-key.md's "Binding a token to the specific run and
    provider that hold it" section) — reimplemented here because that is
    the caller's half and this suite is testing the server's.
    """
    timestamp = str(int(time.time()))
    body_hash = hashlib.sha256(body).hexdigest()
    payload = f"{bearer}:{timestamp}:{body_hash}".encode()
    signature = private_key.sign(payload).hex()
    return {
        "Authorization": f"Bearer {bearer}",
        "X-Souk-Kyok-Timestamp": timestamp,
        "X-Souk-Kyok-Signature": signature,
    }


def _token(run_id: str, agent: AgentRef, session_id: str = "sess_1") -> str:
    return issue_kyok_token(run_id, session_id, agent, "test-signing-secret")


async def _live_run(souk, agent: AgentRef, run_id: str):
    """A run the broker is dispatching, without anybody serving it.

    Through `souk.enqueue_run` rather than `souk.broker.enqueue_run`: the
    broker's own entry point takes a handler map, and a run enqueued
    without one reaches its pipeline and finds nothing to dispatch to.
    """
    return souk.enqueue_run(run_id, agent, "thread_1", {}, "ag-ui")


async def test_chat_completions_without_bearer_401s(client):
    resp = await client.post("/kyok/v1/chat/completions", content=b"{}")
    assert resp.status_code == 401


async def test_chat_completions_with_invalid_token_401s(client):
    resp = await client.post(
        "/kyok/v1/chat/completions", content=b"{}", headers={"Authorization": "Bearer not-a-real-token"}
    )
    assert resp.status_code == 401


async def test_chat_completions_run_not_in_broker_403s(client, souk, register):
    served = await register("greeter")
    token = _token("run_never_started", served.ref())
    resp = await client.post(
        "/kyok/v1/chat/completions", content=b"{}", headers={"Authorization": f"Bearer {token}"}
    )
    assert resp.status_code == 403


async def test_chat_completions_agent_mismatch_403s(client, souk, register):
    """The token names a different agent than the run is for. Both halves
    of the pair are checked, so this covers the same-provider case too —
    a provider cannot spend one of its own agents' runs under another of
    its names."""
    served = await register("greeter", "translator")
    run_id = "run_mismatch"
    await _live_run(souk, served.ref("greeter"), run_id)
    try:
        token = _token(run_id, served.ref("translator"))
        resp = await client.post(
            "/kyok/v1/chat/completions", content=b"{}", headers={"Authorization": f"Bearer {token}"}
        )
        assert resp.status_code == 403
    finally:
        souk.broker.forget(run_id)


async def test_chat_completions_same_name_under_another_provider_403s(client, souk, register):
    """The name matches; the key does not. This is the case an `agent_id`
    could never state — two providers offering `greeter` were two ids and
    nothing said they were the same name — and the one the demo market has
    on purpose."""
    mine = await register("greeter")
    theirs = await register("greeter")
    run_id = "run_other_provider"
    await _live_run(souk, mine.ref(), run_id)
    try:
        token = _token(run_id, theirs.ref())
        resp = await client.post(
            "/kyok/v1/chat/completions", content=b"{}", headers={"Authorization": f"Bearer {token}"}
        )
        assert resp.status_code == 403
    finally:
        souk.broker.forget(run_id)


async def test_chat_completions_cancelled_run_403s(client, souk, register):
    served = await register("greeter")
    run_id = "run_cancelled"
    await _live_run(souk, served.ref(), run_id)
    # Reaching in deliberately. `souk.cancel_run` on an unclaimed run
    # records `cancelled` and forgets it, so the route would 403 on
    # "no such run" and this test would pass without ever exercising the
    # cancel check it is named for.
    souk.broker._runs[run_id].cancel_requested = True
    try:
        token = _token(run_id, served.ref())
        resp = await client.post(
            "/kyok/v1/chat/completions", content=b"{}", headers={"Authorization": f"Bearer {token}"}
        )
        assert resp.status_code == 403
    finally:
        souk.broker.forget(run_id)


async def test_chat_completions_missing_signature_headers_401s(client, souk, register):
    served = await register("greeter")
    run_id = "run_no_sig"
    await _live_run(souk, served.ref(), run_id)
    try:
        token = _token(run_id, served.ref())
        resp = await client.post(
            "/kyok/v1/chat/completions", content=b"{}", headers={"Authorization": f"Bearer {token}"}
        )
        assert resp.status_code == 401
    finally:
        souk.broker.forget(run_id)


async def test_chat_completions_stale_timestamp_401s(client, souk, register):
    served = await register("greeter")
    run_id = "run_stale"
    await _live_run(souk, served.ref(), run_id)
    try:
        token = _token(run_id, served.ref())
        body = b"{}"
        body_hash = hashlib.sha256(body).hexdigest()
        stale_timestamp = str(int(time.time()) - 3600)
        payload = f"{token}:{stale_timestamp}:{body_hash}".encode()
        signature = served.identity._key.sign(payload).hex()
        resp = await client.post(
            "/kyok/v1/chat/completions",
            content=body,
            headers={
                "Authorization": f"Bearer {token}",
                "X-Souk-Kyok-Timestamp": stale_timestamp,
                "X-Souk-Kyok-Signature": signature,
            },
        )
        assert resp.status_code == 401
    finally:
        souk.broker.forget(run_id)


async def test_chat_completions_malformed_timestamp_401s(client, souk, register):
    served = await register("greeter")
    run_id = "run_malformed_ts"
    await _live_run(souk, served.ref(), run_id)
    try:
        token = _token(run_id, served.ref())
        resp = await client.post(
            "/kyok/v1/chat/completions",
            content=b"{}",
            headers={
                "Authorization": f"Bearer {token}",
                "X-Souk-Kyok-Timestamp": "not-a-number",
                "X-Souk-Kyok-Signature": "deadbeef",
            },
        )
        assert resp.status_code == 401
    finally:
        souk.broker.forget(run_id)


async def test_chat_completions_unregistered_agent_403s(client, souk, register):
    """The token names a real, live run — but the agent behind it was
    never actually registered (or has since been delisted), so there's
    no provider key on file to verify the call-time signature against.
    """
    served = await register("greeter")
    ghost = AgentRef(provider_key=served.public_key, name="never-registered")
    run_id = "run_unregistered"
    await _live_run(souk, ghost, run_id)
    try:
        token = _token(run_id, ghost)
        body = b"{}"
        headers = _kyok_headers(token, served.identity._key, body)
        resp = await client.post("/kyok/v1/chat/completions", content=body, headers=headers)
        assert resp.status_code == 403
    finally:
        souk.broker.forget(run_id)


async def test_chat_completions_bad_signature_401s(client, souk, register):
    served = await register("greeter")
    run_id = "run_bad_sig"
    await _live_run(souk, served.ref(), run_id)
    try:
        token = _token(run_id, served.ref())
        body = b"{}"
        headers = _kyok_headers(token, served.identity._key, body)
        headers["X-Souk-Kyok-Signature"] = "00" * 64
        resp = await client.post("/kyok/v1/chat/completions", content=body, headers=headers)
        assert resp.status_code == 401
    finally:
        souk.broker.forget(run_id)


async def test_a_signature_from_another_identity_401s(client, souk, register, new_identity):
    """Holding the token is not enough — that is the whole point of the
    second part. The signature must come from the key that registered the
    name the token carries."""
    served = await register("greeter")
    imposter = new_identity()
    run_id = "run_wrong_key"
    await _live_run(souk, served.ref(), run_id)
    try:
        token = _token(run_id, served.ref())
        body = b"{}"
        headers = _kyok_headers(token, imposter._key, body)
        resp = await client.post("/kyok/v1/chat/completions", content=body, headers=headers)
        assert resp.status_code == 401
    finally:
        souk.broker.forget(run_id)


async def test_claim_timeout_returns_502(client, souk, register, monkeypatch):
    import souk.protocols.kyok as kyok_protocol

    monkeypatch.setattr(kyok_protocol, "CLAIM_TIMEOUT_SECONDS", 0.05)
    served = await register("greeter")
    run_id = "run_claim_timeout"
    await _live_run(souk, served.ref(), run_id)
    try:
        token = _token(run_id, served.ref(), session_id="sess_unclaimed")
        body = json.dumps({"messages": []}).encode()
        headers = {**_kyok_headers(token, served.identity._key, body), "content-type": "application/json"}
        resp = await client.post("/kyok/v1/chat/completions", content=body, headers=headers)
        assert resp.status_code == 502
    finally:
        souk.broker.forget(run_id)
