"""KYOK's one remaining HTTP route: `POST /kyok/v1/chat/completions`
(souk_server.api_llm_bridge) — the provider-facing, OpenAI-compatible
side, and every way its two-part authorization refuses a call. The
bridge's side of the relay is `WS /ws/kyok`; its round trips (including
what used to be probed over poll/respond) live in tests/test_ws_kyok.py.
See docs/keep-your-own-key.md for the full design.
"""

from __future__ import annotations

import hashlib
import json
import time

from souk import repo
from souk.kyok import issue_kyok_token


def _kyok_headers(bearer: str, private_key, body: bytes) -> dict:
    """Mirrors souk_agent_sdk.KyokSigningAuth.auth_flow exactly (see
    docs/keep-your-own-key.md's "Binding a token to the specific run and
    provider that hold it" section) — reimplemented here for the same
    reason conftest.py's Identity.register_body reimplements registration
    signing: this test suite doesn't depend on souk_agent_sdk as a package.
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


async def _register_agent(session, new_identity, name: str = "greeter"):
    identity = new_identity()
    agent_ids = await repo.register_agents(session, identity.public_key, [{"name": name}])
    return identity, agent_ids[name]


# --- souk/kyok.py: token issue/verify -----------------------------------


async def test_chat_completions_without_bearer_401s(client):
    resp = await client.post("/kyok/v1/chat/completions", content=b"{}")
    assert resp.status_code == 401


async def test_chat_completions_with_invalid_token_401s(client):
    resp = await client.post(
        "/kyok/v1/chat/completions", content=b"{}", headers={"Authorization": "Bearer not-a-real-token"}
    )
    assert resp.status_code == 401


async def test_chat_completions_run_not_in_broker_403s(client, session, souk, new_identity):
    identity, agent_id = await _register_agent(session, new_identity)
    token = issue_kyok_token("run_never_started", "sess_1", agent_id, "test-signing-secret")
    resp = await client.post(
        "/kyok/v1/chat/completions", content=b"{}", headers={"Authorization": f"Bearer {token}"}
    )
    assert resp.status_code == 403


async def test_chat_completions_agent_id_mismatch_403s(client, session, souk, new_identity):
    identity, agent_id = await _register_agent(session, new_identity)
    run_id = "run_mismatch"
    souk.broker.enqueue_run(run_id, agent_id, "thread_1", {}, "ag-ui")
    try:
        token = issue_kyok_token(run_id, "sess_1", "some_other_agent_id", "test-signing-secret")
        resp = await client.post(
            "/kyok/v1/chat/completions", content=b"{}", headers={"Authorization": f"Bearer {token}"}
        )
        assert resp.status_code == 403
    finally:
        souk.broker.forget(run_id)


async def test_chat_completions_cancelled_run_403s(client, session, souk, new_identity):
    identity, agent_id = await _register_agent(session, new_identity)
    run_id = "run_cancelled"
    run = souk.broker.enqueue_run(run_id, agent_id, "thread_1", {}, "ag-ui")
    run.cancel_requested = True
    try:
        token = issue_kyok_token(run_id, "sess_1", agent_id, "test-signing-secret")
        resp = await client.post(
            "/kyok/v1/chat/completions", content=b"{}", headers={"Authorization": f"Bearer {token}"}
        )
        assert resp.status_code == 403
    finally:
        souk.broker.forget(run_id)


async def test_chat_completions_missing_signature_headers_401s(client, session, souk, new_identity):
    identity, agent_id = await _register_agent(session, new_identity)
    run_id = "run_no_sig"
    souk.broker.enqueue_run(run_id, agent_id, "thread_1", {}, "ag-ui")
    try:
        token = issue_kyok_token(run_id, "sess_1", agent_id, "test-signing-secret")
        resp = await client.post(
            "/kyok/v1/chat/completions", content=b"{}", headers={"Authorization": f"Bearer {token}"}
        )
        assert resp.status_code == 401
    finally:
        souk.broker.forget(run_id)


async def test_chat_completions_stale_timestamp_401s(client, session, souk, new_identity):
    identity, agent_id = await _register_agent(session, new_identity)
    run_id = "run_stale"
    souk.broker.enqueue_run(run_id, agent_id, "thread_1", {}, "ag-ui")
    try:
        token = issue_kyok_token(run_id, "sess_1", agent_id, "test-signing-secret")
        body = b"{}"
        body_hash = hashlib.sha256(body).hexdigest()
        stale_timestamp = str(int(time.time()) - 3600)
        payload = f"{token}:{stale_timestamp}:{body_hash}".encode()
        signature = identity._key.sign(payload).hex()
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


async def test_chat_completions_malformed_timestamp_401s(client, session, souk, new_identity):
    identity, agent_id = await _register_agent(session, new_identity)
    run_id = "run_malformed_ts"
    souk.broker.enqueue_run(run_id, agent_id, "thread_1", {}, "ag-ui")
    try:
        token = issue_kyok_token(run_id, "sess_1", agent_id, "test-signing-secret")
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


async def test_chat_completions_unregistered_agent_403s(client, session, souk, new_identity):
    """The token names a real, live run — but the agent behind it was
    never actually registered (or has since been delisted), so there's
    no public_key on file to verify the call-time signature against.
    """
    identity, _real_agent_id = await _register_agent(session, new_identity)
    run_id = "run_unregistered"
    souk.broker.enqueue_run(run_id, "agent_does_not_exist", "thread_1", {}, "ag-ui")
    try:
        token = issue_kyok_token(run_id, "sess_1", "agent_does_not_exist", "test-signing-secret")
        body = b"{}"
        headers = _kyok_headers(token, identity._key, body)
        resp = await client.post("/kyok/v1/chat/completions", content=body, headers=headers)
        assert resp.status_code == 403
    finally:
        souk.broker.forget(run_id)


async def test_chat_completions_bad_signature_401s(client, session, souk, new_identity):
    identity, agent_id = await _register_agent(session, new_identity)
    run_id = "run_bad_sig"
    souk.broker.enqueue_run(run_id, agent_id, "thread_1", {}, "ag-ui")
    try:
        token = issue_kyok_token(run_id, "sess_1", agent_id, "test-signing-secret")
        body = b"{}"
        headers = _kyok_headers(token, identity._key, body)
        headers["X-Souk-Kyok-Signature"] = "00" * 64
        resp = await client.post("/kyok/v1/chat/completions", content=body, headers=headers)
        assert resp.status_code == 401
    finally:
        souk.broker.forget(run_id)


async def test_claim_timeout_returns_502(client, session, souk, new_identity, monkeypatch):
    import souk.protocols.kyok as kyok_protocol

    monkeypatch.setattr(kyok_protocol, "CLAIM_TIMEOUT_SECONDS", 0.05)
    identity, agent_id = await _register_agent(session, new_identity)
    run_id = "run_claim_timeout"
    souk.broker.enqueue_run(run_id, agent_id, "thread_1", {}, "ag-ui")
    try:
        token = issue_kyok_token(run_id, "sess_unclaimed", agent_id, "test-signing-secret")
        body = json.dumps({"messages": []}).encode()
        headers = {**_kyok_headers(token, identity._key, body), "content-type": "application/json"}
        resp = await client.post("/kyok/v1/chat/completions", content=body, headers=headers)
        assert resp.status_code == 502
    finally:
        souk.broker.forget(run_id)
