# Build context is this repo's root, with the AgentSouk submodule checked
# out (git clone --recurse-submodules) — souk core and scripts/ come from
# it. The image mirrors the repo layout so the `AgentSouk/…` paths in
# [tool.uv.sources] resolve unchanged: every entry there needs a COPY
# below, and a missing one fails at `uv sync` with "Distribution not found
# at", not at import time.
FROM python:3.12-slim

RUN pip install --no-cache-dir uv

WORKDIR /app
COPY AgentSouk/scripts /app/AgentSouk/scripts
COPY AgentSouk/souk /app/AgentSouk/souk
# The provider contract souk states and the gateway's socket implements —
# `ws_provider.SocketProvider` subclasses its `SoukConnection`.
COPY AgentSouk/souk-provider-sdk /app/AgentSouk/souk-provider-sdk
# The LLM-provider contract behind /ws/kyok — `ws_kyok.SocketLLMProvider`
# is checked against its `CONNECTED_LLM_PROVIDER_ATTRS`.
COPY AgentSouk/souk-llm-provider-sdk /app/AgentSouk/souk-llm-provider-sdk
COPY pyproject.toml uv.lock .python-version /app/
# Dev-group only, and only for `tests/test_kyok_shipped_signer.py` — but
# `uv sync --group dev` below resolves the whole group, so a path source
# without a COPY fails the *build*, not a test. (Which it did: the rule in
# CLAUDE.md exists because both images were once missing
# souk-provider-sdk, and this is the same mistake with a different line.)
COPY souk-agent-sdk /app/souk-agent-sdk
COPY souk_server /app/souk_server

RUN uv sync --group dev

EXPOSE 8000

CMD ["uv", "run", "souk-server"]
