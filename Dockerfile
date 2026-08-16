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
COPY pyproject.toml uv.lock .python-version /app/
COPY souk_server /app/souk_server

RUN uv sync --group dev

EXPOSE 8000

CMD ["uv", "run", "souk-server"]
