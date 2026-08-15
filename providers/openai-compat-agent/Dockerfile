# Build context is the repo root (only so souk-agent-sdk/ is reachable —
# openai-compat-agent is an independent project, not a workspace member).
# This image only ever copies souk-agent-sdk/ and
# providers/openai-compat-agent/ — nothing from souk_server/,
# souk-client-sdk/, or other providers/ ends up in it.
FROM python:3.12-slim

RUN pip install --no-cache-dir uv

WORKDIR /app
COPY souk-agent-sdk /app/souk-agent-sdk
COPY providers/openai-compat-agent /app/providers/openai-compat-agent

WORKDIR /app/souk-agent-sdk
RUN uv sync

WORKDIR /app/providers/openai-compat-agent
RUN uv sync

ENV OPENAI_COMPAT_CONFIG=/app/config.yaml

CMD ["uv", "run", "openai-compat-agent"]
