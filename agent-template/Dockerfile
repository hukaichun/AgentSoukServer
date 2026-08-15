# Build context is the repo root (only so souk-agent-sdk/ is reachable —
# agent-template is an independent project, not a workspace member). This
# image only ever copies souk-agent-sdk/ and agent-template/ — nothing
# from souk_server/, souk-client-sdk/, or providers/ ends up in it.
FROM python:3.12-slim

RUN pip install --no-cache-dir uv

WORKDIR /app
COPY souk-agent-sdk /app/souk-agent-sdk
COPY agent-template /app/agent-template

WORKDIR /app/souk-agent-sdk
RUN uv sync

WORKDIR /app/agent-template
RUN uv sync

CMD ["uv", "run", "agent-template"]
