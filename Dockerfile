# Build context is this repo's root, with the AgentSouk submodule checked
# out (git clone --recurse-submodules) — souk core, proto/ and scripts/
# all come from it. The image mirrors the repo layout so the
# `AgentSouk/souk` path in [tool.uv.sources] resolves unchanged. Nothing
# from the submodule's other subprojects (souk-agent-sdk, souk-client-sdk,
# agent-template, providers/) ends up in it.
FROM python:3.12-slim

RUN pip install --no-cache-dir uv

WORKDIR /app
COPY AgentSouk/proto /app/AgentSouk/proto
COPY AgentSouk/scripts /app/AgentSouk/scripts
COPY AgentSouk/souk /app/AgentSouk/souk
COPY pyproject.toml uv.lock .python-version /app/
COPY souk_server /app/souk_server

RUN uv sync --group dev \
    && uv run bash AgentSouk/scripts/gen_proto.sh souk_server/grpc_gen

EXPOSE 8000 50051

CMD ["uv", "run", "souk-server"]
