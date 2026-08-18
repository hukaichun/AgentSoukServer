"""The reference gateway: assembles a Souk into one HTTP server.

This is the serving layer. It is the only place that binds a port, applies
CORS, or terminates TLS — every such decision belongs to whoever hosts souk,
not to souk itself, which is why `create_app` hands back a plain ASGI app and
`main` is a thin wrapper that happens to serve it. One listener carries
everything (docs/server-mode.md): callers over HTTP+SSE, providers over
WS /ws/provider, LLM providers over WS /ws/kyok.
"""

import asyncio
import logging
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from souk_server import api_a2a, api_agui, api_health, api_llm_bridge, api_registry, ws_kyok, ws_provider
from souk.config import CoreSettings
from souk_server.config import ServingSettings
from souk.core import Souk
from souk_server.deps import install_error_handlers
from souk_server.mcp_docent import create_docent, transport_security_for

logger = logging.getLogger("souk_server")
logging.basicConfig(level=logging.INFO)


def create_app(souk: Souk, serving: ServingSettings | None = None) -> FastAPI:
    """Builds the ASGI app for `souk`. Does not bind anything — the caller
    decides how (or whether) it reaches a network, and is free to wrap it in
    their own middleware or mount it inside a larger app.
    """
    serving = serving or ServingSettings()

    # The docent: MCP discovery over the same listener (docs/server-mode.md).
    # Stateless because it holds nothing per visitor — every answer is a
    # fresh query against the roster, so there is no session worth pinning to
    # one process, and a second replica can answer just as well.
    docent = create_docent(souk, serving.public_http_url)
    docent_app = docent.streamable_http_app(
        streamable_http_path="/",
        stateless_http=True,
        # Which Host headers this answers to — see ServingSettings.
        # Without it the SDK's localhost-only default 421s every caller
        # that reaches souk by any other name, which is every caller in
        # a compose network or behind a proxy.
        transport_security=transport_security_for(serving.mcp_allowed_hosts),
    )

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        # Bringing souk up is souk's own business — reconciling what the last
        # process left behind and keeping the health sweeps running (see
        # Souk.start). This layer only decides *when*, and may call it after
        # _serve already has: start() runs once.
        #
        # The schema is not part of it: `alembic upgrade head` (see
        # souk/alembic/) is a deploy-time step with DDL-capable credentials,
        # separate from starting the server, which only ever runs DML against
        # a possibly DML-only role.
        await souk.start()
        # The MCP session manager owns a task group; without this its
        # requests fail rather than degrade, which is why it is entered here
        # rather than lazily on first call.
        async with docent.session_manager.run():
            yield
        # Deliberately no aclose: this app was handed a Souk it does not own
        # (see create_app's docstring — it may be mounted inside a larger
        # app), and closing someone else's would take their background work
        # and their connection pool with it. Whoever constructed it closes
        # it; _serve below does exactly that for the one it constructs.

    app = FastAPI(title="souk", lifespan=lifespan)
    # Read back by souk.deps' dependencies, so the routers hold no
    # module-level state and two apps can serve two different souks.
    app.state.souk = souk
    app.state.serving_settings = serving
    app.add_middleware(
        CORSMiddleware,
        allow_origins=serving.cors_allow_origins,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    install_error_handlers(app)
    app.include_router(api_health.router)
    app.include_router(api_registry.router)
    app.include_router(api_agui.router)
    app.include_router(api_a2a.router)
    app.include_router(api_llm_bridge.router)
    app.include_router(ws_provider.router)
    app.include_router(ws_kyok.router)
    # Mounted rather than routed: the MCP transport is its own ASGI app, and
    # this is the one surface souk does not frame itself.
    app.mount("/mcp", docent_app)
    return app


async def _serve() -> None:
    souk = Souk(CoreSettings())
    serving = ServingSettings()
    app = create_app(souk, serving)

    if not (serving.http_tls_cert_path and serving.http_tls_key_path):
        logger.warning(
            "HTTP server listening on %s:%s WITHOUT TLS — fine for same-host development, "
            "never for a souk reachable over a real network (see souk.config's http_tls_* settings)",
            serving.http_host,
            serving.http_port,
        )
    config = uvicorn.Config(
        app,
        host=serving.http_host,
        port=serving.http_port,
        log_level="info",
        ssl_certfile=serving.http_tls_cert_path,
        ssl_keyfile=serving.http_tls_key_path,
    )
    http_server = uvicorn.Server(config)

    try:
        # uvicorn's serve() runs the app's ASGI lifespan, which brings souk
        # up (reconciliation, health sweeps) before the listener accepts
        # anything — no work can arrive on any surface before it has run,
        # now that every surface lives on this one listener.
        await http_server.serve()
    finally:
        await souk.aclose()


def main() -> None:
    asyncio.run(_serve())


if __name__ == "__main__":
    main()
