#!/usr/bin/env python3
"""Servidor A2A v1.0 (JSON-RPC sobre HTTP) do Agente da Central de Salas.

Publica o Agent Card em `/.well-known/agent-card.json` e atende `SendMessage`
e `GetTask` em `/a2a` (porta padrao `7300`), consumindo por dentro o servidor
MCP em `http://localhost:7301/mcp`.
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import socket
import sys
from collections.abc import AsyncIterator
from pathlib import Path

# Garante que os modulos locais (`mcp_host`, `ponte`) sejam importaveis
# independentemente do diretorio de trabalho a partir do qual o script foi chamado.
DIRETORIO_AGENTE = Path(__file__).resolve().parent
if str(DIRETORIO_AGENTE) not in sys.path:
    sys.path.insert(0, str(DIRETORIO_AGENTE))

import uvicorn
from a2a.server.context import ServerCallContext
from a2a.server.request_handlers import DefaultRequestHandler
from a2a.server.routes import (
    DefaultServerCallContextBuilder,
    create_agent_card_routes,
    create_jsonrpc_routes,
)
from a2a.server.tasks import InMemoryTaskStore
from a2a.types import (
    AgentCapabilities,
    AgentCard,
    AgentInterface,
    AgentProvider,
    AgentSkill,
)
from starlette.applications import Starlette
from starlette.requests import Request

from mcp_host import HostMCPCentralDeSalas
from ponte import ExecutorPonteSalas


class ContextBuilderA2AV1(DefaultServerCallContextBuilder):
    """Assume A2A 1.0 quando o cliente nao envia o header opcional `A2A-Version`."""

    def build(self, request: Request) -> ServerCallContext:
        ctx = super().build(request)
        headers = ctx.state.setdefault("headers", {})
        if "a2a-version" not in headers and "A2A-Version" not in headers:
            headers["a2a-version"] = "1.0"
        return ctx


def construir_agent_card(url_endpoint_a2a: str) -> AgentCard:
    return AgentCard(
        name="Central de Salas",
        description="Reserva salas de reuniao da Hill Valley Tech.",
        provider=AgentProvider(
            organization="Hill Valley Tech",
            url="https://hillvalley.example",
        ),
        version="1.0.0",
        supported_interfaces=[
            AgentInterface(
                url=url_endpoint_a2a,
                protocol_binding="JSONRPC",
                protocol_version="1.0",
            )
        ],
        capabilities=AgentCapabilities(
            streaming=False,
            push_notifications=False,
            extended_agent_card=False,
        ),
        default_input_modes=["text/plain"],
        default_output_modes=["text/plain"],
        skills=[
            AgentSkill(
                id="reservar-sala",
                name="Reservar sala",
                description="Reserva uma sala em um intervalo. Se houver conflito, pergunta qual alternativa usar.",
                tags=["salas", "agenda"],
                input_modes=["text/plain"],
                output_modes=["text/plain"],
                examples=[
                    "reservar sala=sala-garagem inicio=2026-11-03T14:00:00-03:00 fim=2026-11-03T15:00:00-03:00 responsavel=Marty"
                ],
            )
        ],
    )


def construir_app_a2a(
    *,
    mcp_url: str,
    url_publica_base: str,
    caminho_a2a: str = "/a2a",
) -> Starlette:
    caminho_normalizado = "/" + caminho_a2a.strip("/")
    url_endpoint_a2a = url_publica_base.rstrip("/") + caminho_normalizado

    mcp_host = HostMCPCentralDeSalas(mcp_url=mcp_url)
    executor = ExecutorPonteSalas(mcp_host=mcp_host)
    task_store = InMemoryTaskStore()
    agent_card = construir_agent_card(url_endpoint_a2a=url_endpoint_a2a)
    request_handler = DefaultRequestHandler(
        agent_executor=executor,
        task_store=task_store,
        agent_card=agent_card,
    )

    @contextlib.asynccontextmanager
    async def lifespan(_app: Starlette) -> AsyncIterator[None]:
        await mcp_host.iniciar()
        try:
            yield
        finally:
            await request_handler.aclose()
            await mcp_host.encerrar()

    rotas = [
        *create_agent_card_routes(agent_card=agent_card),
        *create_jsonrpc_routes(
            request_handler=request_handler,
            rpc_url=caminho_normalizado,
            context_builder=ContextBuilderA2AV1(),
        ),
    ]
    return Starlette(routes=rotas, lifespan=lifespan)


def criar_socket_dual_stack(porta: int) -> socket.socket | None:
    """Cria socket dual-stack (IPv4 + IPv6) para atender tanto 127.0.0.1 quanto ::1 sem atraso."""
    try:
        sock = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, 0)
        sock.bind(("::", porta))
        sock.setblocking(False)
        return sock
    except OSError:
        return None


def main() -> None:
    host_env = os.environ.get("AGENTE_HOST")
    host = host_env or "0.0.0.0"
    porta = int(os.environ.get("AGENTE_PORT", os.environ.get("PORT", "7300")))
    caminho_a2a = os.environ.get("AGENTE_PATH", "/a2a")
    url_publica_base = os.environ.get("AGENTE_URL", f"http://localhost:{porta}")
    mcp_url = os.environ.get("MCP_URL", "http://localhost:7301/mcp")

    app = construir_app_a2a(
        mcp_url=mcp_url,
        url_publica_base=url_publica_base,
        caminho_a2a=caminho_a2a,
    )
    print(
        f"[agente] Central de Salas A2A iniciando em {url_publica_base.rstrip('/')}{caminho_a2a} (MCP: {mcp_url})",
        file=sys.stderr,
        flush=True,
    )
    sock = criar_socket_dual_stack(porta) if host_env is None else None
    if sock is not None:
        config = uvicorn.Config(app, host="::", port=porta, log_level="warning")
        asyncio.run(uvicorn.Server(config).serve(sockets=[sock]))
    else:
        uvicorn.run(app, host=host, port=porta, log_level="warning")


if __name__ == "__main__":
    main()

