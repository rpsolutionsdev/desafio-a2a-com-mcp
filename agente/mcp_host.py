"""Cliente MCP (papel de Host MCP) do Agente da Central de Salas.

Conecta-se ao servidor MCP por Streamable HTTP na revisao 2026-07-28 da spec,
declara exclusivamente a capability `{"elicitation": {"form": {}}}`, descobre as
ferramentas via `tools/list` antes do primeiro `tools/call`, le o resource
`politica://uso` e propaga o `traceparent` no `_meta` de todos os requests.
"""

from __future__ import annotations

import asyncio
import re
import secrets
from contextlib import AsyncExitStack
from typing import Any

import mcp_types as types
from mcp.client.session import ClientSession
from mcp.client.streamable_http import streamable_http_client
from mcp.shared.jsonrpc_dispatcher import JSONRPCDispatcher

PROTOCOLO_MCP = "2026-07-28"
URI_POLITICA = "politica://uso"
PADRAO_TRACEPARENT = re.compile(r"^([0-9a-f]{2})-([0-9a-f]{32})-([0-9a-f]{16})-([0-9a-f]{2})$")


def propagar_traceparent(traceparent_entrada: str | None) -> str | None:
    """Preserva o trace-id recebido do cliente A2A gerando um novo span-id filho."""
    if not traceparent_entrada:
        return None
    limpo = traceparent_entrada.strip().lower()
    correspondencia = PADRAO_TRACEPARENT.match(limpo)
    if not correspondencia:
        return traceparent_entrada.strip()
    versao, trace_id, _parent_span, flags = correspondencia.groups()
    novo_span_id = secrets.token_hex(8)
    return f"{versao}-{trace_id}-{novo_span_id}-{flags}"


def montar_meta(traceparent: str | None) -> types.RequestParamsMeta | None:
    propagado = propagar_traceparent(traceparent)
    if not propagado:
        return None
    return {"traceparent": propagado}


class SessaoClienteFormElicitation(ClientSession):
    """ClientSession que anuncia exatamente `{"elicitation": {"form": {}}}`."""

    def _build_capabilities(self, version: str) -> types.ClientCapabilities:
        return types.ClientCapabilities(
            elicitation=types.ElicitationCapability(
                form=types.FormElicitationCapability(),
            )
        )


class HostMCPCentralDeSalas:
    """Host MCP que mantem o cliente Streamable HTTP vivo entre chamadas."""

    def __init__(self, mcp_url: str) -> None:
        self.mcp_url = mcp_url
        self._exit_stack: AsyncExitStack | None = None
        self._sessao: SessaoClienteFormElicitation | None = None
        self._ferramentas_descobertas: dict[str, types.Tool] | None = None
        self._trava_descoberta = asyncio.Lock()

    async def iniciar(self) -> None:
        stack = AsyncExitStack()
        read_stream, write_stream = await stack.enter_async_context(
            streamable_http_client(self.mcp_url)
        )
        dispatcher = JSONRPCDispatcher(read_stream, write_stream)
        sessao = SessaoClienteFormElicitation(
            dispatcher=dispatcher,
            client_info=types.Implementation(
                name="agente-central-de-salas",
                version="1.0.0",
            ),
        )
        await stack.enter_async_context(sessao)
        sessao.adopt(
            types.DiscoverResult(
                supported_versions=[PROTOCOLO_MCP],
                capabilities=types.ServerCapabilities(),
                result_type="complete",
                ttl_ms=0,
                cache_scope="public",
            )
        )
        self._sessao = sessao
        self._exit_stack = stack

    async def encerrar(self) -> None:
        if self._exit_stack is not None:
            await self._exit_stack.aclose()
            self._exit_stack = None
            self._sessao = None

    @property
    def sessao(self) -> SessaoClienteFormElicitation:
        if self._sessao is None:
            raise RuntimeError("HostMCPCentralDeSalas nao foi iniciado.")
        return self._sessao

    async def garantir_descoberta(self, traceparent: str | None = None) -> dict[str, types.Tool]:
        """Executa `tools/list` em runtime antes da primeira chamada de ferramenta."""
        if self._ferramentas_descobertas is not None:
            return self._ferramentas_descobertas

        async with self._trava_descoberta:
            if self._ferramentas_descobertas is None:
                meta = montar_meta(traceparent)
                params = types.PaginatedRequestParams(_meta=meta) if meta else None
                resultado = await self.sessao.list_tools(params=params)
                self._ferramentas_descobertas = {t.name: t for t in resultado.tools}
            return self._ferramentas_descobertas

    async def ler_versao_politica(self, traceparent: str | None = None) -> str:
        """Le o resource `politica://uso` e extrai a versao declarada na primeira linha."""
        resultado = await self.sessao.read_resource(
            URI_POLITICA,
            meta=montar_meta(traceparent),
        )
        if not resultado.contents:
            raise RuntimeError(f"Resource {URI_POLITICA} retornou conteudo vazio.")
        primeiro = resultado.contents[0]
        texto = getattr(primeiro, "text", "")
        if not texto:
            raise RuntimeError(f"Resource {URI_POLITICA} nao contem texto.")
        primeira_linha = texto.splitlines()[0].strip()
        if ":" in primeira_linha:
            return primeira_linha.split(":", 1)[1].strip()
        return primeira_linha

    async def reservar_sala(
        self,
        argumentos: dict[str, Any],
        *,
        input_responses: types.InputResponses | None = None,
        request_state: str | None = None,
        traceparent: str | None = None,
    ) -> types.CallToolResult | types.InputRequiredResult:
        """Chama a tool descoberta `reservar_sala` preservando `InputRequiredResult` cru."""
        ferramentas = await self.garantir_descoberta(traceparent=traceparent)
        if "reservar_sala" not in ferramentas:
            raise RuntimeError("Servidor MCP nao expos a ferramenta 'reservar_sala' em tools/list.")

        return await self.sessao.call_tool(
            "reservar_sala",
            argumentos,
            input_responses=input_responses,
            request_state=request_state,
            meta=montar_meta(traceparent),
            allow_input_required=True,
        )

