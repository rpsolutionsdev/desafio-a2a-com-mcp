#!/usr/bin/env python3
"""Servidor MCP da Central de Salas (Hill Valley Tech).

Implementa o protocolo MCP revisao 2026-07-28 sobre Streamable HTTP (stateless),
com tres tools (`listar_salas`, `consultar_disponibilidade`, `reservar_sala`),
um resource (`politica://uso`) e o ciclo completo de MRTR (`input_required` +
`requestState` autenticado/cifrado via `RequestStateSecurity` do SDK oficial).
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import os
import socket
import sys
from collections.abc import AsyncIterator
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import mcp_types as types
import uvicorn
from mcp.server._streamable_http_modern import handle_modern_request
from mcp.server.mcpserver import Context, MCPServer
from mcp.server.mcpserver.exceptions import ToolError
from mcp.server.request_state import RequestStateSecurity
from mcp.shared.exceptions import MCPError
from mcp_types import INVALID_PARAMS, MISSING_REQUIRED_CLIENT_CAPABILITY
from pydantic import BaseModel
from starlette.applications import Starlette
from starlette.responses import Response
from starlette.types import Receive, Scope, Send

RAIZ_REPO = Path(__file__).resolve().parent.parent
ARQUIVO_SALAS = RAIZ_REPO / "dados" / "salas.json"
ARQUIVO_RESERVAS = RAIZ_REPO / "dados" / "reservas.json"
ARQUIVO_POLITICA = RAIZ_REPO / "dados" / "politica-de-uso.md"

FUSO_SAO_PAULO = timezone(timedelta(hours=-3))
LIMITE_DURACAO = timedelta(hours=2)
TTL_REQUEST_STATE_SEGUNDOS = 900.0  # 15 minutos (entre 5 e 30 minutos exigidos)
CHAVE_ELICITATION = "__main__:escolha_de_sala"


class SalaOut(BaseModel):
    id: str
    nome: str
    capacidade: int
    recursos: list[str]


class ListaDeSalas(BaseModel):
    salas: list[SalaOut]


class ConflitoOut(BaseModel):
    id: str
    inicio: str
    fim: str
    responsavel: str


class Disponibilidade(BaseModel):
    sala: str
    livre: bool
    conflitos: list[ConflitoOut]


class ReservaOut(BaseModel):
    reserva: str | None = None
    reservado: bool = True
    sala: str | None = None
    inicio: str | None = None
    fim: str | None = None
    responsavel: str | None = None
    politica: str | None = None
    motivo: str | None = None


def carregar_segredo_request_state() -> bytes:
    """Lê e valida REQUEST_STATE_SECRET do ambiente (>= 32 bytes de aleatoriedade)."""
    bruto = os.environ.get("REQUEST_STATE_SECRET", "").strip()
    if not bruto:
        print(
            "ERRO: variavel de ambiente REQUEST_STATE_SECRET nao definida. "
            'Gere uma chave com: python3 -c "import secrets; print(secrets.token_hex(32))"',
            file=sys.stderr,
            flush=True,
        )
        raise SystemExit(1)

    try:
        material = bytes.fromhex(bruto)
    except ValueError:
        material = bruto.encode("utf-8")

    if len(material) < 32:
        print(
            f"ERRO: REQUEST_STATE_SECRET deve ter no minimo 32 bytes de aleatoriedade (recebido: {len(material)} bytes).",
            file=sys.stderr,
            flush=True,
        )
        raise SystemExit(1)

    return material


def carregar_salas() -> list[dict[str, Any]]:
    return json.loads(ARQUIVO_SALAS.read_text(encoding="utf-8"))


def carregar_reservas_iniciais() -> list[dict[str, str]]:
    return json.loads(ARQUIVO_RESERVAS.read_text(encoding="utf-8"))


def ler_texto_politica() -> str:
    return ARQUIVO_POLITICA.read_text(encoding="utf-8")


def extrair_versao_politica(texto: str) -> str:
    primeira_linha = texto.splitlines()[0].strip()
    if ":" in primeira_linha:
        return primeira_linha.split(":", 1)[1].strip()
    return primeira_linha


class EstadoDominioSalas:
    """Mantem as salas fixas e as reservas em memoria durante a vida do processo."""

    def __init__(self) -> None:
        self.salas: list[dict[str, Any]] = carregar_salas()
        self.salas_por_id: dict[str, dict[str, Any]] = {s["id"]: s for s in self.salas}
        self.reservas: list[dict[str, str]] = carregar_reservas_iniciais()
        maior_id = 0
        for r in self.reservas:
            partes = r["id"].split("-")
            if len(partes) == 2 and partes[1].isdigit():
                maior_id = max(maior_id, int(partes[1]))
        self._proximo_numero = maior_id + 1

    def gerar_id_reserva(self) -> str:
        novo_id = f"res-{self._proximo_numero:04d}"
        self._proximo_numero += 1
        return novo_id


def parse_iso_sp(valor: str) -> datetime:
    try:
        dt = datetime.fromisoformat(valor)
    except ValueError as exc:
        raise ToolError(f"Data/hora ISO-8601 invalida: {valor}") from exc
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=FUSO_SAO_PAULO)
    return dt.astimezone(FUSO_SAO_PAULO)


def validar_pedido(dominio: EstadoDominioSalas, sala: str, inicio: str, fim: str) -> tuple[datetime, datetime]:
    """Aplica as validacoes de existencia da sala e da politica de uso."""
    if sala not in dominio.salas_por_id:
        raise ToolError(f"Sala inexistente: {sala}")

    dt_inicio = parse_iso_sp(inicio)
    dt_fim = parse_iso_sp(fim)

    if dt_fim <= dt_inicio:
        raise ToolError("Intervalo invalido: fim deve ser posterior a inicio")

    janela_abertura = dt_inicio.replace(hour=8, minute=0, second=0, microsecond=0)
    janela_fechamento = dt_inicio.replace(hour=20, minute=0, second=0, microsecond=0)
    if dt_inicio < janela_abertura or dt_fim > janela_fechamento:
        raise ToolError("Fora da janela de uso: a politica permite reservas entre 08:00 e 20:00")

    if (dt_fim - dt_inicio) > LIMITE_DURACAO:
        raise ToolError("Duracao acima do limite: a politica permite no maximo 2 horas")

    return dt_inicio, dt_fim


def buscar_conflitos(
    dominio: EstadoDominioSalas,
    sala: str,
    dt_inicio: datetime,
    dt_fim: datetime,
) -> list[dict[str, str]]:
    conflitos: list[dict[str, str]] = []
    for reserva in dominio.reservas:
        if reserva["sala"] != sala:
            continue
        r_inicio = parse_iso_sp(reserva["inicio"])
        r_fim = parse_iso_sp(reserva["fim"])
        if dt_inicio < r_fim and dt_fim > r_inicio:
            conflitos.append(reserva)
    return conflitos


def calcular_alternativas(
    dominio: EstadoDominioSalas,
    sala_pedida: str,
    dt_inicio: datetime,
    dt_fim: datetime,
) -> list[str]:
    """Salas livres no intervalo com capacidade >= sala pedida, max 3, por (capacidade, id)."""
    capacidade_minima = dominio.salas_por_id[sala_pedida]["capacidade"]
    candidatas: list[dict[str, Any]] = []
    for candidata in dominio.salas:
        if candidata["id"] == sala_pedida:
            continue
        if candidata["capacidade"] < capacidade_minima:
            continue
        if not buscar_conflitos(dominio, candidata["id"], dt_inicio, dt_fim):
            candidatas.append(candidata)

    candidatas.sort(key=lambda s: (s["capacidade"], s["id"]))
    return [s["id"] for s in candidatas[:3]]


def cliente_suporta_elicitation_form(ctx: Context[Any, Any]) -> bool:
    caps = ctx.client_capabilities
    return bool(caps is not None and caps.elicitation is not None and caps.elicitation.form is not None)


def construir_servidor_mcp(segredo: bytes) -> MCPServer:
    dominio = EstadoDominioSalas()
    seguranca = RequestStateSecurity(keys=[segredo], ttl=TTL_REQUEST_STATE_SEGUNDOS)
    mcp = MCPServer(
        "central-de-salas",
        version="1.0.0",
        request_state_security=seguranca,
        subscriptions=False,
    )

    @mcp.resource(
        "politica://uso",
        name="Politica de uso",
        description="Politica vigente para reserva das salas de reuniao da Hill Valley Tech.",
        mime_type="text/markdown",
    )
    def recurso_politica_uso() -> str:
        return ler_texto_politica()

    @mcp.tool(description="Lista todas as salas com capacidade e recursos.")
    def listar_salas() -> ListaDeSalas:
        return ListaDeSalas(salas=[SalaOut(**s) for s in dominio.salas])

    @mcp.tool(description="Diz se uma sala esta livre no intervalo, e quais reservas conflitam.")
    def consultar_disponibilidade(sala: str, inicio: str, fim: str) -> Disponibilidade:
        dt_inicio, dt_fim = validar_pedido(dominio, sala, inicio, fim)
        conflitos = buscar_conflitos(dominio, sala, dt_inicio, dt_fim)
        return Disponibilidade(
            sala=sala,
            livre=len(conflitos) == 0,
            conflitos=[
                ConflitoOut(
                    id=c["id"],
                    inicio=c["inicio"],
                    fim=c["fim"],
                    responsavel=c["responsavel"],
                )
                for c in conflitos
            ],
        )

    @mcp.tool(description="Reserva uma sala. Se o intervalo estiver ocupado, pergunta qual alternativa usar.")
    def reservar_sala(
        sala: str,
        inicio: str,
        fim: str,
        responsavel: str,
        ctx: Context,
    ) -> ReservaOut | types.InputRequiredResult:
        # Caminho de retry MRTR: reconstrói tudo a partir do requestState autenticado pelo SDK.
        if ctx.request_state is not None:
            try:
                estado_selado = json.loads(ctx.request_state)
            except ValueError as exc:
                raise MCPError(code=INVALID_PARAMS, message="requestState invalido") from exc

            chave = estado_selado["chave"]
            alternativas: list[str] = estado_selado["alternativas"]
            inicio_selado: str = estado_selado["inicio"]
            fim_selado: str = estado_selado["fim"]
            responsavel_selado: str = estado_selado["responsavel"]

            respostas = ctx.input_responses or {}
            if chave not in respostas:
                raise MCPError(
                    code=INVALID_PARAMS,
                    message=f"inputResponses nao contem resposta para '{chave}'",
                )

            resposta = respostas[chave]
            if not isinstance(resposta, types.ElicitResult):
                raise MCPError(code=INVALID_PARAMS, message="Formato de resposta de elicitation invalido")

            if resposta.action in ("decline", "cancel"):
                return ReservaOut(
                    reserva=None,
                    reservado=False,
                    sala=None,
                    inicio=None,
                    fim=None,
                    responsavel=None,
                    politica=None,
                    motivo="recusado",
                )

            conteudo = resposta.content or {}
            sala_escolhida = conteudo.get("sala")
            if not isinstance(sala_escolhida, str) or sala_escolhida not in alternativas:
                raise MCPError(
                    code=INVALID_PARAMS,
                    message=f"Sala escolhida '{sala_escolhida}' nao faz parte das alternativas ofertadas",
                )

            dt_inicio, dt_fim = validar_pedido(dominio, sala_escolhida, inicio_selado, fim_selado)
            if buscar_conflitos(dominio, sala_escolhida, dt_inicio, dt_fim):
                raise ToolError("Sem alternativas disponiveis no intervalo")

            novo_id = dominio.gerar_id_reserva()
            dominio.reservas.append(
                {
                    "id": novo_id,
                    "sala": sala_escolhida,
                    "inicio": inicio_selado,
                    "fim": fim_selado,
                    "responsavel": responsavel_selado,
                }
            )
            versao_politica = extrair_versao_politica(ler_texto_politica())
            return ReservaOut(
                reserva=novo_id,
                reservado=True,
                sala=sala_escolhida,
                inicio=inicio_selado,
                fim=fim_selado,
                responsavel=responsavel_selado,
                politica=versao_politica,
                motivo=None,
            )

        if ctx.input_responses:
            raise MCPError(code=INVALID_PARAMS, message="inputResponses exige requestState correspondente")

        # Chamada inicial
        dt_inicio, dt_fim = validar_pedido(dominio, sala, inicio, fim)
        conflitos = buscar_conflitos(dominio, sala, dt_inicio, dt_fim)
        if not conflitos:
            novo_id = dominio.gerar_id_reserva()
            dominio.reservas.append(
                {
                    "id": novo_id,
                    "sala": sala,
                    "inicio": inicio,
                    "fim": fim,
                    "responsavel": responsavel,
                }
            )
            versao_politica = extrair_versao_politica(ler_texto_politica())
            return ReservaOut(
                reserva=novo_id,
                reservado=True,
                sala=sala,
                inicio=inicio,
                fim=fim,
                responsavel=responsavel,
                politica=versao_politica,
                motivo=None,
            )

        alternativas = calcular_alternativas(dominio, sala, dt_inicio, dt_fim)
        if not alternativas:
            raise ToolError("Sem alternativas disponiveis no intervalo")

        if not cliente_suporta_elicitation_form(ctx):
            raise MCPError(
                code=MISSING_REQUIRED_CLIENT_CAPABILITY,
                message=(
                    "Client did not declare the form elicitation capability "
                    f"required by resolver '{CHAVE_ELICITATION}'"
                ),
                data={"requiredCapabilities": {"elicitation": {"form": {}}}},
            )

        pedido_elicitation = types.ElicitRequest(
            params=types.ElicitRequestFormParams(
                mode="form",
                message="A sala pedida esta ocupada nesse intervalo. Escolha uma alternativa.",
                requestedSchema={
                    "type": "object",
                    "properties": {
                        "sala": {
                            "type": "string",
                            "title": "Sala",
                            "description": "Sala alternativa escolhida",
                            "enum": alternativas,
                        }
                    },
                    "required": ["sala"],
                },
            )
        )
        estado_pedido = json.dumps(
            {
                "chave": CHAVE_ELICITATION,
                "sala": sala,
                "inicio": inicio,
                "fim": fim,
                "responsavel": responsavel,
                "alternativas": alternativas,
            },
            separators=(",", ":"),
        )
        return types.InputRequiredResult(
            input_requests={CHAVE_ELICITATION: pedido_elicitation},
            request_state=estado_pedido,
        )

    return mcp


def registrar_request_stderr(corpo_bytes: bytes) -> None:
    """Registra no stderr metodo, id e traceparent (quando presente em _meta) de cada request."""
    try:
        dados = json.loads(corpo_bytes)
    except ValueError:
        print("[mcp] id=None method=<json-invalido> traceparent=-", file=sys.stderr, flush=True)
        return

    if not isinstance(dados, dict):
        print("[mcp] id=None method=<envelope-invalido> traceparent=-", file=sys.stderr, flush=True)
        return

    req_id = dados.get("id")
    metodo = dados.get("method", "<sem-metodo>")
    params = dados.get("params")
    meta = params.get("_meta") if isinstance(params, dict) else None
    traceparent = meta.get("traceparent") if isinstance(meta, dict) else None
    print(
        f"[mcp] id={req_id} method={metodo} traceparent={traceparent or '-'}",
        file=sys.stderr,
        flush=True,
    )


async def ler_corpo_completo(receive: Receive) -> bytes:
    partes: list[bytes] = []
    while True:
        mensagem = await receive()
        if mensagem["type"] == "http.request":
            partes.append(mensagem.get("body", b""))
            if not mensagem.get("more_body", False):
                break
        elif mensagem["type"] == "http.disconnect":
            break
    return b"".join(partes)


def criar_replay_receive(corpo: bytes, receive_original: Receive) -> Receive:
    entregue = False

    async def replay_receive() -> dict[str, Any]:
        nonlocal entregue
        if not entregue:
            entregue = True
            return {"type": "http.request", "body": corpo, "more_body": False}
        return await receive_original()

    return replay_receive


def construir_app_asgi(mcp: MCPServer, caminho_mcp: str = "/mcp") -> Starlette:
    servidor_baixo_nivel = mcp._lowlevel_server
    estado_lifespan: dict[str, Any] = {"valor": None}
    caminho_normalizado = "/" + caminho_mcp.strip("/")

    @contextlib.asynccontextmanager
    async def lifespan(_app: Starlette) -> AsyncIterator[None]:
        async with servidor_baixo_nivel.lifespan(servidor_baixo_nivel) as estado:
            estado_lifespan["valor"] = estado
            yield
            estado_lifespan["valor"] = None

    starlette_app = Starlette(lifespan=lifespan)

    class RoteadorStreamableHTTP:
        def __init__(self, app_interno: Starlette) -> None:
            self.app_interno = app_interno

        async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
            if scope["type"] != "http":
                await self.app_interno(scope, receive, send)
                return

            caminho = scope.get("path", "")
            if caminho != caminho_normalizado:
                await Response("Not Found", status_code=404)(scope, receive, send)
                return

            if scope.get("method") == "POST":
                corpo = await ler_corpo_completo(receive)
                registrar_request_stderr(corpo)
                receive = criar_replay_receive(corpo, receive)

            await handle_modern_request(
                servidor_baixo_nivel,
                None,
                True,
                estado_lifespan["valor"],
                scope,
                receive,
                send,
            )

    return RoteadorStreamableHTTP(starlette_app)  # type: ignore[return-value]


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
    segredo = carregar_segredo_request_state()
    host_env = os.environ.get("MCP_HOST")
    host = host_env or "0.0.0.0"
    porta = int(os.environ.get("MCP_PORT", os.environ.get("PORT", "7301")))
    caminho_mcp = os.environ.get("MCP_PATH", "/mcp")

    mcp = construir_servidor_mcp(segredo)
    app = construir_app_asgi(mcp, caminho_mcp=caminho_mcp)

    print(
        f"[mcp] Central de Salas iniciando em http://localhost:{porta}{caminho_mcp}",
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

