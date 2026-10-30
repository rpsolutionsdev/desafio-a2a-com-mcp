"""A Ponte: costura de estado entre Tasks do protocolo A2A e MRTR do protocolo MCP.

Quando o servidor MCP responde `resultType: "input_required"` com uma elicitation
em modo `form` e um `requestState` opaco, o `ExecutorPonteSalas`:
1. Guarda o `requestState` opaco em memoria indexado pelo `task_id` (sem jamais
   abri-lo, interpreta-lo ou expo-lo em mensagens/artifacts/card A2A);
2. Interrompe a Task A2A em `TASK_STATE_INPUT_REQUIRED` devolvendo exatamente
   `alternativas: <ids separados por virgula e espaco>`;
3. Na continuacao (`SendMessage` com o mesmo `taskId`), valida se `escolha=<valor>`
   pertence ao `enum` ofertado (ou `recusar`) e repete `tools/call` com um novo id
   JSON-RPC enviando `inputResponses` e o `requestState` ecoado intacto.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Any

import mcp_types as mcp_types
from a2a.helpers.proto_helpers import new_task_from_user_message, new_text_part
from a2a.server.agent_execution import AgentExecutor, RequestContext
from a2a.server.events import EventQueue
from a2a.server.tasks import TaskUpdater
from a2a.types import TaskState

from mcp_host import HostMCPCentralDeSalas

PADRAO_PEDIDO_RESERVA = re.compile(
    r"^reservar\s+sala=(\S+)\s+inicio=(\S+)\s+fim=(\S+)\s+responsavel=(.+)$"
)
PADRAO_ESCOLHA = re.compile(r"^escolha=(\S+)$")


@dataclass(frozen=True)
class EstadoPausaMRTR:
    """Estado interno associado a uma Task A2A pausada aguardando escolha do cliente."""

    argumentos_originais: dict[str, str]
    chave_elicitation: str
    alternativas: list[str]
    request_state: str  # Opaco: guardado e ecoado sem inspecao nem modificacao
    traceparent_inicial: str | None = None


def extrair_traceparent_http(context: RequestContext) -> str | None:
    cabecalhos = context.call_context.state.get("headers") or {}
    if isinstance(cabecalhos, dict):
        valor = cabecalhos.get("traceparent")
        if isinstance(valor, str) and valor.strip():
            return valor.strip()
    return None


def interpretar_pedido_inicial(texto: str) -> dict[str, str] | None:
    correspondencia = PADRAO_PEDIDO_RESERVA.match(texto.strip())
    if not correspondencia:
        return None
    sala, inicio, fim, responsavel = correspondencia.groups()
    return {
        "sala": sala,
        "inicio": inicio,
        "fim": fim,
        "responsavel": responsavel.strip(),
    }


def interpretar_escolha(texto: str) -> str | None:
    correspondencia = PADRAO_ESCOLHA.match(texto.strip())
    if not correspondencia:
        return None
    return correspondencia.group(1).strip()


def extrair_texto_erro_tool(resultado: mcp_types.CallToolResult) -> str:
    partes_texto: list[str] = []
    for bloco in resultado.content:
        texto = getattr(bloco, "text", None)
        if isinstance(texto, str) and texto:
            partes_texto.append(texto)
    return " ".join(partes_texto) or "Erro ao executar ferramenta no servidor MCP."


def extrair_alternativas_elicitation(
    resultado: mcp_types.InputRequiredResult,
) -> tuple[str, list[str]]:
    """Extrai a chave do `inputRequests` e a lista ordenada de salas do `requestedSchema`."""
    if len(resultado.input_requests) != 1:
        raise RuntimeError(
            f"Esperado exatamente 1 inputRequest no MRTR, recebido: {len(resultado.input_requests)}"
        )
    chave, pedido = next(iter(resultado.input_requests.items()))
    params = getattr(pedido, "params", None)
    esquema = getattr(params, "requested_schema", None) or {}
    propriedades = esquema.get("properties") if isinstance(esquema, dict) else None
    campo_sala = propriedades.get("sala") if isinstance(propriedades, dict) else None
    if not isinstance(campo_sala, dict):
        raise RuntimeError("requestedSchema nao contem a propriedade plana 'sala'.")

    if "enum" in campo_sala and isinstance(campo_sala["enum"], list):
        alternativas = [str(item) for item in campo_sala["enum"]]
    elif "const" in campo_sala:
        alternativas = [str(campo_sala["const"])]
    else:
        raise RuntimeError("Propriedade 'sala' no requestedSchema nao define 'enum' nem 'const'.")

    return chave, alternativas


def formatar_linha_alternativas(alternativas: list[str]) -> str:
    return f"alternativas: {', '.join(alternativas)}"


class ExecutorPonteSalas(AgentExecutor):
    """Executor deterministico que traduz Tasks A2A em chamadas MCP com MRTR."""

    def __init__(self, mcp_host: HostMCPCentralDeSalas) -> None:
        self._mcp_host = mcp_host
        # Estado pausado isolado por Task (nunca serializado em objetos A2A)
        self._pausas_por_task: dict[str, EstadoPausaMRTR] = {}

    async def execute(self, context: RequestContext, event_queue: EventQueue) -> None:
        if not context.task_id or not context.context_id or context.message is None:
            raise RuntimeError("Contexto da requisicao A2A incompleto.")

        task_id = context.task_id
        context_id = (
            context.current_task.context_id
            if context.current_task is not None and context.current_task.context_id
            else context.context_id
        )
        if context.current_task is not None and not context.message.context_id:
            context.message.context_id = context_id
        updater = TaskUpdater(event_queue, task_id, context_id)
        texto_entrada = context.get_user_input().strip()
        traceparent_atual = extrair_traceparent_http(context)

        # Se a Task ja esta pausada aguardando escolha, processa a retomada da ponte.
        if context.current_task is not None and task_id in self._pausas_por_task:
            await self._processar_continuacao(
                task_id=task_id,
                texto_entrada=texto_entrada,
                traceparent_http=traceparent_atual,
                updater=updater,
            )
            return

        # Abertura de nova Task: transita por TASK_STATE_SUBMITTED -> TASK_STATE_WORKING
        if context.current_task is None:
            await event_queue.enqueue_event(new_task_from_user_message(context.message))
        await updater.start_work()

        argumentos = interpretar_pedido_inicial(texto_entrada)
        if argumentos is None:
            await self._finalizar_com_falha(
                updater,
                "Formato de pedido invalido. Use: reservar sala=<id> inicio=<iso8601> fim=<iso8601> responsavel=<nome>",
            )
            return

        resultado_mcp = await self._mcp_host.reservar_sala(
            argumentos,
            traceparent=traceparent_atual,
        )

        # Ponte (ida): `input_required` do MCP vira `TASK_STATE_INPUT_REQUIRED` na Task A2A
        if isinstance(resultado_mcp, mcp_types.InputRequiredResult):
            if not resultado_mcp.request_state:
                await self._finalizar_com_falha(
                    updater,
                    "Servidor MCP respondeu input_required sem requestState.",
                )
                return

            chave, alternativas = extrair_alternativas_elicitation(resultado_mcp)
            self._pausas_por_task[task_id] = EstadoPausaMRTR(
                argumentos_originais=argumentos,
                chave_elicitation=chave,
                alternativas=alternativas,
                request_state=resultado_mcp.request_state,
                traceparent_inicial=traceparent_atual,
            )
            mensagem_pausa = updater.new_agent_message(
                [new_text_part(formatar_linha_alternativas(alternativas))]
            )
            await updater.requires_input(mensagem_pausa)
            return

        await self._tratar_resultado_completo(
            resultado_mcp=resultado_mcp,
            traceparent=traceparent_atual,
            updater=updater,
        )

    async def _processar_continuacao(
        self,
        *,
        task_id: str,
        texto_entrada: str,
        traceparent_http: str | None,
        updater: TaskUpdater,
    ) -> None:
        pausa = self._pausas_por_task[task_id]
        escolha = interpretar_escolha(texto_entrada)

        # Escolha invalida ou fora do enum ofertado mantem a Task em TASK_STATE_INPUT_REQUIRED
        if escolha is None or (escolha != "recusar" and escolha not in pausa.alternativas):
            mensagem_repeticao = updater.new_agent_message(
                [new_text_part(formatar_linha_alternativas(pausa.alternativas))]
            )
            await updater.requires_input(mensagem_repeticao)
            return

        # Escolha valida: retoma o trabalho e remove o estado pausado desta Task
        self._pausas_por_task.pop(task_id, None)
        await updater.start_work()
        traceparent_efetivo = traceparent_http or pausa.traceparent_inicial

        if escolha == "recusar":
            resposta_elicitation = mcp_types.ElicitResult(action="decline")
        else:
            resposta_elicitation = mcp_types.ElicitResult(
                action="accept",
                content={"sala": escolha},
            )

        # Ponte (volta): repete o tools/call original com novo id JSON-RPC, inputResponses e requestState
        resultado_retry = await self._mcp_host.reservar_sala(
            pausa.argumentos_originais,
            input_responses={pausa.chave_elicitation: resposta_elicitation},
            request_state=pausa.request_state,
            traceparent=traceparent_efetivo,
        )

        if isinstance(resultado_retry, mcp_types.InputRequiredResult):
            await self._finalizar_com_falha(
                updater,
                "Servidor MCP solicitou nova rodada inesperada apos resposta da elicitation.",
            )
            return

        if escolha == "recusar" and not resultado_retry.is_error:
            await self._finalizar_cancelada(
                updater,
                "Reserva cancelada: alternativas recusadas pelo solicitante.",
            )
            return

        await self._tratar_resultado_completo(
            resultado_mcp=resultado_retry,
            traceparent=traceparent_efetivo,
            updater=updater,
        )

    async def _tratar_resultado_completo(
        self,
        *,
        resultado_mcp: mcp_types.CallToolResult,
        traceparent: str | None,
        updater: TaskUpdater,
    ) -> None:
        if resultado_mcp.is_error:
            await self._finalizar_com_falha(updater, extrair_texto_erro_tool(resultado_mcp))
            return

        dados: dict[str, Any] = dict(resultado_mcp.structured_content or {})
        if not dados.get("reservado", False):
            await self._finalizar_cancelada(
                updater,
                f"Reserva nao realizada: {dados.get('motivo') or 'recusado'}.",
            )
            return

        versao_politica = await self._mcp_host.ler_versao_politica(traceparent=traceparent)
        payload_artifact = {
            "reserva": dados["reserva"],
            "sala": dados["sala"],
            "inicio": dados["inicio"],
            "fim": dados["fim"],
            "responsavel": dados["responsavel"],
            "politica": versao_politica,
        }
        await updater.add_artifact(
            parts=[new_text_part(json.dumps(payload_artifact))],
            name="reserva",
        )
        await self._finalizar_concluida(
            updater,
            f"Reserva {dados['reserva']} confirmada na {dados['sala']}.",
        )

    async def _finalizar_concluida(self, updater: TaskUpdater, texto: str) -> None:
        msg = updater.new_agent_message([new_text_part(texto)])
        await updater.start_work(msg)
        await updater.complete(msg)

    async def _finalizar_cancelada(self, updater: TaskUpdater, texto: str) -> None:
        msg = updater.new_agent_message([new_text_part(texto)])
        await updater.start_work(msg)
        await updater.cancel(msg)

    async def _finalizar_com_falha(self, updater: TaskUpdater, texto_erro: str) -> None:
        msg = updater.new_agent_message([new_text_part(texto_erro)])
        await updater.start_work(msg)
        await updater.failed(msg)

    async def cancel(self, context: RequestContext, event_queue: EventQueue) -> None:
        if context.task_id:
            self._pausas_por_task.pop(context.task_id, None)
        if context.task_id and context.context_id:
            updater = TaskUpdater(event_queue, context.task_id, context.context_id)
            await updater.update_status(TaskState.TASK_STATE_CANCELED)

