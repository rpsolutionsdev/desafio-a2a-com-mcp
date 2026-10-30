# A Ponte: Central de Salas (Agente A2A v1.0 + Servidor MCP v2)

Implementação da fronteira **A2A v1.0** (JSON-RPC 2.0 sobre HTTP, porta `7300`) e **MCP v2** (revisão `2026-07-28` da especificação sobre Streamable HTTP stateless, porta `7301`) para reserva determinística das salas de reunião da Hill Valley Tech, costurando Tasks A2A (`TASK_STATE_INPUT_REQUIRED`) ao ciclo Multi-Round Tool Execution (`MRTR` com `input_required`, elicitation em modo `form` e `requestState` autenticado).

## Como rodar

Pré-requisito: **Python 3.10 ou superior**.

### 1. Clonar o repositório e instalar as dependências travadas

A partir de uma pasta limpa:

```bash
git clone <URL_DO_SEU_FORK> desafio-a2a-com-mcp
cd desafio-a2a-com-mcp

python3 -m venv .venv
source .venv/bin/activate
pip install ./servidor-mcp ./agente
```

No Windows (PowerShell):

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install ./servidor-mcp ./agente
```

### 2. Gerar e exportar `REQUEST_STATE_SECRET` e subir o Servidor MCP (Terminal 1)

Gere uma chave aleatória de 32 bytes (64 caracteres hexadecimais) e inicie o servidor MCP na porta `7301` mantendo o `stderr` visível no terminal:

```bash
source .venv/bin/activate
export REQUEST_STATE_SECRET="$(python3 -c 'import secrets; print(secrets.token_hex(32))')"
python3 servidor-mcp/servidor.py
```

No Windows (PowerShell):

```powershell
.\.venv\Scripts\Activate.ps1
$env:REQUEST_STATE_SECRET = (python -c "import secrets; print(secrets.token_hex(32))")
python servidor-mcp/servidor.py
```

*(Variáveis opcionais suportadas: `MCP_PORT`, padrão `7301`; `MCP_HOST`, padrão dual-stack/`0.0.0.0`; `MCP_PATH`, padrão `/mcp`.)*

### 3. Subir o Agente A2A / Host MCP (Terminal 2)

Em outro terminal:

```bash
source .venv/bin/activate
python3 agente/servidor.py
```

No Windows (PowerShell):

```powershell
.\.venv\Scripts\Activate.ps1
python agente/servidor.py
```

*(Variáveis opcionais suportadas: `AGENTE_PORT`, padrão `7300`; `AGENTE_URL`, padrão `http://localhost:7300`; `AGENTE_PATH`, padrão `/a2a`; `MCP_URL`, padrão `http://localhost:7301/mcp`.)*

### 4. Executar o validador de conformidade (Terminal 3)

Com os dois processos recém-iniciados:

```bash
python3 validador/validar.py --agente http://localhost:7300 --mcp http://localhost:7301
```

No Windows (PowerShell):

```powershell
.\.venv\Scripts\python.exe validador/validar.py --agente http://localhost:7300 --mcp http://localhost:7301
```

## Onde a ponte acontece

A ponte acontece em `agente/ponte.py`, na classe `ExecutorPonteSalas`: na ida (`ExecutorPonteSalas.execute`, linhas 168–188), após invocar `self._mcp_host.reservar_sala(...)` com `allow_input_required=True` (`agente/mcp_host.py`, linhas 136–156), o agente detecta o `mcp_types.InputRequiredResult` cru retornado pelo servidor MCP, extrai a chave de `inputRequests` e as opções de `requestedSchema`, guarda o `requestState` de forma opaca em `self._pausas_por_task[task_id]` (`EstadoPausaMRTR`) sem jamais inspecioná-lo ou expô-lo ao cliente externo, e chama `await updater.requires_input(...)` para interromper a Task em `TASK_STATE_INPUT_REQUIRED` com a mensagem exata `alternativas: <ids>`; na volta (`ExecutorPonteSalas._processar_continuacao`, linhas 196–234), ao receber o `SendMessage` de continuação com o mesmo `taskId` e uma escolha válida (`escolha=<id>` ou `escolha=recusar`), o executor remove a pausa daquela Task, monta o `ElicitResult` (`action="accept"` ou `action="decline"`) sob a mesma chave do `inputRequests` e repete `tools/call` em `self._mcp_host.reservar_sala(...)` com um novo `id` JSON-RPC gerado pelo `ClientSession`, reenviando `input_responses` e ecoando intacto o `requestState` ao servidor MCP.

## Decisões técnicas

- **Proteção de integridade e confidencialidade do `requestState`**: protegida pelo utilitário oficial `RequestStateSecurity` (`AESGCMRequestStateCodec` + `RequestStateBoundary`) do pacote `mcp==2.3.0` (`servidor-mcp/servidor.py`, linhas 82–107 e 226–232). O estado da rodada inicial (chave da elicitation, `sala`, `inicio`, `fim`, `responsavel` e `alternativas`) é cifrado e autenticado com **AES-256-GCM** (AEAD) usando subchave derivada via **HKDF-SHA256** a partir de `REQUEST_STATE_SECRET` (mínimo de 32 bytes de entropia obrigatórios na subida, nunca hardcoded). Além da autenticidade criptográfica, o envelope `v1.` do SDK vincula método (`tools/call`), nome da tool (`reservar_sala`), digest SHA-256 canônico dos `arguments` e `audience`: qualquer adulteração de 1 caractere no `requestState` ou divergência nos argumentos reenviados no retry é rejeitada automaticamente na camada de protocolo com código JSON-RPC `-32602` (`INVALID_PARAMS`). Como todo o contexto necessário viaja selado no próprio `requestState`, o servidor MCP não guarda estado de pausa em memória e aceita normalmente o retry mesmo após reinicialização do processo.
- **Tempo de validade (TTL) do `requestState`**: configurado em **`900` segundos (15 minutos)** por meio de `TTL_REQUEST_STATE_SEGUNDOS = 900.0` em `RequestStateSecurity(keys=[segredo], ttl=900.0)` (`servidor-mcp/servidor.py`, linha 43), dentro da janela exigida de 5 a 30 minutos; estados expirados também são recusados com `-32602`.
- **Onde fica guardado o estado das Tasks**: dividido em duas estruturas em memória no processo do agente (`agente/`), isoladas por `task_id`:
  1. `InMemoryTaskStore` do SDK oficial `a2a-sdk==1.3.0` (`agente/servidor.py`, linha 109), responsável por armazenar o ciclo de vida público das Tasks A2A (`id`, `contextId`, transições `TASK_STATE_SUBMITTED` → `TASK_STATE_WORKING` → `TASK_STATE_INPUT_REQUIRED` / `TASK_STATE_COMPLETED` / `TASK_STATE_CANCELED` / `TASK_STATE_FAILED`, `history` e `artifacts`) e recusar novas mensagens em Tasks terminais.
  2. `ExecutorPonteSalas._pausas_por_task: dict[str, EstadoPausaMRTR]` (`agente/ponte.py`, linha 121), um dicionário privado em memória indexado por `task_id` que associa cada Task pausada ao seu `requestState` opaco, aos argumentos originais, à chave de `inputRequests` e à lista de alternativas. Manter esse vínculo fora do `TaskStore` garante que duas Tasks pausadas simultaneamente nunca troquem de `requestState` e que o `requestState` jamais apareça em `GetTask`, `SendMessage`, `artifacts` ou no Agent Card.

## Saída do validador

```text
trace-id desta execucao: 2d135702d7cb15b477ce68d1414d21ef
procure esse valor no stderr do servidor MCP para conferir a propagacao do traceparent.

PASS 01 tools/list traz as tres tools
PASS 02 toda tool tem inputSchema de objeto
PASS 03 listar_salas devolve structuredContent e o mesmo JSON em texto
PASS 04 _meta sem protocolVersion devolve -32602 e HTTP 400
PASS 05 _meta sem clientCapabilities devolve -32602 e HTTP 400
PASS 06 tool inexistente e recusada, por -32602 ou por isError
PASS 07 resources/read de politica://uso devolve a politica
PASS 08 resources/read de URI inexistente devolve -32602
PASS 09 sala inexistente devolve isError com a mensagem exata
PASS 10 fora da janela devolve isError com a mensagem exata
PASS 11 duracao acima de 2h devolve isError com a mensagem exata
PASS 12 intervalo invertido devolve isError com a mensagem exata
PASS 13 conflito devolve input_required com inputRequests e requestState
PASS 14 a elicitation e form mode e oferece as alternativas na ordem certa
PASS 15 conflito sem a capability elicitation devolve -32021 e HTTP 400
PASS 16 retry com inputResponses e requestState conclui a reserva
PASS 17 requestState adulterado e rejeitado com -32602
PASS 18 argumentos adulterados no retry nao tomam efeito
PASS 19 recusa conclui sem reservar e sem isError
PASS 20 conflito sem alternativa possivel devolve isError com a mensagem exata

PASS 21 agent card responde 200 no well-known com JSON
PASS 22 o card declara a interface JSON-RPC com url e versao 1.0
PASS 23 o card declara a skill reservar-sala
PASS 24 SendMessage com sala livre conclui a Task
PASS 25 o artifact chama reserva e traz a versao da politica
PASS 26 GetTask devolve id, contextId e estado corrente
PASS 27 SendMessage com sala ocupada pausa a Task
PASS 28 a Task pausada lista as alternativas na ordem certa
PASS 29 escolha fora do enum mantem a Task pausada
PASS 30 a continuacao conclui a Task na sala escolhida
PASS 31 SendMessage em Task terminal e recusado
PASS 32 a recusa termina a Task em CANCELED
PASS 33 duas Tasks pausadas ao mesmo tempo concluem cada uma com a sua reserva
PASS 34 nenhuma resposta A2A carrega o requestState
PASS 35 sala inexistente termina a Task em FAILED com a mensagem da tool
PASS 36 o agente e deterministico: o mesmo pedido produz a mesma pausa

resumo: 36 passaram, 0 falharam, de 36 verificacoes
```
