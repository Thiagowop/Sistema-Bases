# EMCCAMP Pipeline Isolado

Pipeline completo para extracao, tratamento, batimento, baixa e enriquecimento de contato da carteira EMCCAMP/TOTVS.  
Com a reestruturacao recente, o codigo foi simplificado (sem diretorios redundantes) e pode ser executado em qualquer ambiente com Python 3.10+.

---

## Sumario

1. [Visao Geral](#visao-geral)
2. [Requisitos e Preparacao](#requisitos-e-preparacao)
3. [Configuracao do Ambiente](#configuracao-do-ambiente)
4. [Como Executar](#como-executar)
5. [Arquitetura do Codigo](#arquitetura-do-codigo)
6. [Estrutura de Diretorios](#estrutura-de-diretorios)
7. [Logs e Troubleshooting](#logs-e-troubleshooting)
8. [Documentacao Complementar](#documentacao-complementar)

---

## Visao Geral

- **Objetivo**: gerar, de forma independente, todas as saidas necessarias (tratamento, batimento, baixa) da carteira EMCCAMP.
- **Destaques**:
  - `main.py` expoe uma CLI unica (`extract`, `treat`, `batimento`, `baixa`).
  - `src/pipeline.py` centraliza a orquestracao.
  - `PathManager` e `DatasetIO` (`src/utils/path_manager.py`, `src/utils/io.py`) garantem portabilidade e evitam caminhos hardcoded.
  - Processadores (`src/processors/*.py`) concentram apenas logica de negocio.
  - Integracoes TOTVS residem em `src/utils/totvs_client.py`.

> Para fluxogramas completos e regras detalhadas, veja [`docs/fluxo_completo.md`](docs/fluxo_completo.md).

---

## Requisitos e Preparacao

| Item | Descricao |
|------|-----------|
| Python | 3.10 ou superior. |
| TOTVS | Credenciais para as APIs `CANDIOTTO.001` (titulos) e `CANDIOTTO.002` (baixas). Veja as variaveis `EMCCAMP_API_URL/USER/PASSWORD` e `TOTVS_BASE_URL/USER/PASS`. |
| Periodo | Datas de filtro para as consultas TOTVS (`EMCCAMP_DATA_VENCIMENTO_INICIAL`, `EMCCAMP_DATA_VENCIMENTO_FINAL=AUTO` opcional) e SQL (`MAX_DATA_VENCIMENTO_*`). |
| SQL Server | Credenciais do banco (variaveis `MSSQL_*`). |
| Dependencias | `pip install -r requirements.txt` (automatizado pelo setup). |

Sugestao de `.env`:

```
MSSQL_SERVER_STD=...
MSSQL_DATABASE_STD=...
MSSQL_USER_STD=...
MSSQL_PASSWORD_STD=...
EMCCAMP_API_URL=https://totvs.emccamp.com.br:8051/api/framework/v1/consultaSQLServer/RealizaConsulta/CANDIOTTO.001/0/X
EMCCAMP_API_USER=...
EMCCAMP_API_PASSWORD=...
TOTVS_BASE_URL=https://totvs.emccamp.com.br:8051
TOTVS_USER=...
TOTVS_PASS=...
EMCCAMP_DATA_VENCIMENTO_INICIAL=2023-01-01
EMCCAMP_DATA_VENCIMENTO_FINAL=AUTO  # usa hoje-6; defina data fixa se precisar
MAX_DATA_VENCIMENTO_INICIAL=2023-01-01
MAX_DATA_VENCIMENTO_FINAL=2023-12-31
```

---

## Configuracao do Ambiente

1. Clone ou extraia o repositorio.
2. Copie `.env.example` para `.env` e preencha as credenciais.
3. Execute o setup (cria `venv`, instala dependencias, cria `.env` se necessario):

   ```bat
   C:\> cd projeto_emccamp
   C:\projeto_emccamp> setup_project_emccamp.bat
   ```

   > Linux/macOS: `python -m venv venv && source venv/bin/activate && pip install -r requirements.txt`

---

## Como Executar

### CLI (recomendado)

```bash
# Extracoes
python main.py extract all
python main.py extract emccamp
python main.py extract baixa
python main.py extract max
python main.py extract judicial
python main.py extract doublecheck

# Tratamentos
python main.py treat emccamp
python main.py treat max
python main.py treat all

# Processos finais
python main.py batimento
python main.py baixa
python main.py enriquecimento
```
> O comando de enriquecimento utiliza `Emccamp.zip` filtrando pelas chaves geradas no batimento (títulos não encontrados na MAX) para montar a planilha de contatos.

### Scripts BAT (Windows)

**`run_pipeline_emccamp.bat`** - Menu interativo com 8 opcoes:
1. Pipeline completo (extrair → tratar → batimento → baixa → enriquecimento)
2. Extrair todas as bases
3. Pipeline sem extracao (tratar → batimento → baixa → enriquecimento)
4. Tratamento completo (EMCCAMP + MAX)
5. Somente Batimento
6. Somente Baixa
7. Somente Enriquecimento
8. Sair

**Modo Run-Once (sem loop):**
```bat
.\run_pipeline_emccamp.bat 1    REM Pipeline completo
.\run_pipeline_emccamp.bat 2    REM Extrair todas as bases
.\run_pipeline_emccamp.bat 3    REM Pipeline sem extracao
```

**`run_completo_emccamp.bat`** - Executa pipeline completo automaticamente com log em `data/logs/execucao_emccamp.log`.

---

## Arquitetura do Codigo

- `src/pipeline.py`: orquestrador de alto nivel (metodos `extract_*`, `treat_*`, `batimento`, `baixa`).
- `src/utils/path_manager.py`: resolve/cria diretorios, expoe `cleanup` e seleciona a pasta de logs.
- `src/utils/io.py`: funcoes de leitura/escrita (CSV/ZIP) e a classe `DatasetIO`.
- `src/utils/totvs_client.py`: integracoes com a API TOTVS (EMCCAMP + Baixas).
- `src/processors/`: logica de negocio de cada etapa (tratamento, batimento, baixa, enriquecimento).
- `src/scripts/`: extracoes SQL/TOTVS reutilizadas pelo pipeline (mantidos para execucoes pontuais).
- `main.py`: CLI enxuto que instancia `Pipeline` e roteia os comandos.

---

## Estrutura de Diretorios

```
projeto_emccamp/
+-- data/
|   +-- input/            # Arquivos extraidos (EMCCAMP, MAX, baixas, judicial, acordos)
|   +-- output/           # Saidas tratadas, batimento, baixa, enriquecimento
+-- docs/
|   +-- fluxo_completo.md # Fluxograma completo do processo
|   +-- mapa_arquivos.md  # Funcao de cada arquivo/pacote
+-- src/
|   +-- pipeline.py       # Orquestrador
|   +-- processors/       # Emccamp, Max, Batimento, Baixa
|   +-- scripts/          # Extracoes (API/SQL)
|   +-- utils/            # Helpers (logger, IO, PathManager, TOTVS, anti-join, SQL, etc.)
+-- main.py
+-- run_pipeline_emccamp.bat
+-- run_completo_emccamp.bat
+-- setup_project_emccamp.bat
+-- requirements.txt
```

---

## Formato de Saida Unificado

Todas as etapas do pipeline usam formatacao padronizada de 80 caracteres com secoes claras:

```
================================================================================
EXTRACAO EMCCAMP (API TOTVS)
================================================================================

>>> Fluxo de execucao
  - Conexao com API TOTVS
  - Download de 19,986 registros
  - Conversao para DataFrame
  - Salvamento em Emccamp.zip

>>> Resultado
  Registros extraidos: 19,986
  Tempo de execucao: 78.63 segundos

>>> Arquivo gerado
  Local: C:\...\data\input\emccamp\Emccamp.zip
================================================================================
```

## Logs e Troubleshooting

| Arquivo | Descricao |
|---------|-----------|
| `data/logs/execucao_emccamp.log` | Log unico do pipeline - contem timestamp, etapa e status (OK/ERRO) de cada operacao. |

Problemas comuns:

1. **API TOTVS 401** - confira `TOTVS_USER`/`TOTVS_PASS` e se `TOTVS_BASE_URL` aponta para `/api/framework/...`.
2. **Erro de conexao SQL** - verifique VPN ativa e variaveis `MSSQL_*`.
3. **Nenhum registro na baixa** - revise `src/config/config.yaml` (campanhas/status) e se as bases tratadas estao atualizadas.
4. **CSV com caracteres estranhos** - arquivos sao `utf-8-sig` com separador `;`; ajuste importacao no Excel.

---

## Documentacao Complementar

- [`docs/fluxo_completo.md`](docs/fluxo_completo.md): fluxogramas, filtros, arquivos produzidos e objetivos de cada etapa.
- [`docs/mapa_arquivos.md`](docs/mapa_arquivos.md): funcoes de cada arquivo/pacote para localizar rapidamente onde investigar erros.
 - [`docs/manual.md`](docs/manual.md): manual conceitual e operacional para replicar o processo (logica, ordem, filtros e criterios), sem depender de nomes de arquivos.

Com isso, voce tem uma visao clara sobre o que o pipeline faz, como faz e quais artefatos esperar em cada etapa.

---

## Checklist Operacional (resumo)

Antes de rodar:
- [ ] Rede/VPN disponivel e credenciais validas (APIs TOTVS e SQL Server).
- [ ] Periodo de vencimento configurado (inicio obrigatorio; fim opcional) nas variaveis de ambiente.
- [ ] Espaco em disco suficiente para o ciclo.

Execucao (ordem):
1) Extracoes - objetivos e parametros: docs/manual.md#extracoes
2) Tratamentos - normalizacao/qualidade: docs/manual.md#tratamentos
3) Batimento - logica e filtros: docs/manual.md#batimento
4) Baixa - filtros, PROCV e splits: docs/manual.md#baixa
5) Enriquecimento de contato: docs/manual.md#enriquecimento
6) Runbook e validacoes finais: docs/manual.md#runbook e docs/manual.md#aceite

Se precisar solucionar problemas:
- Consultar docs/manual.md#troubleshooting (causas provaveis, checagens e acoes).

Fluxo visual para apresentacoes:
- Utilize o fluxograma em docs/fluxo_completo.md (Mermaid) 





