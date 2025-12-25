# Mapa de Arquivos do Projeto

Este guia resume a funcao de cada arquivo (ou grupo de arquivos) para facilitar a navegacao e identificacao de problemas.

## Raiz do Repositorio

| Caminho | Funcao |
|--------|--------|
| `README.md` | Visao geral, requisitos, comandos de execucao, estrutura. |
| `requirements.txt` | Dependencias Python. |
| `main.py` | CLI principal; roteia comandos para o `Pipeline`. |
| `setup_project_emccamp.bat` | Cria/instala `venv`, baixa dependencias e prepara `.env`. |
| `run_pipeline_emccamp.bat` | Menu interativo (1-8) com modo run-once; inclui enriquecimento. |
| `run_completo_emccamp.bat` | Fluxo completo automatizado (7 etapas) com logs e `pause` final. |
| `data/` | Entradas extraidas (input) e saidas/relatorios (output). |
| `docs/fluxo_completo.md` | Detalhamento de cada etapa, filtros e artefatos. |
| `docs/mapa_arquivos.md` | Este guia. |

## Diretorio `src/`

| Caminho | Funcao |
|---------|--------|
| `pipeline.py` | Orquestrador de alto nivel; expoe `extract_*`, `treat_*`, `batimento`, `baixa`, `enriquecimento`. |
| `src/config/config.yaml` | Configuracoes globais: paths, flags, filtros, queries. |
| `config/loader.py` | Carrega o YAML (`ConfigLoader`) e fornece `LoadedConfig`. |
| `processors/emccamp.py` | Tratamento da base EMCCAMP (renomeio, validacao, export). |
| `processors/max.py` | Tratamento da base MAX. |
| `processors/batimento.py` | Anti-join EMCCAMP - MAX, deduplicacao e split judicial/extrajudicial. |
| `processors/baixa.py` | Anti-join MAX - EMCCAMP, filtro de acordos, PROCV com baixas e geracao de layouts. |
| `processors/contact_enrichment.py` | Normaliza telefones/e-mails e gera layout de enriquecimento filtrando chaves do batimento conforme configuracao. |
| `processors/__init__.py` | Facilita importacoes (aliases) incluindo ContactEnrichment. |
| `scripts/extrair_emccamp.py` | Extracao via API TOTVS CANDIOTTO.001 (usa `totvs_client`). |
| `scripts/extrair_baixa_emccamp.py` | Extracao das baixas via TOTVS CANDIOTTO.002. |
| `scripts/extrair_basemax.py` | Consulta SQL para base MAX (via `queries.py`/`sql_conn.py`). |
| `scripts/extrair_judicial.py` | Extrai AutoJUR + MAX Smart, combina e grava `ClientesJudiciais.zip`. |
| `scripts/extrair_doublecheck_acordo.py` | Consulta acordos ativos para bloqueio de baixa. |
| `scripts/__init__.py` | Pacote de scripts (vazio). |
| `utils/io.py` | Funcoes genericas de IO (`read_csv_or_zip`, `write_csv_to_zip`) + `DatasetIO` e `ensure_directory`. |
| `utils/path_manager.py` | Resolve diretorios de entrada/saida, aplica `cleanup`, aponta logs. |
| `utils/anti_join.py` | Funcoes de anti-join (`procv_*`) reutilizadas pelos processadores. |
| `utils/text.py` | Helpers de normalizacao (`digits_only`, `normalize_ascii_upper`). |
| `utils/sql_conn.py` | Conexoes PyODBC (TOTVS standard e AutoJUR). |
| `utils/queries.py` | Templates SQL (MAX, AutoJUR, judicial, acordos) + resolucao via `config.yaml`. |
| `utils/logger.py` | Cria loggers padronizados (console e arquivo). |
| `utils/totvs_client.py` | Chamadas HTTP as APIs TOTVS (Emccamp, Baixas). |
| `utils/output_formatter.py` | Formatacao unificada de saidas (80 chars, secoes padronizadas). |
| `utils/__init__.py` | Exporta utilitarios principais. |
| `__init__.py` | Marca `src/` como pacote. |

## Outras Referencias

| Caminho | Funcao |
|---------|--------|
| `.env` | Variaveis sensiveis (credenciais TOTVS/SQL, filtros de data). |
| `.env.example` | Exemplo base para preenchimento. |
| `.gitignore` | Arquivos ignorados pelo Git. |

Com este mapa e possivel localizar rapidamente o arquivo responsavel por cada parte do fluxo e diagnosticar falhas de execucao.
