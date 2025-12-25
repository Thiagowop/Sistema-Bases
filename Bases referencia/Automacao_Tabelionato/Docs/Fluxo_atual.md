# Fluxo Atual - Tabelionato

Este documento descreve as etapas do pipeline apos os ajustes de outubro/2025.

## Visao geral do pipeline

1. **Extracao MAX** (`extracao_base_max_tabelionato.py`)
   - Executa a query `SQL_MAX_TABELIONATO` via `utils/sql_conn.py`.
   - Exporta `MaxSmart_Tabelionato.zip` em `data/input/max/`.

2. **Extracao de anexos de e-mail** (`extrair_base_tabelionato.py`)
   - Conecta ao servidor IMAP configurado no `.env`.
   - Localiza os anexos mais recentes de cobranca (RAR/ZIP) e custas.
   - Descompacta com o 7-Zip CLI detectado automaticamente via `src/utils/archives.py` (sem reprocessar o `.zip` quando o executável já existir em `bin\7_zip_rar\`).
   - Processa cobranca/custas e exibe apenas um bloco `[STEP]` com email, anexos, contagem de registros e arquivos gerados; os detalhes continuam no log.
   - Gera `Tabelionato.zip` e `RecebimentoCustas_YYYYMMDD_HHMMSS.zip` em `data/input`.
   - **Nota**: Valores monetários são processados sem o símbolo R$ (removido durante normalização).

3. **Tratamentos**
   - `tratamento_max.py`: normaliza campos, valida chaves e exporta `max_tratada.zip` e `max_inconsistencias.zip`.
   - `tratamento_tabelionato.py`: normaliza, calcula aging/campanhas e exporta `tabelionato_tratado.zip` e `tabelionato_inconsistencias.zip`.

4. **Batimento** (`batimento_tabelionato.py`)
   - Anti-join Tabelionato x MAX gerando pendencias principais e enriquecimento.
   - Exporta `batimento_campanha14.zip`, `batimento_campanha58.zip` e `tabela_enriquecimento.zip`.

5. **Baixa** (`baixa_tabelionato.py`)
   - Identifica protocolos da MAX ausentes no Tabelionato.
   - Enriquese com custas, exporta `baixa_tabelionato_*.zip` e apenas registra os casos no exportados em log (sem gerar `checagem_nao_exportados_*`).

## Logs

Todos os modulos utilizam `utils/logger_config.py`, gerando `data/logs/tabelionato.log` em ASCII. O arquivo é recriado se removido.

- A saída do console foi condensada em blocos `[STEP]` (helpers em `src/utils/console.py`), enquanto mensagens detalhadas permanecem no log.
- Para execuções via `cmd.exe`, utilize aspas por conta do espaço no caminho: `cmd /c ""C:\...\run_tabelionato.bat" 2"`.

## Menu em lote

`run_tabelionato.bat` oferece todas as combinacoes de execucao e aceita o numero da opcao como argumento para execucao nao interativa.

| Opcao | Script chamado |
|-------|----------------|
| 1 | Cria/atualiza `.venv` e instala dependencias com `.venv\Scripts\python.exe -m pip install -r requirements.txt` |
| 2 | `fluxo_completo.bat` (usa o Python do `.venv`; aborta se o ambiente nao estiver preparado) |
| 3 | `.venv\Scripts\python.exe extracao_base_max_tabelionato.py` |
| 4 | `.venv\Scripts\python.exe extrair_base_tabelionato.py` |
| 5 | `.venv\Scripts\python.exe tratamento_max.py` |
| 6 | `.venv\Scripts\python.exe tratamento_tabelionato.py` |
| 7 | `.venv\Scripts\python.exe batimento_tabelionato.py` |
| 8 | `.venv\Scripts\python.exe baixa_tabelionato.py` |
| 9 | Remove ZIPs de saida e logs |

## Dependencias

- Python 3.11 (baixado automaticamente pelo helper `bin/python/ensure_portable_python.cmd`)
- pandas >= 2.3.1, < 2.4.0
- numpy >= 1.26.4, < 3.0.0
- pyodbc
- python-dotenv
- py7zr (utilitario complementar)
- pyautogui (utilitario de comparacao em `tests/`)

O 7-Zip CLI (`bin/7z.exe`) acompanha o repositorio.

## Configuracao de ambiente

- Utilize o arquivo `.env_exemplo` como base para criar o `.env` com credenciais de e-mail, SQL Server e (se necessario) o caminho do 7-Zip.
- `run_tabelionato.bat 1` cria/atualiza automaticamente o ambiente virtual `.venv` e todas as etapas subsequentes sao executadas com `.venv\Scripts\python.exe`. O helper de Python portavel garante um runtime 3.11 mesmo em estacoes sem instalacao previa.
- Os diretorios `data/input/` e `data/output/` sao ignorados pelo versionamento (`.gitignore`) para evitar o envio de arquivos sensiveis ou volumosos; mantenha os ZIPs apenas nos ambientes operacionais.

## Boas praticas operacionais

- Execute `run_tabelionato.bat 9` antes de processar novos lotes, garantindo que nenhuma saida antiga interfira.
- Sempre verifique `data/logs/tabelionato.log` ao final do processo.
- Para reprocessar apenas os tratamentos/batimento/baixa, utilize `python fluxo_completo.py --skip-extraction`.
### Ferramenta auxiliar de comparacao

- Localizacao: `tests/teste_procv.py`.
- Dependencias: pandas e pyautogui (presentes no `requirements.txt`).
- Funcionalidades:
  1. Selecionar duas bases (CSV ou ZIP) e comparar chaves escolhidas pelo usuario.
  2. Salvar o resumo da comparacao em `tests/output/resultado_comparacao_*.csv`.
  3. Validar uma planilha gerada anteriormente, verificando consistencia com as bases comparadas.


