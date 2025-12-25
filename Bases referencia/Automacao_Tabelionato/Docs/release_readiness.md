# Avaliacao de Prontidao para Entrega

## Visao Geral
- Orquestracao centralizada via `scripts/fluxo_completo.py`, que garante a criacao do `.venv`, instala dependencias faltantes e reexecuta o pipeline dentro do ambiente isolado antes de acionar as seis etapas principais (extracao, tratamentos, batimento e baixa).
- O menu `run_tabelionato.bat` cobre instalacao, execucao completa e rotinas isoladas; quando chamado fora do menu, lembre-se de envolver o caminho com aspas (`cmd /c ""C:\...\run_tabelionato.bat" 2"`).
- Desde novembro/2025 todos os scripts expõem apenas blocos-resumo no console (helpers em `src/utils/console.py`), mantendo as mensagens detalhadas em `data/logs/tabelionato.log`.

## Portabilidade do Ambiente
- O helper PowerShell (`bin/python/bootstrap_portable_python.ps1`) baixa e configura o Python embed 3.11, adicionando `pip` e `site-packages`, eliminando a necessidade de instalacao previa.
- `src/utils/archives.ensure_7zip_ready` verifica diferentes instalacoes locais (`bin\7z.exe`, `bin\7_zip_rar\7z.exe`, `bin\7_zip_rar\7-Zip\7z.exe`) antes de extrair `7_zip_rar.zip`, evitando reprocessamento desnecessario.
- As conexoes com SQL Server fazem fail-fast para variaveis obrigatorias (.env) e tentam drivers modernos (18/17) com retrocompatibilidade.

## Cobertura de Dependencias e Artefatos
- `requirements.txt` fixa faixas de versao para dependencias criticas (pandas/numpy) e inclui utilitarios adicionais (py7zr, pyautogui).
- A documentacao operacional (`Docs/README.md`, `Docs/Fluxo_atual.md` e `Docs/FLUXO_DETALHADO_COMPLETO.md`) cobre estrutura de diretorios, requisitos, fluxo completo e novas diretrizes de execucao silenciosa.

## Itens Verificados
- [x] Instrucao clara de preparacao do ambiente (.venv, credenciais, requisitos externos).
- [x] Mecanismo de bootstrap de Python portavel funcional e documentado.
- [x] Validacao automatica de dependencias antes de iniciar o fluxo.
- [x] Saidas de console consolidadas em blocos `[STEP]`, com logs completos em `data/logs/tabelionato.log`.

## Riscos/Atencoes antes da Entrega
- O projeto permanece direcionado ao Windows (bat/PowerShell, pyodbc, pyautogui); execucao completa em Linux requer adaptacoes (uso parcial via WSL).
- `pyodbc` depende do Microsoft ODBC Driver 18 (ou 17); confirmar instalacao conforme instrucoes.
- A automacao de comparacao (`tests/teste_procv.py`) requer estacao com desktop (pyautogui), podendo falhar em servidores headless.

## Testes Executados
- `python -m compileall src`
- `run_tabelionato.bat 2` (fluxo completo pós-refatoracoes de saida e 7-Zip)

