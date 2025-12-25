# Projeto Tabelionato - Visao Geral

Este documento resume a organizacao dos principais artefatos do pipeline Tabelionato/MAX.

## Scripts principais
- **`run_tabelionato.bat`** – Menu interativo para instalacao de dependencias, execucao do fluxo completo e etapas isoladas.
- **`fluxo_completo.bat`** – Wrapper para agendamentos; garante `.venv` pronto e chama `fluxo_completo.py`.
- **`fluxo_completo.py`** – Orquestra as etapas de extracao, tratamentos, batimento e baixa.
- **`extracao_base_max_tabelionato.py`** – Consulta o banco MAX via ODBC e escreve um resumo compacto ao finalizar.
- **`extrair_base_tabelionato.py`** – Baixa anexos do e-mail corporativo, processa cobranca/custas e expõe apenas um bloco final com métricas.
- **`tratamento_max.py`** e **`tratamento_tabelionato.py`** – Limpeza e normalizacao das bases com saídas resumidas.
- **`batimento_tabelionato.py`** – Consolida pendencias/campanhas e informa arquivos gerados.
- **`baixa_tabelionato.py`** – Gera o pacote de baixa consolidado.
- **`src/utils/console.py`** – Utilitários compartilhados para formatar números/duração, silenciar logs detalhados no console e padronizar os blocos `[STEP]`.

## Diretórios
- **`bin/`** – Utilitarios embarcados (7-Zip) e helper para baixar o Python portavel (`bin/python`).
- **`data/input/`** – Arquivos brutos (MAX, cobranca, custas).
- **`data/output/`** – Resultados tratados (`max_tratada`, `tabelionato_tratada`, `batimento`, `baixa`, `inconsistencias`).
- **`data/logs/`** – Logs rotativos (`tabelionato.log`).
- **`utils/`** – Modulos compartilhados (conexao SQL, logging, utilitarios de validacao).
- **`tests/`** – Scripts auxiliares para conferencias.

## Preparacao do ambiente
1. Garanta que o ODBC Driver 18 (ou 17) esteja instalado. Consulte a secao correspondente no `README.md` raiz.
2. Execute `bin\python\ensure_portable_python.cmd` se desejar antecipar o download do Python portavel (necessita internet).
3. Rode `run_tabelionato.bat 1` para criar o `.venv` e instalar o `requirements.txt`.
4. Configure o `.env` com credenciais IMAP e SQL Server.
5. Utilize `run_tabelionato.bat 2` para o fluxo completo ou as opcoes individuais para reprocessamentos pontuais.

### Execucao silenciosa (agendadores)

Todos os scripts exibem apenas um cabeçalho `[STEP]` com contagens, caminhos de saída e duração; os detalhes completos continuam registrados em `data/logs/tabelionato.log`.  
Quando precisar acionar via `cmd.exe` (por exemplo em um agendador do Windows), lembre-se de incluir aspas envolvendo o caminho com espaços:

```cmd
cmd /c ""C:\Users\Thiago\Desktop\Versoes finais\Automacao_Tabelionato\run_tabelionato.bat" 2"
```

O `fluxo_completo.bat` aplica a mesma lógica internamente, evitando a reextração do pacote `bin\7_zip_rar.zip` quando o executável já estiver disponível em `bin\7_zip_rar\7z.exe` ou `bin\7_zip_rar\7-Zip\7z.exe`.

## Dependencias relevantes
- **Python 3.11 portavel** (baixado automaticamente no passo 2).
- **Pacotes Python**: ver `requirements.txt` (pandas, numpy, pyodbc, py7zr, pyautogui, etc.).
- **Microsoft ODBC Driver 18/17** para SQL Server.
- **7-Zip CLI** (fornecido em `bin/`).

## Logs e saidas
- Logs em `data/logs/tabelionato.log` (UTF-8).
- Saidas padronizadas em `.zip` + `.csv` dentro de `data/output/`.
- Utilize `run_tabelionato.bat 9` para limpeza rapida das pastas de saida e log antes de um novo processamento.
