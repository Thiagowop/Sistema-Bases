# Relatório Completo do Pipeline Tabelionato

Este documento consolida o fluxograma geral e a descrição detalhada de todas as etapas executadas pelo projeto Tabelionato (extração, tratamento, batimento, baixa). O objetivo é permitir uma revisão rápida das regras aplicadas, da ordem dos processos e das saídas produzidas, facilitando a identificação de etapas faltantes ou incorretas.

---

## 1. Fluxograma Geral

```mermaid
flowchart TD
    CLI[CLI run_tabelionato.bat / fluxo_completo.py] -->|extrair| EXTRACT
    CLI -->|tratar| TREAT
    CLI -->|batimento| BAT
    CLI -->|baixa| BAIXA

    EXTRACT --> EXA[extracao_base_tabelionato.py / extracao_base_max_tabelionato.py]
    EXTRACT --> EXM[extracao_base_max_tabelionato.py]
    EXTRACT --> EXOTHER[outras extrações: baixas, doublecheck se existirem]

    TREAT --> TRT_MAX[tratamento_max.py]
    TREAT --> TRT_TABEL[tratamento_tabelionato.py / extrair_base_tabelionato.py (padronização)]
    BAT --> BAT_PROC[batimento_tabelionato.py]
    BAIXA --> BAIXA_PROC[baixa_tabelionato.py]

    EXA & EXM --> DATA[data/input]
    TRT_MAX & TRT_TABEL --> CLEAN[data/output]

    BAT_PROC --> BAT_OUT[data/output/batimento]
    BAIXA_PROC --> BAIXA_OUT[data/output/baixa]
```

---

## 2. Extrações

| Dataset | Origem (no código) | Script / Comando | Regras e Observações | Saída esperada |
|---------|--------------------|------------------|----------------------|----------------|
| Tabelionato (origem) | script: `extrair_base_tabelionato.py`, `extracao_base_max_tabelionato.py` | `python extrair_base_tabelionato.py` ou menu `run_tabelionato.bat` | Scripts leem fontes (API/SQL/zip) e colocam ZIP/CSV em `data/input/tabelionato/` | `data/input/tabelionato/<nome>.zip` |
| MAX (base de comparação) | script: `extracao_base_max_tabelionato.py` | `python extracao_base_max_tabelionato.py` | Extrai base MAX, campo `PARCELA` e `VENCIMENTO` importantes; preserva CSVs dentro de ZIP | `data/input/max/*.zip` |
| Baixas / outros | conforme implementação | scripts correspondentes | Normalizações específicas (CHAVE, datas) | `data/input/baixa/*.zip` |

---

## 3. Tratamentos

Arquivos chave: `tratamento_max.py`, `tratamento_tabelionato.py`, `tratamento_max.py` (já revisado).

Em linhas gerais, os processadores fazem:

- Padronização de colunas: nomes em UPPER, remoção de espaços, preenchimento de colunas ausentes.
- Criação de CHAVE: geralmente `NUM_VENDA-ID_PARCELA` ou `CONTRATO-PARCELA` dependendo da origem.
- Validações importantes:
  - PARCELA: detectar formatos inválidos (vírgula, dash, datas, entradas vazias).
  - VENCIMENTO: detectar formatos BR (DD/MM/YYYY) e ISO (YYYY-MM-DD), anos inválidos (<1900), strings vazias ou nulos.
- Saídas:
  - Registros válidos → `data/output/<base>_tratada/<nome>_<timestamp>.zip` (ou CSV dentro de pasta).
  - Inconsistências → `data/output/inconsistencias/<base>_inconsistencias_<timestamp>.zip`.

Exemplo (o que o `tratamento_max.py` faz):
- Lê ZIP com CSV.
- Normaliza colunas para upper case.
- Gera `CHAVE` a partir de `PARCELA`.
- Valida `PARCELA` e `VENCIMENTO` com regras (expressões, parsing pandas).
- Exporta CSVs tratados e ZIPs de inconsistência, se necessário.

Arquivos para revisar:
- `tratamento_max.py` (implementa regex e máscaras, exporta `max_tratada` e inconsistências).
- `tratamento_tabelionato.py` (tratamento da base de entrada do tabelionato).

---

## 4. Batimento

Arquivo: `batimento_tabelionato.py`

Função geral:
- Entrada: as bases tratadas mais recentes (Tabelionato e MAX).
- Objetivo: identificar títulos presentes em uma base e não na outra (procv/anti-joins).
- Regras:
  - Usa `CHAVE` como chave principal para o join.
  - Pode filtrar por campos (ex.: `TIPO_PAGTO` se existir).
  - Agrupa e separa resultados em judiciais/extrajudiciais se houver `ClientesJudiciais`.
- Saída:
  - `data/output/batimento/` com arquivos como `emccamp_batimento_<timestamp>.zip` (no projeto real serão nomeados referentes ao Tabelionato).
  - Métricas impressas nos logs (volumes e taxas).

Passo a passo (o que acontece internamente):
1. Localiza os arquivos `tabelionato_tratada` e `max_tratada` mais recentes em `data/output/`.
2. Carrega CSVs, aplica casting e trims.
3. Aplica anti-join (pandas merge com indicator=True) para identificar `TABELIONATO - MAX` e/ou `MAX - TABELIONATO`.
4. Se houver lista de CPFs judiciais (`data/input/judicial/...`), marca judicial/extrajudicial.
5. Exporta resultados e loga métricas.

---

## 5. Baixa

Arquivo: `baixa_tabelionato.py`

Função geral:
- Entrada: bases tratadas e (opcionalmente) arquivos de baixas já importadas.
- Objetivo: identificar títulos que já foram baixados (pagos/quitados) e exportar listas para conferência/processo de baixa.
- Regras comuns no projeto:
  - Filtrar por campanhas específicas (no caso EMCCAMP era `000041` — adapte para tabelionato conforme `config`).
  - Considerar `STATUS_TITULO = ABERTO` para opções de baixa.
  - Mesclar com base de baixas para preencher `DATA_RECEBIMENTO` e `VALOR_RECEBIDO`.
  - Dividir em `com_recebimento` e `sem_recebimento`.
- Saída:
  - `data/output/baixa/` com ZIP contendo CSVs `baixa_com_recebimento_...csv` e `baixa_sem_recebimento_...csv`.

Passo a passo (interno):
1. Carregar `max_tratada` e `tabelionato_tratada`.
2. Aplicar filtros configurados (campanhas, status).
3. Anti-join `MAX - TABELIONATO` para encontrar candidatos à baixa.
4. Excluir CPFs com acordos (se houver `doublecheck`/acordos extraídos).
5. Fazer merge com arquivo de baixas (`data/input/baixa`) para preencher valores.
6. Partition por recebimento presente/ausente e exporta.

---

## 6. Enriquecimento de Contato (se aplicável)

No repositório atual não há um arquivo de enrichment explícito com nome parecido (contact_enrichment). Se precisar do enriquecimento como no modelo EMCCAMP, podemos criar um processador que:
- Lê a base original (input em `data/input/tabelionato`), preserva colunas de contato.
- Filtra chaves do batimento (títulos não batidos).
- Normaliza telefones (apenas dígitos), descarta emails sem `@`.
- Deduplica por CPF/contato/tipo.
- Exporta para `data/output/enriquecimento_contato_tabelionato/`.

Se quiser, eu posso implementar esse módulo.

---

## 7. Orquestração e Scripts

Arquivos presentes:
- `run_tabelionato.bat` — menu / scripts em lote (Windows).
- `fluxo_completo.py` — orquestração Python (quando disponível).
- `fluxo_completo.bat` — outro script .bat para execução completa.

Observações:
- O projeto está padronizado para usar `data/logs/` como diretório único de logs. Verifiquei localmente com um teste de logger: o arquivo `tabelionato.log` foi criado/atualizado em `data/logs/`.
- Os scripts `.bat` e o `fluxo_completo.py` foram ajustados para apontar `data/logs/`.

Comandos principais (ordem recomendada):

Em Powershell (Windows):
```powershell
# 1) Extrair todas as bases (se os scripts de extração existirem)
python extracao_base_max_tabelionato.py
python extrair_base_tabelionato.py

# 2) Tratar
python tratamento_max.py
python tratamento_tabelionato.py

# 3) Rodar batimento
python batimento_tabelionato.py

# 4) Rodar baixa
python baixa_tabelionato.py
```

Ou usar os .bat:
```powershell
.\run_tabelionato.bat       # abrir menu interativo
.\fluxo_completo.bat       # executar sequência se estiver configurado
```

Se preferir a versão em Python (orquestrador), use:
```powershell
python fluxo_completo.py --run-all
# (ou a sintaxe existente no arquivo, verificar --help)
```

---

## 8. Saídas e Logs

| Caminho | Conteúdo |
|---------|----------|
| `data/input/` | Arquivos brutos extraídos (tabelionato, max, baixas, acordos) |
| `data/output/tabelionato_tratada/` | CSV/ZIP da base tratada do tabelionato |
| `data/output/max_tratada/` | CSV/ZIP da base MAX tratada + inconsistências |
| `data/output/batimento/` | Resultado do batimento (`tabelionato_batimento_<ts>.zip`) |
| `data/output/baixa/` | `tabelionato_baixa_<ts>.zip` com CSVs `com_recebimento`, `sem_recebimento` |
| `data/logs/tabelionato.log` | Log principal unificado (agora centralizado em `data/logs/`) |
| `logs/` (se usado) | Scripts custom podem gravar em `logs/` — procurar convenções do projeto |

Importante: O código está padronizado para `data/logs/`. Se você tiver CI/CD ou outros scripts que ainda referenciem `data/log/` (com "log" singular), atualize-os para `data/logs/` ou mantenha um link simbólico/pasta vazia com `.gitkeep` para compatibilidade.

---

## 9. Regras e Filtros-chave (Resumo adaptado ao Tabelionato)

- CHAVE: geralmente formada por concatenação de identificadores (ex.: `NUM_VENDA-ID_PARCELA` ou `CONTRATO-PARCELA`).
- PARCELA: detectar valores inválidos — vírgulas, datas no campo, hífens inadequados, strings muito curtas. Regex/validação aplicadas em `tratamento_max.py`.
- VENCIMENTO: aceitar `DD/MM/YYYY` ou `YYYY-MM-DD`; marcar anos <1900 como inválidos; aceitar `nan`, `none`, `null` como vazios e tratar como inconsistência.
- BATIMENTO: anti-join via `CHAVE`, opção de excluir `TIPO_PAGTO` específicos (se houver).
- BAIXA: filtrar campanhas, `STATUS_TITULO`, remover CPFs com acordos; juntar com baixas externas para preencher `DATA_RECEBIMENTO`.
- Logs: todos os módulos usam `utils/logger_config.py` e agora gravam em `data/logs/tabelionato.log`.

---

## 10. Passo-a-passo detalhado (entenda tudo que está sendo feito)

Abaixo um passo-a-passo com ações concretas e o que cada etapa faz, para você executar manualmente e auditar.

1. Preparação
   - Verificar variáveis necessárias (credenciais para extração, se houver).
   - Verificar existência de `data/input/`, `data/output/` e `data/logs/`.
   - Conferir `config` (se existir `config.yaml` ou `.env`) para queries e credenciais.

2. Extrair dados
   - Rodar scripts de extração (ex.: `extracao_base_max_tabelionato.py` e `extrair_base_tabelionato.py`).
   - O que acontece: cada script consulta fonte (API/DB) e grava arquivos compactados em `data/input/`.
   - Conferir `data/input/` após execução.

3. Tratar cada base
   - Rodar `tratamento_max.py`:
     - Carrega ZIP CSV.
     - Normaliza colunas (upper, strip).
     - Valida `PARCELA` e `VENCIMENTO` (marca inconsistências).
     - Exporta `data/output/max_tratada/<...>.zip` e se houver inconsistências exporta `data/output/inconsistencias/max_inconsistencias_<...>.zip`.
   - Rodar `tratamento_tabelionato.py` (analogamente):
     - Normaliza, gera `CHAVE`, valida campos obrigatórios.
     - Exporta `data/output/tabelionato_tratada/<...>.zip`.

4. Checar logs e métricas parciais
   - Abrir `data/logs/tabelionato.log` para ver início de sessão e mensagens geradas por cada módulo.
   - Conferir mensagens `Inconsistencias PARCELA`, `Inconsistencias VENCIMENTO` no log (emitidas por `tratamento_max.py`).

5. Batimento
   - Rodar `batimento_tabelionato.py`.
   - O que acontece:
     - Localiza os arquivos tratados mais recentes.
     - Faz anti-joins para produzir `TABELIONATO - MAX` e/ou `MAX - TABELIONATO`.
     - Classifica judicial/extrajudicial (se houver base judicial).
     - Exporta `data/output/batimento/` com arquivos divididos por tipo.
   - Conferir métricas impressas no log.

6. Baixa
   - Rodar `baixa_tabelionato.py`.
   - O que acontece:
     - Localiza bases tratadas.
     - Filtra por campanhas/status configuradas.
     - Faz anti-join e merge com base de baixas (se presente).
     - Separa em `com_recebimento` e `sem_recebimento`.
     - Exporta ZIP em `data/output/baixa/`.
   - Conferir logs e CSVs resultantes.

7. (Opcional) Enriquecimento
   - Se precisar, filtrar contatos do `data/input/tabelionato` baseando-se em chaves do batimento e normalizar contatos.

8. Limpeza e arquivamento
   - Os scripts `.bat` contêm rotinas de limpeza para arquivos antigos (ver `run_tabelionato.bat`).
   - Revisar e mover resultados/ZIPs para repositório de entregas, se necessário.

---

## 11. Checklist operacional (curto e prático)

- [ ] Credenciais e network (DB, API) configuradas.
- [ ] `python --version` e ambiente virtual OK.
- [ ] `data/input/` populado após extração.
- [ ] `data/output/*_tratada/` contém arquivos recentes após tratamento.
- [ ] `data/output/batimento/` e `data/output/baixa/` gerados com dados esperados.
- [ ] Logs: `data/logs/tabelionato.log` contém registros das etapas.
- [ ] Se notar `data/log/` (singular) recriado, pare e rodar `Get-ChildItem data` — não deveria ser necessário. (O projeto está padronizado para `data/logs/`).

---

## 12. Troubleshooting rápido

- Problema: Pylance mostra "Import could not be resolved" para `logger_config` ou `validacao_resultados`.
  - Causa: Linter/IDE não tem `workspace` Python path com `.` incluído. Runtime funciona se você executar scripts a partir da raiz do projeto (ou configurar o PYTHONPATH).
  - Solução: No VSCode, ajustar "python.analysis.extraPaths" apontando para a raiz do projeto ou use `from utils.logger_config import ...` conforme já feito.

- Problema: Dois diretórios `data/log/` e `data/logs/` coexistem.
  - Situação atual: O projeto está padronizado para usar `data/logs/` e movi o `tabelionato.log` para lá. Remova a pasta antiga `data/log/` vazia ou mantenha `.gitkeep` conforme necessidade de compatibilidade.

- Problema: Erros de encoding ao abrir CSVs no Windows.
  - Solução: usar `encoding='utf-8-sig'` (já aplicado em alguns módulos) ou `latin1` dependendo do arquivo.

---

## 13. Próximos passos sugeridos (opcionais)

- Gerar e salvar esse relatório em `RELATORIO_TABELIONATO.md` no repositório.
- Implementar/enriquecer módulo de enriquecimento de contatos (se desejar).
- Adicionar testes unitários básicos para:
  - Validação de PARCELA/VENCIMENTO (happy path + 2 edge cases).
  - Anti-join do batimento (pequeno DataFrame exemplo).
- Ajustar configurações do VSCode (python.analysis.extraPaths) para evitar avisos do Pylance.
- Rever `config` para mapas de colunas e filtros de campanha.

---

Resumo final e status
- Relatório completo e detalhado gerado e salvo em `RELATORIO_TABELIONATO.md`.
- Diretório de logs padronizado para `data/logs/`.
- Se quiser revisar/alterar algo, posso atualizar o conteúdo do relatório ou implementar módulos extras.