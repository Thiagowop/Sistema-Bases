# Fluxo do Projeto VIC/MAX

Este documento descreve o fluxo de dados do pipeline e a lógica de cada etapa. O objetivo é facilitar o entendimento do processo e auxiliar na detecção de possíveis gargalos.

## 1. Estrutura de Diretórios
- **data/input/**: arquivos brutos (ZIP/CSV) de VIC, MAX, Judicial e blacklist.
- **data/output/**: resultados processados (VIC, MAX, devolução, batimento, inconsistências).
- **data/logs/**: arquivos de log de execução.

## 2. Extração de Bases
Quando os arquivos de entrada não estão disponíveis, o comando `python main.py --extrair-bases` executa três rotinas:
1. **VIC**: baixa arquivo via e-mail (IMAP).
2. **MAX**: extrai dados do banco SQL Server.
3. **Judicial**: obtém base judicial via banco.

Os arquivos extraídos são armazenados em `data/input/` para uso posterior.

## 3. Processamento VIC
1. **Leitura** do arquivo (`FileManager` aceita ZIP ou CSV).
2. **Normalização** de cabeçalhos e valores (`normalizar_cabecalhos`, `mapear_colunas_canonicas`, `padronizar_valores`).
3. **Filtros**:
   - `STATUS = EM ABERTO`.
   - Tipos permitidos (`PROSOLUTO`, `ITBI`, `EVOLUCAO DE OBRA`).
   - **Aging**: usa `filtrar_clientes_criticos` para manter clientes com parcelas acima do limite configurado.
   - **Blacklist**: remove clientes listados em `data/input/blacklist`.
4. **Exportação**: gera `vic_tratada_<data>.zip` e inconsistências em `data/output/vic_tratada/` e `data/output/inconsistencias/`.

## 4. Processamento MAX
1. **Leitura** do arquivo bruto.
2. **Validações** de colunas obrigatórias e chave (`PARCELA`).
3. **Exportação** das bases tratadas e inconsistências para `data/output/max_tratada/`.

## 5. Devolução (MAX − VIC)
1. **Carrega** arquivos VIC e MAX já tratados.
2. **PROCV** (`procv_max_menos_vic`): identifica parcelas presentes no MAX tratado e ausentes na VIC tratada.
3. **Formatação** para layout de devolução com CNPJ do credor e data de devolução.
4. **Exportação**: arquivo `vic_devolucao_<data>.zip` em `data/output/devolucao/`.

## 6. Batimento (VIC − MAX)
1. **Carrega** arquivos VIC e MAX tratados e a base Judicial (CPFs).
2. **PROCV** (`procv_vic_menos_max`): encontra parcelas VIC ausentes no MAX.
3. **Classificação**: separa registros em judicial/extrajudicial conforme CPF.
4. **Exportação**: gera `vic_batimento_<data>.zip` contendo dois CSVs (judicial e extrajudicial).

## 7. Orquestração
O script `main.py` coordena as etapas por meio do `PipelineOrchestrator`.
- `--pipeline-completo`: executa VIC → MAX → Devolução → Batimento.
- `--vic`, `--max`, `--devolucao`, `--batimento`: executam módulos isolados.
- O orquestrador detecta automaticamente o arquivo mais recente de entrada se nenhum caminho for fornecido.

## 8. Geração de Logs
Todos os processadores utilizam `get_logger` para registrar mensagens. Logs são gravados em `data/logs/pipeline.log`.

## 9. Considerações de Eficiência
- Os arquivos são carregados inteiramente em memória (pandas DataFrames). Para bases superiores a 1 milhão de linhas, considere processar em **lotes** ou usar um banco intermediário.
- O uso de anti‑join via conjuntos (`anti_join.py`) reduz a complexidade de busca em PROCV.
- A exportação em ZIP evita múltiplos arquivos intermediários e economiza espaço em disco.

## 10. Checagem de Aging
- Aplicada durante o processamento da VIC logo ap�s a padroniza��o de valores.
- A fun��o `filtrar_clientes_criticos` calcula o aging real (`data_refer�ncia - vencimento`), mant�m apenas os clientes com parcelas acima do limite configurado e retorna tamb�m o conjunto de documentos removidos.
- O DataFrame resultante preserva exclusivamente as colunas originais, sem colunas auxiliares extras.
- Objetivo: priorizar clientes cr�ticos para cobran�a e garantir que batimento/devolu��o operem somente sobre contratos relevantes.
## 11. Checagens de Batimento e Devolução
### 11.1 Devolução (MAX − VIC)

- Após os filtros de campanha/status, `procv_max_menos_vic` identifica parcelas presentes no MAX tratado e ausentes na VIC tratada.
- A checagem reabre `vic_devolucao_*.zip`, recompõe o DataFrame e confirma que cada `PARCELA` listada continua inexistente na `vic_tratada_*.zip` mais recente. A verificação usa o mesmo anti-join (MAX tratado − VIC tratada) para garantir que nada além do necessário foi devolvido.
- O DataFrame resultante é formatado com CNPJ do credor, data de devolução e status fixo, gerando o layout exigido para retorno ao parceiro.
- Objetivo: sinalizar títulos que precisam ser devolvidos porque não constam mais na carteira VIC tratada.

### 11.2 Batimento (VIC − MAX)
- `procv_vic_menos_max` cruza a base VIC tratada com a MAX tratada para encontrar parcelas em aberto que não foram recebidas pelo MAX.
- A checagem abre `vic_batimento_*.zip` (CSVs judicial e extrajudicial), recompõe os DataFrames e confere que cada `CHAVE`/`PARCELA` não aparece na `max_tratada_*.zip`. O anti-join (VIC tratada − MAX tratada) é reexecutado para validar que os arquivos de batimento mantêm apenas ausências reais.
- O resultado é enriquecido com a classificação judicial/extrajudicial a partir da base de CPFs judiciais e formatado em dois CSVs.
- Objetivo: apontar divergências entre VIC e MAX tratada e direcionar os times responsáveis por carteiras judiciais e extrajudiciais.

- Após os filtros de campanha/status, `procv_max_menos_vic` identifica parcelas presentes no MAX tratado e ausentes na VIC.
- O DataFrame resultante é formatado com CNPJ do credor, data de devolução e status fixo, gerando o layout exigido para retorno ao parceiro.
- Objetivo: sinalizar títulos que precisam ser devolvidos porque não constam mais na carteira VIC.

### 11.2 Batimento (VIC − MAX)
- `procv_vic_menos_max` cruza a base VIC tratada com a MAX para encontrar parcelas em aberto que não foram recebidas pelo MAX.
- O resultado é enriquecido com a classificação judicial/extrajudicial a partir da base de CPFs judiciais e formatado em dois CSVs.
- Objetivo: apontar divergências entre VIC e MAX e direcionar os times responsáveis por carteiras judiciais e extrajudiciais.



