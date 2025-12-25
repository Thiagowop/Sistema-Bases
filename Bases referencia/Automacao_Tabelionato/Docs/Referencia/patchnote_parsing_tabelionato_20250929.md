# Patchnote - Conversão TXT » CSV Tabelionato (29/09/2025)

## Resumo do problema
- A conversão do TXT de cobrança para CSV quebrava após a coluna Devedor.
- Valores de Endereco, Cidade, Cep, CpfCnpj, Intimado e Custas deslizavam para Credor, gerando dezenas de erros na etapa seguinte de tratamento.
- A causa era o formato híbrido do arquivo de origem: o TXT recebido via e-mail é de largura fixa (colunas alinhadas por posição), mas eventualmente recebíamos arquivos em formato CSV (;), e o parser anterior tratava apenas o segundo cenário.

## Causa-raiz
- O algoritmo antigo dividia linhas por ;, \t ou múltiplos espaços, procurando a primeira sequência de 8 dígitos para inferir o CEP e remontar as colunas.
- Em arquivos de largura fixa, os blocos de espaços foram colapsados, fazendo o CEP ser encontrado logo após Devedor. Isso deslocava todas as colunas seguintes.
- Situações com aspas, traços, múltiplas palavras ou números (ex.: "False", CPFs/CNPJs, valores monetários) se confundiam com os delimitadores heurísticos.

## Solução implementada
1. **Detecção de formato**
   - Analisa a primeira linha (cabeçalho). Se não houver ;, calcula automaticamente os offsets de cada coluna (baseado na posição dos títulos) e ativa o modo “largura fixa”.
   - Caso haja ;, reutiliza csv.reader nativo para manter compatibilidade com dumps já estruturados.

2. **Parsing determinístico por coluna**
   - Cada linha é recortada exatamente nos offsets conhecidos ou lida pelo csv.reader, eliminando inferências frágeis.
   - Normalizações mantidas para datas, CEP, moeda, booleanos e textos.

3. **Correções pós-parsing**
   - Recupera Intimado, CpfCnpj e valores de Custas que ainda tenham ficado no texto de Credor (regex).
   - Mantém métricas de linhas totais vs. processadas (agora 100% das linhas válidas).

4. **Operações auxiliares**
   - Reintrodução explícita das funções extrair_zip_com_senha e extrair_rar_com_senha, preservando o fluxo de extração para anexos ZIP/RAR.

## Arquivos alterados
- extrair_base_tabelionato.py
  - Importação adicional de csv.
  - Reescrita total de processar_arquivo_txt para suportar detecção de largura fixa, parsing confiável e pós-tratamento dos campos.
  - Restauração das helpers extrair_zip_com_senha e extrair_rar_com_senha após o novo parser.
- data/input/tabelionato/Tabelionato.zip
  - CSV regenerado com 230.195 linhas coerentes (0 descartadas).

## Indicadores pós-ajuste
- Intimado nunca mais vem vazio com “False/True” em Credor.
- CpfCnpj fica populado sempre que havia um documento no texto original.
- Nenhum valor monetário permanece em Credor.
- O CSV final permanece apenas dentro de `Tabelionato.zip`; em modo debug o arquivo intermediário também é limpo do diretório.

## Lições e próximos passos
- Sempre verificar o cabeçalho do TXT recebido: se o arquivo chegar desalinhado novamente, o log mostrará se o modo largura fixa foi ativado.
- Ao implementar novas tratativas, manter o parser determinístico antes de partir para heurísticas.
- Se o e-mail voltar a enviar apenas CSV, o código atual continua compatível; nenhuma alteração adicional é necessária.
