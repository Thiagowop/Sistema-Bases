# FLUXO COMPLETO DETALHADO - AUTOMAÇÃO TABELIONATO

## Visão Geral do Processo

O sistema processa dados de duas fontes principais (MAX e Tabelionato) através de 4 etapas sequenciais:

1. **TRATAMENTO** - Remove inconsistências e padroniza dados
2. **BATIMENTO** - Identifica pendências (Tabelionato → MAX)  
3. **ENRIQUECIMENTO** - Trata duplicados e dados adicionais
4. **BAIXA** - Processo inverso (MAX → Tabelionato)

---

## 1. TRATAMENTO - REMOÇÃO DE INCONSISTÊNCIAS

### 1.1 TRATAMENTO MAX (`tratamento_max.py`)

**OBJETIVO**: Validar e limpar dados da base MAX, removendo registros inconsistentes do fluxo principal.

#### INPUTS
- **Arquivo**: `data/input/max/MaxSmart_Tabelionato.zip`
- **Formato**: CSV dentro do ZIP, separador `;`, encoding `utf-8-sig`
- **Colunas obrigatórias**: `PARCELA`, `VENCIMENTO`, `CPFCNPJ_CLIENTE`, `NUMERO_CONTRATO`, `VALOR`, `STATUS_TITULO`

#### REGRAS DE VALIDAÇÃO

##### PARCELA - Regras de Inconsistência:
1. **Vazia**: `""`, `"nan"`, `"none"`, `"null"`
2. **Contém vírgula**: qualquer string com `,`
3. **Parece data**: formato `DD/MM/AAAA` (ex: `01/01/2023`)
4. **Contém hífen**: qualquer string com `-`
5. **Muito curta**: apenas 1 dígito (ex: `"1"`)

##### VENCIMENTO - Regras de Inconsistência:
1. **Vazio**: `""`, `"nan"`, `"none"`, `"null"`, `"nat"`, valores `NaN`
2. **Ano inconsistente**: 
   - Anos < 1900
   - Anos iniciando com zero (ex: `0123-01-01`)
3. **Formato inválido**: 
   - Não consegue converter para `DD/MM/AAAA` nem `AAAA-MM-DD`
   - Datas inexistentes (ex: `31/02/2023`)

#### TRATAMENTOS APLICADOS
1. **Padronização de campos**:
   - Colunas convertidas para UPPERCASE
   - Strings com `.strip()` para remover espaços
   - Criação da coluna `CHAVE = PARCELA`

2. **Formatação monetária**:
   - Coluna `VALOR` formatada com `src/utils/formatting.formatar_moeda_serie`
   - Suporte a vírgula como separador decimal brasileiro
   - Formato final: `"1234,56"` (configurável via `CSV_DECIMAL_SEPARATOR`)

#### OUTPUTS
- **Válidos**: `data/output/max_tratada/max_tratada.zip`
- **Inconsistências**: `data/output/inconsistencias/max_inconsistencias.zip`
- **Colunas adicionais**: `MOTIVO_INCONSISTENCIA`, `VENCIMENTO_ORIGINAL` (para inconsistências)

#### MÉTRICAS REGISTRADAS
- Registros originais vs válidos vs inválidos
- Taxa de aproveitamento (%)
- Inconsistências por tipo (PARCELA/VENCIMENTO)

---

### 1.2 TRATAMENTO TABELIONATO (`tratamento_tabelionato.py`)

**OBJETIVO**: Validar e limpar dados do Tabelionato, removendo registros inconsistentes.

#### INPUTS
- **Arquivo**: `data/input/tabelionato/*.zip` (com senha `Mf4tab@`)
- **Formato**: CSV dentro do ZIP, separador `;`, encoding `utf-8`
- **Colunas principais**: `Protocolo`, `DtAnuencia`, `CpfCnpj`, `Devedor`, `Custas`, `Credor`

#### REGRAS DE VALIDAÇÃO

##### DtAnuencia - Regras de Inconsistência:
1. **Vazia/Nula**: `""`, `NaN`, `"nan"`, `"nat"`, `"none"`
2. **Formato inválido**: não consegue converter para datetime
3. **Data antiga**: anterior a `1900-01-01`

##### Quebras de Linha:
1. **Campos textuais**: qualquer coluna contendo `\r` ou `\n`
2. **Registros quebrados**: indicam corrupção de dados

#### TRATAMENTOS APLICADOS
1. **Padronização de campos**:
   - Normalização de CPF/CNPJ (apenas dígitos)
   - Criação da coluna `CHAVE = Protocolo`
   - Remoção de quebras de linha

2. **Formatação monetária**:
   - Coluna `Custas` formatada com `src/utils/formatting.formatar_moeda_serie`
   - Normalização de valores brasileiros (`1.234,56` → `1234.56`)
   - Suporte a formatos mistos (vírgula e ponto)

3. **Atribuição de campanha**:
   - **Campanha 58**: `AGING ≤ 1800` dias
   - **Campanha 78**: `AGING > 1800` dias
   - **Regra especial**: Protocolos com aging misto (parcelas em ambas as faixas) vão integralmente para Campanha 58
   - Cálculo baseado em `DtAnuencia` vs data atual

#### OUTPUTS
- **Válidos**: `data/output/tabelionato_tratada/tabelionato_tratado.zip`
- **Inconsistências**: `data/output/tabelionato_tratada/tabelionato_inconsistencias.zip`
- **Coluna adicional**: `Motivo` (para inconsistências)

---

## 2. BATIMENTO - IDENTIFICAÇÃO DE PENDÊNCIAS

### OBJETIVO
Identificar protocolos que estão no Tabelionato mas **NÃO** estão na MAX (LEFT ANTI-JOIN).

#### INPUTS
- **Tabelionato tratado**: `data/output/tabelionato_tratada/tabelionato_tratado.zip`
- **MAX tratado**: `data/output/max_tratada/max_tratada.zip`
- **Coluna de matching**: `CHAVE` (comum em ambas as bases)

#### LÓGICA DE BATIMENTO

##### 1. Anti-Join (Tabelionato - MAX):
```python
# Pseudocódigo
chaves_max = set(df_max['CHAVE'])
mask = ~df_tabelionato['CHAVE'].isin(chaves_max)
pendencias = df_tabelionato[mask]
```

##### 2. Tratamento de Duplicados:
**Regra de Priorização por Documento**:
- **CNPJ** (18 dígitos): Prioridade 0 (maior)
- **CPF** (14 dígitos): Prioridade 1 (menor)
- **Outros**: Prioridade 2 (menor ainda)

**Processo**:
1. Ordenar por: `CHAVE`, `PRIORIDADE_DOCUMENTO`, `DtAnuencia` (desc)
2. Manter primeiro registro de cada `CHAVE` (principal)
3. Demais registros vão para **enriquecimento**

##### 3. Separação por Campanha:
- **Campanha 58**: `AGING ≤ 1800` dias
- **Campanha 78**: `AGING > 1800` dias
- **Regra especial**: Protocolos com aging misto ficam integralmente na Campanha 58

#### OUTPUTS
- **Batimento Campanha 58**: `data/output/batimento/batimento_campanha58.zip`
- **Batimento Campanha 78**: `data/output/batimento/batimento_campanha78.zip`
- **Enriquecimento**: `data/output/batimento/tabela_enriquecimento.zip`

#### LAYOUT DE SAÍDA (Mapeamento de Colunas):
```
Protocolo → NUMERO CONTRATO
Devedor → NOME / RAZAO SOCIAL  
DtAnuencia → VENCIMENTO
CpfCnpj → CPFCNPJ CLIENTE
Custas → VALOR
Credor → OBSERVACAO CONTRATO
```

---

## 3. ENRIQUECIMENTO - TRATAMENTO DE DUPLICADOS

### OBJETIVO
Processar registros duplicados identificados durante o batimento para permitir contato com todos os envolvidos no protocolo.

#### INPUTS
- **Fonte**: Registros duplicados do batimento (mesmo protocolo, documentos diferentes)

#### REGRAS DE ENRIQUECIMENTO

##### Tipos de Enriquecimento:
1. **CNPJ_DUPLICADO**: Quando há múltiplos CNPJs no mesmo protocolo
2. **CPF_ADICIONAL**: CPFs secundários quando há CNPJ principal
3. **DOCUMENTO_ADICIONAL**: Outros tipos de documento

##### Campos Adicionais:
- **PROTOCOLO_REFERENCIA**: Protocolo original
- **DOCUMENTO_REFERENCIA**: Documento do registro principal
- **TIPO_ENRIQUECIMENTO**: Classificação do tipo

#### OUTPUTS
- **Arquivo**: `data/output/batimento/tabela_enriquecimento.zip`
- **Objetivo**: Permitir contato com todos os CPFs/CNPJs envolvidos

---

## 4. BAIXA - PROCESSO INVERSO

### OBJETIVO
Identificar protocolos que estão na MAX mas **NÃO** estão no Tabelionato (processo inverso do batimento).

#### INPUTS
- **MAX tratado**: Filtrado por `STATUS_TITULO = 'EM ABERTO'`
- **Tabelionato tratado**: Base completa
- **Custas** (opcional): `data/input/baixa/*.zip` para enriquecimento

#### LÓGICA DE BAIXA

##### 1. Filtros Aplicados na MAX:
- **Status**: `STATUS_TITULO = 'EM ABERTO'`
- **Campanhas específicas**: Conforme configuração

##### 2. Anti-Join (MAX - Tabelionato):
```python
# Pseudocódigo
chaves_tabelionato = set(df_tabelionato['CHAVE'])
chaves_max = set(df_max['CHAVE'])
chaves_diferenca = chaves_max - chaves_tabelionato
df_baixa = df_max[df_max['CHAVE'].isin(chaves_diferenca)]
```

##### 3. Enriquecimento com Custas:
- **Merge** com base de custas usando `CHAVE = Protocolo_Tratado`
- **Campos adicionados**: `DATA_RECEBIMENTO`, `VALOR_RECEBIDO`

##### 4. Separação por Recebimento:
- **Com recebimento**: Registros com `DATA_RECEBIMENTO` preenchida
- **Sem recebimento**: Registros sem data de recebimento

#### OUTPUTS
- **Com recebimento**: `data/output/baixa/baixa_com_recebimento_YYYYMMDD_HHMMSS.csv`
- **Sem recebimento**: `data/output/baixa/baixa_sem_recebimento_YYYYMMDD_HHMMSS.csv`
- **Arquivo final**: `data/output/baixa/baixa_tabelionato_YYYYMMDD_HHMMSS.zip`

---

## FLUXO DE DADOS COMPLETO

```
[Extração] → [Tratamento] → [Batimento] → [Enriquecimento]
                ↓              ↓              ↓
         [Inconsistências] [Pendências]  [Duplicados]
                                ↓
                           [Baixa] ← [Custas]
                              ↓
                    [Com/Sem Recebimento]
```

## VALIDAÇÕES E CONTROLES

### Controles de Qualidade:
1. **Contagem de registros** em cada etapa
2. **Taxa de aproveitamento** (válidos/total)
3. **Validação de chaves** ausentes/presentes
4. **Logs detalhados** em `data/logs/tabelionato.log`

### Tratamento de Erros:
1. **Arquivos ausentes**: Erro explícito com caminho esperado
2. **Colunas obrigatórias**: Verificação antes do processamento
3. **Formatos inválidos**: Separação em arquivos de inconsistência
4. **Encoding**: Suporte a `utf-8-sig` e fallbacks

### Configurações:
- **Separador decimal**: `CSV_DECIMAL_SEPARATOR` (padrão: vírgula)
- **Encoding**: `utf-8-sig` para MAX, `utf-8` para Tabelionato
- **Senhas ZIP**: `Mf4tab@` para arquivos do Tabelionato
- **Campanhas**: Configuráveis por aging (1800 dias)

---

## RESUMO DE ARQUIVOS GERADOS

| Etapa | Arquivo | Conteúdo | Localização |
|-------|---------|----------|-------------|
| Tratamento MAX | `max_tratada.zip` | Registros válidos da MAX | `data/output/max_tratada/` |
| Tratamento MAX | `max_inconsistencias.zip` | Registros inválidos da MAX | `data/output/inconsistencias/` |
| Tratamento Tabelionato | `tabelionato_tratado.zip` | Registros válidos do Tabelionato | `data/output/tabelionato_tratada/` |
| Tratamento Tabelionato | `tabelionato_inconsistencias.zip` | Registros inválidos do Tabelionato | `data/output/tabelionato_tratada/` |
| Batimento | `batimento_campanha58.zip` | Pendências aging > 1800 dias | `data/output/batimento/` |
| Batimento | `batimento_campanha78.zip` | Pendências aging ≤ 1800 dias | `data/output/batimento/` |
| Enriquecimento | `tabela_enriquecimento.zip` | Registros duplicados para contato | `data/output/batimento/` |
| Baixa | `baixa_tabelionato_*.zip` | Protocolos MAX sem retorno Tabelionato | `data/output/baixa/` |

Este documento fornece uma visão completa e detalhada de todo o processo, permitindo identificação precisa de erros e compreensão total do fluxo por pessoas externas ao projeto.
