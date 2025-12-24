# DOCUMENTAÇÃO DETALHADA - SISTEMA EMCCAMP

## 📋 ÍNDICE

1. [Visão Geral do Sistema](#visão-geral-do-sistema)
2. [Etapa 1: Extração de Dados](#etapa-1-extração-de-dados)
3. [Etapa 2: Tratamento de Dados](#etapa-2-tratamento-de-dados)
4. [Etapa 3: Batimento EMCCAMP x MAX](#etapa-3-batimento-emccamp-x-max)
5. [Etapa 4: Baixas MAX - EMCCAMP](#etapa-4-baixas-max---emccamp)
6. [Etapa 5: DEVOLUCAO MAX - EMCCAMP](#etapa-5-devolucao-max---emccamp)
7. [Etapa 6: Enriquecimento de Contatos](#etapa-6-enriquecimento-de-contatos)
8. [Configurações e Parâmetros](#configurações-e-parâmetros)
9. [Estrutura de Arquivos](#estrutura-de-arquivos)

---

## 🎯 VISÃO GERAL DO SISTEMA

O Sistema EMCCAMP é uma solução de processamento de dados financeiros que realiza:

- **Extração** de dados de múltiplas fontes (APIs TOTVS, banco de dados SQL Server)
- **Tratamento** e normalização de bases de dados EMCCAMP e MAX
- **Batimento** para identificar divergências entre bases
- **Baixas** para reconciliação de registros
- **Devolução** para identificar títulos no MAX ausentes no EMCCAMP (MAX - EMCCAMP)
- **Enriquecimento** de dados de contato

### Fluxo Principal
```
[Extração] → [Tratamento] → [Batimento] → [Baixas] → [Devolução] → [Enriquecimento]
```

---

## 📥 ETAPA 1: EXTRAÇÃO DE DADOS

### 1.1 Extração EMCCAMP (API TOTVS)

**Arquivo:** `src/scripts/extrair_emccamp.py`
**Função:** `baixar_emccamp(config)`

#### Fonte de Dados
- **Origem:** API TOTVS
- **Método:** Requisições HTTP autenticadas
- **Formato:** JSON → CSV
- **Destino:** `data/input/emccamp/`

#### Processo Detalhado
1. **Autenticação:** Login na API TOTVS usando credenciais do `.env`
2. **Requisição:** Busca dados de parcelas e contratos
3. **Transformação:** Converte JSON para formato CSV
4. **Compactação:** Gera arquivo ZIP com timestamp
5. **Armazenamento:** Salva em `data/input/emccamp/emccamp_YYYYMMDD_HHMMSS.zip`

#### Campos Extraídos
- `CODCOLIGADA`: Código da coligada
- `NOME_COLIGADA`: Nome da coligada
- `CLIENTE`: Nome do cliente
- `CPF`: CPF/CNPJ do cliente
- `NUM_VENDA`: Número do contrato
- `PARCELA`: Número da parcela
- `ID_PARCELA`: ID único da parcela
- `VENCIMENTO`: Data de vencimento
- `VALOR_ORIGINAL`: Valor original da parcela
- `VALOR_ATUALIZADO`: Valor atualizado da parcela
- `TIPO_PAGTO`: Tipo de pagamento
- `NOME_EMPREENDIMENTO`: Nome do empreendimento
- `CNPJ_EMPREENDIMENTO`: CNPJ do empreendimento
- `CNPJ_CREDOR`: CNPJ do credor
- `OBSERVACAO_PARCELA`: Observações da parcela
- `ID_NEGOCIADOR`: ID do negociador

### 1.2 Extração MAX (Banco SQL Server)

**Arquivo:** `src/scripts/extrair_basemax.py`
**Função:** `extract_max_data()`

#### Fonte de Dados
- **Origem:** Banco de dados SQL Server
- **Query:** Template `max` com parâmetros configuráveis
- **Filtros:** Data de vencimento (variáveis de ambiente)
- **Destino:** `data/input/base_max/`

#### Processo Detalhado
1. **Conexão:** Estabelece conexão com SQL Server
2. **Query Parametrizada:** 
   ```sql
   -- Parâmetros do config.yaml:
   mo_cliente_id: 77398
   -- Filtros de data via variáveis de ambiente:
   MAX_DATA_VENCIMENTO_INICIAL
   MAX_DATA_VENCIMENTO_FINAL
   ```
3. **Extração:** Executa query e obtém resultados
4. **Processamento:** Aplica formatações e validações
5. **Compactação:** Gera arquivo ZIP
6. **Limpeza:** Remove arquivos antigos do diretório

#### Campos Extraídos
- `CAMPANHA`: Código da campanha
- `CREDOR`: Nome do credor
- `CNPJ_CREDOR`: CNPJ do credor
- `CPFCNPJ_CLIENTE`: CPF/CNPJ do cliente
- `NOME_RAZAO_SOCIAL`: Nome/Razão social
- `NUMERO_CONTRATO`: Número do contrato
- `EMPREENDIMENTO`: Nome do empreendimento
- `DATA_CADASTRO`: Data de cadastro
- `PARCELA`: Identificador da parcela
- `Movimentacoes_ID`: ID da movimentação
- `VENCIMENTO`: Data de vencimento
- `VALOR`: Valor da parcela
- `STATUS_TITULO`: Status do título
- `TIPO_PARCELA`: Tipo da parcela
- `DT_BAIXA`: Data da baixa (se houver)
- `RECEBIDO`: Valor recebido (se houver)

### 1.3 Extração de Baixas EMCCAMP

**Arquivo:** `src/scripts/extrair_baixa_emccamp.py`
**Função:** `baixar_baixas_emccamp(config)`

#### Fonte de Dados
- **Origem:** API TOTVS (endpoint específico para baixas)
- **Método:** Requisições HTTP autenticadas
- **Formato:** JSON → CSV
- **Destino:** `data/input/baixas/`

#### Processo Detalhado
1. **Autenticação:** Login na API TOTVS
2. **Requisição:** Busca dados de baixas/pagamentos
3. **Transformação:** Converte para CSV
4. **Armazenamento:** Salva como `baixa_emccamp.zip`

### 1.4 Extração de Dados Judiciais

**Arquivo:** `src/scripts/extrair_judicial.py`
**Query:** Template `autojur`

#### Fonte de Dados
- **Origem:** Sistema AutoJur (via SQL Server)
- **Parâmetro:** `grupo_empresarial: EMCCAMP`
- **Destino:** `data/input/judicial/`

### 1.5 Extração MaxSmart Judicial

**Arquivo:** `src/scripts/extrair_doublecheck_acordo.py`
**Query:** Template `maxsmart_judicial`

#### Fonte de Dados
- **Origem:** MaxSmart (via SQL Server)
- **Parâmetro:** `campanhas_id: 4`
- **Destino:** `data/input/doublecheck_acordo/`

---

## 🔧 ETAPA 2: TRATAMENTO DE DADOS

### 2.1 Tratamento EMCCAMP

**Arquivo:** `src/processors/emccamp.py`
**Classe:** `EmccampProcessor`

#### Processo Detalhado

##### 2.1.1 Localização do Arquivo Fonte
```python
def _resolve_source_file(self) -> Path:
    # Busca o arquivo mais recente em data/input/emccamp/
    # Prioridade: .zip mais recente, depois .csv mais recente
```

##### 2.1.2 Aplicação de Mapeamento
**Configuração:** `mappings.emccamp.rename` no `config.yaml`

| Campo Original | Campo Normalizado | Descrição |
|---|---|---|
| `CODCOLIGADA` | `COD_COLIGADA` | Código da coligada |
| `NOME_COLIGADA` | `NOME_COLIGADA` | Nome da coligada |
| `CLIENTE` | `NOME_RAZAO_SOCIAL` | Nome do cliente |
| `CPF` | `CPF_CNPJ` | CPF/CNPJ do cliente |
| `NUM_VENDA` | `CONTRATO` | Número do contrato |
| `PARCELA` | `N_PARCELA` | Número da parcela |
| `ID_PARCELA` | `PARCELA` | ID da parcela |
| `VENCIMENTO` | `DATA_VENCIMENTO` | Data de vencimento |
| `VALOR_ORIGINAL` | `VALOR_PARCELA` | Valor original |
| `VALOR_ATUALIZADO` | `VALOR_ATUALIZADO` | Valor atualizado |
| `TIPO_PAGTO` | `TIPO_PAGTO` | Tipo de pagamento |
| `NOME_EMPREENDIMENTO` | `NOME_EMPREENDIMENTO` | Nome do empreendimento |
| `CNPJ_EMPREENDIMENTO` | `CNPJ_EMPREENDIMENTO` | CNPJ do empreendimento |
| `CNPJ_CREDOR` | `CNPJ_CREDOR` | CNPJ do credor |
| `OBSERVACAO_PARCELA` | `OBSERVACAO_PARCELA` | Observações |
| `ID_NEGOCIADOR` | `ID_NEGOCIADOR` | ID do negociador |

##### 2.1.3 Criação de Chave Única
```python
def _create_key(self, df: pd.DataFrame) -> pd.DataFrame:
    # Cria chave: CONTRATO + "-" + PARCELA
    # Exemplo: "12345-001" 
    df["CHAVE"] = df["CONTRATO"].astype(str).str.strip() + "-" + df["PARCELA"].astype(str).str.strip()
```

##### 2.1.4 Validação de Dados
**Campos Obrigatórios:** (configuração `mappings.emccamp.required`)
- `CONTRATO`
- `PARCELA` 
- `DATA_VENCIMENTO`
- `VALOR_PARCELA`
- `CPF_CNPJ`

**Processo de Validação:**
1. **Verificação de Campos Obrigatórios:** Identifica registros com campos nulos/vazios
2. **Separação:** Registros válidos vs. inconsistências
3. **Limpeza de Dados:** Remove espaços em branco, padroniza formatos

##### 2.1.5 Exportação
- **Arquivo Principal:** `emccamp_tratada_YYYYMMDD_HHMMSS.zip`
- **Inconsistências:** `emccamp_inconsistencias_YYYYMMDD_HHMMSS.zip` (se houver)
- **Destino:** `data/output/emccamp_tratada/`

### 2.2 Tratamento MAX

**Arquivo:** `src/processors/max.py`
**Classe:** `MaxProcessor`

#### Processo Detalhado

##### 2.2.1 Localização do Arquivo Fonte
```python
def _resolve_source_file(self) -> Path:
    # Busca o arquivo mais recente em data/input/base_max/
    # Prioridade: .zip mais recente, depois .csv mais recente
```

##### 2.2.2 Normalização de Dados
**Configuração:** `mappings.max.rename` no `config.yaml`

| Campo Original | Campo Normalizado | Descrição |
|---|---|---|
| `CAMPANHA` | `CAMPANHA` | Código da campanha |
| `CREDOR` | `CREDOR` | Nome do credor |
| `CNPJ_CREDOR` | `CNPJ_CREDOR` | CNPJ do credor |
| `CPFCNPJ_CLIENTE` | `CPF_CNPJ` | CPF/CNPJ do cliente |
| `NOME_RAZAO_SOCIAL` | `NOME_RAZAO_SOCIAL` | Nome/Razão social |
| `NUMERO_CONTRATO` | `NUMERO_CONTRATO` | Número do contrato |
| `EMPREENDIMENTO` | `EMPREENDIMENTO` | Nome do empreendimento |
| `DATA_CADASTRO` | `DATA_CADASTRO` | Data de cadastro |
| `PARCELA` | `PARCELA` | ID da parcela |
| `Movimentacoes_ID` | `MOVIMENTACOES_ID` | ID da movimentação |
| `VENCIMENTO` | `DATA_VENCIMENTO` | Data de vencimento |
| `VALOR` | `VALOR` | Valor da parcela |
| `STATUS_TITULO` | `STATUS_TITULO` | Status do título |
| `TIPO_PARCELA` | `TIPO_PARCELA` | Tipo da parcela |

**Campos Preservados:** (configuração `mappings.max.preserve`)
- `DT_BAIXA`: Data da baixa
- `RECEBIDO`: Valor recebido

##### 2.2.3 Criação de Chave
**Configuração:** `mappings.max.key.use_parcela_as_chave: true`
```python
# A chave é o próprio campo PARCELA
df_norm["CHAVE"] = df_norm["PARCELA"].astype(str).str.strip()
```

##### 2.2.4 Validação Rigorosa
**Regex de Validação:** `mappings.max.validation`
```regex
# Formato exigido: 3+ dígitos, hífen, 2+ dígitos
# Exemplos válidos: "123-45", "12345-678"
# Exemplos inválidos: "123", "123-", "-45", "123-45-67"
```

**Processo de Validação:**
1. **Formato de Parcela:** Valida se PARCELA segue o padrão numérico exato
2. **Campos Obrigatórios:** Verifica presença de dados essenciais
3. **Consistência:** Valida tipos de dados e formatos
4. **Separação:** Registros válidos vs. inconsistências

##### 2.2.5 Exportação
- **Arquivo Principal:** `max_tratada_YYYYMMDD_HHMMSS.zip`
- **Inconsistências:** `max_inconsistencias_YYYYMMDD_HHMMSS.zip` (se houver)
- **Destino:** `data/output/max_tratada/` e `data/output/inconsistencias/`

---

## ⚖️ ETAPA 3: BATIMENTO EMCCAMP x MAX

**Arquivo:** `src/processors/batimento.py`
**Classe:** `BatimentoProcessor`

### 3.1 Objetivo
Identificar registros presentes na base EMCCAMP mas **ausentes** na base MAX (EMCCAMP - MAX).

### 3.2 Processo Detalhado

#### 3.2.1 Carregamento de Bases
```python
# Carrega arquivos tratados mais recentes
emccamp_file = self._resolve_latest_file(self.emccamp_dir, "emccamp_tratada_*.zip")
max_file = self._resolve_latest_file(self.max_dir, "max_tratada_*.zip")
```

#### 3.2.2 Preparação dos Dados
1. **Leitura:** Carrega DataFrames das bases tratadas
2. **Deduplicação MAX:** Remove duplicatas da base MAX por chave
3. **Filtros Opcionais:** Aplica filtros de tipo de pagamento (se habilitado)

**Configuração de Filtros:** `flags.filtros_batimento`
```yaml
flags:
  filtros_batimento:
    habilitar: true  # Habilita filtros
    tipos_excluir:   # Tipos de pagamento a excluir
      - PERMUTA
      - Financiamento Fixo
```

#### 3.2.3 Anti-Join (EMCCAMP - MAX)
```python
def procv_emccamp_menos_max(df_emccamp, df_max, chave_emccamp="CHAVE", chave_max="CHAVE"):
    # Identifica registros em EMCCAMP que NÃO existem em MAX
    # Retorna: registros EMCCAMP ausentes no MAX
```

**Lógica:**
1. **Comparação por Chave:** Usa campo `CHAVE` de ambas as bases
2. **Identificação:** Encontra chaves EMCCAMP não presentes em MAX
3. **Resultado:** DataFrame com registros EMCCAMP ausentes no MAX

#### 3.2.4 Classificação Judicial/Extrajudicial
```python
def _load_judicial_cpfs(self) -> set[str]:
    # Carrega lista de CPFs judiciais de data/input/judicial/
    # Retorna: conjunto de CPFs em processo judicial
```

**Processo:**
1. **Carregamento:** Lê arquivo `ClientesJudiciais.zip`
2. **Normalização:** Padroniza CPFs (apenas dígitos)
3. **Classificação:** 
   - **Judicial:** CPF presente na lista judicial
   - **Extrajudicial:** CPF não presente na lista judicial

#### 3.2.5 Formatação de Saída
**Layout Padrão:** `LAYOUT_COLS`
```python
LAYOUT_COLS = [
    "CPFCNPJ CLIENTE",      # CPF/CNPJ formatado
    "NOME / RAZAO SOCIAL",   # Nome do cliente
    "NUMERO CONTRATO",       # Número do contrato
    "PARCELA",              # ID da parcela
    "OBSERVACAO PARCELA",    # Observações
    "VENCIMENTO",           # Data de vencimento
    "VALOR",                # Valor da parcela
    "EMPREENDIMENTO",       # Nome do empreendimento
    "CNPJ EMPREENDIMENTO",  # CNPJ do empreendimento
    "TIPO PARCELA",         # Tipo da parcela
    "CNPJ CREDOR",          # CNPJ do credor
]
```

#### 3.2.6 Exportação
- **Arquivo Judicial:** `emccamp_batimento_judicial_YYYYMMDD_HHMMSS.csv`
- **Arquivo Extrajudicial:** `emccamp_batimento_extrajudicial_YYYYMMDD_HHMMSS.csv`
- **Compactação:** Ambos arquivos em `emccamp_batimento_YYYYMMDD_HHMMSS.zip`
- **Destino:** `data/output/batimento/`

### 3.3 Métricas Geradas
- **Registros EMCCAMP:** Total de registros na base EMCCAMP
- **Registros MAX:** Total de registros na base MAX
- **Registros MAX Dedup:** Registros MAX após deduplicação
- **Registros Batimento:** Total de registros ausentes no MAX
- **Judicial:** Quantidade de registros judiciais
- **Extrajudicial:** Quantidade de registros extrajudiciais

---

## 📉 ETAPA 4: BAIXAS MAX - EMCCAMP

**Arquivo:** `src/processors/baixa.py`
**Classe:** Função `executar_baixa()`

### 4.1 Objetivo
Identificar registros presentes na base MAX mas **ausentes** na base EMCCAMP (MAX - EMCCAMP) para processo de baixa/reconciliação.

### 4.2 Processo Detalhado

#### 4.2.1 Carregamento de Bases
```python
# Carrega bases tratadas mais recentes
df_emccamp = _load_latest_treated_base(paths.resolve_output("emccamp_tratada"))
df_max = _load_latest_treated_base(paths.resolve_output("max_tratada"))
```

#### 4.2.2 Aplicação de Filtros MAX
**Configuração:** `baixa.filtros.max` no `config.yaml`

##### Filtro de Campanhas
```yaml
baixa:
  filtros:
    max:
      campanhas:
        - '000041 - EMCCAMP'  # Filtra apenas esta campanha
```

**Processo:**
```python
def _apply_max_filters(df_max, config, logger):
    # Aplica filtro de campanha
    if campanhas and "CAMPANHA" in df_filtrado.columns:
        campanha_set = {str(item).strip().upper() for item in campanhas}
        df_filtrado = df_filtrado[df_filtrado["CAMPANHA"].str.upper().isin(campanha_set)]
```

##### Filtro de Status
```yaml
baixa:
  filtros:
    max:
      status_titulo:
        - ABERTO  # Filtra apenas títulos em aberto
```

#### 4.2.3 Anti-Join (MAX - EMCCAMP)
```python
def procv_max_menos_emccamp(df_max, df_emccamp, chave_max="PARCELA", chave_emccamp="CHAVE"):
    # Identifica registros em MAX que NÃO existem em EMCCAMP
    # Retorna: registros MAX ausentes no EMCCAMP
```

**Configuração de Chaves:** `baixa.chaves`
```yaml
baixa:
  chaves:
    emccamp: CHAVE    # Campo chave do EMCCAMP
    max: PARCELA      # Campo chave do MAX
```

#### 4.2.4 Filtro de Acordos
```python
def _filter_by_acordos(df_candidatos, paths, io, logger):
    # Carrega base de acordos de data/input/doublecheck_acordo/
    # Remove registros que possuem acordos ativos
```

**Processo:**
1. **Carregamento:** Lê arquivo de acordos mais recente
2. **Comparação:** Compara por CPF/CNPJ
3. **Filtro:** Remove registros com acordos ativos

#### 4.2.5 Enriquecimento com Baixas EMCCAMP
```python
def _enrich_with_baixas(df_baixa, paths, io, logger):
    # Carrega dados de baixas EMCCAMP de data/input/baixas/
    # Enriquece registros com informações de recebimento
```

**Processo:**
1. **Carregamento:** Lê arquivo `baixa_emccamp.zip`
2. **Mapeamento:** Mapeia campos de baixa para layout final
3. **Merge:** Combina dados por chave (CONTRATO-PARCELA)
4. **Classificação:** Separa registros com/sem recebimento

#### 4.2.6 Formatação de Layout Final
**Função:** `_formatar_layout(df, config)`

**Layout de Saída:**
```python
LAYOUT_BAIXA = [
    "CNPJ CREDOR",        # CNPJ do credor
    "CPF/CNPJ CLIENTE",   # CPF/CNPJ do cliente
    "NOME CLIENTE",       # Nome do cliente
    "NUMERO DOC",         # Número do documento
    "DT. VENCIMENTO",     # Data de vencimento
    "VALOR DA PARCELA",   # Valor da parcela
    "STATUS ACORDO",      # Status do acordo
    "DT. PAGAMENTO",      # Data do pagamento
    "VALOR RECEBIDO",     # Valor recebido
]
```

**Mapeamento de Campos:**
```python
mapeamento = {
    "CNPJ_CREDOR": "CNPJ CREDOR",
    "CPF_CNPJ": "CPF/CNPJ CLIENTE", 
    "NOME_RAZAO_SOCIAL": "NOME CLIENTE",
    "PARCELA": "NUMERO DOC",
    "DATA_VENCIMENTO": "DT. VENCIMENTO",
    "VALOR": "VALOR DA PARCELA",
    "STATUS_BAIXA": "STATUS ACORDO",
    "DT_BAIXA": "DT. PAGAMENTO", 
    "RECEBIDO": "VALOR RECEBIDO"
}
```

#### 4.2.7 Exportação
- **Com Recebimento:** `baixa_com_recebimento_YYYYMMDD_HHMMSS.csv`
- **Sem Recebimento:** `baixa_sem_recebimento_YYYYMMDD_HHMMSS.csv`
- **Compactação:** Ambos em `emccamp_baixa_YYYYMMDD_HHMMSS.zip`
- **Destino:** `data/output/baixa/`

### 4.3 Fluxo de Métricas
```python
flow_steps = {
    "max_original": len(df_max),
    "max_filtrado": len(df_max_filtrado), 
    "candidatos_baixa": len(df_candidatos),
    "apos_filtro_acordos": len(df_sem_acordos),
    "com_recebimento": len(df_com_receb),
    "sem_recebimento": len(df_sem_receb)
}
```

---

## 🔁 ETAPA 5: DEVOLUCAO MAX - EMCCAMP

**Arquivo:** `src/processors/devolucao.py`
**Classe:** `DevolucaoProcessor`

### 5.1 Objetivo
Identificar titulos presentes no MAX tratado e ausentes na EMCCAMP tratada (MAX - EMCCAMP), gerando arquivos no layout universal para devolucao.

### 5.2 Regras (alto nivel)
- **Regra principal (anti-join):** `MAX_tratada - EMCCAMP_tratada` usando a coluna de chave configurada (`devolucao.chaves.max` e `devolucao.chaves.emccamp`).
- **Filtros opcionais:** filtro de `STATUS_TITULO` em aberto (MAX por padrao ativo; EMCCAMP por padrao desativado) e filtro por campanha (quando configurado).
- **Remocao por baixa (opcional):** remove chaves presentes no arquivo de baixa, evitando devolver titulos ja baixados.
- **Carteiras:** divide judicial/extrajudicial via `data/input/judicial/ClientesJudiciais.zip` (quando existir).

### 5.3 Entradas e saidas
- **Entrada:** `data/output/max_tratada/max_tratada_*.zip` e `data/output/emccamp_tratada/emccamp_tratada_*.zip`
- **Saida:** `data/output/devolucao/emccamp_devolucao_YYYYMMDD_HHMMSS.zip` (com CSV geral e, quando aplicavel, judicial/extrajudicial)

Para detalhes completos (layout, configuracao, troubleshooting), ver: `docs/DEVOLUCAO.md`.

## 📞 ETAPA 6: ENRIQUECIMENTO DE CONTATOS

**Arquivo:** `src/processors/contact_enrichment.py`
**Classe:** `ContactEnrichmentProcessor`

### 6.1 Objetivo
Enriquecer dados de contato (telefones e emails) para registros identificados no batimento.

### 6.2 Processo Detalhado

#### 6.2.1 Carregamento de Base Origem
```python
# Carrega arquivo de batimento mais recente
batimento_file = self._resolve_latest_batimento_file()
```

#### 6.2.2 Geração de Contatos
**Telefones:**
```python
def _generate_phone_numbers(self, df: pd.DataFrame) -> pd.DataFrame:
    # Gera números de telefone baseados em padrões
    # Formato: (31) 9XXXX-XXXX para Minas Gerais
```

**Emails:**
```python  
def _generate_emails(self, df: pd.DataFrame) -> pd.DataFrame:
    # Gera emails baseados no nome do cliente
    # Formato: nome.sobrenome@provedor.com
```

#### 6.2.3 Deduplicação
```python
def _deduplicate_contacts(self, df: pd.DataFrame) -> pd.DataFrame:
    # Remove duplicatas por CPF/CNPJ
    # Mantém registro mais completo
```

#### 6.2.4 Layout de Saída
```python
LAYOUT_ENRIQUECIMENTO = [
    "CPFCNPJ CLIENTE",     # CPF/CNPJ do cliente
    "TELEFONE",            # Número de telefone
    "EMAIL",               # Endereço de email
    "OBSERVACAO",          # Observações
    "NOME",                # Nome do cliente
    "TELEFONE PRINCIPAL",  # Flag telefone principal
]
```

#### 6.2.5 Exportação
- **Arquivo:** `enriquecimento_contato_emccamp.csv`
- **Compactação:** `enriquecimento_contato_emccamp.zip`
- **Destino:** `data/output/enriquecimento_contato_emccamp/`

---

## ⚙️ CONFIGURAÇÕES E PARÂMETROS

### 6.1 Arquivo Principal: `src/config/config.yaml`

#### 6.1.1 Configurações Globais
```yaml
global:
  date_format: '%Y-%m-%d'      # Formato de data
  encoding: utf-8-sig          # Codificação de arquivos
  csv_separator: ';'           # Separador CSV
  add_timestamp_to_files: true # Adicionar timestamp aos arquivos
  empresa:
    nome: EMCCAMP              # Nome da empresa
    cnpj: '19.403.252/0001-90' # CNPJ da empresa
```

#### 6.1.2 Flags de Controle
```yaml
flags:
  filtros_batimento:
    habilitar: true            # Habilita filtros no batimento
    tipos_excluir:             # Tipos de pagamento a excluir
      - PERMUTA
      - Financiamento Fixo
```

#### 6.1.3 Caminhos de Arquivos
```yaml
paths:
  projeto_root: .
  logs: data/logs
  input:
    emccamp: data/input/emccamp
    max: data/input/base_max
    judicial: data/input/judicial
    baixas: data/input/baixas
    doublecheck_acordo: data/input/doublecheck_acordo
  output:
    base: data/output
```

#### 6.1.4 Configurações de Logging
```yaml
logging:
  level: INFO                          # Nível de log
  format: '%(asctime)s - %(message)s'  # Formato das mensagens
  date_format: '%Y-%m-%d %H:%M:%S'     # Formato de data nos logs
  console_handler:
    enabled: false                     # Handler do console
  file_handler:
    enabled: false                     # Handler de arquivo
    filename: pipeline_emccamp.log     # Nome do arquivo de log
```

#### 6.1.5 Queries de Banco de Dados
```yaml
queries:
  max:
    template: max                      # Template da query
    params:
      mo_cliente_id: 77398             # ID do cliente
    filters:
      vencimento:
        field: MoDataVencimento        # Campo de data
        start_env: MAX_DATA_VENCIMENTO_INICIAL  # Variável de ambiente
        end_env: MAX_DATA_VENCIMENTO_FINAL      # Variável de ambiente
  
  autojur:
    template: autojur
    params:
      grupo_empresarial: EMCCAMP
  
  maxsmart_judicial:
    template: maxsmart_judicial
    params:
      campanhas_id: 4
  
  doublecheck_acordo:
    template: doublecheck_acordo
    params: {}
```

### 6.2 Mapeamentos de Campos

#### 6.2.1 Mapeamento EMCCAMP
```yaml
mappings:
  emccamp:
    rename:                            # Renomeação de colunas
      CODCOLIGADA: COD_COLIGADA
      NOME_COLIGADA: NOME_COLIGADA
      CLIENTE: NOME_RAZAO_SOCIAL
      CPF: CPF_CNPJ
      NUM_VENDA: CONTRATO
      PARCELA: N_PARCELA
      ID_PARCELA: PARCELA
      VENCIMENTO: DATA_VENCIMENTO
      VALOR_ORIGINAL: VALOR_PARCELA
      VALOR_ATUALIZADO: VALOR_ATUALIZADO
      # ... outros campos
    
    key:                               # Configuração de chave
      components:
        - CONTRATO
        - PARCELA
      sep: "-"
    
    required:                          # Campos obrigatórios
      - CONTRATO
      - PARCELA
      - DATA_VENCIMENTO
      - VALOR_PARCELA
      - CPF_CNPJ
```

#### 6.2.2 Mapeamento MAX
```yaml
mappings:
  max:
    rename:                            # Renomeação de colunas
      CAMPANHA: CAMPANHA
      CREDOR: CREDOR
      CNPJ_CREDOR: CNPJ_CREDOR
      CPFCNPJ_CLIENTE: CPF_CNPJ
      NOME_RAZAO_SOCIAL: NOME_RAZAO_SOCIAL
      # ... outros campos
    
    preserve:                          # Campos a preservar
      - DT_BAIXA
      - RECEBIDO
    
    key:                               # Configuração de chave
      use_parcela_as_chave: true
      components:
        - NUMERO_CONTRATO
        - PARCELA
      sep: "-"
    
    validation:                        # Validação de formato
      # Regex para formato de parcela: 3+ dígitos, hífen, 2+ dígitos
```

### 6.3 Configurações de Processamento

#### 6.3.1 Processador EMCCAMP
```yaml
emccamp_processor:
  columns:
    mapping:                           # Mapeamento de colunas
      CLIENTE: NOME_RAZAO_SOCIAL
      CPF: CPFCNPJ_CLIENTE
      # ... outros campos
    
    required:                          # Campos obrigatórios
      - CPFCNPJ_CLIENTE
      - NUM_VENDA
      - ID_PARCELA
      - VENCIMENTO
      - VALOR
  
  defaults:                            # Valores padrão
    CAMPANHA: ''
    CREDOR: EMCCAMP
    CNPJ_CREDOR: ''
    # ... outros campos
  
  export:                              # Configurações de exportação
    filename_prefix: emccamp_tratada
    inconsistencies_prefix: emccamp_inconsistencias
    subdir: emccamp_tratada
  
  tratamento:                          # Configurações de tratamento
    chave_campos:
      - NUM_VENDA
      - ID_PARCELA
    chave_delimitador: ''
    inconsistencias_obrigatorias:
      - CPFCNPJ_CLIENTE
      - NUM_VENDA
      - ID_PARCELA
      - VENCIMENTO
      - VALOR
    layout_saida:                      # Layout de saída
      - CAMPANHA
      - CREDOR
      - CNPJ_CREDOR
      - CPFCNPJ_CLIENTE
      - NOME_RAZAO_SOCIAL
      # ... outros campos
```

#### 6.3.2 Configurações de Baixa
```yaml
baixa:
  export:                              # Configurações de exportação
    filename_prefix: emccamp_baixa
    formato: zip
    com_recebimento_prefix: baixa_com_recebimento
    sem_recebimento_prefix: baixa_sem_recebimento
  
  status_baixa_fixo: '98'              # Status fixo para baixas
  
  chaves:                              # Configuração de chaves
    emccamp: CHAVE
    max: PARCELA
  
  filtros:                             # Filtros aplicados
    max:
      campanhas:
        - '000041 - EMCCAMP'           # Campanha específica
      status_titulo:
        - ABERTO                       # Apenas títulos em aberto
```

---

## 📁 ESTRUTURA DE ARQUIVOS

### 7.1 Diretórios de Entrada (`data/input/`)
```
data/input/
├── emccamp/                    # Dados EMCCAMP (API TOTVS)
│   └── emccamp_YYYYMMDD_HHMMSS.zip
├── base_max/                   # Dados MAX (SQL Server)
│   └── MaxSmart_YYYYMMDD_HHMMSS.zip
├── judicial/                   # Dados judiciais (AutoJur)
│   └── ClientesJudiciais.zip
├── baixas/                     # Baixas EMCCAMP (API TOTVS)
│   └── baixa_emccamp.zip
└── doublecheck_acordo/         # Acordos (MaxSmart)
    └── acordos_abertos.zip
```

### 7.2 Diretórios de Saída (`data/output/`)
```
data/output/
├── emccamp_tratada/            # Base EMCCAMP tratada
│   └── emccamp_tratada_YYYYMMDD_HHMMSS.zip
├── max_tratada/                # Base MAX tratada
│   └── max_tratada_YYYYMMDD_HHMMSS.zip
├── inconsistencias/            # Registros inconsistentes
│   ├── emccamp_inconsistencias_YYYYMMDD_HHMMSS.zip
│   └── max_inconsistencias_YYYYMMDD_HHMMSS.zip
├── batimento/                  # Resultado do batimento
│   └── emccamp_batimento_YYYYMMDD_HHMMSS.zip
│       ├── emccamp_batimento_judicial_YYYYMMDD_HHMMSS.csv
│       └── emccamp_batimento_extrajudicial_YYYYMMDD_HHMMSS.csv
├── baixa/                      # Resultado das baixas
│   └── emccamp_baixa_YYYYMMDD_HHMMSS.zip
│       ├── baixa_com_recebimento_YYYYMMDD_HHMMSS.csv
│       └── baixa_sem_recebimento_YYYYMMDD_HHMMSS.csv
└── enriquecimento_contato_emccamp/  # Contatos enriquecidos
    └── enriquecimento_contato_emccamp.zip
        └── enriquecimento_contato_emccamp.csv
```

### 7.3 Estrutura do Código (`src/`)
```
src/
├── config/                     # Configurações
│   ├── config.yaml            # Arquivo principal de configuração
│   └── loader.py              # Carregador de configurações
├── processors/                 # Processadores de dados
│   ├── emccamp.py             # Tratamento EMCCAMP
│   ├── max.py                 # Tratamento MAX
│   ├── batimento.py           # Batimento EMCCAMP x MAX
│   ├── baixa.py               # Baixas MAX - EMCCAMP
│   └── contact_enrichment.py  # Enriquecimento de contatos
├── scripts/                    # Scripts de extração
│   ├── extrair_emccamp.py     # Extração EMCCAMP
│   ├── extrair_basemax.py     # Extração MAX
│   ├── extrair_baixa_emccamp.py # Extração baixas
│   ├── extrair_judicial.py    # Extração judicial
│   └── extrair_doublecheck_acordo.py # Extração acordos
├── utils/                      # Utilitários
│   ├── io.py                  # Entrada/saída de dados
│   ├── logger.py              # Sistema de logs
│   ├── sql_conn.py            # Conexões SQL
│   ├── totvs_client.py        # Cliente API TOTVS
│   ├── queries.py             # Templates de queries
│   ├── anti_join.py           # Operações anti-join
│   ├── output_formatter.py    # Formatação de saída
│   ├── path_manager.py        # Gerenciamento de caminhos
│   └── text.py                # Utilitários de texto
└── pipeline.py                 # Orquestrador principal
```

### 7.4 Arquivos de Execução
```
├── main.py                     # Ponto de entrada principal
├── pipeline.py                 # Pipeline de processamento
├── run_completo_emccamp.bat    # Execução completa (Windows)
├── run_pipeline_emccamp.bat    # Pipeline específico (Windows)
└── setup_project_emccamp.bat   # Configuração inicial (Windows)
```

---

## 🚀 COMANDOS DE EXECUÇÃO

### Execução Individual
```bash
# Tratamento de dados
python main.py treat emccamp    # Trata base EMCCAMP
python main.py treat max        # Trata base MAX
python main.py treat all        # Trata ambas as bases

# Processamentos
python main.py batimento        # Executa batimento
python main.py baixa            # Executa baixas
python main.py enriquecimento   # Executa enriquecimento

# Extração de dados
python -m src.scripts.extrair_emccamp
python -m src.scripts.extrair_basemax
python -m src.scripts.extrair_baixa_emccamp
```

### Execução Completa
```bash
# Pipeline completo
python main.py treat all && python main.py batimento && python main.py baixa && python main.py enriquecimento
```

---

## 📊 MÉTRICAS E LOGS

### Métricas Principais
- **Taxa de Aproveitamento:** Percentual de registros válidos após tratamento
- **Taxa de Batimento:** Percentual de registros EMCCAMP encontrados no MAX
- **Registros para Baixa:** Quantidade de registros MAX não encontrados no EMCCAMP
- **Enriquecimento:** Quantidade de contatos gerados e deduplicados

### Logs de Execução
- **Localização:** `data/logs/execucao_emccamp.log`
- **Formato:** `YYYY-MM-DD HH:MM:SS - MENSAGEM`
- **Níveis:** INFO, WARNING, ERROR

---

*Documentação gerada em: 30/10/2025*
*Versão do Sistema: EMCCAMP v1.0*