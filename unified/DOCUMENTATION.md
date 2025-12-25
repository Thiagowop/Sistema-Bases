# Sistema Unificado de Processamento de Carteiras

## Visao Geral

O Sistema Unificado e uma arquitetura hibrida (Config-First + Python Extensions + REST API) que consolida tres projetos isolados em uma plataforma escalavel:

- **Automacao_Vic**: Processamento de carteira de credito VIC
- **Automacao_Tabelionato**: Processamento de registros de cartorio
- **Emccamp**: Processamento de carteira EMCCAMP

### Principios Arquiteturais

1. **Config-First**: Regras de negocio definidas em YAML (sem necessidade de codigo)
2. **Extensibilidade**: Classes Python para logica complexa quando necessario
3. **Reusabilidade**: Componentes genericos compartilhados entre clientes
4. **Observabilidade**: Logs detalhados e metricas de execucao
5. **Integracao**: REST API para orquestracao via n8n/Airflow

---

## Estrutura de Arquivos

```
unified/
├── configs/
│   └── clients/                    # Configuracoes YAML por cliente
│       ├── vic.yaml               # Configuracao VIC
│       ├── emccamp.yaml           # Configuracao EMCCAMP
│       └── tabelionato.yaml       # Configuracao Tabelionato
│
├── src/
│   ├── core/                       # Nucleo do sistema
│   │   ├── __init__.py
│   │   ├── base.py                # Classes base abstratas
│   │   ├── config.py              # Carregamento de configuracao
│   │   ├── engine.py              # Motor de execucao do pipeline
│   │   ├── keys.py                # Geradores de CHAVE
│   │   └── schemas.py             # Esquemas de dados (dataclasses)
│   │
│   ├── loaders/                    # Carregadores de dados
│   │   ├── __init__.py
│   │   ├── file_loader.py         # Arquivos CSV/Excel/ZIP (com senha)
│   │   ├── email_loader.py        # Anexos de email (IMAP)
│   │   ├── sql_loader.py          # Banco de dados SQL Server
│   │   └── api_loader.py          # APIs REST (TOTVS)
│   │
│   ├── validators/                 # Validadores de dados
│   │   ├── __init__.py            # Factory e registro
│   │   ├── required.py            # Campos obrigatorios
│   │   ├── aging.py               # Idade maxima (dias de atraso)
│   │   ├── blacklist.py           # Lista negra de CPF/CNPJ
│   │   ├── regex.py               # Validacao por expressao regular
│   │   ├── campaign.py            # Atribuicao de campanha
│   │   ├── status.py              # Filtro por status
│   │   ├── type_filter.py         # Filtro por tipo/categoria
│   │   ├── linebreak.py           # Deteccao de quebras de linha
│   │   └── daterange.py           # Validacao de intervalo de datas
│   │
│   ├── splitters/                  # Divisores de dados
│   │   ├── __init__.py
│   │   ├── judicial.py            # Separacao judicial/extrajudicial
│   │   ├── campaign.py            # Divisao por campanha
│   │   └── field_value.py         # Divisao por valor de campo
│   │
│   ├── processors/                 # Processadores do pipeline
│   │   ├── __init__.py
│   │   ├── tratamento.py          # Limpeza e normalizacao
│   │   ├── batimento.py           # Anti-join (PROCV A-B, B-A)
│   │   ├── baixa.py               # Geracao de arquivo de baixa
│   │   ├── devolucao.py           # Separacao de devolucoes
│   │   └── enriquecimento.py      # Formatacao final e exportacao
│   │
│   ├── api/                        # REST API
│   │   ├── __init__.py
│   │   └── app.py                 # Aplicacao Flask
│   │
│   ├── clients/                    # Extensoes por cliente (opcional)
│   │   └── __init__.py
│   │
│   ├── utils/                      # Utilitarios
│   │   └── __init__.py
│   │
│   ├── cli.py                      # Interface de linha de comando
│   └── __init__.py
│
├── tests/                          # Testes automatizados
│   ├── __init__.py
│   └── test_emccamp_validation.py
│
├── data/                           # Dados (por cliente)
│   ├── input/                      # Arquivos de entrada
│   ├── output/                     # Arquivos gerados
│   └── temp/                       # Arquivos temporarios
│
├── requirements.txt                # Dependencias Python
├── README.md                       # Documentacao resumida
└── DOCUMENTATION.md                # Esta documentacao
```

---

## Componentes Detalhados

### 1. Loaders (Carregadores)

Os loaders sao responsaveis por extrair dados de diferentes fontes.

#### FileLoader
Carrega dados de arquivos locais (CSV, Excel, ZIP).

```yaml
loader:
  type: file
  params:
    path: "data/input/arquivo.zip"
    encoding: "utf-8-sig"
    separator: ";"
    password: "senha123"  # Para ZIPs protegidos
```

**Suporte a ZIP com senha:**
- pyzipper (AES encryption)
- 7-Zip (linha de comando)
- unzip (linha de comando)
- zipfile padrao com senha

#### EmailLoader
Extrai anexos de emails via IMAP.

```yaml
loader:
  type: email
  params:
    subject_filter: "RELATORIO"
    sender_filter: "cliente@email.com"
    days_back: 7
    attachment_pattern: "*.zip"
```

#### SQLLoader
Executa queries no SQL Server.

```yaml
loader:
  type: sql
  params:
    server: "${SQL_SERVER}"
    database: "MAX"
    query: |
      SELECT * FROM dbo.VW_CARTEIRA
      WHERE STATUS = 'ATIVO'
```

#### APILoader
Consome APIs REST (ex: TOTVS).

```yaml
loader:
  type: api
  params:
    url: "${TOTVS_API_URL}/carteira"
    method: GET
    headers:
      Authorization: "Bearer ${TOTVS_TOKEN}"
```

---

### 2. Validators (Validadores)

Os validadores filtram e validam registros antes do processamento.

#### RequiredValidator
Verifica campos obrigatorios.

```yaml
- type: required
  params:
    columns:
      - CPF_CNPJ
      - CONTRATO
      - SALDO
```

#### AgingValidator
Filtra por idade maxima (dias desde vencimento).

```yaml
- type: aging
  params:
    date_column: VENCIMENTO
    max_age_days: 1825  # 5 anos
    null_action: include
```

#### BlacklistValidator
Remove registros em lista negra.

```yaml
- type: blacklist
  params:
    column: CPF_CNPJ
    source: "data/blacklist.csv"
    source_column: DOCUMENTO
```

#### RegexValidator
Valida formato via expressao regular.

```yaml
- type: regex
  params:
    column: CPF_CNPJ
    pattern: "^\\d{11}$|^\\d{14}$"
    mode: fullmatch
```

#### CampaignValidator
Atribui campanha baseado em regras.

```yaml
- type: campaign
  params:
    output_column: CAMPANHA
    rules:
      - name: cobranca
        conditions:
          aging_min: 0
          aging_max: 90
      - name: recuperacao
        conditions:
          aging_min: 91
          aging_max: 365
    default_campaign: outros
```

#### StatusValidator
Filtra por valores de status.

```yaml
- type: status
  params:
    column: STATUS_TITULO
    include:
      - EM ABERTO
    case_sensitive: false
```

#### TypeFilterValidator
Filtra por tipo/categoria.

```yaml
- type: type_filter
  params:
    column: TIPO_PAGTO
    exclude:
      - PERMUTA
      - Financiamento Fixo
    case_sensitive: false
    match_mode: exact  # exact, contains, startswith
```

#### LineBreakValidator
Detecta/limpa quebras de linha internas.

```yaml
- type: linebreak
  params:
    check_all: true
    action: clean  # exclude, flag, clean
```

#### DateRangeValidator
Valida intervalo de datas.

```yaml
- type: daterange
  params:
    column: VENCIMENTO
    min_year: 1900
    max_year: 2100
    null_action: include
```

---

### 3. Splitters (Divisores)

Os splitters dividem dados em grupos para exportacao separada.

#### JudicialSplitter
Separa judicial de extrajudicial.

```yaml
- type: judicial
  params:
    judicial_source: "data/judicial.csv"
    match_column: CPF_CNPJ
```

#### CampaignSplitter
Divide por campanha.

```yaml
- type: campaign
  params:
    column: CAMPANHA
    rules:
      - name: cobranca
        patterns: [COBRANCA, COB]
      - name: recuperacao
        patterns: [RECUPERACAO, REC]
    default_group: outros
```

#### FieldValueSplitter
Divide por valor de campo.

```yaml
- type: field_value
  params:
    column: UF
    mode: exact
    normalize: true
    mappings:
      sudeste: [SP, RJ, MG, ES]
      sul: [PR, SC, RS]
    default_group: outras
```

---

### 4. Processors (Processadores)

Os processadores executam as etapas do pipeline.

#### TratamentoProcessor
Limpeza e normalizacao de dados.

```yaml
- type: tratamento
  params:
    cpf_columns: [CPF_CNPJ]
    value_columns: [SALDO, VALOR]
    date_columns: [VENCIMENTO]
    text_columns: [NOME]
    remove_duplicates: true
    key_column: CHAVE
```

**Funcoes:**
- Formatacao de CPF/CNPJ (apenas numeros)
- Normalizacao de valores monetarios
- Conversao de datas
- Limpeza de textos (trim, upper)
- Remocao de duplicatas

#### BatimentoProcessor
Anti-join entre cliente e MAX.

```yaml
- type: batimento
  params:
    client_key: CHAVE
    max_key: CHAVE
    compute_a_minus_b: true   # novos
    compute_b_minus_a: true   # baixas
    compute_intersection: false
```

**Outputs:**
- `novos`: Registros no cliente que nao estao no MAX
- `baixas`: Registros no MAX que nao estao no cliente

#### BaixaProcessor
Gera arquivo de baixa para importacao.

```yaml
- type: baixa
  params:
    status_baixa_fixo: "98"
    export_columns:
      - CPF_CNPJ
      - CONTRATO
      - PARCELA
      - CHAVE
```

#### DevolucaoProcessor
Separa devolucoes judiciais.

```yaml
- type: devolucao
  params:
    judicial_source: "data/judicial.zip"
    judicial_column: CPF_CNPJ
    target_column: CPF_CNPJ
    add_motivo: true
    add_data: true
```

#### EnriquecimentoProcessor
Formatacao final e exportacao.

```yaml
- type: enriquecimento
  params:
    filename_prefix: cliente_novos
    computed_fields:
      DATA_CARGA:
        type: date
        format: "%d/%m/%Y"
      ORIGEM:
        type: constant
        value: "CLIENTE"
    select_columns:
      - CAMPANHA
      - CPF_CNPJ
      - NOME
      - VALOR
```

---

### 5. Key Generators (Geradores de CHAVE)

A CHAVE e o identificador unico para batimento.

#### Composite
Concatena multiplas colunas.

```yaml
key:
  type: composite
  components:
    - CPF_CNPJ
    - CONTRATO
    - PARCELA
  separator: "-"
  output_column: CHAVE
```

Resultado: `12345678901-123456-001`

#### Column
Usa uma coluna existente.

```yaml
key:
  type: column
  column: PARCELA
  output_column: CHAVE
```

---

## Pipeline de Execucao

```
┌──────────────────────────────────────────────────────────────────┐
│                      CARREGAMENTO                                 │
│  ┌─────────┐  ┌─────────┐  ┌─────────┐  ┌─────────┐             │
│  │  FILE   │  │  EMAIL  │  │   SQL   │  │   API   │             │
│  └────┬────┘  └────┬────┘  └────┬────┘  └────┬────┘             │
│       └────────────┴────────────┴────────────┘                   │
└────────────────────────────┬─────────────────────────────────────┘
                             ▼
┌──────────────────────────────────────────────────────────────────┐
│                    GERACAO DE CHAVE                               │
│           CPF_CNPJ + CONTRATO + PARCELA → CHAVE                  │
└────────────────────────────┬─────────────────────────────────────┘
                             ▼
┌──────────────────────────────────────────────────────────────────┐
│                      VALIDACAO                                    │
│  required → aging → blacklist → regex → status → type_filter    │
│                  → linebreak → daterange                         │
└────────────────────────────┬─────────────────────────────────────┘
                             ▼
┌──────────────────────────────────────────────────────────────────┐
│                     TRATAMENTO                                    │
│    Limpeza CPF │ Normalizacao valores │ Formatacao datas        │
│              │ Limpeza textos │ Remocao duplicatas               │
└────────────────────────────┬─────────────────────────────────────┘
                             ▼
┌──────────────────────────────────────────────────────────────────┐
│                      BATIMENTO                                    │
│         ┌─────────────┐         ┌─────────────┐                  │
│         │   CLIENTE   │         │     MAX     │                  │
│         └──────┬──────┘         └──────┬──────┘                  │
│                │                       │                          │
│         ┌──────┴───────────────────────┴──────┐                  │
│         │           Anti-Join                 │                  │
│         ├─────────────────────────────────────┤                  │
│         │  A - B = NOVOS    │  B - A = BAIXAS │                  │
│         └─────────────────────────────────────┘                  │
└────────────────────────────┬─────────────────────────────────────┘
                             ▼
┌──────────────────────────────────────────────────────────────────┐
│                 BAIXA / DEVOLUCAO                                 │
│         ┌─────────────┐         ┌─────────────┐                  │
│         │    BAIXA    │         │  DEVOLUCAO  │                  │
│         │ (para MAX)  │         │  (judicial) │                  │
│         └─────────────┘         └─────────────┘                  │
└────────────────────────────┬─────────────────────────────────────┘
                             ▼
┌──────────────────────────────────────────────────────────────────┐
│                   ENRIQUECIMENTO                                  │
│    Campos calculados │ Splits por campanha │ Formatacao final    │
└────────────────────────────┬─────────────────────────────────────┘
                             ▼
┌──────────────────────────────────────────────────────────────────┐
│                     EXPORTACAO                                    │
│              Arquivos ZIP/CSV com timestamp                      │
└──────────────────────────────────────────────────────────────────┘
```

---

## Configuracao por Cliente

### VIC (vic.yaml)

| Caracteristica | Valor |
|---------------|-------|
| Fonte Cliente | Email (anexo ZIP) |
| Fonte MAX | SQL Server (VW_CARTEIRA_VIC) |
| CHAVE | CPF_CNPJ-CONTRATO-PARCELA |
| Validadores | required, aging, status, type_filter |
| Filtro Status | STATUS_TITULO = "EM ABERTO" |
| Filtro Tipo | Exclui ENTRADA, TAXA |

### Tabelionato (tabelionato.yaml)

| Caracteristica | Valor |
|---------------|-------|
| Fonte Cliente | Email (ZIP com senha: Mf4tab@) |
| Fonte MAX | SQL Server (VW_CARTEIRA_TABELIONATO) |
| CHAVE | CPF_CNPJ-PROTOCOLO |
| Validadores | required, regex, linebreak, daterange |
| Validacao CPF | Regex 11 ou 14 digitos |
| Validacao Data | min_year: 1900 |
| Limpeza | Remove quebras de linha |

### EMCCAMP (emccamp.yaml)

| Caracteristica | Valor |
|---------------|-------|
| Fonte Cliente | Arquivo ZIP (Emccamp.zip) |
| Fonte MAX | Arquivo ZIP (MaxSmart.zip) |
| CHAVE | NUM_VENDA-ID_PARCELA |
| Validadores | required, type_filter |
| Filtro Tipo | Exclui PERMUTA, Financiamento Fixo |
| Campos Extras | CREDOR, CNPJ_CREDOR (constantes) |

---

## Uso

### CLI (Linha de Comando)

```bash
# Listar clientes disponiveis
python -m unified.src.cli list

# Validar configuracao
python -m unified.src.cli validate vic

# Executar pipeline
python -m unified.src.cli run vic --output-dir ./output

# Com logs detalhados
python -m unified.src.cli run vic --log-level DEBUG --log-file ./logs/vic.log
```

### REST API

```bash
# Iniciar servidor
python -m unified.src.api.app

# Porta padrao: 5000
```

**Endpoints:**

| Metodo | Endpoint | Descricao |
|--------|----------|-----------|
| GET | /health | Health check |
| GET | /clients | Listar clientes |
| GET | /clients/{name} | Detalhes do cliente |
| POST | /run/{name} | Executar pipeline |
| POST | /validate/{name} | Validar configuracao |

**Exemplo de resposta (/run/vic):**

```json
{
  "success": true,
  "client": "vic",
  "duration_seconds": 45.3,
  "outputs": {
    "novos": "output/vic/vic_novos_20240115_143022.zip",
    "baixas": "output/vic/vic_baixas_20240115_143022.zip",
    "devolucao": "output/vic/vic_devolucao_20240115_143022.zip"
  },
  "stats": {
    "client_records": 15000,
    "max_records": 12000,
    "novos": 3500,
    "baixas": 500,
    "judicial": 150
  }
}
```

### Programatico (Python)

```python
from unified.src.core import PipelineEngine

# Criar engine
engine = PipelineEngine(
    config_dir="./configs/clients",
    output_dir="./output"
)

# Executar para um cliente
result = engine.run("vic")

if result.success:
    print(f"Processamento concluido em {result.duration_seconds:.2f}s")
    for name, path in result.context.outputs.items():
        print(f"  {name}: {path}")
else:
    print(f"Erro: {result.error}")
```

---

## Integracao com n8n

### Configuracao Basica

1. **Inicie a API REST:**
   ```bash
   python -m unified.src.api.app
   ```

2. **No n8n, crie um workflow:**
   - **Trigger**: Schedule ou Webhook
   - **HTTP Request**: POST para executar pipeline
   - **IF**: Verificar sucesso
   - **Email/Slack**: Notificar resultado

### Exemplo de Workflow n8n

```json
{
  "nodes": [
    {
      "name": "Executar Pipeline",
      "type": "n8n-nodes-base.httpRequest",
      "parameters": {
        "method": "POST",
        "url": "http://localhost:5000/run/vic",
        "responseFormat": "json"
      }
    },
    {
      "name": "Verificar Resultado",
      "type": "n8n-nodes-base.if",
      "parameters": {
        "conditions": {
          "boolean": [
            {
              "value1": "={{ $json.success }}",
              "value2": true
            }
          ]
        }
      }
    },
    {
      "name": "Notificar Sucesso",
      "type": "n8n-nodes-base.emailSend",
      "parameters": {
        "subject": "Pipeline VIC Concluido",
        "text": "Processamento concluido com sucesso.\nNovos: {{ $json.stats.novos }}\nBaixas: {{ $json.stats.baixas }}"
      }
    }
  ]
}
```

### Funciona sem n8n?

**Sim!** O sistema funciona de forma independente:

1. **Via CLI**: `python -m unified.src.cli run vic`
2. **Via Script Python**: Importando o engine diretamente
3. **Via API REST**: Chamando endpoints com curl/Postman
4. **Agendamento**: Usando cron ou Task Scheduler

O n8n e apenas uma opcao de orquestracao, nao uma dependencia.

---

## Adicionando Novo Cliente

### Passo 1: Criar Arquivo de Configuracao

Crie `configs/clients/novo_cliente.yaml`:

```yaml
name: novo_cliente
version: "1.0"
description: "Descricao do novo cliente"

# Fonte de dados do cliente
client_source:
  loader:
    type: file
    params:
      path: "data/input/novo_cliente/dados.zip"
      encoding: "utf-8-sig"
      separator: ";"

  # Geracao de chave unica
  key:
    type: composite
    components:
      - CPF_CNPJ
      - CONTRATO
    separator: "-"
    output_column: CHAVE

  # Mapeamento de colunas
  columns:
    DOCUMENTO: CPF_CNPJ
    NUMERO_CONTRATO: CONTRATO
    VALOR_DIVIDA: SALDO

  # Colunas obrigatorias
  required_columns:
    - CPF_CNPJ
    - CONTRATO
    - SALDO

  # Validadores
  validators:
    - type: required
      params:
        columns: [CPF_CNPJ, CONTRATO, SALDO]

    - type: aging
      params:
        date_column: VENCIMENTO
        max_age_days: 1825

  export:
    filename_prefix: novo_cliente
    format: zip

# Fonte MAX (opcional)
max_source:
  loader:
    type: sql
    params:
      server: "${SQL_SERVER}"
      database: "MAX"
      query: "SELECT * FROM dbo.VW_NOVO_CLIENTE"

  key:
    type: column
    column: CHAVE

# Pipeline
pipeline:
  processors:
    - type: tratamento
      params:
        cpf_columns: [CPF_CNPJ]
        value_columns: [SALDO]

    - type: batimento
      params:
        client_key: CHAVE
        max_key: CHAVE
        compute_a_minus_b: true
        compute_b_minus_a: true

    - type: enriquecimento
      params:
        filename_prefix: novo_cliente_novos

# Caminhos
paths:
  output_dir: "./output/novo_cliente"
```

### Passo 2: Validar Configuracao

```bash
python -m unified.src.cli validate novo_cliente
```

### Passo 3: Testar Execucao

```bash
python -m unified.src.cli run novo_cliente --log-level DEBUG
```

### Passo 4: Criar Extensao (Opcional)

Para logica complexa que nao pode ser expressa em YAML:

```python
# unified/src/clients/novo_cliente.py

from unified.src.core.base import BaseClientExtension
import pandas as pd

class NovoClienteExtension(BaseClientExtension):
    """Extensao customizada para Novo Cliente."""

    def pre_process(self, df: pd.DataFrame, source: str) -> pd.DataFrame:
        """Executado antes do processamento."""
        # Logica customizada aqui
        return df

    def post_process(self, df: pd.DataFrame, stage: str) -> pd.DataFrame:
        """Executado apos cada etapa."""
        return df

    def custom_validation(self, df: pd.DataFrame) -> ValidationResult:
        """Validacao customizada."""
        # Implementar validacoes especificas
        pass
```

Registrar no YAML:

```yaml
extension_class: unified.src.clients.novo_cliente.NovoClienteExtension
```

---

## Variaveis de Ambiente

Crie um arquivo `.env` na raiz do projeto:

```bash
# SQL Server
SQL_SERVER=servidor.database.windows.net
SQL_DATABASE=MAX
SQL_USER=usuario
SQL_PASSWORD=senha_segura

# Email (IMAP)
EMAIL_SERVER=imap.gmail.com
EMAIL_PORT=993
EMAIL_USER=automacao@empresa.com
EMAIL_PASSWORD=senha_app

# API TOTVS
TOTVS_API_URL=https://api.totvs.com.br
TOTVS_TOKEN=token_de_acesso

# Caminhos
JUDICIAL_FILE_PATH=./data/judicial/clientes_judiciais.zip
OUTPUT_BASE_DIR=./output
```

---

## Troubleshooting

### ZIP com senha nao abre

1. Instale pyzipper: `pip install pyzipper`
2. Ou instale 7-Zip: `apt install p7zip-full`
3. Verifique a senha no config

### Erro de conexao SQL

1. Instale driver: `pip install pyodbc`
2. Verifique variaveis de ambiente
3. Teste conexao manualmente

### Email nao encontra anexos

1. Verifique filtros (subject_filter, sender_filter)
2. Aumente days_back
3. Verifique permissoes IMAP

### Batimento com resultados incorretos

1. Verifique geracao de CHAVE em ambas as fontes
2. Compare colunas componentes
3. Verifique encoding dos arquivos

---

## Proximos Passos

### Melhorias Planejadas

1. **Dashboard de Monitoramento**: Interface web para acompanhar execucoes
2. **Cache de Dados**: Evitar reprocessamento desnecessario
3. **Paralelizacao**: Processar multiplos clientes simultaneamente
4. **Notificacoes**: Integracao com Slack/Teams/Email
5. **Auditoria**: Log detalhado de alteracoes

### Novos Validadores Potenciais

- `duplicate`: Detectar duplicatas com regras customizadas
- `range`: Validar valores numericos em intervalo
- `lookup`: Validar contra tabela de referencia
- `phone`: Validar formato de telefone

### Novos Loaders Potenciais

- `sftp`: Carregar de servidor SFTP
- `s3`: Carregar de bucket S3
- `sharepoint`: Carregar de SharePoint
- `google_sheets`: Carregar de Google Sheets

---

## Suporte

Para duvidas ou problemas:

1. Verifique esta documentacao
2. Consulte logs em `data/logs/`
3. Execute com `--log-level DEBUG`
4. Abra issue no repositorio
