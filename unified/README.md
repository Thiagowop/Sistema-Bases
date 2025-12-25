# Unified Pipeline System

Sistema unificado para processamento de carteiras de credito, combinando os projetos Automacao_Vic, Automacao_Tabelionato e Emccamp em uma arquitetura hibrida e extensivel.

## Inicio Rapido

```bash
# Instalar dependencias
pip install -r requirements.txt

# Listar clientes disponiveis
python -m unified.src.cli list

# Executar pipeline
python -m unified.src.cli run emccamp --output-dir ./output
```

## Arquitetura

```
unified/
├── configs/
│   └── clients/          # Configuracoes YAML por cliente
│       ├── vic.yaml
│       ├── emccamp.yaml
│       └── tabelionato.yaml
├── src/
│   ├── core/             # Engine, schemas, configuracao
│   ├── loaders/          # Carregadores (file, email, sql, api)
│   ├── validators/       # Validadores (9 tipos disponiveis)
│   ├── splitters/        # Divisores (judicial, campaign, field)
│   ├── processors/       # Processadores do pipeline
│   ├── api/              # REST API para integracoes
│   └── cli.py            # Interface de linha de comando
└── tests/                # Testes automatizados
```

## Validadores Disponiveis

| Tipo | Descricao | Exemplo de Uso |
|------|-----------|----------------|
| required | Campos obrigatorios | CPF_CNPJ, CONTRATO, SALDO |
| aging | Idade maxima (dias) | max_age_days: 1825 |
| blacklist | Lista negra CPF/CNPJ | source: blacklist.csv |
| regex | Expressao regular | pattern: "^\\d{11}$" |
| campaign | Atribuicao de campanha | Por faixa de aging |
| status | Filtro por status | STATUS_TITULO = "EM ABERTO" |
| type_filter | Filtro por tipo | Exclui PERMUTA |
| linebreak | Quebras de linha | action: clean |
| daterange | Intervalo de datas | min_year: 1900 |

## Uso

### CLI

```bash
# Listar clientes
python -m unified.src.cli list

# Validar configuracao
python -m unified.src.cli validate vic

# Executar pipeline
python -m unified.src.cli run vic --output-dir ./output

# Com logs detalhados
python -m unified.src.cli run vic --log-level DEBUG
```

### REST API

```bash
# Iniciar servidor
python -m unified.src.api.app

# Endpoints:
# GET  /health           - Health check
# GET  /clients          - Listar clientes
# GET  /clients/{name}   - Detalhes do cliente
# POST /run/{name}       - Executar pipeline
# POST /validate/{name}  - Validar configuracao
```

### Python

```python
from unified.src.core import PipelineEngine

engine = PipelineEngine(
    config_dir="./configs/clients",
    output_dir="./output"
)

result = engine.run("vic")
print(f"Success: {result.success}")
print(f"Outputs: {result.context.outputs}")
```

---

## Adicionando Novo Cliente

### Passo 1: Criar arquivo de configuracao

Crie `configs/clients/meu_cliente.yaml`:

```yaml
name: meu_cliente
version: "1.0"
description: "Descricao do cliente"

# Fonte de dados do cliente
client_source:
  loader:
    type: file  # ou email, sql, api
    params:
      path: "data/input/meu_cliente/dados.zip"
      encoding: "utf-8-sig"
      separator: ";"
      password: "senha123"  # se ZIP com senha

  # Geracao de CHAVE unica
  key:
    type: composite
    components:
      - CPF_CNPJ
      - CONTRATO
      - PARCELA
    separator: "-"
    output_column: CHAVE

  # Mapeamento de colunas (origem -> padrao)
  columns:
    DOCUMENTO: CPF_CNPJ
    NUMERO_CONTRATO: CONTRATO
    VALOR_DIVIDA: SALDO

  # Colunas obrigatorias
  required_columns:
    - CPF_CNPJ
    - CONTRATO
    - SALDO

  # Validadores (escolha os necessarios)
  validators:
    - type: required
      params:
        columns: [CPF_CNPJ, CONTRATO, SALDO]

    - type: aging
      params:
        date_column: VENCIMENTO
        max_age_days: 1825

    - type: status
      params:
        column: STATUS
        include: [EM ABERTO]

  export:
    filename_prefix: meu_cliente
    format: zip

# Fonte MAX (dados internos para batimento)
max_source:
  loader:
    type: sql
    params:
      server: "${SQL_SERVER}"
      database: "MAX"
      query: |
        SELECT * FROM dbo.VW_CARTEIRA_CLIENTE
        WHERE STATUS = 'ATIVO'

  key:
    type: column
    column: CHAVE

# Pipeline de processamento
pipeline:
  processors:
    - type: tratamento
      params:
        cpf_columns: [CPF_CNPJ]
        value_columns: [SALDO]
        date_columns: [VENCIMENTO]

    - type: batimento
      params:
        client_key: CHAVE
        max_key: CHAVE
        compute_a_minus_b: true   # novos
        compute_b_minus_a: true   # baixas

    - type: baixa
      params:
        export_columns: [CPF_CNPJ, CONTRATO, CHAVE]

    - type: devolucao
      params:
        judicial_source: "data/judicial.zip"
        add_motivo: true

    - type: enriquecimento
      params:
        filename_prefix: meu_cliente_novos
        computed_fields:
          DATA_CARGA:
            type: date
            format: "%d/%m/%Y"
          ORIGEM:
            type: constant
            value: "MEU_CLIENTE"

# Caminhos de saida
paths:
  output_dir: "./output/meu_cliente"
```

### Passo 2: Validar configuracao

```bash
python -m unified.src.cli validate meu_cliente
```

### Passo 3: Testar execucao

```bash
python -m unified.src.cli run meu_cliente --log-level DEBUG
```

### Passo 4: Extensao Python (opcional)

Para logica complexa que nao cabe em YAML, crie uma extensao:

```python
# unified/src/clients/meu_cliente.py

from unified.src.core.base import BaseClientExtension
import pandas as pd

class MeuClienteExtension(BaseClientExtension):
    def pre_process(self, df: pd.DataFrame, source: str) -> pd.DataFrame:
        # Logica customizada antes do processamento
        df['CAMPO_CALCULADO'] = df['A'] + df['B']
        return df

    def custom_validation(self, df: pd.DataFrame):
        # Validacao especifica do cliente
        pass
```

E registre no YAML:

```yaml
extension_class: unified.src.clients.meu_cliente.MeuClienteExtension
```

---

## Integracao com n8n

### O sistema funciona sem n8n?

**Sim!** O sistema e totalmente independente. Voce pode executar:

1. **Via CLI**: `python -m unified.src.cli run vic`
2. **Via Script Python**: Importando o engine
3. **Via API REST**: Chamando endpoints
4. **Via Cron/Scheduler**: Agendando execucoes

O n8n e apenas uma **opcao** de orquestracao, nao uma dependencia.

### Configurando n8n

1. **Inicie a API REST:**
   ```bash
   python -m unified.src.api.app
   # Servidor rodando em http://localhost:5000
   ```

2. **Configure n8n para chamar a API:**

   **Executar Pipeline:**
   ```
   HTTP Request Node
   Method: POST
   URL: http://localhost:5000/run/vic
   ```

   **Verificar Resultado:**
   ```json
   {
     "success": true,
     "client": "vic",
     "duration_seconds": 45.3,
     "outputs": {
       "novos": "output/vic/vic_novos_20240115.zip",
       "baixas": "output/vic/vic_baixas_20240115.zip"
     },
     "stats": {
       "novos": 3500,
       "baixas": 500
     }
   }
   ```

3. **Workflow exemplo:**
   - **Trigger**: Schedule (diario as 8h) ou Webhook
   - **HTTP Request**: POST /run/{cliente}
   - **IF Node**: Verificar `{{ $json.success }}`
   - **Email Node**: Notificar resultado

### Endpoints da API

| Metodo | Endpoint | Descricao |
|--------|----------|-----------|
| GET | /health | Verificar se API esta rodando |
| GET | /clients | Listar todos os clientes |
| GET | /clients/{name} | Detalhes de um cliente |
| POST | /run/{name} | Executar pipeline |
| POST | /validate/{name} | Validar configuracao |

---

## Variaveis de Ambiente

Crie um arquivo `.env`:

```bash
# SQL Server
SQL_SERVER=servidor.database.windows.net
SQL_DATABASE=MAX
SQL_USER=usuario
SQL_PASSWORD=senha

# Email (IMAP)
EMAIL_SERVER=imap.gmail.com
EMAIL_PORT=993
EMAIL_USER=automacao@empresa.com
EMAIL_PASSWORD=senha_app

# API TOTVS
TOTVS_API_URL=https://api.totvs.com.br
TOTVS_TOKEN=token

# Arquivos
JUDICIAL_FILE_PATH=./data/judicial.zip
```

---

## Fluxo de Processamento

```
ENTRADA                    PROCESSAMENTO                      SAIDA
────────────────────────────────────────────────────────────────────
                           ┌─────────────┐
[Email/File/SQL/API] ──────▶ Carregamento │
                           └──────┬──────┘
                                  ▼
                           ┌─────────────┐
                           │ Gerar CHAVE │
                           └──────┬──────┘
                                  ▼
                           ┌─────────────┐
                           │  Validacao  │◀── required, aging, status...
                           └──────┬──────┘
                                  ▼
                           ┌─────────────┐
                           │ Tratamento  │◀── CPF, valores, datas
                           └──────┬──────┘
                                  ▼
                    ┌─────────────────────────┐
                    │       BATIMENTO         │
                    │  Cliente ↔ MAX          │
                    │                         │
                    │  A - B = Novos          │
                    │  B - A = Baixas         │
                    └──────────┬──────────────┘
                               ▼
              ┌────────────────┴────────────────┐
              ▼                                 ▼
        ┌───────────┐                     ┌───────────┐
        │   BAIXA   │                     │ DEVOLUCAO │
        │ (p/ MAX)  │                     │ (judicial)│
        └─────┬─────┘                     └─────┬─────┘
              │                                 │
              └─────────────┬───────────────────┘
                            ▼
                    ┌───────────────┐
                    │ Enriquecimento│◀── campos calculados, splits
                    └───────┬───────┘
                            ▼
                    ┌───────────────┐
                    │   EXPORTACAO  │──────▶ [ZIP/CSV]
                    └───────────────┘
```

---

## Documentacao Completa

Para documentacao detalhada de todos os componentes, validadores, processadores e configuracoes, consulte:

**[DOCUMENTATION.md](./DOCUMENTATION.md)**

---

## Suporte

1. Verifique a documentacao completa
2. Consulte logs em `data/logs/`
3. Execute com `--log-level DEBUG`
4. Abra issue no repositorio
