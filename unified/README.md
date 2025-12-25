# Unified Pipeline System

Sistema unificado para processamento de dados de clientes, combinando os projetos Automacao_Vic, Automacao_Tabelionato e Emccamp em uma arquitetura híbrida e extensível.

## Arquitetura

```
unified/
├── configs/
│   └── clients/          # Configurações YAML por cliente
│       ├── vic.yaml
│       ├── emccamp.yaml
│       └── tabelionato.yaml
├── src/
│   ├── core/             # Engine, schemas, configuração
│   ├── loaders/          # Carregadores de dados (file, email, sql, api)
│   ├── validators/       # Validadores (required, aging, blacklist, regex)
│   ├── splitters/        # Divisores (judicial, campaign, field_value)
│   ├── processors/       # Processadores (tratamento, batimento, baixa, etc)
│   ├── api/              # REST API para integrações
│   └── cli.py            # Interface de linha de comando
└── tests/                # Testes automatizados
```

## Instalação

```bash
# Instalar dependências
pip install -r requirements.txt

# Ou com suporte a SQL Server
pip install -r requirements.txt pyodbc pymssql
```

## Uso

### CLI

```bash
# Listar clientes disponíveis
python -m unified.src.cli list

# Validar configuração de um cliente
python -m unified.src.cli validate vic

# Executar pipeline para um cliente
python -m unified.src.cli run vic --output-dir ./output

# Com logs detalhados
python -m unified.src.cli run vic --log-level DEBUG --log-file ./logs/vic.log
```

### API REST

```bash
# Iniciar servidor
python -m unified.src.api.app

# Endpoints disponíveis:
# GET  /health           - Health check
# GET  /clients          - Listar clientes
# GET  /clients/{name}   - Detalhes do cliente
# POST /run/{name}       - Executar pipeline
# POST /validate/{name}  - Validar configuração
```

### Programático

```python
from unified.src.core import PipelineEngine, ProcessorType
from unified.src.processors import (
    TratamentoProcessor,
    BatimentoProcessor,
    BaixaProcessor,
    DevolucaoProcessor,
    EnriquecimentoProcessor,
)

# Criar engine
engine = PipelineEngine(
    config_dir="./configs/clients",
    output_dir="./output"
)

# Registrar processadores
engine.register_processor(ProcessorType.TRATAMENTO, TratamentoProcessor)
engine.register_processor(ProcessorType.BATIMENTO, BatimentoProcessor)
engine.register_processor(ProcessorType.BAIXA, BaixaProcessor)
engine.register_processor(ProcessorType.DEVOLUCAO, DevolucaoProcessor)
engine.register_processor(ProcessorType.ENRIQUECIMENTO, EnriquecimentoProcessor)

# Executar
result = engine.run("vic")

print(f"Success: {result.success}")
print(f"Duration: {result.duration_seconds:.2f}s")
print(f"Outputs: {result.context.outputs}")
```

## Configuração de Clientes

Cada cliente é configurado via arquivo YAML. Exemplo mínimo:

```yaml
name: meu_cliente
version: "1.0"
description: "Descrição do cliente"

client_source:
  loader:
    type: file
    params:
      path: "./data/cliente.csv"
  key:
    type: composite
    components: [CPF_CNPJ, CONTRATO]
    separator: "-"

max_source:
  loader:
    type: sql
    params:
      query: "SELECT * FROM carteira"
  key:
    type: column
    column: CHAVE

pipeline:
  processors:
    - type: tratamento
    - type: batimento
    - type: enriquecimento
```

## Extensões

Para regras complexas, crie uma classe de extensão:

```python
from unified.src.core import BaseClientExtension

class MeuClienteExtension(BaseClientExtension):
    def pre_process(self, df, source):
        # Lógica customizada antes do processamento
        return df

    def custom_validation(self, df):
        # Validação customizada
        return ValidationResult(valid=df, invalid=pd.DataFrame(), errors=[])
```

E registre no engine:

```python
engine.register_extension("meu_cliente", MeuClienteExtension)
```

## Integração com n8n

A API REST pode ser chamada diretamente do n8n usando HTTP Request nodes:

1. **Listar clientes**: `GET http://localhost:5000/clients`
2. **Executar pipeline**: `POST http://localhost:5000/run/{cliente}`
3. **Verificar resultado**: Resposta JSON com status e arquivos gerados

## Variáveis de Ambiente

```bash
# SQL Server
SQL_SERVER=servidor
SQL_DATABASE=banco
SQL_USER=usuario
SQL_PASSWORD=senha

# Email (IMAP)
EMAIL_SERVER=imap.servidor.com
EMAIL_PORT=993
EMAIL_USER=usuario@email.com
EMAIL_PASSWORD=senha

# API (TOTVS)
TOTVS_API_URL=https://api.totvs.com
TOTVS_API_TOKEN=token

# Arquivos judiciais
JUDICIAL_FILE_PATH=./data/judicial.zip
```

## Fluxo de Processamento

```
┌─────────────────┐
│   Carregamento  │  ← Loaders (file, email, sql, api)
└────────┬────────┘
         ▼
┌─────────────────┐
│  Geração CHAVE  │  ← KeyGenerators (composite, column)
└────────┬────────┘
         ▼
┌─────────────────┐
│   Validação     │  ← Validators (required, aging, blacklist)
└────────┬────────┘
         ▼
┌─────────────────┐
│   Tratamento    │  ← Limpeza e normalização
└────────┬────────┘
         ▼
┌─────────────────┐
│   Batimento     │  ← Anti-join (novos, baixas)
└────────┬────────┘
         ▼
┌─────────────────┐
│  Baixa/Devolução│  ← Separação judicial/extrajudicial
└────────┬────────┘
         ▼
┌─────────────────┐
│  Enriquecimento │  ← Formatação final + splits
└────────┬────────┘
         ▼
┌─────────────────┐
│   Exportação    │  ← Arquivos ZIP/CSV
└─────────────────┘
```

## Adicionando Novo Cliente

1. Crie `configs/clients/novo_cliente.yaml`
2. Configure as fontes de dados (client_source, max_source)
3. Defina os processadores no pipeline
4. Teste: `python -m unified.src.cli validate novo_cliente`
5. Execute: `python -m unified.src.cli run novo_cliente`

Para regras complexas, adicione uma classe de extensão.
