# Análise de Unificação dos Projetos de Automação de Bases

> **Data:** 2025-12-24
> **Versão:** 1.0
> **Objetivo:** Documentar análise completa dos 3 projetos e proposta de arquitetura unificada

---

## Sumário Executivo

Os 3 projetos (Automacao_Vic, Automacao_Tabelionato, Emccamp) compartilham **~70% da estrutura e lógica**. A unificação permitirá:
- Reduzir código duplicado em ~50%
- Facilitar adição de novos clientes (de semanas para horas)
- Centralizar manutenção e correções de bugs
- Padronizar outputs e logs

---

## 1. Inventário dos Projetos Atuais

### 1.1 Automacao_Vic
| Aspecto | Detalhe |
|---------|---------|
| **Fonte Primária** | VIC (extração via email) |
| **Fonte Secundária** | MAX (SQL Server - Candiotto) |
| **Geração de CHAVE** | `NUMERO_CONTRATO-PARCELA` |
| **Filtros Específicos** | Aging, Blacklist, Status |
| **Processadores** | VicProcessor, MaxProcessor, BatimentoProcessor, DevolucaoProcessor, EnriquecimentoVicProcessor |
| **Separação Carteira** | Judicial vs Extrajudicial (por CPF) |
| **Outputs** | vic_tratada, max_tratada, batimento, devolucao, inconsistencias |

### 1.2 Automacao_Tabelionato
| Aspecto | Detalhe |
|---------|---------|
| **Fonte Primária** | Tabelionato (email com ZIP protegido senha) |
| **Fonte Secundária** | MAX (SQL Server via ODBC) |
| **Geração de CHAVE** | `Protocolo` |
| **Filtros Específicos** | Campanhas (58, 78, 94), Aging por protocolo misto |
| **Processadores** | TabelionatoProcessor, TabelionatoMaxProcessor, BatimentoProcessor, BaixaProcessor |
| **Separação Carteira** | Por Campanha |
| **Outputs** | tabelionato_tratada, max_tratada, batimento, baixa, inconsistencias |

### 1.3 Emccamp
| Aspecto | Detalhe |
|---------|---------|
| **Fonte Primária** | EMCCAMP (API REST TOTVS) |
| **Fonte Secundária** | MAX (SQL Server) |
| **Geração de CHAVE** | `CONTRATO-PARCELA` |
| **Filtros Específicos** | TIPO_PAGTO (excluir PERMUTA, etc), Acordos |
| **Processadores** | EmccampProcessor, MaxProcessor, BatimentoProcessor, BaixaProcessor, DevolucaoProcessor, ContactEnrichmentProcessor |
| **Separação Carteira** | Judicial vs Extrajudicial + Com/Sem Recebimento |
| **Outputs** | emccamp_tratada, max_tratada, batimento, baixa, devolucao, enriquecimento_contato |

---

## 2. Análise de Pontos em Comum

### 2.1 Fluxo de Processamento

Todos os projetos seguem o mesmo pipeline conceitual:

```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│  EXTRAÇÃO   │───▶│ TRATAMENTO  │───▶│  BATIMENTO  │───▶│ BAIXA/DEV   │───▶│ENRIQUECIM.  │
│ (Sources)   │    │ (Normalize) │    │ (Anti-Join) │    │ (Retornos)  │    │ (Contatos)  │
└─────────────┘    └─────────────┘    └─────────────┘    └─────────────┘    └─────────────┘
       │                  │                  │                  │                  │
       ▼                  ▼                  ▼                  ▼                  ▼
  - Email            - Validação       - PROCV A-B        - MAX-Source       - Telefones
  - SQL Server       - Normalização    - Dedup            - Judicial/Extra   - Emails
  - API REST         - Formatação      - Exportação       - Recebimentos     - Contatos
  - ZIP com senha    - CHAVE
```

### 2.2 Estrutura de Código Comum

```
projeto/
├── src/
│   ├── config/          # Carregamento de configurações
│   │   ├── __init__.py
│   │   └── loader.py    # ConfigLoader
│   ├── processors/      # Lógica de negócio
│   │   ├── tratamento.py
│   │   ├── max.py
│   │   ├── batimento.py
│   │   ├── baixa.py
│   │   └── devolucao.py
│   ├── scripts/         # Extração de dados
│   │   ├── extrair_*.py
│   │   └── pipeline_cli.py
│   └── utils/           # Utilitários
│       ├── sql_conn.py
│       ├── logger.py
│       ├── anti_join.py
│       ├── text.py
│       └── helpers.py
├── data/
│   ├── input/
│   ├── output/
│   └── logs/
├── config.yaml
├── .env
└── main.py
```

### 2.3 Componentes Compartilháveis (~70%)

| Componente | Função | Compatibilidade |
|------------|--------|-----------------|
| `sql_conn.py` | Conexão SQL Server | 100% igual |
| `logger.py` | Sistema de logging | 100% igual |
| `anti_join.py` | PROCV A-B e B-A | 100% igual |
| `text.py` | `digits_only()`, `normalize_ascii_upper()` | 100% igual |
| `io/DatasetIO` | Leitura/escrita CSV/ZIP | 100% igual |
| `PathManager` | Resolução de caminhos | 100% igual |
| `ConfigLoader` | Carregamento YAML | 95% igual (pequenas variações) |
| `BaseProcessor` | Padrão de processador | 80% igual (interface comum) |
| `BatimentoProcessor` | Anti-join genérico | 85% igual (parametrizável) |
| `OutputFormatter` | Formatação de output console | 100% igual |

### 2.4 Padrões de Configuração

Todos usam `config.yaml` com estrutura similar:

```yaml
global:
  encoding: utf-8-sig
  csv_separator: ";"
  date_format: "%d/%m/%Y"
  empresa:
    nome: CLIENT_NAME
    cnpj: "XX.XXX.XXX/XXXX-XX"

paths:
  input: data/input
  output: data/output
  logs: data/logs

logging:
  level: INFO
  format: "%(asctime)s - %(message)s"

mappings:
  source:
    rename: {...}
    key: {...}
    required: [...]
  max:
    rename: {...}
    key: {...}
```

---

## 3. Análise de Diferenças Específicas

### 3.1 Fontes de Dados (Extração)

| Cliente | Fonte Primária | Método | Autenticação |
|---------|---------------|--------|--------------|
| VIC | Base VIC | Email (attachment ZIP) | Email App Password |
| Tabelionato | Base Tabelionato | Email (attachment ZIP com senha) | Email + ZIP Password |
| EMCCAMP | API TOTVS | REST API | Basic Auth |

**Implicação:** Criar extractors modulares:
- `EmailExtractor` - para VIC e Tabelionato
- `ApiExtractor` - para EMCCAMP
- `SqlExtractor` - para MAX (comum a todos)

### 3.2 Geração de CHAVE

| Cliente | Componentes | Formato | Exemplo |
|---------|-------------|---------|---------|
| VIC | NUMERO_CONTRATO + PARCELA | `{contrato}-{parcela}` | `12345-01` |
| Tabelionato | Protocolo | `{protocolo}` | `ABC123456` |
| EMCCAMP | CONTRATO + PARCELA | `{contrato}-{parcela}` | `67890-02` |

**Implicação:** Configuração de key_generator por cliente:
```yaml
key:
  components: [CONTRATO, PARCELA]
  separator: "-"
  # ou
  use_column: Protocolo
```

### 3.3 Regras de Validação

| Cliente | Validações Específicas |
|---------|----------------------|
| VIC | Aging (dias desde vencimento), Blacklist de CPFs |
| Tabelionato | DtAnuencia válida, Campanhas (58/78/94), Aging misto por protocolo |
| EMCCAMP | CHAVE duplicada, TIPO_PAGTO, Acordos vigentes |

**Implicação:** Validators configuráveis:
```yaml
validators:
  - type: aging
    field: VENCIMENTO
    reference: today
    min_days: 0
  - type: blacklist
    source: data/input/blacklist/cpfs.csv
    field: CPF_CNPJ
  - type: exclude_values
    field: TIPO_PAGTO
    values: [PERMUTA, "Financiamento Fixo"]
```

### 3.4 Separação de Carteira

| Cliente | Critério | Outputs |
|---------|----------|---------|
| VIC | CPF em lista judicial | judicial, extrajudicial |
| Tabelionato | Campanha (58, 78, 94) | campanha_58, campanha_78, campanha_94 |
| EMCCAMP | CPF judicial + Recebimento | judicial, extrajudicial, com_receb, sem_receb |

**Implicação:** Splitters configuráveis:
```yaml
output_split:
  - type: judicial_cpf
    source: data/input/judicial/ClientesJudiciais.zip
    outputs:
      match: judicial
      no_match: extrajudicial
  - type: field_value
    field: Campanha
    outputs:
      "Campanha 58": campanha_58
      "Campanha 78": campanha_78
```

### 3.5 Layouts de Exportação

Cada cliente tem layout específico de colunas para output:

**VIC - Batimento:**
```
CPFCNPJ CLIENTE, NOME RAZAO SOCIAL, NUMERO CONTRATO, PARCELA, VENCIMENTO, VALOR, TIPO PARCELA
```

**EMCCAMP - Baixa:**
```
CNPJ CREDOR, CPF/CNPJ CLIENTE, NOME CLIENTE, NUMERO DOC, DT. VENCIMENTO, VALOR DA PARCELA, STATUS ACORDO, DT. PAGAMENTO, VALOR RECEBIDO
```

**Implicação:** Layouts configuráveis por stage:
```yaml
layouts:
  batimento:
    columns:
      - name: "CPFCNPJ CLIENTE"
        source: CPF_CNPJ
      - name: "NOME RAZAO SOCIAL"
        source: NOME
        fallback: CLIENTE
```

---

## 4. Arquitetura Unificada Proposta

### 4.1 Estrutura de Diretórios

```
Sistema-Bases/
├── src/
│   ├── core/                      # Componentes genéricos reutilizáveis
│   │   ├── __init__.py
│   │   ├── config/
│   │   │   ├── __init__.py
│   │   │   ├── loader.py          # ConfigLoader universal
│   │   │   ├── registry.py        # ClientRegistry
│   │   │   └── models.py          # ClientConfig, ProcessorConfig
│   │   │
│   │   ├── extractors/
│   │   │   ├── __init__.py
│   │   │   ├── base.py            # BaseExtractor (ABC)
│   │   │   ├── email.py           # EmailExtractor
│   │   │   ├── sql.py             # SqlExtractor
│   │   │   └── api.py             # ApiExtractor
│   │   │
│   │   ├── processors/
│   │   │   ├── __init__.py
│   │   │   ├── base.py            # BaseProcessor (ABC)
│   │   │   ├── treatment.py       # TreatmentProcessor
│   │   │   ├── max.py             # MaxProcessor
│   │   │   ├── batimento.py       # BatimentoProcessor
│   │   │   ├── baixa.py           # BaixaProcessor
│   │   │   ├── devolucao.py       # DevolucaoProcessor
│   │   │   └── enrichment.py      # EnrichmentProcessor
│   │   │
│   │   ├── validators/
│   │   │   ├── __init__.py
│   │   │   ├── base.py            # BaseValidator (ABC)
│   │   │   ├── aging.py           # AgingValidator
│   │   │   ├── blacklist.py       # BlacklistValidator
│   │   │   ├── required.py        # RequiredFieldsValidator
│   │   │   └── regex.py           # RegexValidator
│   │   │
│   │   ├── splitters/
│   │   │   ├── __init__.py
│   │   │   ├── base.py            # BaseSplitter (ABC)
│   │   │   ├── judicial.py        # JudicialSplitter
│   │   │   ├── campaign.py        # CampaignSplitter
│   │   │   └── field_value.py     # FieldValueSplitter
│   │   │
│   │   ├── io/
│   │   │   ├── __init__.py
│   │   │   ├── dataset.py         # DatasetIO
│   │   │   ├── file_manager.py    # FileManager
│   │   │   └── packager.py        # ZipPackager
│   │   │
│   │   └── utils/
│   │       ├── __init__.py
│   │       ├── sql_conn.py
│   │       ├── logger.py
│   │       ├── anti_join.py
│   │       ├── text.py
│   │       ├── helpers.py
│   │       └── output_formatter.py
│   │
│   ├── clients/                   # Implementações específicas por cliente
│   │   ├── __init__.py
│   │   ├── vic/
│   │   │   ├── __init__.py
│   │   │   ├── config.yaml        # Configuração específica VIC
│   │   │   ├── extractors.py      # VicEmailExtractor (override)
│   │   │   └── validators.py      # VicAgingValidator, VicBlacklistValidator
│   │   │
│   │   ├── tabelionato/
│   │   │   ├── __init__.py
│   │   │   ├── config.yaml        # Configuração específica Tabelionato
│   │   │   ├── extractors.py      # TabelionatoZipExtractor (ZIP com senha)
│   │   │   └── validators.py      # CampaignValidator, MixedAgingValidator
│   │   │
│   │   └── emccamp/
│   │       ├── __init__.py
│   │       ├── config.yaml        # Configuração específica EMCCAMP
│   │       ├── extractors.py      # TotvsApiExtractor
│   │       └── validators.py      # AcordoValidator, TipoPagtoValidator
│   │
│   ├── pipeline/
│   │   ├── __init__.py
│   │   ├── orchestrator.py        # PipelineOrchestrator
│   │   ├── runner.py              # PipelineRunner
│   │   └── stages.py              # Stage definitions
│   │
│   └── cli/
│       ├── __init__.py
│       ├── main.py                # Entry point
│       └── commands.py            # CLI commands
│
├── config/
│   ├── base.yaml                  # Configurações base compartilhadas
│   └── clients/                   # Symlinks ou includes dos configs
│
├── data/
│   ├── vic/
│   │   ├── input/
│   │   │   ├── vic/
│   │   │   ├── max/
│   │   │   ├── judicial/
│   │   │   └── blacklist/
│   │   ├── output/
│   │   │   ├── vic_tratada/
│   │   │   ├── max_tratada/
│   │   │   ├── batimento/
│   │   │   ├── devolucao/
│   │   │   └── inconsistencias/
│   │   └── logs/
│   │
│   ├── tabelionato/
│   │   ├── input/
│   │   ├── output/
│   │   └── logs/
│   │
│   └── emccamp/
│       ├── input/
│       ├── output/
│       └── logs/
│
├── tests/
│   ├── core/
│   ├── clients/
│   └── integration/
│
├── docs/
│   ├── README.md
│   ├── ARCHITECTURE.md
│   ├── ADDING_NEW_CLIENT.md
│   └── API_REFERENCE.md
│
├── scripts/
│   ├── setup_project.bat
│   ├── run_pipeline.bat
│   └── add_client.py
│
├── requirements.txt
├── pyproject.toml
├── .env.example
└── main.py
```

### 4.2 Componentes Core

#### 4.2.1 BaseProcessor (Abstract)

```python
from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Any, Tuple
import pandas as pd

@dataclass
class ProcessorStats:
    registros_entrada: int
    registros_saida: int
    inconsistencias: int
    arquivo_saida: Path
    duracao: float
    extras: Dict[str, Any] = None

class BaseProcessor(ABC):
    """Classe base para todos os processadores."""

    def __init__(self, config: 'ClientConfig'):
        self.config = config
        self.logger = config.get_logger(self.__class__.__name__)
        self.io = config.get_io()
        self.paths = config.get_paths()

    def process(self, entrada: Path) -> ProcessorStats:
        """Template method para processamento."""
        inicio = time.time()

        # 1. Carregar dados
        df_raw = self.load(entrada)

        # 2. Normalizar/mapear colunas
        df_norm = self.normalize(df_raw)

        # 3. Validar dados
        df_valid, df_invalid = self.validate(df_norm)

        # 4. Transformar (regras de negócio)
        df_transformed = self.transform(df_valid)

        # 5. Exportar
        arquivo_saida = self.export(df_transformed, df_invalid)

        return ProcessorStats(
            registros_entrada=len(df_raw),
            registros_saida=len(df_transformed),
            inconsistencias=len(df_invalid),
            arquivo_saida=arquivo_saida,
            duracao=time.time() - inicio
        )

    def load(self, entrada: Path) -> pd.DataFrame:
        """Carrega dados do arquivo de entrada."""
        return self.io.read(entrada)

    def normalize(self, df: pd.DataFrame) -> pd.DataFrame:
        """Aplica mapeamento de colunas e normalização."""
        mapping = self.config.get_mapping(self.source_name)
        return self._apply_mapping(df, mapping)

    @abstractmethod
    def validate(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Valida dados. Retorna (válidos, inválidos)."""
        pass

    @abstractmethod
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Aplica transformações específicas."""
        pass

    def export(self, df: pd.DataFrame, df_invalid: pd.DataFrame) -> Path:
        """Exporta dados processados."""
        output_dir = self.paths.resolve_output(self.output_name)
        return self.io.write_zip(df, output_dir / f"{self.output_name}.zip")
```

#### 4.2.2 TreatmentProcessor (Genérico)

```python
class TreatmentProcessor(BaseProcessor):
    """Processador genérico de tratamento."""

    def __init__(self, config: 'ClientConfig', source_name: str):
        super().__init__(config)
        self.source_name = source_name
        self.output_name = f"{source_name}_tratada"

        # Carregar configuração específica
        self.mapping = config.get_mapping(source_name)
        self.validators = self._load_validators()
        self.key_config = self.mapping.get('key', {})

    def _load_validators(self) -> List[BaseValidator]:
        """Carrega validators configurados."""
        validators = []
        for v_config in self.config.get('validators', []):
            validator_class = ValidatorRegistry.get(v_config['type'])
            validators.append(validator_class(v_config))
        return validators

    def validate(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Aplica todos os validators configurados."""
        invalid_mask = pd.Series(False, index=df.index)

        for validator in self.validators:
            mask = validator.validate(df)
            invalid_mask |= mask

        return df[~invalid_mask].copy(), df[invalid_mask].copy()

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Gera CHAVE e aplica transformações."""
        df = self._generate_key(df)
        df = self._apply_defaults(df)
        return df

    def _generate_key(self, df: pd.DataFrame) -> pd.DataFrame:
        """Gera coluna CHAVE conforme configuração."""
        if 'use_column' in self.key_config:
            df['CHAVE'] = df[self.key_config['use_column']].astype(str).str.strip()
        else:
            components = self.key_config.get('components', [])
            sep = self.key_config.get('separator', '-')
            parts = [df[col].astype(str).str.strip() for col in components]
            df['CHAVE'] = parts[0]
            for part in parts[1:]:
                df['CHAVE'] = df['CHAVE'] + sep + part
        return df
```

#### 4.2.3 BatimentoProcessor (Genérico)

```python
class BatimentoProcessor(BaseProcessor):
    """Processador genérico de batimento (anti-join)."""

    def __init__(self, config: 'ClientConfig'):
        super().__init__(config)
        self.batimento_config = config.get('batimento', {})

    def process(self) -> BatimentoStats:
        """Executa batimento A - B."""
        # Carregar bases tratadas
        source_path = self._resolve_source()
        target_path = self._resolve_target()

        df_source = self.io.read(source_path)
        df_target = self.io.read(target_path)

        # Aplicar filtros pré-batimento
        df_source = self._apply_pre_filters(df_source, 'source')
        df_target = self._apply_pre_filters(df_target, 'target')

        # Deduplicar target se configurado
        if self.batimento_config.get('dedup_target', True):
            df_target = self._deduplicate(df_target)

        # Anti-join: source - target
        source_key = self.batimento_config.get('source_key', 'CHAVE')
        target_key = self.batimento_config.get('target_key', 'CHAVE')
        df_batimento = procv_a_menos_b(df_source, df_target, source_key, target_key)

        # Separar por carteira
        outputs = self._split_output(df_batimento)

        # Exportar
        return self._export(outputs)

    def _split_output(self, df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        """Separa output por splitters configurados."""
        splitters = self._load_splitters()
        if not splitters:
            return {'geral': df}

        outputs = {}
        for splitter in splitters:
            split_result = splitter.split(df)
            outputs.update(split_result)
        return outputs
```

#### 4.2.4 PipelineOrchestrator

```python
class PipelineOrchestrator:
    """Orquestrador central do pipeline."""

    def __init__(self, client_name: str):
        self.client_name = client_name
        self.config = ConfigLoader().load_client(client_name)
        self.stages = self._build_stages()

    def _build_stages(self) -> Dict[str, Callable]:
        """Constrói stages do pipeline."""
        return {
            'extract': self._run_extraction,
            'treat': self._run_treatment,
            'batimento': self._run_batimento,
            'baixa': self._run_baixa,
            'devolucao': self._run_devolucao,
            'enriquecimento': self._run_enriquecimento,
        }

    def run(self, stages: List[str] = None) -> Dict[str, Any]:
        """Executa stages especificados ou todos."""
        stages = stages or ['extract', 'treat', 'batimento', 'baixa']
        results = {}

        for stage_name in stages:
            if stage_name in self.stages:
                self.logger.info(f"Executando stage: {stage_name}")
                results[stage_name] = self.stages[stage_name]()

        return results

    def run_full(self) -> Dict[str, Any]:
        """Executa pipeline completo."""
        return self.run(['extract', 'treat', 'batimento', 'baixa', 'devolucao'])
```

### 4.3 Configuração por Cliente

#### 4.3.1 base.yaml (Compartilhado)

```yaml
# config/base.yaml
global:
  encoding: utf-8-sig
  csv_separator: ";"
  date_format: "%d/%m/%Y"

logging:
  level: INFO
  format: "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

processors:
  treatment:
    class: src.core.processors.TreatmentProcessor
  max:
    class: src.core.processors.MaxProcessor
  batimento:
    class: src.core.processors.BatimentoProcessor
  baixa:
    class: src.core.processors.BaixaProcessor
  devolucao:
    class: src.core.processors.DevolucaoProcessor

validators:
  available:
    - required_fields
    - aging
    - blacklist
    - regex
    - exclude_values

splitters:
  available:
    - judicial_cpf
    - campaign
    - field_value
```

#### 4.3.2 Exemplo: vic/config.yaml

```yaml
# src/clients/vic/config.yaml
client:
  name: vic
  display_name: VIC Candiotto

empresa:
  nome: VIC
  cnpj: "XX.XXX.XXX/XXXX-XX"

paths:
  data_root: data/vic
  input:
    vic: input/vic
    max: input/max
    judicial: input/judicial
    blacklist: input/blacklist
  output:
    vic_tratada: output/vic_tratada
    max_tratada: output/max_tratada
    batimento: output/batimento
    devolucao: output/devolucao
    inconsistencias: output/inconsistencias

extractors:
  vic:
    type: email
    class: src.clients.vic.extractors.VicEmailExtractor
    config:
      subject_filter: "Base VIC"
      attachment_pattern: "*.zip"
  max:
    type: sql
    class: src.core.extractors.SqlExtractor
    config:
      query_template: max
      params:
        mo_cliente_id: 35268

mappings:
  vic:
    rename:
      CPFCNPJ: CPFCNPJ_CLIENTE
      "NOME/RAZÃO SOCIAL": NOME_RAZAO_SOCIAL
      "NÚMERO DO CONTRATO": NUMERO_CONTRATO
      PARCEL: PARCELA
      "DATA VENCIMENTO": VENCIMENTO
      VALOR: VALOR
    key:
      components: [NUMERO_CONTRATO, PARCELA]
      separator: "-"
    required:
      - CPFCNPJ_CLIENTE
      - NUMERO_CONTRATO
      - PARCELA
      - VENCIMENTO
      - VALOR

  max:
    rename:
      CPFCNPJ_CLIENTE: CPF_CNPJ
      # ... etc
    key:
      use_parcela_as_chave: true
    validation:
      parcela_regex: '^[0-9]{3,}-[0-9]{2,}$'

validators:
  vic:
    - type: required_fields
      fields: [CPFCNPJ_CLIENTE, VENCIMENTO]
    - type: aging
      field: VENCIMENTO
      min_days: 0
      max_days: 1800
    - type: blacklist
      source: input/blacklist/cpfs.csv
      field: CPFCNPJ_CLIENTE

batimento:
  source: vic_tratada
  target: max_tratada
  source_key: CHAVE
  target_key: CHAVE
  dedup_target: true

  output_split:
    - type: judicial_cpf
      source: input/judicial/ClientesJudiciais.zip
      outputs:
        match: judicial
        no_match: extrajudicial

layouts:
  batimento:
    columns:
      - name: "CPFCNPJ CLIENTE"
        source: CPFCNPJ_CLIENTE
      - name: "NOME / RAZAO SOCIAL"
        source: NOME_RAZAO_SOCIAL
      - name: "NUMERO CONTRATO"
        source: NUMERO_CONTRATO
      - name: "PARCELA"
        source: CHAVE
      - name: "VENCIMENTO"
        source: VENCIMENTO
        format: date
      - name: "VALOR"
        source: VALOR
        format: currency
      - name: "TIPO PARCELA"
        source: TIPO_PARCELA
```

### 4.4 CLI Interface

```python
# main.py
import argparse
from src.pipeline.orchestrator import PipelineOrchestrator
from src.core.config.registry import ClientRegistry

def main():
    parser = argparse.ArgumentParser(description="Sistema de Bases Unificado")
    parser.add_argument('--client', '-c', required=True,
                        choices=ClientRegistry.list_clients(),
                        help='Cliente a processar')

    subparsers = parser.add_subparsers(dest='command', required=True)

    # Comandos
    subparsers.add_parser('extract', help='Executar extração')
    subparsers.add_parser('treat', help='Executar tratamento')
    subparsers.add_parser('batimento', help='Executar batimento')
    subparsers.add_parser('baixa', help='Executar baixa')
    subparsers.add_parser('devolucao', help='Executar devolução')
    subparsers.add_parser('full', help='Pipeline completo')

    args = parser.parse_args()

    orchestrator = PipelineOrchestrator(args.client)

    if args.command == 'full':
        orchestrator.run_full()
    else:
        orchestrator.run([args.command])

if __name__ == '__main__':
    main()
```

**Uso:**
```bash
# Pipeline completo para VIC
python main.py --client vic full

# Apenas batimento para EMCCAMP
python main.py --client emccamp batimento

# Extração para Tabelionato
python main.py --client tabelionato extract
```

---

## 5. Guia para Adicionar Novo Cliente

### 5.1 Passos

1. **Criar diretório do cliente:**
   ```bash
   mkdir -p src/clients/novo_cliente
   touch src/clients/novo_cliente/__init__.py
   ```

2. **Criar config.yaml:**
   ```yaml
   # src/clients/novo_cliente/config.yaml
   client:
     name: novo_cliente
     display_name: Novo Cliente

   empresa:
     nome: NOVO CLIENTE
     cnpj: "XX.XXX.XXX/XXXX-XX"

   paths:
     data_root: data/novo_cliente
     # ... definir paths

   extractors:
     # Usar extractor existente ou criar novo
     source:
       type: email  # ou sql, api
       class: src.core.extractors.EmailExtractor
       config:
         # configurações específicas

   mappings:
     source:
       rename:
         COLUNA_ORIGEM: COLUNA_DESTINO
       key:
         components: [CAMPO1, CAMPO2]
         separator: "-"
       required:
         - CAMPO1
         - CAMPO2

   validators:
     source:
       - type: required_fields
         fields: [CAMPO1, CAMPO2]

   batimento:
     source: source_tratada
     target: max_tratada
     source_key: CHAVE
     target_key: CHAVE
   ```

3. **(Opcional) Criar extractors específicos:**
   ```python
   # src/clients/novo_cliente/extractors.py
   from src.core.extractors.base import BaseExtractor

   class NovoClienteExtractor(BaseExtractor):
       # Implementar se necessário override
       pass
   ```

4. **(Opcional) Criar validators específicos:**
   ```python
   # src/clients/novo_cliente/validators.py
   from src.core.validators.base import BaseValidator

   class NovoClienteValidator(BaseValidator):
       # Implementar validação específica
       pass
   ```

5. **Criar estrutura de dados:**
   ```bash
   mkdir -p data/novo_cliente/{input,output,logs}
   mkdir -p data/novo_cliente/input/{source,max,judicial}
   mkdir -p data/novo_cliente/output/{source_tratada,max_tratada,batimento,baixa}
   ```

6. **Registrar cliente:**
   ```python
   # src/clients/__init__.py
   from .novo_cliente import config as novo_cliente_config

   CLIENTS = {
       'vic': ...,
       'tabelionato': ...,
       'emccamp': ...,
       'novo_cliente': novo_cliente_config,
   }
   ```

### 5.2 Tempo Estimado

| Complexidade | Descrição | Tempo |
|--------------|-----------|-------|
| **Simples** | Mesma fonte de dados (email/SQL/API existente), validações padrão | 2-4 horas |
| **Média** | Nova fonte de dados similar, algumas validações customizadas | 1-2 dias |
| **Complexa** | Nova fonte de dados, muitas regras de negócio específicas | 3-5 dias |

---

## 6. Plano de Migração

### Fase 1: Preparação (1-2 dias)
- [ ] Criar estrutura de diretórios
- [ ] Implementar componentes core (BaseProcessor, ConfigLoader, etc.)
- [ ] Implementar utilities compartilhados
- [ ] Criar testes unitários básicos

### Fase 2: Core Processors (3-5 dias)
- [ ] TreatmentProcessor genérico
- [ ] MaxProcessor genérico
- [ ] BatimentoProcessor genérico
- [ ] BaixaProcessor genérico
- [ ] DevolucaoProcessor genérico
- [ ] Validators base (required, aging, blacklist, regex)
- [ ] Splitters base (judicial, campaign, field_value)

### Fase 3: Migrar VIC (2-3 dias)
- [ ] Criar config.yaml VIC
- [ ] Migrar VicEmailExtractor
- [ ] Migrar validadores específicos
- [ ] Testar pipeline completo
- [ ] Comparar outputs com versão antiga

### Fase 4: Migrar Tabelionato (2-3 dias)
- [ ] Criar config.yaml Tabelionato
- [ ] Migrar TabelionatoZipExtractor (senha)
- [ ] Migrar CampaignValidator
- [ ] Testar pipeline completo
- [ ] Comparar outputs

### Fase 5: Migrar EMCCAMP (2-3 dias)
- [ ] Criar config.yaml EMCCAMP
- [ ] Migrar TotvsApiExtractor
- [ ] Migrar validators específicos
- [ ] Testar pipeline completo
- [ ] Comparar outputs

### Fase 6: Validação e Documentação (2-3 dias)
- [ ] Testes de integração completos
- [ ] Documentação de uso
- [ ] Documentação de adição de novos clientes
- [ ] Cleanup dos projetos antigos

**Total Estimado:** 2-3 semanas

---

## 7. Benefícios da Unificação

| Aspecto | Antes | Depois |
|---------|-------|--------|
| **Código duplicado** | ~70% | ~10% |
| **Arquivos Python** | ~85 | ~40 |
| **Tempo para novo cliente** | 2-3 semanas | 1-3 dias |
| **Manutenção de bugs** | 3 lugares | 1 lugar |
| **Consistência de outputs** | Variável | Padronizado |
| **Testes** | Dispersos | Centralizados |
| **Documentação** | 3 READMEs | 1 documentação completa |

---

## 8. Conclusão

A análise revela que os 3 projetos são fundamentalmente variações do mesmo sistema com fontes de dados e regras de negócio diferentes. A unificação proposta:

1. **Mantém a flexibilidade** para regras específicas de cada cliente
2. **Centraliza componentes comuns** reduzindo duplicação
3. **Facilita manutenção** com código em um só lugar
4. **Acelera adição de novos clientes** com estrutura plug-and-play
5. **Padroniza outputs e logs** para consistência operacional

A arquitetura proposta segue princípios SOLID e padrões de design (Template Method, Strategy, Registry) que garantem extensibilidade e manutenibilidade a longo prazo.
