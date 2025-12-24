# Arquitetura Técnica Detalhada

## 1. Visão Geral dos Padrões de Design

```
┌────────────────────────────────────────────────────────────────────────────┐
│                           PADRÕES UTILIZADOS                                │
├────────────────────────────────────────────────────────────────────────────┤
│ • Template Method  → BaseProcessor.process() define o fluxo               │
│ • Strategy         → Validators/Extractors intercambiáveis                │
│ • Factory          → ClientRegistry cria instâncias de clientes           │
│ • Composition      → Cliente composto por Extractors + Processors         │
│ • Registry         → Registro central de clientes disponíveis             │
└────────────────────────────────────────────────────────────────────────────┘
```

---

## 2. Hierarquia de Classes

### 2.1 Diagrama de Classes Principal

```
                                    ┌─────────────────────┐
                                    │   ClientRegistry    │
                                    │   (Singleton)       │
                                    ├─────────────────────┤
                                    │ + register(client)  │
                                    │ + get(name) → Client│
                                    │ + list() → [names]  │
                                    └──────────┬──────────┘
                                               │ cria
                                               ▼
┌──────────────────────────────────────────────────────────────────────────┐
│                            BaseClient (ABC)                               │
├──────────────────────────────────────────────────────────────────────────┤
│ Atributos:                                                                │
│   - name: str                          # identificador único              │
│   - display_name: str                  # nome para exibição               │
│   - config: ClientConfig               # configurações carregadas         │
│   - _extractors: Dict[str, Extractor]  # cache de extractors              │
│   - _processors: Dict[str, Processor]  # cache de processors              │
├──────────────────────────────────────────────────────────────────────────┤
│ Métodos Abstratos (cada cliente DEVE implementar):                        │
│   + configure_extractors() → Dict[str, ExtractorConfig]                   │
│   + configure_processors() → Dict[str, ProcessorConfig]                   │
│   + get_key_generator() → KeyGenerator                                    │
├──────────────────────────────────────────────────────────────────────────┤
│ Métodos Concretos (herdados):                                             │
│   + get_extractor(name: str) → BaseExtractor                              │
│   + get_processor(name: str) → BaseProcessor                              │
│   + get_pipeline() → Pipeline                                             │
│   + run_stage(stage: str) → StageResult                                   │
│   + run_full() → PipelineResult                                           │
└──────────────────────────────────────────────────────────────────────────┘
                                       ▲
                                       │ extends
           ┌───────────────────────────┼───────────────────────────┐
           │                           │                           │
┌──────────┴──────────┐    ┌──────────┴──────────┐    ┌──────────┴──────────┐
│     VicClient       │    │  TabelionatoClient  │    │   EmccampClient     │
├─────────────────────┤    ├─────────────────────┤    ├─────────────────────┤
│ name = "vic"        │    │ name = "tabelionato"│    │ name = "emccamp"    │
├─────────────────────┤    ├─────────────────────┤    ├─────────────────────┤
│ Extractors:         │    │ Extractors:         │    │ Extractors:         │
│  - VicEmailExtract  │    │  - TabZipExtractor  │    │  - TotvsApiExtract  │
│  - SqlExtractor     │    │  - SqlExtractor     │    │  - SqlExtractor     │
├─────────────────────┤    ├─────────────────────┤    ├─────────────────────┤
│ Validators:         │    │ Validators:         │    │ Validators:         │
│  - AgingValidator   │    │  - CampaignValid    │    │  - TipoPagtoValid   │
│  - BlacklistValid   │    │  - MixedAgingValid  │    │  - AcordoValidator  │
├─────────────────────┤    ├─────────────────────┤    ├─────────────────────┤
│ KeyGenerator:       │    │ KeyGenerator:       │    │ KeyGenerator:       │
│  CONTRATO-PARCELA   │    │  PROTOCOLO          │    │  CONTRATO-PARCELA   │
├─────────────────────┤    ├─────────────────────┤    ├─────────────────────┤
│ Splitters:          │    │ Splitters:          │    │ Splitters:          │
│  - JudicialSplitter │    │  - CampaignSplitter │    │  - JudicialSplitter │
│                     │    │                     │    │  - RecebimentoSplit │
└─────────────────────┘    └─────────────────────┘    └─────────────────────┘
```

---

## 3. Implementação Detalhada

### 3.1 BaseClient - Classe Base

```python
# src/core/client/base.py

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Type, Any

from src.core.config import ClientConfig, ConfigLoader
from src.core.extractors import BaseExtractor
from src.core.processors import BaseProcessor
from src.core.validators import BaseValidator
from src.core.pipeline import Pipeline, StageResult


@dataclass
class ExtractorConfig:
    """Configuração de um extractor."""
    extractor_class: Type[BaseExtractor]
    params: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ProcessorConfig:
    """Configuração de um processor."""
    processor_class: Type[BaseProcessor]
    validators: List[BaseValidator] = field(default_factory=list)
    params: Dict[str, Any] = field(default_factory=dict)


class BaseClient(ABC):
    """
    Classe base abstrata para todos os clientes.

    Cada cliente representa uma fonte de dados específica com suas
    próprias regras de extração, tratamento e validação.

    Responsabilidades:
    - Configurar extractors específicos
    - Configurar processors com validators apropriados
    - Definir como a CHAVE é gerada
    - Fornecer pipeline configurado para execução
    """

    def __init__(self, config_path: Optional[Path] = None):
        """
        Inicializa o cliente.

        Args:
            config_path: Caminho para config.yaml do cliente.
                        Se None, usa caminho padrão baseado no name.
        """
        self._config: Optional[ClientConfig] = None
        self._config_path = config_path
        self._extractors: Dict[str, BaseExtractor] = {}
        self._processors: Dict[str, BaseProcessor] = {}
        self._pipeline: Optional[Pipeline] = None

    # =========================================================================
    # PROPRIEDADES ABSTRATAS - Cada cliente DEVE definir
    # =========================================================================

    @property
    @abstractmethod
    def name(self) -> str:
        """Identificador único do cliente (ex: 'vic', 'emccamp')."""
        pass

    @property
    @abstractmethod
    def display_name(self) -> str:
        """Nome para exibição (ex: 'VIC Candiotto', 'EMCCAMP')."""
        pass

    # =========================================================================
    # MÉTODOS ABSTRATOS - Cada cliente DEVE implementar
    # =========================================================================

    @abstractmethod
    def configure_extractors(self) -> Dict[str, ExtractorConfig]:
        """
        Configura os extractors do cliente.

        Returns:
            Dicionário mapeando nome do dataset para ExtractorConfig.

        Exemplo:
            {
                'source': ExtractorConfig(
                    extractor_class=VicEmailExtractor,
                    params={'subject_filter': 'Base VIC'}
                ),
                'max': ExtractorConfig(
                    extractor_class=SqlExtractor,
                    params={'query': 'max', 'cliente_id': 35268}
                ),
            }
        """
        pass

    @abstractmethod
    def configure_processors(self) -> Dict[str, ProcessorConfig]:
        """
        Configura os processors do cliente.

        Returns:
            Dicionário mapeando nome do stage para ProcessorConfig.

        Exemplo:
            {
                'treatment': ProcessorConfig(
                    processor_class=TreatmentProcessor,
                    validators=[AgingValidator(max_days=1800)],
                    params={'source': 'vic'}
                ),
                'batimento': ProcessorConfig(
                    processor_class=BatimentoProcessor,
                    params={'source_key': 'CHAVE', 'target_key': 'CHAVE'}
                ),
            }
        """
        pass

    @abstractmethod
    def get_key_generator(self) -> 'KeyGenerator':
        """
        Retorna o gerador de CHAVE do cliente.

        Returns:
            KeyGenerator configurado para este cliente.

        Exemplo VIC/EMCCAMP:
            return CompositeKeyGenerator(
                components=['NUMERO_CONTRATO', 'PARCELA'],
                separator='-'
            )

        Exemplo Tabelionato:
            return ColumnKeyGenerator(column='Protocolo')
        """
        pass

    # =========================================================================
    # MÉTODOS OPCIONAIS - Override se necessário
    # =========================================================================

    def get_validators(self, stage: str) -> List[BaseValidator]:
        """
        Retorna validators para um stage específico.
        Override para adicionar validators customizados.
        """
        processor_config = self.configure_processors().get(stage)
        if processor_config:
            return processor_config.validators
        return []

    def get_splitters(self, stage: str) -> List['BaseSplitter']:
        """
        Retorna splitters para separar output por carteira.
        Override para definir separação (judicial, campanha, etc).
        """
        return []

    def get_output_layout(self, stage: str) -> Optional['OutputLayout']:
        """
        Retorna layout de colunas para output de um stage.
        Override para customizar formato de saída.
        """
        return None

    # =========================================================================
    # MÉTODOS CONCRETOS - Herdados por todos os clientes
    # =========================================================================

    @property
    def config(self) -> ClientConfig:
        """Carrega e retorna configuração do cliente (lazy loading)."""
        if self._config is None:
            loader = ConfigLoader()
            if self._config_path:
                self._config = loader.load_from_path(self._config_path)
            else:
                self._config = loader.load_client(self.name)
        return self._config

    def get_extractor(self, name: str) -> BaseExtractor:
        """
        Obtém extractor por nome (com cache).

        Args:
            name: Nome do dataset (ex: 'source', 'max', 'judicial')

        Returns:
            Instância do extractor configurado.
        """
        if name not in self._extractors:
            extractor_configs = self.configure_extractors()
            if name not in extractor_configs:
                raise ValueError(f"Extractor '{name}' não configurado para {self.name}")

            config = extractor_configs[name]
            self._extractors[name] = config.extractor_class(
                client_config=self.config,
                **config.params
            )
        return self._extractors[name]

    def get_processor(self, stage: str) -> BaseProcessor:
        """
        Obtém processor por stage (com cache).

        Args:
            stage: Nome do stage (ex: 'treatment', 'batimento', 'baixa')

        Returns:
            Instância do processor configurado.
        """
        if stage not in self._processors:
            processor_configs = self.configure_processors()
            if stage not in processor_configs:
                raise ValueError(f"Processor '{stage}' não configurado para {self.name}")

            config = processor_configs[stage]
            self._processors[stage] = config.processor_class(
                client=self,
                validators=config.validators,
                **config.params
            )
        return self._processors[stage]

    def get_pipeline(self) -> Pipeline:
        """
        Retorna pipeline completo configurado para este cliente.
        """
        if self._pipeline is None:
            self._pipeline = Pipeline(client=self)
        return self._pipeline

    def run_stage(self, stage: str, **kwargs) -> StageResult:
        """
        Executa um stage específico do pipeline.

        Args:
            stage: Nome do stage ('extract', 'treat', 'batimento', etc)
            **kwargs: Argumentos adicionais para o stage

        Returns:
            Resultado da execução do stage.
        """
        return self.get_pipeline().run_stage(stage, **kwargs)

    def run_full(self, stages: Optional[List[str]] = None) -> Dict[str, StageResult]:
        """
        Executa pipeline completo ou stages específicos.

        Args:
            stages: Lista de stages a executar. Se None, executa todos.

        Returns:
            Dicionário com resultados de cada stage.
        """
        return self.get_pipeline().run(stages)

    # =========================================================================
    # MÉTODOS UTILITÁRIOS
    # =========================================================================

    def get_data_path(self, *parts: str) -> Path:
        """Retorna caminho dentro do diretório de dados do cliente."""
        return self.config.data_root / Path(*parts)

    def get_input_path(self, dataset: str) -> Path:
        """Retorna caminho de input para um dataset."""
        return self.config.get_input_path(dataset)

    def get_output_path(self, output_name: str) -> Path:
        """Retorna caminho de output."""
        return self.config.get_output_path(output_name)

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__}(name='{self.name}')>"
```

### 3.2 Implementação de Cliente Específico (VIC)

```python
# src/clients/vic/client.py

from typing import Dict, List

from src.core.client import BaseClient, ExtractorConfig, ProcessorConfig
from src.core.extractors import SqlExtractor
from src.core.processors import (
    TreatmentProcessor,
    MaxProcessor,
    BatimentoProcessor,
    DevolucaoProcessor
)
from src.core.validators import (
    RequiredFieldsValidator,
    AgingValidator,
    BlacklistValidator,
    RegexValidator
)
from src.core.splitters import JudicialSplitter
from src.core.key_generators import CompositeKeyGenerator

# Extractor específico do VIC
from src.clients.vic.extractors import VicEmailExtractor


class VicClient(BaseClient):
    """
    Cliente VIC Candiotto.

    Características:
    - Fonte primária: Email com attachment ZIP
    - Fonte secundária: MAX via SQL Server
    - CHAVE: NUMERO_CONTRATO-PARCELA
    - Validações: Aging (<=1800 dias), Blacklist de CPFs
    - Separação: Judicial vs Extrajudicial
    """

    @property
    def name(self) -> str:
        return "vic"

    @property
    def display_name(self) -> str:
        return "VIC Candiotto"

    def configure_extractors(self) -> Dict[str, ExtractorConfig]:
        return {
            # Extração da base VIC via email
            'vic': ExtractorConfig(
                extractor_class=VicEmailExtractor,
                params={
                    'subject_filter': 'Base VIC',
                    'attachment_pattern': '*.zip',
                    'days_back': 7,
                }
            ),

            # Extração da base MAX via SQL
            'max': ExtractorConfig(
                extractor_class=SqlExtractor,
                params={
                    'query_template': 'max',
                    'query_params': {
                        'mo_cliente_id': 35268
                    }
                }
            ),

            # Extração de clientes judiciais
            'judicial': ExtractorConfig(
                extractor_class=SqlExtractor,
                params={
                    'query_template': 'judicial',
                    'output_name': 'ClientesJudiciais'
                }
            ),
        }

    def configure_processors(self) -> Dict[str, ProcessorConfig]:
        return {
            # Tratamento da base VIC
            'treatment_vic': ProcessorConfig(
                processor_class=TreatmentProcessor,
                validators=[
                    RequiredFieldsValidator(
                        fields=['CPFCNPJ_CLIENTE', 'NUMERO_CONTRATO', 'PARCELA', 'VENCIMENTO', 'VALOR']
                    ),
                    AgingValidator(
                        date_field='VENCIMENTO',
                        max_days=1800,
                        reference='today'
                    ),
                    BlacklistValidator(
                        field='CPFCNPJ_CLIENTE',
                        blacklist_path='input/blacklist/cpfs.csv'
                    ),
                ],
                params={
                    'source_name': 'vic',
                    'output_name': 'vic_tratada',
                }
            ),

            # Tratamento da base MAX
            'treatment_max': ProcessorConfig(
                processor_class=MaxProcessor,
                validators=[
                    RequiredFieldsValidator(
                        fields=['PARCELA', 'VENCIMENTO', 'CPF_CNPJ']
                    ),
                    RegexValidator(
                        field='PARCELA',
                        pattern=r'^[0-9]{3,}-[0-9]{2,}$',
                        error_message='PARCELA deve ter formato NNNNN-NN'
                    ),
                ],
                params={
                    'source_name': 'max',
                    'output_name': 'max_tratada',
                }
            ),

            # Batimento VIC - MAX
            'batimento': ProcessorConfig(
                processor_class=BatimentoProcessor,
                params={
                    'source': 'vic_tratada',
                    'target': 'max_tratada',
                    'source_key': 'CHAVE',
                    'target_key': 'CHAVE',
                    'operation': 'source_minus_target',  # VIC - MAX
                }
            ),

            # Devolução MAX - VIC
            'devolucao': ProcessorConfig(
                processor_class=DevolucaoProcessor,
                params={
                    'source': 'max_tratada',
                    'target': 'vic_tratada',
                    'source_key': 'CHAVE',
                    'target_key': 'CHAVE',
                    'status_devolucao': '98',
                }
            ),
        }

    def get_key_generator(self) -> CompositeKeyGenerator:
        """CHAVE = NUMERO_CONTRATO-PARCELA"""
        return CompositeKeyGenerator(
            components=['NUMERO_CONTRATO', 'PARCELA'],
            separator='-'
        )

    def get_splitters(self, stage: str) -> List:
        """Separa batimento/devolução em judicial e extrajudicial."""
        if stage in ('batimento', 'devolucao'):
            return [
                JudicialSplitter(
                    cpf_field='CPFCNPJ_CLIENTE',
                    judicial_source='input/judicial/ClientesJudiciais.zip',
                    outputs={
                        'match': 'judicial',
                        'no_match': 'extrajudicial'
                    }
                )
            ]
        return []
```

### 3.3 Implementação de Cliente Específico (EMCCAMP)

```python
# src/clients/emccamp/client.py

from typing import Dict, List

from src.core.client import BaseClient, ExtractorConfig, ProcessorConfig
from src.core.extractors import SqlExtractor
from src.core.processors import (
    TreatmentProcessor,
    MaxProcessor,
    BatimentoProcessor,
    BaixaProcessor,
    DevolucaoProcessor,
    EnrichmentProcessor
)
from src.core.validators import (
    RequiredFieldsValidator,
    ExcludeValuesValidator,
    DuplicateKeyValidator
)
from src.core.splitters import JudicialSplitter, FieldValueSplitter
from src.core.key_generators import CompositeKeyGenerator

# Extractor específico do EMCCAMP
from src.clients.emccamp.extractors import TotvsApiExtractor
from src.clients.emccamp.validators import AcordoValidator


class EmccampClient(BaseClient):
    """
    Cliente EMCCAMP.

    Características:
    - Fonte primária: API REST TOTVS
    - Fonte secundária: MAX via SQL Server
    - CHAVE: CONTRATO-PARCELA
    - Validações: TIPO_PAGTO, Acordos vigentes, Chave duplicada
    - Separação: Judicial/Extra + Com/Sem Recebimento
    """

    @property
    def name(self) -> str:
        return "emccamp"

    @property
    def display_name(self) -> str:
        return "EMCCAMP"

    def configure_extractors(self) -> Dict[str, ExtractorConfig]:
        return {
            # Extração via API TOTVS
            'emccamp': ExtractorConfig(
                extractor_class=TotvsApiExtractor,
                params={
                    'endpoint': 'emccamp',
                    'date_field': 'DATA_VENCIMENTO',
                }
            ),

            # Extração de baixas via API TOTVS
            'baixas': ExtractorConfig(
                extractor_class=TotvsApiExtractor,
                params={
                    'endpoint': 'baixas',
                    'filter_field': 'HONORARIO_BAIXADO',
                    'filter_condition': '!= 0',
                }
            ),

            # MAX via SQL
            'max': ExtractorConfig(
                extractor_class=SqlExtractor,
                params={
                    'query_template': 'max',
                    'query_params': {'mo_cliente_id': 77398}
                }
            ),

            # Acordos abertos (doublecheck)
            'acordos': ExtractorConfig(
                extractor_class=SqlExtractor,
                params={
                    'query_template': 'doublecheck_acordo',
                    'output_name': 'acordos_abertos'
                }
            ),

            # Clientes judiciais
            'judicial': ExtractorConfig(
                extractor_class=SqlExtractor,
                params={
                    'query_template': 'judicial',
                    'output_name': 'ClientesJudiciais'
                }
            ),
        }

    def configure_processors(self) -> Dict[str, ProcessorConfig]:
        return {
            # Tratamento EMCCAMP
            'treatment_emccamp': ProcessorConfig(
                processor_class=TreatmentProcessor,
                validators=[
                    RequiredFieldsValidator(
                        fields=['CONTRATO', 'PARCELA', 'DATA_VENCIMENTO', 'VALOR', 'CPF_CNPJ']
                    ),
                    DuplicateKeyValidator(
                        key_field='CHAVE',
                        action='reject'  # rejeita duplicadas
                    ),
                    ExcludeValuesValidator(
                        field='TIPO_PAGTO',
                        values=['PERMUTA', 'Financiamento Fixo'],
                    ),
                ],
                params={
                    'source_name': 'emccamp',
                    'output_name': 'emccamp_tratada',
                }
            ),

            # Tratamento MAX
            'treatment_max': ProcessorConfig(
                processor_class=MaxProcessor,
                validators=[
                    RequiredFieldsValidator(
                        fields=['PARCELA', 'DATA_VENCIMENTO', 'CPF_CNPJ']
                    ),
                ],
                params={
                    'source_name': 'max',
                    'output_name': 'max_tratada',
                }
            ),

            # Batimento EMCCAMP - MAX
            'batimento': ProcessorConfig(
                processor_class=BatimentoProcessor,
                params={
                    'source': 'emccamp_tratada',
                    'target': 'max_tratada',
                    'source_key': 'CHAVE',
                    'target_key': 'CHAVE',
                }
            ),

            # Baixa MAX - EMCCAMP
            'baixa': ProcessorConfig(
                processor_class=BaixaProcessor,
                validators=[
                    AcordoValidator(
                        acordos_path='input/doublecheck_acordo/acordos_abertos.zip',
                        cpf_field='CPF_CNPJ',
                        action='exclude'  # remove quem tem acordo
                    ),
                ],
                params={
                    'source': 'max_tratada',
                    'target': 'emccamp_tratada',
                    'baixas_path': 'input/baixas/baixa_emccamp.zip',
                }
            ),

            # Devolução
            'devolucao': ProcessorConfig(
                processor_class=DevolucaoProcessor,
                params={
                    'source': 'max_tratada',
                    'target': 'emccamp_tratada',
                    'campaign_filter': 'EMCCAMP',
                    'status_devolucao': '98',
                }
            ),

            # Enriquecimento de contato
            'enriquecimento': ProcessorConfig(
                processor_class=EnrichmentProcessor,
                params={
                    'source': 'batimento',
                    'contact_fields': ['TELEFONE', 'EMAIL'],
                }
            ),
        }

    def get_key_generator(self) -> CompositeKeyGenerator:
        """CHAVE = CONTRATO-PARCELA"""
        return CompositeKeyGenerator(
            components=['CONTRATO', 'PARCELA'],
            separator='-'
        )

    def get_splitters(self, stage: str) -> List:
        """Separação por carteira."""
        if stage == 'batimento':
            return [
                JudicialSplitter(
                    cpf_field='CPF_CNPJ',
                    judicial_source='input/judicial/ClientesJudiciais.zip',
                    outputs={'match': 'judicial', 'no_match': 'extrajudicial'}
                )
            ]
        elif stage == 'baixa':
            return [
                FieldValueSplitter(
                    field='DATA_RECEBIMENTO',
                    conditions={
                        'not_empty': 'com_recebimento',
                        'empty': 'sem_recebimento'
                    }
                )
            ]
        return []
```

### 3.4 Implementação de Cliente Específico (Tabelionato)

```python
# src/clients/tabelionato/client.py

from typing import Dict, List

from src.core.client import BaseClient, ExtractorConfig, ProcessorConfig
from src.core.extractors import SqlExtractor
from src.core.processors import (
    TreatmentProcessor,
    MaxProcessor,
    BatimentoProcessor,
    BaixaProcessor
)
from src.core.validators import (
    RequiredFieldsValidator,
    DateValidator
)
from src.core.splitters import CampaignSplitter
from src.core.key_generators import ColumnKeyGenerator

# Extractors específicos
from src.clients.tabelionato.extractors import TabelionatoZipExtractor
from src.clients.tabelionato.validators import MixedAgingValidator


class TabelionatoClient(BaseClient):
    """
    Cliente Tabelionato.

    Características:
    - Fonte primária: Email com ZIP protegido por senha
    - Fonte secundária: MAX via ODBC
    - CHAVE: Protocolo (coluna única)
    - Validações: DtAnuencia válida, Aging misto por protocolo
    - Separação: Por Campanha (58, 78, 94)
    """

    @property
    def name(self) -> str:
        return "tabelionato"

    @property
    def display_name(self) -> str:
        return "Tabelionato"

    def configure_extractors(self) -> Dict[str, ExtractorConfig]:
        return {
            # Extração do ZIP protegido
            'tabelionato': ExtractorConfig(
                extractor_class=TabelionatoZipExtractor,
                params={
                    'zip_password': b"Mf4tab@",
                    'priority_filename': 'tabelionato',
                }
            ),

            # MAX via SQL/ODBC
            'max': ExtractorConfig(
                extractor_class=SqlExtractor,
                params={
                    'query_template': 'max_tabelionato',
                }
            ),
        }

    def configure_processors(self) -> Dict[str, ProcessorConfig]:
        return {
            # Tratamento Tabelionato
            'treatment_tabelionato': ProcessorConfig(
                processor_class=TreatmentProcessor,
                validators=[
                    RequiredFieldsValidator(
                        fields=['Protocolo', 'DtAnuencia', 'CPFCNPJ_CLIENTE']
                    ),
                    DateValidator(
                        field='DtAnuencia',
                        min_date='1900-01-01',
                        formats=['%d/%m/%Y', '%Y-%m-%d']
                    ),
                    MixedAgingValidator(
                        date_field='DtAnuencia',
                        protocol_field='Protocolo',
                        threshold_days=1800,
                        mixed_campaign='Campanha 58'
                    ),
                ],
                params={
                    'source_name': 'tabelionato',
                    'output_name': 'tabelionato_tratada',
                }
            ),

            # Tratamento MAX
            'treatment_max': ProcessorConfig(
                processor_class=MaxProcessor,
                params={
                    'source_name': 'max',
                    'output_name': 'max_tratada',
                }
            ),

            # Batimento
            'batimento': ProcessorConfig(
                processor_class=BatimentoProcessor,
                params={
                    'source': 'tabelionato_tratada',
                    'target': 'max_tratada',
                    'source_key': 'CHAVE',
                    'target_key': 'CHAVE',
                }
            ),

            # Baixa
            'baixa': ProcessorConfig(
                processor_class=BaixaProcessor,
                params={
                    'source': 'max_tratada',
                    'target': 'tabelionato_tratada',
                }
            ),
        }

    def get_key_generator(self) -> ColumnKeyGenerator:
        """CHAVE = Protocolo (coluna única)"""
        return ColumnKeyGenerator(column='Protocolo')

    def get_splitters(self, stage: str) -> List:
        """Separa por campanha."""
        if stage in ('batimento', 'baixa'):
            return [
                CampaignSplitter(
                    field='Campanha',
                    outputs={
                        'Campanha 58': 'campanha_58',
                        'Campanha 78': 'campanha_78',
                        'Campanha 94': 'campanha_94',
                    }
                )
            ]
        return []
```

---

## 4. Componentes de Suporte

### 4.1 KeyGenerator (Gerador de CHAVE)

```python
# src/core/key_generators.py

from abc import ABC, abstractmethod
from typing import List
import pandas as pd


class BaseKeyGenerator(ABC):
    """Base para geradores de CHAVE."""

    @abstractmethod
    def generate(self, df: pd.DataFrame) -> pd.Series:
        """Gera coluna CHAVE para o DataFrame."""
        pass


class CompositeKeyGenerator(BaseKeyGenerator):
    """
    Gera CHAVE concatenando múltiplas colunas.
    Usado por: VIC, EMCCAMP

    Exemplo: NUMERO_CONTRATO + "-" + PARCELA → "12345-01"
    """

    def __init__(self, components: List[str], separator: str = '-'):
        self.components = components
        self.separator = separator

    def generate(self, df: pd.DataFrame) -> pd.Series:
        parts = [df[col].astype(str).str.strip() for col in self.components]
        result = parts[0]
        for part in parts[1:]:
            result = result + self.separator + part
        return result


class ColumnKeyGenerator(BaseKeyGenerator):
    """
    Usa uma coluna existente como CHAVE.
    Usado por: Tabelionato

    Exemplo: Protocolo → "ABC123456"
    """

    def __init__(self, column: str):
        self.column = column

    def generate(self, df: pd.DataFrame) -> pd.Series:
        return df[self.column].astype(str).str.strip()
```

### 4.2 ClientRegistry (Registro de Clientes)

```python
# src/core/client/registry.py

from typing import Dict, List, Type, Optional
from src.core.client.base import BaseClient


class ClientRegistry:
    """
    Registro central de clientes disponíveis.
    Singleton que permite registro e lookup de clientes.
    """

    _instance: Optional['ClientRegistry'] = None
    _clients: Dict[str, Type[BaseClient]] = {}

    def __new__(cls) -> 'ClientRegistry':
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    @classmethod
    def register(cls, client_class: Type[BaseClient]) -> Type[BaseClient]:
        """
        Decorator para registrar uma classe de cliente.

        Uso:
            @ClientRegistry.register
            class VicClient(BaseClient):
                ...
        """
        # Instancia temporariamente para obter o name
        temp_instance = object.__new__(client_class)
        # Chama __init__ mínimo se necessário
        if hasattr(temp_instance, 'name'):
            name = temp_instance.name
        else:
            name = client_class.__name__.lower().replace('client', '')

        cls._clients[name] = client_class
        return client_class

    @classmethod
    def get(cls, name: str) -> BaseClient:
        """Obtém instância de cliente por nome."""
        if name not in cls._clients:
            raise ValueError(
                f"Cliente '{name}' não registrado. "
                f"Disponíveis: {list(cls._clients.keys())}"
            )
        return cls._clients[name]()

    @classmethod
    def list_clients(cls) -> List[str]:
        """Lista nomes de todos os clientes registrados."""
        return list(cls._clients.keys())

    @classmethod
    def get_all(cls) -> Dict[str, BaseClient]:
        """Retorna todos os clientes instanciados."""
        return {name: client_class() for name, client_class in cls._clients.items()}


# Decorator de conveniência
def register_client(cls: Type[BaseClient]) -> Type[BaseClient]:
    """Decorator para registrar cliente."""
    return ClientRegistry.register(cls)
```

### 4.3 Pipeline (Orquestrador)

```python
# src/core/pipeline/pipeline.py

from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Any
import logging

from src.core.client.base import BaseClient


@dataclass
class StageResult:
    """Resultado de execução de um stage."""
    stage: str
    success: bool
    records_in: int = 0
    records_out: int = 0
    duration: float = 0.0
    output_files: List[Path] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)
    extras: Dict[str, Any] = field(default_factory=dict)


class Pipeline:
    """
    Orquestrador de pipeline para um cliente.

    Gerencia a execução sequencial ou seletiva de stages:
    extract → treat → batimento → baixa → devolucao → enriquecimento
    """

    # Ordem padrão de execução
    DEFAULT_STAGES = [
        'extract',
        'treat',
        'batimento',
        'baixa',
        'devolucao',
        'enriquecimento'
    ]

    def __init__(self, client: BaseClient):
        self.client = client
        self.logger = logging.getLogger(f"pipeline.{client.name}")
        self._results: Dict[str, StageResult] = {}

    def run(self, stages: Optional[List[str]] = None) -> Dict[str, StageResult]:
        """
        Executa pipeline com stages especificados ou todos.

        Args:
            stages: Lista de stages a executar. Se None, executa todos.

        Returns:
            Dicionário com resultados de cada stage executado.
        """
        stages = stages or self.DEFAULT_STAGES
        self._results = {}

        self.logger.info(f"Iniciando pipeline para {self.client.display_name}")
        self.logger.info(f"Stages: {stages}")

        for stage in stages:
            if stage not in self._get_stage_handlers():
                self.logger.warning(f"Stage '{stage}' não reconhecido, pulando...")
                continue

            result = self.run_stage(stage)
            self._results[stage] = result

            if not result.success:
                self.logger.error(f"Stage '{stage}' falhou. Abortando pipeline.")
                break

        self._print_summary()
        return self._results

    def run_stage(self, stage: str, **kwargs) -> StageResult:
        """Executa um único stage."""
        self.logger.info(f"Executando stage: {stage}")
        inicio = datetime.now()

        try:
            handler = self._get_stage_handlers().get(stage)
            if not handler:
                raise ValueError(f"Stage '{stage}' não tem handler definido")

            result = handler(**kwargs)
            result.duration = (datetime.now() - inicio).total_seconds()
            result.success = True

        except Exception as e:
            self.logger.exception(f"Erro no stage '{stage}': {e}")
            result = StageResult(
                stage=stage,
                success=False,
                duration=(datetime.now() - inicio).total_seconds(),
                errors=[str(e)]
            )

        return result

    def _get_stage_handlers(self) -> Dict[str, callable]:
        """Retorna mapeamento de stages para handlers."""
        return {
            'extract': self._run_extraction,
            'treat': self._run_treatment,
            'batimento': self._run_batimento,
            'baixa': self._run_baixa,
            'devolucao': self._run_devolucao,
            'enriquecimento': self._run_enriquecimento,
        }

    def _run_extraction(self, datasets: Optional[List[str]] = None) -> StageResult:
        """Executa extração de dados."""
        datasets = datasets or list(self.client.configure_extractors().keys())
        output_files = []
        total_records = 0

        for dataset in datasets:
            extractor = self.client.get_extractor(dataset)
            result = extractor.extract()
            output_files.append(result.output_path)
            total_records += result.records

        return StageResult(
            stage='extract',
            success=True,
            records_out=total_records,
            output_files=output_files
        )

    def _run_treatment(self, sources: Optional[List[str]] = None) -> StageResult:
        """Executa tratamento de dados."""
        # Identifica processors de tratamento
        treatment_processors = [
            name for name in self.client.configure_processors().keys()
            if name.startswith('treatment_')
        ]

        if sources:
            treatment_processors = [
                f"treatment_{s}" for s in sources
                if f"treatment_{s}" in treatment_processors
            ]

        total_in = 0
        total_out = 0
        output_files = []

        for processor_name in treatment_processors:
            processor = self.client.get_processor(processor_name)
            result = processor.process()
            total_in += result.registros_entrada
            total_out += result.registros_saida
            output_files.append(result.arquivo_saida)

        return StageResult(
            stage='treat',
            success=True,
            records_in=total_in,
            records_out=total_out,
            output_files=output_files
        )

    def _run_batimento(self) -> StageResult:
        """Executa batimento."""
        processor = self.client.get_processor('batimento')
        result = processor.process()

        return StageResult(
            stage='batimento',
            success=True,
            records_in=result.registros_source + result.registros_target,
            records_out=result.registros_batimento,
            output_files=[result.arquivo_saida] if result.arquivo_saida else [],
            extras={
                'judicial': result.judicial,
                'extrajudicial': result.extrajudicial,
            }
        )

    def _run_baixa(self) -> StageResult:
        """Executa baixa."""
        processor = self.client.get_processor('baixa')
        result = processor.process()

        return StageResult(
            stage='baixa',
            success=True,
            records_out=result.registros_baixa,
            output_files=[result.arquivo_saida] if result.arquivo_saida else []
        )

    def _run_devolucao(self) -> StageResult:
        """Executa devolução."""
        processor = self.client.get_processor('devolucao')
        result = processor.process()

        return StageResult(
            stage='devolucao',
            success=True,
            records_out=result.registros_devolucao,
            output_files=[result.arquivo_zip] if result.arquivo_zip else []
        )

    def _run_enriquecimento(self) -> StageResult:
        """Executa enriquecimento."""
        if 'enriquecimento' not in self.client.configure_processors():
            return StageResult(stage='enriquecimento', success=True)

        processor = self.client.get_processor('enriquecimento')
        result = processor.process()

        return StageResult(
            stage='enriquecimento',
            success=True,
            records_out=result.registros,
            output_files=[result.arquivo_saida]
        )

    def _print_summary(self) -> None:
        """Imprime resumo da execução."""
        print("\n" + "=" * 80)
        print(f"PIPELINE {self.client.display_name.upper()} - RESUMO")
        print("=" * 80)

        for stage, result in self._results.items():
            status = "✓" if result.success else "✗"
            print(f"\n{status} {stage.upper()}")
            print(f"   Registros: {result.records_in} → {result.records_out}")
            print(f"   Duração: {result.duration:.2f}s")
            if result.output_files:
                for f in result.output_files:
                    print(f"   Output: {f}")
            if result.errors:
                for e in result.errors:
                    print(f"   ERRO: {e}")

        print("\n" + "=" * 80)
```

---

## 5. Fluxo de Execução

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           FLUXO DE EXECUÇÃO                                  │
└─────────────────────────────────────────────────────────────────────────────┘

     python main.py --client vic full
                    │
                    ▼
    ┌───────────────────────────────┐
    │      ClientRegistry.get()     │
    │      "vic" → VicClient()      │
    └───────────────┬───────────────┘
                    │
                    ▼
    ┌───────────────────────────────┐
    │    VicClient.get_pipeline()   │
    │    → Pipeline(client=vic)     │
    └───────────────┬───────────────┘
                    │
                    ▼
    ┌───────────────────────────────┐
    │      Pipeline.run(stages)     │
    └───────────────┬───────────────┘
                    │
        ┌───────────┴───────────┐
        ▼                       ▼
    ┌───────────┐         ┌───────────┐
    │ EXTRACT   │         │   TREAT   │
    │           │         │           │
    │ VicEmail  │────────▶│ Treatment │
    │ Extractor │         │ Processor │
    │           │         │ + Aging   │
    │ Sql       │         │ + Blackl. │
    │ Extractor │         │           │
    └───────────┘         └─────┬─────┘
                                │
        ┌───────────────────────┴───────────────────────┐
        ▼                                               ▼
    ┌───────────┐                               ┌───────────┐
    │ BATIMENTO │                               │ DEVOLUCAO │
    │           │                               │           │
    │ VIC - MAX │                               │ MAX - VIC │
    │           │                               │           │
    │ Judicial  │                               │ Judicial  │
    │ Splitter  │                               │ Splitter  │
    └─────┬─────┘                               └─────┬─────┘
          │                                           │
          ▼                                           ▼
    ┌─────────────┐                           ┌─────────────┐
    │ judicial.csv│                           │ jud_dev.csv │
    │ extrajud.csv│                           │ extra_dev.csv│
    └─────────────┘                           └─────────────┘
```

---

## 6. Resumo da Abordagem

| Aspecto | Decisão | Justificativa |
|---------|---------|---------------|
| **Clientes como Classes** | ✓ Sim | Permite herança, override, tipagem forte |
| **Composição sobre Herança** | ✓ Sim | Extractors/Validators são injetados |
| **Configuração via YAML** | ✓ Híbrido | Parâmetros em YAML, lógica em código |
| **Registry Pattern** | ✓ Sim | Descoberta automática de clientes |
| **Template Method** | ✓ Sim | Fluxo comum em BaseProcessor |
| **Strategy Pattern** | ✓ Sim | Validators/Splitters intercambiáveis |

### Vantagens desta abordagem:

1. **Type Safety**: Classes tipadas = IDE autocomplete + verificação
2. **Extensibilidade**: Override métodos específicos sem tocar no core
3. **Testabilidade**: Mock de componentes individuais
4. **Clareza**: Fácil entender o que cada cliente faz olhando sua classe
5. **Flexibilidade**: Mistura config YAML + lógica Python quando necessário
