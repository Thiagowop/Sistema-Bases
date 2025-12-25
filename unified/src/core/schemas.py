"""
Schema definitions for client configuration validation.
Uses dataclasses for type safety and validation.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class LoaderType(str, Enum):
    """Supported data loader types."""
    EMAIL = "email"
    SQL = "sql"
    API = "api"
    FILE = "file"


class ProcessorType(str, Enum):
    """Pipeline processor types."""
    TRATAMENTO = "tratamento"
    BATIMENTO = "batimento"
    BAIXA = "baixa"
    DEVOLUCAO = "devolucao"
    ENRIQUECIMENTO = "enriquecimento"


class ValidatorType(str, Enum):
    """Supported validator types."""
    REQUIRED = "required"
    AGING = "aging"
    BLACKLIST = "blacklist"
    REGEX = "regex"
    CAMPAIGN = "campaign"
    STATUS = "status"
    TYPE_FILTER = "type_filter"
    LINEBREAK = "linebreak"
    DATERANGE = "daterange"
    CUSTOM = "custom"


class SplitterType(str, Enum):
    """Supported splitter types."""
    JUDICIAL = "judicial"
    CAMPAIGN = "campaign"
    FIELD_VALUE = "field_value"
    CUSTOM = "custom"


class KeyGeneratorType(str, Enum):
    """Key generation strategies."""
    COMPOSITE = "composite"
    COLUMN = "column"
    CUSTOM = "custom"


@dataclass
class KeyConfig:
    """Configuration for CHAVE (key) generation."""
    type: KeyGeneratorType = KeyGeneratorType.COMPOSITE
    components: list[str] = field(default_factory=list)
    separator: str = "-"
    column: str | None = None
    output_column: str = "CHAVE"


@dataclass
class ColumnMapping:
    """Configuration for column renaming."""
    source: str
    target: str


@dataclass
class ValidatorConfig:
    """Configuration for a single validator."""
    type: ValidatorType
    enabled: bool = True
    params: dict[str, Any] = field(default_factory=dict)


@dataclass
class SplitterConfig:
    """Configuration for a single splitter."""
    type: SplitterType
    enabled: bool = True
    params: dict[str, Any] = field(default_factory=dict)


@dataclass
class LoaderConfig:
    """Configuration for data loading."""
    type: LoaderType
    params: dict[str, Any] = field(default_factory=dict)


@dataclass
class ProcessorConfig:
    """Configuration for a pipeline processor."""
    type: ProcessorType
    enabled: bool = True
    params: dict[str, Any] = field(default_factory=dict)


@dataclass
class ExportConfig:
    """Configuration for data export."""
    filename_prefix: str
    subdir: str = ""
    format: str = "zip"
    add_timestamp: bool = True
    encoding: str = "utf-8-sig"
    separator: str = ";"


@dataclass
class SourceConfig:
    """Configuration for a data source (client or max)."""
    loader: LoaderConfig
    key: KeyConfig
    columns: dict[str, str] = field(default_factory=dict)
    required_columns: list[str] = field(default_factory=list)
    validators: list[ValidatorConfig] = field(default_factory=list)
    splitters: list[SplitterConfig] = field(default_factory=list)
    export: ExportConfig | None = None


@dataclass
class PipelineConfig:
    """Configuration for the complete pipeline."""
    processors: list[ProcessorConfig] = field(default_factory=list)


@dataclass
class ClientConfig:
    """Complete client configuration."""
    name: str
    version: str = "1.0"
    description: str = ""

    # Data sources
    client_source: SourceConfig | None = None
    max_source: SourceConfig | None = None

    # Pipeline configuration
    pipeline: PipelineConfig = field(default_factory=PipelineConfig)

    # Global settings
    global_settings: dict[str, Any] = field(default_factory=dict)

    # Extension class (optional)
    extension_class: str | None = None

    # Paths
    paths: dict[str, str] = field(default_factory=dict)
