"""
Core package.
Contains base classes, schemas, configuration, and pipeline engine.
"""
from .schemas import (
    ClientConfig,
    ExportConfig,
    KeyConfig,
    KeyGeneratorType,
    LoaderConfig,
    LoaderType,
    PipelineConfig,
    ProcessorConfig,
    ProcessorType,
    SourceConfig,
    SplitterConfig,
    SplitterType,
    ValidatorConfig,
    ValidatorType,
)

from .base import (
    BaseValidator,
    BaseSplitter,
    BaseLoader,
    BaseProcessor,
    BaseKeyGenerator,
    BaseClientExtension,
    ValidationResult,
    SplitResult,
    LoaderResult,
    ProcessorResult,
)

from .config import (
    ConfigLoader,
    ConfigError,
    load_client_config,
)

from .keys import (
    CompositeKeyGenerator,
    ColumnKeyGenerator,
    CustomKeyGenerator,
    create_key_generator,
    register_key_generator,
)

from .engine import (
    PipelineEngine,
    PipelineContext,
    PipelineResult,
)


__all__ = [
    # Schemas
    "ClientConfig",
    "ExportConfig",
    "KeyConfig",
    "KeyGeneratorType",
    "LoaderConfig",
    "LoaderType",
    "PipelineConfig",
    "ProcessorConfig",
    "ProcessorType",
    "SourceConfig",
    "SplitterConfig",
    "SplitterType",
    "ValidatorConfig",
    "ValidatorType",
    # Base classes
    "BaseValidator",
    "BaseSplitter",
    "BaseLoader",
    "BaseProcessor",
    "BaseKeyGenerator",
    "BaseClientExtension",
    "ValidationResult",
    "SplitResult",
    "LoaderResult",
    "ProcessorResult",
    # Config
    "ConfigLoader",
    "ConfigError",
    "load_client_config",
    # Keys
    "CompositeKeyGenerator",
    "ColumnKeyGenerator",
    "CustomKeyGenerator",
    "create_key_generator",
    "register_key_generator",
    # Engine
    "PipelineEngine",
    "PipelineContext",
    "PipelineResult",
]
