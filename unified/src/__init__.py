"""
Unified Pipeline System.
A hybrid architecture for processing client data with YAML-based configuration.

Usage:
    # CLI
    python -m unified.src.cli run vic

    # API
    from unified.src.api import create_app
    app = create_app()
    app.run()

    # Programmatic
    from unified.src.core import PipelineEngine
    engine = PipelineEngine(config_dir="./configs/clients")
    result = engine.run("vic")
"""
from .core import (
    ClientConfig,
    ConfigLoader,
    PipelineEngine,
    PipelineResult,
    ProcessorType,
    ValidatorType,
    SplitterType,
    LoaderType,
)

__version__ = "1.0.0"
__all__ = [
    "ClientConfig",
    "ConfigLoader",
    "PipelineEngine",
    "PipelineResult",
    "ProcessorType",
    "ValidatorType",
    "SplitterType",
    "LoaderType",
]
