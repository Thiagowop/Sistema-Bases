"""
Processors package.
Provides pipeline processing components.
"""
from __future__ import annotations

from typing import Any, Callable

from ..core.base import BaseProcessor, ProcessorResult
from ..core.schemas import ClientConfig, ProcessorType

from .tratamento import TratamentoProcessor, create_tratamento_processor
from .batimento import BatimentoProcessor, EnhancedBatimentoProcessor, create_batimento_processor
from .baixa import BaixaProcessor, create_baixa_processor
from .devolucao import DevolucaoProcessor, create_devolucao_processor
from .enriquecimento import EnriquecimentoProcessor, create_enriquecimento_processor


# Registry of processor factories
_PROCESSOR_REGISTRY: dict[
    ProcessorType, Callable[[ClientConfig, dict[str, Any]], BaseProcessor]
] = {
    ProcessorType.TRATAMENTO: create_tratamento_processor,
    ProcessorType.BATIMENTO: create_batimento_processor,
    ProcessorType.BAIXA: create_baixa_processor,
    ProcessorType.DEVOLUCAO: create_devolucao_processor,
    ProcessorType.ENRIQUECIMENTO: create_enriquecimento_processor,
}


def create_processor(
    processor_type: ProcessorType,
    config: ClientConfig,
    params: dict[str, Any] | None = None,
) -> BaseProcessor:
    """
    Factory function to create a processor based on type.

    Args:
        processor_type: Type of processor to create
        config: Client configuration
        params: Processor-specific parameters

    Returns:
        Configured processor instance

    Raises:
        ValueError: If processor type is not registered
    """
    factory = _PROCESSOR_REGISTRY.get(processor_type)
    if factory is None:
        raise ValueError(f"Unknown processor type: {processor_type}")
    return factory(config, params or {})


def register_processor(
    processor_type: ProcessorType,
    factory: Callable[[ClientConfig, dict[str, Any]], BaseProcessor],
) -> None:
    """
    Register a custom processor factory.

    Args:
        processor_type: The processor type to register
        factory: Factory function that creates the processor
    """
    _PROCESSOR_REGISTRY[processor_type] = factory


def get_processor_class(processor_type: ProcessorType) -> type | None:
    """Get the processor class for a given type."""
    mapping = {
        ProcessorType.TRATAMENTO: TratamentoProcessor,
        ProcessorType.BATIMENTO: BatimentoProcessor,
        ProcessorType.BAIXA: BaixaProcessor,
        ProcessorType.DEVOLUCAO: DevolucaoProcessor,
        ProcessorType.ENRIQUECIMENTO: EnriquecimentoProcessor,
    }
    return mapping.get(processor_type)


__all__ = [
    "BaseProcessor",
    "ProcessorResult",
    "ProcessorType",
    "TratamentoProcessor",
    "BatimentoProcessor",
    "EnhancedBatimentoProcessor",
    "BaixaProcessor",
    "DevolucaoProcessor",
    "EnriquecimentoProcessor",
    "create_processor",
    "register_processor",
    "get_processor_class",
]
