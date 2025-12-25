"""
Splitters package.
Provides data splitting components for the pipeline.
"""
from __future__ import annotations

from typing import Callable

from ..core.base import BaseSplitter, SplitResult
from ..core.schemas import SplitterConfig, SplitterType

from .judicial import JudicialSplitter, create_judicial_splitter
from .campaign import CampaignSplitter, create_campaign_splitter
from .field_value import (
    FieldValueSplitter,
    UniqueValueSplitter,
    create_field_value_splitter,
    create_unique_value_splitter,
)


# Registry of splitter factories
_SPLITTER_REGISTRY: dict[SplitterType, Callable[[SplitterConfig], BaseSplitter]] = {
    SplitterType.JUDICIAL: create_judicial_splitter,
    SplitterType.CAMPAIGN: create_campaign_splitter,
    SplitterType.FIELD_VALUE: create_field_value_splitter,
}


def create_splitter(config: SplitterConfig) -> BaseSplitter:
    """
    Factory function to create a splitter based on configuration.

    Args:
        config: Splitter configuration

    Returns:
        Configured splitter instance

    Raises:
        ValueError: If splitter type is not registered
    """
    factory = _SPLITTER_REGISTRY.get(config.type)
    if factory is None:
        raise ValueError(f"Unknown splitter type: {config.type}")
    return factory(config)


def register_splitter(
    splitter_type: SplitterType,
    factory: Callable[[SplitterConfig], BaseSplitter],
) -> None:
    """
    Register a custom splitter factory.

    Args:
        splitter_type: The splitter type to register
        factory: Factory function that creates the splitter
    """
    _SPLITTER_REGISTRY[splitter_type] = factory


__all__ = [
    "BaseSplitter",
    "SplitResult",
    "SplitterConfig",
    "SplitterType",
    "JudicialSplitter",
    "CampaignSplitter",
    "FieldValueSplitter",
    "UniqueValueSplitter",
    "create_splitter",
    "register_splitter",
]
