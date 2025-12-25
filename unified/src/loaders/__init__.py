"""
Loaders package.
Provides data loading components for the pipeline.
"""
from __future__ import annotations

from typing import TYPE_CHECKING, Callable

from ..core.base import BaseLoader, LoaderResult
from ..core.schemas import LoaderConfig, LoaderType

from .file_loader import FileLoader, create_file_loader
from .email_loader import EmailLoader, create_email_loader
from .sql_loader import SQLLoader, create_sql_loader
from .api_loader import APILoader, create_api_loader

if TYPE_CHECKING:
    from ..core.schemas import ClientConfig


# Registry of loader factories
_LOADER_REGISTRY: dict[
    LoaderType, Callable[[LoaderConfig, "ClientConfig"], BaseLoader]
] = {
    LoaderType.FILE: create_file_loader,
    LoaderType.EMAIL: create_email_loader,
    LoaderType.SQL: create_sql_loader,
    LoaderType.API: create_api_loader,
}


def create_loader(config: LoaderConfig, client_config: "ClientConfig") -> BaseLoader:
    """
    Factory function to create a loader based on configuration.

    Args:
        config: Loader configuration
        client_config: Client configuration

    Returns:
        Configured loader instance

    Raises:
        ValueError: If loader type is not registered
    """
    factory = _LOADER_REGISTRY.get(config.type)
    if factory is None:
        raise ValueError(f"Unknown loader type: {config.type}")
    return factory(config, client_config)


def register_loader(
    loader_type: LoaderType,
    factory: Callable[[LoaderConfig, "ClientConfig"], BaseLoader],
) -> None:
    """
    Register a custom loader factory.

    Args:
        loader_type: The loader type to register
        factory: Factory function that creates the loader
    """
    _LOADER_REGISTRY[loader_type] = factory


__all__ = [
    "BaseLoader",
    "LoaderResult",
    "LoaderConfig",
    "LoaderType",
    "FileLoader",
    "EmailLoader",
    "SQLLoader",
    "APILoader",
    "create_loader",
    "register_loader",
]
