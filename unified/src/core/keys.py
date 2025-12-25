"""
Key generators for CHAVE creation.
Different strategies for generating unique identifiers.
"""
from __future__ import annotations

import re
from typing import Callable

import pandas as pd

from .base import BaseKeyGenerator
from .schemas import KeyConfig, KeyGeneratorType


class CompositeKeyGenerator(BaseKeyGenerator):
    """Generates keys by concatenating multiple columns."""

    def __init__(self, config: KeyConfig):
        self.config = config
        self.components = config.components
        self.separator = config.separator
        self._output_column = config.output_column

    @property
    def output_column(self) -> str:
        return self._output_column

    def generate(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty or not self.components:
            return df

        df = df.copy()

        # Check which components exist
        existing = [c for c in self.components if c in df.columns]
        if not existing:
            df[self._output_column] = ""
            return df

        # Generate composite key
        def make_key(row: pd.Series) -> str:
            parts = []
            for col in existing:
                value = str(row[col]) if pd.notna(row[col]) else ""
                # Clean value: remove special chars, normalize
                cleaned = re.sub(r"[^\w]", "", value).upper()
                parts.append(cleaned)
            return self.separator.join(parts)

        df[self._output_column] = df.apply(make_key, axis=1)
        return df


class ColumnKeyGenerator(BaseKeyGenerator):
    """Uses an existing column as the key."""

    def __init__(self, config: KeyConfig):
        self.config = config
        self.source_column = config.column or "CHAVE"
        self._output_column = config.output_column

    @property
    def output_column(self) -> str:
        return self._output_column

    def generate(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df

        df = df.copy()

        if self.source_column not in df.columns:
            df[self._output_column] = ""
            return df

        if self.source_column != self._output_column:
            # Clean and normalize the key
            df[self._output_column] = (
                df[self.source_column]
                .astype(str)
                .str.strip()
                .str.upper()
                .str.replace(r"[^\w-]", "", regex=True)
            )
        else:
            # Just normalize existing column
            df[self._output_column] = (
                df[self.source_column]
                .astype(str)
                .str.strip()
                .str.upper()
                .str.replace(r"[^\w-]", "", regex=True)
            )

        return df


class CustomKeyGenerator(BaseKeyGenerator):
    """Custom key generator using a provided function."""

    def __init__(self, config: KeyConfig, generator_func: Callable[[pd.DataFrame], pd.DataFrame]):
        self.config = config
        self._output_column = config.output_column
        self.generator_func = generator_func

    @property
    def output_column(self) -> str:
        return self._output_column

    def generate(self, df: pd.DataFrame) -> pd.DataFrame:
        return self.generator_func(df)


# Registry of key generator factories
_KEY_GENERATOR_REGISTRY: dict[KeyGeneratorType, type] = {
    KeyGeneratorType.COMPOSITE: CompositeKeyGenerator,
    KeyGeneratorType.COLUMN: ColumnKeyGenerator,
}


def create_key_generator(config: KeyConfig) -> BaseKeyGenerator:
    """
    Factory function to create a key generator based on configuration.

    Args:
        config: Key configuration

    Returns:
        Configured key generator instance

    Raises:
        ValueError: If key type is not registered
    """
    generator_class = _KEY_GENERATOR_REGISTRY.get(config.type)
    if generator_class is None:
        raise ValueError(f"Unknown key generator type: {config.type}")
    return generator_class(config)


def register_key_generator(key_type: KeyGeneratorType, generator_class: type) -> None:
    """Register a custom key generator class."""
    _KEY_GENERATOR_REGISTRY[key_type] = generator_class


__all__ = [
    "BaseKeyGenerator",
    "CompositeKeyGenerator",
    "ColumnKeyGenerator",
    "CustomKeyGenerator",
    "create_key_generator",
    "register_key_generator",
]
