"""
Base classes and interfaces for the unified pipeline system.
All components inherit from these abstract base classes.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pandas as pd

if TYPE_CHECKING:
    from .schemas import ClientConfig, ValidatorConfig, SplitterConfig, LoaderConfig


@dataclass
class ValidationResult:
    """Result of a validation operation."""
    valid: pd.DataFrame
    invalid: pd.DataFrame
    errors: list[str]

    @property
    def total_valid(self) -> int:
        return len(self.valid)

    @property
    def total_invalid(self) -> int:
        return len(self.invalid)


@dataclass
class SplitResult:
    """Result of a split operation."""
    splits: dict[str, pd.DataFrame]

    def get(self, name: str, default: pd.DataFrame | None = None) -> pd.DataFrame:
        return self.splits.get(name, default or pd.DataFrame())

    @property
    def names(self) -> list[str]:
        return list(self.splits.keys())


@dataclass
class LoaderResult:
    """Result of a data loading operation."""
    data: pd.DataFrame
    metadata: dict[str, Any]
    source_path: Path | None = None


@dataclass
class ProcessorResult:
    """Result of a processor execution."""
    data: pd.DataFrame
    metadata: dict[str, Any]
    output_files: list[Path]
    errors: list[str]


class BaseValidator(ABC):
    """Abstract base class for data validators."""

    def __init__(self, config: ValidatorConfig):
        self.config = config
        self.enabled = config.enabled
        self.params = config.params

    @abstractmethod
    def validate(self, df: pd.DataFrame) -> ValidationResult:
        """Validate the dataframe and return valid/invalid splits."""
        pass

    @property
    @abstractmethod
    def name(self) -> str:
        """Validator name for logging/reporting."""
        pass


class BaseSplitter(ABC):
    """Abstract base class for data splitters."""

    def __init__(self, config: SplitterConfig):
        self.config = config
        self.enabled = config.enabled
        self.params = config.params

    @abstractmethod
    def split(self, df: pd.DataFrame) -> SplitResult:
        """Split the dataframe into named groups."""
        pass

    @property
    @abstractmethod
    def name(self) -> str:
        """Splitter name for logging/reporting."""
        pass


class BaseLoader(ABC):
    """Abstract base class for data loaders."""

    def __init__(self, config: LoaderConfig, client_config: ClientConfig):
        self.config = config
        self.client_config = client_config
        self.params = config.params

    @abstractmethod
    def load(self) -> LoaderResult:
        """Load data from the source."""
        pass

    @property
    @abstractmethod
    def name(self) -> str:
        """Loader name for logging/reporting."""
        pass


class BaseProcessor(ABC):
    """Abstract base class for pipeline processors."""

    def __init__(self, client_config: ClientConfig, params: dict[str, Any] | None = None):
        self.client_config = client_config
        self.params = params or {}

    @abstractmethod
    def process(
        self,
        client_data: pd.DataFrame | None,
        max_data: pd.DataFrame | None,
        context: dict[str, Any],
    ) -> ProcessorResult:
        """Process the data and return results."""
        pass

    @property
    @abstractmethod
    def name(self) -> str:
        """Processor name for logging/reporting."""
        pass


class BaseKeyGenerator(ABC):
    """Abstract base class for CHAVE (key) generators."""

    @abstractmethod
    def generate(self, df: pd.DataFrame) -> pd.DataFrame:
        """Generate keys and add them to the dataframe."""
        pass

    @property
    @abstractmethod
    def output_column(self) -> str:
        """Name of the generated key column."""
        pass


class BaseClientExtension(ABC):
    """
    Base class for client-specific extensions.
    Override methods to add custom behavior beyond YAML config.
    """

    def __init__(self, client_config: ClientConfig):
        self.client_config = client_config

    def pre_process(self, df: pd.DataFrame, source: str) -> pd.DataFrame:
        """Hook called before processing. Override for custom logic."""
        return df

    def post_process(self, df: pd.DataFrame, source: str) -> pd.DataFrame:
        """Hook called after processing. Override for custom logic."""
        return df

    def custom_validation(self, df: pd.DataFrame) -> ValidationResult:
        """Custom validation logic. Override for complex rules."""
        return ValidationResult(valid=df, invalid=pd.DataFrame(), errors=[])

    def custom_transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Custom transformation logic. Override for complex rules."""
        return df

    def on_error(self, error: Exception, stage: str) -> None:
        """Error handler. Override for custom error handling."""
        pass
