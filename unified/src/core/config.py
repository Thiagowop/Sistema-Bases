"""
Configuration loader for client YAML files.
Validates and converts YAML to typed dataclasses.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

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


class ConfigError(Exception):
    """Configuration loading or validation error."""
    pass


class ConfigLoader:
    """Loads and validates client configuration from YAML files."""

    def __init__(self, config_dir: Path | str | None = None):
        """Initialize loader with optional config directory."""
        self.config_dir = Path(config_dir) if config_dir else None

    def load(self, client_name: str) -> ClientConfig:
        """Load configuration for a specific client."""
        if self.config_dir:
            config_path = self.config_dir / f"{client_name}.yaml"
        else:
            config_path = Path(client_name)

        if not config_path.exists():
            raise ConfigError(f"Configuration file not found: {config_path}")

        return self.load_from_file(config_path)

    def load_from_file(self, path: Path | str) -> ClientConfig:
        """Load configuration from a specific file path."""
        path = Path(path)
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = yaml.safe_load(f)
        except yaml.YAMLError as e:
            raise ConfigError(f"Invalid YAML in {path}: {e}")

        return self._parse_config(data, path.stem)

    def load_from_dict(self, data: dict[str, Any], name: str = "unknown") -> ClientConfig:
        """Load configuration from a dictionary."""
        return self._parse_config(data, name)

    def _parse_config(self, data: dict[str, Any], default_name: str) -> ClientConfig:
        """Parse raw config data into typed dataclasses."""
        if not isinstance(data, dict):
            raise ConfigError("Configuration must be a dictionary")

        return ClientConfig(
            name=data.get("name", default_name),
            version=str(data.get("version", "1.0")),
            description=data.get("description", ""),
            client_source=self._parse_source(data.get("client_source")),
            max_source=self._parse_source(data.get("max_source")),
            pipeline=self._parse_pipeline(data.get("pipeline", {})),
            global_settings=data.get("global", {}),
            extension_class=data.get("extension_class"),
            paths=data.get("paths", {}),
        )

    def _parse_source(self, data: dict[str, Any] | None) -> SourceConfig | None:
        """Parse a data source configuration."""
        if not data:
            return None

        return SourceConfig(
            loader=self._parse_loader(data.get("loader", {})),
            key=self._parse_key(data.get("key", {})),
            columns=data.get("columns", {}),
            required_columns=data.get("required_columns", []),
            validators=self._parse_validators(data.get("validators", [])),
            splitters=self._parse_splitters(data.get("splitters", [])),
            export=self._parse_export(data.get("export")),
        )

    def _parse_loader(self, data: dict[str, Any]) -> LoaderConfig:
        """Parse loader configuration."""
        loader_type = data.get("type", "file")
        try:
            loader_enum = LoaderType(loader_type)
        except ValueError:
            raise ConfigError(f"Unknown loader type: {loader_type}")

        return LoaderConfig(
            type=loader_enum,
            params=data.get("params", {}),
        )

    def _parse_key(self, data: dict[str, Any]) -> KeyConfig:
        """Parse key generation configuration."""
        key_type = data.get("type", "composite")
        try:
            key_enum = KeyGeneratorType(key_type)
        except ValueError:
            raise ConfigError(f"Unknown key type: {key_type}")

        return KeyConfig(
            type=key_enum,
            components=data.get("components", []),
            separator=data.get("separator", "-"),
            column=data.get("column"),
            output_column=data.get("output_column", "CHAVE"),
        )

    def _parse_validators(self, data: list[dict[str, Any]]) -> list[ValidatorConfig]:
        """Parse list of validator configurations."""
        validators = []
        for item in data:
            validator_type = item.get("type", "required")
            try:
                validator_enum = ValidatorType(validator_type)
            except ValueError:
                raise ConfigError(f"Unknown validator type: {validator_type}")

            validators.append(ValidatorConfig(
                type=validator_enum,
                enabled=item.get("enabled", True),
                params=item.get("params", {}),
            ))
        return validators

    def _parse_splitters(self, data: list[dict[str, Any]]) -> list[SplitterConfig]:
        """Parse list of splitter configurations."""
        splitters = []
        for item in data:
            splitter_type = item.get("type", "field_value")
            try:
                splitter_enum = SplitterType(splitter_type)
            except ValueError:
                raise ConfigError(f"Unknown splitter type: {splitter_type}")

            splitters.append(SplitterConfig(
                type=splitter_enum,
                enabled=item.get("enabled", True),
                params=item.get("params", {}),
            ))
        return splitters

    def _parse_export(self, data: dict[str, Any] | None) -> ExportConfig | None:
        """Parse export configuration."""
        if not data:
            return None

        return ExportConfig(
            filename_prefix=data.get("filename_prefix", "output"),
            subdir=data.get("subdir", ""),
            format=data.get("format", "zip"),
            add_timestamp=data.get("add_timestamp", True),
            encoding=data.get("encoding", "utf-8-sig"),
            separator=data.get("separator", ";"),
        )

    def _parse_pipeline(self, data: dict[str, Any]) -> PipelineConfig:
        """Parse pipeline configuration."""
        processors = []
        for item in data.get("processors", []):
            proc_type = item.get("type", "tratamento")
            try:
                proc_enum = ProcessorType(proc_type)
            except ValueError:
                raise ConfigError(f"Unknown processor type: {proc_type}")

            processors.append(ProcessorConfig(
                type=proc_enum,
                enabled=item.get("enabled", True),
                params=item.get("params", {}),
            ))

        return PipelineConfig(processors=processors)


def load_client_config(client_name: str, config_dir: Path | str | None = None) -> ClientConfig:
    """Convenience function to load a client configuration."""
    loader = ConfigLoader(config_dir)
    return loader.load(client_name)
