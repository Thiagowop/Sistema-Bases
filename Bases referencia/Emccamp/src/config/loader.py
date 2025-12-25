"""Configuration loader for the EMCCAMP project."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict

import yaml


class LoadedConfig:
    """Represents the project configuration loaded from disk."""

    def __init__(self, data: Dict[str, Any], base_path: Path):
        self.data = data
        self.base_path = base_path
        self._mapping_cache: Dict[str, Dict[str, Any]] = {}

    def get(self, key: str, default: Any | None = None) -> Any:
        """Returns a top-level configuration value."""

        return self.data.get(key, default)

    def get_mapping(self, name: str) -> Dict[str, Any]:
        """Loads and caches column mapping definitions from config.yaml."""

        if name not in self._mapping_cache:
            mappings = self.data.get("mappings", {})
            if name not in mappings:
                raise KeyError(f"Mapping '{name}' not found in config.yaml under 'mappings' section")
            self._mapping_cache[name] = mappings[name]
        return self._mapping_cache[name]


class ConfigLoader:
    """Loads YAML configuration files for the pipeline."""

    def __init__(self, base_path: Path | None = None, config_path: Path | None = None) -> None:
        if base_path is None:
            base_path = Path.cwd()
        self.base_path = Path(base_path).resolve()

        if config_path is None:
            config_path = self.base_path / "src" / "config" / "config.yaml"
        self.config_path = Path(config_path).resolve()

    def load(self) -> LoadedConfig:
        """Loads the YAML configuration file."""

        if not self.config_path.exists():
            raise FileNotFoundError(f"Configuration file not found: {self.config_path}")

        with self.config_path.open("r", encoding="utf-8") as handle:
            data: Dict[str, Any] = yaml.safe_load(handle) or {}

        return LoadedConfig(data=data, base_path=self.base_path)
