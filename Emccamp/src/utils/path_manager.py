from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Dict

from .io import ensure_directory


@dataclass(frozen=True, slots=True)
class PathManager:
    """Resolves configured input/output directories relative to the project root."""

    base_path: Path
    config: Dict[str, object]

    def resolve_input(self, key: str, default: str) -> Path:
        paths_cfg = self.config.get("paths", {}) if isinstance(self.config, dict) else {}
        inputs_cfg = paths_cfg.get("input", {}) if isinstance(paths_cfg, dict) else {}
        raw = inputs_cfg.get(key, default) if isinstance(inputs_cfg, dict) else default
        path = Path(raw) if isinstance(raw, str) else Path(default)
        resolved = path if path.is_absolute() else self.base_path / path
        ensure_directory(resolved.parent if resolved.suffix else resolved)
        return resolved

    def resolve_output(self, key: str, default: str) -> Path:
        paths_cfg = self.config.get("paths", {}) if isinstance(self.config, dict) else {}
        outputs_cfg = paths_cfg.get("output", {}) if isinstance(paths_cfg, dict) else {}
        base = outputs_cfg.get("base", "data/output") if isinstance(outputs_cfg, dict) else "data/output"
        base_path = Path(base) if isinstance(base, str) else Path("data/output")
        base_resolved = base_path if base_path.is_absolute() else self.base_path / base_path

        subdir = outputs_cfg.get(key, default) if isinstance(outputs_cfg, dict) else default
        sub_path = Path(subdir) if isinstance(subdir, str) else Path(default)
        resolved = sub_path if sub_path.is_absolute() else base_resolved / sub_path
        ensure_directory(resolved)
        return resolved

    def resolve_configured_input(self, key: str, default: str) -> Path:
        inputs_cfg = self.config.get("inputs", {}) if isinstance(self.config, dict) else {}
        raw = inputs_cfg.get(key, default) if isinstance(inputs_cfg, dict) else default
        path = Path(raw) if isinstance(raw, str) else Path(default)
        resolved = path if path.is_absolute() else self.base_path / path
        ensure_directory(resolved.parent if resolved.suffix else resolved)
        return resolved

    def resolve_logs(self) -> Path:
        paths_cfg = self.config.get("paths", {}) if isinstance(self.config, dict) else {}
        raw = paths_cfg.get("logs", "logs") if isinstance(paths_cfg, dict) else "logs"
        path = Path(raw) if isinstance(raw, str) else Path("logs")
        resolved = path if path.is_absolute() else self.base_path / path
        ensure_directory(resolved)
        return resolved

    @staticmethod
    def cleanup(
        directory: Path,
        pattern: str,
        logger: logging.Logger | None = None,
        *,
        silent: bool = False,
    ) -> None:
        """Remove matching files in directory (best-effort)."""
        if not directory.exists():
            return
        for candidate in directory.glob(pattern):
            try:
                candidate.unlink()
                if logger and not silent:
                    logger.info("Removed old file: %s", candidate.name)
            except Exception as exc:  # pragma: no cover - defensive cleanup
                if logger:
                    logger.warning("Could not remove %s: %s", candidate.name, exc)
