from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Mapping


def _resolve_level(value: Any, default: int) -> int:
    """Return a logging level coerced from config."""
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        level = logging.getLevelName(value.upper())
        if isinstance(level, int):
            return level
    return default


def get_logger(
    name: str,
    log_dir: Path,
    logging_cfg: Mapping[str, Any] | None = None,
) -> logging.Logger:
    """Build a configured logger honoring project logging settings."""

    log_dir.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger(name)

    if logger.handlers:
        return logger

    logging_cfg = logging_cfg or {}
    level = _resolve_level(logging_cfg.get("level"), logging.INFO)
    message_format = logging_cfg.get("format", "%(asctime)s - %(levelname)s - %(message)s")
    date_format = logging_cfg.get("date_format")

    logger.setLevel(level)
    logger.propagate = False

    console_cfg = logging_cfg.get("console_handler", {}) if isinstance(logging_cfg, Mapping) else {}
    console_enabled = bool(console_cfg.get("enabled", True)) if isinstance(console_cfg, Mapping) else True
    if console_enabled:
        console_format = console_cfg.get("format", "%(message)s") if isinstance(console_cfg, Mapping) else "%(message)s"
        console_date_format = console_cfg.get("date_format") if isinstance(console_cfg, Mapping) else None
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(logging.Formatter(console_format, datefmt=console_date_format))
        logger.addHandler(console_handler)

    file_cfg = logging_cfg.get("file_handler", {}) if isinstance(logging_cfg, Mapping) else {}
    file_enabled = bool(file_cfg.get("enabled", False)) if isinstance(file_cfg, Mapping) else False
    if file_enabled:
        filename = file_cfg.get("filename") if isinstance(file_cfg, Mapping) else None
        if not filename:
            filename = f"{name}.log"
        encoding = file_cfg.get("encoding", "utf-8") if isinstance(file_cfg, Mapping) else "utf-8"
        file_handler = logging.FileHandler(log_dir / filename, encoding=encoding)
        file_handler.setFormatter(logging.Formatter(message_format, datefmt=date_format))
        logger.addHandler(file_handler)

    return logger
