"""Configuracao centralizada de logging para o projeto Tabelionato."""

from __future__ import annotations

import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, Optional

FILE_LOG_FORMAT = "%(asctime)s | %(levelname)-8s | %(message)s"
CONSOLE_LOG_FORMAT = "%(message)s"
LOG_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
_HEADER_SEPARATOR = "=" * 72
_SECTION_SEPARATOR = "-" * 72


class _ConsoleFormatter(logging.Formatter):
    """Formatter para console que exibe o nível apenas em avisos/erros."""

    def format(self, record: logging.LogRecord) -> str:
        message = super().format(record)
        if record.levelno >= logging.WARNING:
            return f"{record.levelname.upper()}: {message}"
        return message


class TabelionatoLogger:
    """Logger centralizado para todo o projeto Tabelionato."""

    _instance: Optional["TabelionatoLogger"] = None
    _logger: Optional[logging.Logger] = None

    def __new__(cls) -> "TabelionatoLogger":
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self) -> None:
        if self._logger is None:
            self._setup_logger()

    def _setup_logger(self) -> None:
        base_dir = Path(__file__).resolve().parent.parent.parent
        logs_dir = base_dir / "data" / "logs"
        logs_dir.mkdir(parents=True, exist_ok=True)
        log_file = logs_dir / "tabelionato.log"

        logger = logging.getLogger("tabelionato")
        logger.setLevel(logging.INFO)
        logger.propagate = False

        if logger.handlers:
            for handler in logger.handlers:
                try:
                    handler.close()
                except Exception:
                    pass
            logger.handlers.clear()

        file_handler = logging.FileHandler(log_file, mode="a", encoding="utf-8")
        file_handler.setLevel(logging.INFO)
        file_handler.setFormatter(logging.Formatter(FILE_LOG_FORMAT, datefmt=LOG_DATE_FORMAT))

        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(_ConsoleFormatter(CONSOLE_LOG_FORMAT))

        logger.addHandler(file_handler)
        logger.addHandler(console_handler)

        logger.info(_HEADER_SEPARATOR)
        logger.info("NOVA SESSAO TABELIONATO - %s", datetime.now().strftime("%d/%m/%Y %H:%M:%S"))
        logger.info(_HEADER_SEPARATOR)

        self._logger = logger

    def get_logger(self) -> logging.Logger:
        assert self._logger is not None
        return self._logger

    def info(self, message: str) -> None:
        self.get_logger().info(message)

    def warning(self, message: str) -> None:
        self.get_logger().warning(message)

    def error(self, message: str) -> None:
        self.get_logger().error(message)

    def debug(self, message: str) -> None:
        self.get_logger().debug(message)


def get_logger(name: str | None = None) -> logging.Logger:
    base_logger = TabelionatoLogger().get_logger()
    if not name:
        return base_logger

    child_logger = logging.getLogger(f"tabelionato.{name}")
    if not child_logger.handlers:
        child_logger.propagate = True
    return child_logger


def _log_section_header(logger: logging.Logger, title: str) -> None:
    logger.info("")
    logger.info(_SECTION_SEPARATOR)
    logger.info(title)
    logger.info(_SECTION_SEPARATOR)


def log_session_start(module_name: str) -> None:
    logger = get_logger()
    _log_section_header(logger, f"INICIO {module_name.upper()}")


def log_session_end(module_name: str, success: bool = True) -> None:
    logger = get_logger()
    status = "CONCLUIDO" if success else "ERRO"
    prefix = "FINALIZADO" if success else "FINALIZADO COM ERRO"
    _log_section_header(logger, f"{prefix} {module_name.upper()} - {status}")
    logger.info("")


def log_error_section(error_message: str) -> None:
    logger = get_logger()
    _log_section_header(logger, "ERRO")
    logger.error("ERRO AO PROCESSAR: %s", error_message)
    logger.error(_SECTION_SEPARATOR)
    logger.error("")


def log_info_section(title: str, message: str = "") -> None:
    logger = get_logger()
    _log_section_header(logger, title.upper())
    if message:
        logger.info(message)
    logger.info(_SECTION_SEPARATOR)
    logger.info("")


def log_metrics(title: str, metrics: Dict[str, object]) -> None:
    logger = get_logger()
    _log_section_header(logger, title.upper())
    for key, value in metrics.items():
        logger.info("%s: %s", key, value)
    logger.info(_SECTION_SEPARATOR)
    logger.info("")


def log_validation_result(context: str, total_checked: int, inconsistencies: Iterable[str]) -> None:
    inconsistencies = list(inconsistencies)
    logger = get_logger()

    if not inconsistencies:
        logger.info(
            "Validacao '%s': nenhum dos %s registros aparece na base de comparacao.",
            context,
            total_checked,
        )
        return

    logger.error(
        "Validacao '%s' falhou: %s registros aparecem na base de comparacao.",
        context,
        len(inconsistencies),
    )


def log_validation_presence(
    context: str, total_checked: int, missing_keys: Iterable[str]
) -> None:
    ausentes = list(missing_keys)
    logger = get_logger()

    if not ausentes:
        logger.info(
            "Validacao '%s': todos os %s registros foram localizados na base de origem.",
            context,
            total_checked,
        )
        return

    logger.error(
        "Validacao '%s' falhou: %s registros nao foram encontrados na base de origem.",
        context,
        len(ausentes),
    )
    for sample in ausentes[:10]:
        logger.error("Chave ausente: %s", sample)
