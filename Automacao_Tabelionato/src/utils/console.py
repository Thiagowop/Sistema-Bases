"""Utilitarios para padronizar saidas no console."""

from __future__ import annotations

import logging
from typing import Iterable, Sequence

BORDER = "=" * 60


def format_int(value: int | None) -> str:
    """Formata inteiros com separador de milhar."""

    if value is None:
        return "-"
    return f"{value:,}"


def format_percent(value: float, *, precision: int = 1) -> str:
    """Formata percentuais com casas decimais controladas."""

    return f"{value:.{precision}f}%"


def format_duration(seconds: float, *, precision: int = 1) -> str:
    """Formata duracoes em segundos."""

    return f"{seconds:.{precision}f}s"


def suppress_console_info(logger: logging.Logger, level: int = logging.WARNING) -> None:
    """Eleva o nivel dos handlers de console para reduzir ruido."""

    for handler in logger.handlers:
        if isinstance(handler, logging.StreamHandler):
            handler.setLevel(level)


def print_section(
    title: str,
    lines: Sequence[str],
    *,
    leading_break: bool = True,
    trailing_break: bool = True,
) -> None:
    """Imprime um bloco padronizado com borda e corpo."""

    if leading_break:
        print()

    print(BORDER)
    print(title)
    print(BORDER)
    print()

    for line in lines:
        if line:
            print(line)
        else:
            print()

    if trailing_break:
        print()


def print_list(title: str, items: Iterable[str]) -> None:
    """Imprime uma lista simples precedida de um titulo."""

    print(title)
    for item in items:
        print(f"- {item}")
