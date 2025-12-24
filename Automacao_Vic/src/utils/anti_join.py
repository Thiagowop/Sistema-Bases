"""Operações de anti-join utilizadas nos processadores.

Centraliza funções de diferença de conjuntos (``procv``) reaproveitadas
por batimento e devolução, evitando código duplicado nos processadores.
"""

from __future__ import annotations

from typing import Iterable

import pandas as pd


def _normalize_series(values: pd.Series) -> pd.Series:
    """Normaliza série para comparação eficiente (string strip)."""
    return values.astype(str).str.strip()


def procv_left_minus_right(
    df_left: pd.DataFrame,
    df_right: pd.DataFrame,
    col_left: str,
    col_right: str,
) -> pd.DataFrame:
    """Retorna linhas de df_left cujas chaves não estão em df_right.

    Implementa anti-join simples usando conjunto de chaves normalizadas para
    boa performance e legibilidade.
    """

    if col_left not in df_left.columns:
        raise ValueError(f"Coluna obrigatória ausente no LEFT: {col_left}")
    if col_right not in df_right.columns:
        raise ValueError(f"Coluna obrigatória ausente no RIGHT: {col_right}")

    right_keys: Iterable[str] = set(_normalize_series(df_right[col_right]).dropna())
    mask = ~_normalize_series(df_left[col_left]).isin(right_keys)
    return df_left.loc[mask].copy()


def procv_max_menos_vic(
    df_max: pd.DataFrame,
    df_vic: pd.DataFrame,
    col_max: str = "PARCELA",
    col_vic: str = "CHAVE",
) -> pd.DataFrame:
    """Retorna K_dev = K_max − K_vic (linhas de MAX não presentes em VIC)."""

    return procv_left_minus_right(df_max, df_vic, col_max, col_vic)


def procv_vic_menos_max(
    df_vic: pd.DataFrame,
    df_max: pd.DataFrame,
    col_vic: str = "CHAVE",
    col_max: str = "PARCELA",
) -> pd.DataFrame:
    """Retorna K_bat = K_vic − K_max (linhas de VIC não presentes em MAX)."""

    return procv_left_minus_right(df_vic, df_max, col_vic, col_max)
 

__all__ = [
    "procv_left_minus_right",
    "procv_max_menos_vic",
    "procv_vic_menos_max",
]
