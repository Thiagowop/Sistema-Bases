"""Operações de anti-join utilizadas nos processadores locais.

Centraliza funções de diferença de conjuntos reaproveitadas
por batimento e devolução, evitando duplicação de lógica.
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
    """Retorna linhas de df_left cujas chaves não estão em df_right."""

    if col_left not in df_left.columns:
        raise ValueError(f"Coluna obrigatória ausente no LEFT: {col_left}")
    if col_right not in df_right.columns:
        raise ValueError(f"Coluna obrigatória ausente no RIGHT: {col_right}")

    right_keys: Iterable[str] = set(_normalize_series(df_right[col_right]).dropna())
    mask = ~_normalize_series(df_left[col_left]).isin(right_keys)
    return df_left.loc[mask].copy()


def procv_max_menos_emccamp(
    df_max: pd.DataFrame,
    df_emccamp: pd.DataFrame,
    col_max: str = "PARCELA",
    col_emccamp: str = "CHAVE",
) -> pd.DataFrame:
    """Retorna registros MAX que NÃO estão em EMCCAMP (MAX - EMCCAMP).
    
    Usado para gerar arquivo de devolução: títulos no sistema de cobrança
    que não existem mais no credor.
    """
    return procv_left_minus_right(df_max, df_emccamp, col_max, col_emccamp)


def procv_emccamp_menos_max(
    df_emccamp: pd.DataFrame,
    df_max: pd.DataFrame,
    col_emccamp: str = "CHAVE",
    col_max: str = "PARCELA",
) -> pd.DataFrame:
    """Retorna registros EMCCAMP que NÃO estão em MAX (EMCCAMP - MAX).
    
    Usado para batimento: títulos do credor ausentes no sistema de cobrança.
    """
    return procv_left_minus_right(df_emccamp, df_max, col_emccamp, col_max)


__all__ = [
    "procv_left_minus_right",
    "procv_max_menos_emccamp",
    "procv_emccamp_menos_max",
]
