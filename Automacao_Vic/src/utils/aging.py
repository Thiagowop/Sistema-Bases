"""Utility helpers for aging calculation and critical client detection."""
from __future__ import annotations

from datetime import datetime
from typing import Set, Tuple

import pandas as pd


def filtrar_clientes_criticos(
    df: pd.DataFrame,
    col_cliente: str,
    col_vencimento: str,
    limite: int,
    data_referencia: datetime | None = None,
) -> Tuple[pd.DataFrame, Set[str]]:
    """Filter clients with aging above ``limite`` and report removed ids.

    Args:
        df: Original dataset.
        col_cliente: Column name containing the client identifier.
        col_vencimento: Column name with the due date.
        limite: Threshold (in days) to consider a client critical.
        data_referencia: Reference date for the aging calculation (defaults to now).

    Returns:
        Tuple[pd.DataFrame, Set[str]]: DataFrame filtered by critical clients and the
        set of client identifiers removed during the process.
    """

    if col_cliente not in df.columns:
        raise ValueError("Coluna de cliente ausente para calculo de aging")
    if col_vencimento not in df.columns:
        raise ValueError("Coluna de vencimento ausente para calculo de aging")

    if df.empty:
        return df.copy(), set()

    ref = pd.Timestamp(data_referencia or datetime.now())

    df_work = df.copy()
    vencimentos = pd.to_datetime(df_work[col_vencimento], errors="coerce")
    invalid_mask = vencimentos.isna()

    clientes_invalidos = set(
        df_work.loc[invalid_mask, col_cliente].astype(str).str.strip()
    )
    df_work = df_work.loc[~invalid_mask].copy()

    if df_work.empty:
        return df_work, {c for c in clientes_invalidos if c}

    aging = (ref - vencimentos.loc[~invalid_mask]).dt.days.clip(lower=0)
    df_work["_AGING_POS"] = aging

    aging_por_cliente = df_work.groupby(col_cliente)["_AGING_POS"].max()
    clientes_criticos = set(aging_por_cliente[aging_por_cliente >= limite].index)

    df_filtrado = df_work[df_work[col_cliente].isin(clientes_criticos)].copy()
    df_filtrado.drop(columns=["_AGING_POS"], inplace=True, errors="ignore")

    clientes_remanescentes = set(
        df_filtrado[col_cliente].astype(str).str.strip()
    )
    clientes_originais = set(df[col_cliente].astype(str).str.strip())

    clientes_removidos = {
        c for c in clientes_originais if c and c not in clientes_remanescentes
    }.union({c for c in clientes_invalidos if c})

    return df_filtrado, clientes_removidos


__all__ = ["filtrar_clientes_criticos"]
