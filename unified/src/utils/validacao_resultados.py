"""Utilitrios para validar a consistncia dos resultados gerados no pipeline."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Sequence

import pandas as pd


@dataclass
class ValidacaoResultado:
    """Resultado da validao de chaves geradas em uma etapa."""

    total_verificado: int
    inconsistencias: pd.DataFrame

    @property
    def possui_inconsistencias(self) -> bool:
        return not self.inconsistencias.empty

    @property
    def amostras_inconsistentes(self) -> Sequence[str]:
        if "CHAVE" in self.inconsistencias.columns:
            return list(self.inconsistencias["CHAVE"].astype(str).head(10))
        return list(self.inconsistencias.head(10).astype(str))


def _normalizar_coluna_chave(series: pd.Series) -> pd.Series:
    """Padroniza a coluna de chave para comparao entre bases."""

    series_normalizada = (
        series.astype(str)
        .str.strip()
        .replace({"": pd.NA, "nan": pd.NA, "None": pd.NA})
    )
    return series_normalizada


def localizar_chaves_presentes(
    df_resultado: pd.DataFrame,
    df_comparacao: pd.DataFrame,
    *,
    coluna_chave: str = "CHAVE",
) -> ValidacaoResultado:
    """Retorna registros do resultado que ainda aparecem na base de comparao."""

    if coluna_chave not in df_resultado.columns:
        raise KeyError(f"Coluna '{coluna_chave}' no encontrada no resultado gerado.")
    if coluna_chave not in df_comparacao.columns:
        raise KeyError(
            f"Coluna '{coluna_chave}' no encontrada na base de comparao."
        )

    chaves_resultado = _normalizar_coluna_chave(df_resultado[coluna_chave])
    chaves_comparacao = _normalizar_coluna_chave(df_comparacao[coluna_chave])

    conjunto_comparacao = set(chaves_comparacao.dropna())
    mascara_inconsistencia = chaves_resultado.isin(conjunto_comparacao)

    inconsistencias = df_resultado.loc[mascara_inconsistencia.fillna(False)].copy()

    return ValidacaoResultado(
        total_verificado=len(df_resultado),
        inconsistencias=inconsistencias,
    )


def localizar_chaves_ausentes(
    df_resultado: pd.DataFrame,
    df_comparacao: pd.DataFrame,
    *,
    coluna_chave: str = "CHAVE",
) -> ValidacaoResultado:
    """Retorna registros do resultado que no aparecem na base de comparao."""

    if coluna_chave not in df_resultado.columns:
        raise KeyError(f"Coluna '{coluna_chave}' no encontrada no resultado gerado.")
    if coluna_chave not in df_comparacao.columns:
        raise KeyError(
            f"Coluna '{coluna_chave}' no encontrada na base de comparao."
        )

    chaves_resultado = _normalizar_coluna_chave(df_resultado[coluna_chave])
    chaves_comparacao = _normalizar_coluna_chave(df_comparacao[coluna_chave])

    conjunto_comparacao = set(chaves_comparacao.dropna())
    mascara_ausencia = ~chaves_resultado.isin(conjunto_comparacao)

    inconsistencias = df_resultado.loc[mascara_ausencia.fillna(True)].copy()

    return ValidacaoResultado(
        total_verificado=len(df_resultado),
        inconsistencias=inconsistencias,
    )


def resumir_amostras(amostras: Iterable[str], limite: int = 5) -> str:
    """Gera uma string amigvel com algumas chaves inconsistentes."""

    amostras = list(amostras)
    if not amostras:
        return ""
    if len(amostras) <= limite:
        return ", ".join(amostras)
    primeiros = ", ".join(amostras[:limite])
    return f"{primeiros} e mais {len(amostras) - limite}..."