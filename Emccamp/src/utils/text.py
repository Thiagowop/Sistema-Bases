"""Utilidades de normalização de texto utilizadas localmente."""
from __future__ import annotations

import unicodedata

import pandas as pd


def normalize_ascii_upper(serie: pd.Series) -> pd.Series:
    """Retorna série normalizada removendo acentos, strip e maiúsculas."""

    def _norm(txt: str) -> str:
        chars = unicodedata.normalize("NFKD", txt)
        chars = "".join(ch for ch in chars if not unicodedata.combining(ch))
        return chars.upper().strip()

    return serie.astype(str).map(_norm)


def digits_only(serie: pd.Series) -> pd.Series:
    """Remove todos os caracteres não numéricos de uma série."""
    return serie.astype(str).str.replace(r"\D", "", regex=True)


__all__ = ["normalize_ascii_upper", "digits_only"]
