"""Funções utilitárias de formatação usadas em diferentes etapas do pipeline."""

from __future__ import annotations

import pandas as pd


def formatar_moeda_serie(
    serie: pd.Series,
    *,
    decimal_separator: str = ",",
) -> pd.Series:
    """Normaliza valores monetários e retorna strings formatadas com duas casas."""

    texto = (
        serie.astype(str)
        .str.replace("R$", "", regex=False)
        .str.replace(" ", "", regex=False)
        .str.strip()
    )

    possui_virgula = texto.str.contains(",", na=False)
    texto_normalizado = texto.copy()
    texto_normalizado[possui_virgula] = (
        texto_normalizado[possui_virgula]
        .str.replace(".", "", regex=False)
        .str.replace(",", ".", regex=False)
    )
    texto_normalizado[~possui_virgula] = texto_normalizado[~possui_virgula].str.replace(",", ".", regex=False)

    valores = pd.to_numeric(texto_normalizado, errors="coerce")
    return valores.map(lambda valor: "" if pd.isna(valor) else ("%.2f" % valor).replace(".", decimal_separator))

