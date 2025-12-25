"""Funções auxiliares reutilizáveis para processadores."""

from __future__ import annotations

from datetime import datetime
from typing import Any, List, Optional

import pandas as pd


def primeiro_valor(series: Optional[pd.Series]) -> Optional[Any]:
    """Retorna o primeiro valor não-nulo de uma Series.
    
    Args:
        series: Series do pandas para extrair o primeiro valor.
    
    Returns:
        Primeiro valor não-nulo ou None se a série estiver vazia/toda nula.
    """
    if series is None or series.empty:
        return None
    
    valores_validos = series.dropna()
    if valores_validos.empty:
        return None
    
    return valores_validos.iloc[0]


def normalizar_data_string(valor: Any, formato_saida: str = "%d/%m/%Y") -> Optional[str]:
    """Normaliza valores de data para string no formato especificado.
    
    Args:
        valor: Valor a ser normalizado (string, datetime, etc).
        formato_saida: Formato de saída da data (padrão: DD/MM/YYYY).
    
    Returns:
        String da data formatada ou None se não for possível converter.
    """
    if pd.isna(valor) or valor == "":
        return None
    
    # Se já for datetime
    if isinstance(valor, (pd.Timestamp, datetime)):
        return valor.strftime(formato_saida)
    
    # Tentar converter string para datetime
    try:
        dt = pd.to_datetime(valor, errors="coerce")
        if pd.isna(dt):
            return None
        return dt.strftime(formato_saida)
    except Exception:
        return None


def extrair_data_referencia(
    df: pd.DataFrame,
    colunas_candidatas: Optional[List[str]] = None,
    formato_saida: str = "%d/%m/%Y"
) -> str:
    """Extrai data de referência de um DataFrame.
    
    Procura em colunas candidatas a primeira data válida.
    Se não encontrar, retorna a data atual.
    
    Args:
        df: DataFrame para extrair a data.
        colunas_candidatas: Lista de nomes de colunas para buscar a data.
        formato_saida: Formato de saída da data.
    
    Returns:
        String da data de referência no formato especificado.
    """
    if colunas_candidatas is None:
        colunas_candidatas = [
            "DATA_BASE",
            "DATA BASE",
            "DATA_EXTRACAO_BASE",
            "DATA EXTRACAO BASE",
            "DATA_EXTRACAO",
            "DATA EXTRACAO",
            "DATA_REFERENCIA",
        ]
    
    for coluna in colunas_candidatas:
        if coluna in df.columns:
            valor = primeiro_valor(df[coluna])
            normalizado = normalizar_data_string(valor, formato_saida)
            if normalizado:
                return normalizado
    
    # Fallback: data atual
    return datetime.now().strftime(formato_saida)


__all__ = [
    "primeiro_valor",
    "normalizar_data_string",
    "extrair_data_referencia",
]
