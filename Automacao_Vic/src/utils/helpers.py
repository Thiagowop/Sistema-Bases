"""Utilitários auxiliares comuns para processadores."""

import re
from numbers import Number
from typing import Any, Optional

import pandas as pd


def primeiro_valor(series: Optional[pd.Series]) -> Optional[Any]:
    """Retorna o primeiro valor válido (não nulo e não vazio) de uma Series.
    
    Args:
        series: Series do pandas para extrair o primeiro valor válido
        
    Returns:
        Optional[Any]: Primeiro valor válido encontrado ou None se não houver
        
    Examples:
        >>> import pandas as pd
        >>> s = pd.Series([None, '', 'valor1', 'valor2'])
        >>> primeiro_valor(s)
        'valor1'
        
        >>> s_empty = pd.Series([None, '', 'nan'])
        >>> primeiro_valor(s_empty)
        None
    """
    if series is None:
        return None
    
    for valor in series:
        if pd.isna(valor):
            continue
        texto = str(valor).strip()
        if not texto or texto.lower() == "nan":
            continue
        return valor
    
    return None


def normalizar_data_string(valor: Any) -> Optional[str]:
    """Normaliza um valor para string de data no formato dd/mm/yyyy.
    
    Args:
        valor: Valor a ser normalizado (str, pd.Timestamp, ou outro tipo)
        
    Returns:
        Optional[str]: Data normalizada no formato dd/mm/yyyy ou None se inválida
        
    Examples:
        >>> from datetime import datetime
        >>> normalizar_data_string("2024-01-15")
        '15/01/2024'
        
        >>> normalizar_data_string(pd.Timestamp("2024-01-15"))
        '15/01/2024'
        
        >>> normalizar_data_string("")
        None
    """
    if valor is None:
        return None
    
    if isinstance(valor, str):
        texto = valor.strip()
        if not texto or texto.lower() == "nan":
            return None
        dt = pd.to_datetime(texto, errors="coerce", dayfirst=True)
    elif isinstance(valor, pd.Timestamp):
        dt = valor
    else:
        dt = pd.to_datetime(valor, errors="coerce", dayfirst=True)
    
    if pd.isna(dt):
        return None
    
    return dt.strftime("%d/%m/%Y")


def extrair_data_referencia(df: pd.DataFrame, colunas_candidatas: list[str]) -> Optional[str]:
    """Extrai data de referência do DataFrame usando lista de colunas candidatas.
    
    Args:
        df: DataFrame para extrair a data
        colunas_candidatas: Lista de nomes de colunas para tentar extrair a data
        
    Returns:
        Optional[str]: Data de referência normalizada ou None se não encontrada
        
    Examples:
        >>> df = pd.DataFrame({'DATA_REF': ['2024-01-15'], 'OUTRO': ['valor']})
        >>> extrair_data_referencia(df, ['DATA_REF', 'DATA_REFERENCIA'])
        '15/01/2024'
    """
    candidatos = []
    
    for coluna in colunas_candidatas:
        if coluna in df.columns:
            candidatos.append(primeiro_valor(df[coluna]))
    
    for candidato in candidatos:
        if candidato is not None:
            valor_normalizado = normalizar_data_string(candidato)
            if valor_normalizado:
                return valor_normalizado
    
    return None


def normalizar_decimal(valor: Any) -> Optional[float]:
    """Converte valores com diferentes separadores decimais em float."""
    if valor is None:
        return None

    if isinstance(valor, Number):
        if pd.isna(valor):
            return None
        try:
            return float(valor)
        except (TypeError, ValueError):
            return None

    if pd.isna(valor):
        return None

    texto = str(valor).strip()
    if not texto:
        return None

    if texto.lower() in {"nan", "none", "null"}:
        return None

    texto = (
        texto.replace("R$", "")
        .replace("\u00A0", "")
        .replace("\u202F", "")
        .replace(" ", "")
        .replace("\t", "")
        .replace("\r", "")
        .replace("\n", "")
    )

    if "," in texto and "." in texto:
        texto = texto.replace(".", "").replace(",", ".")
    elif "," in texto:
        texto = texto.replace(",", ".")
    elif texto.count(".") > 1:
        texto = texto.replace(".", "")

    texto = texto.replace("'", "")
    texto = re.sub(r"[^0-9\.\-]", "", texto)

    if not texto or texto in {".", "-", "-.", ".-", "--"}:
        return None

    try:
        return float(texto)
    except ValueError:
        return None


def formatar_valor_string(valor: Any) -> str:
    """Formata um valor como string, tratando casos especiais.
    
    Args:
        valor: Valor a ser formatado
        
    Returns:
        str: Valor formatado como string
        
    Examples:
        >>> formatar_valor_string(None)
        ''
        
        >>> formatar_valor_string(123.45)
        '123.45'
        
        >>> formatar_valor_string('  texto  ')
        'texto'
    """
    if valor is None or (isinstance(valor, float) and pd.isna(valor)):
        return ""
    
    texto = str(valor).strip()
    return texto if texto.lower() != "nan" else ""


def extrair_telefone(valor: Any) -> str:
    """Extrai e formata número de telefone removendo caracteres não numéricos.
    
    Args:
        valor: Valor a ser processado
        
    Returns:
        String com apenas dígitos do telefone
    """
    if pd.isna(valor) or valor is None:
        return ""
    
    texto = str(valor).strip()
    if not texto or texto.lower() in ("nan", "none", "null"):
        return ""
    
    # Remove todos os caracteres não numéricos
    digitos = ''.join(filter(str.isdigit, texto))
    return digitos


def formatar_datas_serie(serie: pd.Series, formato: str = "%d/%m/%Y") -> pd.Series:
    """Formata uma série de datas para string no formato especificado.
    
    Args:
        serie: Série pandas com datas
        formato: Formato de saída da data (padrão: dd/mm/yyyy)
        
    Returns:
        Série com datas formatadas como string
    """
    valores = pd.to_datetime(serie, errors="coerce", dayfirst=True)
    formatted = valores.dt.strftime(formato)
    return formatted.fillna("")
