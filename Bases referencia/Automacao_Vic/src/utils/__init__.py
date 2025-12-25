"""Utilitários compartilhados do projeto VIC."""

from .aging import filtrar_clientes_criticos
from .anti_join import (
    procv_left_minus_right,
    procv_max_menos_vic,
    procv_vic_menos_max,
)
from .text import normalize_ascii_upper, digits_only
from .helpers import (
    primeiro_valor,
    normalizar_data_string,
    extrair_data_referencia,
    formatar_valor_string,
    extrair_telefone,
    formatar_datas_serie,
)
from .filters import VicFilterApplier
from .logger import get_logger, log_section

__all__ = [
    "filtrar_clientes_criticos",
    "procv_left_minus_right",
    "procv_max_menos_vic",
    "procv_vic_menos_max",
    "normalize_ascii_upper",
    "digits_only",
    "primeiro_valor",
    "normalizar_data_string",
    "extrair_data_referencia",
    "formatar_valor_string",
    "extrair_telefone",
    "formatar_datas_serie",
    "VicFilterApplier",
    "get_logger",
    "log_section",
]
