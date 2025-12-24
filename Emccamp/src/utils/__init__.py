"""Utilitários compartilhados da pipeline EMCCAMP."""

from .anti_join import (
	procv_left_minus_right,
	procv_max_menos_emccamp,
	procv_emccamp_menos_max,
)
from .text import digits_only, normalize_ascii_upper
from typing import Any

__all__ = [
	"procv_left_minus_right",
	"procv_max_menos_emccamp",
	"procv_emccamp_menos_max",
	"digits_only",
	"normalize_ascii_upper",
]
