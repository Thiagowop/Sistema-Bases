"""Processadores do sistema VIC."""

from .max import MaxProcessor
from .vic import VicProcessor
from .devolucao import DevolucaoProcessor
from .batimento import BatimentoProcessor
from .baixa import BaixaProcessor
from .enriquecimento import EnriquecimentoVicProcessor

__all__ = [
    'MaxProcessor',
    'VicProcessor',
    'DevolucaoProcessor',
    'BatimentoProcessor',
    'EnriquecimentoVicProcessor',
    'BaixaProcessor',
]
