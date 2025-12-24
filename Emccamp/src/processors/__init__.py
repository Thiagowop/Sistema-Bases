"""Processadores disponíveis para o pipeline EMCCAMP."""

from .emccamp import EmccampProcessor, ProcessorStats as EmccampStats
from .max import MaxProcessor, MaxStats
from .contact_enrichment import ContactEnrichmentProcessor, ContactEnrichmentStats

__all__ = [
    "EmccampProcessor",
    "EmccampStats",
    "MaxProcessor",
    "MaxStats",
    "ContactEnrichmentProcessor",
    "ContactEnrichmentStats",
]
