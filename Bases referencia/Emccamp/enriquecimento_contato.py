from __future__ import annotations

import argparse

from src.config.loader import ConfigLoader
from src.processors.contact_enrichment import ContactEnrichmentProcessor, ContactEnrichmentStats


def executar(dataset: str = "emccamp_batimento") -> ContactEnrichmentStats:
    loader = ConfigLoader()
    config = loader.load()
    processor = ContactEnrichmentProcessor(config, dataset)
    stats = processor.run()
    processor.print_summary(stats)
    return stats


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Gera enriquecimento de contato a partir de bases tratadas."
    )
    parser.add_argument(
        "--dataset",
        default="emccamp_batimento",
        help="Chave de configuracao em config.yaml (padrao: emccamp_batimento).",
    )
    args = parser.parse_args()
    executar(args.dataset)


if __name__ == "__main__":
    main()
