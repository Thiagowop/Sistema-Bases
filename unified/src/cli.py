"""
CLI interface for the Unified Pipeline System.
Provides command-line access to run pipelines for clients.
"""
from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime
from pathlib import Path

from .core import (
    ConfigLoader,
    PipelineEngine,
    ProcessorType,
)
from .processors import (
    TratamentoProcessor,
    BatimentoProcessor,
    BaixaProcessor,
    DevolucaoProcessor,
    EnriquecimentoProcessor,
)


def setup_logging(level: str = "INFO", log_file: Path | None = None) -> None:
    """Configure logging for the CLI."""
    log_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    handlers: list[logging.Handler] = [logging.StreamHandler(sys.stdout)]

    if log_file:
        log_file.parent.mkdir(parents=True, exist_ok=True)
        handlers.append(logging.FileHandler(log_file, encoding="utf-8"))

    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format=log_format,
        handlers=handlers,
    )


def register_processors(engine: PipelineEngine) -> None:
    """Register all standard processors with the engine."""
    engine.register_processor(ProcessorType.TRATAMENTO, TratamentoProcessor)
    engine.register_processor(ProcessorType.BATIMENTO, BatimentoProcessor)
    engine.register_processor(ProcessorType.BAIXA, BaixaProcessor)
    engine.register_processor(ProcessorType.DEVOLUCAO, DevolucaoProcessor)
    engine.register_processor(ProcessorType.ENRIQUECIMENTO, EnriquecimentoProcessor)


def cmd_run(args: argparse.Namespace) -> int:
    """Run pipeline for a client."""
    config_dir = Path(args.config_dir)
    output_dir = Path(args.output_dir)

    # Setup logging
    log_file = None
    if args.log_file:
        log_file = Path(args.log_file)
    setup_logging(args.log_level, log_file)

    logger = logging.getLogger(__name__)
    logger.info(f"Starting pipeline for client: {args.client}")

    # Initialize engine
    engine = PipelineEngine(config_dir=config_dir, output_dir=output_dir)
    register_processors(engine)

    # Run pipeline
    result = engine.run(args.client)

    # Print results
    print("\n" + "=" * 60)
    print(f"Pipeline Result: {'SUCCESS' if result.success else 'FAILED'}")
    print("=" * 60)
    print(f"Client: {result.context.client_config.name}")
    print(f"Duration: {result.duration_seconds:.2f} seconds")
    print(f"Client records: {result.summary.get('client_records', 0)}")
    print(f"MAX records: {result.summary.get('max_records', 0)}")
    print(f"Errors: {result.summary.get('errors', 0)}")

    if result.context.outputs:
        print("\nOutput files:")
        for name, path in result.context.outputs.items():
            print(f"  - {name}: {path}")

    if result.context.errors:
        print("\nErrors encountered:")
        for error in result.context.errors:
            print(f"  - {error}")

    print("=" * 60)

    return 0 if result.success else 1


def cmd_list(args: argparse.Namespace) -> int:
    """List available clients."""
    config_dir = Path(args.config_dir)

    if not config_dir.exists():
        print(f"Config directory not found: {config_dir}")
        return 1

    clients = list(config_dir.glob("*.yaml")) + list(config_dir.glob("*.yml"))

    if not clients:
        print("No client configurations found.")
        return 0

    print("\nAvailable clients:")
    print("-" * 40)

    loader = ConfigLoader(config_dir)
    for client_path in sorted(clients):
        try:
            config = loader.load_from_file(client_path)
            print(f"  {config.name}")
            if config.description:
                print(f"    {config.description}")
        except Exception as e:
            print(f"  {client_path.stem} (error: {e})")

    print("-" * 40)
    return 0


def cmd_validate(args: argparse.Namespace) -> int:
    """Validate client configuration."""
    config_dir = Path(args.config_dir)
    loader = ConfigLoader(config_dir)

    try:
        config = loader.load(args.client)
        print(f"\nConfiguration for '{args.client}' is valid!")
        print("-" * 40)
        print(f"Name: {config.name}")
        print(f"Version: {config.version}")
        print(f"Description: {config.description}")

        if config.client_source:
            print(f"\nClient source:")
            print(f"  Loader: {config.client_source.loader.type.value}")
            print(f"  Key type: {config.client_source.key.type.value}")
            print(f"  Validators: {len(config.client_source.validators)}")
            print(f"  Splitters: {len(config.client_source.splitters)}")

        if config.max_source:
            print(f"\nMAX source:")
            print(f"  Loader: {config.max_source.loader.type.value}")
            print(f"  Key type: {config.max_source.key.type.value}")

        if config.pipeline.processors:
            print(f"\nPipeline processors:")
            for proc in config.pipeline.processors:
                status = "enabled" if proc.enabled else "disabled"
                print(f"  - {proc.type.value} ({status})")

        print("-" * 40)
        return 0

    except Exception as e:
        print(f"\nConfiguration error: {e}")
        return 1


def main() -> int:
    """Main entry point for the CLI."""
    parser = argparse.ArgumentParser(
        description="Unified Pipeline System - CLI",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run pipeline for VIC client
  python -m unified.src.cli run vic

  # Run with custom config and output directories
  python -m unified.src.cli run vic --config-dir ./configs/clients --output-dir ./output

  # List all available clients
  python -m unified.src.cli list

  # Validate a client configuration
  python -m unified.src.cli validate vic
        """,
    )

    parser.add_argument(
        "--config-dir",
        type=str,
        default="./configs/clients",
        help="Directory containing client configuration files",
    )

    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # Run command
    run_parser = subparsers.add_parser("run", help="Run pipeline for a client")
    run_parser.add_argument("client", type=str, help="Client name (without .yaml)")
    run_parser.add_argument(
        "--output-dir",
        type=str,
        default="./output",
        help="Output directory for results",
    )
    run_parser.add_argument(
        "--log-level",
        type=str,
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging level",
    )
    run_parser.add_argument(
        "--log-file",
        type=str,
        default=None,
        help="Log file path (optional)",
    )
    run_parser.set_defaults(func=cmd_run)

    # List command
    list_parser = subparsers.add_parser("list", help="List available clients")
    list_parser.set_defaults(func=cmd_list)

    # Validate command
    validate_parser = subparsers.add_parser("validate", help="Validate client configuration")
    validate_parser.add_argument("client", type=str, help="Client name to validate")
    validate_parser.set_defaults(func=cmd_validate)

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return 0

    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
