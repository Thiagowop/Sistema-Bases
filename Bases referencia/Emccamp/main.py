import argparse
from pathlib import Path

from dotenv import load_dotenv

from src.pipeline import Pipeline
from src.config.loader import ConfigLoader


BASE_DIR = Path(__file__).resolve().parent
ENV_FILE = BASE_DIR / ".env"


if ENV_FILE.exists():
    load_dotenv(ENV_FILE)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python main.py", description="Pipeline EMCCAMP")
    subparsers = parser.add_subparsers(dest="command", required=True)

    extract = subparsers.add_parser("extract", help="Executa extracoes")
    extract.add_argument(
        "dataset",
        choices=["emccamp", "max", "judicial", "baixa", "doublecheck", "all"],
        help="Fonte de dados a extrair",
    )

    treat = subparsers.add_parser("treat", help="Executa tratamentos")
    treat.add_argument("dataset", choices=["emccamp", "max", "all"], help="Fonte de dados a tratar")

    subparsers.add_parser("batimento", help="Executa batimento EMCCAMP x MAX")
    subparsers.add_parser("baixa", help="Executa baixa MAX x EMCCAMP")
    subparsers.add_parser("devolucao", help="Executa devolucao MAX - EMCCAMP")
    enri = subparsers.add_parser("enriquecimento", help="Gera enriquecimento de contato")
    enri.add_argument(
        "--dataset",
        default="emccamp_batimento",
        help="Chave de configuracao em config.yaml para enriquecimento (padrao: emccamp_batimento)",
    )
    return parser


def handle_extract(args: argparse.Namespace, pipeline: Pipeline) -> None:
    if args.dataset in {"emccamp", "all"}:
        pipeline.extract_emccamp()

    if args.dataset in {"max", "all"}:
        pipeline.extract_max()

    if args.dataset in {"judicial", "all"}:
        pipeline.extract_judicial()

    if args.dataset in {"baixa", "all"}:
        pipeline.extract_baixa()

    if args.dataset in {"doublecheck", "all"}:
        pipeline.extract_doublecheck()


def handle_treat(args: argparse.Namespace, pipeline: Pipeline) -> None:
    if args.dataset in {"emccamp", "all"}:
        pipeline.treat_emccamp()
    if args.dataset in {"max", "all"}:
        pipeline.treat_max()


def main(argv: list[str] | None = None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)
    loader = ConfigLoader(base_path=BASE_DIR)
    pipeline = Pipeline(loader=loader)

    if args.command == "extract":
        handle_extract(args, pipeline)
    elif args.command == "treat":
        handle_treat(args, pipeline)
    elif args.command == "batimento":
        pipeline.batimento()
    elif args.command == "baixa":
        pipeline.baixa()
    elif args.command == "devolucao":
        pipeline.devolucao()
    elif args.command == "enriquecimento":
        pipeline.enriquecimento(args.dataset)


if __name__ == "__main__":
    main()
