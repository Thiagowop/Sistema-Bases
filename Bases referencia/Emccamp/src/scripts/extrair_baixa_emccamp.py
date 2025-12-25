#!/usr/bin/env python3
"""Extração da planilha de baixas EMCCAMP via API TOTVS."""

from __future__ import annotations

import sys
import time
from pathlib import Path

from dotenv import load_dotenv

from src.config.loader import ConfigLoader
from src.utils.totvs_client import baixar_baixas_emccamp

BASE = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(BASE))
load_dotenv(BASE / ".env")


def extract_baixas() -> Path:
    loader = ConfigLoader(base_path=BASE)
    config = loader.load()
    return baixar_baixas_emccamp(config)


def main() -> None:
    print("=" * 60)
    print("     EXTRACAO DE BAIXAS EMCCAMP (API TOTVS)")
    print("=" * 60)
    print()

    inicio = time.time()
    try:
        caminho = extract_baixas()
    except Exception as exc:  # pragma: no cover - execução manual
        print(f"[ERRO] Falha na extração de baixas: {exc}")
        sys.exit(1)

    duracao = time.time() - inicio
    print("\n[RESULTADO] Extração de baixas concluída:")
    print(f"        Arquivo salvo em : {caminho}")
    print(f"        Tempo de execução: {duracao:.2f} segundos")


if __name__ == "__main__":
    main()
