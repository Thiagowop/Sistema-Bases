#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Extração da base EMCCAMP via API TOTVS."""

from __future__ import annotations

import sys
import time
from pathlib import Path

from dotenv import load_dotenv

from src.config.loader import ConfigLoader
from src.utils.totvs_client import baixar_emccamp

BASE = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(BASE))
load_dotenv(BASE / ".env")


def main() -> None:
    print('=' * 60)
    print('     EXTRACAO DE DADOS EMCCAMP (API TOTVS)')
    print('=' * 60)
    print()

    inicio = time.time()
    loader = ConfigLoader(base_path=BASE)
    config = loader.load()
    zip_path = baixar_emccamp(config)
    duracao = time.time() - inicio

    print('\n[RESULTADO] Extração concluída com sucesso:')
    print(f'        Arquivo salvo em : {zip_path}')
    print(f'        Tempo de execução: {duracao:.2f} segundos')


if __name__ == '__main__':
    main()
