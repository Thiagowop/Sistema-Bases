#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Extração da base MAX diretamente do SQL Server."""

from __future__ import annotations

import argparse
import sys
import time
import warnings
import zipfile
from datetime import datetime
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

warnings.filterwarnings('ignore', message='pandas only supports SQLAlchemy connectable')

BASE = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BASE))
sys.path.insert(0, str(BASE / "src"))
load_dotenv(BASE / ".env")

from src.utils.sql_conn import get_std_connection  # type: ignore
from src.utils.queries_sql import SQL_MAX  # type: ignore
from src.config.loader import load_cfg  # type: ignore


def load_config():
    try:
        return load_cfg()
    except Exception as exc:  # pragma: no cover - falha de bootstrap
        print(f"[ERRO] Falha ao carregar configurações: {exc}")
        sys.exit(1)


def extract_max_data(profile: str = 'max'):
    config = load_config()
    max_cfg = config.get(profile, {})

    if not isinstance(max_cfg, dict) or not max_cfg:
        available_profiles = [
            key
            for key, value in config.items()
            if isinstance(value, dict) and {'output_dir', 'output_filename'} & set(value.keys())
        ]
        print(
            f"[ERRO] Configuração '{profile}' não encontrada em config.yaml. "
            f"Perfis disponíveis: {', '.join(sorted(available_profiles)) or 'nenhum perfil com output_dir/output_filename'}"
        )
        return None, 0

    project_root = Path(__file__).parent.parent
    output_dir = project_root / max_cfg.get('output_dir', 'data/input/max')
    output_filename = max_cfg.get('output_filename', 'MaxSmart.zip')
    output_dir.mkdir(parents=True, exist_ok=True)

    conn = get_std_connection()
    if not conn.connect():
        print('[ERRO] Falha na conexão com o banco de dados. Verifique VPN ou credenciais e tente novamente.')
        return None, 0

    try:
        print('[EXEC] Executando consulta MAX no banco de dados...')
        df = conn.execute_query(SQL_MAX)
        if df is None or df.empty:
            print('[ERRO] Nenhum dado retornado pela consulta. Confirme a conexão (VPN ativa) e execute novamente.')
            return None, 0

        registros = len(df)
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        csv_name = f'MaxSmart_{timestamp}.csv'
        zip_path = output_dir / output_filename
        temp_csv = output_dir / csv_name

        df.to_csv(temp_csv, index=False, encoding='utf-8-sig', sep=';')
        print(f'[INFO] CSV temporário gerado: {temp_csv}')

        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zf:
            zf.write(temp_csv, csv_name)
        temp_csv.unlink()

        print(f'[INFO] Arquivo compactado salvo em: {zip_path}')
        print(f'[INFO] Registros extraídos: {registros:,}')
        return zip_path, registros
    finally:
        conn.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Extrai a base MAX em formato ZIP.')
    parser.add_argument(
        '--profile',
        default='max',
        help='Nome da seção do config.yaml com as configurações de saída (default: max).',
    )
    return parser.parse_args()


def main() -> None:
    print('=' * 60)
    print('     EXTRACAO DE DADOS MAX DO BANCO SQL SERVER')
    print('=' * 60)
    print()

    args = parse_args()

    inicio = time.time()

    try:
        print(f"[INFO] Perfil de configuração utilizado: {args.profile}")
        zip_path, registros = extract_max_data(args.profile)
        tempo = time.time() - inicio

        if zip_path:
            print('\n[RESULTADO] Extração concluída com sucesso:')
            print(f'        Arquivo salvo em : {zip_path}')
            print(f'        Registros extraídos: {registros:,}')
            print(f'        Tempo de execução: {tempo:.2f} segundos')
        else:
            print('[ERRO] Falha na extração dos dados.')
            print(f'[INFO] Tempo de execução: {tempo:.2f} segundos')
            sys.exit(1)
    except Exception as exc:
        tempo = time.time() - inicio
        print(f'[ERRO] Exceção durante a extração: {exc}')
        print(f'[INFO] Tempo de execução: {tempo:.2f} segundos')
        sys.exit(1)


if __name__ == '__main__':
    main()

