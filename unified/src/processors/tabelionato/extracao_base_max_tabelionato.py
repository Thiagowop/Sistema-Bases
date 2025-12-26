#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Extrao da base MAX especfica para Tabelionato diretamente do SQL Server."""

from __future__ import annotations

import argparse
import logging
import sys
import time
import warnings
import zipfile
from datetime import datetime
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

from src.utils.console import format_duration, format_int, print_section
from src.utils.logger_config import get_logger

warnings.filterwarnings('ignore', message='pandas only supports SQLAlchemy connectable')

# Configurar caminhos para o projeto principal
BASE = Path(__file__).resolve().parent
PARENT_BASE = BASE.parent

# Carregar .env da raiz do projeto
load_dotenv(PARENT_BASE / ".env")

from src.utils.sql_conn import get_std_connection  # type: ignore
from src.utils.queries_tabelionato import SQL_MAX_TABELIONATO  # type: ignore

logger = get_logger("extracao_max")


def _suppress_console_info(logger_obj: logging.Logger) -> None:
    for handler in logger_obj.handlers:
        if isinstance(handler, logging.StreamHandler):
            handler.setLevel(logging.WARNING)


def load_config():
    """Configurao padro para Tabelionato."""
    return {
        'tabelionato': {
            'output_dir': 'data/input/max',
            'output_filename': 'MaxSmart_Tabelionato.zip'
        }
    }


def extract_max_tabelionato_data(profile: str = 'tabelionato'):
    """Extrai dados MAX especficos para Tabelionato."""
    config = load_config()
    max_cfg = config.get(profile, {})
    
    if not isinstance(max_cfg, dict) or not max_cfg:
        available_profiles = [
            key
            for key, value in config.items()
            if isinstance(value, dict) and {'output_dir', 'output_filename'} & set(value.keys())
        ]
        logger.error(
            "Configuracao '%s' nao encontrada. Perfis disponiveis: %s",
            profile,
            ", ".join(sorted(available_profiles)) or "nenhum perfil com output_dir/output_filename",
        )
        return None, 0
    
    # Configurar diretrios
    project_root = PARENT_BASE
    output_dir = project_root / max_cfg.get('output_dir', 'data/input/max')
    output_filename = max_cfg.get('output_filename', 'MaxSmart_Tabelionato.zip')
    output_dir.mkdir(parents=True, exist_ok=True)

    # Conectar ao banco
    conn = get_std_connection()
    if not conn.connect():
        logger.error('Falha na conexao com o banco de dados. Verifique VPN ou credenciais e tente novamente.')
        return None, 0

    try:
        logger.info('Executando consulta MAX Tabelionato no banco de dados...')
        df = conn.execute_query(SQL_MAX_TABELIONATO)
        if df is None or df.empty:
            logger.error('Nenhum dado retornado pela consulta. Confirme a conexao (VPN ativa) e execute novamente.')
            return None, 0

        registros = len(df)
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        csv_name = f'MaxSmart_Tabelionato_{timestamp}.csv'
        zip_path = output_dir / output_filename
        temp_csv = output_dir / csv_name

        # Salvar CSV temporrio
        df.to_csv(temp_csv, index=False, encoding='utf-8-sig', sep=';')
        logger.info('CSV temporario gerado: %s', temp_csv)

        # Compactar em ZIP
        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zf:
            zf.write(temp_csv, csv_name)
        temp_csv.unlink()

        logger.info('Arquivo compactado salvo em: %s', zip_path)
        logger.info('Registros extraidos: %s', f"{registros:,}")
        return zip_path, registros
    finally:
        conn.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Extrai a base MAX Tabelionato em formato ZIP.')
    parser.add_argument(
        '--profile',
        default='tabelionato',
        help='Nome da seo do config.yaml com as configuraes de sada (default: tabelionato).',
    )
    return parser.parse_args()


def main() -> None:
    """Funcao principal de extracao."""

    _suppress_console_info(logger)
    args = parse_args()

    inicio = time.time()

    try:
        logger.info("Perfil de configuracao utilizado: %s", args.profile)
        zip_path, registros = extract_max_tabelionato_data(args.profile)
        tempo = time.time() - inicio

        if zip_path:
            linhas = [
                "[STEP] Extracao MAX Tabelionato",
                "",
                f"Perfil utilizado: {args.profile}",
                f"Registros extraidos: {format_int(registros)}",
                "",
                f"Arquivo exportado: {zip_path}",
                f"Duracao: {format_duration(tempo, precision=2)}",
            ]
            print_section("EXTRACAO - MAX", linhas, leading_break=False)
        else:
            linhas = [
                "[ERRO] Falha na extracao dos dados.",
                "",
                f"Perfil utilizado: {args.profile}",
                f"Duracao: {format_duration(tempo, precision=2)}",
            ]
            print_section("EXTRACAO - MAX", linhas, leading_break=False)
            sys.exit(1)
    except Exception as exc:
        tempo = time.time() - inicio
        logger.exception("Erro durante a extracao")
        linhas = [
            "[ERRO] Excecao durante a extracao.",
            "",
            f"Detalhes: {exc}",
            f"Duracao: {format_duration(tempo, precision=2)}",
        ]
        print_section("EXTRACAO - MAX", linhas, leading_break=False)
        sys.exit(1)


if __name__ == '__main__':
    main()
