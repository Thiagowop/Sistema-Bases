#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Extração das bases judiciais (Autojur + MAX Smart) para EMCCAMP."""

from __future__ import annotations

import sys
import time
from pathlib import Path

from dotenv import load_dotenv
import pandas as pd

from src.config.loader import ConfigLoader
from src.utils.io import write_csv_to_zip
from src.utils.queries import get_query
from src.utils.sql_conn import get_candiotto_connection, get_std_connection
from src.utils.output_formatter import format_extraction_judicial_output

BASE = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(BASE))
load_dotenv(BASE / ".env")


def _executar(conn_factory, config, nome_query: str) -> pd.DataFrame:
    conn = conn_factory(config.base_path)
    if not conn.connect():
        print(f"\n[ERRO] Falha na conexao SQL para {nome_query}")
        print("[INFO] Verifique VPN ou credenciais do banco de dados\n")
        raise RuntimeError(f'Falha na conexao para consulta {nome_query}')
    try:
        query = get_query(config, nome_query)
        return conn.execute_query(query)
    finally:
        conn.close()


def main() -> None:
    inicio = time.time()
    loader = ConfigLoader(base_path=BASE)
    config = loader.load()

    df_autojur = _executar(get_candiotto_connection, config, 'autojur')
    df_maxsmart = _executar(get_std_connection, config, 'maxsmart_judicial')
    
    df_final = pd.concat([df_autojur, df_maxsmart], ignore_index=True)
    df_final['CPF_CNPJ'] = df_final['CPF_CNPJ'].astype(str).str.replace(r'[^0-9]', '', regex=True)
    
    duplicatas = len(df_final) - len(df_final.drop_duplicates(subset=['CPF_CNPJ'], keep='first'))
    df_final = df_final.drop_duplicates(subset=['CPF_CNPJ'], keep='first')

    output_cfg = config.get('paths', {}).get('output', {})
    input_cfg = config.get('paths', {}).get('input', {})

    input_dir = Path(input_cfg.get('judicial', 'data/input/judicial'))
    input_dir = input_dir if input_dir.is_absolute() else config.base_path / input_dir
    input_dir.mkdir(parents=True, exist_ok=True)

    # Limpar arquivos antigos
    for arquivo_antigo in input_dir.glob('ClientesJudiciais*.zip'):
        try:
            arquivo_antigo.unlink()
        except Exception:
            pass

    global_cfg = config.get('global', {})
    sep = global_cfg.get('csv_separator', ',')
    encoding = global_cfg.get('encoding', 'utf-8-sig')

    timestamp = time.strftime('%Y%m%d_%H%M%S')
    csv_name = f'ClientesJudiciais_{timestamp}.csv'
    zip_path = input_dir / 'ClientesJudiciais.zip'
    write_csv_to_zip({csv_name: df_final}, zip_path, sep=sep, encoding=encoding)

    duracao = time.time() - inicio
    
    format_extraction_judicial_output(
        autojur_records=len(df_autojur),
        maxsmart_records=len(df_maxsmart),
        duplicates_removed=duplicatas,
        total_unique=len(df_final),
        output_file=str(zip_path),
        duration=duracao
    )


if __name__ == '__main__':
    main()
