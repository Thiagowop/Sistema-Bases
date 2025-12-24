#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Extração consolidada das bases judiciais (Autojur + MAX Smart)."""

from __future__ import annotations

import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional
import zipfile

import pandas as pd
from dotenv import load_dotenv

BASE = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BASE))
sys.path.insert(0, str(BASE / "src"))
load_dotenv(BASE / ".env")

from src.config.loader import load_cfg  # type: ignore
from src.utils.sql_conn import get_std_connection, get_candiotto_connection  # type: ignore
from src.utils.queries_sql import SQL_AUTOJUR, SQL_MAXSMART_JUDICIAL  # type: ignore


def load_config() -> Dict[str, str]:
    try:
        config = load_cfg()
        return config.get("judicial", {})
    except Exception as exc:  # pragma: no cover - falha de bootstrap
        print(f"[ERRO] Falha ao carregar configurações: {exc}")
        sys.exit(1)


def _executar_consulta(conector, sql: str, descricao: str) -> pd.DataFrame:
    print(f"[EXEC] {descricao}...")
    if not conector.connect():
        raise RuntimeError(f"Falha na conexão com {descricao}. Verifique VPN/conexão e tente novamente.")
    try:
        df = conector.execute_query(sql)
        total = 0 if df is None else len(df)
        print(f"[OK] {descricao}: {total} registros")
        if df is None:
            return pd.DataFrame(columns=["CPF_CNPJ", "ORIGEM"])
        return df
    finally:
        conector.close()


def extract_from_autojur() -> pd.DataFrame:
    conn = get_candiotto_connection()
    return _executar_consulta(conn, SQL_AUTOJUR, "Consulta AUTOJUR")


def extract_from_maxsmart() -> pd.DataFrame:
    conn = get_std_connection()
    return _executar_consulta(conn, SQL_MAXSMART_JUDICIAL, "Consulta MAX Smart Judicial")


def combine_and_deduplicate(df_autojur: pd.DataFrame, df_max_smart: pd.DataFrame) -> pd.DataFrame:
    print("[INFO] Combinando resultados...")
    print(f"        AUTOJUR : {len(df_autojur)} registros")
    print(f"        MAX_SMART: {len(df_max_smart)} registros")

    combinados = pd.concat([df_autojur, df_max_smart], ignore_index=True)
    if 'CPF_CNPJ' in combinados.columns:
        combinados['_CPF_DIGITO'] = combinados['CPF_CNPJ'].astype(str).str.replace(r"[^0-9]", "", regex=True)
        chave = '_CPF_DIGITO'
    else:
        chave = 'CPF_CNPJ'

    unicos = combinados.drop_duplicates(subset=[chave], keep='first')
    removidos = len(combinados) - len(unicos)
    print(f"[INFO] Duplicatas removidas: {removidos}")
    print(f"[INFO] Total único: {len(unicos)} registros")

    if '_CPF_DIGITO' in unicos.columns:
        unicos = unicos.drop(columns=['_CPF_DIGITO'])

    return unicos


def save_to_zip(df: pd.DataFrame, config: Dict[str, str]) -> Path:
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    csv_name = f'ClientesJudiciais_{timestamp}.csv'

    project_root = Path(__file__).parent.parent
    output_dir = project_root / config.get('output_dir', 'data/input/judicial')
    zip_name = config.get('output_filename', 'ClientesJudiciais.zip')
    output_dir.mkdir(parents=True, exist_ok=True)

    csv_path = output_dir / csv_name
    zip_path = output_dir / zip_name

    print(f"[INFO] Salvando CSV temporário em: {csv_path}")
    df.to_csv(csv_path, index=False, encoding='utf-8-sig', sep=';')

    print(f"[INFO] Compactando para: {zip_path}")
    with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zf:
        zf.write(csv_path, csv_name)

    csv_path.unlink()
    return zip_path


def main() -> None:
    print('=' * 60)
    print('     EXTRACAO DE DADOS JUDICIAIS')
    print('=' * 60)
    print()

    inicio = time.time()

    try:
        config = load_config()

        df_autojur = extract_from_autojur()
        df_max = extract_from_maxsmart()

        df_final = combine_and_deduplicate(df_autojur, df_max)

        if df_final.empty:
            print('\n[AVISO] Nenhum dado judicial encontrado para extrair.')
            print(f"[INFO] Tempo de execução: {time.time() - inicio:.2f} segundos")
            return

        zip_path = save_to_zip(df_final, config)

        tempo = time.time() - inicio
        print('\n[RESULTADO] Extração concluída com sucesso:')
        print(f'        Arquivo gerado : {zip_path}')
        print(f'        Registros únicos: {len(df_final):,}')
        print(f"        Tempo de execução: {tempo:.2f} segundos")

    except Exception as exc:
        tempo = time.time() - inicio
        print(f"\n[ERRO] Falha durante a extração judicial: {exc}")
        print(f"[INFO] Tempo de execução: {tempo:.2f} segundos")
        sys.exit(1)


if __name__ == '__main__':
    main()
