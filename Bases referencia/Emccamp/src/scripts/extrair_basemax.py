#!/usr/bin/env python3
"""Extraction routine for the MAX dataset used by EMCCAMP."""

from __future__ import annotations

import sys
import time
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv

from src.config.loader import ConfigLoader
from src.utils.io import write_csv_to_zip
from src.utils.queries import get_query
from src.utils.sql_conn import get_std_connection
from src.utils.output_formatter import OutputFormatter, format_extraction_output

BASE = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(BASE))
load_dotenv(BASE / ".env")


def extract_max_data() -> tuple[Path | None, int]:
    loader = ConfigLoader(base_path=BASE)
    config = loader.load()

    global_cfg = config.get("global", {})
    encoding = global_cfg.get("encoding", "utf-8-sig")
    csv_sep = global_cfg.get("csv_separator", ",")

    output_cfg = config.get("paths", {}).get("output", {})
    output_dir = Path(output_cfg.get("max_tratada", "data/output/max_tratada"))
    if not output_dir.is_absolute():
        output_dir = config.base_path / output_dir
    output_dir.parent.mkdir(parents=True, exist_ok=True)

    input_dir = Path(config.get("paths", {}).get("input", {}).get("base_max", "data/input/base_max"))
    if not input_dir.is_absolute():
        input_dir = config.base_path / input_dir
    input_dir.mkdir(parents=True, exist_ok=True)

    for old_file in input_dir.glob("MaxSmart*.zip"):
        try:
            old_file.unlink()
        except Exception:
            pass

    conn = get_std_connection(config.base_path)
    if not conn.connect():
        print("\n[ERRO] Falha na conexao SQL")
        print("[INFO] Verifique VPN ou credenciais do banco de dados\n")
        return None, 0

    try:
        query = get_query(config, "max")
        df = conn.execute_query(query)
        if df is None or df.empty:
            print("[ERRO] Consulta nao retornou registros.")
            return None, 0

        registros = len(df)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        csv_name = f"MaxSmart_{timestamp}.csv"
        zip_path = input_dir / "MaxSmart.zip"
        write_csv_to_zip({csv_name: df}, zip_path, sep=csv_sep, encoding=encoding)

        return zip_path, registros
    finally:
        conn.close()


def main() -> None:
    inicio = time.time()
    zip_path, registros = extract_max_data()
    duracao = time.time() - inicio

    if zip_path:
        format_extraction_output(
            source="MAX (SQL Server STD2016)",
            output_file=str(zip_path),
            records=registros,
            duration=duracao,
            steps=[
                "Conexão com banco SQL Server",
                "Execução de query MAX",
                f"Processamento de {OutputFormatter.format_count(registros)} registros",
                f"Salvamento em {zip_path.name}"
            ]
        )
    else:
        print("[ERRO] Falha na extracao dos dados.")
        print(f"[INFO] Tempo de execucao: {duracao:.2f} segundos")
        sys.exit(1)


if __name__ == "__main__":
    main()
