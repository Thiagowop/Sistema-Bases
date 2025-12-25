#!/usr/bin/env python3
"""Extração da base de acordos em aberto para doublecheck."""

from __future__ import annotations

import sys
import time
from pathlib import Path

from dotenv import load_dotenv

from src.config.loader import ConfigLoader
from src.utils.queries import get_query
from src.utils.sql_conn import get_std_connection
from src.utils.output_formatter import OutputFormatter, format_extraction_output
from src.utils.io import write_csv_to_zip

BASE = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(BASE))
load_dotenv(BASE / ".env")


def extract_doublecheck_acordo() -> tuple[Path | None, int]:
    loader = ConfigLoader(base_path=BASE)
    config = loader.load()

    conn = get_std_connection(loader.base_path)
    if not conn.connect():
        print("[ERRO] Falha ao conectar ao SQL Server para acordos.")
        return None, 0

    try:
        query = get_query(config, "doublecheck_acordo")
        df = conn.execute_query(query)
        if df is None:
            print("[ERRO] Consulta retornou resultado vazio.")
            return None, 0

        # Diretório destino e nome padrão do ZIP
        path_str = config.data.get("inputs", {}).get(
            "acordos_abertos_path", "data/input/doublecheck_acordo/acordos_abertos.zip"
        )
        zip_path = Path(path_str)
        if not zip_path.is_absolute():
            zip_path = loader.base_path / zip_path
        zip_path.parent.mkdir(parents=True, exist_ok=True)
        if zip_path.exists():
            zip_path.unlink()

        global_cfg = config.get("global", {})
        sep = global_cfg.get("csv_separator", ";")
        encoding = global_cfg.get("encoding", "utf-8-sig")

        timestamp = time.strftime('%Y%m%d_%H%M%S')
        csv_name = f'acordos_abertos_{timestamp}.csv'
        write_csv_to_zip({csv_name: df}, zip_path, sep=sep, encoding=encoding)
        return zip_path, len(df)
    finally:
        conn.close()


def main() -> None:
    inicio = time.time()
    caminho, registros = extract_doublecheck_acordo()
    duracao = time.time() - inicio

    if caminho:
        format_extraction_output(
            source="ACORDOS ABERTOS (Doublecheck)",
            output_file=str(caminho),
            records=registros,
            duration=duracao,
            steps=[
                "Conexão com SQL Server",
                "Query de acordos em aberto",
                f"Exportação de {OutputFormatter.format_count(registros)} registros",
                f"Salvamento em {caminho.name}"
            ]
        )
    else:
        print("[ERRO] A extração de acordos não foi concluída.")
        print(f"[INFO] Tempo de execução: {duracao:.2f} segundos")
        sys.exit(1)


if __name__ == "__main__":
    main()
