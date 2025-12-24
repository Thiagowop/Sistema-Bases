#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Verificador das saídas finais do pipeline VIC/MAX."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Dict, Iterable, Tuple

import pandas as pd
import zipfile


def latest_file(directory: Path, pattern: str) -> Path:
    files = sorted(directory.glob(pattern))
    if not files:
        raise FileNotFoundError(f"Nenhum arquivo encontrado em {directory}/{pattern}")
    return files[-1]


def read_csvs_from_zip(zip_path: Path) -> Dict[str, pd.DataFrame]:
    if not zip_path.exists():
        raise FileNotFoundError(f"Arquivo não encontrado: {zip_path}")

    frames: Dict[str, pd.DataFrame] = {}
    with zipfile.ZipFile(zip_path) as zf:
        csv_names = [name for name in zf.namelist() if name.lower().endswith('.csv')]
        if not csv_names:
            raise ValueError(f"Nenhum CSV encontrado dentro de {zip_path}")
        for name in csv_names:
            with zf.open(name) as fh:
                frames[name] = pd.read_csv(fh, sep=';', encoding='utf-8-sig')
    return frames


def concat_frames(values: Iterable[pd.DataFrame]) -> pd.DataFrame:
    frames = list(values)
    if not frames:
        return pd.DataFrame()
    if len(frames) == 1:
        return frames[0]
    return pd.concat(frames, ignore_index=True)


def verificar_batimento(df_batimento: pd.DataFrame, df_max: pd.DataFrame) -> Tuple[bool, int]:
    if df_batimento.empty:
        return False, 0
    parcelas_bat = df_batimento['PARCELA'].astype(str).str.strip()
    parcelas_max = df_max['PARCELA'].astype(str).str.strip()
    intersec = set(parcelas_bat) & set(parcelas_max)
    return len(intersec) == 0, len(intersec)


def verificar_devolucao(df_devolucao: pd.DataFrame, df_vic: pd.DataFrame) -> Tuple[bool, int]:
    if df_devolucao.empty:
        return False, 0
    parcelas_dev = df_devolucao['PARCELA'].astype(str).str.strip()
    chaves_vic = df_vic['CHAVE'].astype(str).str.strip()
    intersec = set(parcelas_dev) & set(chaves_vic)
    return len(intersec) == 0, len(intersec)


def verificar_aging(df_vic: pd.DataFrame, tolerancia: int) -> Tuple[bool, float]:
    if 'AGING' not in df_vic.columns:
        return False, float('inf')
    vencimento = pd.to_datetime(df_vic['VENCIMENTO'], errors='coerce')
    aging_calculado = (pd.Timestamp.now().normalize() - vencimento).dt.days
    aging_exportado = pd.to_numeric(df_vic['AGING'], errors='coerce')
    diff = (aging_exportado - aging_calculado).abs()
    return (diff.max() <= tolerancia), float(diff.max())


def main() -> None:
    parser = argparse.ArgumentParser(description='Verifica as saídas tratadas do pipeline')
    parser.add_argument('--base-dir', type=Path, default=Path('data/output'),
                        help='Diretório base dos outputs (default: data/output)')
    parser.add_argument('--aging-tolerance', type=int, default=1,
                        help='Tolerância máxima (dias) para diferença no aging (default: 1)')
    args = parser.parse_args()

    base_dir: Path = args.base_dir
    if not base_dir.exists():
        sys.exit(f"Diretório de outputs não encontrado: {base_dir}")

    try:
        max_zip = latest_file(base_dir / 'max_tratada', 'max_tratada_*.zip')
        vic_zip = latest_file(base_dir / 'vic_tratada', 'vic_tratada_*.zip')
        dev_zip = latest_file(base_dir / 'devolucao', 'vic_devolucao_*.zip')
        bat_zip = latest_file(base_dir / 'batimento', 'vic_batimento_*.zip')
    except FileNotFoundError as exc:
        sys.exit(f"[ERRO] {exc}")

    dfs_max = read_csvs_from_zip(max_zip)
    dfs_vic = read_csvs_from_zip(vic_zip)
    dfs_dev = read_csvs_from_zip(dev_zip)
    dfs_bat = read_csvs_from_zip(bat_zip)

    df_max = next(iter(dfs_max.values()))
    df_vic = next(iter(dfs_vic.values()))
    df_dev = next(iter(dfs_dev.values()))
    df_bat = concat_frames(dfs_bat.values())

    resultados = []
    ok_bat, qtd_bat = verificar_batimento(df_bat, df_max)
    resultados.append(("Batimento", ok_bat, qtd_bat))
    ok_dev, qtd_dev = verificar_devolucao(df_dev, df_vic)
    resultados.append(("Devolução", ok_dev, qtd_dev))
    ok_aging, diff_aging = verificar_aging(df_vic, args.aging_tolerance)
    resultados.append(("Aging", ok_aging, diff_aging))

    print("Resumo das verificações:\n")
    print(f"- MAX tratado: {len(df_max):,} registros ({max_zip.name})")
    print(f"- VIC tratada: {len(df_vic):,} registros ({vic_zip.name})")
    print(f"- Devolução: {len(df_dev):,} registros ({dev_zip.name})")
    print(f"- Batimento: {len(df_bat):,} registros totais em {len(dfs_bat)} arquivo(s) ({bat_zip.name})\n")

    tudo_ok = True
    for nome, status, valor in resultados:
        if nome == 'Aging':
            info = f"maior diferença absoluta = {valor:.1f} dia(s)"
        else:
            info = f"interseções encontradas = {int(valor)}"
        estado = 'OK' if status else 'FALHA'
        print(f"{nome:10s}: {estado} ({info})")
        tudo_ok &= status

    if tudo_ok:
        print("\n[OK] Todas as verificações passaram dentro dos limites definidos.")
    else:
        print("\n[ERRO] Foram encontradas inconsistências nas verificações.")
        sys.exit(1)


if __name__ == '__main__':
    main()
