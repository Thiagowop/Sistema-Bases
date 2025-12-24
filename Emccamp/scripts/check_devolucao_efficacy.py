"""Conferencia rapida de eficacia da devolucao (MAX - EMCCAMP).

Valida que as chaves presentes no arquivo de devolução:
- existem no MAX tratado (subconjunto)
- não existem na EMCCAMP tratada (interseção vazia)

Uso:
  python scripts/check_devolucao_efficacy.py

Opcional (paths específicos):
  python scripts/check_devolucao_efficacy.py --devolucao-zip <zip> --max-zip <zip> --emccamp-zip <zip>
"""

from __future__ import annotations

import argparse
import zipfile
from pathlib import Path

import pandas as pd


def latest_zip(directory: Path, pattern: str) -> Path:
    files = sorted(directory.glob(pattern), key=lambda p: p.stat().st_mtime, reverse=True)
    if not files:
        raise FileNotFoundError(f"Nenhum arquivo {pattern} em {directory}")
    return files[0]


def read_csv_from_zip(
    zip_path: Path,
    *,
    prefer_name_contains: str | None = None,
    usecols: list[str] | None = None,
    sep: str = ';',
    encoding: str = 'utf-8-sig',
) -> pd.DataFrame:
    with zipfile.ZipFile(zip_path) as z:
        names = [n for n in z.namelist() if n.lower().endswith('.csv')]
        if not names:
            raise ValueError(f'ZIP sem CSV: {zip_path}')

        chosen = None
        if prefer_name_contains:
            for name in names:
                if prefer_name_contains in name:
                    chosen = name
                    break

        if chosen is None:
            chosen = sorted(names)[0]

        with z.open(chosen) as f:
            return pd.read_csv(f, sep=sep, dtype=str, encoding=encoding, usecols=usecols)


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog='python scripts/check_devolucao_efficacy.py',
        description='Valida eficacia da devolucao (MAX - EMCCAMP) usando os outputs mais recentes.',
    )
    p.add_argument('--base-dir', default='.', help='Diretorio base do projeto (padrao: .)')
    p.add_argument('--devolucao-zip', default='', help='Path do ZIP de devolucao (padrao: ultimo em data/output/devolucao)')
    p.add_argument('--max-zip', default='', help='Path do ZIP de MAX tratado (padrao: ultimo em data/output/max_tratada)')
    p.add_argument('--emccamp-zip', default='', help='Path do ZIP de EMCCAMP tratada (padrao: ultimo em data/output/emccamp_tratada)')
    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    base = Path(args.base_dir).resolve()

    devolucao_zip = Path(args.devolucao_zip).resolve() if args.devolucao_zip else latest_zip(base / 'data/output/devolucao', 'emccamp_devolucao_*.zip')
    max_zip = Path(args.max_zip).resolve() if args.max_zip else latest_zip(base / 'data/output/max_tratada', 'max_tratada_*.zip')
    emccamp_zip = Path(args.emccamp_zip).resolve() if args.emccamp_zip else latest_zip(base / 'data/output/emccamp_tratada', 'emccamp_tratada_*.zip')

    df_dev = read_csv_from_zip(devolucao_zip, prefer_name_contains='emccamp_devolucao', usecols=['PARCELA'])
    df_max = read_csv_from_zip(max_zip, usecols=['CHAVE'])
    df_emc = read_csv_from_zip(emccamp_zip, usecols=['CHAVE'])

    s_dev = set(df_dev['PARCELA'].astype(str).str.strip())
    s_max = set(df_max['CHAVE'].astype(str).str.strip())
    s_emc = set(df_emc['CHAVE'].astype(str).str.strip())

    missing_in_max = s_dev - s_max
    intersection = s_dev & s_emc

    print('=== CONFERENCIA DEVOLUCAO (MAX - EMCCAMP) ===')
    print('devolucao_zip:', devolucao_zip.name)
    print('max_zip:', max_zip.name)
    print('emccamp_zip:', emccamp_zip.name)
    print('registros_devolucao:', len(df_dev))
    print('chaves_devolucao_unicas:', len(s_dev))
    print('faltando_no_max:', len(missing_in_max))
    print('intersecao_com_emccamp:', len(intersection))

    if missing_in_max:
        print('exemplos_faltando_no_max:', list(sorted(missing_in_max))[:10])
    if intersection:
        print('exemplos_intersecao_emccamp:', list(sorted(intersection))[:10])

    if missing_in_max or intersection:
        return 2

    print('OK: devolucao esta 100% no MAX e 0% no EMCCAMP.')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
