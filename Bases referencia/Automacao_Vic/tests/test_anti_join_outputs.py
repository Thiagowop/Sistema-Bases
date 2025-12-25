import io
from pathlib import Path

import pandas as pd
import pytest


def _latest_file(p: Path, pattern: str):
    files = sorted(p.glob(pattern), key=lambda x: x.stat().st_mtime, reverse=True)
    return files[0] if files else None


@pytest.mark.skipif(not Path('data/output').exists(), reason="output dir missing")
def test_batimento_chaves_nao_existem_em_max():
    base_out = Path('data/output')
    max_dir = base_out / 'max_tratada'
    vic_dir = base_out / 'vic_tratada'
    bat_dir = base_out / 'batimento'

    max_zip = _latest_file(max_dir, 'max_tratada_*.zip')
    vic_zip = _latest_file(vic_dir, 'vic_tratada_*.zip')
    bat_zip = _latest_file(bat_dir, 'vic_batimento_*.zip')

    if not (max_zip and vic_zip and bat_zip):
        pytest.skip("Arquivos tratados/saida de batimento nao encontrados")

    import zipfile

    with zipfile.ZipFile(max_zip, 'r') as z:
        name = [n for n in z.namelist() if n.endswith('.csv')][0]
        df_max = pd.read_csv(io.TextIOWrapper(z.open(name), encoding='utf-8-sig'), sep=';')

    with zipfile.ZipFile(bat_zip, 'r') as z:
        dfs = []
        for n in z.namelist():
            if n.endswith('.csv'):
                dfs.append(pd.read_csv(io.TextIOWrapper(z.open(n), encoding='utf-8-sig'), sep=';'))
    df_bat = pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

    assert not df_bat.empty, "Batimento vazio—nada a verificar"

    chaves_vic_bat = df_bat['PARCELA'].astype(str).str.strip()
    parcelas_max = df_max['PARCELA'].astype(str).str.strip()

    intersec = set(chaves_vic_bat) & set(parcelas_max)
    assert len(intersec) == 0, f"Foram encontradas {len(intersec)} chaves do batimento presentes no MAX"


@pytest.mark.skipif(not Path('data/output').exists(), reason="output dir missing")
def test_devolucao_parcelas_nao_existem_em_vic():
    base_out = Path('data/output')
    max_dir = base_out / 'max_tratada'
    vic_dir = base_out / 'vic_tratada'
    dev_dir = base_out / 'devolucao'

    max_zip = _latest_file(max_dir, 'max_tratada_*.zip')
    vic_zip = _latest_file(vic_dir, 'vic_tratada_*.zip')
    dev_zip = _latest_file(dev_dir, 'vic_devolucao_*.zip')

    if not (max_zip and vic_zip and dev_zip):
        pytest.skip("Arquivos tratados/saida de devolucao nao encontrados")

    import zipfile

    with zipfile.ZipFile(vic_zip, 'r') as z:
        name = [n for n in z.namelist() if n.endswith('.csv')][0]
        df_vic = pd.read_csv(io.TextIOWrapper(z.open(name), encoding='utf-8-sig'), sep=';')

    with zipfile.ZipFile(dev_zip, 'r') as z:
        name = [n for n in z.namelist() if n.endswith('.csv')][0]
        df_dev = pd.read_csv(io.TextIOWrapper(z.open(name), encoding='utf-8-sig'), sep=';')

    assert not df_dev.empty, "Devolucao vazia—nada a verificar"

    parcelas_dev = df_dev['PARCELA'].astype(str).str.strip()
    chaves_vic = df_vic['CHAVE'].astype(str).str.strip()

    intersec = set(parcelas_dev) & set(chaves_vic)
    assert len(intersec) == 0, f"Foram encontradas {len(intersec)} parcelas da devolucao presentes na VIC"

