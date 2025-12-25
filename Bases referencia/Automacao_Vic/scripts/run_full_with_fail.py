#!/usr/bin/env python3
"""Executa o pipeline completo (MAX -> VIC -> Devolucao -> Batimento)
e retorna erro (codigo 2) se alguma etapa tiver registros = 0.

Uso: venv\Scripts\python.exe scripts/run_full_with_fail.py
"""

import sys
from pathlib import Path
import shutil

project_root = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(project_root))

try:
    sys.stdout.reconfigure(encoding='utf-8')
except Exception:
    pass
sys.path.insert(0, str(project_root / 'src'))

from src.config.loader import ConfigLoader
from src.processors import MaxProcessor, VicProcessor, DevolucaoProcessor, BatimentoProcessor
from src.utils.logger import get_logger



def _limpar_outputs(cfg) -> None:
    output_cfg = cfg.get('paths', {}).get('output', {})
    base_dir = Path(output_cfg.get('base', 'data/output'))
    subdirs = ['vic_tratada', 'max_tratada', 'devolucao', 'batimento', 'inconsistencias']
    for sub in subdirs:
        dir_path = base_dir / sub
        if not dir_path.exists():
            continue
        for item in dir_path.iterdir():
            try:
                if item.is_dir():
                    shutil.rmtree(item)
                else:
                    if item.name == '.gitkeep':
                        continue
                    item.unlink()
            except Exception:
                continue


def _localizar_entrada(cfg, tipo: str) -> Path | None:
    input_cfg = cfg.get('paths', {}).get('input', {})
    dir_path = Path(input_cfg.get(tipo, ''))
    if not dir_path.exists():
        return None
    candidatos = sorted(list(dir_path.glob('*.zip')) + list(dir_path.glob('*.csv')), key=lambda p: p.stat().st_mtime)
    return candidatos[-1] if candidatos else None
def main() -> int:
    cfg = ConfigLoader().get_config()
    logger = get_logger('run_full_with_fail', cfg)

    _limpar_outputs(cfg)

    entrada_max = _localizar_entrada(cfg, 'max')
    entrada_vic = _localizar_entrada(cfg, 'vic')

    max_p = MaxProcessor(cfg, logger)
    vic_p = VicProcessor(cfg, logger)
    dev_p = DevolucaoProcessor(cfg, logger)
    bat_p = BatimentoProcessor(cfg, logger)

    print("[1/4] MAX: processando (ou extraindo do DB)...")
    r_max = max_p.processar(entrada=entrada_max)
    max_path = r_max.get('arquivo_gerado') or r_max.get('arquivo_saida')
    if not max_path:
        print("ERRO: Nao foi possivel obter arquivo MAX tratado")
        return 2

    print("[2/4] VIC: processando (ou extraindo do DB)...")
    r_vic = vic_p.processar(entrada=entrada_vic)
    vic_path = r_vic.get('arquivo_gerado') or r_vic.get('arquivo_saida')
    if not vic_path:
        print("ERRO: Nao foi possivel obter arquivo VIC tratado")
        return 2

    print("[3/4] Devolucao: processando...")
    r_dev = dev_p.processar(vic_path=Path(vic_path), max_path=Path(max_path))

    print("[4/4] Batimento: processando...")
    r_bat = bat_p.processar(vic_path=Path(vic_path), max_path=Path(max_path))

    # Resumo simples
    print("\n=== Resumo ===")
    print(f"MAX tratados: {r_max.get('registros_finais', 'NA')}")
    print(f"VIC tratados: {r_vic.get('registros_finais', 'NA')}")
    print(f"Devolucoes identificadas: {r_dev.get('registros_devolucao', 'NA')}")
    print(f"Batimento (VIC - MAX): {r_bat.get('registros_batimento', 'NA')}")

    erros = []
    if r_max.get('registros_finais', 1) == 0:
        erros.append('MAX com 0 registros tratados')
    if r_vic.get('registros_finais', 1) == 0:
        erros.append('VIC com 0 registros tratados')
    if r_dev.get('registros_devolucao', 1) == 0:
        erros.append('Devolucao com 0 registros identificados')
    if r_bat.get('registros_batimento', 1) == 0:
        erros.append('Batimento com 0 registros (VIC - MAX)')

    if erros:
        print("\nERROS:")
        for e in erros:
            print(f"- {e}")
        return 2

    print("\nOK.")
    return 0


if __name__ == '__main__':
    sys.exit(main())
