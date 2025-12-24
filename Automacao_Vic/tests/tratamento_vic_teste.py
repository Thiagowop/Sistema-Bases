"""Teste de tratamento VIC gerando apenas a base canônica limpa.

Este teste executa o ``VicProcessor`` com os dados reais utilizados no
projeto e valida que a etapa de tratamento gera um único artefato
(``vic_base_limpa.csv``) com as colunas auxiliares necessárias. Os
filtros de negócio (status, aging, blacklist) são delegados para as
etapas subsequentes.
"""

from __future__ import annotations

from copy import deepcopy
from pathlib import Path
import zipfile

import pandas as pd

from src.config.loader import ConfigLoader
from src.processors import VicProcessor

DATA_DIR = Path(__file__).resolve().parents[1] / "data"


def _build_config(tmp_path: Path) -> dict:
    """Carrega a configuração padrão ajustando apenas a saída."""

    loader = ConfigLoader()
    config = deepcopy(loader.get_config())

    # Redireciona a saída para o diretório temporário do teste
    config.setdefault("paths", {}).setdefault("output", {})["base"] = str(tmp_path)

    # Desativa timestamp para facilitar asserções determinísticas
    global_cfg = config.setdefault("global", {})
    global_cfg["add_timestamp_to_files"] = False
    global_cfg["add_timestamp"] = False

    # Para fins de teste, desativa aging nas etapas de inclusão/batimento
    config.setdefault("vic_processor", {}).setdefault("filtros_inclusao", {})[
        "aging"
    ] = False

    return config


def test_tratamento_vic_base_limpa(tmp_path):
    """Executa o tratamento VIC completo e valida a base limpa."""

    config = _build_config(tmp_path)
    processor = VicProcessor(config=config)

    entrada_vic = DATA_DIR / "input" / "vic" / "VicCandiotto.zip"
    data_base_ref = "27/10/2025"
    stats = processor.processar(entrada=entrada_vic, data_base=data_base_ref)

    # Arquivo exportado
    arquivo_base = Path(stats["arquivo_base_limpa"])
    assert arquivo_base.exists()
    assert arquivo_base.parent == tmp_path / "vic_tratada"

    # Base limpa com colunas auxiliares
    with zipfile.ZipFile(arquivo_base) as zf:
        csv_files = [name for name in zf.namelist() if name.endswith(".csv")]
        assert csv_files == ["vic_base_limpa.csv"]
        with zf.open(csv_files[0]) as fh:
            df_base = pd.read_csv(fh, sep=";", encoding="utf-8-sig")

    assert len(df_base) == stats["registros_base_limpa"] == stats["registros_finais"]
    assert "CPFCNPJ_LIMPO" in df_base.columns
    assert "TELEFONE_LIMPO" in df_base.columns
    assert "ID_NEGOCIADOR" in df_base.columns
    assert "DATA_BASE" in df_base.columns
    assert stats["data_base_utilizada"] == data_base_ref
    assert df_base["DATA_BASE"].nunique() == 1
    assert df_base["DATA_BASE"].iloc[0] == data_base_ref

    # Estatísticas básicas
    assert stats["arquivo_gerado"] == stats["arquivo_base_limpa"]
    assert stats["registros_validos"] >= stats["registros_base_limpa"]
    assert stats["registros_finais"] > 0
