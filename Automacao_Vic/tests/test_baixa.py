from __future__ import annotations

import zipfile
from pathlib import Path

import pandas as pd

from src.processors.baixa import BaixaProcessor

from .tratamento_vic_teste import _build_config


def _write_zip(path: Path, name: str, df: pd.DataFrame) -> None:
    content = df.to_csv(sep=";", encoding="utf-8-sig", index=False)
    with zipfile.ZipFile(path, "w") as zf:
        zf.writestr(name, content)


def test_baixa_processor_identifica_divergencias(tmp_path: Path) -> None:
    config = _build_config(tmp_path)
    config.setdefault("baixa_processor", {}).setdefault("export", {})[
        "add_timestamp"
    ] = False

    processor = BaixaProcessor(config=config)

    df_vic = pd.DataFrame(
        [
            {
                "CHAVE": "C1-001",
                "NUMERO_CONTRATO": "C1",
                "PARCELA": "001",
                "STATUS_TITULO": "BAIXADO",
                "TIPO_TITULO": "PROSOLUTO",
                "TIPO_PARCELA": "PROSOLUTO",
                "CREDOR": "Credor A",
                "CNPJ_CREDOR": "12.345.678/0001-90",
                "CPF_CNPJ": "111.222.333-44",
                "CPFCNPJ_CLIENTE": "111.222.333-44",
                "NOME_RAZAO_SOCIAL": "Cliente 1",
                "NOME": "Cliente 1",
                "EMPREENDIMENTO": "Empreendimento X",
                "DATA_CADASTRO": "01/01/2024",
                "VENCIMENTO": "10/02/2024",
                "VALOR_PARCELA": "1000.50",
                "VALOR": "1000.50",
                "DATA_BAIXA": "15/02/2024",
                "VALOR_RECEBIDO": "1000.50",
                "TIPO_FLUXO": "Judicial",
                "DATA_BASE": "17/10/2025",
            },
            {
                "CHAVE": "C2-002",
                "NUMERO_CONTRATO": "C2",
                "PARCELA": "002",
                "STATUS_TITULO": "BAIXADO",
                "TIPO_TITULO": "PROSOLUTO",
                "TIPO_PARCELA": "PROSOLUTO",
                "CREDOR": "Credor A",
                "CNPJ_CREDOR": "12.345.678/0001-90",
                "CPF_CNPJ": "555.666.777-88",
                "CPFCNPJ_CLIENTE": "555.666.777-88",
                "NOME_RAZAO_SOCIAL": "Cliente 2",
                "NOME": "Cliente 2",
                "EMPREENDIMENTO": "Empreendimento X",
                "DATA_CADASTRO": "05/01/2024",
                "VENCIMENTO": "10/03/2024",
                "VALOR_PARCELA": "850.00",
                "VALOR": "850.00",
                "DATA_BAIXA": "12/03/2024",
                "VALOR_RECEBIDO": "850.00",
                "TIPO_FLUXO": "Extrajudicial",
                "DATA_BASE": "17/10/2025",
            },
            {
                "CHAVE": "C3-003",
                "NUMERO_CONTRATO": "C3",
                "PARCELA": "003",
                "STATUS_TITULO": "EM ABERTO",
                "TIPO_TITULO": "PROSOLUTO",
                "TIPO_PARCELA": "PROSOLUTO",
                "CREDOR": "Credor A",
                "CNPJ_CREDOR": "12.345.678/0001-90",
                "CPF_CNPJ": "999.888.777-66",
                "NOME_RAZAO_SOCIAL": "Cliente 3",
                "EMPREENDIMENTO": "Empreendimento X",
                "DATA_CADASTRO": "10/01/2024",
                "VENCIMENTO": "10/04/2024",
                "VALOR_PARCELA": "400.00",
                "VALOR": "400.00",
                "DATA_BAIXA": "",
                "VALOR_RECEBIDO": "",
                "TIPO_FLUXO": "Extrajudicial",
                "DATA_BASE": "17/10/2025",
            },
        ]
    )

    vic_zip = tmp_path / "vic_tratada" / "vic_base_limpa.zip"
    vic_zip.parent.mkdir(parents=True, exist_ok=True)
    _write_zip(vic_zip, "vic_base_limpa.csv", df_vic)

    df_max = pd.DataFrame(
        [
            {
                "PARCELA": "C1-001",
                "STATUS_TITULO": "EM ABERTO",
                "TIPO_TITULO": "PROSOLUTO",
                "CPFCNPJ_CLIENTE": "111.222.333-44",
                "VALOR": "1000.50",
            },
            {
                "PARCELA": "C2-002",
                "STATUS_TITULO": "EM ABERTO",
                "TIPO_TITULO": "PROSOLUTO",
                "CPFCNPJ_CLIENTE": "555.666.777-88",
                "VALOR": "850.00",
            },
            {
                "PARCELA": "C4-004",
                "STATUS_TITULO": "LIQUIDADO",
                "TIPO_TITULO": "PROSOLUTO",
                "CPFCNPJ_CLIENTE": "000.000.000-00",
                "VALOR": "50.00",
            },
        ]
    )

    max_zip = tmp_path / "max_tratada" / "max_tratada.zip"
    max_zip.parent.mkdir(parents=True, exist_ok=True)
    _write_zip(max_zip, "max_tratada.csv", df_max)

    stats = processor.processar(vic_zip, max_zip)

    assert stats["divergencias"] == 2
    assert stats["judicial"] == 1
    assert stats["extrajudicial"] == 1

    arquivo_zip = Path(stats["arquivo_zip"])
    assert arquivo_zip.exists()

    internos = stats.get("arquivos_no_zip", {})
    nome_jud = internos.get("arquivo_judicial")
    nome_ext = internos.get("arquivo_extrajudicial")

    assert nome_jud and nome_ext

    with zipfile.ZipFile(arquivo_zip) as zf:
        with zf.open(nome_jud) as fh:
            df_jud = pd.read_csv(fh, sep=";", encoding="utf-8-sig")
        with zf.open(nome_ext) as fh:
            df_ext = pd.read_csv(fh, sep=";", encoding="utf-8-sig")

    assert set(df_jud.columns) == set(BaixaProcessor.LAYOUT_COLUMNS)
    assert set(df_ext.columns) == set(BaixaProcessor.LAYOUT_COLUMNS)

    assert df_jud.loc[0, "CPF/CNPJ CLIENTE"].endswith("44")
    assert df_ext.loc[0, "CPF/CNPJ CLIENTE"].endswith("88")
    assert stats["campanha_utilizada"] == "Base Vic - 17/10/2025"
