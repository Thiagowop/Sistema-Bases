"""Testes para o processador de enriquecimento da base VIC."""

from __future__ import annotations

import zipfile
from pathlib import Path

import pandas as pd

from src.processors import EnriquecimentoVicProcessor

from .tratamento_vic_teste import _build_config


def _write_zip_csv(path: Path, name: str, df: pd.DataFrame) -> None:
    content = df.to_csv(sep=";", encoding="utf-8-sig", index=False)
    with zipfile.ZipFile(path, "w") as zf:
        zf.writestr(name, content)


def test_enriquecimento_vic_processa_contatos(tmp_path: Path) -> None:
    """Gera o layout de enriquecimento a partir de bases sintéticas."""

    config = _build_config(tmp_path)
    config.setdefault("enriquecimento_vic", {}).setdefault("export", {})[
        "add_timestamp"
    ] = False

    processor = EnriquecimentoVicProcessor(config=config)

    # Base VIC tratada com colunas auxiliares
    df_vic = pd.DataFrame(
        [
            {
                "CPFCNPJ_CLIENTE": "111.222.333-44",
                "CPFCNPJ_LIMPO": "11122233344",
                "NOME_RAZAO_SOCIAL": "Cliente 1",
                "TEL RESIDENCIAL": "(31) 98888-1111",
                "TEL_RESIDENCIAL_LIMPO": "31988881111",
                "TEL COMERCIAL": "",
                "TEL_COMERCIAL_LIMPO": "",
                "TEL CELULAR": "(31) 98765-0000",
                "TEL_CELULAR_LIMPO": "31987650000",
                "TELEFONE_LIMPO": "31988881111",
                "EMAIL": "cliente1@example.com",
                "DATA_BASE": "01/01/2024",
            },
            {
                "CPFCNPJ_CLIENTE": "555.666.777-88",
                "CPFCNPJ_LIMPO": "55566677788",
                "NOME_RAZAO_SOCIAL": "Cliente 2",
                "TEL RESIDENCIAL": "(21) 4002-8922",
                "TEL_RESIDENCIAL_LIMPO": "2140028922",
                "TEL COMERCIAL": "",
                "TEL_COMERCIAL_LIMPO": "",
                "TEL CELULAR": "",
                "TEL_CELULAR_LIMPO": "",
                "TELEFONE_LIMPO": "2140028922",
                "EMAIL": "",
                "DATA_BASE": "05/01/2024",
            },
        ]
    )

    vic_zip = tmp_path / "vic_tratada" / "vic_base_limpa.zip"
    vic_zip.parent.mkdir(parents=True, exist_ok=True)
    _write_zip_csv(vic_zip, "vic_base_limpa.csv", df_vic)

    # Arquivo de batimento com um registro judicial e outro extrajudicial
    df_judicial = pd.DataFrame(
        [
            {
                "CPFCNPJ CLIENTE": "111.222.333-44",
                "NOME / RAZAO SOCIAL": "Cliente 1",
                "NUMERO CONTRATO": "C1",
                "PARCELA": "C1-001",
                "OBSERVACAO PARCELA": "001",
                "VENCIMENTO": "2024-02-01",
                "VALOR": "100.00",
                "EMPREENDIMENTO": "Emp",
                "TIPO PARCELA": "PROSOLUTO",
                "CNPJ CREDOR": "12.345.678/0001-00",
            }
        ]
    )

    df_extrajudicial = pd.DataFrame(
        [
            {
                "CPFCNPJ CLIENTE": "555.666.777-88",
                "NOME / RAZAO SOCIAL": "Cliente 2",
                "NUMERO CONTRATO": "C2",
                "PARCELA": "C2-001",
                "OBSERVACAO PARCELA": "001",
                "VENCIMENTO": "2024-03-01",
                "VALOR": "150.00",
                "EMPREENDIMENTO": "Emp",
                "TIPO PARCELA": "PROSOLUTO",
                "CNPJ CREDOR": "12.345.678/0001-00",
            }
        ]
    )

    bat_zip = tmp_path / "batimento" / "vic_batimento.zip"
    bat_zip.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(bat_zip, "w") as zf:
        zf.writestr(
            "vic_batimento_judicial.csv",
            df_judicial.to_csv(sep=";", encoding="utf-8-sig", index=False),
        )
        zf.writestr(
            "vic_batimento_extrajudicial.csv",
            df_extrajudicial.to_csv(sep=";", encoding="utf-8-sig", index=False),
        )

    stats = processor.processar(vic_zip, bat_zip)

    assert stats["registros_batimento"] == 2
    assert stats["contatos_telefone"] == 3  # dois telefones do primeiro + um do segundo
    assert stats["contatos_email"] == 1
    assert stats["data_base_utilizada"] == "01/01/2024"

    arquivo_saida = Path(stats["arquivo_gerado"])
    assert arquivo_saida.exists()

    with zipfile.ZipFile(arquivo_saida) as zf:
        nomes = zf.namelist()
        assert nomes == ["enriquecimento_vic.csv"]
        with zf.open(nomes[0]) as fh:
            df_saida = pd.read_csv(fh, sep=";", encoding="utf-8-sig", dtype=str)

    assert set(df_saida.columns) == set(EnriquecimentoVicProcessor.OUTPUT_COLUMNS)
    assert len(df_saida) == stats["registros_enriquecimento"] == 4

    telefones = df_saida["TELEFONE"].dropna()
    telefones = {valor for valor in telefones if valor}
    assert telefones == {"31988881111", "31987650000", "2140028922"}
    emails = [email for email in df_saida["EMAIL"].dropna() if email]
    assert emails == ["cliente1@example.com"]
    assert df_saida["OBSERVACAO"].nunique() == 1
    assert df_saida["OBSERVACAO"].iloc[0] == "Base Vic - 01/01/2024"
    principais = (
        df_saida[df_saida["TELEFONE"].fillna("") != ""]["TELEFONE PRINCIPAL"].fillna("").unique()
    )
    assert list(principais) == ["1"]
