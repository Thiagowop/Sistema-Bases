import zipfile
import pandas as pd
import pytest
import zipfile

from src.config.loader import ConfigLoader
from src.processors.batimento import BatimentoProcessor


def build_processor() -> BatimentoProcessor:
    config = ConfigLoader().get_config()
    return BatimentoProcessor(config=config)


def test_cruzamento_e_separacao_judicial(tmp_path):
    proc = build_processor()
    proc.judicial_cpfs = {"111"}

    df_vic = pd.DataFrame(
        {
            "CHAVE": ["A", "B"],
            "CPFCNPJ_CLIENTE": ["111", "222"],
            "VENCIMENTO": ["2024-01-01", "2024-01-02"],
            "VALOR": [10.0, 20.0],
        }
    )
    df_max = pd.DataFrame({"PARCELA": ["B"]})

    df_cross = proc.realizar_cruzamento(df_vic, df_max)
    assert df_cross["CHAVE"].tolist() == ["A"]

    df_fmt = proc.formatar_batimento(df_cross)
    zip_path, n_jud, n_ext = proc.gerar_arquivos_batimento(df_fmt, tmp_path, "123")
    assert n_jud == 1 and n_ext == 0
    with zipfile.ZipFile(zip_path) as zf:
        assert sorted(zf.namelist()) == ["vic_batimento_judicial_123.csv"]


def test_formatar_batimento_parcela_e_observacao():
    proc = build_processor()
    df = pd.DataFrame(
        {
            "CHAVE": ["UNICA-001"],
            "PARCELA": ["001"],
            "CPFCNPJ_CLIENTE": ["123"],
            "VENCIMENTO": ["2024-01-01"],
            "VALOR": [100.0],
        }
    )

    df_formatado = proc.formatar_batimento(df)

    assert df_formatado.loc[0, "PARCELA"] == "UNICA-001"
    assert df_formatado.loc[0, "OBSERVACAO PARCELA"] == "001"


def test_formatar_batimento_observacao_fallback_quando_sem_parcela():
    proc = build_processor()
    df = pd.DataFrame(
        {
            "CHAVE": ["UNICA-001"],
            "OBSERVACAO PARCELA": ["texto"],
            "CPFCNPJ_CLIENTE": ["123"],
            "VENCIMENTO": ["2024-01-01"],
            "VALOR": [100.0],
        }
    )

    df_formatado = proc.formatar_batimento(df)

    assert df_formatado.loc[0, "OBSERVACAO PARCELA"] == "texto"


def test_formatar_batimento_sem_chave_lanca_erro():
    proc = build_processor()
    df = pd.DataFrame(
        {
            "PARCELA": ["001"],
            "CPFCNPJ_CLIENTE": ["123"],
            "VENCIMENTO": ["2024-01-01"],
            "VALOR": [100.0],
        }
    )

    with pytest.raises(ValueError):
        proc.formatar_batimento(df)


def test_chave_duplicada_failfast():
    proc = build_processor()
    df_vic = pd.DataFrame(
        {
            "CHAVE": ["A", "A"],
            "CPFCNPJ_CLIENTE": ["1", "2"],
            "VENCIMENTO": ["2024-01-01", "2024-02-01"],
            "VALOR": [1.0, 2.0],
        }
    )
    df_max = pd.DataFrame({"PARCELA": []}, dtype=str)

    with pytest.raises(ValueError):
        proc.realizar_cruzamento(df_vic, df_max)
