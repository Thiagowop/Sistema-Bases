from pathlib import Path

import pandas as pd

from src.processors.devolucao import DevolucaoProcessor


def _base_config():
    return {
        "global": {"add_timestamp": False, "empresa": {"cnpj": "123"}},
        "paths": {"input": {}, "output": {"base": "tests/output"}},
        "vic_processor": {
            "status_em_aberto": "EM ABERTO",
            "filtros_inclusao": {
                "status_em_aberto": True,
                "tipos_validos": False,
                "aging": False,
                "blacklist": False,
            },
        },
        "devolucao": {
            "campanha_termo": "Vic Extra",
            "status_excluir": ["LIQUIDADO COM ACORDO"],
            "chaves": {"vic": "CHAVE", "max": "PARCELA"},
            "filtros_max": {
                "status_em_aberto": True,
                "tipos_validos": False,
                "blacklist": False,
            },
            "export": {
                "filename_prefix": "vic_devolucao",
                "subdir": "devolucao",
                "judicial_subdir": "devolucao/jud",
                "extrajudicial_subdir": "devolucao/extra",
                "geral_subdir": "devolucao",
                "add_timestamp": False,
            },
            "status_devolucao_fixo": "98",
        },
    }


def test_identificar_devolucao_aplica_filtros_e_diferenca():
    proc = DevolucaoProcessor(config=_base_config())

    df_vic = pd.DataFrame({"CHAVE": ["1-1", "1-2"]})

    df_max = pd.DataFrame(
        {
            "PARCELA": ["1-1", "1-2", "1-3", "1-4", "1-5"],
            "STATUS_TITULO": [
                "EM ABERTO",
                "EM ABERTO",
                "EM ABERTO",
                "LIQUIDADO COM ACORDO",
                "EM ABERTO",
            ],
            "CAMPANHA": [
                "000001 - Vic Extra",
                "000001 - Vic Extra",
                "000001 - Vic Extra",
                "000001 - Vic Extra",
                "Outra",
            ],
        }
    )

    df_max_filtrado, max_metrics = proc._aplicar_filtros_max(df_max)

    df_out = proc.identificar_devolucao(
        df_vic, df_max_filtrado, counts_iniciais=max_metrics
    )
    metrics = proc.metrics_ultima_execucao

    assert metrics["max_antes_filtros"] == 5
    assert metrics["max_apos_status_aberto"] == 4
    assert metrics["max_apos_campanha"] == 3
    assert metrics["max_apos_status_excluir"] == 3
    assert metrics["registros_devolucao"] == 1
    assert df_out["PARCELA"].tolist() == ["1-3"]


def test_formatar_devolucao_layout_minimo():
    proc = DevolucaoProcessor(config=_base_config())

    df = pd.DataFrame(
        {
            "PARCELA": ["1-3"],
            "CPFCNPJ_CLIENTE": ["999"],
            "NOME": ["CLIENTE"],
            "VENCIMENTO": ["2024-01-01"],
            "VALOR": ["100"],
            "TIPO_PARCELA": ["PROSOLUTO"],
            "DATA_BASE": ["27/10/2025"],
        }
    )

    out = proc.formatar_devolucao(df)

    expected_cols = [
        "CNPJ CREDOR",
        "CPFCNPJ CLIENTE",
        "NOME / RAZAO SOCIAL",
        "PARCELA",
        "VENCIMENTO",
        "VALOR",
        "TIPO PARCELA",
        "DATA DEVOLUCAO",
        "STATUS",
    ]

    assert list(out.columns) == expected_cols
    assert out.loc[0, "PARCELA"] == "1-3"
    assert out.loc[0, "CNPJ CREDOR"] == "123"
    assert out.loc[0, "STATUS"] == "98"
    assert out.loc[0, "DATA DEVOLUCAO"] == "27/10/2025"


def test_processar_remove_registros_presentes_na_baixa(tmp_path):
    cfg = _base_config()
    cfg["paths"]["output"]["base"] = str(tmp_path)
    proc = DevolucaoProcessor(config=cfg)

    df_vic = pd.DataFrame({"CHAVE": ["1-1"], "STATUS_TITULO": ["EM ABERTO"]})
    df_max = pd.DataFrame(
        {
            "PARCELA": ["1-1", "1-2"],
            "STATUS_TITULO": ["EM ABERTO", "EM ABERTO"],
            "CAMPANHA": ["000001 - Vic Extra", "000001 - Vic Extra"],
        }
    )

    vic_path = tmp_path / "vic.csv"
    max_path = tmp_path / "max.csv"
    df_vic.to_csv(vic_path, index=False, sep=";")
    df_max.to_csv(max_path, index=False, sep=";")

    baixa_path = tmp_path / "baixa.csv"
    pd.DataFrame({"PARCELA": ["1-2"]}).to_csv(baixa_path, index=False, sep=";")

    stats = proc.processar(vic_path, max_path, {"arquivo_geral": baixa_path})

    assert stats["registros_devolucao"] == 0
    assert stats["removidos_por_baixa"] == 1
    assert stats.get("arquivo_extrajudicial") is None
