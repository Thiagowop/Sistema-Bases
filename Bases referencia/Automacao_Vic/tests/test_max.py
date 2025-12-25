import pandas as pd

from src.config.loader import ConfigLoader
from src.processors.max import MaxProcessor


def build_processor() -> MaxProcessor:
    config = ConfigLoader().get_config()
    return MaxProcessor(config=config)


def test_padronizacao_e_validacao_chave():
    proc = build_processor()
    df_in = pd.DataFrame({
        'CPFCNPJ_CLIENTE': ['111', '222'],
        'NUMERO_CONTRATO': ['1', '2'],
        'PARCELA': ['ABC-1', 'INV,2'],
        'VENCIMENTO': ['2024-01-01', '2024-02-01'],
        'VALOR': [100.0, 200.0],
    })
    df_pad = proc.padronizar_campos(df_in)
    assert 'CHAVE' not in df_pad.columns

    df_val, df_inv = proc.validar_dados(df_pad)
    assert len(df_val) == 1
    assert 'CHAVE_FORMATO_INVALIDO' in df_inv['motivo_inconsistencia'].iloc[0]

    df_final = df_val.copy()
    df_final['CHAVE'] = df_final['PARCELA']
    assert df_final.loc[0, 'CHAVE'] == 'ABC-1'


def test_chaves_duplicadas_permitidas():
    proc = build_processor()
    df_in = pd.DataFrame({
        'CPFCNPJ_CLIENTE': ['111', '222'],
        'NUMERO_CONTRATO': ['1', '1'],
        'PARCELA': ['ABC-1', 'ABC-1'],
        'VENCIMENTO': ['2024-01-01', '2024-02-01'],
        'VALOR': [100.0, 200.0],
    })
    df_pad = proc.padronizar_campos(df_in)
    df_val, df_inv = proc.validar_dados(df_pad)
    df_val['CHAVE'] = df_val['PARCELA']
    assert len(df_val) == 2
    assert df_inv.empty
