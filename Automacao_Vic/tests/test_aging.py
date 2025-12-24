import pandas as pd

from datetime import datetime, timedelta

from src.utils.aging import filtrar_clientes_criticos


def test_filtrar_clientes_criticos():
    hoje = datetime(2024, 1, 31)
    df = pd.DataFrame(
        {
            "CPFCNPJ_CLIENTE": ["123", "123", "456", "456"],
            "VENCIMENTO": [
                hoje - timedelta(days=95),  # cliente 123 atrasado > 90
                hoje - timedelta(days=10),
                hoje - timedelta(days=85),
                hoje - timedelta(days=5),
            ],
        }
    )

    filtrado, removidos = filtrar_clientes_criticos(
        df,
        col_cliente="CPFCNPJ_CLIENTE",
        col_vencimento="VENCIMENTO",
        limite=90,
        data_referencia=hoje,
    )

    assert removidos == {"456"}
    assert set(filtrado["CPFCNPJ_CLIENTE"]) == {"123"}
    assert set(filtrado.columns) == set(df.columns)
    assert len(filtrado) == 2
