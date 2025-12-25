"""Testes para funções críticas de aging."""

import pytest
import pandas as pd
from datetime import datetime, timedelta
from src.utils.aging import filtrar_clientes_criticos


class TestFiltrarClientesCriticos:
    """Testes para a função crítica filtrar_clientes_criticos."""
    
    def test_filtrar_clientes_criticos_basico(self):
        """Testa filtro básico de clientes críticos por aging."""
        hoje = datetime.now()
        data_antiga = hoje - timedelta(days=100)
        data_recente = hoje - timedelta(days=30)
        
        df = pd.DataFrame({
            "CPFCNPJ_CLIENTE": ["12345678901", "98765432109", "11111111111"],
            "VENCIMENTO": [data_antiga, data_recente, data_antiga],
            "VALOR": [1000, 500, 2000]
        })
        
        df_filtrado, removidos = filtrar_clientes_criticos(
            df, 
            col_cliente="CPFCNPJ_CLIENTE",
            col_vencimento="VENCIMENTO",
            limite=90
        )
        
        # Deve manter apenas registros com aging > 90 dias
        assert len(df_filtrado) == 2
        assert "98765432109" not in df_filtrado["CPFCNPJ_CLIENTE"].values
        assert len(removidos) == 1
    
    def test_filtrar_clientes_criticos_sem_dados(self):
        """Testa comportamento com DataFrame vazio."""
        df = pd.DataFrame(columns=["CPFCNPJ_CLIENTE", "VENCIMENTO", "VALOR"])
        
        df_filtrado, removidos = filtrar_clientes_criticos(
            df,
            col_cliente="CPFCNPJ_CLIENTE", 
            col_vencimento="VENCIMENTO",
            limite=90
        )
        
        assert len(df_filtrado) == 0
        assert len(removidos) == 0
    
    def test_filtrar_clientes_criticos_datas_invalidas(self):
        """Testa comportamento com datas inválidas."""
        df = pd.DataFrame({
            "CPFCNPJ_CLIENTE": ["12345678901", "98765432109"],
            "VENCIMENTO": [None, "data_invalida"],
            "VALOR": [1000, 500]
        })
        
        df_filtrado, removidos = filtrar_clientes_criticos(
            df,
            col_cliente="CPFCNPJ_CLIENTE",
            col_vencimento="VENCIMENTO", 
            limite=90
        )
        
        # Registros com datas inválidas devem ser removidos
        assert len(df_filtrado) == 0
        assert len(removidos) == 2
    
    def test_filtrar_clientes_criticos_limite_zero(self):
        """Testa comportamento com limite zero."""
        hoje = datetime.now()
        ontem = hoje - timedelta(days=1)
        
        df = pd.DataFrame({
            "CPFCNPJ_CLIENTE": ["12345678901"],
            "VENCIMENTO": [ontem],
            "VALOR": [1000]
        })
        
        df_filtrado, removidos = filtrar_clientes_criticos(
            df,
            col_cliente="CPFCNPJ_CLIENTE",
            col_vencimento="VENCIMENTO",
            limite=0
        )
        
        # Com limite 0, qualquer data vencida deve ser incluída
        assert len(df_filtrado) == 1
        assert len(removidos) == 0
    
    def test_filtrar_clientes_criticos_colunas_inexistentes(self):
        """Testa comportamento com colunas inexistentes."""
        df = pd.DataFrame({
            "CLIENTE": ["12345678901"],
            "DATA": [datetime.now()],
            "VALOR": [1000]
        })
        
        with pytest.raises((KeyError, ValueError)):
            filtrar_clientes_criticos(
                df,
                col_cliente="CPFCNPJ_CLIENTE",  # Coluna inexistente
                col_vencimento="VENCIMENTO",     # Coluna inexistente
                limite=90
            )
    
    def test_filtrar_clientes_criticos_preserva_colunas(self):
        """Testa se todas as colunas originais são preservadas."""
        hoje = datetime.now()
        data_antiga = hoje - timedelta(days=100)
        
        df = pd.DataFrame({
            "CPFCNPJ_CLIENTE": ["12345678901"],
            "VENCIMENTO": [data_antiga],
            "VALOR": [1000],
            "DESCRICAO": ["Teste"],
            "STATUS": ["ATIVO"]
        })
        
        df_filtrado, _ = filtrar_clientes_criticos(
            df,
            col_cliente="CPFCNPJ_CLIENTE",
            col_vencimento="VENCIMENTO",
            limite=90
        )
        
        # Todas as colunas originais devem estar presentes
        assert list(df_filtrado.columns) == list(df.columns)
        assert df_filtrado["DESCRICAO"].iloc[0] == "Teste"
        assert df_filtrado["STATUS"].iloc[0] == "ATIVO"
    
    def test_filtrar_clientes_criticos_multiplos_registros_mesmo_cliente(self):
        """Testa comportamento com múltiplos registros do mesmo cliente."""
        hoje = datetime.now()
        data_antiga = hoje - timedelta(days=100)
        data_recente = hoje - timedelta(days=30)
        
        df = pd.DataFrame({
            "CPFCNPJ_CLIENTE": ["12345678901", "12345678901", "98765432109"],
            "VENCIMENTO": [data_antiga, data_recente, data_antiga],
            "VALOR": [1000, 500, 2000],
            "PARCELA": [1, 2, 1]
        })
        
        df_filtrado, removidos = filtrar_clientes_criticos(
            df,
            col_cliente="CPFCNPJ_CLIENTE",
            col_vencimento="VENCIMENTO",
            limite=90
        )
        
        # Deve manter registros do cliente que tem pelo menos uma parcela crítica
        clientes_filtrados = df_filtrado["CPFCNPJ_CLIENTE"].unique()
        assert "12345678901" in clientes_filtrados
        assert "98765432109" in clientes_filtrados