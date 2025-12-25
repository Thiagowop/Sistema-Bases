"""Testes para funções utilitárias críticas do módulo helpers."""

import pytest
import pandas as pd
from datetime import datetime
from src.utils.helpers import (
    primeiro_valor,
    normalizar_data_string,
    extrair_data_referencia,
    normalizar_decimal,
    formatar_valor_string,
    extrair_telefone,
    formatar_datas_serie,
)


class TestPrimeiroValor:
    """Testes para a função primeiro_valor."""
    
    def test_primeiro_valor_com_serie_valida(self):
        """Testa extração do primeiro valor válido de uma série."""
        serie = pd.Series([None, "", "valor1", "valor2"])
        resultado = primeiro_valor(serie)
        assert resultado == "valor1"
    
    def test_primeiro_valor_com_serie_vazia(self):
        """Testa comportamento com série vazia."""
        serie = pd.Series([])
        resultado = primeiro_valor(serie)
        assert resultado is None
    
    def test_primeiro_valor_com_todos_nulos(self):
        """Testa comportamento quando todos os valores são nulos."""
        serie = pd.Series([None, "", "nan", "NaN"])
        resultado = primeiro_valor(serie)
        assert resultado is None
    
    def test_primeiro_valor_com_numeros(self):
        """Testa extração com valores numéricos."""
        serie = pd.Series([None, 0, 123, 456])
        resultado = primeiro_valor(serie)
        assert resultado == 0


class TestNormalizarDataString:
    """Testes para a função normalizar_data_string."""
    
    def test_normalizar_data_formato_brasileiro(self):
        """Testa normalização de data no formato brasileiro."""
        resultado = normalizar_data_string("31/12/2023")
        assert resultado == "31/12/2023"
    
    def test_normalizar_data_formato_americano(self):
        """Testa normalização de data no formato americano."""
        resultado = normalizar_data_string("2023-12-31")
        assert resultado == "31/12/2023"
    
    def test_normalizar_data_invalida(self):
        """Testa comportamento com data inválida."""
        resultado = normalizar_data_string("data_invalida")
        assert resultado is None
    
    def test_normalizar_data_nula(self):
        """Testa comportamento com valor nulo."""
        resultado = normalizar_data_string(None)
        assert resultado is None
    
    def test_normalizar_data_vazia(self):
        """Testa comportamento com string vazia."""
        resultado = normalizar_data_string("")
        assert resultado is None


class TestExtrairDataReferencia:
    """Testes para a função extrair_data_referencia."""
    
    def test_extrair_data_referencia_sucesso(self):
        """Testa extração bem-sucedida de data de referência."""
        df = pd.DataFrame({
            "DATA_BASE": ["31/12/2023", "01/01/2024"],
            "DATA_REFERENCIA": [None, "15/01/2024"],
            "OUTRAS_COLUNAS": ["valor1", "valor2"]
        })
        colunas = ["DATA_BASE", "DATA_REFERENCIA"]
        resultado = extrair_data_referencia(df, colunas)
        assert resultado == "31/12/2023"
    
    def test_extrair_data_referencia_sem_dados_validos(self):
        """Testa comportamento quando não há dados válidos."""
        df = pd.DataFrame({
            "DATA_BASE": [None, ""],
            "DATA_REFERENCIA": [None, None]
        })
        colunas = ["DATA_BASE", "DATA_REFERENCIA"]
        resultado = extrair_data_referencia(df, colunas)
        assert resultado is None
    
    def test_extrair_data_referencia_colunas_inexistentes(self):
        """Testa comportamento com colunas inexistentes."""
        df = pd.DataFrame({"OUTRA_COLUNA": ["valor"]})
        colunas = ["DATA_INEXISTENTE"]
        resultado = extrair_data_referencia(df, colunas)
        assert resultado is None


class TestNormalizarDecimal:
    """Testes para a fun��ǜo normalizar_decimal."""

    def test_normalizar_decimal_formato_brasileiro(self):
        """Converte n��mero com v��rgula decimal."""
        resultado = normalizar_decimal("0,01")
        assert resultado == pytest.approx(0.01)

    def test_normalizar_decimal_com_milhar(self):
        """Converte valor com separador de milhar e v��rgula decimal."""
        resultado = normalizar_decimal("1.234,56")
        assert resultado == pytest.approx(1234.56)

    def test_normalizar_decimal_formato_americano(self):
        """Converte valor com ponto decimal."""
        resultado = normalizar_decimal("1234.56")
        assert resultado == pytest.approx(1234.56)

    def test_normalizar_decimal_com_moeda(self):
        """Ignora prefixos monet��rios e espa��os."""
        resultado = normalizar_decimal("R$ 1.234,56")
        assert resultado == pytest.approx(1234.56)

    def test_normalizar_decimal_invalido(self):
        """Retorna None para texto n��o num��rico."""
        assert normalizar_decimal("valor") is None

    def test_normalizar_decimal_nulo(self):
        """Retorna None para entrada nula."""
        assert normalizar_decimal(None) is None


class TestFormatarValorString:
    """Testes para a função formatar_valor_string."""
    
    def test_formatar_valor_numerico(self):
        """Testa formatação de valor numérico."""
        resultado = formatar_valor_string(123.45)
        assert resultado == "123.45"
    
    def test_formatar_valor_nulo(self):
        """Testa formatação de valor nulo."""
        resultado = formatar_valor_string(None)
        assert resultado == ""
    
    def test_formatar_valor_string(self):
        """Testa formatação de string."""
        resultado = formatar_valor_string("texto")
        assert resultado == "texto"
    
    def test_formatar_valor_zero(self):
        """Testa formatação de zero."""
        resultado = formatar_valor_string(0)
        assert resultado == "0"


class TestExtrairTelefone:
    """Testes para a função extrair_telefone."""
    
    def test_extrair_telefone_formatado(self):
        """Testa extração de telefone formatado."""
        resultado = extrair_telefone("(11) 99999-9999")
        assert resultado == "11999999999"
    
    def test_extrair_telefone_apenas_numeros(self):
        """Testa extração de telefone com apenas números."""
        resultado = extrair_telefone("11999999999")
        assert resultado == "11999999999"
    
    def test_extrair_telefone_nulo(self):
        """Testa extração de telefone nulo."""
        resultado = extrair_telefone(None)
        assert resultado == ""
    
    def test_extrair_telefone_vazio(self):
        """Testa extração de telefone vazio."""
        resultado = extrair_telefone("")
        assert resultado == ""
    
    def test_extrair_telefone_com_espacos(self):
        """Testa extração de telefone com espaços."""
        resultado = extrair_telefone("11 9 9999 9999")
        assert resultado == "11999999999"


class TestFormatarDatasSerie:
    """Testes para a função formatar_datas_serie."""
    
    def test_formatar_datas_serie_validas(self):
        """Testa formatação de série com datas válidas."""
        serie = pd.Series(["2023-12-31", "2024-01-01"])
        resultado = formatar_datas_serie(serie)
        esperado = pd.Series(["31/12/2023", "01/01/2024"])
        pd.testing.assert_series_equal(resultado, esperado)
    
    def test_formatar_datas_serie_com_nulos(self):
        """Testa formatação de série com valores nulos."""
        serie = pd.Series(["2023-12-31", None, "data_invalida"])
        resultado = formatar_datas_serie(serie)
        esperado = pd.Series(["31/12/2023", "", ""])
        pd.testing.assert_series_equal(resultado, esperado)
    
    def test_formatar_datas_serie_formato_customizado(self):
        """Testa formatação com formato customizado."""
        serie = pd.Series(["2023-12-31"])
        resultado = formatar_datas_serie(serie, formato="%Y-%m-%d")
        esperado = pd.Series(["2023-12-31"])
        pd.testing.assert_series_equal(resultado, esperado)
    
    def test_formatar_datas_serie_vazia(self):
        """Testa formatação de série vazia."""
        serie = pd.Series([], dtype=object)
        resultado = formatar_datas_serie(serie)
        esperado = pd.Series([], dtype=object)
        pd.testing.assert_series_equal(resultado, esperado)
