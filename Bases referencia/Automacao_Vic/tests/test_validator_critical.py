import logging
from typing import Dict

import pandas as pd
import pytest

from src.utils.validator import MaxValidator, VicValidator


@pytest.fixture()
def logger() -> logging.Logger:
    return logging.getLogger("test")


class TestMaxValidator:
    def test_validar_dados_identifica_chaves_invalidas(self, logger: logging.Logger) -> None:
        validator = MaxValidator({"validation": {"chave_regex": r"^[A-Z]{3}-\d{3}$"}}, logger)

        df = pd.DataFrame(
            {
                "CHAVE": ["ABC-123", "SEM_TRACO", ""],
                "PARCELA": [1, 2, 3],
            }
        )

        validos, invalidos = validator.validar_dados(df)

        assert list(validos["CHAVE"]) == ["ABC-123"]
        assert set(invalidos["CHAVE"]) == {"SEM_TRACO", ""}

    def test_validar_dados_utiliza_coluna_parcela_quando_ausente(self, logger: logging.Logger) -> None:
        validator = MaxValidator({}, logger)

        df = pd.DataFrame({"PARCELA": ["A-1", "SEMFORMATO", ""]})
        validos, invalidos = validator.validar_dados(df)

        assert list(validos["PARCELA"]) == ["A-1"]
        assert set(invalidos["PARCELA"]) == {"SEMFORMATO", ""}

    def test_validar_amostra_retorna_informacoes(self, logger: logging.Logger) -> None:
        validator = MaxValidator({}, logger)

        df = pd.DataFrame({"CHAVE": ["A-1", "B-2", "C-3"]})
        resultado = validator.validar_amostra(df, n_amostras=2)

        assert resultado["total"] == 3
        assert len(resultado["amostras"]) == 2
        assert "regex_chave" in resultado


class TestVicValidator:
    def test_validar_dados_flag_cpf_vazio(self, logger: logging.Logger) -> None:
        validator = VicValidator({}, logger)

        df = pd.DataFrame(
            {
                "CPFCNPJ_CLIENTE": ["12345678901", "", None],
                "VENCIMENTO": pd.to_datetime(["2024-01-01", "2024-01-01", "2024-01-01"]),
            }
        )

        validos, invalidos = validator.validar_dados(df)

        assert list(validos["CPFCNPJ_CLIENTE"]) == ["12345678901"]
        assert len(invalidos) == 2

    def test_validar_dados_flag_vencimento_nao_informado(self, logger: logging.Logger) -> None:
        validator = VicValidator({}, logger)

        df = pd.DataFrame(
            {
                "CPFCNPJ_CLIENTE": ["12345678901", "98765432100"],
                "VENCIMENTO": [pd.NaT, pd.Timestamp("2024-02-01")],
            }
        )

        validos, invalidos = validator.validar_dados(df)

        assert len(validos) == 1
        assert invalidos.iloc[0]["motivo_inconsistencia"].startswith("VENCIMENTO_INVALIDO")

    def test_obter_estatisticas_inconsistencias(self, logger: logging.Logger) -> None:
        validator = VicValidator({}, logger)

        df = pd.DataFrame(
            {
                "CPFCNPJ_CLIENTE": ["", ""],
                "VENCIMENTO": [pd.NaT, pd.NaT],
            }
        )

        _, invalidos = validator.validar_dados(df)
        stats: Dict[str, int] = validator.obter_estatisticas_inconsistencias(invalidos)

        assert stats["VENCIMENTO_INVALIDO"] == 2
        assert stats["CPF/CNPJ nulo ou vazio"] == 2
