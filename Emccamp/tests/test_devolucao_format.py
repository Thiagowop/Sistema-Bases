import tempfile
import unittest
from pathlib import Path

import pandas as pd

from src.config.loader import LoadedConfig
from src.processors.devolucao import DevolucaoProcessor


class TestDevolucaoFormat(unittest.TestCase):
    def test_formatar_layout_universal_minimo(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base_path = Path(tmp)
            cfg = {
                'global': {
                    'empresa': {'cnpj': '12345678000199'},
                    'encoding': 'utf-8-sig',
                    'csv_separator': ';',
                    'date_format': '%d/%m/%Y',
                },
                'logging': {},
                'paths': {
                    'output': {'base': 'data/output'},
                    'input': {},
                    'logs': 'logs',
                },
                'devolucao': {
                    'chaves': {'emccamp': 'CHAVE', 'max': 'CHAVE'},
                    'status_devolucao_fixo': '98',
                },
            }
            loaded = LoadedConfig(data=cfg, base_path=base_path)
            proc = DevolucaoProcessor(loaded)

            df = pd.DataFrame(
                {
                    'CHAVE': ['X1'],
                    'CPF_CNPJ': ['123.456.789-01'],
                    'NOME_RAZAO_SOCIAL': ['Fulano'],
                    'DATA_VENCIMENTO': ['2024-01-31'],
                    'VALOR': ['10,00'],
                    'TIPO_PARCELA': ['teste'],
                    'DATA_BASE': ['2024-02-15'],
                }
            )

            out = proc._formatar_devolucao(df)
            self.assertEqual(
                out.columns.tolist(),
                [
                    'CNPJ CREDOR',
                    'CPFCNPJ CLIENTE',
                    'NOME / RAZAO SOCIAL',
                    'PARCELA',
                    'VENCIMENTO',
                    'VALOR',
                    'TIPO PARCELA',
                    'DATA DEVOLUCAO',
                    'STATUS',
                ],
            )
            self.assertEqual(out.loc[0, 'CNPJ CREDOR'], '12345678000199')
            self.assertEqual(out.loc[0, 'PARCELA'], 'X1')
            self.assertEqual(out.loc[0, 'STATUS'], '98')
            self.assertEqual(out.loc[0, 'DATA DEVOLUCAO'], '15/02/2024')


if __name__ == '__main__':
    unittest.main()
