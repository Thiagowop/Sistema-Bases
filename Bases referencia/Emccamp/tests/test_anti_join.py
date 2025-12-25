import unittest

import pandas as pd

from src.utils.anti_join import procv_left_minus_right, procv_max_menos_emccamp


class TestAntiJoin(unittest.TestCase):
    def test_procv_left_minus_right_basic(self) -> None:
        df_left = pd.DataFrame({'A': ['1', '2', '3'], 'X': [10, 20, 30]})
        df_right = pd.DataFrame({'B': ['2']})

        out = procv_left_minus_right(df_left, df_right, col_left='A', col_right='B')
        self.assertEqual(out['A'].tolist(), ['1', '3'])

    def test_procv_max_menos_emccamp_uses_cols(self) -> None:
        df_max = pd.DataFrame({'CHAVE': ['a', 'b', 'c'], 'VAL': [1, 2, 3]})
        df_emc = pd.DataFrame({'CHAVE': ['b']})

        out = procv_max_menos_emccamp(df_max, df_emc, col_max='CHAVE', col_emccamp='CHAVE')
        self.assertEqual(out['CHAVE'].tolist(), ['a', 'c'])


if __name__ == '__main__':
    unittest.main()
