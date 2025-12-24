"""Ferramenta interativa para comparar bases (PROCV simplificado).

O fluxo usa caixas de dialogo do PyAutoGUI e filedialog do Tkinter.
"""

from __future__ import annotations

import csv
import sys
import tempfile
import zipfile
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, Tuple

import pandas as pd
import pyautogui
from tkinter import Tk, filedialog

ALLOWED_TRUE = {"1", "true", "sim", "yes", "presente", "x"}
ALLOWED_FALSE = {"0", "false", "nao", "ausente", "", "nan", "none"}


def _ask_file(title: str) -> Path:
    pyautogui.alert(text=title, title="Selecao de arquivo", button="OK")
    root = Tk()
    root.withdraw()
    file_path = filedialog.askopenfilename()
    root.destroy()
    if not file_path:
        raise RuntimeError("Nenhum arquivo selecionado.")
    return Path(file_path)


def _load_dataframe(path: Path) -> pd.DataFrame:
    if path.suffix.lower() == ".zip":
        with zipfile.ZipFile(path, "r") as zf:
            csv_members = [n for n in zf.namelist() if n.lower().endswith(".csv")]
            if not csv_members:
                raise ValueError(f"Nenhum CSV encontrado dentro de {path.name}")
            target = csv_members[0]
            with zf.open(target) as stream:
                return pd.read_csv(stream, dtype=str, keep_default_na=False)
    if path.suffix.lower() in {".csv", ".txt"}:
        return pd.read_csv(path, dtype=str, keep_default_na=False)
    raise ValueError(f"Formato nao suportado: {path.suffix}")


def _prompt_column(df: pd.DataFrame, base_label: str) -> str:
    columns = "\n".join(df.columns.tolist())
    prompt = (
        f"Colunas disponiveis na base {base_label}:\n{columns}\n\n"
        "Digite exatamente o nome da coluna para comparar:"
    )
    coluna = pyautogui.prompt(text=prompt, title=f"Coluna da base {base_label}")
    if not coluna:
        raise RuntimeError("Coluna nao informada.")
    if coluna not in df.columns:
        raise RuntimeError(f"Coluna '{coluna}' nao encontrada na base {base_label}.")
    return coluna


def _normalizar_serie(serie: pd.Series) -> pd.Series:
    return serie.astype(str).str.strip().str.lower()


def comparar_bases(df_a: pd.DataFrame, col_a: str, df_b: pd.DataFrame, col_b: str) -> Tuple[pd.Series, pd.Series]:
    serie_a = _normalizar_serie(df_a[col_a]).replace({"": pd.NA}).dropna()
    serie_b = _normalizar_serie(df_b[col_b]).replace({"": pd.NA}).dropna()

    set_a = set(serie_a)
    set_b = set(serie_b)

    apenas_a = sorted(set_a - set_b)
    apenas_b = sorted(set_b - set_a)

    return pd.Series(apenas_a), pd.Series(apenas_b)


def salvar_resultados(apenas_a: pd.Series, apenas_b: pd.Series, base_dir: Path) -> Path:
    base_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    destino = base_dir / f"resultado_comparacao_{timestamp}.csv"
    with destino.open("w", newline="", encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["origem", "valor"])
        for valor in apenas_a:
            writer.writerow(["apenas_base_a", valor])
        for valor in apenas_b:
            writer.writerow(["apenas_base_b", valor])
    return destino


def interpretar_flag(valor: str) -> bool:
    texto = (valor or "").strip().lower()
    if texto in ALLOWED_TRUE:
        return True
    if texto in ALLOWED_FALSE:
        return False
    raise ValueError(f"Valor de flag nao esperado: '{valor}'")


def validar_planilha(planilha: Path, df_a: pd.DataFrame, col_a: str, df_b: pd.DataFrame, col_b: str) -> Dict[str, int]:
    df_check = _load_dataframe(planilha)
    valor_col = pyautogui.prompt(
        text=(
            f"Planilha selecionada: {planilha.name}\n"
            f"Colunas: {', '.join(df_check.columns)}\n\n"
            "Informe a coluna com os valores a validar:"
        ),
        title="Planilha de validacao"
    )
    if not valor_col or valor_col not in df_check.columns:
        raise RuntimeError("Coluna de valores invalida para a planilha de validacao.")

    flag_a_col = pyautogui.prompt(
        text=(
            "Informe a coluna que indica presenca na Base A "
            "(valores aceitos: 1, sim, true, presente ou 0, nao, false, ausente):"
        ),
        title="Planilha de validacao"
    )
    flag_b_col = pyautogui.prompt(
        text=(
            "Informe a coluna que indica presenca na Base B "
            "(valores aceitos: 1, sim, true, presente ou 0, nao, false, ausente):"
        ),
        title="Planilha de validacao"
    )
    for required in (flag_a_col, flag_b_col):
        if not required or required not in df_check.columns:
            raise RuntimeError("Coluna de presenca nao encontrada na planilha informada.")

    valores_a = set(_normalizar_serie(df_a[col_a]).dropna())
    valores_b = set(_normalizar_serie(df_b[col_b]).dropna())

    inconsistencias = {
        "esperado_presentes_a_mas_ausentes": 0,
        "esperado_presentes_b_mas_ausentes": 0,
        "esperado_ausentes_a_mas_presentes": 0,
        "esperado_ausentes_b_mas_presentes": 0,
    }

    for _, linha in df_check.iterrows():
        valor_norm = str(linha[valor_col]).strip().lower()
        try:
            esperado_a = interpretar_flag(linha[flag_a_col])
            esperado_b = interpretar_flag(linha[flag_b_col])
        except ValueError as exc:
            pyautogui.alert(text=str(exc), title="Valor inesperado", button="OK")
            continue

        presente_a = valor_norm in valores_a
        presente_b = valor_norm in valores_b

        if esperado_a and not presente_a:
            inconsistencias["esperado_presentes_a_mas_ausentes"] += 1
        if esperado_b and not presente_b:
            inconsistencias["esperado_presentes_b_mas_ausentes"] += 1
        if not esperado_a and presente_a:
            inconsistencias["esperado_ausentes_a_mas_presentes"] += 1
        if not esperado_b and presente_b:
            inconsistencias["esperado_ausentes_b_mas_presentes"] += 1

    return inconsistencias


def main() -> None:
    try:
        pyautogui.alert(text="Bem-vindo ao comparador de bases.", title="Teste PROCV", button="OK")
        caminho_a = _ask_file("Selecione a base A (CSV ou ZIP contendo CSV).")
        caminho_b = _ask_file("Selecione a base B (CSV ou ZIP contendo CSV).")

        df_a = _load_dataframe(caminho_a)
        df_b = _load_dataframe(caminho_b)

        col_a = _prompt_column(df_a, "A")
        col_b = _prompt_column(df_b, "B")

        apenas_a, apenas_b = comparar_bases(df_a, col_a, df_b, col_b)
        destino = salvar_resultados(apenas_a, apenas_b, Path("tests/output"))

        resumo = (
            f"Comparacao concluida!\n\n"
            f"Valores apenas na base A: {len(apenas_a)}\n"
            f"Valores apenas na base B: {len(apenas_b)}\n\n"
            f"Resultado salvo em: {destino}"
        )
        pyautogui.alert(text=resumo, title="Resumo da comparacao", button="OK")

        if pyautogui.confirm(text="Deseja validar uma planilha gerada anteriormente?", buttons=["Sim", "Nao"], title="Validacao complementar") == "Sim":
            planilha = _ask_file("Selecione a planilha de comparacao a validar (CSV ou ZIP com CSV).")
            inconsistencias = validar_planilha(planilha, df_a, col_a, df_b, col_b)
            texto_validacao = "\n".join(f"{chave}: {valor}" for chave, valor in inconsistencias.items())
            pyautogui.alert(text=f"Validacao concluida:\n{texto_validacao}", title="Resultado da validacao", button="OK")

        pyautogui.alert(text="Processo finalizado.", title="Teste PROCV", button="OK")

    except Exception as exc:  # catch-all para interacao humana
        pyautogui.alert(text=f"Erro: {exc}", title="Teste PROCV", button="OK")
        raise


if __name__ == "__main__":
    try:
        main()
    except SystemExit:
        raise
    except Exception:
        sys.exit(1)
