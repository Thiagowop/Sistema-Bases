"""Validação real de ocorrências da blacklist nas bases do pipeline.

Este teste executa os processadores com os dados reais disponíveis no
repositório e verifica, em cada etapa, quantos registros pertencem à
blacklist. Além de validar a existência de ocorrências, o teste garante a
contagem total de registros e o detalhamento por CPF para as bases de
entrada, tratadas, devolução e batimento.
"""

from __future__ import annotations

from collections import Counter
from copy import deepcopy
from pathlib import Path
import re
import zipfile
import xml.etree.ElementTree as ET
from typing import Dict, Iterable, Iterator

import pandas as pd

from src.config.loader import ConfigLoader
from src.processors import (
    BatimentoProcessor,
    DevolucaoProcessor,
    MaxProcessor,
    VicProcessor,
)

DATA_DIR = Path(__file__).resolve().parents[1] / "data"
CHUNKSIZE = 50_000


def _normalize_document(series: pd.Series) -> pd.Series:
    """Remove caracteres não numéricos preservando zeros à esquerda."""

    return series.astype(str).str.replace(r"\D", "", regex=True).str.strip()


def _find_cpf_column(df: pd.DataFrame) -> str:
    """Identifica a coluna que contém o CPF/CNPJ do cliente."""

    normalized = {
        col: "".join(ch for ch in col.upper() if ch.isalnum()) for col in df.columns
    }
    # Prioridade para colunas claramente identificadas
    for col, norm in normalized.items():
        if "CPFCNPJCLIENTE" in norm or norm.startswith("CPFCNPJ"):
            return col
    for col, norm in normalized.items():
        if norm.startswith("CPF") or "CPF" in norm:
            return col
    raise ValueError(f"Coluna de CPF/CNPJ não encontrada. Colunas: {list(df.columns)}")


def _iter_csv_frames(path: Path, *, chunksize: int = CHUNKSIZE) -> Iterator[pd.DataFrame]:
    """Itera sobre DataFrames carregados de um CSV ou ZIP com CSVs."""

    path = Path(path)
    if path.suffix.lower() == ".zip":
        with zipfile.ZipFile(path) as zf:
            csv_names = [n for n in zf.namelist() if n.lower().endswith(".csv")]
            for name in csv_names:
                with zf.open(name) as fh:
                    reader = pd.read_csv(
                        fh,
                        sep=";",
                        dtype=str,
                        encoding="utf-8-sig",
                        chunksize=chunksize,
                    )
                    if isinstance(reader, pd.DataFrame):
                        yield reader
                    else:
                        yield from reader
    elif path.suffix.lower() == ".csv":
        reader = pd.read_csv(
            path,
            sep=";",
            dtype=str,
            encoding="utf-8-sig",
            chunksize=chunksize,
        )
        if isinstance(reader, pd.DataFrame):
            yield reader
        else:
            yield from reader
    else:
        raise ValueError(f"Formato não suportado para leitura: {path}")


def _summarize_base(path: Path, blacklist: set[str]) -> Dict[str, object]:
    """Retorna contagem total e incidências da blacklist para um arquivo."""

    total = 0
    counts: Counter[str] = Counter()
    cpf_column: str | None = None

    for chunk in _iter_csv_frames(path):
        if chunk.empty:
            continue
        if cpf_column is None:
            cpf_column = _find_cpf_column(chunk)
        docs = _normalize_document(chunk[cpf_column])
        total += int(len(docs))
        mask = docs.isin(blacklist)
        counts.update(docs[mask])

    return {
        "total_registros": total,
        "registros_blacklist": int(sum(counts.values())),
        "por_cpf": {cpf: int(qtd) for cpf, qtd in sorted(counts.items())},
        "possui_blacklist": bool(counts),
    }


def _assert_summary_integrity(
    nome_base: str, resumo: Dict[str, object], blacklist: set[str]
) -> None:
    """Valida coerência interna do resumo calculado para uma base."""

    total = resumo.get("total_registros")
    blacklist_total = resumo.get("registros_blacklist")
    por_cpf = resumo.get("por_cpf")
    possui = resumo.get("possui_blacklist")

    assert isinstance(total, int) and total >= 0, (
        f"Total inválido para {nome_base}: {total}"
    )
    assert isinstance(blacklist_total, int) and blacklist_total >= 0, (
        f"Contagem de blacklist inválida para {nome_base}: {blacklist_total}"
    )
    assert isinstance(por_cpf, dict), f"Resumo por CPF inválido para {nome_base}: {por_cpf}"
    assert isinstance(possui, bool), f"Indicador de blacklist inválido para {nome_base}: {possui}"

    for cpf, qtd in por_cpf.items():
        assert isinstance(cpf, str) and cpf, f"CPF inválido em {nome_base}: {cpf!r}"
        assert cpf in blacklist, f"CPF {cpf} não pertence à blacklist em {nome_base}"
        assert isinstance(qtd, int) and qtd > 0, (
            f"Quantidade inválida para CPF {cpf} em {nome_base}: {qtd}"
        )

    assert blacklist_total == sum(por_cpf.values()), (
        f"Soma por CPF divergente do total para {nome_base}: {blacklist_total}"
    )
    assert total >= blacklist_total, (
        f"Total de registros menor que a soma da blacklist para {nome_base}: "
        f"{total} < {blacklist_total}"
    )

    if possui:
        assert blacklist_total > 0, (
            f"Indicador de blacklist inconsistente para {nome_base}: possui=True,"
            f" mas contagem={blacklist_total}"
        )
        assert por_cpf, f"Indicador de blacklist verdadeiro sem detalhamento em {nome_base}"
    else:
        assert blacklist_total == 0, (
            f"Indicador de blacklist inconsistente para {nome_base}: possui=False,"
            f" mas contagem={blacklist_total}"
        )
        assert not por_cpf, (
            f"Resumo por CPF deveria estar vazio para {nome_base} sem blacklist"
        )


def _format_summary(nome_base: str, resumo: Dict[str, object]) -> str:
    """Gera uma representação textual amigável de um resumo calculado."""

    linhas = [
        f"Base: {nome_base}",
        f"  Total de registros: {resumo['total_registros']}",
        f"  Registros na blacklist: {resumo['registros_blacklist']}",
    ]
    if resumo["possui_blacklist"]:
        linhas.append("  Detalhamento por CPF:")
        for cpf, qtd in resumo["por_cpf"].items():
            linhas.append(f"    - {cpf}: {qtd}")
    else:
        linhas.append("  Sem ocorrências de blacklist nesta base.")
    return "\n".join(linhas)


def _ensure_path(value: object) -> Path:
    """Converte o retorno dos processadores em Path válido."""

    if isinstance(value, Path):
        return value
    return Path(str(value))


def _load_blacklist_cpfs(xlsx_path: Path) -> set[str]:
    """Extrai os CPFs presentes na planilha de blacklist."""

    with zipfile.ZipFile(xlsx_path) as zf:
        ns = {"main": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
        shared: list[str] = []
        if "xl/sharedStrings.xml" in zf.namelist():
            root = ET.fromstring(zf.read("xl/sharedStrings.xml"))
            for si in root.findall("main:si", ns):
                text = "".join(t.text or "" for t in si.findall(".//main:t", ns))
                shared.append(text)
        sheet = ET.fromstring(zf.read("xl/worksheets/sheet1.xml"))
        valores: list[str] = []
        for row in sheet.findall("main:sheetData/main:row", ns):
            for cell in row.findall("main:c", ns):
                ref = cell.attrib.get("r", "")
                if not ref.startswith("A"):
                    continue
                node = cell.find("main:v", ns)
                if node is None:
                    valores.append("")
                    continue
                raw = node.text or ""
                if cell.attrib.get("t") == "s":
                    idx = int(raw)
                    valores.append(shared[idx] if idx < len(shared) else raw)
                else:
                    valores.append(raw)
    documentos = {
        re.sub(r"\D", "", valor)
        for valor in valores[1:]  # Ignora o cabeçalho
        if valor
    }
    return {doc for doc in documentos if doc}


def _ajustar_config(base_config: Dict[str, object], tmp_dir: Path, blacklist: Iterable[str]) -> Dict[str, object]:
    """Cria uma cópia da configuração apontando para diretórios temporários."""

    cfg = deepcopy(base_config)
    output_dir = tmp_dir / "output"
    output_dir.mkdir(parents=True, exist_ok=True)
    cfg.setdefault("paths", {}).setdefault("output", {})["base"] = str(output_dir)

    fake_blacklist = tmp_dir / "blacklist"
    fake_blacklist.mkdir(parents=True, exist_ok=True)
    cfg.setdefault("paths", {}).setdefault("input", {})["blacklist"] = str(fake_blacklist)

    cfg.setdefault("global", {})["add_timestamp"] = False
    cfg["global"]["add_timestamp_to_files"] = False

    vic_cfg = cfg.setdefault("vic_processor", {})
    vic_cfg["blacklist_clientes"] = list(blacklist)
    return cfg


def test_blacklist_nas_bases_reais(tmp_path: Path) -> None:
    """Garante que as bases reais reportam corretamente as ocorrências da blacklist."""

    base_config = ConfigLoader().get_config()
    blacklist_cpfs = _load_blacklist_cpfs(DATA_DIR / "input/blacklist/Blacklist VIC.xlsx")
    cfg = _ajustar_config(base_config, tmp_path, blacklist_cpfs)

    vic_input = DATA_DIR / "input/vic/VicCandiotto.zip"
    max_input = DATA_DIR / "input/max/MaxSmart.zip"

    vic_proc = VicProcessor(config=cfg)
    vic_stats = vic_proc.processar(vic_input)
    vic_path = _ensure_path(vic_stats["arquivo_gerado"])

    max_proc = MaxProcessor(config=cfg)
    max_stats = max_proc.processar(max_input)
    max_path = _ensure_path(max_stats["arquivo_gerado"])

    devolucao_proc = DevolucaoProcessor(config=cfg)
    devolucao_stats = devolucao_proc.processar(vic_path, max_path)
    devolucao_path = _ensure_path(devolucao_stats["arquivo_gerado"])

    batimento_proc = BatimentoProcessor(config=cfg)
    batimento_stats = batimento_proc.processar(vic_path, max_path, output_dir=Path(cfg["paths"]["output"]["base"]))
    batimento_path = _ensure_path(batimento_stats["arquivo_gerado"])

    summaries = {
        "entrada_vic": _summarize_base(vic_input, blacklist_cpfs),
        "saida_vic": _summarize_base(vic_path, blacklist_cpfs),
        "saida_max": _summarize_base(max_path, blacklist_cpfs),
        "saida_devolucao": _summarize_base(devolucao_path, blacklist_cpfs),
        "saida_batimento": _summarize_base(batimento_path, blacklist_cpfs),
    }

    for nome, resumo in summaries.items():
        _assert_summary_integrity(nome, resumo, blacklist_cpfs)

    assert any(resumo["possui_blacklist"] for resumo in summaries.values()), (
        "Nenhuma das bases analisadas possui ocorrências de blacklist."
    )

    relatorio = [
        "Resumo da análise de blacklist por base:",
        *(_format_summary(nome, resumo) for nome, resumo in summaries.items()),
    ]
    print("\n".join(relatorio))

