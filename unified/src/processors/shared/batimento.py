"""Batimento EMCCAMP x MAX - identifica registros EMCCAMP ausentes no MAX."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional, Tuple

import pandas as pd

from src.config.loader import ConfigLoader, LoadedConfig
from src.utils import digits_only, procv_emccamp_menos_max
from src.utils.io import DatasetIO
from src.utils.logger import get_logger
from src.utils.output_formatter import format_batimento_output
from src.utils.path_manager import PathManager

LAYOUT_COLS = [
    "CPFCNPJ CLIENTE",
    "NOME / RAZAO SOCIAL",
    "NUMERO CONTRATO",
    "PARCELA",
    "OBSERVACAO PARCELA",
    "VENCIMENTO",
    "VALOR",
    "EMPREENDIMENTO",
    "CNPJ EMPREENDIMENTO",
    "TIPO PARCELA",
    "CNPJ CREDOR",
]


@dataclass
class BatimentoStats:
    registros_emccamp: int
    registros_max: int
    registros_max_dedup: int
    registros_batimento: int
    judicial: int
    extrajudicial: int
    arquivo_saida: Path | None
    duracao: float


class BatimentoProcessor:
    """Executa anti-join EMCCAMP - MAX e separa judicial/extrajudicial."""

    def __init__(self, config: LoadedConfig) -> None:
        self.config = config

        self.paths = PathManager(config.base_path, config.data)

        self.emccamp_dir = self.paths.resolve_output("emccamp_tratada", "emccamp_tratada")
        self.max_dir = self.paths.resolve_output("max_tratada", "max_tratada")
        self.batimento_dir = self.paths.resolve_output("batimento", "batimento")

        judicial_dir = self.paths.resolve_input("judicial", "data/input/judicial")
        self.judicial_dir = judicial_dir

        logging_cfg = config.get("logging", {})
        logs_dir = self.paths.resolve_logs()
        self.logger = get_logger("batimento_emccamp", logs_dir, logging_cfg)

        global_cfg = config.get("global", {})
        self.encoding = global_cfg.get("encoding", "utf-8-sig")
        self.separator = global_cfg.get("csv_separator", ";")
        empresa_cfg = global_cfg.get("empresa", {})
        self.cnpj_credor = str(empresa_cfg.get("cnpj", "")).strip()
        if not self.cnpj_credor:
            raise ValueError("CNPJ do credor nao configurado (global.empresa.cnpj)")
        self.io = DatasetIO(separator=self.separator, encoding=self.encoding)

        self.judicial_cpfs: set[str] = set()

        flags_cfg = config.get("flags", {})
        filtros_cfg = flags_cfg.get("filtros_batimento", {})
        self.filtrar_tipo_pagto = bool(filtros_cfg.get("habilitar", False))
        tipos_excluir = filtros_cfg.get("tipos_excluir", [])
        self.tipos_pagto_excluir = {str(item).strip().upper() for item in tipos_excluir if item is not None}

    def process(self) -> BatimentoStats:
        inicio = datetime.now()

        emccamp_path = self._resolve_file(self.emccamp_dir, "emccamp_tratada_*.zip")
        max_path = self._resolve_file(self.max_dir, "max_tratada_*.zip")

        df_emccamp = self.io.read(emccamp_path)
        df_max = self.io.read(max_path)

        if self.filtrar_tipo_pagto:
            if "TIPO_PAGTO" not in df_emccamp.columns:
                self.logger.warning("Filtro de TIPO_PAGTO configurado, mas coluna TIPO_PAGTO nao encontrada na base EMCCAMP.")
            else:
                serie = df_emccamp["TIPO_PAGTO"].astype(str).str.strip().str.upper()
                df_emccamp = df_emccamp[~serie.isin(self.tipos_pagto_excluir)].copy()

        if "CHAVE" not in df_emccamp.columns:
            raise ValueError("Coluna CHAVE ausente na base EMCCAMP tratada")
        if "CHAVE" not in df_max.columns:
            raise ValueError("Coluna CHAVE ausente na base MAX tratada")

        df_max_dedup = self._deduplicate_max(df_max)
        df_batimento = procv_emccamp_menos_max(
            df_emccamp,
            df_max_dedup,
            col_emccamp="CHAVE",
            col_max="CHAVE",
        )

        df_formatado = self._format_layout(df_batimento)
        self._load_judicial_cpfs()
        df_judicial, df_extrajudicial = self._split_portfolios(df_formatado)

        arquivo_saida = self._export(df_judicial, df_extrajudicial)

        duracao = (datetime.now() - inicio).total_seconds()
        stats = BatimentoStats(
            registros_emccamp=len(df_emccamp),
            registros_max=len(df_max),
            registros_max_dedup=len(df_max_dedup),
            registros_batimento=len(df_formatado),
            judicial=len(df_judicial),
            extrajudicial=len(df_extrajudicial),
            arquivo_saida=arquivo_saida,
            duracao=duracao,
        )
        self._show_summary(stats, emccamp_path.name, max_path.name)
        return stats

    def _resolve_file(self, directory: Path, pattern: str) -> Path:
        if not directory.exists():
            raise FileNotFoundError(f"Diretorio nao encontrado: {directory}")

        candidates = sorted(directory.glob(pattern), key=lambda path: path.stat().st_mtime, reverse=True)
        if not candidates:
            raise FileNotFoundError(f"Nenhum arquivo correspondente a {pattern} em {directory}")
        return candidates[0]

    def _deduplicate_max(self, df_max: pd.DataFrame) -> pd.DataFrame:
        if "PARCELA" not in df_max.columns:
            raise ValueError("Base MAX tratada nao contem coluna PARCELA")

        if not df_max["PARCELA"].duplicated().any():
            return df_max.copy()

        df_tmp = df_max.copy()
        if "DT_BAIXA" in df_tmp.columns:
            df_tmp["__dt_sort"] = pd.to_datetime(df_tmp["DT_BAIXA"], format="%Y-%m-%d", errors="coerce")
            df_tmp = df_tmp.sort_values("__dt_sort", ascending=False, na_position="last")
            df_tmp = df_tmp.drop(columns=["__dt_sort"])
        return df_tmp.drop_duplicates(subset=["PARCELA"], keep="first")

    def _format_layout(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return pd.DataFrame(columns=LAYOUT_COLS)

        def _column(column_name: str, fallback: Optional[str] = None) -> pd.Series:
            if column_name in df.columns:
                return df[column_name].astype(str).str.strip()
            if fallback and fallback in df.columns:
                return df[fallback].astype(str).str.strip()
            return pd.Series(["" for _ in range(len(df))], index=df.index)

        formatted = pd.DataFrame(index=df.index)
        formatted["CPFCNPJ CLIENTE"] = _column("CPF_CNPJ", "CPFCNPJ_CLIENTE")
        formatted["NOME / RAZAO SOCIAL"] = _column("NOME_RAZAO_SOCIAL", "CLIENTE")
        formatted["NUMERO CONTRATO"] = _column("CONTRATO", "NUMERO_CONTRATO")
        formatted["PARCELA"] = _column("CHAVE")
        formatted["OBSERVACAO PARCELA"] = _column("PARCELA")
        formatted["VENCIMENTO"] = _column("DATA_VENCIMENTO", "VENCIMENTO")
        formatted["VALOR"] = _column("VALOR_PARCELA", "VALOR")
        # Converte VALOR para numérico para que a escrita CSV aplique vírgula decimal
        serie_valor = formatted["VALOR"].astype(str).str.strip()
        mask_comma = serie_valor.str.contains(',', na=False)
        # Valores com vírgula: mantém formato original com vírgula
        valor_comma = serie_valor[mask_comma]
        # Valores sem vírgula: mantém formato original  
        valor_dot = serie_valor[~mask_comma]
        formatted.loc[mask_comma, "VALOR"] = valor_comma
        formatted.loc[~mask_comma, "VALOR"] = valor_dot
        # Não converter para numérico - manter vírgulas como decimal no CSV
        # formatted["VALOR"] = pd.to_numeric(formatted["VALOR"], errors="coerce")  # REMOVIDO
        formatted["EMPREENDIMENTO"] = _column("NOME_EMPREENDIMENTO", "EMPREENDIMENTO")
        formatted["CNPJ EMPREENDIMENTO"] = _column("CNPJ_EMPREENDIMENTO")
        formatted["TIPO PARCELA"] = _column("TIPO_PAGTO").str.upper()
        formatted["CNPJ CREDOR"] = self.cnpj_credor
        return formatted[LAYOUT_COLS]

    def _load_judicial_cpfs(self) -> None:
        zip_path = self.judicial_dir / "ClientesJudiciais.zip"
        if not zip_path.exists():
            self.logger.info("ClientesJudiciais.zip nao encontrado; todos serao extrajudiciais")
            return

        import zipfile

        with zipfile.ZipFile(zip_path) as archive:
            names = [name for name in archive.namelist() if name.lower().endswith(".csv")]
            if not names:
                self.logger.warning("Arquivo judicial sem CSV; ignorando")
                return
            with archive.open(names[0]) as buffer:
                df = pd.read_csv(buffer, sep=self.separator, encoding=self.encoding, dtype=str)

        column_name: Optional[str] = None
        if "CPF_CNPJ" in df.columns:
            column_name = "CPF_CNPJ"
        elif "CPF" in df.columns:
            column_name = "CPF"

        if not column_name:
            self.logger.warning("Coluna CPF ou CPF_CNPJ ausente no arquivo judicial; ignorando")
            return

        self.judicial_cpfs = set(digits_only(df[column_name].dropna().astype(str)).tolist())

    def _split_portfolios(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
        if df.empty:
            return df, df

        normalizado = digits_only(df["CPFCNPJ CLIENTE"].fillna(""))
        mask_judicial = normalizado.isin(self.judicial_cpfs)
        return df[mask_judicial].copy(), df[~mask_judicial].copy()

    def _export(self, df_judicial: pd.DataFrame, df_extrajudicial: pd.DataFrame) -> Path | None:
        if df_judicial.empty and df_extrajudicial.empty:
            return None

        self.batimento_dir.mkdir(parents=True, exist_ok=True)

        PathManager.cleanup(self.batimento_dir, "emccamp_batimento_*.zip", self.logger, silent=True)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        zip_path = self.batimento_dir / f"emccamp_batimento_{timestamp}.zip"

        arquivos: Dict[str, pd.DataFrame] = {}
        if not df_judicial.empty:
            arquivos[f"emccamp_batimento_judicial_{timestamp}.csv"] = df_judicial
        if not df_extrajudicial.empty:
            arquivos[f"emccamp_batimento_extrajudicial_{timestamp}.csv"] = df_extrajudicial

        self.io.write_zip(arquivos, zip_path)
        return zip_path

    def _show_summary(self, stats: BatimentoStats, emccamp_nome: str, max_nome: str) -> None:
        format_batimento_output(
            emccamp_records=stats.registros_emccamp,
            max_records=stats.registros_max,
            max_dedup=stats.registros_max_dedup,
            batimento_records=stats.registros_batimento,
            judicial=stats.judicial,
            extrajudicial=stats.extrajudicial,
            output_file=stats.arquivo_saida if stats.arquivo_saida else "N/A",
            duration=stats.duracao
        )


def run(config_loader: ConfigLoader | None = None) -> BatimentoStats:
    loader = config_loader or ConfigLoader()
    config = loader.load()
    processor = BatimentoProcessor(config)
    return processor.process()
