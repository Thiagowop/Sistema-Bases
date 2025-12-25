"""Processing logic for the EMCCAMP dataset (TOTVS API output)."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict

import pandas as pd

from src.config.loader import ConfigLoader, LoadedConfig
from src.utils.io import DatasetIO
from src.utils.logger import get_logger
from src.utils.output_formatter import format_treatment_output
from src.utils.path_manager import PathManager


@dataclass
class ProcessorStats:
    registros_originais: int
    registros_finais: int
    inconsistencias: int
    arquivo_saida: Path
    arquivo_inconsistencias: Path | None


class EmccampProcessor:
    """Normalize and validate EMCCAMP data before downstream stages."""

    def __init__(self, config: LoadedConfig) -> None:
        self.config = config
        self.mapping = self.config.get_mapping("emccamp")

        self.paths = PathManager(config.base_path, config.data)

        logging_cfg = config.get("logging", {})
        logs_dir = self.paths.resolve_logs()
        self.logger = get_logger("emccamp_processor", logs_dir, logging_cfg)

        global_cfg = config.get("global", {})
        self.encoding = global_cfg.get("encoding", "utf-8-sig")
        self.separator = global_cfg.get("csv_separator", ",")
        self.io = DatasetIO(separator=self.separator, encoding=self.encoding)

        self.input_dir = self.paths.resolve_input("emccamp", "data/input/emccamp")
        self.output_dir = self.paths.resolve_output("emccamp_tratada", "emccamp_tratada")
        inconsist_dir_default = "inconsistencias"
        self.inconsist_dir = self.paths.resolve_output("inconsistencias", inconsist_dir_default)

    def process(self) -> ProcessorStats:
        source = self._resolve_source_file()
        df_raw = self.io.read(source)

        df_norm = self._apply_mapping(df_raw)
        df_norm = self._create_key(df_norm)
        df_valid, df_incons = self._validate(df_norm)

        stats = self._export(df_valid, df_incons, len(df_raw))
        return stats

    def _resolve_source_file(self) -> Path:
        if not self.input_dir.exists():
            raise FileNotFoundError(f"Diretorio de entrada nao encontrado: {self.input_dir}")

        candidates = sorted(self.input_dir.glob("*.zip"), key=lambda item: item.stat().st_mtime, reverse=True)
        candidates += sorted(self.input_dir.glob("*.csv"), key=lambda item: item.stat().st_mtime, reverse=True)
        if not candidates:
            raise FileNotFoundError(f"Nenhum arquivo EMCCAMP encontrado em {self.input_dir}")
        return candidates[0]

    def _apply_mapping(self, df: pd.DataFrame) -> pd.DataFrame:
        rename_map: Dict[str, str] = self.mapping.get("rename", {})
        df_norm = df.rename(columns=rename_map).copy()

        for column in df_norm.columns:
            if column in rename_map.values():
                try:
                    df_norm[column] = df_norm[column].astype(str).str.strip()
                except AttributeError:
                    self.logger.warning("Coluna %s nao retornou Series. Ignorando padronizacao.", column)

        return df_norm

    def _create_key(self, df: pd.DataFrame) -> pd.DataFrame:
        if "CONTRATO" not in df.columns or "PARCELA" not in df.columns:
            raise ValueError("Colunas CONTRATO e PARCELA sao obrigatorias para criar CHAVE")

        df = df.copy()
        contrato = df["CONTRATO"].astype(str).str.strip()
        parcela = df["PARCELA"].astype(str).str.strip()
        df["CHAVE"] = contrato + "-" + parcela
        return df

    def _validate(self, df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
        required = set(self.mapping.get("required", []))
        if required and not required.issubset(df.columns):
            missing = required.difference(df.columns)
            raise ValueError(f"Colunas obrigatorias ausentes apos mapeamento: {missing}")

        if "CHAVE" not in df.columns:
            raise ValueError("Coluna CHAVE nao foi gerada na base EMCCAMP")

        # EMCCAMP valida: CHAVE vazia, CHAVE duplicada, CPF_CNPJ vazio
        inconsist_mask = pd.Series(False, index=df.index)
        
        # 1. Validar CHAVE vazia (ja pega CONTRATO ou PARCELA vazio automaticamente)
        inconsist_mask |= df["CHAVE"].astype(str).str.strip().eq("")
        
        # 2. Validar CHAVE duplicada (CRITICO - pode causar problemas no batimento)
        chaves_duplicadas = df["CHAVE"].duplicated(keep=False)
        inconsist_mask |= chaves_duplicadas
        
        # 3. Validar CPF_CNPJ vazio (necessario para identificacao do cliente)
        if "CPF_CNPJ" in df.columns:
            inconsist_mask |= df["CPF_CNPJ"].astype(str).str.strip().eq("")
        
        inconsistencias_df = df[inconsist_mask].copy()
        df_valid = df[~inconsist_mask].copy()
        return df_valid, inconsistencias_df

    def _export(
        self,
        df_valid: pd.DataFrame,
        inconsistencias_df: pd.DataFrame,
        registros_originais: int,
    ) -> ProcessorStats:
        PathManager.cleanup(self.output_dir, "emccamp_tratada_*.zip", self.logger)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        csv_name = f"emccamp_tratada_{timestamp}.csv"
        zip_path = self.output_dir / f"emccamp_tratada_{timestamp}.zip"
        self.io.write_zip({csv_name: df_valid}, zip_path)

        inconsist_zip = None
        if not inconsistencias_df.empty:
            PathManager.cleanup(self.inconsist_dir, "emccamp_inconsistencias_*.zip", self.logger)
            inconsist_zip = self.inconsist_dir / f"emccamp_inconsistencias_{timestamp}.zip"
            inconsist_csv = f"emccamp_inconsistencias_{timestamp}.csv"
            self.io.write_zip({inconsist_csv: inconsistencias_df}, inconsist_zip)

        return ProcessorStats(
            registros_originais=registros_originais,
            registros_finais=len(df_valid),
            inconsistencias=len(inconsistencias_df),
            arquivo_saida=zip_path,
            arquivo_inconsistencias=inconsist_zip,
        )


def run(config_loader: ConfigLoader | None = None) -> ProcessorStats:
    from time import time
    
    loader = config_loader or ConfigLoader()
    config = loader.load()
    processor = EmccampProcessor(config)
    
    inicio = time()
    stats = processor.process()
    duracao = time() - inicio

    format_treatment_output(
        source="EMCCAMP",
        input_records=stats.registros_originais,
        output_records=stats.registros_finais,
        inconsistencies=stats.inconsistencias,
        output_file=stats.arquivo_saida,
        inconsistencies_file=stats.arquivo_inconsistencias,
        duration=duracao
    )

    return stats
