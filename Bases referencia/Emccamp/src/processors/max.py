"""Processing logic for the MAX dataset."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, List

import pandas as pd

from src.config.loader import ConfigLoader, LoadedConfig
from src.utils.io import DatasetIO
from src.utils.logger import get_logger
from src.utils.output_formatter import format_treatment_output
from src.utils.path_manager import PathManager


@dataclass
class MaxStats:
    registros_originais: int
    registros_finais: int
    inconsistencias: int
    arquivo_saida: Path
    arquivo_inconsistencias: Path | None


class MaxProcessor:
    """Treats the MAX dataset using the configured mappings."""

    def __init__(self, config: LoadedConfig) -> None:
        self.config = config
        self.mapping = self.config.get_mapping("max")

        self.paths = PathManager(config.base_path, config.data)

        logging_cfg = config.get("logging", {})
        logs_dir = self.paths.resolve_logs()
        self.logger = get_logger("max_processor", logs_dir, logging_cfg)

        global_cfg = config.get("global", {})
        self.encoding = global_cfg.get("encoding", "utf-8-sig")
        self.separator = global_cfg.get("csv_separator", ",")
        self.io = DatasetIO(separator=self.separator, encoding=self.encoding)

        self.input_dir = self.paths.resolve_input("base_max", "data/input/base_max")
        self.output_dir = self.paths.resolve_output("max_tratada", "max_tratada")
        self.inconsist_dir = self.paths.resolve_output("inconsistencias", "inconsistencias")

    def process(self) -> MaxStats:
        source_file = self._resolve_source_file()
        df_raw = self.io.read(source_file)

        df_norm = self._normalize(df_raw)
        df_valid, df_incons = self._validate(df_norm)

        PathManager.cleanup(self.output_dir, "max_tratada_*.zip", self.logger, silent=True)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        csv_name = f"max_tratada_{timestamp}.csv"
        zip_path = self.output_dir / f"max_tratada_{timestamp}.zip"
        self.io.write_zip({csv_name: df_valid}, zip_path)

        zip_incons = None
        if not df_incons.empty:
            PathManager.cleanup(self.inconsist_dir, "max_inconsistencias_*.zip", self.logger, silent=True)
            zip_incons = self.inconsist_dir / f"max_inconsistencias_{timestamp}.zip"
            csv_incons = f"max_inconsistencias_{timestamp}.csv"
            self.io.write_zip({csv_incons: df_incons}, zip_incons)

        
        return MaxStats(
            registros_originais=len(df_raw),
            registros_finais=len(df_valid),
            inconsistencias=len(df_incons),
            arquivo_saida=zip_path,
            arquivo_inconsistencias=zip_incons,
        )

    def _resolve_source_file(self) -> Path:
        candidates = sorted(self.input_dir.glob("*.zip"), key=lambda p: p.stat().st_mtime, reverse=True)
        candidates += sorted(self.input_dir.glob("*.csv"), key=lambda p: p.stat().st_mtime, reverse=True)
        if not candidates:
            raise FileNotFoundError(f"Nenhum arquivo MAX encontrado em {self.input_dir}")
        return candidates[0]

    def _normalize(self, df: pd.DataFrame) -> pd.DataFrame:
        rename_map: Dict[str, str] = self.mapping.get("rename", {})
        df_norm = df.rename(columns=rename_map).copy()

        if "NUMERO_CONTRATO" in df_norm.columns:
            df_norm["NUMERO_CONTRATO"] = df_norm["NUMERO_CONTRATO"].astype(str).str.strip()

        if "PARCELA" in df_norm.columns:
            df_norm["PARCELA"] = df_norm["PARCELA"].astype(str).str.strip()

        key_cfg = self.mapping.get("key", {})
        if key_cfg.get("use_parcela_as_chave") and "PARCELA" in df_norm.columns:
            df_norm["CHAVE"] = df_norm["PARCELA"].astype(str).str.strip()
        else:
            components: List[str] = key_cfg.get("components", ["NUMERO_CONTRATO", "PARCELA"])
            sep = key_cfg.get("sep", "")
            if all(column in df_norm.columns for column in components):
                df_norm["CHAVE"] = df_norm[components].astype(str).apply(
                    lambda row: sep.join(cell.strip() for cell in row), axis=1
                )

        if "DATA_VENCIMENTO" in df_norm.columns:
            df_norm["DATA_VENCIMENTO"] = pd.to_datetime(df_norm["DATA_VENCIMENTO"], errors="coerce")
        elif "VENCIMENTO" in df_norm.columns:
            df_norm["DATA_VENCIMENTO"] = pd.to_datetime(df_norm["VENCIMENTO"], errors="coerce")

        # VALOR mantém formato original (vírgulas como decimal) - não converter para numérico
        # if "VALOR" in df_norm.columns:
        #     df_norm["VALOR"] = pd.to_numeric(df_norm["VALOR"], errors="coerce")  # REMOVIDO

        preserve_fields = self.mapping.get("preserve", [])
        for field in preserve_fields:
            if field in df.columns and field not in df_norm.columns:
                df_norm[field] = df[field]

        return df_norm

    def _validate(self, df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
        required = set(self.mapping.get("required", []))
        if required and not required.issubset(df.columns):
            missing = required.difference(df.columns)
            raise ValueError(f"Colunas obrigatorias ausentes na base MAX: {missing}")

        if "CHAVE" not in df.columns:
            raise ValueError("Coluna CHAVE nao foi gerada na base MAX")

        # Valida PARCELA com regex estrito (exige hífen)
        inconsist_mask = pd.Series(False, index=df.index)
        
        # Valida PARCELA e CHAVE com regex estrito (exige hífen, bloqueia barras/pontos)
        validation_cfg = self.mapping.get("validation", {})
        parcela_regex = validation_cfg.get("parcela_regex", r'^[0-9]{3,}-[0-9]{2,}$')
        pattern = re.compile(parcela_regex)
        
        # Valida PARCELA
        if "PARCELA" in df.columns:
            parcela_str = df["PARCELA"].astype(str).str.strip()
            mask_vazia = parcela_str.eq('') | df["PARCELA"].isna()
            inconsist_mask |= mask_vazia
            
            mask_invalida = ~parcela_str.apply(lambda v: bool(pattern.match(v)) if v else False)
            inconsist_mask |= (mask_invalida & ~mask_vazia)
            
            # Valida duplicatas - marca todas as ocorrências duplicadas como inconsistência
            mask_duplicada = parcela_str.duplicated(keep=False) & ~mask_vazia
            inconsist_mask |= mask_duplicada
        
        # Valida CHAVE com o mesmo padrão (usada no batimento)
        if "CHAVE" in df.columns:
            chave_str = df["CHAVE"].astype(str).str.strip()
            mask_vazia = chave_str.eq('') | df["CHAVE"].isna()
            inconsist_mask |= mask_vazia
            
            mask_invalida = ~chave_str.apply(lambda v: bool(pattern.match(v)) if v else False)
            inconsist_mask |= (mask_invalida & ~mask_vazia)
            
            # Valida duplicatas em CHAVE também
            mask_duplicada = chave_str.duplicated(keep=False) & ~mask_vazia
            inconsist_mask |= mask_duplicada

        for campo in required:
            if campo in df.columns:
                inconsist_mask |= df[campo].astype(str).str.strip().eq("")

        inconsist = df[inconsist_mask].copy()
        validos = df[~inconsist_mask].copy()
        return validos, inconsist


def run(config_loader: ConfigLoader | None = None) -> MaxStats:
    from time import time
    
    loader = config_loader or ConfigLoader()
    config = loader.load()
    processor = MaxProcessor(config)
    
    inicio = time()
    stats = processor.process()
    duracao = time() - inicio

    format_treatment_output(
        source="MAX",
        input_records=stats.registros_originais,
        output_records=stats.registros_finais,
        inconsistencies=stats.inconsistencias,
        output_file=stats.arquivo_saida,
        inconsistencies_file=stats.arquivo_inconsistencias,
        duration=duracao
    )

    return stats
