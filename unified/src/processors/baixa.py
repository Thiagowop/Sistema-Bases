"""
Baixa processor.
Handles records that need to be removed/discharged from MAX.
"""
from __future__ import annotations

import zipfile
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

from ..core.base import BaseProcessor, ProcessorResult
from ..core.schemas import ClientConfig
from ..splitters import create_splitter
from ..core.schemas import SplitterConfig, SplitterType


class BaixaProcessor(BaseProcessor):
    """Processor for generating baixa (discharge) files."""

    @property
    def name(self) -> str:
        return "baixa"

    def process(
        self,
        client_data: pd.DataFrame | None,
        max_data: pd.DataFrame | None,
        context: dict[str, Any],
    ) -> ProcessorResult:
        errors = []
        output_files = []
        output_dir = context.get("output_dir", Path.cwd())

        # Get baixas data from context or compute
        baixas = context.get("baixas")
        if baixas is None:
            # Compute baixas (B - A): MAX records not in client
            if max_data is not None and client_data is not None:
                client_key = self.params.get("client_key", "CHAVE")
                max_key = self.params.get("max_key", "CHAVE")

                if client_key in client_data.columns and max_key in max_data.columns:
                    client_keys = set(
                        client_data[client_key].dropna().astype(str).str.strip()
                    )
                    max_keys_col = max_data[max_key].astype(str).str.strip()
                    baixas = max_data[~max_keys_col.isin(client_keys)].copy()
                else:
                    baixas = pd.DataFrame()
            else:
                baixas = pd.DataFrame()

        if baixas.empty:
            return ProcessorResult(
                data=client_data if client_data is not None else pd.DataFrame(),
                metadata={"baixas_count": 0},
                output_files=[],
                errors=errors,
            )

        # Apply splitters if configured
        splitter_configs = self.params.get("splitters", [])
        if splitter_configs:
            splits = {"all": baixas}
            for splitter_dict in splitter_configs:
                splitter_config = SplitterConfig(
                    type=SplitterType(splitter_dict.get("type", "field_value")),
                    enabled=splitter_dict.get("enabled", True),
                    params=splitter_dict.get("params", {}),
                )
                splitter = create_splitter(splitter_config)

                new_splits = {}
                for name, df in splits.items():
                    if df.empty:
                        continue
                    result = splitter.split(df)
                    for split_name, split_df in result.splits.items():
                        key = f"{name}_{split_name}" if name != "all" else split_name
                        new_splits[key] = split_df
                splits = new_splits
        else:
            splits = {"baixas": baixas}

        # Export each split
        encoding = self.params.get("encoding", "utf-8-sig")
        separator = self.params.get("separator", ";")
        export_format = self.params.get("export_format", "zip")
        add_timestamp = self.params.get("add_timestamp", True)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S") if add_timestamp else ""

        for name, df in splits.items():
            if df.empty:
                continue

            # Select columns to export
            export_columns = self.params.get("export_columns", [])
            if export_columns:
                existing_cols = [c for c in export_columns if c in df.columns]
                df = df[existing_cols]

            # Generate filename
            filename_base = f"baixa_{name}"
            if timestamp:
                filename_base = f"{filename_base}_{timestamp}"

            if export_format == "zip":
                output_path = output_dir / f"{filename_base}.zip"
                csv_path = output_dir / f"{filename_base}.csv"
                df.to_csv(csv_path, index=False, encoding=encoding, sep=separator)
                with zipfile.ZipFile(output_path, "w", zipfile.ZIP_DEFLATED) as zf:
                    zf.write(csv_path, f"{filename_base}.csv")
                csv_path.unlink()
            else:
                output_path = output_dir / f"{filename_base}.csv"
                df.to_csv(output_path, index=False, encoding=encoding, sep=separator)

            output_files.append(output_path)

        # Build metadata
        metadata = {
            "baixas_count": len(baixas),
            "splits": {name: len(df) for name, df in splits.items()},
        }

        return ProcessorResult(
            data=client_data if client_data is not None else pd.DataFrame(),
            metadata=metadata,
            output_files=output_files,
            errors=errors,
        )


def create_baixa_processor(
    config: ClientConfig, params: dict[str, Any]
) -> BaixaProcessor:
    """Factory function to create a BaixaProcessor."""
    return BaixaProcessor(config, params)
