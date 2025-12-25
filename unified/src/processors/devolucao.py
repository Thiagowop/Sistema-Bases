"""
Devolucao processor.
Handles records that need to be returned/devolved.
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


class DevolucaoProcessor(BaseProcessor):
    """Processor for generating devolucao (return) files."""

    @property
    def name(self) -> str:
        return "devolucao"

    def process(
        self,
        client_data: pd.DataFrame | None,
        max_data: pd.DataFrame | None,
        context: dict[str, Any],
    ) -> ProcessorResult:
        errors = []
        output_files = []
        output_dir = context.get("output_dir", Path.cwd())

        # Get baixas data (records to return)
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
                metadata={"devolucao_count": 0},
                output_files=[],
                errors=errors,
            )

        # Apply judicial splitter to separate judicial from extrajudicial
        judicial_source = self.params.get("judicial_source")
        if judicial_source:
            splitter_config = SplitterConfig(
                type=SplitterType.JUDICIAL,
                enabled=True,
                params={
                    "source_path": judicial_source,
                    "source_column": self.params.get("judicial_column", "CPF_CNPJ"),
                    "target_column": self.params.get("target_column", "CPF_CNPJ"),
                    "judicial_name": "judicial",
                    "extrajudicial_name": "extrajudicial",
                },
            )
            splitter = create_splitter(splitter_config)
            result = splitter.split(baixas)
            splits = result.splits
        else:
            splits = {"extrajudicial": baixas}

        # Export configuration
        encoding = self.params.get("encoding", "utf-8-sig")
        separator = self.params.get("separator", ";")
        export_format = self.params.get("export_format", "zip")
        add_timestamp = self.params.get("add_timestamp", True)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S") if add_timestamp else ""

        # Column mappings for output
        output_columns = self.params.get("output_columns", {})
        select_columns = self.params.get("select_columns", [])

        for name, df in splits.items():
            if df.empty:
                continue

            # Apply column mappings
            if output_columns:
                df = df.rename(columns=output_columns)

            # Select specific columns
            if select_columns:
                existing_cols = [c for c in select_columns if c in df.columns]
                df = df[existing_cols]

            # Add metadata columns if specified
            if self.params.get("add_motivo", False):
                motivo = "JUDICIAL" if name == "judicial" else "EXTRAJUDICIAL"
                df["MOTIVO_DEVOLUCAO"] = motivo

            if self.params.get("add_data", False):
                df["DATA_DEVOLUCAO"] = datetime.now().strftime("%d/%m/%Y")

            # Generate filename
            filename_base = f"devolucao_{name}"
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
            "devolucao_count": len(baixas),
            "splits": {name: len(df) for name, df in splits.items()},
        }

        return ProcessorResult(
            data=client_data if client_data is not None else pd.DataFrame(),
            metadata=metadata,
            output_files=output_files,
            errors=errors,
        )


def create_devolucao_processor(
    config: ClientConfig, params: dict[str, Any]
) -> DevolucaoProcessor:
    """Factory function to create a DevolucaoProcessor."""
    return DevolucaoProcessor(config, params)
