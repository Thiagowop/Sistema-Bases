"""
Enriquecimento processor.
Enriches data by adding additional information from external sources.
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


class EnriquecimentoProcessor(BaseProcessor):
    """Processor for data enrichment."""

    @property
    def name(self) -> str:
        return "enriquecimento"

    def process(
        self,
        client_data: pd.DataFrame | None,
        max_data: pd.DataFrame | None,
        context: dict[str, Any],
    ) -> ProcessorResult:
        errors = []
        output_files = []
        output_dir = context.get("output_dir", Path.cwd())

        # Get novos (new records to enrich)
        novos = context.get("novos")
        if novos is None:
            novos = client_data if client_data is not None else pd.DataFrame()

        if novos.empty:
            return ProcessorResult(
                data=pd.DataFrame(),
                metadata={"enriquecimento_count": 0},
                output_files=[],
                errors=errors,
            )

        # Apply enrichment logic
        novos = self._enrich_data(novos)

        # Apply splitters if configured
        splitter_configs = self.params.get("splitters", [])
        if splitter_configs:
            splits = {"all": novos}
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
            splits = {"novos_enriquecidos": novos}

        # Export configuration
        encoding = self.params.get("encoding", "utf-8-sig")
        separator = self.params.get("separator", ";")
        export_format = self.params.get("export_format", "zip")
        add_timestamp = self.params.get("add_timestamp", True)
        filename_prefix = self.params.get("filename_prefix", "enriquecimento")

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S") if add_timestamp else ""

        # Column selection
        select_columns = self.params.get("select_columns", [])
        output_columns = self.params.get("output_columns", {})

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

            # Generate filename
            filename_base = f"{filename_prefix}_{name}"
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
            "enriquecimento_count": len(novos),
            "splits": {name: len(df) for name, df in splits.items()},
        }

        return ProcessorResult(
            data=novos,
            metadata=metadata,
            output_files=output_files,
            errors=errors,
        )

    def _enrich_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply enrichment rules to the data."""
        df = df.copy()

        # Add computed fields if specified
        computed_fields = self.params.get("computed_fields", {})
        for field_name, config in computed_fields.items():
            field_type = config.get("type", "constant")

            if field_type == "constant":
                df[field_name] = config.get("value", "")
            elif field_type == "date":
                date_format = config.get("format", "%d/%m/%Y")
                df[field_name] = datetime.now().strftime(date_format)
            elif field_type == "concat":
                columns = config.get("columns", [])
                separator = config.get("separator", " ")
                existing = [c for c in columns if c in df.columns]
                if existing:
                    df[field_name] = df[existing].astype(str).agg(separator.join, axis=1)
            elif field_type == "lookup":
                # Simple lookup from source column to target value
                source_col = config.get("source_column")
                mapping = config.get("mapping", {})
                default = config.get("default", "")
                if source_col in df.columns:
                    df[field_name] = df[source_col].map(mapping).fillna(default)

        # Apply filters for MAX system requirements
        max_filters = self.params.get("max_filters", {})

        # CPF/CNPJ formatting for MAX
        cpf_column = max_filters.get("cpf_column", "CPF_CNPJ")
        if cpf_column in df.columns and max_filters.get("format_cpf", False):
            df[cpf_column] = df[cpf_column].apply(self._format_cpf_cnpj)

        # Phone formatting
        phone_column = max_filters.get("phone_column")
        if phone_column and phone_column in df.columns:
            df[phone_column] = df[phone_column].apply(self._format_phone)

        # Value formatting
        value_column = max_filters.get("value_column")
        if value_column and value_column in df.columns:
            decimal_places = max_filters.get("decimal_places", 2)
            df[value_column] = df[value_column].apply(
                lambda x: f"{float(x or 0):.{decimal_places}f}"
            )

        return df

    def _format_cpf_cnpj(self, value: Any) -> str:
        """Format CPF/CNPJ with punctuation."""
        if pd.isna(value):
            return ""
        digits = "".join(filter(str.isdigit, str(value)))
        if len(digits) == 11:  # CPF
            return f"{digits[:3]}.{digits[3:6]}.{digits[6:9]}-{digits[9:]}"
        elif len(digits) == 14:  # CNPJ
            return f"{digits[:2]}.{digits[2:5]}.{digits[5:8]}/{digits[8:12]}-{digits[12:]}"
        return digits

    def _format_phone(self, value: Any) -> str:
        """Format phone number."""
        if pd.isna(value):
            return ""
        digits = "".join(filter(str.isdigit, str(value)))
        if len(digits) == 11:  # Celular
            return f"({digits[:2]}) {digits[2:7]}-{digits[7:]}"
        elif len(digits) == 10:  # Fixo
            return f"({digits[:2]}) {digits[2:6]}-{digits[6:]}"
        return digits


def create_enriquecimento_processor(
    config: ClientConfig, params: dict[str, Any]
) -> EnriquecimentoProcessor:
    """Factory function to create an EnriquecimentoProcessor."""
    return EnriquecimentoProcessor(config, params)
