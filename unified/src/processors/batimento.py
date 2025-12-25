"""
Batimento processor.
Performs matching operations between client and MAX data (anti-join/PROCV).
"""
from __future__ import annotations

import zipfile
from pathlib import Path
from typing import Any

import pandas as pd

from ..core.base import BaseProcessor, ProcessorResult
from ..core.schemas import ClientConfig


class BatimentoProcessor(BaseProcessor):
    """Processor for data matching (batimento/anti-join)."""

    @property
    def name(self) -> str:
        return "batimento"

    def process(
        self,
        client_data: pd.DataFrame | None,
        max_data: pd.DataFrame | None,
        context: dict[str, Any],
    ) -> ProcessorResult:
        errors = []
        output_files = []

        if client_data is None or client_data.empty:
            errors.append("No client data for batimento")
            return ProcessorResult(
                data=pd.DataFrame(),
                metadata={},
                output_files=[],
                errors=errors,
            )

        if max_data is None or max_data.empty:
            errors.append("No MAX data for batimento")
            return ProcessorResult(
                data=client_data,
                metadata={},
                output_files=[],
                errors=errors,
            )

        # Get key columns
        client_key = self.params.get("client_key", "CHAVE")
        max_key = self.params.get("max_key", "CHAVE")
        output_dir = context.get("output_dir", Path.cwd())

        # Ensure key columns exist
        if client_key not in client_data.columns:
            errors.append(f"Client key column '{client_key}' not found")
            return ProcessorResult(
                data=client_data,
                metadata={},
                output_files=[],
                errors=errors,
            )

        if max_key not in max_data.columns:
            errors.append(f"MAX key column '{max_key}' not found")
            return ProcessorResult(
                data=client_data,
                metadata={},
                output_files=[],
                errors=errors,
            )

        # Get sets of keys
        client_keys = set(client_data[client_key].dropna().astype(str).str.strip())
        max_keys = set(max_data[max_key].dropna().astype(str).str.strip())

        # Perform matching operations
        results = {}

        # A - B (Client records not in MAX) - NOVOS
        if self.params.get("compute_a_minus_b", True):
            a_minus_b_keys = client_keys - max_keys
            a_minus_b = client_data[
                client_data[client_key].astype(str).str.strip().isin(a_minus_b_keys)
            ].copy()
            results["novos"] = a_minus_b

        # B - A (MAX records not in Client) - BAIXAS
        if self.params.get("compute_b_minus_a", True):
            b_minus_a_keys = max_keys - client_keys
            b_minus_a = max_data[
                max_data[max_key].astype(str).str.strip().isin(b_minus_a_keys)
            ].copy()
            results["baixas"] = b_minus_a

        # A ∩ B (Records in both) - MANTIDOS
        if self.params.get("compute_intersection", False):
            intersection_keys = client_keys & max_keys
            intersection = client_data[
                client_data[client_key].astype(str).str.strip().isin(intersection_keys)
            ].copy()
            results["mantidos"] = intersection

        # Export results
        export_format = self.params.get("export_format", "csv")
        encoding = self.params.get("encoding", "utf-8-sig")
        separator = self.params.get("separator", ";")

        for name, df in results.items():
            if df.empty:
                continue

            if export_format == "zip":
                output_path = output_dir / f"{name}.zip"
                csv_path = output_dir / f"{name}.csv"
                df.to_csv(csv_path, index=False, encoding=encoding, sep=separator)
                with zipfile.ZipFile(output_path, "w", zipfile.ZIP_DEFLATED) as zf:
                    zf.write(csv_path, f"{name}.csv")
                csv_path.unlink()  # Remove temporary CSV
            else:
                output_path = output_dir / f"{name}.csv"
                df.to_csv(output_path, index=False, encoding=encoding, sep=separator)

            output_files.append(output_path)

        # Build metadata
        metadata = {
            "client_records": len(client_data),
            "max_records": len(max_data),
            "client_unique_keys": len(client_keys),
            "max_unique_keys": len(max_keys),
        }

        for name, df in results.items():
            metadata[f"{name}_count"] = len(df)

        # Return the "novos" (new records) as the main output
        main_output = results.get("novos", client_data)

        return ProcessorResult(
            data=main_output,
            metadata=metadata,
            output_files=output_files,
            errors=errors,
        )


class EnhancedBatimentoProcessor(BatimentoProcessor):
    """
    Enhanced batimento with support for:
    - Value comparison (saldo updates)
    - Multiple key matching
    - Enrichment during matching
    """

    @property
    def name(self) -> str:
        return "enhanced_batimento"

    def process(
        self,
        client_data: pd.DataFrame | None,
        max_data: pd.DataFrame | None,
        context: dict[str, Any],
    ) -> ProcessorResult:
        # First run basic batimento
        result = super().process(client_data, max_data, context)

        if self.params.get("compare_values", False) and not result.data.empty:
            result = self._compare_values(client_data, max_data, result, context)

        return result

    def _compare_values(
        self,
        client_data: pd.DataFrame,
        max_data: pd.DataFrame,
        result: ProcessorResult,
        context: dict[str, Any],
    ) -> ProcessorResult:
        """Compare values between matched records."""
        client_key = self.params.get("client_key", "CHAVE")
        max_key = self.params.get("max_key", "CHAVE")
        value_column = self.params.get("value_column", "SALDO")

        if value_column not in client_data.columns or value_column not in max_data.columns:
            return result

        # Get intersection keys
        client_keys = set(client_data[client_key].dropna().astype(str).str.strip())
        max_keys = set(max_data[max_key].dropna().astype(str).str.strip())
        common_keys = client_keys & max_keys

        # Compare values for common records
        updates = []
        for key in common_keys:
            client_row = client_data[
                client_data[client_key].astype(str).str.strip() == key
            ].iloc[0]
            max_row = max_data[
                max_data[max_key].astype(str).str.strip() == key
            ].iloc[0]

            client_value = float(client_row.get(value_column, 0) or 0)
            max_value = float(max_row.get(value_column, 0) or 0)

            if abs(client_value - max_value) > 0.01:  # Value changed
                updates.append({
                    "CHAVE": key,
                    "VALOR_CLIENTE": client_value,
                    "VALOR_MAX": max_value,
                    "DIFERENCA": client_value - max_value,
                })

        if updates:
            updates_df = pd.DataFrame(updates)
            output_dir = context.get("output_dir", Path.cwd())
            updates_path = output_dir / "atualizacoes_saldo.csv"
            updates_df.to_csv(updates_path, index=False, encoding="utf-8-sig", sep=";")
            result.output_files.append(updates_path)
            result.metadata["value_updates"] = len(updates)

        return result


def create_batimento_processor(
    config: ClientConfig, params: dict[str, Any]
) -> BatimentoProcessor:
    """Factory function to create a BatimentoProcessor."""
    if params.get("enhanced", False):
        return EnhancedBatimentoProcessor(config, params)
    return BatimentoProcessor(config, params)
