"""
Tratamento processor.
Cleans, normalizes, and transforms raw data.
"""
from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import pandas as pd

from ..core.base import BaseProcessor, ProcessorResult
from ..core.schemas import ClientConfig


class TratamentoProcessor(BaseProcessor):
    """Processor for data treatment/cleaning."""

    @property
    def name(self) -> str:
        return "tratamento"

    def process(
        self,
        client_data: pd.DataFrame | None,
        max_data: pd.DataFrame | None,
        context: dict[str, Any],
    ) -> ProcessorResult:
        errors = []
        output_files = []

        # Process client data
        if client_data is not None and not client_data.empty:
            client_data = self._treat_data(client_data, "client")

        # Process max data
        if max_data is not None and not max_data.empty:
            max_data = self._treat_data(max_data, "max")

        return ProcessorResult(
            data=client_data if client_data is not None else pd.DataFrame(),
            metadata={"treated": True},
            output_files=output_files,
            errors=errors,
        )

    def _treat_data(self, df: pd.DataFrame, source: str) -> pd.DataFrame:
        """Apply treatment rules to dataframe."""
        df = df.copy()

        # Apply column mappings if specified
        column_mappings = self.params.get("column_mappings", {})
        if column_mappings:
            df = df.rename(columns=column_mappings)

        # Normalize text columns
        text_columns = self.params.get("text_columns", [])
        for col in text_columns:
            if col in df.columns:
                df[col] = df[col].astype(str).str.strip().str.upper()

        # Clean CPF/CNPJ columns
        cpf_columns = self.params.get("cpf_columns", ["CPF_CNPJ", "CPF", "CNPJ"])
        for col in cpf_columns:
            if col in df.columns:
                df[col] = df[col].apply(self._clean_cpf_cnpj)

        # Clean phone columns
        phone_columns = self.params.get("phone_columns", ["TELEFONE", "CELULAR", "FONE"])
        for col in phone_columns:
            if col in df.columns:
                df[col] = df[col].apply(self._clean_phone)

        # Clean value columns (monetary)
        value_columns = self.params.get("value_columns", ["VALOR", "SALDO", "VALOR_ORIGINAL"])
        for col in value_columns:
            if col in df.columns:
                df[col] = df[col].apply(self._clean_value)

        # Clean date columns
        date_columns = self.params.get("date_columns", ["VENCIMENTO", "DATA", "DT_NASCIMENTO"])
        for col in date_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors="coerce", dayfirst=True)

        # Remove duplicates based on key column
        key_column = self.params.get("key_column", "CHAVE")
        if key_column in df.columns and self.params.get("remove_duplicates", False):
            df = df.drop_duplicates(subset=[key_column], keep="first")

        # Drop empty rows
        if self.params.get("drop_empty", False):
            df = df.dropna(how="all")

        return df

    def _clean_cpf_cnpj(self, value: Any) -> str:
        """Clean CPF/CNPJ, keeping only digits."""
        if pd.isna(value):
            return ""
        cleaned = re.sub(r"\D", "", str(value))
        # Pad with zeros if needed
        if len(cleaned) == 11:  # CPF
            return cleaned.zfill(11)
        elif len(cleaned) == 14:  # CNPJ
            return cleaned.zfill(14)
        return cleaned

    def _clean_phone(self, value: Any) -> str:
        """Clean phone number, keeping only digits."""
        if pd.isna(value):
            return ""
        cleaned = re.sub(r"\D", "", str(value))
        # Remove leading zeros and country code
        if cleaned.startswith("55") and len(cleaned) > 11:
            cleaned = cleaned[2:]
        if cleaned.startswith("0"):
            cleaned = cleaned[1:]
        return cleaned

    def _clean_value(self, value: Any) -> float:
        """Clean monetary value."""
        if pd.isna(value):
            return 0.0
        if isinstance(value, (int, float)):
            return float(value)
        # Handle Brazilian format (1.234,56)
        cleaned = str(value).strip()
        cleaned = cleaned.replace("R$", "").strip()
        cleaned = cleaned.replace(" ", "")
        # Check if it's Brazilian format (comma as decimal)
        if "," in cleaned and "." in cleaned:
            cleaned = cleaned.replace(".", "").replace(",", ".")
        elif "," in cleaned:
            cleaned = cleaned.replace(",", ".")
        try:
            return float(cleaned)
        except ValueError:
            return 0.0


def create_tratamento_processor(
    config: ClientConfig, params: dict[str, Any]
) -> TratamentoProcessor:
    """Factory function to create a TratamentoProcessor."""
    return TratamentoProcessor(config, params)
