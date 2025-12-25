"""
Blacklist validator.
Filters records based on blacklist files (e.g., judicial clients, blocked CPFs).
"""
from __future__ import annotations

import zipfile
from pathlib import Path

import pandas as pd

from ..core.base import BaseValidator, ValidationResult
from ..core.schemas import ValidatorConfig


class BlacklistValidator(BaseValidator):
    """Filters records based on blacklist files."""

    @property
    def name(self) -> str:
        return "blacklist"

    def validate(self, df: pd.DataFrame) -> ValidationResult:
        if not self.enabled or df.empty:
            return ValidationResult(valid=df, invalid=pd.DataFrame(), errors=[])

        # Get configuration
        source_path = self.params.get("source_path")
        source_column = self.params.get("source_column", "CPF_CNPJ")
        target_column = self.params.get("target_column", "CPF_CNPJ")
        mode = self.params.get("mode", "exclude")  # exclude or include

        if not source_path:
            return ValidationResult(
                valid=df,
                invalid=pd.DataFrame(),
                errors=["Blacklist source_path not configured"],
            )

        # Load blacklist
        blacklist_values = self._load_blacklist(source_path, source_column)
        if blacklist_values is None:
            return ValidationResult(
                valid=df,
                invalid=pd.DataFrame(),
                errors=[f"Could not load blacklist from {source_path}"],
            )

        if target_column not in df.columns:
            return ValidationResult(
                valid=df,
                invalid=pd.DataFrame(),
                errors=[f"Target column '{target_column}' not found"],
            )

        # Normalize values for comparison
        df_values = df[target_column].astype(str).str.strip().str.upper()
        blacklist_set = {str(v).strip().upper() for v in blacklist_values}

        # Apply filter
        in_blacklist = df_values.isin(blacklist_set)
        errors = []

        if mode == "exclude":
            # Exclude records in blacklist
            valid_mask = ~in_blacklist
            errors.append(f"{in_blacklist.sum()} records excluded by blacklist")
        else:
            # Include only records in blacklist (whitelist mode)
            valid_mask = in_blacklist
            errors.append(f"{(~in_blacklist).sum()} records excluded (not in whitelist)")

        return ValidationResult(
            valid=df[valid_mask].copy(),
            invalid=df[~valid_mask].copy(),
            errors=errors,
        )

    def _load_blacklist(self, source_path: str, column: str) -> set | None:
        """Load blacklist values from file (CSV, ZIP, or Excel)."""
        path = Path(source_path)

        if not path.exists():
            # Try glob pattern
            parent = path.parent
            if parent.exists():
                matches = list(parent.glob(path.name))
                if matches:
                    path = max(matches, key=lambda p: p.stat().st_mtime)
                else:
                    return None
            else:
                return None

        try:
            if path.suffix.lower() == ".zip":
                return self._load_from_zip(path, column)
            elif path.suffix.lower() == ".csv":
                df = pd.read_csv(path, sep=";", encoding="utf-8-sig", dtype=str)
                return self._extract_column(df, column)
            elif path.suffix.lower() in (".xlsx", ".xls"):
                df = pd.read_excel(path, dtype=str)
                return self._extract_column(df, column)
            else:
                return None
        except Exception:
            return None

    def _load_from_zip(self, zip_path: Path, column: str) -> set | None:
        """Load data from a ZIP file containing CSV/Excel."""
        try:
            with zipfile.ZipFile(zip_path, "r") as zf:
                # Find the first CSV or Excel file
                for name in zf.namelist():
                    lower = name.lower()
                    if lower.endswith(".csv"):
                        with zf.open(name) as f:
                            df = pd.read_csv(f, sep=";", encoding="utf-8-sig", dtype=str)
                            return self._extract_column(df, column)
                    elif lower.endswith((".xlsx", ".xls")):
                        with zf.open(name) as f:
                            df = pd.read_excel(f, dtype=str)
                            return self._extract_column(df, column)
            return None
        except Exception:
            return None

    def _extract_column(self, df: pd.DataFrame, column: str) -> set:
        """Extract values from a column, handling different column name formats."""
        # Normalize column names
        df.columns = [str(c).strip().upper() for c in df.columns]
        column_upper = column.upper()

        if column_upper in df.columns:
            return set(df[column_upper].dropna().astype(str).str.strip())

        # Try common variations
        variations = [
            column_upper.replace("_", ""),
            column_upper.replace("_", " "),
            "CPF",
            "CNPJ",
            "CPFCNPJ",
            "CPF_CNPJ",
            "DOCUMENTO",
        ]
        for var in variations:
            if var in df.columns:
                return set(df[var].dropna().astype(str).str.strip())

        # If no matching column, return first column values
        if len(df.columns) > 0:
            return set(df.iloc[:, 0].dropna().astype(str).str.strip())

        return set()


def create_blacklist_validator(config: ValidatorConfig) -> BlacklistValidator:
    """Factory function to create a BlacklistValidator."""
    return BlacklistValidator(config)
