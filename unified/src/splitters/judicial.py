"""
Judicial splitter.
Splits data into judicial and extrajudicial records based on blacklist.
"""
from __future__ import annotations

import zipfile
from pathlib import Path

import pandas as pd

from ..core.base import BaseSplitter, SplitResult
from ..core.schemas import SplitterConfig


class JudicialSplitter(BaseSplitter):
    """Splits records into judicial and extrajudicial based on blacklist file."""

    @property
    def name(self) -> str:
        return "judicial"

    def split(self, df: pd.DataFrame) -> SplitResult:
        if not self.enabled or df.empty:
            return SplitResult(splits={"extrajudicial": df})

        # Get configuration
        source_path = self.params.get("source_path")
        source_column = self.params.get("source_column", "CPF_CNPJ")
        target_column = self.params.get("target_column", "CPF_CNPJ")
        judicial_name = self.params.get("judicial_name", "judicial")
        extrajudicial_name = self.params.get("extrajudicial_name", "extrajudicial")

        if not source_path:
            return SplitResult(splits={extrajudicial_name: df})

        # Load judicial list
        judicial_values = self._load_judicial_list(source_path, source_column)
        if judicial_values is None or len(judicial_values) == 0:
            return SplitResult(splits={extrajudicial_name: df})

        if target_column not in df.columns:
            return SplitResult(splits={extrajudicial_name: df})

        # Normalize values for comparison
        df_values = df[target_column].astype(str).str.strip().str.upper()
        judicial_set = {str(v).strip().upper() for v in judicial_values}

        # Split
        is_judicial = df_values.isin(judicial_set)

        return SplitResult(splits={
            judicial_name: df[is_judicial].copy(),
            extrajudicial_name: df[~is_judicial].copy(),
        })

    def _load_judicial_list(self, source_path: str, column: str) -> set | None:
        """Load judicial CPF/CNPJ list from file."""
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
        """Load data from a ZIP file."""
        try:
            with zipfile.ZipFile(zip_path, "r") as zf:
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
        """Extract values from a column."""
        df.columns = [str(c).strip().upper() for c in df.columns]
        column_upper = column.upper()

        if column_upper in df.columns:
            return set(df[column_upper].dropna().astype(str).str.strip())

        # Try common variations
        for var in ["CPF", "CNPJ", "CPFCNPJ", "CPF_CNPJ", "DOCUMENTO"]:
            if var in df.columns:
                return set(df[var].dropna().astype(str).str.strip())

        # Return first column if no match
        if len(df.columns) > 0:
            return set(df.iloc[:, 0].dropna().astype(str).str.strip())

        return set()


def create_judicial_splitter(config: SplitterConfig) -> JudicialSplitter:
    """Factory function to create a JudicialSplitter."""
    return JudicialSplitter(config)
