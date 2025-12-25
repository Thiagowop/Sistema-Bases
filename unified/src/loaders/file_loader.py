"""
File loader.
Loads data from local files (CSV, Excel, ZIP).
"""
from __future__ import annotations

import zipfile
from pathlib import Path
from typing import TYPE_CHECKING

import pandas as pd

from ..core.base import BaseLoader, LoaderResult

if TYPE_CHECKING:
    from ..core.schemas import ClientConfig, LoaderConfig


class FileLoader(BaseLoader):
    """Loads data from local files."""

    @property
    def name(self) -> str:
        return "file"

    def load(self) -> LoaderResult:
        file_path = self.params.get("path")
        if not file_path:
            return LoaderResult(
                data=pd.DataFrame(),
                metadata={"error": "No file path specified"},
            )

        path = Path(file_path)

        # Handle glob patterns
        if "*" in str(path):
            parent = path.parent
            pattern = path.name
            if parent.exists():
                matches = list(parent.glob(pattern))
                if matches:
                    # Get most recent file
                    path = max(matches, key=lambda p: p.stat().st_mtime)
                else:
                    return LoaderResult(
                        data=pd.DataFrame(),
                        metadata={"error": f"No files matching pattern: {file_path}"},
                    )

        if not path.exists():
            return LoaderResult(
                data=pd.DataFrame(),
                metadata={"error": f"File not found: {path}"},
            )

        # Load based on file type
        suffix = path.suffix.lower()
        encoding = self.params.get("encoding", "utf-8-sig")
        separator = self.params.get("separator", ";")
        sheet_name = self.params.get("sheet_name", 0)

        try:
            if suffix == ".zip":
                df = self._load_from_zip(path, encoding, separator, sheet_name)
            elif suffix == ".csv":
                df = pd.read_csv(
                    path,
                    sep=separator,
                    encoding=encoding,
                    dtype=str,
                    low_memory=False,
                )
            elif suffix in (".xlsx", ".xls"):
                df = pd.read_excel(path, sheet_name=sheet_name, dtype=str)
            else:
                return LoaderResult(
                    data=pd.DataFrame(),
                    metadata={"error": f"Unsupported file type: {suffix}"},
                )

            # Normalize column names
            df.columns = [str(c).strip().upper() for c in df.columns]

            return LoaderResult(
                data=df,
                metadata={
                    "rows": len(df),
                    "columns": list(df.columns),
                    "source": str(path),
                },
                source_path=path,
            )

        except Exception as e:
            return LoaderResult(
                data=pd.DataFrame(),
                metadata={"error": f"Failed to load file: {e}"},
            )

    def _load_from_zip(
        self,
        zip_path: Path,
        encoding: str,
        separator: str,
        sheet_name: int | str,
    ) -> pd.DataFrame:
        """Load data from a ZIP file."""
        with zipfile.ZipFile(zip_path, "r") as zf:
            # Find data file in ZIP
            for name in zf.namelist():
                lower = name.lower()
                if lower.endswith(".csv"):
                    with zf.open(name) as f:
                        return pd.read_csv(
                            f,
                            sep=separator,
                            encoding=encoding,
                            dtype=str,
                            low_memory=False,
                        )
                elif lower.endswith((".xlsx", ".xls")):
                    with zf.open(name) as f:
                        return pd.read_excel(f, sheet_name=sheet_name, dtype=str)

        raise ValueError(f"No CSV or Excel file found in ZIP: {zip_path}")


def create_file_loader(config: LoaderConfig, client_config: ClientConfig) -> FileLoader:
    """Factory function to create a FileLoader."""
    return FileLoader(config, client_config)
