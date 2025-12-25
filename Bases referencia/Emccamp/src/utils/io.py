from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Tuple

import pandas as pd


def ensure_directory(path: Path) -> Path:
    """Create directory hierarchy if needed and return the path."""
    path.mkdir(parents=True, exist_ok=True)
    return path


def read_csv_or_zip(path: Path, sep: str = ',', encoding: str = 'utf-8-sig') -> pd.DataFrame:
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(path)

    if path.suffix.lower() == '.zip':
        import zipfile
        with zipfile.ZipFile(path) as zf:
            members = zf.namelist()
            if not members:
                raise ValueError(f"ZIP vazio: {path}")
            with zf.open(members[0]) as buffer:
                return pd.read_csv(buffer, sep=sep, encoding=encoding, dtype=str)
    return pd.read_csv(path, sep=sep, encoding=encoding, dtype=str)


def write_csv_to_zip(
    dataframes: Dict[str, pd.DataFrame],
    zip_path: Path,
    sep: str = ',',
    encoding: str = 'utf-8-sig',
) -> Path:
    import io
    import zipfile

    zip_path = Path(zip_path)
    zip_path.parent.mkdir(parents=True, exist_ok=True)

    with zipfile.ZipFile(zip_path, 'w', compression=zipfile.ZIP_DEFLATED) as zf:
        for name, df in dataframes.items():
            buffer = io.StringIO()
            # Usa vírgula como separador decimal em todos os CSVs
            df.to_csv(buffer, index=False, sep=sep, decimal=',')
            zf.writestr(name, buffer.getvalue().encode(encoding))
    return zip_path


@dataclass(slots=True)
class DatasetIO:
    """High-level helpers for reading and writing project datasets."""

    separator: str
    encoding: str

    def read(self, path: Path) -> pd.DataFrame:
        return read_csv_or_zip(path, sep=self.separator, encoding=self.encoding)

    def write_zip(self, frames: Dict[str, pd.DataFrame], path: Path) -> Path:
        return write_csv_to_zip(frames, path, sep=self.separator, encoding=self.encoding)

    def split_by_mask(
        self,
        df: pd.DataFrame,
        mask: pd.Series,
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Return (match, no_match) copies given a boolean mask."""
        return df[mask].copy(), df[~mask].copy()

    @staticmethod
    def latest_file(directory: Path, pattern: str) -> Path:
        candidates = sorted(directory.glob(pattern), key=lambda item: item.stat().st_mtime, reverse=True)
        if not candidates:
            raise FileNotFoundError(f"Nenhum arquivo correspondente a {pattern} em {directory}")
        return candidates[0]
