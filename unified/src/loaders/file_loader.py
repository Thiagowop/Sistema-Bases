"""
File loader.
Loads data from local files (CSV, Excel, ZIP).
Supports password-protected ZIP files (7-Zip/AES encryption).
"""
from __future__ import annotations

import io
import subprocess
import tempfile
import zipfile
from pathlib import Path
from typing import TYPE_CHECKING

import pandas as pd

from ..core.base import BaseLoader, LoaderResult

if TYPE_CHECKING:
    from ..core.schemas import ClientConfig, LoaderConfig


class FileLoader(BaseLoader):
    """Loads data from local files, including password-protected ZIPs."""

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
        password = self.params.get("password")

        try:
            if suffix == ".zip":
                df = self._load_from_zip(path, encoding, separator, sheet_name, password)
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
        password: str | None = None,
    ) -> pd.DataFrame:
        """Load data from a ZIP file, with optional password support."""

        # Try password-protected extraction if password provided
        if password:
            return self._load_password_protected_zip(
                zip_path, encoding, separator, sheet_name, password
            )

        # Standard ZIP extraction
        try:
            with zipfile.ZipFile(zip_path, "r") as zf:
                return self._extract_data_from_zip(zf, encoding, separator, sheet_name)
        except RuntimeError as e:
            if "encrypted" in str(e).lower() or "password" in str(e).lower():
                raise ValueError(
                    f"ZIP file is password-protected. Provide 'password' parameter: {zip_path}"
                )
            raise

    def _load_password_protected_zip(
        self,
        zip_path: Path,
        encoding: str,
        separator: str,
        sheet_name: int | str,
        password: str,
    ) -> pd.DataFrame:
        """Load data from password-protected ZIP using multiple methods."""

        # Method 1: Try pyzipper (AES encryption support)
        try:
            import pyzipper
            with pyzipper.AESZipFile(zip_path, 'r') as zf:
                zf.setpassword(password.encode())
                return self._extract_data_from_pyzipper(zf, encoding, separator, sheet_name)
        except ImportError:
            pass
        except Exception:
            pass

        # Method 2: Try standard zipfile with password
        try:
            with zipfile.ZipFile(zip_path, "r") as zf:
                zf.setpassword(password.encode())
                return self._extract_data_from_zip(zf, encoding, separator, sheet_name)
        except Exception:
            pass

        # Method 3: Try 7-Zip command line
        try:
            return self._extract_with_7zip(zip_path, encoding, separator, sheet_name, password)
        except Exception:
            pass

        # Method 4: Try unzip command line
        try:
            return self._extract_with_unzip(zip_path, encoding, separator, sheet_name, password)
        except Exception as e:
            raise ValueError(
                f"Failed to extract password-protected ZIP. "
                f"Install pyzipper or 7-zip: {e}"
            )

    def _extract_data_from_zip(
        self,
        zf: zipfile.ZipFile,
        encoding: str,
        separator: str,
        sheet_name: int | str,
    ) -> pd.DataFrame:
        """Extract data from an open ZipFile object."""
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

        raise ValueError(f"No CSV or Excel file found in ZIP")

    def _extract_data_from_pyzipper(
        self,
        zf,
        encoding: str,
        separator: str,
        sheet_name: int | str,
    ) -> pd.DataFrame:
        """Extract data from a pyzipper ZipFile object."""
        for name in zf.namelist():
            lower = name.lower()
            if lower.endswith(".csv"):
                data = zf.read(name)
                return pd.read_csv(
                    io.BytesIO(data),
                    sep=separator,
                    encoding=encoding,
                    dtype=str,
                    low_memory=False,
                )
            elif lower.endswith((".xlsx", ".xls")):
                data = zf.read(name)
                return pd.read_excel(io.BytesIO(data), sheet_name=sheet_name, dtype=str)

        raise ValueError(f"No CSV or Excel file found in ZIP")

    def _extract_with_7zip(
        self,
        zip_path: Path,
        encoding: str,
        separator: str,
        sheet_name: int | str,
        password: str,
    ) -> pd.DataFrame:
        """Extract using 7-Zip command line tool."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

            # Try different 7z commands
            for cmd in ["7z", "7za", "7zz"]:
                try:
                    result = subprocess.run(
                        [cmd, "x", f"-p{password}", f"-o{temp_path}", str(zip_path), "-y"],
                        capture_output=True,
                        text=True,
                        timeout=60,
                    )
                    if result.returncode == 0:
                        break
                except (FileNotFoundError, subprocess.TimeoutExpired):
                    continue
            else:
                raise FileNotFoundError("7-Zip not found")

            # Find and load extracted file
            for file in temp_path.rglob("*"):
                if file.suffix.lower() == ".csv":
                    return pd.read_csv(
                        file,
                        sep=separator,
                        encoding=encoding,
                        dtype=str,
                        low_memory=False,
                    )
                elif file.suffix.lower() in (".xlsx", ".xls"):
                    return pd.read_excel(file, sheet_name=sheet_name, dtype=str)

            raise ValueError("No CSV or Excel file found after extraction")

    def _extract_with_unzip(
        self,
        zip_path: Path,
        encoding: str,
        separator: str,
        sheet_name: int | str,
        password: str,
    ) -> pd.DataFrame:
        """Extract using unzip command line tool."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

            result = subprocess.run(
                ["unzip", "-P", password, "-d", str(temp_path), str(zip_path)],
                capture_output=True,
                text=True,
                timeout=60,
            )

            if result.returncode != 0:
                raise ValueError(f"unzip failed: {result.stderr}")

            # Find and load extracted file
            for file in temp_path.rglob("*"):
                if file.suffix.lower() == ".csv":
                    return pd.read_csv(
                        file,
                        sep=separator,
                        encoding=encoding,
                        dtype=str,
                        low_memory=False,
                    )
                elif file.suffix.lower() in (".xlsx", ".xls"):
                    return pd.read_excel(file, sheet_name=sheet_name, dtype=str)

            raise ValueError("No CSV or Excel file found after extraction")


def create_file_loader(config: LoaderConfig, client_config: ClientConfig) -> FileLoader:
    """Factory function to create a FileLoader."""
    return FileLoader(config, client_config)
