"""
Schema analysis for Tabelionato and MAX files.
Compares columns with expected config.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from src.loaders.file_loader import FileLoader
from src.core.schemas import LoaderConfig, LoaderType


def get_columns(name, path_parts, password=None, encoding="utf-8-sig"):
    """Get column names from a ZIP file."""
    ref_path = Path(__file__).parent.parent
    for part in path_parts:
        ref_path = ref_path / part
    
    config = LoaderConfig(
        type=LoaderType.FILE,
        params={
            "path": str(ref_path),
            "password": password,
            "encoding": encoding,
            "separator": ";",
        }
    )
    
    loader = FileLoader(config, None)
    result = loader.load()
    
    print(f"\n=== {name} ===")
    print(f"Rows: {len(result.data)}")
    print(f"Columns ({len(result.data.columns)}):")
    for col in result.data.columns:
        print(f"  - {col}")
    return list(result.data.columns)


if __name__ == "__main__":
    # Tabelionato input
    tab_cols = get_columns(
        "TABELIONATO INPUT", 
        ["Bases referencia", "Automacao_Tabelionato", "data", "input", "tabelionato", "Tabelionato.zip"],
        password="Mf4tab@",
        encoding="utf-8"
    )
    
    # MAX input
    max_cols = get_columns(
        "MAX INPUT",
        ["Bases referencia", "Automacao_Tabelionato", "data", "input", "max", "MaxSmart_Tabelionato.zip"],
        encoding="utf-8-sig"
    )
    
    print("\n=== SUMMARY ===")
    print(f"Tabelionato: {len(tab_cols)} columns")
    print(f"MAX: {len(max_cols)} columns")
