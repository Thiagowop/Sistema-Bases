"""
Test script for MAX file loading.
Validates that the unified system can load MAX ZIP files.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from src.loaders.file_loader import FileLoader
from src.core.schemas import LoaderConfig, LoaderType


def test_max_load():
    """Test loading MAX ZIP file."""
    
    ref_path = Path(__file__).parent.parent / "Bases referencia" / "Automacao_Tabelionato" / "data" / "input" / "max" / "MaxSmart_Tabelionato.zip"
    
    print(f"File exists: {ref_path.exists()}")
    
    if not ref_path.exists():
        print("ERROR: Reference file not found!")
        return False
        
    config = LoaderConfig(
        type=LoaderType.FILE,
        params={
            "path": str(ref_path),
            "encoding": "utf-8-sig",
            "separator": ";",
        }
    )
    
    loader = FileLoader(config, None)
    result = loader.load()
    
    print(f"Rows: {len(result.data)}, Cols: {len(result.data.columns)}")
    
    if result.data.empty:
        print(f"ERROR: {result.metadata.get('error', 'Unknown')}")
        return False
    
    print(f"Cols: {list(result.data.columns)}")
    return True


if __name__ == "__main__":
    success = test_max_load()
    print(f"RESULT: {'OK' if success else 'FAIL'}")
    sys.exit(0 if success else 1)
