"""
Test script for Tabelionato file loading.
Validates that the unified system can load ZIP files with password.
"""
import sys
from pathlib import Path

# Add unified to path
sys.path.insert(0, str(Path(__file__).parent))

from src.loaders.file_loader import FileLoader
from src.core.schemas import LoaderConfig, LoaderType


def test_tabelionato_load():
    """Test loading Tabelionato ZIP file with password."""
    
    # Path to reference file
    ref_path = Path(__file__).parent.parent / "Bases referencia" / "Automacao_Tabelionato" / "data" / "input" / "tabelionato" / "Tabelionato.zip"
    
    print(f"File exists: {ref_path.exists()}")
    
    if not ref_path.exists():
        print("ERROR: Reference file not found!")
        return False
        
    # Create loader config
    config = LoaderConfig(
        type=LoaderType.FILE,
        params={
            "path": str(ref_path),
            "password": "Mf4tab@",
            "encoding": "utf-8",
            "separator": ";",
        }
    )
    
    # Create and run loader
    loader = FileLoader(config, None)
    result = loader.load()
    
    print(f"Rows: {len(result.data)}, Cols: {len(result.data.columns)}")
    
    if result.data.empty:
        print(f"ERROR: {result.metadata.get('error', 'Unknown')}")
        return False
    
    print(f"Cols: {list(result.data.columns)[:8]}...")
    return True


if __name__ == "__main__":
    success = test_tabelionato_load()
    print(f"RESULT: {'OK' if success else 'FAIL'}")
    sys.exit(0 if success else 1)
