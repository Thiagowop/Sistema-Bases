"""Debug validator issue - check why all data is being rejected."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

from src.loaders.file_loader import FileLoader
from src.core.schemas import LoaderConfig, LoaderType

# Load Tabelionato data
ref_path = Path(__file__).parent.parent / "Bases referencia" / "Automacao_Tabelionato" / "data" / "input" / "tabelionato" / "Tabelionato.zip"
config = LoaderConfig(
    type=LoaderType.FILE,
    params={"path": str(ref_path), "password": "Mf4tab@", "encoding": "utf-8", "separator": ";"}
)
loader = FileLoader(config, None)
result = loader.load()
df = result.data

print(f"Loaded: {len(df)} rows")
print(f"Columns: {list(df.columns)}")
print()

# Check CPFCNPJ column
cpf_col = "CPFCNPJ"
if cpf_col in df.columns:
    sample = df[cpf_col].head(10)
    print(f"Sample {cpf_col}:")
    for i, val in enumerate(sample):
        print(f"  {i}: '{val}'")
    
    # Count how many match regex ^\d{11}$ or ^\d{14}$
    import re
    pattern = r"^\d{11}$|^\d{14}$"
    matches = df[cpf_col].astype(str).str.strip().str.match(pattern, na=False)
    print(f"\nMatch pattern (only digits 11 or 14): {matches.sum()} / {len(df)}")
    
    # Check if cleaning helps
    cleaned = df[cpf_col].astype(str).str.replace(r"\D", "", regex=True)
    matches_cleaned = cleaned.str.match(pattern, na=False)
    print(f"Match after cleaning (remove non-digits): {matches_cleaned.sum()} / {len(df)}")

# Check DTANUENCIA
dt_col = "DTANUENCIA"
if dt_col in df.columns:
    sample = df[dt_col].head(10)
    print(f"\nSample {dt_col}:")
    for i, val in enumerate(sample):
        print(f"  {i}: '{val}'")
    
    # Check how many are valid dates
    import pandas as pd
    dt = pd.to_datetime(df[dt_col], errors="coerce", dayfirst=True)
    valid_dates = dt.notna().sum()
    print(f"\nValid dates: {valid_dates} / {len(df)}")
    
    # Check dates in range 1900-2100
    in_range = ((dt.dt.year >= 1900) & (dt.dt.year <= 2100)).sum()
    print(f"Dates in range 1900-2100: {in_range}")
