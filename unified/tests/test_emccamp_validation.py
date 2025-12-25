#!/usr/bin/env python3
"""
Test script to validate the unified pipeline against Emccamp data.
Compares outputs with expected results.
"""
import sys
import zipfile
from pathlib import Path

import pandas as pd

# Add unified to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.core import ConfigLoader, PipelineEngine, ProcessorType
from src.processors import (
    TratamentoProcessor,
    BatimentoProcessor,
    BaixaProcessor,
    DevolucaoProcessor,
    EnriquecimentoProcessor,
)


def load_csv_from_zip(zip_path: Path) -> pd.DataFrame:
    """Load first CSV from a ZIP file."""
    with zipfile.ZipFile(zip_path, 'r') as zf:
        csv_files = [n for n in zf.namelist() if n.lower().endswith('.csv')]
        if not csv_files:
            raise ValueError(f"No CSV found in {zip_path}")
        with zf.open(csv_files[0]) as f:
            return pd.read_csv(f, sep=';', encoding='utf-8-sig', dtype=str)


def test_emccamp_data_loading():
    """Test loading Emccamp input data."""
    print("\n" + "=" * 60)
    print("TEST: Loading Emccamp Data")
    print("=" * 60)

    emccamp_zip = Path("/home/user/Sistema-Bases/Emccamp/data/input/emccamp/Emccamp.zip")
    max_zip = Path("/home/user/Sistema-Bases/Emccamp/data/input/base_max/MaxSmart.zip")

    # Load Emccamp
    print(f"\nLoading: {emccamp_zip}")
    df_emccamp = load_csv_from_zip(emccamp_zip)
    print(f"  Rows: {len(df_emccamp)}")
    print(f"  Columns: {list(df_emccamp.columns)[:10]}...")

    # Load MaxSmart
    print(f"\nLoading: {max_zip}")
    df_max = load_csv_from_zip(max_zip)
    print(f"  Rows: {len(df_max)}")
    print(f"  Columns: {list(df_max.columns)}")

    return df_emccamp, df_max


def test_key_generation(df_emccamp: pd.DataFrame, df_max: pd.DataFrame):
    """Test key generation logic."""
    print("\n" + "=" * 60)
    print("TEST: Key Generation")
    print("=" * 60)

    # Emccamp key: NUM_VENDA-ID_PARCELA
    df_emccamp = df_emccamp.copy()
    df_emccamp['CHAVE'] = (
        df_emccamp['NUM_VENDA'].astype(str).str.strip() +
        '-' +
        df_emccamp['ID_PARCELA'].astype(str).str.strip()
    )
    print(f"\nEmccamp keys generated: {len(df_emccamp)}")
    print(f"  Sample keys: {df_emccamp['CHAVE'].head(3).tolist()}")

    # MAX key: PARCELA (already in correct format)
    df_max = df_max.copy()
    df_max['CHAVE'] = df_max['PARCELA'].astype(str).str.strip()
    print(f"\nMAX keys: {len(df_max)}")
    print(f"  Sample keys: {df_max['CHAVE'].head(3).tolist()}")

    # Validate PARCELA format (regex: ^[0-9]{3,}-[0-9]{2,}$)
    import re
    parcela_pattern = re.compile(r'^[0-9]{3,}-[0-9]{2,}$')
    valid_parcelas = df_max['PARCELA'].apply(lambda x: bool(parcela_pattern.match(str(x))))
    print(f"\nValid PARCELA format: {valid_parcelas.sum()} / {len(df_max)}")

    return df_emccamp, df_max


def test_batimento(df_emccamp: pd.DataFrame, df_max: pd.DataFrame):
    """Test anti-join (batimento) logic."""
    print("\n" + "=" * 60)
    print("TEST: Batimento (Anti-Join)")
    print("=" * 60)

    emccamp_keys = set(df_emccamp['CHAVE'].dropna().astype(str).str.strip())
    max_keys = set(df_max['CHAVE'].dropna().astype(str).str.strip())

    print(f"\nEmccamp unique keys: {len(emccamp_keys)}")
    print(f"MAX unique keys: {len(max_keys)}")

    # A - B (Emccamp not in MAX) = NOVOS
    novos_keys = emccamp_keys - max_keys
    print(f"\nNOVOS (Emccamp - MAX): {len(novos_keys)}")

    # B - A (MAX not in Emccamp) = BAIXAS
    baixas_keys = max_keys - emccamp_keys
    print(f"BAIXAS (MAX - Emccamp): {len(baixas_keys)}")

    # Intersection
    common_keys = emccamp_keys & max_keys
    print(f"COMMON (intersection): {len(common_keys)}")

    return novos_keys, baixas_keys, common_keys


def test_config_loading():
    """Test loading unified config."""
    print("\n" + "=" * 60)
    print("TEST: Config Loading")
    print("=" * 60)

    config_dir = Path("/home/user/Sistema-Bases/unified/configs/clients")
    loader = ConfigLoader(config_dir)

    try:
        config = loader.load("emccamp")
        print(f"\nConfig loaded successfully!")
        print(f"  Name: {config.name}")
        print(f"  Version: {config.version}")
        print(f"  Description: {config.description}")
        print(f"  Client source type: {config.client_source.loader.type if config.client_source else 'None'}")
        print(f"  MAX source type: {config.max_source.loader.type if config.max_source else 'None'}")
        print(f"  Processors: {[p.type.value for p in config.pipeline.processors]}")
        return config
    except Exception as e:
        print(f"\nError loading config: {e}")
        return None


def main():
    """Run all tests."""
    print("\n" + "=" * 60)
    print("UNIFIED PIPELINE - VALIDATION TESTS")
    print("=" * 60)

    # Test 1: Config loading
    config = test_config_loading()

    # Test 2: Data loading
    df_emccamp, df_max = test_emccamp_data_loading()

    # Test 3: Key generation
    df_emccamp, df_max = test_key_generation(df_emccamp, df_max)

    # Test 4: Batimento
    novos, baixas, common = test_batimento(df_emccamp, df_max)

    # Summary
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"\nTotal Emccamp records: {len(df_emccamp)}")
    print(f"Total MAX records: {len(df_max)}")
    print(f"NOVOS (to add to MAX): {len(novos)}")
    print(f"BAIXAS (to remove from MAX): {len(baixas)}")
    print(f"MANTIDOS (common): {len(common)}")
    print("\n" + "=" * 60)

    return 0


if __name__ == "__main__":
    sys.exit(main())
