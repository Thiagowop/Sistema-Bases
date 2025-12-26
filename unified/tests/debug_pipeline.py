"""
Debug script to run pipeline and capture errors.
"""
import sys
import traceback
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from src.core import ConfigLoader, PipelineEngine, ProcessorType
from src.processors import (
    TratamentoProcessor,
    BatimentoProcessor,
    BaixaProcessor,
    DevolucaoProcessor,
    EnriquecimentoProcessor,
)


def main():
    try:
        config_dir = Path("./configs/clients")
        output_dir = Path("./output")
        
        print("Loading config...")
        engine = PipelineEngine(config_dir=config_dir, output_dir=output_dir)
        
        print("Registering processors...")
        engine.register_processor(ProcessorType.TRATAMENTO, TratamentoProcessor)
        engine.register_processor(ProcessorType.BATIMENTO, BatimentoProcessor)
        engine.register_processor(ProcessorType.BAIXA, BaixaProcessor)
        engine.register_processor(ProcessorType.DEVOLUCAO, DevolucaoProcessor)
        engine.register_processor(ProcessorType.ENRIQUECIMENTO, EnriquecimentoProcessor)
        
        print("Running pipeline for tabelionato...")
        result = engine.run("tabelionato")
        
        print(f"\nResult: {'SUCCESS' if result.success else 'FAILED'}")
        print(f"Duration: {result.duration_seconds:.2f}s")
        print(f"Errors: {result.context.errors}")
        
        if result.context.outputs:
            print("\nOutputs:")
            for name, path in result.context.outputs.items():
                print(f"  {name}: {path}")
                
    except Exception as e:
        print(f"\n=== ERROR ===")
        print(f"Type: {type(e).__name__}")
        print(f"Message: {e}")
        print(f"\n=== TRACEBACK ===")
        traceback.print_exc()
        return 1
        
    return 0


if __name__ == "__main__":
    sys.exit(main())
