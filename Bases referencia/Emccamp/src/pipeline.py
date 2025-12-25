from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict

from src.config.loader import ConfigLoader, LoadedConfig
from src.processors import batimento as batimento_proc
from src.processors import baixa as baixa_proc
from src.processors import contact_enrichment as enrichment_proc
from src.processors import devolucao as devolucao_proc
from src.processors import emccamp as emccamp_proc
from src.processors import max as max_proc
from src.scripts import extrair_basemax, extrair_doublecheck_acordo, extrair_judicial
from src.utils.totvs_client import baixar_baixas_emccamp, baixar_emccamp
from src.utils.output_formatter import OutputFormatter, format_extraction_output
from time import time


@dataclass(slots=True)
class Pipeline:
    """High-level access point for extraction and processing workflows."""

    loader: ConfigLoader = field(default_factory=ConfigLoader)
    _config: LoadedConfig | None = field(default=None, init=False, repr=False)

    def _get_config(self) -> LoadedConfig:
        if self._config is None:
            self._config = self.loader.load()
        return self._config

    # ---- Extraction layer -------------------------------------------------

    def extract_emccamp(self) -> None:
        config = self._get_config()
        inicio = time()
        zip_path, records = baixar_emccamp(config)
        duracao = time() - inicio
        
        format_extraction_output(
            source="EMCCAMP (API TOTVS)",
            output_file=str(zip_path),
            records=records,
            duration=duracao,
            steps=[
                "Conexao com API TOTVS",
                f"Download de {OutputFormatter.format_count(records)} registros",
                "Conversao para DataFrame",
                f"Salvamento em {zip_path.name}"
            ]
        )

    def extract_max(self) -> None:
        extrair_basemax.main()

    def extract_judicial(self) -> None:
        extrair_judicial.main()

    def extract_baixa(self) -> None:
        config = self._get_config()
        inicio = time()
        zip_path, records = baixar_baixas_emccamp(config)
        duracao = time() - inicio
        
        format_extraction_output(
            source="BAIXAS EMCCAMP (API TOTVS)",
            output_file=str(zip_path),
            records=records,
            duration=duracao,
            steps=[
                "Conexao com API TOTVS",
                f"Download de dados de baixas",
                f"Filtro: HONORARIO_BAIXADO != 0",
                f"Resultado: {OutputFormatter.format_count(records)} registros",
                f"Salvamento em {zip_path.name}"
            ]
        )

    def extract_doublecheck(self) -> None:
        extrair_doublecheck_acordo.main()

    def extract_all(self) -> None:
        self.extract_emccamp()
        self.extract_baixa()
        self.extract_max()
        self.extract_judicial()
        self.extract_doublecheck()

    # ---- Treatment / processing -------------------------------------------

    def treat_emccamp(self) -> emccamp_proc.ProcessorStats:
        return emccamp_proc.run(self.loader)

    def treat_max(self) -> max_proc.MaxStats:
        return max_proc.run(self.loader)

    def treat_all(self) -> Dict[str, object]:
        return {
            "emccamp": self.treat_emccamp(),
            "max": self.treat_max(),
        }

    def batimento(self):
        return batimento_proc.run(self.loader)

    def baixa(self) -> None:
        baixa_proc.run(self.loader)

    def devolucao(self):
        """Executa processamento de devolução MAX - EMCCAMP."""
        return devolucao_proc.run(self.loader)

    def enriquecimento(self, dataset: str | None = None):
        return enrichment_proc.run(dataset, self.loader)
