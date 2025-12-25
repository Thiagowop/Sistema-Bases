"""Processador de Batimento.

Identifica parcelas presentes na VIC que não estão na MAX e gera arquivos
separados (judicial/extrajudicial).
"""

from __future__ import annotations

import logging
import traceback
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional, Union, Set

import pandas as pd

from src.config.loader import ConfigLoader
from src.io.file_manager import FileManager
from src.utils.validator import InconsistenciaManager
from src.io.packager import ExportacaoService
from src.utils.logger import get_logger, log_section
from src.utils.anti_join import procv_vic_menos_max
from src.utils.text import digits_only
from src.processors.vic import VicFilterApplier


class BatimentoProcessor:
    """Processador para batimento usando novos utilitários da Fase 1."""

    def __init__(self, config: Optional[Dict[str, Any]] = None, logger: Optional[logging.Logger] = None):
        # Carregar configurações usando novo ConfigLoader
        self.config_loader = ConfigLoader()
        self.config = config or self.config_loader.get_config()
        self.logger = logger or get_logger(__name__, self.config)
        self.logger.setLevel(logging.WARNING)

        # Configurações do processador de batimento alinhadas com config.yaml
        self.batimento_config = self.config_loader.get_nested_value(self.config, 'batimento_processor', {})
        self.global_config = self.config_loader.get_nested_value(self.config, 'global', {})
        self.paths_config = self.config_loader.get_nested_value(self.config, 'paths', {})

        # Inicializar utilitários da Fase 1
        self.file_manager = FileManager(self.config)
        self.inconsistencia_manager = InconsistenciaManager(self.config)
        self.exportacao_service = ExportacaoService(self.config, self.file_manager)

        # Configurações específicas de batimento
        self.columns_config = self.batimento_config.get('columns', {})
        self.required_columns = self.columns_config.get('required', ['CHAVE', 'CPFCNPJ_CLIENTE', 'VENCIMENTO'])

        empresa_cfg = self.global_config.get('empresa', {})
        self.cnpj_credor = str(empresa_cfg.get('cnpj', '')).strip()
        if not self.cnpj_credor:
            raise ValueError('CNPJ da empresa não configurado. Defina global.empresa.cnpj no config.yaml')

        # Timestamp
        self.add_timestamp = self.global_config.get('add_timestamp', True)

        # CPFs judiciais
        self.judicial_cpfs: Set[str] = set()

        self.logger.info("BatimentoProcessor inicializado com novos utilitários da Fase 1")

    def carregar_arquivo(self, caminho: Union[str, Path]) -> pd.DataFrame:
        """Carrega arquivo CSV ou extrai de ZIP usando FileManager."""
        caminho = Path(caminho)
        if caminho.suffix.lower() == '.zip':
            return self.file_manager.ler_csv_ou_zip(caminho)
        return self.file_manager.ler_csv(caminho)

    def carregar_cpfs_judiciais(self) -> None:
        """Carrega CPFs dos clientes judiciais a partir de CSV/ZIP."""
        try:
            inputs_config = self.config.get('inputs', {})
            judicial_path_cfg = inputs_config.get('clientes_judiciais_path')

            if judicial_path_cfg:
                judicial_file = Path(judicial_path_cfg)
            else:
                judicial_dir = self.paths_config.get('input', {}).get('judicial')
                if judicial_dir:
                    judicial_file = Path(judicial_dir) / "ClientesJudiciais.zip"
                else:
                    judicial_file = Path("data/input/judicial/ClientesJudiciais.zip")

            if not judicial_file.is_absolute():
                # Usar caminho absoluto correto sem duplicação
                base_path = Path.cwd()
                judicial_file = base_path / judicial_file
            if not judicial_file.exists():
                self.logger.warning(
                    f"Arquivo de clientes judiciais não encontrado: {judicial_file}"
                )
                self.logger.warning(
                    "Todos os registros serão classificados como EXTRAJUDICIAL"
                )
                return
            self.logger.info(f"Carregando clientes judiciais: {judicial_file}")
            df_judicial = self.file_manager.ler_csv_ou_zip(judicial_file)
            cpf_columns = [col for col in df_judicial.columns if 'CPF' in col.upper()]
            if not cpf_columns:
                self.logger.warning(
                    "Nenhuma coluna de CPF encontrada no arquivo judicial"
                )
                return
            cpfs_raw = df_judicial[cpf_columns[0]].dropna()
            cpfs_normalized = digits_only(cpfs_raw)
            cpfs_valid = cpfs_normalized[cpfs_normalized.str.len() == 11]
            self.judicial_cpfs = set(cpfs_valid.tolist())
            self.logger.info(
                f"CPFs judiciais carregados: {len(self.judicial_cpfs):,}"
            )
        except Exception as e:
            self.logger.error(f"Erro ao carregar CPFs judiciais: {e}")
            self.judicial_cpfs = set()

    def realizar_cruzamento(self, df_vic: pd.DataFrame, df_max: pd.DataFrame) -> pd.DataFrame:
        """Identifica parcelas em aberto na VIC que não estão na MAX (left anti-join)."""
        self.logger.info("PROCV VIC−MAX: iniciando identificação...")

        # No batimento não há filtro por status; usar MAX como recebido
        df_max_filtrado = df_max.copy()

        if 'CHAVE' not in df_vic.columns:
            raise ValueError(
                "Coluna CHAVE ausente na base VIC tratada para cruzamento com MAX"
            )
        if 'PARCELA' not in df_max_filtrado.columns:
            raise ValueError(
                "Coluna PARCELA ausente na base MAX tratada para cruzamento com VIC"
            )

        if df_vic['CHAVE'].duplicated().any():
            dups = df_vic['CHAVE'][df_vic['CHAVE'].duplicated(keep=False)]
            self.logger.error(
                "CHAVE duplicada na base VIC para batimento (exemplos): %s",
                ", ".join(map(str, dups.head(5)))
            )
            raise ValueError("CHAVE duplicada detectada na base VIC para batimento")

        df_nao_encontradas = procv_vic_menos_max(df_vic, df_max_filtrado, 'CHAVE', 'PARCELA')

        self.metrics_ultima_execucao = {
            'registros_vic': len(df_vic),
            'registros_max': len(df_max_filtrado),
            'registros_batimento': len(df_nao_encontradas),
        }

        # Guardar MAX filtrado para validações posteriores
        self._max_filtrado = df_max_filtrado

        self.logger.info("PROCV VIC−MAX: %s registros", f"{len(df_nao_encontradas):,}")
        return df_nao_encontradas

    def formatar_batimento(self, df: pd.DataFrame) -> pd.DataFrame:
        """Formata dados de batimento conforme layout esperado (sem negociador)."""
        if df.empty:
            return pd.DataFrame()
        self.logger.info("Formatando dados para batimento...")

        layout_cols = [
            "CPFCNPJ CLIENTE",
            "NOME / RAZAO SOCIAL",
            "NUMERO CONTRATO",
            "PARCELA",
            "OBSERVACAO PARCELA",
            "VENCIMENTO",
            "VALOR",
            "EMPREENDIMENTO",
            "TIPO PARCELA",
            "CNPJ CREDOR",
        ]

        df_formatado = pd.DataFrame()
        df_formatado["CPFCNPJ CLIENTE"] = df["CPFCNPJ_CLIENTE"].astype(str)
        df_formatado["NOME / RAZAO SOCIAL"] = df.get("NOME_RAZAO_SOCIAL", "")
        df_formatado["NUMERO CONTRATO"] = df.get("NUMERO_CONTRATO", "")

        def _serie_as_str(col_name: str) -> Optional[pd.Series]:
            serie = df.get(col_name)
            if serie is None:
                return None
            return serie.astype(str)

        def _col_as_str(*col_names: str) -> pd.Series:
            for name in col_names:
                serie = _serie_as_str(name)
                if serie is not None:
                    return serie
            return pd.Series([""] * len(df), index=df.index, dtype=str)

        chave = _serie_as_str("CHAVE")
        parcela_original = _serie_as_str("PARCELA")

        def _blank_series() -> pd.Series:
            return pd.Series([""] * len(df), index=df.index, dtype=str)

        if parcela_original is not None:
            parcela_original = parcela_original.fillna("").str.strip()
        if chave is not None:
            chave = chave.fillna("").str.strip()

        if chave is None:
            raise ValueError("Coluna CHAVE ausente para formatação do batimento")

        chave_final = chave

        df_formatado["PARCELA"] = chave_final

        if parcela_original is not None:
            observacao_parcela = parcela_original
        else:
            observacao_parcela = _col_as_str(
                "OBSERVACAO_PARCELA", "OBSERVACAO PARCELA"
            )

        if observacao_parcela is None:
            observacao_parcela = _blank_series()

        df_formatado["OBSERVACAO PARCELA"] = observacao_parcela
        df_formatado["VENCIMENTO"] = df["VENCIMENTO"].astype(str)
        df_formatado["VALOR"] = df["VALOR"]
        df_formatado["EMPREENDIMENTO"] = df.get("EMPREENDIMENTO", "")

        tipo_parcela_values = df.get("TIPO_PARCELA")
        if tipo_parcela_values is None:
            df_formatado["TIPO PARCELA"] = ""
        elif isinstance(tipo_parcela_values, str):
            df_formatado["TIPO PARCELA"] = tipo_parcela_values.upper()
        else:
            df_formatado["TIPO PARCELA"] = tipo_parcela_values.astype(str).str.upper()

        df_formatado["CNPJ CREDOR"] = str(self.cnpj_credor).strip()
        df_formatado = df_formatado[layout_cols]
        self.logger.info(f"Dados formatados: {len(df_formatado):,} registros")
        return df_formatado

    def gerar_arquivos_batimento(
        self, df_batimento: pd.DataFrame, output_dir: Path, timestamp: str
    ) -> tuple[str, int, int]:
        """Gera arquivos separados (judicial e extrajudicial) em um ZIP."""
        if df_batimento.empty:
            # Sempre gerar um ZIP para compatibilidade com consumidores que
            # esperam 'arquivo_gerado' como caminho válido.
            prefix = (
                self.config.get('batimento_processor', {})
                .get('export', {})
                .get('filename_prefix', 'batimento_vic')
            )
            layout_cols = [
                "CPFCNPJ CLIENTE",
                "NOME / RAZAO SOCIAL",
                "NUMERO CONTRATO",
                "PARCELA",
                "OBSERVACAO PARCELA",
                "VENCIMENTO",
                "VALOR",
                "EMPREENDIMENTO",
                "TIPO PARCELA",
                "CNPJ CREDOR",
            ]
            vazio = pd.DataFrame(columns=layout_cols)
            zip_path = self.exportacao_service.exportar_zip(
                {f"{prefix}_{timestamp}.csv": vazio}, prefix, "batimento"
            )
            return (str(zip_path) if zip_path else ""), 0, 0

        self.logger.info("Separando registros em judicial e extrajudicial...")
        df_b = df_batimento.copy()
        df_b["CPF_NORMALIZADO"] = digits_only(df_b["CPFCNPJ CLIENTE"])
        mask_judicial = df_b["CPF_NORMALIZADO"].isin(self.judicial_cpfs)
        df_judicial = df_batimento[mask_judicial].copy()
        df_extrajudicial = df_batimento[~mask_judicial].copy()

        arquivos: Dict[str, pd.DataFrame] = {}
        prefix = (
            self.config.get('batimento_processor', {})
            .get('export', {})
            .get('filename_prefix', 'batimento_vic')
        )
        if not df_judicial.empty:
            arquivos[f"{prefix}_judicial_{timestamp}.csv"] = df_judicial
        if not df_extrajudicial.empty:
            arquivos[f"{prefix}_extrajudicial_{timestamp}.csv"] = df_extrajudicial
        if not arquivos:
            return "", 0, 0

        self.logger.info(
            "Registros judicial: %s | extrajudicial: %s",
            f"{len(df_judicial):,}",
            f"{len(df_extrajudicial):,}",
        )

        # Nome do ZIP baseado na configuração
        zip_path = self.exportacao_service.exportar_zip(
            arquivos, prefix, "batimento"
        )
        return str(zip_path), len(df_judicial), len(df_extrajudicial)

    def processar(
        self, 
        vic_path: Union[str, Path], 
        max_path: Union[str, Path], 
        output_dir: Optional[Union[str, Path]] = None
    ) -> Dict[str, Any]:
        """Executa o pipeline de batimento completo (carrega, cruza, formata e exporta)."""
        inicio = datetime.now()
        try:
            self.logger.info("Iniciando pipeline de batimento...")

            # Carregar CPFs judiciais
            self.carregar_cpfs_judiciais()

            # Carregar dados
            self.logger.info(f"Carregando dados VIC: {vic_path}")
            df_vic_raw = self.carregar_arquivo(vic_path)
            self.logger.info(f"VIC carregado: {len(df_vic_raw):,} registros")

            vic_filter = VicFilterApplier(self.config, self.logger)
            df_vic, vic_metrics = vic_filter.aplicar_filtros_inclusao(df_vic_raw)
            self.logger.info(
                "VIC após filtros configurados para batimento: %s",
                f"{len(df_vic):,} registros",
            )

            self.logger.info(f"Carregando dados MAX: {max_path}")
            df_max = self.carregar_arquivo(max_path)
            self.logger.info(f"MAX carregado: {len(df_max):,} registros")

            # Cruzamento
            df_cross = self.realizar_cruzamento(df_vic, df_max)

            filtro_counts = {
                'vic_registros_iniciais': vic_metrics.get('registros_iniciais', len(df_vic_raw)),
                'vic_apos_status': vic_metrics.get('apos_status', len(df_vic)),
                'vic_apos_tipos': vic_metrics.get('apos_tipos', len(df_vic)),
                'vic_apos_aging': vic_metrics.get('apos_aging', len(df_vic)),
                'vic_apos_blacklist': vic_metrics.get('apos_blacklist', len(df_vic)),
            }
            self.metrics_ultima_execucao.update(filtro_counts)

            # Formatação
            df_fmt = self.formatar_batimento(df_cross)

            # Saída
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            base_output = (
                Path(output_dir) if output_dir 
                else Path(self.paths_config.get('output', {}).get('base', 'data/output'))
            )
            if not base_output.exists():
                raise FileNotFoundError(f"Diretório de saída não existe: {base_output}")
            arquivo_gerado, n_jud, n_ext = self.gerar_arquivos_batimento(
                df_fmt, base_output, timestamp
            )

            # Calcular validações
            vic_keys = set(df_vic['CHAVE'].astype(str).str.strip())
            max_df = getattr(self, '_max_filtrado', df_max)
            max_keys = set(max_df['PARCELA'].astype(str).str.strip()) if 'PARCELA' in max_df.columns else set()
            bat_keys = set(df_cross['CHAVE'].astype(str).str.strip()) if not df_cross.empty else set()
            
            validacao_subset = bat_keys.issubset(vic_keys)
            validacao_disj = bat_keys.isdisjoint(max_keys)
            validacao_forte = bat_keys == (vic_keys - max_keys)

            duracao = (datetime.now() - inicio).total_seconds()
            stats = {
                **getattr(self, 'metrics_ultima_execucao', {}),
                'arquivo_gerado': arquivo_gerado or 'N/A',
                'duracao': duracao,
                'judicial': n_jud,
                'extrajudicial': n_ext,
                'validacao_subset': validacao_subset,
                'validacao_disj': validacao_disj,
                'validacao_forte': validacao_forte,
            }

            log_section(self.logger, "BATIMENTO - VIC - MAX")
            print("📌 Etapa 3 — Batimento VIC−MAX (LEFT ANTI-JOIN)")
            print("")
            vic_iniciais = filtro_counts['vic_registros_iniciais']
            print(f"VIC base limpa recebida: {vic_iniciais:,} registros")
            if vic_filter.filtros_inclusao.get('status_em_aberto', True):
                print(f"Após STATUS em aberto: {filtro_counts['vic_apos_status']:,}")
            else:
                print("Filtro STATUS (batimento) desabilitado")
            if vic_filter.filtros_inclusao.get('tipos_validos', True) and vic_filter.tipos_validos:
                print(
                    f"Após filtro TIPO ({', '.join(vic_filter.tipos_validos)}): {filtro_counts['vic_apos_tipos']:,}"
                )
            elif not vic_filter.filtros_inclusao.get('tipos_validos', True):
                print("Filtro TIPO (batimento) desabilitado")
            if vic_filter.filtros_inclusao.get('aging', True):
                print(
                    f"Após filtro AGING > {vic_filter.aging_minimo} dias: {filtro_counts['vic_apos_aging']:,}"
                )
            else:
                print("Filtro AGING (batimento) desabilitado")
            if vic_filter.filtros_inclusao.get('blacklist', True):
                removidos = filtro_counts['vic_apos_aging'] - filtro_counts['vic_apos_blacklist']
                print(
                    f"Após filtro Blacklist: {filtro_counts['vic_apos_blacklist']:,} (removidos: {removidos:,})"
                )
            else:
                print("Filtro Blacklist (batimento) desabilitado")
            print(f"VIC para batimento: {stats['registros_vic']:,} registros")
            print(f"MAX tratado: {stats['registros_max']:,} registros")
            print(
                f"Parcelas VIC ausentes no MAX: {stats['registros_batimento']:,}"
            )
            print(
                f"Divisão por carteira: Judicial = {n_jud:,} | Extrajudicial = {n_ext:,}"
            )
            taxa_bat = (
                stats['registros_batimento'] / stats['registros_vic'] * 100
                if stats['registros_vic'] else 0.0
            )
            print(f"🔹 Taxa de batimento: {taxa_bat:.2f}%")
            consist_ok = stats['validacao_subset'] and stats['validacao_disj']
            print(
                "✓ Consistência: {}".format("OK" if consist_ok else "FAIL")
            )
            print("")
            print(f"📦 Exportado: {arquivo_gerado}")
            print(f"⏱️Duração: {duracao:.2f}s")
            return stats
        except Exception as e:
            self.logger.error(f"Erro no pipeline de batimento: {e}")
            self.logger.error(f"Traceback: {traceback.format_exc()}")
            raise
