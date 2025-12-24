"""Processador MAX conforme manual técnico (tratamento e validação)."""

from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional, Tuple, Union

import pandas as pd

from src.config.loader import ConfigLoader
from src.io.file_manager import FileManager
from src.utils.validator import InconsistenciaManager, MaxValidator
from src.io.packager import ExportacaoService
from src.utils.logger import get_logger, log_section
from src.utils.queries_sql import get_query
from src.utils.sql_conn import get_std_connection
from src.utils.helpers import normalizar_decimal


class MaxProcessor:
    def __init__(self, config: Optional[Dict[str, Any]] = None, logger: Optional[logging.Logger] = None):
        self.config_loader = ConfigLoader()
        self.config = config or self.config_loader.get_config()
        self.logger = logger or get_logger(__name__, self.config)
        self.logger.setLevel(logging.WARNING)

        self.max_config = self.config_loader.get_nested_value(self.config, 'max_processor', {})
        self.global_config = self.config_loader.get_nested_value(self.config, 'global', {})
        self.paths_config = self.config_loader.get_nested_value(self.config, 'paths', {})

        self.file_manager = FileManager(self.config)
        self.inconsistencia_manager = InconsistenciaManager(self.config)
        self.exportacao_service = ExportacaoService(self.config, self.file_manager)

        self.columns_config = self.max_config.get('columns', {})
        self.validator = MaxValidator(self.max_config, self.logger)

        validation_flags = self.max_config.get('validation', {})
        self.remove_parcela_duplicada: bool = bool(
            validation_flags.get('remover_parcela_duplicada', True)
        )
        self.block_tipo_parcela_vazio: bool = bool(
            validation_flags.get('bloquear_tipo_parcela_vazio', False)
        )

        self.encoding = self.global_config.get('encoding', 'utf-8')
        self.csv_separator = self.global_config.get('csv_separator', ';')
        self.timestamp_format = self.global_config.get('timestamp_format', '%Y%m%d_%H%M%S')
        self.add_timestamp = self.global_config.get('add_timestamp_to_files', True)

    # -------------- IO --------------
    def extrair_dados_max(self) -> pd.DataFrame:
        query = get_query('max')
        conn = get_std_connection()
        if not conn.connect():
            raise RuntimeError("Falha ao conectar com banco de dados")
        try:
            df = pd.read_sql(query, conn.connection)
            return df
        finally:
            conn.close()

    def carregar_arquivo(self, caminho_arquivo: Union[str, Path]) -> pd.DataFrame:
        caminho_arquivo = Path(caminho_arquivo)
        self.file_manager.validar_arquivo_existe(caminho_arquivo)
        return self.file_manager.ler_csv_ou_zip(caminho_arquivo)

    # -------------- Padronização e Validação --------------
    def padronizar_campos(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        mapping = self.columns_config.get('mapping', {})
        if mapping:
            df = df.rename(columns=mapping)

        required = self.columns_config.get(
            'required', ['CPFCNPJ_CLIENTE', 'NUMERO_CONTRATO', 'PARCELA', 'VENCIMENTO', 'VALOR']
        )
        missing = [c for c in required if c not in df.columns]
        if missing:
            raise ValueError(f"Colunas obrigatórias ausentes na base MAX: {missing}")

        # Padroniza PARCELA para futura criação da CHAVE
        df['PARCELA'] = df['PARCELA'].astype(str).str.strip()

        # Datas e docs
        date_format = self.global_config.get('date_format')
        raw_venc = df['VENCIMENTO'].copy()
        if date_format:
            df['VENCIMENTO'] = pd.to_datetime(
                raw_venc, format=date_format, errors='coerce'
            )
            if df['VENCIMENTO'].isna().all():
                df['VENCIMENTO'] = pd.to_datetime(
                    raw_venc, errors='coerce'
                )
        else:
            df['VENCIMENTO'] = pd.to_datetime(raw_venc, errors='coerce')

        df['CPFCNPJ_CLIENTE'] = df['CPFCNPJ_CLIENTE'].astype(str).str.strip()
        
        # Formatar valores com vírgula como separador decimal
        if 'VALOR' in df.columns:
            df['VALOR'] = df['VALOR'].apply(self._formatar_valor_decimal)
        
        return df

    @staticmethod
    def _formatar_valor_decimal(valor: Any) -> Any:
        numero = normalizar_decimal(valor)
        if numero is None:
            return valor
        return f"{numero:.2f}".replace(".", ",")

    def validar_dados(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
        return self.validator.validar_dados(df)

    # -------------- Pipeline --------------
    def processar(self, entrada: Optional[Union[str, Path]] = None,
                  saida: Optional[Union[str, Path]] = None) -> Dict[str, Any]:
        inicio = datetime.now()

        # Carregar dados
        if entrada:
            df = self.carregar_arquivo(entrada)
        else:
            df = self.extrair_dados_max()

        orig = len(df)

        # Padronização
        df = self.padronizar_campos(df)

        # Validação
        df_val, df_inv = self.validar_dados(df)

        if not df_inv.empty and 'motivo_inconsistencia' not in df_inv.columns:
            df_inv = df_inv.copy()
            df_inv['motivo_inconsistencia'] = 'VALIDACAO_BASE'

        if self.remove_parcela_duplicada and 'PARCELA' in df_val.columns:
            duplicados_mask = df_val['PARCELA'].duplicated(keep=False)
            if duplicados_mask.any():
                df_dup = df_val.loc[duplicados_mask].copy()
                df_dup['motivo_inconsistencia'] = 'PARCELA_DUPLICADA'
                df_inv = pd.concat([df_inv, df_dup], ignore_index=False)
                df_val = df_val.loc[~duplicados_mask].copy()

        if self.block_tipo_parcela_vazio and 'TIPO_PARCELA' in df_val.columns:
            tipo_series = df_val['TIPO_PARCELA']
            mask_tipo_vazio = tipo_series.isna() | tipo_series.astype(str).str.strip().eq('')
            if mask_tipo_vazio.any():
                df_tipo = df_val.loc[mask_tipo_vazio].copy()
                df_tipo['motivo_inconsistencia'] = 'TIPO_PARCELA_VAZIO'
                df_inv = pd.concat([df_inv, df_tipo], ignore_index=False)
                df_val = df_val.loc[~mask_tipo_vazio].copy()

        # MAX não cria CHAVE; mantém PARCELA como chave nativa
        df_final = df_val.copy()

        # Export (ZIP com CSV)
        export_config = self.max_config.get('export', {})
        nome_base = export_config.get('filename_prefix', 'max_tratada')
        arquivo_saida = self.exportacao_service.exportar_zip(
            {f"{nome_base}.csv": df_final},
            nome_base,
            subdir='max_tratada'
        )

        # Inconsistências
        arquivo_inconsistencias = None
        if len(df_inv) > 0:
            df_inc = self.inconsistencia_manager.criar_dataframe_inconsistencias(df_inv)
            arquivo_inconsistencias = self.exportacao_service.exportar_inconsistencias(
                df_inc,
                nome_base=export_config.get('inconsistencies_prefix', 'max_inconsistencias'),
                subdir='inconsistencias',
                como_zip=True
            )

        duracao = (datetime.now() - inicio).total_seconds()
        taxa_ap = (len(df_final) / orig * 100) if orig > 0 else 0.0
        stats = {
            'registros_originais': orig,
            'registros_validos': len(df_val),
            'registros_invalidos': len(df_inv),
            'registros_finais': len(df_final),
            'taxa_aproveitamento': taxa_ap,
            'arquivo_gerado': arquivo_saida,
            'arquivo_inconsistencias': arquivo_inconsistencias,
            'duracao': duracao,
        }

        # Logs – pretty summary
        log_section(self.logger, "TRATAMENTO - MAX")
        print("📌 Etapa 2 — Tratamento MAX")
        print("")
        print(f"Registros originais: {orig:,}")
        print(f"Inconsistências iniciais (PARCELA inválida): {len(df_inv):,}")
        print(f"Registros válidos: {len(df_val):,}")
        print(f"Registros finais MAX tratados: {len(df_final):,}")
        print(f"🔹 Taxa de aproveitamento: {taxa_ap:.1f}%")
        print("")
        print(f"📦 Exportado: {arquivo_saida}")
        print(f"⏱️Duração: {duracao:.1f}s")

        return stats

