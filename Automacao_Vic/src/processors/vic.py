"""Processador VIC conforme manual técnico (tratamento).

Fluxo:
0) Padronização mínima
1) Validação inicial (importação)
2) Padronização de valores e criação de colunas auxiliares
3) Remoção de duplicados pela CHAVE
4) Export da base tratada (sem filtros de negócio)

Os filtros de STATUS/TIPO/AGING/blacklist são aplicados sob demanda nas
etapas subsequentes (batimento, devolução, baixa).
"""

from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

import pandas as pd

from src.config.loader import ConfigLoader
from src.io.file_manager import FileManager
from src.utils.validator import InconsistenciaManager, VicValidator
from src.io.packager import ExportacaoService
from src.utils.logger import get_logger, log_section
from src.utils.queries_sql import get_query
from src.utils.sql_conn import get_std_connection
from src.utils.aging import filtrar_clientes_criticos
from src.utils.text import digits_only
from src.utils.helpers import (
    primeiro_valor,
    normalizar_data_string,
    extrair_data_referencia,
    normalizar_decimal,
)
from src.utils.filters import VicFilterApplier


class VicProcessor:
    """Processador para dados VIC com configuraÃ§Ãµes injetÃ¡veis."""

    def __init__(self, config: Optional[Dict[str, Any]] = None, logger: Optional[logging.Logger] = None):
        self.config_loader = ConfigLoader()
        self.config = config or self.config_loader.get_config()
        self.logger = logger or get_logger(__name__, self.config)
        self.logger.setLevel(logging.WARNING)

        self.vic_config = self.config_loader.get_nested_value(self.config, 'vic_processor', {})
        self.global_config = self.config_loader.get_nested_value(self.config, 'global', {})
        self.paths_config = self.config_loader.get_nested_value(self.config, 'paths', {})

        self.file_manager = FileManager(self.config)
        self.inconsistencia_manager = InconsistenciaManager(self.config)
        self.exportacao_service = ExportacaoService(self.config, self.file_manager)

        self.columns_config = self.vic_config.get('columns', {})
        self.validator = VicValidator(self.config, self.logger)

        self.encoding = self.global_config.get('encoding', 'utf-8')
        self.csv_separator = self.global_config.get('csv_separator', ';')
        self.timestamp_format = self.global_config.get('timestamp_format', '%Y%m%d_%H%M%S')
        self.add_timestamp = self.global_config.get('add_timestamp_to_files', True)
        self.date_format = self.global_config.get('date_format', '%d/%m/%Y')

        self.filter_applier = VicFilterApplier(self.config, self.logger)
        self.status_em_aberto = self.filter_applier.status_em_aberto
        self.tipos_validos = self.filter_applier.tipos_validos
        self.aging_minimo = self.filter_applier.aging_minimo
        self.blacklist_clientes = self.filter_applier.blacklist_clientes
        self.status_baixa = self.filter_applier.status_baixa
        self.filtros_inclusao = self.filter_applier.filtros_inclusao
        self.filtros_baixa = self.filter_applier.filtros_baixa

        phone_cols_cfg = self.vic_config.get('phone_columns', [])
        self.phone_columns: List[str] = [str(col) for col in phone_cols_cfg if str(col).strip()]
        self.id_negociador_column = self.vic_config.get('id_negociador_column', 'ID_NEGOCIADOR')
        self.export_config = self.vic_config.get('export', {})

        # Atributos auxiliares
        self._default_data_columns = [
            'DATA_BASE',
            'DATA BASE',
            'DATA_EXTRACAO_BASE',
            'DATA EXTRACAO BASE',
            'DATA_EXTRACAO',
            'DATA EXTRACAO',
            'DATA_REFERENCIA',
            'DATA REFERENCIA',
        ]

    # ---------------- I/O ----------------
    def extrair_dados_vic(self) -> pd.DataFrame:
        query = get_query('vic')
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
        if not caminho_arquivo.exists():
            raise FileNotFoundError(f"Arquivo nÃ£o encontrado: {caminho_arquivo}")
        # Usa mÃ©todo unificado para CSV/ZIP
        return self.file_manager.ler_csv_ou_zip(caminho_arquivo)

    # --------------- PadronizaÃ§Ã£o ---------------
    def normalizar_cabecalhos(self, df: pd.DataFrame) -> pd.DataFrame:
        """Normaliza apenas os cabeÃ§alhos bÃ¡sicos sem renomear para nomes canÃ´nicos"""
        df = df.copy()
        import unicodedata, re
        def norm(s: str) -> str:
            s = unicodedata.normalize('NFKD', s)
            s = ''.join(ch for ch in s if not unicodedata.combining(ch))
            s = s.replace('*', ' ').replace('/', ' ').replace('\n', ' ')
            s = re.sub(r'\s+', ' ', s).strip().upper()
            return s
        
        # Apenas normaliza os nomes das colunas, sem renomear
        normalized_columns = {c: norm(c) for c in df.columns}
        df = df.rename(columns=normalized_columns)
        return df
    
    def mapear_colunas_canonicas(self, df: pd.DataFrame) -> pd.DataFrame:
            """Mapeia colunas normalizadas para nomes canônicos."""
            df = df.copy()
            synonyms = {
                'CPF CNPJ': 'CPFCNPJ_CLIENTE', 'CPFCNPJ': 'CPFCNPJ_CLIENTE', 'CPFCNPJ CLIENTE': 'CPFCNPJ_CLIENTE',
                'CPF/CNPJ': 'CPFCNPJ_CLIENTE',
                'NOME RAZAO SOCIAL': 'NOME_RAZAO_SOCIAL', 'NOME / RAZAO SOCIAL': 'NOME_RAZAO_SOCIAL',
                'NUMERO CONTRATO': 'NUMERO_CONTRATO',
                'PARCELA': 'PARCELA',
                'VENCIMENTO': 'VENCIMENTO',
                'VALOR': 'VALOR',
                'STATUS TITULO': 'STATUS_TITULO', 'STATUS': 'STATUS_TITULO',
                'TIPO PARCELA': 'TIPO_PARCELA', 'TIPO': 'TIPO_PARCELA',
                'COLIGADA': 'EMPREENDIMENTO',
            }
            rename_map = {col: synonyms.get(col, col) for col in df.columns}
            if rename_map:
                df = df.rename(columns=rename_map)
            required = ['CPFCNPJ_CLIENTE', 'NUMERO_CONTRATO', 'PARCELA', 'VENCIMENTO', 'VALOR', 'STATUS_TITULO']
            missing = [c for c in required if c not in df.columns]
            if missing:
                raise ValueError(f"Colunas obrigatorias ausentes apos normalizacao: {missing}")
            return df

    def padronizar_valores(self, df: pd.DataFrame) -> pd.DataFrame:
        """Padroniza apenas os valores das colunas"""
        df = df.copy()
        # Padronizar valores
        df['NUMERO_CONTRATO'] = df['NUMERO_CONTRATO'].astype(str).str.strip()
        df['PARCELA'] = df['PARCELA'].astype(str).str.strip()
        date_format = self.global_config.get('date_format')
        if date_format:
            df['VENCIMENTO'] = pd.to_datetime(df['VENCIMENTO'], format=date_format, errors='coerce')
        else:
            df['VENCIMENTO'] = pd.to_datetime(df['VENCIMENTO'], errors='coerce')
        df['CPFCNPJ_CLIENTE'] = df['CPFCNPJ_CLIENTE'].astype(str).str.strip()
        
        # Formatar valores com vírgula como separador decimal
        if 'VALOR' in df.columns:
            df['VALOR'] = df['VALOR'].apply(self._formatar_valor_decimal)
        
        # CHAVE = NUMERO_CONTRATO + '-' + PARCELA
        df['CHAVE'] = df['NUMERO_CONTRATO'].astype(str).str.strip() + '-' + df['PARCELA'].astype(str).str.strip()
        return df

    @staticmethod
    def _formatar_valor_decimal(valor: Any) -> Any:
        numero = normalizar_decimal(valor)
        if numero is None:
            return valor
        return f"{numero:.2f}".replace(".", ",")

    def criar_colunas_auxiliares(self, df: pd.DataFrame) -> pd.DataFrame:
        """Gera colunas auxiliares reutilizáveis (CPF/CNPJ limpo e telefone limpo)."""

        df = df.copy()

        if 'CPFCNPJ_CLIENTE' in df.columns:
            df['CPFCNPJ_LIMPO'] = digits_only(df['CPFCNPJ_CLIENTE'])
        else:
            df['CPFCNPJ_LIMPO'] = ''

        telefone_cols = self._obter_colunas_telefone(df)
        telefone_limpo_cols: List[str] = []
        for col in telefone_cols:
            clean_col = self._nome_coluna_limpa(col)
            df[clean_col] = digits_only(df[col])
            telefone_limpo_cols.append(clean_col)

        if telefone_limpo_cols:
            telefone_frame = df[telefone_limpo_cols].replace({'': pd.NA, 'nan': pd.NA})
            df['TELEFONE_LIMPO'] = telefone_frame.bfill(axis=1).iloc[:, 0].fillna('')
        else:
            df['TELEFONE_LIMPO'] = ''

        if self.id_negociador_column and self.id_negociador_column not in df.columns:
            df[self.id_negociador_column] = ''

        return df

    def _obter_colunas_telefone(self, df: pd.DataFrame) -> List[str]:
        """Identifica as colunas de telefone a partir da configuração ou heurística."""

        configuradas = [col for col in self.phone_columns if col in df.columns]
        if configuradas:
            return configuradas

        candidatos: List[str] = []
        for col in df.columns:
            normalized = ''.join(ch for ch in col.upper() if ch.isalnum())
            if normalized.startswith('TEL') or 'TELEFONE' in normalized:
                candidatos.append(col)
        return candidatos

    @staticmethod
    def _nome_coluna_limpa(col: str) -> str:
        base = ''.join(ch if ch.isalnum() else '_' for ch in col.upper())
        base = '_'.join(filter(None, base.split('_')))
        return f"{base}_LIMPO"

    # ------------------------------------------------------------------
    # Funções auxiliares movidas para src.utils.helpers
    # Mantidas aqui apenas para compatibilidade com testes e outros módulos
    @staticmethod
    def _primeiro_valor(series: Optional[pd.Series]) -> Optional[Any]:
        """DEPRECATED: Use src.utils.helpers.primeiro_valor"""
        return primeiro_valor(series)

    def _normalizar_data_string(self, valor: Any) -> Optional[str]:
        """DEPRECATED: Use src.utils.helpers.normalizar_data_string"""
        return normalizar_data_string(valor)

    def _resolver_data_base(
        self,
        df: pd.DataFrame,
        entrada_path: Optional[Path],
        data_base: Optional[str],
    ) -> str:
        candidatos: list[Any] = []
        if data_base:
            candidatos.append(data_base)

        for coluna in self._default_data_columns:
            if coluna in df.columns:
                candidatos.append(self._primeiro_valor(df[coluna]))

        for candidato in candidatos:
            valor_normalizado = self._normalizar_data_string(candidato)
            if valor_normalizado:
                return valor_normalizado

        if entrada_path and entrada_path.exists():
            try:
                mtime = datetime.fromtimestamp(entrada_path.stat().st_mtime)
                return mtime.strftime(self.date_format)
            except Exception:
                pass

        return datetime.now().strftime(self.date_format)

    # --------------- Filtros ---------------
    def filtrar_status_em_aberto(self, df: pd.DataFrame) -> pd.DataFrame:
        return self.filter_applier.filtrar_status_em_aberto(df)

    def filtrar_status_em_aberto_canonico(self, df: pd.DataFrame) -> pd.DataFrame:
        return self.filter_applier.filtrar_status_em_aberto(df)

    def filtrar_tipos_especificos(self, df: pd.DataFrame) -> pd.DataFrame:
        return self.filter_applier.filtrar_tipos_validos(df)

    def filtrar_tipos_especificos_canonico(self, df: pd.DataFrame) -> pd.DataFrame:
        return self.filter_applier.filtrar_tipos_validos(df)

    def aplicar_filtro_aging(self, df: pd.DataFrame) -> pd.DataFrame:
        return self.filter_applier.aplicar_aging(df)

    def aplicar_blacklist(self, df: pd.DataFrame) -> pd.DataFrame:
        return self.filter_applier.aplicar_blacklist(df)

    def filtrar_status_baixa(self, df: pd.DataFrame) -> pd.DataFrame:
        return self.filter_applier.filtrar_status_baixa(df)

    def remover_duplicados_chave(self, df: pd.DataFrame) -> tuple[pd.DataFrame, int, Optional[str]]:
        """Remove duplicados pela coluna CHAVE exportando ocorrência para inconsistências."""

        if 'CHAVE' not in df.columns:
            raise ValueError("Coluna CHAVE ausente apos padronizacao de valores (VIC)")

        arquivo_dup: Optional[str] = None
        duplicatas_removidas = 0

        dup_mask = df['CHAVE'].duplicated(keep=False)
        if dup_mask.any():
            detalhes_cols = [
                col
                for col in (
                    'CHAVE',
                    'PARCELA',
                    'NUMERO_CONTRATO',
                    'CPFCNPJ_CLIENTE',
                    'NOME_RAZAO_SOCIAL',
                    'VALOR',
                    'STATUS_TITULO',
                )
                if col in df.columns
            ]
            duplicatas = df.loc[dup_mask, detalhes_cols].copy()
            if not duplicatas.empty:
                duplicatas['DUP_COUNT'] = (
                    duplicatas.groupby('CHAVE')['CHAVE'].transform('size')
                )
                duplicatas.sort_values(
                    ['CHAVE'] + [c for c in detalhes_cols if c != 'CHAVE'],
                    inplace=True,
                )
                arquivo_dup = self.exportacao_service.exportar_inconsistencias(
                    duplicatas,
                    nome_base=self.export_config.get(
                        'inconsistencies_prefix', 'vic_inconsistencias_chave_duplicada'
                    ),
                    subdir='inconsistencias',
                    add_timestamp=self.add_timestamp,
                    como_zip=True,
                )

        antes = len(df)
        df_unico = df.drop_duplicates(subset='CHAVE', keep='first').copy()
        duplicatas_removidas = antes - len(df_unico)

        return df_unico, duplicatas_removidas, arquivo_dup

    # --------------- Pipeline ---------------
    def processar(
        self,
        entrada: Optional[Union[str, Path]] = None,
        saida: Optional[Union[str, Path]] = None,
        data_base: Optional[str] = None,
    ) -> Dict[str, Any]:
        inicio = datetime.now()

        # Carregar
        entrada_path: Optional[Path] = Path(entrada) if entrada else None

        if entrada_path:
            df = self.carregar_arquivo(entrada_path)
        else:
            df = self.extrair_dados_vic()
        orig = len(df)

        # 1) Normalizar apenas cabeÃ§alhos (sem renomear)
        df = self.normalizar_cabecalhos(df)
        # 2) Mapear para nomes canônicos
        df = self.mapear_colunas_canonicas(df)

        # Anexar data-base da extração
        data_base_val = self._resolver_data_base(df, entrada_path, data_base)
        df["DATA_BASE"] = data_base_val

        # 3) Validação inicial (antes de filtros de negócio)
        df_val, df_inv = self.validator.validar_dados(df)
        inconsistencias_iniciais = len(df_inv)

        # 4) Padronização e colunas auxiliares
        df_val = self.padronizar_valores(df_val)
        df_val = self.criar_colunas_auxiliares(df_val)

        # 5) Base canônica (sem filtros)
        df_base_limpa, duplicatas_removidas, arquivo_dup = self.remover_duplicados_chave(df_val)

        # 6) Exportação única da base tratada
        nome_base_limpa = self.export_config.get('base_limpa_prefix', 'vic_base_limpa')
        arquivo_base_limpa = self.exportacao_service.exportar_zip(
            {f"{nome_base_limpa}.csv": df_base_limpa},
            nome_base=nome_base_limpa,
            subdir='vic_tratada',
            add_timestamp=self.add_timestamp,
        )

        # Inconsistências (dados inválidos)
        arquivo_inconsistencias = None
        if len(df_inv) > 0:
            df_inc = self.inconsistencia_manager.criar_dataframe_inconsistencias(df_inv)
            arquivo_inconsistencias = self.exportacao_service.exportar_inconsistencias(
                df_inc,
                nome_base=self.export_config.get('inconsistencies_prefix', 'vic_inconsistencias'),
                subdir='inconsistencias',
                add_timestamp=self.add_timestamp,
                como_zip=True,
            )

        # Estatísticas
        duracao = (datetime.now() - inicio).total_seconds()
        taxa_ap = (len(df_base_limpa) / orig * 100) if orig > 0 else 0.0
        stats = {
            'registros_originais': orig,
            'inconsistencias_iniciais': inconsistencias_iniciais,
            'registros_validos': len(df_val),
            'registros_invalidos': len(df_inv),
            'duplicatas_removidas': duplicatas_removidas,
            'registros_base_limpa': len(df_base_limpa),
            'registros_finais': len(df_base_limpa),
            'taxa_aproveitamento': taxa_ap,
            'arquivo_base_limpa': arquivo_base_limpa,
            'arquivo_gerado': arquivo_base_limpa,
            'arquivo_inconsistencias': arquivo_inconsistencias,
            'arquivo_inconsistencias_chave': arquivo_dup,
            'data_base_utilizada': data_base_val,
            'duracao': duracao,
        }

        # Logs – resumo amigável
        log_section(self.logger, "INICIANDO PIPELINE - VIC")
        print("📌 Etapa 1 — Tratamento VIC")
        print("")
        print(f"Registros originais: {orig:,}")
        print(
            "Inconsistências iniciais (CPF/CNPJ vazio, VENCIMENTO inválido ou VALOR inválido):",
            f"{inconsistencias_iniciais:,}"
        )
        print(f"Registros válidos após padronização: {len(df_val):,}")
        if duplicatas_removidas:
            print(f"• Duplicatas removidas por CHAVE: {duplicatas_removidas:,}")
        print(f"Registros finais base VIC: {len(df_base_limpa):,}")
        print(f"🔹 Taxa de aproveitamento: {taxa_ap:.2f}%")
        print("")
        print(f"📦 Base limpa: {arquivo_base_limpa}")
        if arquivo_inconsistencias:
            print(f"📦 Inconsistências: {arquivo_inconsistencias}")
        if arquivo_dup:
            print(f"📦 Duplicatas (CHAVE): {arquivo_dup}")
        print(f"⏱️Duração: {duracao:.1f}s")
        print("ℹ️ Filtros de negócio serão aplicados nas etapas subsequentes (batimento, devolução, baixa).")

        return stats














