"""Módulo de filtros de negócio para processadores VIC.

Este módulo centraliza a lógica de filtragem aplicada aos dados VIC,
reduzindo o acoplamento entre processadores e promovendo reutilização.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Set, Tuple

import pandas as pd

from config.loader import ConfigLoader
from utils.logger import get_logger
from utils.queries_sql import get_query
from utils.sql_conn import get_std_connection
from utils.aging import filtrar_clientes_criticos
from utils.text import digits_only


class VicFilterApplier:
    """Aplica filtros de negócio da base VIC sob demanda."""

    def __init__(
        self, config: Dict[str, Any], logger: Optional[logging.Logger] = None
    ) -> None:
        self.config_loader = ConfigLoader()
        self.config = config
        self.logger = logger or get_logger(__name__, config)

        self.vic_config = self.config_loader.get_nested_value(
            self.config, "vic_processor", {}
        )
        self.global_config = self.config_loader.get_nested_value(
            self.config, "global", {}
        )
        self.paths_config = self.config_loader.get_nested_value(
            self.config, "paths", {}
        )

        self.status_em_aberto: set[str] = self._normalize_status_values(
            self.vic_config.get("status_em_aberto", "EM ABERTO"), "EM ABERTO"
        )
        tipos_cfg = self.vic_config.get("tipos_validos", [])
        self.tipos_validos: List[str] = [
            str(tipo).upper().strip() for tipo in tipos_cfg if str(tipo).strip()
        ]
        self.aging_minimo: int = int(self.vic_config.get("aging_minimo", 90))

        self.status_baixa: set[str] = self._normalize_status_values(
            self.vic_config.get("status_baixa", "BAIXADO"), "BAIXADO"
        )

        self.blacklist_clientes: set[str] = {
            str(x) for x in self.vic_config.get("blacklist_clientes", [])
        }

        self.filtros_inclusao: Dict[str, bool] = {
            "status_em_aberto": True,
            "tipos_validos": True,
            "aging": True,
            "blacklist": True,
        }
        self.filtros_inclusao.update(
            {k: bool(v) for k, v in self.vic_config.get("filtros_inclusao", {}).items()}
        )

        self.filtros_baixa: Dict[str, bool] = {
            "status_baixa": True,
            "tipos_validos": True,
            "aging": False,
            "blacklist": True,
        }
        self.filtros_baixa.update(
            {k: bool(v) for k, v in self.vic_config.get("filtros_baixa", {}).items()}
        )

        self._blacklist_cache: Optional[set[str]] = None

    def _normalize_status_values(
        self, config_value: Any, default: str
    ) -> Set[str]:
        """Normaliza valores de status da configuração."""
        if isinstance(config_value, str):
            return {config_value.upper().strip()}
        elif isinstance(config_value, list):
            return {str(s).upper().strip() for s in config_value if str(s).strip()}
        else:
            return {default}

    def _obter_blacklist_docs(self) -> Set[str]:
        """Obtém documentos da blacklist (cache + SQL + arquivos)."""
        if self._blacklist_cache is not None:
            return self._blacklist_cache

        docs_total: set[str] = set()
        docs_total.update(self.blacklist_clientes)

        # Carregar blacklist de arquivos
        blacklist_dir = self.paths_config.get("input", {}).get("blacklist")
        if blacklist_dir:
            try:
                import os
                import pandas as pd
                from src.utils.text import digits_only
                
                if os.path.exists(blacklist_dir):
                    for arquivo in os.listdir(blacklist_dir):
                        if arquivo.endswith(('.xlsx', '.csv')):
                            arquivo_path = os.path.join(blacklist_dir, arquivo)
                            try:
                                if arquivo.endswith('.xlsx'):
                                    df_blacklist = pd.read_excel(arquivo_path)
                                else:
                                    df_blacklist = pd.read_csv(arquivo_path)
                                
                                # Buscar coluna com CPF/CNPJ
                                coluna_cpf = None
                                for col in df_blacklist.columns:
                                    if any(termo in col.upper() for termo in ['CPF', 'CNPJ', 'DOCUMENTO']):
                                        coluna_cpf = col
                                        break
                                
                                if coluna_cpf and not df_blacklist.empty:
                                    cpfs_arquivo = df_blacklist[coluna_cpf].astype(str)
                                    cpfs_normalizados = digits_only(cpfs_arquivo)
                                    cpfs_validos = {doc for doc in cpfs_normalizados if doc and doc != 'nan'}
                                    docs_total.update(cpfs_validos)
                                    self.logger.info(
                                        "Blacklist arquivo %s carregada: %s documentos", 
                                        arquivo, len(cpfs_validos)
                                    )
                            except Exception as e:
                                self.logger.warning("Erro ao processar arquivo blacklist %s: %s", arquivo, e)
            except Exception as e:
                self.logger.warning("Erro ao carregar blacklist de arquivos: %s", e)

        # Carregar blacklist via SQL
        blacklist_query = self.vic_config.get("blacklist_query")
        if blacklist_query:
            try:
                query_sql = get_query(blacklist_query)
                with get_std_connection(self.config) as conn:
                    df_blacklist = pd.read_sql(query_sql, conn)
                if not df_blacklist.empty and len(df_blacklist.columns) > 0:
                    primeira_coluna = df_blacklist.columns[0]
                    docs_sql = digits_only(df_blacklist[primeira_coluna])
                    docs_validos = {doc for doc in docs_sql if doc}
                    docs_total.update(docs_validos)
                    self.logger.info(
                        "Blacklist SQL carregada: %s documentos", len(docs_validos)
                    )
            except Exception as e:
                self.logger.warning("Erro ao carregar blacklist SQL: %s", e)

        self._blacklist_cache = docs_total
        return docs_total

    def filtrar_status_em_aberto_max(self, df: pd.DataFrame) -> pd.DataFrame:
        """Filtra DataFrame baseado no STATUS_TITULO usando configuração específica do MAX."""
        if "STATUS_TITULO" not in df.columns:
            self.logger.warning("Coluna STATUS_TITULO não encontrada para filtro MAX")
            return df
        
        # Usar configuração específica do MAX
        max_config = self.config_loader.get_nested_value(self.config, "max_processor", {})
        status_em_aberto_max = self._normalize_status_values(
            max_config.get("status_em_aberto", "Aberto"), "Aberto"
        )
        
        mask = (
            df["STATUS_TITULO"]
            .astype(str)
            .str.upper()
            .str.strip()
            .isin(status_em_aberto_max)
        )
        return df[mask]

    def filtrar_status_em_aberto(self, df: pd.DataFrame) -> pd.DataFrame:
        """Filtra registros com status em aberto."""
        if "STATUS_TITULO" not in df.columns:
            raise ValueError(
                f"Coluna STATUS_TITULO não encontrada. Colunas: {list(df.columns)}"
            )

        mask = (
            df["STATUS_TITULO"]
            .astype(str)
            .str.upper()
            .str.strip()
            .isin(self.status_em_aberto)
        )
        out = df[mask].copy()
        self.logger.info(
            "VIC após filtro status=EM ABERTO: %s", f"{len(out):,}"
        )
        return out

    def filtrar_status_baixa(self, df: pd.DataFrame) -> pd.DataFrame:
        """Filtra registros com status de baixa."""
        if "STATUS_TITULO" not in df.columns:
            raise ValueError(
                f"Coluna STATUS_TITULO não encontrada. Colunas: {list(df.columns)}"
            )

        mask = (
            df["STATUS_TITULO"]
            .astype(str)
            .str.upper()
            .str.strip()
            .isin(self.status_baixa)
        )
        out = df[mask].copy()
        self.logger.info(
            "VIC após filtro status=BAIXADO: %s", f"{len(out):,}"
        )
        return out

    def filtrar_tipos_validos(self, df: pd.DataFrame) -> pd.DataFrame:
        """Filtra registros por tipos de parcela válidos."""
        if not self.tipos_validos:
            return df

        col_tipo = "TIPO_PARCELA" if "TIPO_PARCELA" in df.columns else "TIPO"
        if col_tipo not in df.columns:
            raise ValueError(
                f"Coluna de tipo não encontrada. Colunas disponíveis: {list(df.columns)}"
            )

        mask = (
            df[col_tipo]
            .astype(str)
            .str.upper()
            .str.strip()
            .isin(self.tipos_validos)
        )
        out = df[mask].copy()
        self.logger.info(
            "VIC após filtro TIPO (%s): %s",
            ", ".join(self.tipos_validos),
            f"{len(out):,}",
        )
        return out

    def aplicar_aging(self, df: pd.DataFrame) -> pd.DataFrame:
        """Aplica filtro de aging por cliente."""
        df_out, _ = filtrar_clientes_criticos(
            df,
            col_cliente="CPFCNPJ_CLIENTE",
            col_vencimento="VENCIMENTO",
            limite=self.aging_minimo,
        )
        self.logger.info(
            "VIC após filtro AGING > %s dias: %s",
            self.aging_minimo,
            f"{len(df_out):,}",
        )
        return df_out

    def aplicar_blacklist(self, df: pd.DataFrame) -> pd.DataFrame:
        """Remove clientes presentes na blacklist."""
        docs_total = self._obter_blacklist_docs()
        if not docs_total:
            self.logger.info(
                "VIC após filtro Blacklist: %s (sem blacklist configurada)",
                f"{len(df):,}",
            )
            return df

        if "CPFCNPJ_CLIENTE" not in df.columns:
            raise ValueError(
                "Coluna CPFCNPJ_CLIENTE não encontrada para aplicar blacklist"
            )

        df_cpf_norm = digits_only(df["CPFCNPJ_CLIENTE"])
        mask = ~df_cpf_norm.isin(docs_total)
        out = df[mask].copy()
        removidos = len(df) - len(out)

        self.logger.info(
            "VIC após filtro Blacklist: %s (removidos: %s)",
            f"{len(out):,}",
            f"{removidos:,}",
        )
        return out

    def aplicar_filtros_inclusao(
        self, df: pd.DataFrame
    ) -> Tuple[pd.DataFrame, Dict[str, int]]:
        """Aplica sequência de filtros para inclusão (devolução, batimento)."""
        out = df.copy()
        metrics: Dict[str, int] = {"registros_iniciais": len(out)}

        if self.filtros_inclusao.get("status_em_aberto", True):
            out = self.filtrar_status_em_aberto(out)
        metrics["apos_status"] = len(out)

        if self.filtros_inclusao.get("tipos_validos", True):
            out = self.filtrar_tipos_validos(out)
        metrics["apos_tipos"] = len(out)

        if self.filtros_inclusao.get("aging", True):
            out = self.aplicar_aging(out)
        metrics["apos_aging"] = len(out)

        if self.filtros_inclusao.get("blacklist", True):
            out = self.aplicar_blacklist(out)
        metrics["apos_blacklist"] = len(out)

        return out, metrics

    def aplicar_filtros_baixa(
        self, df: pd.DataFrame
    ) -> Tuple[pd.DataFrame, Dict[str, int]]:
        """Aplica sequência de filtros para baixa."""
        out = df.copy()
        metrics: Dict[str, int] = {"registros_iniciais": len(out)}

        if self.filtros_baixa.get("status_baixa", True):
            out = self.filtrar_status_baixa(out)
        metrics["apos_status_baixa"] = len(out)

        if self.filtros_baixa.get("tipos_validos", True):
            out = self.filtrar_tipos_validos(out)
        metrics["apos_tipos"] = len(out)

        if self.filtros_baixa.get("aging", False):
            out = self.aplicar_aging(out)
        metrics["apos_aging"] = len(out)

        if self.filtros_baixa.get("blacklist", True):
            out = self.aplicar_blacklist(out)
        metrics["apos_blacklist"] = len(out)

        return out, metrics


__all__ = ["VicFilterApplier"]