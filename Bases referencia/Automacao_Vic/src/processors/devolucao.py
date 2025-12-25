"""Módulo de processamento da Devolução (MAX − VIC).

Identifica títulos que estão presentes no MAX tratado e ausentes na VIC
tratada e gera planilha para devolução .
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import logging
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Union

import pandas as pd

from src.config.loader import ConfigLoader
from src.io.packager import ExportacaoService
from src.io.file_manager import FileManager
from src.utils.validator import InconsistenciaManager
from src.utils.logger import get_logger, log_section
from src.utils.anti_join import procv_max_menos_vic
from src.utils.text import normalize_ascii_upper, digits_only
from src.utils.helpers import primeiro_valor, normalizar_data_string, extrair_data_referencia
from src.processors.vic import VicFilterApplier


@dataclass(frozen=True)
class _ExportCfg:
    """Estrutura de configuração de exportação da devolução."""

    prefix: str
    subdir: str
    judicial_subdir: str
    extrajudicial_subdir: str
    geral_subdir: str
    add_timestamp: Optional[bool]
    gerar_geral: bool


class DevolucaoProcessor:
    """Processador para gerar a planilha de devolução MAX − VIC."""

    def __init__(
        self,
        config: Optional[Dict[str, Any]] = None,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        self.config_loader = ConfigLoader()
        self.config = config or self.config_loader.get_config()
        self.logger = logger or get_logger(__name__, self.config)
        self.logger.setLevel(logging.WARNING)

        # Configuração do módulo
        self.devolucao_config = self.config_loader.get_nested_value(
            self.config, "devolucao", {}
        )
        self.global_config = self.config_loader.get_nested_value(
            self.config, "global", {}
        )
        self.paths_config = self.config_loader.get_nested_value(
            self.config, "paths", {}
        )

        self.file_manager = FileManager(self.config)
        self.inconsistencia_manager = InconsistenciaManager(self.config)
        self.exportacao_service = ExportacaoService(self.config, self.file_manager)
        self.filter_applier = VicFilterApplier(self.config, self.logger)

        # Parâmetros
        self.campanha_termo = (self.devolucao_config.get("campanha_termo") or "").strip()
        self.status_excluir = [
            s.upper()
            for s in self.devolucao_config.get("status_excluir", [])
            if str(s).strip()
        ]
        chaves_cfg = self.devolucao_config.get("chaves", {})
        self.ch_vic = chaves_cfg.get("vic", "CHAVE")
        self.ch_max = chaves_cfg.get("max", "PARCELA")
        filtros_max_cfg = self.devolucao_config.get("filtros_max", {}) or {}
        self.aplicar_status_max = filtros_max_cfg.get("status_em_aberto", True)
        export_cfg = self._resolver_export_cfg(self.devolucao_config.get("export", {}) or {})
        self.export_cfg = export_cfg
        self.status_devolucao_fixo = self.devolucao_config.get(
            "status_devolucao_fixo", "98"
        )
        empresa_cfg = self.global_config.get("empresa", {})
        self.cnpj_credor = str(empresa_cfg.get("cnpj", "")).strip()
        self.date_format = self.global_config.get("date_format", "%d/%m/%Y")
        if not self.cnpj_credor:
            raise ValueError('CNPJ da empresa não configurado. Defina global.empresa.cnpj no config.yaml')

        self._judicial_cpfs: set[str] = set()

        self.logger.info("DevolucaoProcessor inicializado")

    # ------------------------------------------------------------------
    def carregar_arquivo(self, caminho: Union[str, Path]) -> pd.DataFrame:
        """Lê CSV ou ZIP usando o ``FileManager``."""

        return self.file_manager.ler_csv_ou_zip(Path(caminho))

    # ------------------------------------------------------------------
    @staticmethod
    def _resolver_export_cfg(raw: Dict[str, Any]) -> _ExportCfg:
        prefix = raw.get("filename_prefix", "vic_devolucao")
        subdir = raw.get("subdir", "devolucao")
        judicial_subdir = raw.get("judicial_subdir", f"{subdir}/jud")
        extrajudicial_subdir = raw.get("extrajudicial_subdir", f"{subdir}/extra")
        geral_subdir = raw.get("geral_subdir", subdir)
        add_timestamp = raw.get("add_timestamp")
        gerar_geral = bool(raw.get("gerar_geral", True))
        return _ExportCfg(
            prefix=prefix,
            subdir=subdir,
            judicial_subdir=judicial_subdir,
            extrajudicial_subdir=extrajudicial_subdir,
            geral_subdir=geral_subdir,
            add_timestamp=add_timestamp,
            gerar_geral=gerar_geral,
        )

    # ------------------------------------------------------------------
    # Funções auxiliares movidas para src.utils.helpers
    # Mantidas aqui apenas para compatibilidade com testes e outros módulos
    @staticmethod
    def _primeiro_valor(series: Optional[pd.Series]) -> Optional[Any]:
        """DEPRECATED: Use src.utils.helpers.primeiro_valor"""
        return primeiro_valor(series)

    def _normalizar_data(self, valor: Any) -> Optional[str]:
        """DEPRECATED: Use src.utils.helpers.normalizar_data_string"""
        return normalizar_data_string(valor)

    # ------------------------------------------------------------------
    def _aplicar_filtros_max(self, df: pd.DataFrame) -> tuple[pd.DataFrame, Dict[str, int]]:
        out = df.copy()
        metrics: Dict[str, int] = {"max_antes_filtros": len(out)}

        if "TIPO_PARCELA" not in out.columns and "TIPO_TITULO" in out.columns:
            out["TIPO_PARCELA"] = out["TIPO_TITULO"]

        if self.aplicar_status_max:
            out = self.filter_applier.filtrar_status_em_aberto_max(out)
        metrics["max_apos_status_aberto"] = len(out)

        return out, metrics

    # ------------------------------------------------------------------
    def _coletar_caminhos_baixa(
        self, baixa_paths: Optional[Union[Dict[str, Any], Sequence[Union[str, Path]], str, Path]]
    ) -> List[Tuple[Path, Optional[str]]]:
        if not baixa_paths:
            return []

        caminhos: List[Tuple[Path, Optional[str]]] = []

        def _add_path(valor: Any, nome_csv: Optional[str] = None) -> None:
            if not valor:
                return
            path = Path(valor)
            if not path.exists():
                self.logger.warning("Arquivo de baixa não encontrado: %s", path)
                return
            caminhos.append((path, nome_csv))

        if isinstance(baixa_paths, (str, Path)):
            _add_path(baixa_paths)
        elif isinstance(baixa_paths, dict):
            internos = baixa_paths.get("arquivos_no_zip") or {}
            zip_path = baixa_paths.get("arquivo_zip") or baixa_paths.get("arquivo_geral")
            if zip_path and internos:
                nomes_unicos = set(internos.values())
                for nome_csv in nomes_unicos:
                    _add_path(zip_path, nome_csv)
            for chave in ("arquivo_geral", "arquivo_judicial", "arquivo_extrajudicial"):
                valor = baixa_paths.get(chave)
                if not valor:
                    continue
                if zip_path and internos and Path(valor) == Path(zip_path):
                    continue
                _add_path(valor)
            extras = baixa_paths.get("arquivos")
            if extras:
                for item in extras:
                    _add_path(item)
        else:
            for item in baixa_paths:
                _add_path(item)

        # Remover duplicados preservando ordem
        vistos: set[Tuple[Path, Optional[str]]] = set()
        unicos: List[Tuple[Path, Optional[str]]] = []
        for caminho in caminhos:
            if caminho not in vistos:
                vistos.add(caminho)
                unicos.append(caminho)
        return unicos

    # ------------------------------------------------------------------
    def _remover_registros_baixa(
        self, df: pd.DataFrame, baixa_paths: Optional[Union[Dict[str, Any], Sequence[Union[str, Path]], str, Path]]
    ) -> tuple[pd.DataFrame, int]:
        caminhos = self._coletar_caminhos_baixa(baixa_paths)
        if not caminhos:
            return df, 0

        chaves_baixa: set[str] = set()
        for caminho, nome_csv in caminhos:
            try:
                if caminho.suffix.lower() == ".zip" and nome_csv:
                    df_baixa = self.file_manager.ler_zip_csv(caminho, nome_csv)
                else:
                    df_baixa = self.file_manager.ler_csv_ou_zip(caminho)
            except Exception as exc:  # pragma: no cover - logging auxiliar
                self.logger.warning("Falha ao carregar baixa %s: %s", caminho, exc)
                continue
            coluna_baixa = self.ch_max
            if coluna_baixa not in df_baixa.columns:
                for candidato in ("NUMERO DOC", "NUMERO_DOC", "CHAVE", "PARCELA"):
                    if candidato in df_baixa.columns:
                        coluna_baixa = candidato
                        break
            if coluna_baixa not in df_baixa.columns:
                self.logger.warning(
                    "Arquivo de baixa %s sem coluna '%s'", caminho, self.ch_max
                )
                continue
            serie = df_baixa[coluna_baixa].astype(str).str.strip()
            chaves_baixa.update(serie.dropna().tolist())

        if not chaves_baixa:
            return df, 0

        serie_dev = df[self.ch_max].astype(str).str.strip()
        mask = ~serie_dev.isin(chaves_baixa)
        removidos = int((~mask).sum())
        return df.loc[mask].copy(), removidos


    # ------------------------------------------------------------------
    def _carregar_cpfs_judiciais(self) -> None:
        if self._judicial_cpfs:
            return

        inputs_config = self.config.get("inputs", {})
        judicial_path_cfg = inputs_config.get("clientes_judiciais_path")

        if judicial_path_cfg:
            judicial_file = Path(judicial_path_cfg)
        else:
            judicial_dir = self.paths_config.get("input", {}).get("judicial")
            if judicial_dir:
                judicial_file = Path(judicial_dir) / "ClientesJudiciais.zip"
            else:
                judicial_file = Path("data/input/judicial/ClientesJudiciais.zip")

        if not judicial_file.is_absolute():
            judicial_file = Path.cwd() / judicial_file

        if not judicial_file.exists():
            self.logger.warning(
                "Arquivo de clientes judiciais não encontrado: %s",
                judicial_file,
            )
            self._judicial_cpfs = set()
            return

        try:
            df_judicial = self.file_manager.ler_csv_ou_zip(judicial_file)
        except Exception as exc:  # pragma: no cover - logging auxiliar
            self.logger.warning("Falha ao carregar clientes judiciais: %s", exc)
            self._judicial_cpfs = set()
            return

        cpf_columns = [col for col in df_judicial.columns if "CPF" in str(col).upper()]
        if not cpf_columns:
            self._judicial_cpfs = set()
            return

        cpfs_norm = digits_only(df_judicial[cpf_columns[0]].dropna())
        cpfs_valid = cpfs_norm[cpfs_norm.str.len().isin({11, 14})]
        self._judicial_cpfs = set(cpfs_valid.tolist())

    # ------------------------------------------------------------------
    def _mask_judicial(self, df: pd.DataFrame) -> pd.Series:
        if "IS_JUDICIAL" in df.columns:
            serie = df["IS_JUDICIAL"].astype(str).str.upper().str.strip()
            return serie.isin({"1", "SIM", "TRUE", "JUDICIAL"})
        if "TIPO_FLUXO" in df.columns:
            serie = df["TIPO_FLUXO"].astype(str).str.upper().str.strip()
            return serie.eq("JUDICIAL")

        self._carregar_cpfs_judiciais()
        if not self._judicial_cpfs:
            return pd.Series([False] * len(df), index=df.index)

        serie = digits_only(
            df.get(
                "CPFCNPJ_CLIENTE",
                df.get("CPF_CNPJ", df.get("CPF/CNPJ", pd.Series("", index=df.index))),
            )
        )
        return serie.isin(self._judicial_cpfs)

    # ------------------------------------------------------------------
    def _dividir_carteiras(
        self, df: pd.DataFrame
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        if df.empty:
            vazio = df.iloc[0:0].copy()
            return vazio, vazio

        mask_jud = self._mask_judicial(df)
        df_jud = df[mask_jud].copy()
        df_ext = df[~mask_jud].copy()
        return df_jud, df_ext

    # ------------------------------------------------------------------
    def _exportar(
        self,
        df_jud: pd.DataFrame,
        df_ext: pd.DataFrame,
        df_geral: pd.DataFrame,
    ) -> Dict[str, Optional[str]]:
        resultados: Dict[str, Optional[str]] = {
            "arquivo_zip": None,
            "arquivo_judicial": None,
            "arquivo_extrajudicial": None,
            "arquivo_geral": None,
            "arquivos_no_zip": None,
        }

        arquivos_zip: Dict[str, pd.DataFrame] = {}
        internos: Dict[str, str] = {}

        if not df_jud.empty:
            nome_csv = f"{self.export_cfg.prefix}_jud.csv"
            arquivos_zip[nome_csv] = df_jud
            internos["arquivo_judicial"] = nome_csv

        if not df_ext.empty:
            nome_csv = f"{self.export_cfg.prefix}_extra.csv"
            arquivos_zip[nome_csv] = df_ext
            internos["arquivo_extrajudicial"] = nome_csv

        if self.export_cfg.gerar_geral and not df_geral.empty:
            nome_csv = f"{self.export_cfg.prefix}.csv"
            arquivos_zip[nome_csv] = df_geral
            internos["arquivo_geral"] = nome_csv

        if arquivos_zip:
            caminho_zip = self.exportacao_service.exportar_zip(
                arquivos_zip,
                nome_base=self.export_cfg.prefix,
                subdir=self.export_cfg.subdir,
                add_timestamp=self.export_cfg.add_timestamp,
            )
            if caminho_zip:
                resultados["arquivo_zip"] = str(caminho_zip)
                resultados["arquivos_no_zip"] = internos
                for chave in ("arquivo_judicial", "arquivo_extrajudicial", "arquivo_geral"):
                    if chave in internos:
                        resultados[chave] = str(caminho_zip)

        return resultados

    def _resolver_data_base(self, df: pd.DataFrame) -> str:
        candidatos_colunas = [
            "DATA_BASE",
            "DATA BASE",
            "DATA_EXTRACAO_BASE",
            "DATA EXTRACAO BASE",
            "DATA_EXTRACAO",
            "DATA EXTRACAO",
        ]

        for coluna in candidatos_colunas:
            if coluna in df.columns:
                valor = self._primeiro_valor(df[coluna])
                normalizado = self._normalizar_data(valor)
                if normalizado:
                    return normalizado

        return datetime.now().strftime(self.date_format)

    def identificar_devolucao(
        self,
        df_vic: pd.DataFrame,
        df_max: pd.DataFrame,
        counts_iniciais: Optional[Dict[str, Any]] = None,
    ) -> pd.DataFrame:
        """Calcula K_dev = MAX − VIC e retorna DataFrame filtrado (PROCV)."""

        self.logger.info("PROCV MAX−VIC: iniciando identificação...")

        # Fail-fast em colunas obrigatórias
        if self.ch_vic not in df_vic.columns:
            raise ValueError(f"Coluna obrigatoria ausente em VIC: {self.ch_vic}")
        if self.ch_max not in df_max.columns:
            raise ValueError(f"Coluna obrigatoria ausente em MAX: {self.ch_max}")
        if self.campanha_termo and "CAMPANHA" not in df_max.columns:
            raise ValueError(
                "Coluna CAMPANHA obrigatoria quando filtro de campanha esta ativo"
            )
        if self.status_excluir and "STATUS_TITULO" not in df_max.columns:
            raise ValueError(
                "Coluna STATUS_TITULO obrigatoria quando filtro de status esta ativo"
            )

        df_max_f = df_max.copy()
        counts: Dict[str, Any] = counts_iniciais.copy() if counts_iniciais else {}
        counts.setdefault("max_antes_filtros", len(df_max_f))

        # Filtro de campanha
        if self.campanha_termo:
            import re

            camp = normalize_ascii_upper(df_max_f["CAMPANHA"])
            termo = normalize_ascii_upper(pd.Series([self.campanha_termo])).iloc[0]
            antes = len(df_max_f)
            df_max_f = df_max_f[camp.str.contains(re.escape(termo), na=False)]
            counts["max_apos_campanha"] = len(df_max_f)
            self.logger.info(
                "MAX apos filtro CAMPANHA contendo '%s': %s (filtrados %s)",
                self.campanha_termo,
                f"{len(df_max_f):,}",
                f"{antes - len(df_max_f):,}",
            )
        # Filtro de status
        if self.status_excluir:
            st = normalize_ascii_upper(df_max_f["STATUS_TITULO"])
            mask = st.isin(self.status_excluir)
            if mask.any():
                antes = len(df_max_f)
                df_max_f = df_max_f[~mask].copy()
                self.logger.info(
                    "MAX apos exclusao de status %s: %s (removidos %s)",
                    self.status_excluir,
                    f"{len(df_max_f):,}",
                    f"{antes - len(df_max_f):,}",
                )
        if self.status_excluir:
            counts["max_apos_status_excluir"] = len(df_max_f)

        # PROCV: MAX − VIC
        df_out = procv_max_menos_vic(df_max_f, df_vic, self.ch_max, self.ch_vic)

        counts["registros_devolucao"] = len(df_out)
        self.logger.info("PROCV MAX−VIC: %s registros", f"{len(df_out):,}")

        # Armazena contadores para uso posterior (processar)
        self.metrics_ultima_execucao = counts

        return df_out

    # ------------------------------------------------------------------
    def formatar_devolucao(self, df: pd.DataFrame) -> pd.DataFrame:
        """Formata o DataFrame de devolução para o layout final."""

        if df.empty:
            return pd.DataFrame()

        data_devolucao = self._resolver_data_base(df)

        layout_cols = [
            "CNPJ CREDOR",
            "CPFCNPJ CLIENTE",
            "NOME / RAZAO SOCIAL",
            "PARCELA",
            "VENCIMENTO",
            "VALOR",
            "TIPO PARCELA",
            "DATA DEVOLUCAO",
            "STATUS",
        ]

        out = pd.DataFrame(index=df.index)
        out["CNPJ CREDOR"] = self.cnpj_credor.strip()
        out["CPFCNPJ CLIENTE"] = df.get(
            "CPFCNPJ_CLIENTE", pd.Series("", index=df.index)
        )
        nome_serie = df.get(
            "NOME_RAZAO_SOCIAL",
            df.get("NOME", pd.Series("", index=df.index)),
        )
        out["NOME / RAZAO SOCIAL"] = nome_serie
        out["PARCELA"] = df[self.ch_max]
        venc_series = df.get("VENCIMENTO", pd.Series("", index=df.index))
        if pd.api.types.is_datetime64_any_dtype(venc_series):
            out["VENCIMENTO"] = venc_series.dt.strftime("%d/%m/%Y")
        else:
            out["VENCIMENTO"] = pd.to_datetime(
                venc_series, errors="coerce"
            ).dt.strftime("%d/%m/%Y")
        out["VALOR"] = df.get("VALOR", pd.Series("", index=df.index))
        tipo_series = df.get("TIPO_PARCELA")
        if tipo_series is None:
            # Não criar default; manter vazio quando ausente
            out["TIPO PARCELA"] = ""
        elif isinstance(tipo_series, str):
            out["TIPO PARCELA"] = tipo_series.upper()
        else:
            out["TIPO PARCELA"] = tipo_series.astype(str).str.upper()
        out["DATA DEVOLUCAO"] = data_devolucao
        out["STATUS"] = self.status_devolucao_fixo

        return out[layout_cols].reset_index(drop=True)

    # ------------------------------------------------------------------
    def processar(
        self,
        vic_path: Union[str, Path],
        max_path: Union[str, Path],
        baixa_paths: Optional[Union[Dict[str, Any], Sequence[Union[str, Path]], str, Path]] = None,
    ) -> Dict[str, Any]:
        """Executa a pipeline completa de devolução."""

        inicio = datetime.now()
        self.logger.info("Iniciando pipeline de devolucao...")

        df_vic_raw = self.carregar_arquivo(vic_path)
        vic_filter = self.filter_applier
        df_vic, vic_metrics = vic_filter.aplicar_filtros_inclusao(df_vic_raw)

        df_max_raw = self.carregar_arquivo(max_path)
        df_max_filtrado, max_metrics = self._aplicar_filtros_max(df_max_raw)

        df_dev_raw = self.identificar_devolucao(
            df_vic, df_max_filtrado, counts_iniciais=max_metrics
        )

        df_dev_sem_baixa, removidos_baixa = self._remover_registros_baixa(
            df_dev_raw, baixa_paths
        )

        df_jud_raw, df_ext_raw = self._dividir_carteiras(df_dev_sem_baixa)

        df_geral_layout = self.formatar_devolucao(df_dev_sem_baixa)
        df_jud_layout = self.formatar_devolucao(df_jud_raw)
        df_ext_layout = self.formatar_devolucao(df_ext_raw)

        export_paths = self._exportar(df_jud_layout, df_ext_layout, df_geral_layout)

        metrics = getattr(self, "metrics_ultima_execucao", {}).copy()
        registros_devolucao_bruto = metrics.get('registros_devolucao', len(df_dev_raw))
        metrics.update(
            {
                'vic_registros_iniciais': vic_metrics.get('registros_iniciais', len(df_vic_raw)),
                'vic_apos_status': vic_metrics.get('apos_status', len(df_vic)),
                'vic_apos_tipos': vic_metrics.get('apos_tipos', len(df_vic)),
                'vic_apos_aging': vic_metrics.get('apos_aging', len(df_vic)),
                'vic_apos_blacklist': vic_metrics.get('apos_blacklist', len(df_vic)),
                'max_registros_iniciais': max_metrics.get('max_antes_filtros', len(df_max_raw)),
                'max_apos_status_aberto': max_metrics.get('max_apos_status_aberto', len(df_max_filtrado)),
                'removidos_por_baixa': removidos_baixa,
                'registros_devolucao_bruto': registros_devolucao_bruto,
            }
        )
        metrics.pop('registros_devolucao', None)

        duracao = (datetime.now() - inicio).total_seconds()
        stats = {
            "registros_vic": len(df_vic),
            "registros_max": len(df_max_filtrado),
            "registros_devolucao": len(df_dev_sem_baixa),
            "judicial": len(df_jud_raw),
            "extrajudicial": len(df_ext_raw),
            "removidos_por_baixa": removidos_baixa,
            "arquivo_zip": export_paths.get("arquivo_zip"),
            "arquivos_no_zip": export_paths.get("arquivos_no_zip") or {},
            "arquivo_judicial": export_paths.get("arquivo_judicial"),
            "arquivo_extrajudicial": export_paths.get("arquivo_extrajudicial"),
            "arquivo_geral": export_paths.get("arquivo_geral"),
            # Compatibilidade com testes e outros módulos: chave padrão
            # 'arquivo_gerado' apontará para o ZIP consolidado quando disponível,
            # caso contrário utiliza o caminho do arquivo geral.
            "arquivo_gerado": export_paths.get("arquivo_zip") or export_paths.get("arquivo_geral"),
            "duracao": duracao,
            **metrics,
        }

        # Summary
        log_section(self.logger, "DEVOLUÇÃO - MAX - VIC")
        print("📌 Etapa 6 — Devolução MAX−VIC (RIGHT ANTI-JOIN)")
        print("")
        vic_iniciais = metrics["vic_registros_iniciais"]
        print(f"VIC base limpa recebida: {vic_iniciais:,} registros")
        if vic_filter.filtros_inclusao.get("status_em_aberto", True):
            print(f"Após STATUS em aberto: {metrics['vic_apos_status']:,}")
        else:
            print("Filtro STATUS (devolução) desabilitado")
        if vic_filter.filtros_inclusao.get("tipos_validos", True) and vic_filter.tipos_validos:
            print(
                f"Após filtro TIPO ({', '.join(vic_filter.tipos_validos)}): {metrics['vic_apos_tipos']:,}"
            )
        elif not vic_filter.filtros_inclusao.get("tipos_validos", True):
            print("Filtro TIPO (devolução) desabilitado")
        if vic_filter.filtros_inclusao.get("aging", True):
            print(
                f"Após filtro AGING > {vic_filter.aging_minimo} dias: {metrics['vic_apos_aging']:,}"
            )
        else:
            print("Filtro AGING (devolução) desabilitado")
        if vic_filter.filtros_inclusao.get("blacklist", True):
            removidos = metrics['vic_apos_aging'] - metrics['vic_apos_blacklist']
            print(
                f"Após filtro Blacklist: {metrics['vic_apos_blacklist']:,} (removidos: {removidos:,})"
            )
        else:
            print("Filtro Blacklist (devolução) desabilitado")
        print(f"VIC tratado para devolução: {len(df_vic):,} registros")
        print(
            f"MAX tratado (antes de filtros): {metrics['max_registros_iniciais']:,} registros"
        )
        if self.aplicar_status_max:
            print(
                f"MAX após STATUS em aberto: {metrics['max_apos_status_aberto']:,} registros"
            )
        else:
            print("Filtro STATUS (MAX) desabilitado")
        if self.campanha_termo:
            print(
                f"MAX após filtro CAMPANHA: {metrics.get('max_apos_campanha', len(df_max_filtrado)):,} registros"
            )
        if self.status_excluir:
            max_apos_status = metrics.get('max_apos_status_excluir', len(df_max_filtrado))
            print(f"MAX após exclusão de status: {max_apos_status:,} registros")
        print(
            f"Registros identificados antes da baixa: {metrics['registros_devolucao_bruto']:,}"
        )
        print(
            f"Registros identificados para devolução (após baixa): {stats['registros_devolucao']:,}"
        )
        if removidos_baixa:
            print(f"Registros removidos por baixa: {removidos_baixa:,}")
        base_max = (
            metrics.get('max_apos_status_aberto') if self.aplicar_status_max 
            else metrics.get('max_registros_iniciais', 0)
        )
        max_status = base_max or metrics.get('max_registros_iniciais', 0)
        taxa_dev = (stats['registros_devolucao'] / max_status * 100) if max_status else 0.0
        print(f"🔹 Taxa de devolução: {taxa_dev:.2f}%")
        print(
            f"Divisão por carteira: Judicial = {len(df_jud_raw):,} | Extrajudicial = {len(df_ext_raw):,}"
        )
        print("")
        if export_paths.get("arquivo_zip"):
            conteudos = export_paths.get("arquivos_no_zip", {})
            nomes = [
                conteudos.get("arquivo_geral"),
                conteudos.get("arquivo_judicial"),
                conteudos.get("arquivo_extrajudicial"),
            ]
            nomes = [nome for nome in nomes if nome]
            if nomes:
                print(f"📦 Exportado: {export_paths['arquivo_zip']} ({', '.join(nomes)})")
            else:
                print(f"📦 Exportado: {export_paths['arquivo_zip']}")
        print(f"⏱️Duração: {duracao:.2f}s")

        return stats

