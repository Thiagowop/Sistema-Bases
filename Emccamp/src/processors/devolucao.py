"""Módulo de processamento da Devolução (MAX − EMCCAMP).

Identifica títulos que estão presentes no MAX tratado e ausentes na EMCCAMP
tratada e gera planilha para devolução.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional, Sequence, Tuple, Union

import pandas as pd

from src.config.loader import ConfigLoader, LoadedConfig
from src.utils import digits_only, procv_max_menos_emccamp
from src.utils.helpers import extrair_data_referencia, primeiro_valor
from src.utils.io import DatasetIO
from src.utils.logger import get_logger
from src.utils.output_formatter import OutputFormatter
from src.utils.path_manager import PathManager


@dataclass
class DevolucaoStats:
    """Estatísticas da execução da devolução."""
    
    emccamp_registros_iniciais: int
    emccamp_apos_filtros: int
    max_registros_iniciais: int
    max_apos_filtros: int
    registros_devolucao_bruto: int
    removidos_por_baixa: int
    registros_devolucao: int
    judicial: int
    extrajudicial: int
    arquivo_zip: Optional[Path]
    arquivos_no_zip: Dict[str, str]
    duracao: float


class DevolucaoProcessor:
    """Processador para gerar a planilha de devolução MAX − EMCCAMP."""

    def __init__(self, config: LoadedConfig) -> None:
        self.config = config

        # Configuração do módulo
        self.devolucao_config = self.config.get("devolucao", {})
        self.global_config = self.config.get("global", {})

        self.paths = PathManager(config.base_path, config.data)

        self.emccamp_dir = self.paths.resolve_output("emccamp_tratada", "emccamp_tratada")
        self.max_dir = self.paths.resolve_output("max_tratada", "max_tratada")
        self.devolucao_dir = self.paths.resolve_output("devolucao", "devolucao")
        self.judicial_dir = self.paths.resolve_input("judicial", "data/input/judicial")

        logging_cfg = config.get("logging", {})
        logs_dir = self.paths.resolve_logs()
        self.logger = get_logger("devolucao_emccamp", logs_dir, logging_cfg)

        global_cfg = config.get("global", {})
        self.encoding = global_cfg.get("encoding", "utf-8-sig")
        self.separator = global_cfg.get("csv_separator", ";")
        self.date_format = global_cfg.get("date_format", "%d/%m/%Y")
        
        empresa_cfg = global_cfg.get("empresa", {})
        self.cnpj_credor = str(empresa_cfg.get("cnpj", "")).strip()
        if not self.cnpj_credor:
            raise ValueError("CNPJ da empresa não configurado. Defina global.empresa.cnpj no config.yaml")

        self.io = DatasetIO(separator=self.separator, encoding=self.encoding)

        # Parâmetros de devolução
        self.campanha_termo = (self.devolucao_config.get("campanha_termo") or "").strip()
        self.status_excluir = [
            s.upper()
            for s in self.devolucao_config.get("status_excluir", [])
            if str(s).strip()
        ]
        
        chaves_cfg = self.devolucao_config.get("chaves", {})
        self.ch_emccamp = chaves_cfg.get("emccamp", "CHAVE")
        self.ch_max = chaves_cfg.get("max", "CHAVE")
        
        # Filtros
        filtros_max_cfg = self.devolucao_config.get("filtros_max", {}) or {}
        self.aplicar_status_max = filtros_max_cfg.get("status_em_aberto", True)
        
        filtros_emccamp_cfg = self.devolucao_config.get("filtros_emccamp", {}) or {}
        self.aplicar_status_emccamp = filtros_emccamp_cfg.get("status_em_aberto", True)
        
        # Exportação
        export_cfg = self.devolucao_config.get("export", {}) or {}
        self.export_prefix = export_cfg.get("filename_prefix", "emccamp_devolucao")
        self.export_subdir = export_cfg.get("subdir", "devolucao")
        self.gerar_geral = bool(export_cfg.get("gerar_geral", True))
        
        self.status_devolucao_fixo = self.devolucao_config.get("status_devolucao_fixo", "98")
        self.remover_por_baixa = bool(self.devolucao_config.get("remover_por_baixa", True))

        self._judicial_cpfs: set[str] = set()

    def process(self) -> DevolucaoStats:
        """Executa a pipeline completa de devolução."""
        inicio = datetime.now()
        self.logger.info("Iniciando pipeline de devolução MAX - EMCCAMP...")

        # Carregar arquivos
        emccamp_path = self._resolve_file(self.emccamp_dir, "emccamp_tratada_*.zip")
        max_path = self._resolve_file(self.max_dir, "max_tratada_*.zip")

        df_emccamp_raw = self.io.read(emccamp_path)
        df_max_raw = self.io.read(max_path)

        # Aplicar filtros
        df_emccamp_filtrado, emccamp_metrics = self._aplicar_filtros_emccamp(df_emccamp_raw)
        df_max_filtrado, max_metrics = self._aplicar_filtros_max(df_max_raw)

        # PROCV: MAX − EMCCAMP
        df_devolucao_raw = self._identificar_devolucao(df_emccamp_filtrado, df_max_filtrado)

        # Remover por baixa (se configurado)
        df_devolucao_sem_baixa, removidos_baixa = self._remover_registros_baixa(df_devolucao_raw)

        # Dividir carteiras
        self._carregar_cpfs_judiciais()
        df_judicial_raw, df_extrajudicial_raw = self._dividir_carteiras(df_devolucao_sem_baixa)

        # Formatar layout
        df_geral_layout = self._formatar_devolucao(df_devolucao_sem_baixa)
        df_judicial_layout = self._formatar_devolucao(df_judicial_raw)
        df_extrajudicial_layout = self._formatar_devolucao(df_extrajudicial_raw)

        # Exportar
        arquivo_zip, arquivos_no_zip = self._exportar(
            df_judicial_layout,
            df_extrajudicial_layout,
            df_geral_layout
        )

        duracao = (datetime.now() - inicio).total_seconds()

        stats = DevolucaoStats(
            emccamp_registros_iniciais=len(df_emccamp_raw),
            emccamp_apos_filtros=len(df_emccamp_filtrado),
            max_registros_iniciais=len(df_max_raw),
            max_apos_filtros=len(df_max_filtrado),
            registros_devolucao_bruto=len(df_devolucao_raw),
            removidos_por_baixa=removidos_baixa,
            registros_devolucao=len(df_devolucao_sem_baixa),
            judicial=len(df_judicial_raw),
            extrajudicial=len(df_extrajudicial_raw),
            arquivo_zip=arquivo_zip,
            arquivos_no_zip=arquivos_no_zip,
            duracao=duracao,
        )

        self._show_summary(stats)
        return stats

    def _resolve_file(self, directory: Path, pattern: str) -> Path:
        """Resolve o arquivo mais recente que corresponde ao padrão."""
        if not directory.exists():
            raise FileNotFoundError(f"Diretório não encontrado: {directory}")

        candidates = sorted(
            directory.glob(pattern),
            key=lambda path: path.stat().st_mtime,
            reverse=True
        )
        if not candidates:
            raise FileNotFoundError(f"Nenhum arquivo correspondente a {pattern} em {directory}")
        
        return candidates[0]

    def _aplicar_filtros_emccamp(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, int]]:
        """Aplica filtros configurados na base EMCCAMP."""
        out = df.copy()
        metrics: Dict[str, int] = {"emccamp_antes_filtros": len(out)}

        if self.aplicar_status_emccamp:
            # Filtrar por status em aberto (se a coluna existir)
            if "STATUS_TITULO" in out.columns:
                # Normalizar status
                status_norm = out["STATUS_TITULO"].astype(str).str.strip().str.upper()
                # Manter apenas registros com status indicando "aberto" ou similar
                # Ajustar conforme necessário para o seu contexto
                mask_aberto = status_norm.isin(["ABERTO", "EM ABERTO", "VENCIDO", "A VENCER"])
                out = out[mask_aberto].copy()
                self.logger.info(
                    "EMCCAMP após filtro STATUS em aberto: %s registros",
                    f"{len(out):,}"
                )

        metrics["emccamp_apos_filtros"] = len(out)
        return out, metrics

    def _aplicar_filtros_max(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, int]]:
        """Aplica filtros configurados na base MAX."""
        out = df.copy()
        metrics: Dict[str, int] = {"max_antes_filtros": len(out)}

        if self.aplicar_status_max:
            # Filtrar por status em aberto (se a coluna existir)
            if "STATUS_TITULO" in out.columns:
                status_norm = out["STATUS_TITULO"].astype(str).str.strip().str.upper()
                mask_aberto = status_norm.isin(["ABERTO", "EM ABERTO", "VENCIDO", "A VENCER"])
                out = out[mask_aberto].copy()
                self.logger.info(
                    "MAX após filtro STATUS em aberto: %s registros",
                    f"{len(out):,}"
                )

        # Filtro de campanha (se configurado)
        if self.campanha_termo and "CAMPANHA" in out.columns:
            from src.utils.text import normalize_ascii_upper
            import re

            camp = normalize_ascii_upper(out["CAMPANHA"])
            termo = normalize_ascii_upper(pd.Series([self.campanha_termo])).iloc[0]
            antes = len(out)
            out = out[camp.str.contains(re.escape(termo), na=False)].copy()
            self.logger.info(
                "MAX após filtro CAMPANHA contendo '%s': %s (filtrados %s)",
                self.campanha_termo,
                f"{len(out):,}",
                f"{antes - len(out):,}",
            )

        # Filtro de status a excluir
        if self.status_excluir and "STATUS_TITULO" in out.columns:
            from src.utils.text import normalize_ascii_upper
            
            st = normalize_ascii_upper(out["STATUS_TITULO"])
            mask = st.isin(self.status_excluir)
            if mask.any():
                antes = len(out)
                out = out[~mask].copy()
                self.logger.info(
                    "MAX após exclusão de status %s: %s (removidos %s)",
                    self.status_excluir,
                    f"{len(out):,}",
                    f"{antes - len(out):,}",
                )

        metrics["max_apos_filtros"] = len(out)
        return out, metrics

    def _identificar_devolucao(
        self,
        df_emccamp: pd.DataFrame,
        df_max: pd.DataFrame,
    ) -> pd.DataFrame:
        """Calcula MAX − EMCCAMP e retorna DataFrame filtrado (PROCV)."""
        self.logger.info("PROCV MAX−EMCCAMP: iniciando identificação...")

        # Validações
        if self.ch_emccamp not in df_emccamp.columns:
            raise ValueError(f"Coluna obrigatória ausente em EMCCAMP: {self.ch_emccamp}")
        if self.ch_max not in df_max.columns:
            raise ValueError(f"Coluna obrigatória ausente em MAX: {self.ch_max}")

        # PROCV: MAX − EMCCAMP
        df_out = procv_max_menos_emccamp(
            df_max,
            df_emccamp,
            col_max=self.ch_max,
            col_emccamp=self.ch_emccamp
        )

        self.logger.info("PROCV MAX−EMCCAMP: %s registros", f"{len(df_out):,}")
        return df_out

    def _remover_registros_baixa(
        self,
        df: pd.DataFrame,
    ) -> Tuple[pd.DataFrame, int]:
        """Remove registros presentes no arquivo de baixa."""
        if not self.remover_por_baixa:
            return df, 0

        # Tentar carregar arquivo de baixa
        inputs_config = self.config.get("inputs", {})
        baixa_path_cfg = inputs_config.get("baixa_emccamp_path")
        
        if not baixa_path_cfg:
            # Fallback: procurar no diretório de baixas
            baixas_dir = self.paths.resolve_input("baixas", "data/input/baixas")
            if baixas_dir.exists():
                candidates = sorted(
                    list(baixas_dir.glob("baixa_emccamp_*.zip")) + 
                    list(baixas_dir.glob("baixa_emccamp.zip")),
                    key=lambda p: p.stat().st_mtime,
                    reverse=True
                )
                if candidates:
                    baixa_path = candidates[0]
                else:
                    self.logger.info("Arquivo de baixa não encontrado. Continuando sem remover registros.")
                    return df, 0
            else:
                return df, 0
        else:
            baixa_path = Path(baixa_path_cfg)
            if not baixa_path.exists():
                self.logger.warning("Arquivo de baixa configurado não existe: %s", baixa_path)
                return df, 0

        try:
            df_baixa = self.io.read(baixa_path)
        except Exception as exc:
            self.logger.warning("Falha ao carregar baixa %s: %s", baixa_path, exc)
            return df, 0

        # Identificar coluna de chave na baixa
        coluna_baixa = self.ch_max
        if coluna_baixa not in df_baixa.columns:
            for candidato in ("CHAVE", "PARCELA", "NUMERO_DOC", "NUMERO DOC"):
                if candidato in df_baixa.columns:
                    coluna_baixa = candidato
                    break

        if coluna_baixa not in df_baixa.columns:
            self.logger.warning(
                "Arquivo de baixa %s sem coluna '%s'. Ignorando.",
                baixa_path,
                self.ch_max
            )
            return df, 0

        # Criar conjunto de chaves da baixa
        chaves_baixa = set(
            df_baixa[coluna_baixa]
            .astype(str)
            .str.strip()
            .dropna()
            .tolist()
        )

        if not chaves_baixa:
            return df, 0

        # Filtrar registros
        serie_dev = df[self.ch_max].astype(str).str.strip()
        mask = ~serie_dev.isin(chaves_baixa)
        removidos = int((~mask).sum())

        return df.loc[mask].copy(), removidos

    def _carregar_cpfs_judiciais(self) -> None:
        """Carrega CPFs/CNPJs de clientes judiciais."""
        if self._judicial_cpfs:
            return

        judicial_file = self.judicial_dir / "ClientesJudiciais.zip"

        if not judicial_file.exists():
            self.logger.info(
                "Arquivo de clientes judiciais não encontrado: %s. Todos serão extrajudiciais.",
                judicial_file,
            )
            self._judicial_cpfs = set()
            return

        try:
            df_judicial = self.io.read(judicial_file)
        except Exception as exc:
            self.logger.warning("Falha ao carregar clientes judiciais: %s", exc)
            self._judicial_cpfs = set()
            return

        # Procurar coluna de CPF/CNPJ
        cpf_columns = [
            col for col in df_judicial.columns
            if "CPF" in str(col).upper() or "CNPJ" in str(col).upper()
        ]
        if not cpf_columns:
            self._judicial_cpfs = set()
            return

        cpfs_norm = digits_only(df_judicial[cpf_columns[0]].dropna())
        cpfs_valid = cpfs_norm[cpfs_norm.str.len().isin({11, 14})]
        self._judicial_cpfs = set(cpfs_valid.tolist())

    def _dividir_carteiras(
        self,
        df: pd.DataFrame
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Divide DataFrame em judicial e extrajudicial."""
        if df.empty:
            vazio = df.iloc[0:0].copy()
            return vazio, vazio

        # Identificar coluna de CPF/CNPJ
        cpf_col = None
        for candidato in ("CPF_CNPJ", "CPFCNPJ_CLIENTE", "CPFCNPJ CLIENTE"):
            if candidato in df.columns:
                cpf_col = candidato
                break

        if not cpf_col:
            # Sem coluna de CPF, todos extrajudiciais
            return df.iloc[0:0].copy(), df.copy()

        # Normalizar CPF/CNPJ e verificar se está na lista judicial
        serie = digits_only(df[cpf_col].fillna(""))
        mask_judicial = serie.isin(self._judicial_cpfs)

        df_judicial = df[mask_judicial].copy()
        df_extrajudicial = df[~mask_judicial].copy()

        return df_judicial, df_extrajudicial

    def _formatar_devolucao(self, df: pd.DataFrame) -> pd.DataFrame:
        """Formata o DataFrame de devolução para o layout final."""
        if df.empty:
            return pd.DataFrame()

        data_devolucao = extrair_data_referencia(df, formato_saida=self.date_format)

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

        # CPF/CNPJ
        cpf_col = None
        for candidato in ("CPF_CNPJ", "CPFCNPJ_CLIENTE", "CPFCNPJ CLIENTE"):
            if candidato in df.columns:
                cpf_col = candidato
                break
        out["CPFCNPJ CLIENTE"] = df.get(cpf_col, pd.Series("", index=df.index)) if cpf_col else ""

        # Nome
        nome_serie = df.get(
            "NOME_RAZAO_SOCIAL",
            df.get("NOME", df.get("CLIENTE", pd.Series("", index=df.index))),
        )
        out["NOME / RAZAO SOCIAL"] = nome_serie

        # Parcela (chave do MAX)
        out["PARCELA"] = df[self.ch_max]

        # Vencimento
        venc_col = None
        for candidato in ("DATA_VENCIMENTO", "VENCIMENTO"):
            if candidato in df.columns:
                venc_col = candidato
                break
        
        if venc_col:
            venc_series = df[venc_col]
            if pd.api.types.is_datetime64_any_dtype(venc_series):
                out["VENCIMENTO"] = venc_series.dt.strftime("%d/%m/%Y")
            else:
                out["VENCIMENTO"] = pd.to_datetime(
                    venc_series, errors="coerce"
                ).dt.strftime("%d/%m/%Y")
        else:
            out["VENCIMENTO"] = ""

        # Valor
        out["VALOR"] = df.get("VALOR", pd.Series("", index=df.index))

        # Tipo Parcela
        tipo_series = df.get("TIPO_PARCELA")
        if tipo_series is None:
            out["TIPO PARCELA"] = ""
        elif isinstance(tipo_series, str):
            out["TIPO PARCELA"] = tipo_series.upper()
        else:
            out["TIPO PARCELA"] = tipo_series.astype(str).str.upper()

        out["DATA DEVOLUCAO"] = data_devolucao
        out["STATUS"] = self.status_devolucao_fixo

        return out[layout_cols].reset_index(drop=True)

    def _exportar(
        self,
        df_judicial: pd.DataFrame,
        df_extrajudicial: pd.DataFrame,
        df_geral: pd.DataFrame,
    ) -> Tuple[Optional[Path], Dict[str, str]]:
        """Exporta arquivos de devolução."""
        if df_judicial.empty and df_extrajudicial.empty and df_geral.empty:
            return None, {}

        self.devolucao_dir.mkdir(parents=True, exist_ok=True)

        # Limpar arquivos antigos
        PathManager.cleanup(
            self.devolucao_dir,
            f"{self.export_prefix}_*.zip",
            self.logger,
            silent=True
        )

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        zip_path = self.devolucao_dir / f"{self.export_prefix}_{timestamp}.zip"

        arquivos_zip: Dict[str, pd.DataFrame] = {}
        internos: Dict[str, str] = {}

        if not df_judicial.empty:
            nome_csv = f"{self.export_prefix}_jud.csv"
            arquivos_zip[nome_csv] = df_judicial
            internos["arquivo_judicial"] = nome_csv

        if not df_extrajudicial.empty:
            nome_csv = f"{self.export_prefix}_extra.csv"
            arquivos_zip[nome_csv] = df_extrajudicial
            internos["arquivo_extrajudicial"] = nome_csv

        if self.gerar_geral and not df_geral.empty:
            nome_csv = f"{self.export_prefix}.csv"
            arquivos_zip[nome_csv] = df_geral
            internos["arquivo_geral"] = nome_csv

        if arquivos_zip:
            self.io.write_zip(arquivos_zip, zip_path)
            return zip_path, internos

        return None, {}

    def _show_summary(self, stats: DevolucaoStats) -> None:
        """Exibe resumo da execução."""
        print("\n" + "=" * 80)
        print("DEVOLUCAO MAX - EMCCAMP")
        print("=" * 80)
        print()
        
        print(f"EMCCAMP base recebida: {OutputFormatter.format_count(stats.emccamp_registros_iniciais)}")
        if self.aplicar_status_emccamp:
            print(f"Após filtro STATUS em aberto: {OutputFormatter.format_count(stats.emccamp_apos_filtros)}")
        
        print(f"\nMAX base recebida: {OutputFormatter.format_count(stats.max_registros_iniciais)}")
        if self.aplicar_status_max:
            print(f"Após filtro STATUS em aberto: {OutputFormatter.format_count(stats.max_apos_filtros)}")
        
        print(f"\nRegistros identificados para devolucao (antes baixa): {OutputFormatter.format_count(stats.registros_devolucao_bruto)}")
        
        if stats.removidos_por_baixa > 0:
            print(f"Registros removidos por baixa: {OutputFormatter.format_count(stats.removidos_por_baixa)}")
        
        print(f"Registros identificados para devolucao (apos baixa): {OutputFormatter.format_count(stats.registros_devolucao)}")
        
        if stats.max_apos_filtros > 0:
            taxa_dev = (stats.registros_devolucao / stats.max_apos_filtros * 100)
            print(f"Taxa de devolucao: {taxa_dev:.2f}%")
        
        print(f"\nDivisao por carteira:")
        print(f"  Judicial: {OutputFormatter.format_count(stats.judicial)}")
        print(f"  Extrajudicial: {OutputFormatter.format_count(stats.extrajudicial)}")
        
        if stats.arquivo_zip:
            print(f"\nArquivo exportado: {stats.arquivo_zip}")
            if stats.arquivos_no_zip:
                print(f"   Conteudo: {', '.join(stats.arquivos_no_zip.values())}")
        
        print(f"\nDuracao: {stats.duracao:.2f}s")
        print("=" * 80)
        print()


def run(config_loader: Optional[ConfigLoader] = None) -> DevolucaoStats:
    """Executa o processamento de devolução."""
    loader = config_loader or ConfigLoader()
    config = loader.load()
    processor = DevolucaoProcessor(config)
    return processor.process()
