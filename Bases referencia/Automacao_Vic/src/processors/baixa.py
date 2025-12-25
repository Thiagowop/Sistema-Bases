"""Processador da etapa de Baixa (VIC ↓ × MAX ↑).

Responsável por identificar parcelas baixadas na VIC que ainda constam
em aberto na base MAX. A saída é o layout ``vic_baixa`` dividido em
duas planilhas (judicial e extrajudicial) conforme o padrão utilizado
pelas demais etapas do pipeline.
"""

from __future__ import annotations

from dataclasses import dataclass
import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, Optional, Sequence, Tuple

import pandas as pd

from src.config.loader import ConfigLoader
from src.io.file_manager import FileManager
from src.io.packager import ExportacaoService
from src.utils.filters import VicFilterApplier
from src.utils import get_logger, log_section, digits_only, formatar_datas_serie


@dataclass(frozen=True)
class _ExportPaths:
    """Convenience structure for diretórios/prefixos de exportação."""

    prefix: str
    base_subdir: str
    judicial_subdir: str
    extrajudicial_subdir: str
    geral_subdir: str
    add_timestamp: Optional[bool]
    gerar_geral: bool


class BaixaProcessor:
    """Executa a etapa de baixa (VIC baixado contra MAX em aberto)."""

    LAYOUT_COLUMNS: Sequence[str] = (
        "NOME CLIENTE",
        "CPF/CNPJ CLIENTE",
        "CNPJ CREDOR",
        "NUMERO DOC",
        "VALOR DA PARCELA",
        "DT. VENCIMENTO",
        "STATUS ACORDO",
        "DT. PAGAMENTO",
        "VALOR RECEBIDO",
    )

    def __init__(
        self,
        config: Optional[Dict[str, Any]] = None,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        loader = ConfigLoader()
        self.config = config or loader.get_config()
        self.logger = logger or get_logger(__name__, self.config)
        self.logger.setLevel(logging.WARNING)

        self.file_manager = FileManager(self.config)
        self.exportacao_service = ExportacaoService(self.config, self.file_manager)
        self.filter_applier = VicFilterApplier(self.config, self.logger)

        self.global_cfg = loader.get_nested_value(self.config, "global", {})
        self.baixa_cfg = loader.get_nested_value(
            self.config, "baixa_processor", {}
        )
        self.paths_cfg = loader.get_nested_value(self.config, "paths", {})

        empresa_cfg = self.global_cfg.get("empresa", {})
        self.cnpj_credor: str = str(empresa_cfg.get("cnpj", "")).strip()
        status_padrao_cfg = self.baixa_cfg.get("status_acordo_padrao", "2")
        self.status_acordo_padrao: str = (
            str(status_padrao_cfg).strip() if str(status_padrao_cfg).strip() else "2"
        )

        self.date_format = self.global_cfg.get("date_format", "%d/%m/%Y")
        self.encoding = self.global_cfg.get("encoding", "utf-8-sig")
        self.csv_separator = self.global_cfg.get("csv_separator", ";")

        chave_cfg = self.baixa_cfg.get("chave", {})
        self.chave_vic = chave_cfg.get("vic", "CHAVE")
        self.chave_max = chave_cfg.get("max", "PARCELA")
        self.key_column_name = chave_cfg.get("coluna_auxiliar", "__CHAVE_BAIXA__")
        combination_cfg = chave_cfg.get("combination", {})
        self.combination_vic: Sequence[Sequence[str]] = tuple(
            self._normalize_combination(combination_cfg.get("vic", []))
        )
        self.combination_max: Sequence[Sequence[str]] = tuple(
            self._normalize_combination(combination_cfg.get("max", []))
        )
        filtros_max_cfg = self.baixa_cfg.get("filtros_max", {})
        self.aplicar_filtro_status_max = filtros_max_cfg.get(
            "status_em_aberto", True
        )

        export_cfg = self._resolve_export_cfg(self.baixa_cfg.get("export", {}))
        self.export_cfg = export_cfg

        self.include_status_max = bool(self.baixa_cfg.get("incluir_status_max"))
        self.campanha_prefix: Optional[str] = self.baixa_cfg.get(
            "campanha_prefix"
        )
        self.campanha_override: Optional[str] = self.baixa_cfg.get("campanha")

        self._judicial_cpfs: set[str] = set()

    # ------------------------------------------------------------------
    @staticmethod
    def _normalize_combination(value: Any) -> Iterable[Sequence[str]]:
        if not value:
            return []
        normalized: list[Sequence[str]] = []
        if isinstance(value, (str, bytes)):
            value = [value]
        for entry in value:
            if isinstance(entry, (list, tuple, set)):
                normalized.append(tuple(str(opt) for opt in entry))
            else:
                normalized.append((str(entry),))
        return normalized

    @staticmethod
    def _resolve_export_cfg(raw: Dict[str, Any]) -> _ExportPaths:
        prefix = raw.get("filename_prefix", "vic_baixa")
        base_subdir = raw.get("subdir", "baixa")
        judicial_subdir = raw.get("judicial_subdir", f"{base_subdir}/jud")
        extrajudicial_subdir = raw.get(
            "extrajudicial_subdir", f"{base_subdir}/extra"
        )
        geral_subdir = raw.get("geral_subdir", base_subdir)
        add_timestamp = raw.get("add_timestamp")
        gerar_geral = bool(raw.get("gerar_geral", True))
        return _ExportPaths(
            prefix=prefix,
            base_subdir=base_subdir,
            judicial_subdir=judicial_subdir,
            extrajudicial_subdir=extrajudicial_subdir,
            geral_subdir=geral_subdir,
            add_timestamp=add_timestamp,
            gerar_geral=gerar_geral,
        )

    # ------------------------------------------------------------------
    def _carregar_csv(self, caminho: Path | str) -> pd.DataFrame:
        caminho_path = Path(caminho)
        self.logger.info("Carregando arquivo: %s", caminho_path)
        df = self.file_manager.ler_csv_ou_zip(caminho_path)
        self.logger.info("Arquivo carregado: %s registros", f"{len(df):,}")
        return df

    # ------------------------------------------------------------------
    def _aplicar_filtros_vic(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, int]]:
        filtrado, metrics = self.filter_applier.aplicar_filtros_baixa(df)
        return filtrado, metrics

    # ------------------------------------------------------------------
    def _aplicar_filtros_max(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, int]]:
        out = df.copy()
        if "TIPO_PARCELA" not in out.columns and "TIPO_TITULO" in out.columns:
            out["TIPO_PARCELA"] = out["TIPO_TITULO"]
        metrics: Dict[str, int] = {"registros_iniciais": len(out)}

        if self.aplicar_filtro_status_max:
            out = self.filter_applier.filtrar_status_em_aberto_max(out)
        metrics["apos_status_aberto"] = len(out)

        return out, metrics

    # ------------------------------------------------------------------
    def _resolver_coluna(
        self, df: pd.DataFrame, candidatos: Sequence[str]
    ) -> pd.Series:
        for coluna in candidatos:
            if coluna in df.columns:
                return df[coluna]
        raise ValueError(
            f"Nenhuma das colunas {candidatos} encontrada. Disponíveis: {list(df.columns)}"
        )

    def _combinar_chave(
        self, df: pd.DataFrame, combinacao: Sequence[Sequence[str]], dataset: str
    ) -> pd.Series:
        if not combinacao:
            raise ValueError(
                f"Combinação de chave não configurada para {dataset}."
            )
        partes: list[pd.Series] = []
        for grupo in combinacao:
            serie = self._resolver_coluna(df, grupo).astype(str).fillna("").str.strip()
            partes.append(serie)
        chave = partes[0]
        for serie in partes[1:]:
            chave = chave + "||" + serie
        return chave

    def _serie_chave(
        self,
        df: pd.DataFrame,
        preferida: str,
        combinacao: Sequence[Sequence[str]],
        dataset: str,
    ) -> pd.Series:
        if preferida and preferida in df.columns:
            serie = df[preferida].astype(str).fillna("").str.strip()
            if serie.ne("").any():
                return serie
        return self._combinar_chave(df, combinacao, dataset)

    # ------------------------------------------------------------------
    def _criar_chaves(
        self,
        df_vic: pd.DataFrame,
        df_max: pd.DataFrame,
    ) -> Tuple[pd.Series, pd.Series]:
        chave_vic = self._serie_chave(
            df_vic, self.chave_vic, self.combination_vic, "VIC"
        )
        chave_max = self._serie_chave(
            df_max, self.chave_max, self.combination_max, "MAX"
        )

        return chave_vic.str.strip(), chave_max.str.strip()

    # ------------------------------------------------------------------
    def _identificar_divergencias(
        self,
        df_vic: pd.DataFrame,
        df_max: pd.DataFrame,
    ) -> Tuple[pd.DataFrame, Dict[str, Any]]:
        chave_vic, chave_max = self._criar_chaves(df_vic, df_max)

        df_vic_local = df_vic.copy()
        df_vic_local[self.key_column_name] = chave_vic

        conjunto_max = set(chave_max[chave_max != ""])
        mask = df_vic_local[self.key_column_name].isin(conjunto_max)
        divergentes = df_vic_local[mask].copy()

        metrics = {
            "chaves_vic": len(chave_vic),
            "chaves_max": len(chave_max),
            "divergencias": len(divergentes),
        }

        if self.include_status_max and divergentes.shape[0] > 0:
            status_col = "STATUS_TITULO" if "STATUS_TITULO" in df_max.columns else None
            if status_col:
                status_map = (
                    pd.Series(chave_max)
                    .to_frame(self.key_column_name)
                    .assign(status=df_max[status_col].astype(str).fillna(""))
                    .drop_duplicates(subset=self.key_column_name)
                    .set_index(self.key_column_name)["status"]
                )
                divergentes["STATUS_MAX"] = divergentes[
                    self.key_column_name
                ].map(status_map).fillna("")

        return divergentes, metrics

    # ------------------------------------------------------------------
    def _primeiro_valor(self, serie: pd.Series) -> Optional[str]:
        for valor in serie:
            if pd.isna(valor):
                continue
            texto = str(valor).strip()
            if texto:
                return texto
        return None

    def _resolver_campanha(self, df: pd.DataFrame) -> str:
        if self.campanha_override:
            return str(self.campanha_override)
        candidatos = ["DATA_BASE", "DATA BASE", "DATA_EXTRACAO", "DATA EXTRACAO"]
        data_base = None
        for coluna in candidatos:
            if coluna in df.columns:
                valor = self._primeiro_valor(df[coluna])
                if valor:
                    data_base = valor
                    break
        if not data_base:
            return self.campanha_prefix or ""
        if self.campanha_prefix:
            return f"{self.campanha_prefix}{data_base}"
        return str(data_base)

    def _formatar_datas(self, serie: pd.Series) -> pd.Series:
        """DEPRECATED: Use src.utils.helpers.formatar_datas_serie"""
        return formatar_datas_serie(serie, self.date_format)

    def _formatar_valores(self, serie: pd.Series) -> pd.Series:
        if serie.empty:
            return serie

        texto = serie.astype(str).str.strip()
        texto = texto.replace({"": None, "nan": None}, regex=False)
        mask_virgula = texto.str.contains(",", na=False)

        texto_normalizado = texto.copy()
        if mask_virgula.any():
            texto_normalizado = texto_normalizado.where(
                ~mask_virgula,
                texto_normalizado.str.replace(".", "", regex=False).str.replace(",", ".", regex=False),
            )

        valores = pd.to_numeric(texto_normalizado, errors="coerce")
        return valores

    def _copiar_coluna(
        self, df: pd.DataFrame, candidatos: Sequence[str]
    ) -> pd.Series:
        for coluna in candidatos:
            if coluna in df.columns:
                return df[coluna]
        return pd.Series([""] * len(df), index=df.index)

    def _mapear_layout(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df.loc[:, []].copy()

        out = pd.DataFrame(index=df.index)

        def _series_text(serie: pd.Series) -> pd.Series:
            return serie.astype(str).str.strip().fillna("")

        nome_cliente = self._copiar_coluna(
            df, ["NOME_RAZAO_SOCIAL", "NOME", "NOME CLIENTE"]
        )
        out["NOME CLIENTE"] = _series_text(nome_cliente)

        cpf_cliente = self._copiar_coluna(
            df, ["CPFCNPJ_CLIENTE", "CPF_CNPJ", "CPF/CNPJ", "CPF"]
        )
        out["CPF/CNPJ CLIENTE"] = _series_text(cpf_cliente)

        cnpj_series = _series_text(
            self._copiar_coluna(df, ["CNPJ_CREDOR", "CNPJ CREDOR"])
        )
        if self.cnpj_credor:
            cnpj_series = cnpj_series.replace("", self.cnpj_credor)
        out["CNPJ CREDOR"] = cnpj_series

        numero_doc = self._copiar_coluna(
            df, [self.key_column_name, "CHAVE", "PARCELA", "NUMERO_CONTRATO"]
        )
        out["NUMERO DOC"] = _series_text(numero_doc)

        valor_parcela = self._formatar_valores(
            self._copiar_coluna(
                df,
                ["VALOR ORIGINAL", "VALOR", "VALOR_PARCELA"]
            )
        )
        out["VALOR DA PARCELA"] = valor_parcela

        out["DT. VENCIMENTO"] = self._formatar_datas(
            self._copiar_coluna(df, ["VENCIMENTO", "DATA_VENCIMENTO"])
        )

        out["STATUS ACORDO"] = pd.Series(
            [self.status_acordo_padrao] * len(df), index=df.index
        )

        out["DT. PAGAMENTO"] = self._formatar_datas(
            self._copiar_coluna(df, ["DT_BAIXA", "DATA_RECEBIMENTO", "DATA_BAIXA"])
        )

        valor_recebido = self._formatar_valores(
            self._copiar_coluna(df, ["RECEBIDO", "VALOR_RECEBIDO"])
        )
        out["VALOR RECEBIDO"] = valor_recebido

        out = out.loc[:, self.LAYOUT_COLUMNS]
        for coluna in ("VALOR DA PARCELA", "VALOR RECEBIDO"):
            out[coluna] = out[coluna].apply(
                lambda valor: f"{valor:.2f}".replace(".", ",") if pd.notna(valor) else ""
            )

        out = out.fillna("")
        return out

    # ------------------------------------------------------------------
    def _carregar_cpfs_judiciais(self) -> None:
        if self._judicial_cpfs:
            return
        try:
            inputs_config = self.config.get("inputs", {})
            judicial_path_cfg = inputs_config.get("clientes_judiciais_path")

            if judicial_path_cfg:
                judicial_file = Path(judicial_path_cfg)
            else:
                judicial_dir = self.paths_cfg.get("input", {}).get("judicial")
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
                return

            df_judicial = self.file_manager.ler_csv_ou_zip(judicial_file)
            cpf_columns = [
                col for col in df_judicial.columns if "CPF" in str(col).upper()
            ]
            if not cpf_columns:
                return
            cpfs_norm = digits_only(df_judicial[cpf_columns[0]].dropna())
            cpfs_valid = cpfs_norm[cpfs_norm.str.len().isin({11, 14})]
            self._judicial_cpfs = set(cpfs_valid.tolist())
        except Exception as exc:  # pragma: no cover - logging auxiliar
            self.logger.warning("Falha ao carregar CPFs judiciais: %s", exc)
            self._judicial_cpfs = set()

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
        serie = digits_only(self._copiar_coluna(
            df,
            [
                "CPF_CNPJ",
                "CPF/CNPJ",
                "CPFCNPJ_CLIENTE",
                "CPF",
            ],
        ))
        return serie.isin(self._judicial_cpfs)

    def _dividir_carteiras(
        self, df: pd.DataFrame
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
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
                subdir=self.export_cfg.base_subdir,
                add_timestamp=self.export_cfg.add_timestamp,
            )
            if caminho_zip:
                resultados["arquivo_zip"] = str(caminho_zip)
                resultados["arquivos_no_zip"] = internos
                for chave in ("arquivo_judicial", "arquivo_extrajudicial", "arquivo_geral"):
                    if chave in internos:
                        resultados[chave] = str(caminho_zip)

        return resultados

    # ------------------------------------------------------------------
    def processar(
        self,
        vic_path: Path | str,
        max_path: Path | str,
    ) -> Dict[str, Any]:
        inicio = datetime.now()

        df_vic_raw = self._carregar_csv(vic_path)
        df_max_raw = self._carregar_csv(max_path)

        df_vic_filtrado, vic_metrics = self._aplicar_filtros_vic(df_vic_raw)
        df_max_filtrado, max_metrics = self._aplicar_filtros_max(df_max_raw)

        divergentes, cruzamento_metrics = self._identificar_divergencias(
            df_vic_filtrado, df_max_filtrado
        )

        df_jud_raw, df_ext_raw = self._dividir_carteiras(divergentes)
        df_layout = self._mapear_layout(divergentes)
        df_jud = self._mapear_layout(df_jud_raw)
        df_ext = self._mapear_layout(df_ext_raw)

        exportados = self._exportar(df_jud, df_ext, df_layout)

        duracao = (datetime.now() - inicio).total_seconds()

        stats: Dict[str, Any] = {
            "vic_registros_iniciais": vic_metrics.get("registros_iniciais", len(df_vic_raw)),
            "vic_apos_status": vic_metrics.get("apos_status_baixa", len(df_vic_filtrado)),
            "vic_apos_tipos": vic_metrics.get("apos_tipos", len(df_vic_filtrado)),
            "vic_apos_aging": vic_metrics.get("apos_aging", len(df_vic_filtrado)),
            "vic_apos_blacklist": vic_metrics.get("apos_blacklist", len(df_vic_filtrado)),
            "max_registros_iniciais": max_metrics.get("registros_iniciais", len(df_max_raw)),
            "max_apos_status": max_metrics.get("apos_status_aberto", len(df_max_filtrado)),
            "divergencias": cruzamento_metrics.get("divergencias", len(df_layout)),
            "judicial": len(df_jud),
            "extrajudicial": len(df_ext),
            "arquivo_zip": exportados.get("arquivo_zip"),
            "arquivos_no_zip": exportados.get("arquivos_no_zip") or {},
            "arquivo_judicial": exportados.get("arquivo_judicial"),
            "arquivo_extrajudicial": exportados.get("arquivo_extrajudicial"),
            "arquivo_geral": exportados.get("arquivo_geral"),
            "arquivo_gerado": exportados.get("arquivo_zip")
            or exportados.get("arquivo_geral")
            or exportados.get("arquivo_judicial")
            or exportados.get("arquivo_extrajudicial"),
            "campanha_utilizada": self._resolver_campanha(df_vic_filtrado),
            "duracao": duracao,
        }

        log_section(self.logger, "BAIXA - VIC ↓ × MAX ↑")
        print("📌 Etapa 5 — Baixa VIC baixado × MAX em aberto")
        print("")
        print("VIC (baixado) após filtros sequenciais:")
        print(f"   • Registros iniciais: {stats['vic_registros_iniciais']:,}")
        print(f"   • Após STATUS=BAIXADO: {stats['vic_apos_status']:,}")
        print(f"   • Após tipos válidos: {stats['vic_apos_tipos']:,}")
        print(f"   • Após aging: {stats['vic_apos_aging']:,}")
        print(f"   • Após blacklist: {stats['vic_apos_blacklist']:,}")
        print("")
        print("MAX (aberto) após filtros sequenciais:")
        print(f"   • Registros iniciais: {stats['max_registros_iniciais']:,}")
        print(f"   • Após STATUS em aberto: {stats['max_apos_status']:,}")
        print("")
        print(f"Parcelas divergentes (baixado x aberto): {stats['divergencias']:,}")
        print(
            "Divisão carteira → Judicial: {jud:,} | Extrajudicial: {ext:,}".format(
                jud=stats["judicial"], ext=stats["extrajudicial"]
            )
        )
        if stats["arquivo_zip"]:
            conteudos = stats.get("arquivos_no_zip", {})
            nomes = [
                conteudos.get("arquivo_geral"),
                conteudos.get("arquivo_judicial"),
                conteudos.get("arquivo_extrajudicial")
            ]
            nomes = [nome for nome in nomes if nome]
            if nomes:
                print(f"📦 Exportado: {stats['arquivo_zip']} ({', '.join(nomes)})")
            else:
                print(f"📦 Exportado: {stats['arquivo_zip']}")
        print(f"⏱️Duração: {duracao:.2f}s")

        return stats

