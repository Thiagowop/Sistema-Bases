"""Processador responsável por gerar o layout de enriquecimento VIC.

Ele consome a base tratada (`vic_base_limpa`) e o resultado do batimento
(`vic_batimento_*.csv`) para montar o arquivo canônico esperado pelo
fornecedor de enriquecimento. A lógica privilegia reutilização das colunas
auxiliares criadas no tratamento (CPF/CNPJ e telefones limpos), evitando
reprocessamentos ou duplicações nas etapas seguintes do pipeline.
"""

from __future__ import annotations

import logging
import zipfile
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple, Union

import pandas as pd

from src.config.loader import ConfigLoader
from src.io.file_manager import FileManager
from src.io.packager import ExportacaoService
from src.utils.logger import get_logger, log_section
from src.utils.text import digits_only
from src.utils.helpers import (
    primeiro_valor, normalizar_data_string, extrair_data_referencia,
    formatar_valor_string, extrair_telefone
)


class EnriquecimentoVicProcessor:
    """Gera os arquivos de enriquecimento (telefones/e-mails) em um ZIP."""

    OUTPUT_COLUMNS: List[str] = [
        "CPFCNPJ CLIENTE",
        "TELEFONE",
        "EMAIL",
        "OBSERVACAO",
        "NOME",
        "TELEFONE PRINCIPAL",
    ]

    def __init__(
        self,
        config: Optional[Dict[str, Any]] = None,
        logger: Optional[logging.Logger] = None,
        file_manager: Optional[FileManager] = None,
    ) -> None:
        loader = ConfigLoader()
        self.config = config or loader.get_config()
        self.logger = logger or get_logger(__name__, self.config)
        self.logger.setLevel(logging.WARNING)

        self.file_manager = file_manager or FileManager(self.config)
        self.exportacao_service = ExportacaoService(self.config, self.file_manager)

        self.section_cfg = self.config.get("enriquecimento_vic", {})
        if not self.section_cfg:
            raise ValueError(
                "Configuracao 'enriquecimento_vic' ausente no config.yaml."
            )

        self.phone_columns: Iterable[str] = self.section_cfg.get("phone_columns", [])
        self.email_columns: Iterable[str] = self.section_cfg.get("email_columns", [])
        self.observacao_prefix: str = self.section_cfg.get(
            "observacao_prefix", "Base Vic - "
        )
        self.telefone_principal_flag: str = str(
            self.section_cfg.get("telefone_principal_flag", "1")
        )
        self.marcar_telefone_principal_todos: bool = bool(
            self.section_cfg.get("telefone_principal_em_todos", False)
        )

        global_cfg = self.config.get("global", {})
        self.encoding: str = global_cfg.get("encoding", "utf-8")
        self.csv_separator: str = global_cfg.get("csv_separator", ";")
        self.date_format: str = self.section_cfg.get("date_format") or global_cfg.get(
            "date_format", "%d/%m/%Y"
        )

        self.vic_csv_name: str = (
            self.section_cfg.get("vic_csv_name")
            or self.config.get("vic_processor", {})
            .get("export", {})
            .get("base_limpa_prefix", "vic_base_limpa")
            + ".csv"
        )

        self._data_base_candidates: List[str] = [
            "DATA_BASE",
            "DATA BASE",
            "DATA_EXTRACAO_BASE",
            "DATA EXTRACAO BASE",
            "DATA_EXTRACAO",
            "DATA EXTRACAO",
        ]

    # ------------------------------------------------------------------
    def processar(
        self,
        vic_base_zip: Union[str, Path],
        batimento_zip: Union[str, Path],
    ) -> Dict[str, Any]:
        """Executa o pipeline de enriquecimento."""

        inicio = datetime.now()

        vic_base_zip = self.file_manager.validar_arquivo_existe(vic_base_zip)
        batimento_zip = self.file_manager.validar_arquivo_existe(batimento_zip)

        df_vic_base = self._carregar_vic_base(vic_base_zip)
        df_batimento, origem_counts = self._carregar_batimento(batimento_zip)

        df_vic_enriq = self._preparar_base_enriquecimento(df_vic_base, df_batimento)

        data_base_utilizada = self._resolver_data_base(df_vic_base)

        df_saida, telefones_emitidos, emails_emitidos = self._montar_dataframe(
            df_vic_enriq
        )

        export_cfg = self.section_cfg.get("export", {})
        filename_prefix = export_cfg.get("filename_prefix", "enriquecimento_vic")
        subdir = export_cfg.get("subdir", "inclusao")
        add_timestamp = export_cfg.get("add_timestamp")

        caminho_zip = None
        if not df_saida.empty:
            caminho_zip = self.exportacao_service.exportar_zip(
                {f"{filename_prefix}.csv": df_saida},
                filename_prefix,
                subdir=subdir,
                add_timestamp=add_timestamp,
            )

        duracao = (datetime.now() - inicio).total_seconds()

        stats = {
            "registros_vic_base": len(df_vic_base),
            "registros_batimento": len(df_batimento),
            "batimento_judicial": origem_counts.get("judicial", 0),
            "batimento_extrajudicial": origem_counts.get("extrajudicial", 0),
            "registros_para_enriquecimento": len(df_vic_enriq),
            "contatos_telefone": telefones_emitidos,
            "contatos_email": emails_emitidos,
            "registros_enriquecimento": len(df_saida),
            "arquivo_gerado": str(caminho_zip) if caminho_zip else "",
            "data_base_utilizada": data_base_utilizada,
            "duracao": duracao,
        }

        log_section(self.logger, "ENRIQUECIMENTO - VIC")
        print("📌 Etapa 4 — Enriquecimento de Contato")
        print("")
        print(f"Registros recebidos do batimento: {len(df_batimento):,}")
        if df_batimento.empty:
            print("Nenhum registro de batimento para enriquecer.")
        else:
            print(
                "Divisao por carteira: Judicial = {jud:,} | Extrajudicial = {ext:,}".format(
                    jud=origem_counts.get("judicial", 0),
                    ext=origem_counts.get("extrajudicial", 0),
                )
            )
            print(
                "Registros VIC com contato elegivel (apos casamento com batimento): {total:,}".format(
                    total=len(df_vic_enriq)
                )
            )
            print(
                "Telefones emitidos: {tel:,} | E-mails emitidos: {mail:,}".format(
                    tel=telefones_emitidos,
                    mail=emails_emitidos,
                )
            )
            print(
                "Linhas finais no layout: {linhas:,}".format(
                    linhas=len(df_saida)
                )
            )

        if caminho_zip:
            print(f"Exportado: {caminho_zip}")
        else:
            print("Nenhum arquivo gerado (sem contatos validos)")
        print(f"Duracao: {duracao:.2f}s")

        return stats

    # ------------------------------------------------------------------
    def _resolver_colunas_telefone(self, df: pd.DataFrame) -> List[str]:
        prioridade: List[str] = []

        if "TELEFONE_LIMPO" in df.columns:
            prioridade.append("TELEFONE_LIMPO")

        for coluna in self.phone_columns:
            if coluna in df.columns and coluna not in prioridade:
                prioridade.append(coluna)

        for coluna in df.columns:
            coluna_upper = str(coluna).upper()
            if coluna_upper.endswith("_LIMPO") and (
                coluna_upper.startswith("TEL") or "TELEFONE" in coluna_upper
            ):
                if coluna not in prioridade:
                    prioridade.append(coluna)

        return prioridade

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

    def _resolver_data_base(self, df: pd.DataFrame) -> str:
        for coluna in self._data_base_candidates:
            if coluna in df.columns:
                valor = self._primeiro_valor(df[coluna])
                normalizado = self._normalizar_data(valor)
                if normalizado:
                    return normalizado

        return datetime.now().strftime(self.date_format)

    def _gerar_observacao(self, df: pd.DataFrame) -> str:
        data_base = self._resolver_data_base(df)
        return f"{self.observacao_prefix}{data_base}"

    # ------------------------------------------------------------------
    def _carregar_vic_base(self, arquivo_zip: Path) -> pd.DataFrame:
        """Lê a base tratada produzida pelo VicProcessor."""

        with zipfile.ZipFile(arquivo_zip, "r") as zf:
            if self.vic_csv_name not in zf.namelist():
                raise ValueError(
                    f"Arquivo {self.vic_csv_name} não encontrado no ZIP {arquivo_zip}"
                )
            with zf.open(self.vic_csv_name) as fh:
                df = pd.read_csv(
                    fh,
                    sep=self.csv_separator,
                    encoding=self.encoding,
                    dtype=str,
                )
        df = df.fillna("")
        return df

    # ------------------------------------------------------------------
    def _carregar_batimento(self, arquivo_zip: Path) -> Tuple[pd.DataFrame, Dict[str, int]]:
        """Lê todos os CSVs gerados pelo batimento (judicial/extrajudicial)."""

        with zipfile.ZipFile(arquivo_zip, "r") as zf:
            csv_files = [
                member for member in zf.namelist() if member.lower().endswith(".csv")
            ]
            if not csv_files:
                raise ValueError(
                    f"Nenhum CSV encontrado no arquivo de batimento: {arquivo_zip}"
                )

            frames: List[pd.DataFrame] = []
            origem_counts = {"judicial": 0, "extrajudicial": 0}

            for member in csv_files:
                member_lower = member.lower()
                with zf.open(member) as fh:
                    df = pd.read_csv(
                        fh,
                        sep=self.csv_separator,
                        encoding=self.encoding,
                        dtype=str,
                    )
                df = df.fillna("")
                if "extrajudicial" in member_lower:
                    origem = "extrajudicial"
                elif "judicial" in member_lower:
                    origem = "judicial"
                else:
                    origem = "extrajudicial"
                origem_counts[origem] += len(df)
                df["__origem__"] = origem
                frames.append(df)

        if not frames:
            return pd.DataFrame(), origem_counts

        combinado = pd.concat(frames, ignore_index=True)
        return combinado, origem_counts

    # ------------------------------------------------------------------
    def _preparar_base_enriquecimento(
        self, df_vic: pd.DataFrame, df_batimento: pd.DataFrame
    ) -> pd.DataFrame:
        """Relaciona a base VIC com os CPFs presentes no batimento."""

        if df_batimento.empty:
            return df_vic.iloc[0:0].copy()

        df_vic_local = df_vic.copy()
        if "CPFCNPJ_LIMPO" not in df_vic_local.columns:
            df_vic_local["CPFCNPJ_LIMPO"] = digits_only(
                df_vic_local.get("CPFCNPJ_CLIENTE", "")
            )
        else:
            df_vic_local["CPFCNPJ_LIMPO"] = digits_only(
                df_vic_local["CPFCNPJ_LIMPO"]
            )

        df_bat_local = df_batimento.copy()
        df_bat_local["CPF_BATIMENTO_LIMPO"] = digits_only(
            df_bat_local["CPFCNPJ CLIENTE"]
        )

        relacionados = df_bat_local.merge(
            df_vic_local,
            left_on="CPF_BATIMENTO_LIMPO",
            right_on="CPFCNPJ_LIMPO",
            how="left",
            suffixes=("_BAT", ""),
        )

        relacionados = relacionados.fillna("")

        # Garante que cada CPF apareça ao menos uma vez, mantendo informações do batimento
        relacionados.sort_values(
            by=["CPF_BATIMENTO_LIMPO", "__origem__"], inplace=True
        )

        return relacionados

    # ------------------------------------------------------------------
    def _montar_dataframe(
        self, df_origem: pd.DataFrame
    ) -> Tuple[pd.DataFrame, int, int]:
        """Transforma o DataFrame de origem no layout esperado."""

        registros_tel: List[Dict[str, Any]] = []
        registros_email: List[Dict[str, Any]] = []
        telefones_emitidos = 0
        emails_emitidos = 0

        if df_origem.empty:
            return pd.DataFrame(columns=self.OUTPUT_COLUMNS), 0, 0

        observacao = self._gerar_observacao(df_origem)

        telefone_cols = self._resolver_colunas_telefone(df_origem)
        email_cols = [col for col in self.email_columns if col in df_origem.columns]

        if not telefone_cols:
            self.logger.warning("Nenhuma coluna de telefone encontrada para enriquecer.")
        if not email_cols:
            self.logger.warning("Nenhuma coluna de e-mail encontrada para enriquecer.")

        contatos_emitidos: set[tuple[str, str, str]] = set()

        for _, row in df_origem.iterrows():
            cpf = self._as_str(row.get("CPFCNPJ CLIENTE") or row.get("CPFCNPJ_CLIENTE"))
            if not cpf:
                cpf = self._as_str(row.get("CPF_BATIMENTO_LIMPO"))
            nome = self._as_str(
                row.get("NOME_RAZAO_SOCIAL") or row.get("NOME / RAZAO SOCIAL")
            )

            base_registro = {
                "CPFCNPJ CLIENTE": cpf,
                "NOME": nome,
                "OBSERVACAO": observacao,
            }

            telefones_unicos: List[str] = []
            for col in telefone_cols:
                telefone = self._extrair_telefone(row.get(col))
                if not telefone:
                    continue
                if not self._telefone_valido(telefone):
                    continue
                if telefone not in telefones_unicos:
                    telefones_unicos.append(telefone)

            emails_unicos: List[str] = []
            emails_vistos: set[str] = set()
            for col in email_cols:
                email = self._as_str(row.get(col))
                if email and "@" in email:
                    email_lower = email.lower()
                    if email_lower in emails_vistos:
                        continue
                    emails_vistos.add(email_lower)
                    emails_unicos.append(email)

            for telefone in telefones_unicos:
                chave = (cpf, "telefone", telefone)
                if chave in contatos_emitidos:
                    continue
                contatos_emitidos.add(chave)
                registros_tel.append(
                    {
                        **base_registro,
                        "TELEFONE": telefone,
                        "EMAIL": "",
                        "TELEFONE PRINCIPAL": self.telefone_principal_flag,
                    }
                )
                telefones_emitidos += 1

            for email in emails_unicos:
                chave = (cpf, "email", email.lower())
                if chave in contatos_emitidos:
                    continue
                contatos_emitidos.add(chave)
                registros_email.append(
                    {
                        **base_registro,
                        "TELEFONE": "",
                        "EMAIL": email,
                        "TELEFONE PRINCIPAL": "",
                    }
                )
                emails_emitidos += 1

        df_tel = (
            pd.DataFrame(registros_tel, columns=self.OUTPUT_COLUMNS)
            if registros_tel
            else pd.DataFrame(columns=self.OUTPUT_COLUMNS)
        )
        df_email = (
            pd.DataFrame(registros_email, columns=self.OUTPUT_COLUMNS)
            if registros_email
            else pd.DataFrame(columns=self.OUTPUT_COLUMNS)
        )
        if df_tel.empty and df_email.empty:
            df_saida = pd.DataFrame(columns=self.OUTPUT_COLUMNS)
        elif df_tel.empty:
            df_saida = df_email.reset_index(drop=True)
        elif df_email.empty:
            df_saida = df_tel.reset_index(drop=True)
        else:
            df_saida = pd.concat([df_tel, df_email], ignore_index=True)

        if df_saida.empty:
            return df_saida, telefones_emitidos, emails_emitidos

        df_saida["TELEFONE"] = df_saida["TELEFONE"].fillna("")
        df_saida["TELEFONE PRINCIPAL"] = df_saida["TELEFONE PRINCIPAL"].fillna("")

        if self.marcar_telefone_principal_todos:
            df_saida["TELEFONE PRINCIPAL"] = self.telefone_principal_flag
        else:
            mask_tel = df_saida["TELEFONE"].astype(str).str.strip() != ""
            df_saida.loc[mask_tel, "TELEFONE PRINCIPAL"] = self.telefone_principal_flag
            df_saida.loc[~mask_tel, "TELEFONE PRINCIPAL"] = ""

        return df_saida, telefones_emitidos, emails_emitidos

    # ------------------------------------------------------------------
    @staticmethod
    def _as_str(valor: Any) -> str:
        """DEPRECATED: Use src.utils.helpers.formatar_valor_string"""
        return formatar_valor_string(valor)

    @staticmethod
    def _extrair_telefone(valor: Any) -> str:
        """DEPRECATED: Use src.utils.helpers.extrair_telefone"""
        return extrair_telefone(valor)

    @staticmethod
    def _telefone_valido(telefone: str) -> bool:
        """Valida tamanho mínimo/máximo para telefone (DDD + número)."""

        tamanho = len(telefone)
        return tamanho in (10, 11)


__all__ = ["EnriquecimentoVicProcessor"]
