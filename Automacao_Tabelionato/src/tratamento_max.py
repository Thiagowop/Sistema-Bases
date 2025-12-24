"""Tratamento da base MAX especfica para o projeto Tabelionato."""

from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, Tuple
import zipfile

import pandas as pd

from src.utils.console import format_duration, format_int, format_percent, print_section, suppress_console_info
from src.utils.formatting import formatar_moeda_serie
from src.utils.logger_config import get_logger, log_session_end, log_session_start

# Configuracao de separador decimal para exportacao CSV
DECIMAL_SEP = os.getenv('CSV_DECIMAL_SEPARATOR', ',')


class TabelionatoMaxProcessor:
    """Responsvel por carregar, validar e exportar a base MAX."""

    def __init__(self) -> None:
        self.base_dir = Path(__file__).resolve().parent.parent
        self.input_dir = self.base_dir / "data" / "input" / "max"
        self.output_dir = self.base_dir / "data" / "output"
        self.output_tratada_dir = self.output_dir / "max_tratada"
        self.output_inconsistencias_dir = self.output_dir / "inconsistencias"
        self.logs_dir = self.base_dir / "data" / "logs"

        self.output_tratada_dir.mkdir(parents=True, exist_ok=True)
        self.output_inconsistencias_dir.mkdir(parents=True, exist_ok=True)
        self.logs_dir.mkdir(parents=True, exist_ok=True)

        self.logger = get_logger("tratamento_max")
        suppress_console_info(self.logger)
        self.encoding = "utf-8-sig"
        self.csv_separator = ";"
        self.inconsistencias_parcela: list[int] = []
        self.inconsistencias_vencimento: list[int] = []
        self.raw_vencimentos: pd.Series | None = None

    # ------------------------------------------------------------------
    # Carregamento e padronizao
    # ------------------------------------------------------------------
    def carregar_arquivo_zip(self, caminho_zip: Path) -> pd.DataFrame:
        if not caminho_zip.exists():
            raise FileNotFoundError(f"Arquivo nao encontrado: {caminho_zip}")

        with zipfile.ZipFile(caminho_zip, "r") as arquivo_zip:
            nomes = [nome for nome in arquivo_zip.namelist() if nome.lower().endswith(".csv")]
            if not nomes:
                raise ValueError(f"Nenhum CSV encontrado dentro de {caminho_zip}")
            nome_csv = nomes[0]
            with arquivo_zip.open(nome_csv) as arquivo_csv:
                df = pd.read_csv(arquivo_csv, sep=self.csv_separator, encoding=self.encoding, dtype=str)
        self.logger.info("Arquivo carregado: %s (%s registros)", caminho_zip, len(df))
        return df

    def padronizar_campos(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()

        # Normalizar nomes de colunas
        df.columns = [col.upper() for col in df.columns]

        colunas_interesse = [
            "PARCELA",
            "VENCIMENTO",
            "CPFCNPJ_CLIENTE",
            "NUMERO_CONTRATO",
            "VALOR",
            "STATUS_TITULO",
        ]
        for coluna in colunas_interesse:
            if coluna not in df.columns:
                df[coluna] = ""

        for coluna in df.select_dtypes(include="object").columns:
            df[coluna] = df[coluna].astype(str).str.strip()

        df['CHAVE'] = df['PARCELA'].astype(str).str.strip()

        self.raw_vencimentos = df["VENCIMENTO"].copy()
        return df

    # ------------------------------------------------------------------
    # Validacao
    # ------------------------------------------------------------------
    def validar_dados(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
        df = df.copy()
        df["MOTIVO_INCONSISTENCIA"] = ""

        # --- PARCELA ---------------------------------------------------
        parcelas = df["PARCELA"].astype(str).str.strip()
        parcelas_lower = parcelas.str.lower()

        mask_parcela_vazia = (parcelas == "") | parcelas_lower.isin({"nan", "none", "null"})
        mask_parcela_virgula = parcelas.str.contains(",", na=False)
        mask_parcela_data = parcelas.str.match(r"\d{2}/\d{2}/\d{4}", na=False)
        mask_parcela_hifen = parcelas.str.contains("-", na=False)
        mask_parcela_curta = parcelas.str.fullmatch(r"\d", na=False)

        mask_parcela_inconsistente = (
            mask_parcela_vazia
            | mask_parcela_virgula
            | mask_parcela_data
            | mask_parcela_hifen
            | mask_parcela_curta
        )

        df.loc[mask_parcela_vazia, "MOTIVO_INCONSISTENCIA"] += "PARCELA vazia; "
        df.loc[mask_parcela_virgula, "MOTIVO_INCONSISTENCIA"] += "PARCELA contem virgula; "
        df.loc[mask_parcela_data, "MOTIVO_INCONSISTENCIA"] += "PARCELA parece data; "
        df.loc[mask_parcela_hifen, "MOTIVO_INCONSISTENCIA"] += "PARCELA contem hifen; "
        df.loc[mask_parcela_curta, "MOTIVO_INCONSISTENCIA"] += "PARCELA muito curta; "

        self.inconsistencias_parcela = df[mask_parcela_inconsistente].index.tolist()

        # --- VENCIMENTO ------------------------------------------------
        vencimento_base = self.raw_vencimentos.reindex(df.index) if self.raw_vencimentos is not None else df["VENCIMENTO"]
        vencimento_str = vencimento_base.astype(str).str.strip()
        vencimento_lower = vencimento_str.str.lower()

        mask_venc_vazio = (
            (vencimento_str == "")
            | vencimento_lower.isin({"nan", "none", "null", "nat"})
            | vencimento_base.isna()
        )

        df.loc[mask_venc_vazio, "MOTIVO_INCONSISTENCIA"] += "VENCIMENTO vazio; "

        ano_iso = vencimento_str.str.extract(r"^(\d{4})[-/]\d{2}[-/]\d{2}$", expand=False)
        ano_dmy = vencimento_str.str.extract(r"\d{2}[-/]\d{2}[-/](\d{4})$", expand=False)
        anos = ano_iso.fillna(ano_dmy)
        anos_num = pd.to_numeric(anos, errors="coerce")
        mask_ano_menor_1900 = anos_num < 1900
        mask_ano_inicia_zero = vencimento_str.str.contains(r"^0\d{3}(?:[-/]\d{2}){2}", na=False)

        mask_ano_inconsistente = mask_ano_inicia_zero | mask_ano_menor_1900.fillna(False)
        df.loc[mask_ano_inconsistente, "MOTIVO_INCONSISTENCIA"] += "VENCIMENTO ano inconsistente; "

        datas_br = pd.to_datetime(vencimento_str, format="%d/%m/%Y", errors="coerce")
        mask_falha_br = datas_br.isna()
        datas_iso = pd.to_datetime(vencimento_str[mask_falha_br], format="%Y-%m-%d", errors="coerce")
        mask_data_invalida = mask_falha_br & datas_iso.reindex(vencimento_str.index).isna()

        mask_conversao_falha = df["VENCIMENTO"].isna() & ~mask_venc_vazio
        mask_data_invalida = (mask_data_invalida | mask_conversao_falha) & ~mask_venc_vazio & ~mask_ano_inconsistente

        df.loc[mask_data_invalida, "MOTIVO_INCONSISTENCIA"] += "VENCIMENTO formato invalido; "

        mask_venc_inconsistente = mask_venc_vazio | mask_ano_inconsistente | mask_data_invalida
        self.inconsistencias_vencimento = df[mask_venc_inconsistente].index.tolist()

        # --- Resultado final -------------------------------------------
        todas_inconsistencias = sorted(set(self.inconsistencias_parcela + self.inconsistencias_vencimento))
        df["MOTIVO_INCONSISTENCIA"] = df["MOTIVO_INCONSISTENCIA"].str.rstrip("; ")
        df.loc[~df.index.isin(todas_inconsistencias), "MOTIVO_INCONSISTENCIA"] = ""

        df_invalido = df.loc[todas_inconsistencias].copy() if todas_inconsistencias else pd.DataFrame()
        if self.raw_vencimentos is not None and not df_invalido.empty:
            original = self.raw_vencimentos.reindex(df_invalido.index)
            df_invalido["VENCIMENTO_ORIGINAL"] = original
            mask_motivo_venc = df_invalido["MOTIVO_INCONSISTENCIA"].str.contains("VENCIMENTO", na=False)
            if mask_motivo_venc.any():
                df_invalido.loc[mask_motivo_venc, "VENCIMENTO"] = original[mask_motivo_venc]

        df_valido = df.drop(todas_inconsistencias).copy()

        self.logger.info("Inconsistencias PARCELA: %s", len(self.inconsistencias_parcela))
        self.logger.info("Inconsistencias VENCIMENTO: %s", len(self.inconsistencias_vencimento))
        self.logger.info("Total inconsistencias: %s", len(todas_inconsistencias))
        self.logger.info("Validacao: %s validos, %s invalidos", len(df_valido), len(df_invalido))

        return df_valido, df_invalido

    # ------------------------------------------------------------------
    # Exportao
    # ------------------------------------------------------------------
    def _exportar_inconsistencias(self, df_invalido: pd.DataFrame) -> str | None:
        if df_invalido.empty:
            return None

        self.output_inconsistencias_dir.mkdir(parents=True, exist_ok=True)
        for arquivo in self.output_inconsistencias_dir.glob("max_inconsistencias*.zip"):
            arquivo.unlink(missing_ok=True)

        csv_temp = self.output_inconsistencias_dir / "max_inconsistencias.csv"
        df_export = df_invalido.copy()
        if 'VALOR' in df_export.columns:
            df_export['VALOR'] = formatar_moeda_serie(df_export['VALOR'], decimal_separator=DECIMAL_SEP)
        df_export.to_csv(csv_temp, index=False, encoding=self.encoding, sep=self.csv_separator)

        zip_path = self.output_inconsistencias_dir / "max_inconsistencias.zip"
        with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zip_file:
            zip_file.write(csv_temp, csv_temp.name)
        csv_temp.unlink(missing_ok=True)

        self.logger.info("Inconsistencias exportadas: %s", zip_path)
        return str(zip_path)

    def exportar_resultado(self, df: pd.DataFrame, nome_base: str = "max_tratada") -> Path:
        self.output_tratada_dir.mkdir(parents=True, exist_ok=True)
        for arquivo in self.output_tratada_dir.glob(f"{nome_base}*.zip"):
            arquivo.unlink(missing_ok=True)

        csv_path = self.output_tratada_dir / f"{nome_base}.csv"
        df_export = df.copy()
        if 'VALOR' in df_export.columns:
            df_export['VALOR'] = formatar_moeda_serie(df_export['VALOR'], decimal_separator=DECIMAL_SEP)
        df_export.to_csv(csv_path, index=False, encoding=self.encoding, sep=self.csv_separator)

        zip_path = self.output_tratada_dir / f"{nome_base}.zip"
        with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zip_file:
            zip_file.write(csv_path, csv_path.name)
        csv_path.unlink(missing_ok=True)

        self.logger.info("Arquivo exportado: %s", zip_path)
        return zip_path

    # ------------------------------------------------------------------
    # Execuo principal
    # ------------------------------------------------------------------
    def processar(self) -> Dict[str, object]:
        inicio = datetime.now()
        log_session_start("TRATAMENTO MAX")
        self.logger.info("Inicio do tratamento MAX")

        try:
            arquivo_entrada = self.input_dir / "MaxSmart_Tabelionato.zip"
            df = self.carregar_arquivo_zip(arquivo_entrada)
            registros_originais = len(df)

            df_padronizado = self.padronizar_campos(df)
            df_validos, df_invalidos = self.validar_dados(df_padronizado)

            arquivo_inconsistencias = self._exportar_inconsistencias(df_invalidos)
            arquivo_saida = self.exportar_resultado(df_validos)

            duracao = (datetime.now() - inicio).total_seconds()
            taxa_aproveitamento = (len(df_validos) / registros_originais * 100) if registros_originais else 0.0

            linhas = [
                "[STEP] Tratamento MAX",
                "",
                f"Registros originais: {format_int(registros_originais)}",
                f"Registros validos: {format_int(len(df_validos))}",
                f"Registros invalidos: {format_int(len(df_invalidos))}",
                f"Taxa de aproveitamento: {format_percent(taxa_aproveitamento)}",
            ]

            if self.inconsistencias_parcela or self.inconsistencias_vencimento:
                linhas.extend(
                    [
                        "",
                        f"Inconsistencias parcela: {format_int(len(self.inconsistencias_parcela))}",
                        f"Inconsistencias vencimento: {format_int(len(self.inconsistencias_vencimento))}",
                    ]
                )

            linhas.extend(
                [
                    "",
                    f"Saida principal: {arquivo_saida}",
                ]
            )

            if arquivo_inconsistencias:
                linhas.append(f"Inconsistencias: {arquivo_inconsistencias}")

            linhas.append(f"Duracao: {format_duration(duracao)}")

            print_section("TRATAMENTO - MAX", linhas, leading_break=False)

            log_session_end("TRATAMENTO MAX", success=True)
            resultado: Dict[str, object] = {
                "registros_originais": registros_originais,
                "registros_validos": len(df_validos),
                "registros_invalidos": len(df_invalidos),
                "taxa_aproveitamento": taxa_aproveitamento,
                "arquivo_gerado": str(arquivo_saida),
                "duracao": duracao,
            }
            if arquivo_inconsistencias:
                resultado["arquivo_inconsistencias"] = arquivo_inconsistencias
            return resultado
        except Exception as exc:
            log_session_end("TRATAMENTO MAX", success=False)
            self.logger.exception("Erro durante o tratamento MAX: %s", exc)
            raise


def main() -> Dict[str, object]:
    processor = TabelionatoMaxProcessor()
    return processor.processar()


if __name__ == "__main__":
    main()
