"""Baixa: registros MAX que nao existem em EMCCAMP (MAX - EMCCAMP)."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import pandas as pd

from src.config.loader import ConfigLoader, LoadedConfig
from src.utils import digits_only, procv_max_menos_emccamp
from src.utils.io import DatasetIO
from src.utils.logger import get_logger
from src.utils.path_manager import PathManager
from src.utils.output_formatter import format_baixa_output


@dataclass
class BaixaStats:
    registros_emccamp: int
    registros_max: int
    registros_max_filtrado: int
    registros_baixa: int
    registros_com_recebimento: int
    registros_sem_recebimento: int
    arquivo_saida: Path | None
    arquivo_com_recebimento: str | None
    arquivo_sem_recebimento: str | None
    filtros_aplicados: dict = None
    flow_steps: dict = None  # Métricas do fluxo de processamento
    duracao: float = 0.0


def _to_number(series: pd.Series) -> pd.Series:
    """Converte strings com ponto/vírgula em números (robusto a separadores).

    Casos tratados:
    - "1234,56" -> 1234.56
    - "1234.56" -> 1234.56
    - "1.234,56" -> 1234.56
    - "1,234.56" -> 1234.56
    """
    s = series.astype(str).str.strip()
    has_comma = s.str.contains(",", na=False)
    has_dot = s.str.contains("\\.", na=False)
    both = has_comma & has_dot
    
    # Se tem ambos vírgula e ponto, assume vírgula como decimal
    s_both = s.str.replace(".", "", regex=False).str.replace(",", ".", regex=False)
    
    # Se só tem vírgula, troca por ponto
    s_comma_only = s.str.replace(",", ".", regex=False)
    
    # Aplica as transformações condicionalmente
    s = s.where(~both, s_both)  # Se tem ambos, usa s_both
    s = s.where(~(has_comma & ~has_dot), s_comma_only)  # Se só vírgula, usa s_comma_only
    
    return pd.to_numeric(s, errors="coerce")


def _apply_max_filters(df_max: pd.DataFrame, config: LoadedConfig, logger: logging.Logger) -> tuple[pd.DataFrame, dict]:
    """Aplica filtros de campanha e status configurados para a base MAX.
    Retorna: (df_filtrado, dict_filtros_aplicados)
    """
    filtros_cfg = config.data.get("baixa", {}).get("filtros", {}).get("max", {})
    df_filtrado = df_max.copy()
    filtros_info = {}

    # Filtro de campanhas
    campanhas = filtros_cfg.get("campanhas")
    if not campanhas:
        campanha_unica = filtros_cfg.get("campanha")
        if campanha_unica:
            campanhas = [campanha_unica]

    if campanhas and "CAMPANHA" in df_filtrado.columns:
        campanha_set = {str(item).strip().upper() for item in campanhas if item is not None}
        antes = len(df_filtrado)
        serie = df_filtrado["CAMPANHA"].astype(str).str.strip().str.upper()
        df_filtrado = df_filtrado[serie.isin(campanha_set)].copy()
        filtros_info["Campanha"] = {
            "antes": antes,
            "depois": len(df_filtrado),
            "valores": sorted(campanha_set)
        }

    # Filtro de status
    status_cfg = filtros_cfg.get("status_titulo", [])
    if status_cfg and "STATUS_TITULO" in df_filtrado.columns:
        status_set = {str(item).strip().upper() for item in status_cfg if item is not None}
        antes = len(df_filtrado)
        serie = df_filtrado["STATUS_TITULO"].astype(str).str.strip().str.upper()
        df_filtrado = df_filtrado[serie.isin(status_set)].copy()
        filtros_info["Status"] = {
            "antes": antes,
            "depois": len(df_filtrado),
            "valores": sorted(status_set)
        }

    return df_filtrado, filtros_info


def _formatar_layout(df: pd.DataFrame, config: LoadedConfig) -> pd.DataFrame:
    """Formata DataFrame para layout final com os novos nomes de colunas especificados."""
    layout_cols = [
        "CNPJ CREDOR",
        "CPF/CNPJ CLIENTE", 
        "NOME CLIENTE",
        "NUMERO DOC",
        "DT. VENCIMENTO",
        "VALOR DA PARCELA",
        "STATUS ACORDO",
        "DT. PAGAMENTO",
        "VALOR RECEBIDO",
    ]

    if df.empty:
        return pd.DataFrame(columns=layout_cols)

    cnpj_credor = config.data.get("global", {}).get("empresa", {}).get("cnpj", "")

    out = pd.DataFrame(index=df.index)
    
    # CNPJ CREDOR - usar CNPJ_CREDOR se existir, senão usar config
    if "CNPJ_CREDOR" in df.columns:
        out["CNPJ CREDOR"] = df["CNPJ_CREDOR"]
    else:
        out["CNPJ CREDOR"] = cnpj_credor

    # CPF/CNPJ CLIENTE - mapear de CPF_CNPJ
    if "CPF_CNPJ" in df.columns:
        out["CPF/CNPJ CLIENTE"] = df["CPF_CNPJ"]
    elif "CPFCNPJ_CLIENTE" in df.columns:
        out["CPF/CNPJ CLIENTE"] = df["CPFCNPJ_CLIENTE"]
    else:
        out["CPF/CNPJ CLIENTE"] = ""

    # NOME CLIENTE - mapear de NOME_RAZAO_SOCIAL
    if "NOME_RAZAO_SOCIAL" in df.columns:
        out["NOME CLIENTE"] = df["NOME_RAZAO_SOCIAL"]
    elif "NOME" in df.columns:
        out["NOME CLIENTE"] = df["NOME"]
    else:
        out["NOME CLIENTE"] = ""

    # NUMERO DOC - mapear de CHAVE
    out["NUMERO DOC"] = df["CHAVE"] if "CHAVE" in df.columns else ""

    # DT. VENCIMENTO - mapear de DATA_VENCIMENTO
    venc_col = None
    if "DATA_VENCIMENTO" in df.columns:
        venc_col = "DATA_VENCIMENTO"
    elif "VENCIMENTO" in df.columns:
        venc_col = "VENCIMENTO"

    if venc_col:
        venc = df[venc_col]
        if pd.api.types.is_datetime64_any_dtype(venc):
            out["DT. VENCIMENTO"] = venc.dt.strftime("%d/%m/%Y")
        else:
            out["DT. VENCIMENTO"] = pd.to_datetime(venc, errors="coerce").dt.strftime("%d/%m/%Y")
    else:
        out["DT. VENCIMENTO"] = ""

    # VALOR DA PARCELA - mapear de VALOR
    out["VALOR DA PARCELA"] = df["VALOR"] if "VALOR" in df.columns else ""
    
    # STATUS ACORDO - valor fixo "2"
    out["STATUS ACORDO"] = "2"
    
    # DT. PAGAMENTO - mapear de DATA_RECEBIMENTO
    if "DATA_RECEBIMENTO" in df.columns:
        receb = df["DATA_RECEBIMENTO"]
        if pd.api.types.is_datetime64_any_dtype(receb):
            out["DT. PAGAMENTO"] = receb.dt.strftime("%d/%m/%Y")
        else:
            out["DT. PAGAMENTO"] = pd.to_datetime(receb, errors="coerce").dt.strftime("%d/%m/%Y")
    else:
        out["DT. PAGAMENTO"] = ""
    
    # VALOR RECEBIDO - mapear de VALOR_RECEBIDO
    out["VALOR RECEBIDO"] = df["VALOR_RECEBIDO"] if "VALOR_RECEBIDO" in df.columns else ""
    
    return out[layout_cols].reset_index(drop=True)


def executar_baixa(
    emccamp_path: Path,
    max_path: Path,
    config: LoadedConfig,
    logger: logging.Logger,
) -> BaixaStats:
    # Coletar métricas de fluxo para exibição posterior
    flow_steps = {}
    
    sep = config.data.get("global", {}).get("csv_separator", ",")
    encoding = config.data.get("global", {}).get("encoding", "utf-8-sig")
    paths = PathManager(config.base_path, config.data)
    io = DatasetIO(separator=sep, encoding=encoding)

    df_emccamp = io.read(emccamp_path)
    df_max = io.read(max_path)

    df_max_filtrado, filtros_aplicados = _apply_max_filters(df_max, config, logger)

    chaves_cfg = config.data.get("baixa", {}).get("chaves", {})
    chave_max = chaves_cfg.get("max", "PARCELA")
    chave_emccamp = chaves_cfg.get("emccamp", "CHAVE")

    df_baixa = procv_max_menos_emccamp(df_max_filtrado, df_emccamp, chave_max, chave_emccamp)
    flow_steps['anti_join'] = len(df_baixa)

    df_trabalho = df_baixa.copy()

    # Normaliza VALOR para manter vírgulas como decimal (não converter para numérico)
    # if "VALOR" in df_trabalho.columns:
    #     df_trabalho["VALOR"] = _to_number(df_trabalho["VALOR"])  # REMOVIDO - manter vírgulas

    # PASSO 1: Filtro de acordos (remove clientes com acordo vigente)
    # Feito ANTES do PROCV com baixas para ser mais eficiente
    acordo_path = paths.resolve_configured_input(
        "acordos_abertos_path",
        "data/input/doublecheck_acordo/acordos_abertos.zip",
    )
    if acordo_path.exists():
        df_acordo = io.read(acordo_path)
        flow_steps['acordos_loaded'] = len(df_acordo)
        if "CPFCNPJ_CLIENTE" not in df_acordo.columns:
            logger.warning("Arquivo de acordos %s sem coluna CPFCNPJ_CLIENTE; filtro nao aplicado.", acordo_path)
        else:
            cpfs_acordo = set(digits_only(df_acordo["CPFCNPJ_CLIENTE"].dropna()))
            
            # Identifica coluna de CPF/CNPJ do cliente
            if "CPF_CNPJ" in df_trabalho.columns:
                coluna_cliente = "CPF_CNPJ"
            elif "CPFCNPJ_CLIENTE" in df_trabalho.columns:
                coluna_cliente = "CPFCNPJ_CLIENTE"
            else:
                coluna_cliente = None
            
            if coluna_cliente:
                cpfs_trabalho = digits_only(df_trabalho[coluna_cliente].fillna(""))
                mask_sem_acordo = ~cpfs_trabalho.isin(cpfs_acordo)
                removidos_acordo = int((~mask_sem_acordo).sum())
                flow_steps['acordos_removed'] = removidos_acordo
                df_trabalho = df_trabalho[mask_sem_acordo].copy()
            else:
                logger.warning("Coluna de CPF nao encontrada para aplicar filtro de acordo.")
    
    flow_steps['apos_filtro_acordo'] = len(df_trabalho)

    # PASSO 2: PROCV com baixas (preenche DATA_RECEBIMENTO e VALOR_RECEBIDO)
    # Feito DEPOIS do filtro de acordos (menos registros para fazer PROCV)
    baixa_path = paths.resolve_configured_input("baixa_emccamp_path", "data/input/baixas/baixa_emccamp.zip")
    if baixa_path.exists():
        df_baixa = io.read(baixa_path)
        flow_steps['baixas_loaded'] = len(df_baixa)
        if "CHAVE" not in df_baixa.columns:
            raise ValueError(f"Arquivo de baixas {baixa_path} sem coluna CHAVE")
        colunas_baixa = ["CHAVE", "DATA_RECEBIMENTO", "VALOR_RECEBIDO"]
        ausentes = [col for col in colunas_baixa if col not in df_baixa.columns]
        if ausentes:
            raise ValueError(f"Arquivo de baixas {baixa_path} sem colunas obrigatorias: {ausentes}")
        df_baixa["CHAVE"] = df_baixa["CHAVE"].astype(str).str.strip()
        df_trabalho["CHAVE"] = df_trabalho["CHAVE"].astype(str).str.strip()
        df_trabalho = df_trabalho.merge(df_baixa[colunas_baixa], on="CHAVE", how="left")
        flow_steps['procv_baixas'] = int(df_trabalho["DATA_RECEBIMENTO"].notna().sum())

    if "DATA_RECEBIMENTO" not in df_trabalho.columns:
        df_trabalho["DATA_RECEBIMENTO"] = pd.NA
    if "VALOR_RECEBIDO" not in df_trabalho.columns:
        df_trabalho["VALOR_RECEBIDO"] = pd.NA
    df_trabalho["DATA_RECEBIMENTO"] = df_trabalho["DATA_RECEBIMENTO"].replace("", pd.NA)
    df_trabalho["VALOR_RECEBIDO"] = df_trabalho["VALOR_RECEBIDO"].replace("", pd.NA)

    # Normaliza VALOR_RECEBIDO para manter vírgulas como decimal (não converter para numérico)  
    # if "VALOR_RECEBIDO" in df_trabalho.columns:
    #     df_trabalho["VALOR_RECEBIDO"] = _to_number(df_trabalho["VALOR_RECEBIDO"])  # REMOVIDO - manter vírgulas

    mask_com_receb = df_trabalho["DATA_RECEBIMENTO"].notna() & df_trabalho["VALOR_RECEBIDO"].notna()
    df_com_receb = df_trabalho[mask_com_receb].copy()
    df_sem_receb = df_trabalho[~mask_com_receb].copy()

    # Aplicar formatação do layout nos DataFrames antes de salvar
    df_com_receb_formatado = _formatar_layout(df_com_receb, config)
    df_sem_receb_formatado = _formatar_layout(df_sem_receb, config)

    export_cfg = config.data.get("baixa", {}).get("export", {})
    prefix_zip = export_cfg.get("filename_prefix", "emccamp_baixa")
    prefix_com_receb = export_cfg.get("com_recebimento_prefix", "baixa_com_recebimento")
    prefix_sem_receb = export_cfg.get("sem_recebimento_prefix", "baixa_sem_recebimento")

    output_dir = paths.resolve_output("baixa", "baixa")
    PathManager.cleanup(output_dir, f"{prefix_zip}_*.zip", logger)
    PathManager.cleanup(output_dir, f"{prefix_zip}.zip", logger)

    add_timestamp = config.data.get("global", {}).get("add_timestamp_to_files", True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    sufixo = f"_{timestamp}" if add_timestamp else ""

    nome_com = f"{prefix_com_receb}{sufixo}.csv"
    nome_sem = f"{prefix_sem_receb}{sufixo}.csv"
    arquivo_saida = output_dir / f"{prefix_zip}{sufixo}.zip"
    io.write_zip({nome_com: df_com_receb_formatado, nome_sem: df_sem_receb_formatado}, arquivo_saida)

    return BaixaStats(
        registros_emccamp=len(df_emccamp),
        registros_max=len(df_max),
        registros_max_filtrado=len(df_max_filtrado),
        registros_baixa=len(df_trabalho),
        registros_com_recebimento=len(df_com_receb),
        registros_sem_recebimento=len(df_sem_receb),
        arquivo_saida=arquivo_saida,
        arquivo_com_recebimento=f"{arquivo_saida.name}:{nome_com}",
        filtros_aplicados=filtros_aplicados,
        arquivo_sem_recebimento=f"{arquivo_saida.name}:{nome_sem}",
        flow_steps=flow_steps
    )


def run(loader: ConfigLoader) -> None:
    """Executa a etapa de baixa via CLI."""
    config = loader.load()
    paths = PathManager(config.base_path, config.data)
    logging_cfg = config.get("logging", {})
    logs_dir = paths.resolve_logs()
    logger = get_logger(__name__, logs_dir, logging_cfg)

    emccamp_tratada = paths.resolve_output("emccamp_tratada", "emccamp_tratada")
    max_tratada = paths.resolve_output("max_tratada", "max_tratada")

    emccamp_files = sorted(emccamp_tratada.glob("emccamp_tratada*.zip"), key=lambda p: p.stat().st_mtime, reverse=True)
    max_files = sorted(max_tratada.glob("max_tratada*.zip"), key=lambda p: p.stat().st_mtime, reverse=True)

    if not emccamp_files:
        raise FileNotFoundError(f"Arquivo EMCCAMP tratado nao encontrado em {emccamp_tratada}")
    if not max_files:
        raise FileNotFoundError(f"Arquivo MAX tratado nao encontrado em {max_tratada}")

    emccamp_file = emccamp_files[0]
    max_file = max_files[0]

    inicio = datetime.now()
    stats = executar_baixa(emccamp_file, max_file, config, logger)
    duracao = (datetime.now() - inicio).total_seconds()


    format_baixa_output(
        emccamp_records=stats.registros_emccamp,
        max_records_raw=stats.registros_max,
        max_records_filtered=stats.registros_max_filtrado,
        baixa_records=stats.registros_baixa,
        com_receb=stats.registros_com_recebimento,
        sem_receb=stats.registros_sem_recebimento,
        output_file=str(stats.arquivo_saida),
        duration=duracao,
        filtros_aplicados=stats.filtros_aplicados,
        flow_steps=stats.flow_steps
    )
