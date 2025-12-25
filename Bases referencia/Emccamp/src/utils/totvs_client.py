from __future__ import annotations

import os
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import requests

from src.config.loader import LoadedConfig
from src.utils.io import write_csv_to_zip
from src.utils.logger import get_logger
from src.utils.path_manager import PathManager
from src.utils.output_formatter import OutputFormatter


def baixar_emccamp(config: LoadedConfig) -> tuple[Path, int]:
    """Baixa dados EMCCAMP via API. Retorna (path, num_registros)."""
    paths = PathManager(config.base_path, config.data)
    input_dir = paths.resolve_input("emccamp", "data/input/emccamp")
    logging_cfg = config.get("logging", {})
    logger = get_logger("api_totvs", paths.resolve_logs(), logging_cfg)
    PathManager.cleanup(input_dir, "Emccamp*.zip", logger)

    # Configurações da API EMCCAMP
    base_url = os.getenv("EMCCAMP_API_URL")
    if not base_url:
        raise RuntimeError("Variavel EMCCAMP_API_URL nao configurada")
    
    user = os.getenv("EMCCAMP_API_USER")
    password = os.getenv("EMCCAMP_API_PASSWORD")
    if not user or not password:
        raise RuntimeError("Variaveis EMCCAMP_API_USER e EMCCAMP_API_PASSWORD devem estar configuradas")

    data_inicio = os.getenv("EMCCAMP_DATA_VENCIMENTO_INICIAL")
    if not data_inicio:
        raise RuntimeError("Variavel EMCCAMP_DATA_VENCIMENTO_INICIAL deve estar configurada")
    
    data_fim_env = (os.getenv("EMCCAMP_DATA_VENCIMENTO_FINAL") or "").strip()
    if not data_fim_env or data_fim_env.upper() == "AUTO":
        # Repete a lógica do script base: vencimento até hoje - 6 dias
        data_fim_dt = date.today() - timedelta(days=6)
        data_fim = data_fim_dt.strftime("%Y-%m-%d")
    else:
        data_fim = data_fim_env
    full_url = base_url

    parametros = [f"DATA_VENCIMENTO_INICIAL={data_inicio}"]
    if data_fim:
        parametros.append(f"DATA_VENCIMENTO_FINAL={data_fim}")

    parametros_str = ";".join(parametros)
    print(OutputFormatter.header("INTERVALO DE VENCIMENTO EMCCAMP"))
    print(OutputFormatter.metric("Data inicial (env)", data_inicio))
    if data_fim_env.upper() == "AUTO" or not data_fim_env:
        print(OutputFormatter.metric("Data final (auto, hoje-6)", data_fim))
    else:
        print(OutputFormatter.metric("Data final (env)", data_fim))
    print(OutputFormatter.footer())
    print(f"{base_url}?parameters={parametros_str}")

    params = {"parameters": parametros_str}

    resp = requests.get(full_url, params=params, auth=(user, password), timeout=(15, 180))
    resp.raise_for_status()

    data = resp.json()
    df = pd.DataFrame(data)
    timestamp = os.environ.get("EMCCAMP_RUN_TS") or pd.Timestamp.utcnow().strftime("%Y%m%d_%H%M%S")
    csv_name = f"Emccamp_{timestamp}.csv"
    zip_path = input_dir / "Emccamp.zip"

    global_cfg = config.get("global", {})
    sep = global_cfg.get("csv_separator", ";")
    encoding = global_cfg.get("encoding", "utf-8-sig")

    write_csv_to_zip({csv_name: df}, zip_path, sep=sep, encoding=encoding)
    return zip_path, len(df)


def baixar_baixas_emccamp(config: LoadedConfig) -> tuple[Path, int]:
    """Baixa planilha de pagamentos (baixas) via API TOTVS e grava CSV. Retorna (path, num_registros)."""
    paths = PathManager(config.base_path, config.data)
    input_dir = paths.resolve_input("baixas", "data/input/baixas")
    logging_cfg = config.get("logging", {})
    logger = get_logger("api_totvs_baixas", paths.resolve_logs(), logging_cfg)
    zip_path = input_dir / "baixa_emccamp.zip"
    if zip_path.exists():
        try:
            zip_path.unlink()
        except Exception as exc:  # pragma: no cover - defensive
            logger.warning("Nao foi possivel remover %s: %s", zip_path.name, exc)

    # Configurações da API de baixas EMCCAMP
    base_url = os.getenv("TOTVS_BASE_URL")
    if not base_url:
        raise RuntimeError("Variavel TOTVS_BASE_URL nao configurada")
    
    endpoint = "/api/framework/v1/consultaSQLServer/RealizaConsulta/CANDIOTTO.002/0/X"
    full_url = f"{base_url}{endpoint}"

    user = os.getenv("TOTVS_USER")
    password = os.getenv("TOTVS_PASS")
    if not user or not password:
        raise RuntimeError("Variaveis TOTVS_USER e TOTVS_PASS devem estar configuradas")

    resp = requests.get(full_url, auth=(user, password), timeout=(15, 180))
    resp.raise_for_status()

    data = resp.json()
    df = pd.DataFrame(data)
    if df.empty:
        logger.warning("API de baixas retornou nenhum registro.")
        df = pd.DataFrame(columns=["NUM_VENDA", "ID_PARCELA", "HONORARIO_BAIXADO", "DATA_RECEBIMENTO", "VALOR_RECEBIDO"])

    df.columns = [str(col).upper() for col in df.columns]

    def _pick_column(candidates: tuple[str, ...]) -> str:
        for candidate in candidates:
            if candidate in df.columns:
                return candidate
        raise ValueError(
            f"Colunas {candidates} ausentes no retorno de baixas | disponiveis={list(df.columns)}"
        )

    data_col = _pick_column(("DATA_RECEBIMENTO", "DATA_BAIXA"))
    valor_col = _pick_column(("VALOR_RECEBIDO", "VALOR_ATUALIZADO", "VALOR_ORIGINAL"))

    required_cols = {"NUM_VENDA", "ID_PARCELA", "HONORARIO_BAIXADO", data_col, valor_col}
    missing = required_cols.difference(df.columns)
    if missing:
        raise ValueError(
            f"Colunas ausentes no retorno de baixas: {sorted(missing)} | disponiveis={list(df.columns)}"
        )

    df["HONORARIO_BAIXADO"] = pd.to_numeric(df["HONORARIO_BAIXADO"], errors="coerce").fillna(0)
    df_filtrado = df[df["HONORARIO_BAIXADO"] != 0].copy()

    df_filtrado["CHAVE"] = (
        df_filtrado["NUM_VENDA"].astype(str).str.strip()
        + "-"
        + df_filtrado["ID_PARCELA"].astype(str).str.strip()
    )
    df_filtrado["VALOR_RECEBIDO"] = pd.to_numeric(df_filtrado[valor_col], errors="coerce")
    df_filtrado["DATA_RECEBIMENTO"] = pd.to_datetime(
        df_filtrado[data_col], errors="coerce", dayfirst=True
    ).dt.strftime("%Y-%m-%d")
    df_filtrado["DATA_RECEBIMENTO"] = df_filtrado["DATA_RECEBIMENTO"].fillna("")

    global_cfg = config.get("global", {})
    sep = global_cfg.get("csv_separator", ";")
    encoding = global_cfg.get("encoding", "utf-8-sig")
    timestamp = pd.Timestamp.utcnow().strftime("%Y%m%d_%H%M%S")
    csv_name = f"baixa_emccamp_{timestamp}.csv"
    write_csv_to_zip({csv_name: df_filtrado}, zip_path, sep=sep, encoding=encoding)
    return zip_path, len(df_filtrado)
