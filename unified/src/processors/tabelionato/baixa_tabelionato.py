#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Processo de baixa do Tabelionato.

Identifica protocolos que esto na MAX tratada mas no no Tabelionato tratado,
enriquece com dados de custas e gera layout final de recebimento.
"""

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional
import zipfile
import os

import pandas as pd

from src.utils.console import format_duration, format_int, print_section, suppress_console_info
from src.utils.logger_config import (
    get_logger,
    log_metrics,
    log_session_end,
    log_session_start,
    log_validation_presence,
    log_validation_result,
)

from src.utils.validacao_resultados import (
    localizar_chaves_ausentes,
    localizar_chaves_presentes,
    resumir_amostras,
)

# Diretrios
BASE_DIR = Path(__file__).parent.parent
OUTPUT_DIR = BASE_DIR / "data" / "output"
BAIXA_DIR = OUTPUT_DIR / "baixa"
LOG_DIR = BASE_DIR / "data" / "logs"

LOG_DIR.mkdir(parents=True, exist_ok=True)

logger = get_logger("baixa")
suppress_console_info(logger)

# Configuracao leve (via .env) para separador decimal do CSV final
# Padrao brasileiro: ','
DECIMAL_SEP = os.getenv("CSV_DECIMAL_SEPARATOR", ",")


@dataclass
class ResultadoBaixa:
    """Representa o resultado da execucao do processo de baixa."""

    status: str
    mensagem: str = ""
    arquivo_final: Optional[str] = None
    arquivo_checagem: Optional[str] = None
    total_exportados: int = 0
    total_nao_exportados: int = 0
    duracao: float = 0.0


def limpar_arquivos_antigos(diretorio: Path, padrao: str) -> None:
    """Remove arquivos antigos no diretrio informado conforme padro."""

    if not diretorio.exists():
        return

    for arquivo in diretorio.glob(padrao):
        try:
            arquivo.unlink()
            logger.debug("Arquivo antigo removido: %s", arquivo)
        except OSError as exc:
            logger.warning("No foi possvel remover arquivo antigo %s: %s", arquivo, exc)


def carregar_base_zip(
    caminho_zip: Path | str,
    nome_csv: str | None = None,
    *,
    encoding: str = 'utf-8',
    separador: str = ';',
) -> pd.DataFrame:
    """
    Carrega DataFrame de um arquivo ZIP.
    
    Args:
        caminho_zip: Caminho para o arquivo ZIP
        nome_csv: Nome especfico do CSV dentro do ZIP (opcional)
        
    Returns:
        DataFrame carregado
    """
    try:
        caminho = Path(caminho_zip)

        if not caminho.exists():
            raise FileNotFoundError(f"Arquivo no encontrado: {caminho}")

        with zipfile.ZipFile(caminho, 'r') as zip_file:
            # Se nome especfico no fornecido, usar o primeiro CSV
            if nome_csv is None:
                csv_files = [f for f in zip_file.namelist() if f.endswith('.csv')]
                if not csv_files:
                    raise ValueError(f"Nenhum arquivo CSV encontrado em {caminho_zip}")
                nome_csv = csv_files[0]
            
            # Ler CSV do ZIP
            with zip_file.open(nome_csv) as csv_file:
                df = pd.read_csv(csv_file, encoding=encoding, sep=separador, dtype=str)
                
        logger.info("Base carregada de %s: %s registros", caminho, len(df))
        return df

    except Exception as e:
        logger.error("Erro ao carregar base %s: %s", caminho, e)
        raise

def carregar_base_custas() -> pd.DataFrame:
    """
    Carrega a base de custas do arquivo ZIP original.
    
    Returns:
        DataFrame com dados de custas
    """
    try:
        # Buscar arquivo de custas ZIP no diretrio de input
        custas_input_dir = BASE_DIR / "data" / "input" / "tabelionato custas"

        if not custas_input_dir.exists():
            raise FileNotFoundError(
                "Diretrio de custas no encontrado: "
                f"{custas_input_dir}. Gere ou informe o arquivo de custas antes de prosseguir."
            )

        arquivos_custas = sorted(custas_input_dir.glob("*.zip"))

        if not arquivos_custas:
            raise FileNotFoundError(
                "Nenhum arquivo de custas encontrado em "
                f"{custas_input_dir}. A baixa depende desta base."
            )

        # Usar o arquivo mais recente
        arquivo_custas = max(arquivos_custas, key=lambda x: x.stat().st_mtime)
        logger.info(f"Carregando custas de: {arquivo_custas}")

        df = carregar_base_zip(arquivo_custas)
        logger.info("Base de custas carregada: %s registros", len(df))

        # Criar coluna CHAVE_STR para compatibilidade
        serie_protocolo = df.get('Protocolo', pd.Series(index=df.index, dtype='object'))
        df['Protocolo_Tratado'] = serie_protocolo.astype(str).str.strip()

        if 'Valor Total Pago' not in df.columns:
            df['Valor Total Pago'] = 0.0
        else:
            df['Valor Total Pago'] = pd.to_numeric(
                df['Valor Total Pago'], errors='coerce'
            ).fillna(0.0)

        return df
        
    except Exception as e:
        logger.error("Erro ao carregar base de custas: %s", e)
        raise

def filtrar_max_status_aberto(df_max: pd.DataFrame) -> pd.DataFrame:
    """
    Filtra registros com status em aberto na base MAX.
    
    Args:
        df_max: DataFrame da base MAX tratada
        
    Returns:
        DataFrame filtrado apenas com status em aberto
    """
    try:
        # Verificar se a coluna STATUS_TITULO existe
        if 'STATUS_TITULO' not in df_max.columns:
            logger.warning("Coluna STATUS_TITULO no encontrada, retornando todos os registros")
            return df_max.copy()
        
        # Filtrar por status "Aberto" (string, no nmero)
        registros_antes = len(df_max)
        status_normalizado = (
            df_max['STATUS_TITULO']
            .astype(str)
            .str.strip()
            .str.lower()
        )
        status_validos = {'aberto', 'em aberto', 'a', '0'}
        mask_aberto = status_normalizado.isin(status_validos)
        df_filtrado = df_max[mask_aberto].copy()
        registros_depois = len(df_filtrado)
        
        logger.info(
            "Filtro status aberto MAX: %s  %s registros",
            registros_antes,
            registros_depois,
        )
        if registros_depois == 0:
            logger.warning("Nenhum registro com status considerado como aberto na base MAX.")
        return df_filtrado
        
    except Exception as e:
        logger.error("Erro ao filtrar status aberto: %s", e)
        raise

def identificar_diferenca_max_tabelionato(df_max: pd.DataFrame, df_tabelionato: pd.DataFrame) -> pd.DataFrame:
    """
    Identifica protocolos que esto na MAX mas no esto no Tabelionato.
    (Operao contrria ao batimento)
    
    Args:
        df_max: DataFrame da base MAX tratada (filtrada por status aberto)
        df_tabelionato: DataFrame da base Tabelionato tratada
        
    Returns:
        DataFrame com protocolos que esto apenas na MAX
    """
    try:
        # Usar coluna CHAVE para comparao (comum em ambas as bases)
        chaves_max = set(df_max['CHAVE'].dropna())
        chaves_tabelionato = set(df_tabelionato['CHAVE'].dropna())
        
        # Chaves que esto na MAX mas no no Tabelionato
        chaves_diferenca = chaves_max - chaves_tabelionato
        
        logger.info("Chaves na MAX: %s", len(chaves_max))
        logger.info("Chaves no Tabelionato: %s", len(chaves_tabelionato))
        logger.info("Chaves apenas na MAX: %s", len(chaves_diferenca))

        # Filtrar DataFrame da MAX para manter apenas as chaves da diferena
        df_resultado = df_max[df_max['CHAVE'].isin(chaves_diferenca)].copy()

        logger.info("Registros finais aps diferena: %s", len(df_resultado))
        return df_resultado

    except Exception as e:
        logger.error("Erro ao identificar diferena MAX-Tabelionato: %s", e)
        raise

def enriquecer_com_custas(
    df_diferenca: pd.DataFrame, df_custas: pd.DataFrame
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Enriquece os dados da diferena com informaes de custas.
    Faz apenas comparao de protocolos sem filtrar por valores.
    
    Args:
        df_diferenca: DataFrame com protocolos apenas na MAX
        df_custas: DataFrame com dados de custas tratados
        
    Returns:
        Tuple com (DataFrame enriquecido, DataFrame de checagem)
    """
    try:
        registros_antes = len(df_diferenca)
        
        # Fazer merge com base de custas usando CHAVE (protocolo)
        # Converter CHAVE para string para garantir compatibilidade
        df_trabalho = df_diferenca.copy()
        df_trabalho['CHAVE_STR'] = df_trabalho['CHAVE'].astype(str)

        df_custas_trabalho = df_custas.copy()
        df_custas_trabalho['CHAVE_STR'] = (
            df_custas_trabalho['Protocolo_Tratado'].astype(str)
        )

        # Merge LEFT para manter todos os registros da diferena
        df_merged = df_trabalho.merge(
            df_custas_trabalho[['CHAVE_STR', 'Valor Total Pago']],
            on='CHAVE_STR',
            how='left',
        )

        # Separar registros com match (encontrados na base custas) e sem match
        df_enriquecido = df_merged[df_merged['Valor Total Pago'].notna()].copy()
        df_checagem = df_merged[df_merged['Valor Total Pago'].isna()].copy()

        if not df_enriquecido.empty:
            df_enriquecido['Valor Total Pago'] = df_enriquecido['Valor Total Pago'].fillna(0)

        # Adicionar motivo na checagem apenas se houver registros
        if not df_checagem.empty:
            df_checagem = df_checagem.copy()
            df_checagem['MOTIVO_NAO_EXPORTADO'] = 'Sem match na base custas'

        # Remover coluna auxiliar
        for dataset in (df_enriquecido, df_checagem):
            if 'CHAVE_STR' in dataset.columns:
                dataset.drop(columns=['CHAVE_STR'], inplace=True)

        registros_exportados = len(df_enriquecido)
        registros_nao_exportados = len(df_checagem)
        
        logger.info(
            "Comparacao de protocolos: %s  %s registros para baixa",
            registros_antes,
            registros_exportados,
        )
        logger.info("Registros sem match na base custas: %s", registros_nao_exportados)

        return df_enriquecido, df_checagem

    except Exception as e:
        logger.error("Erro ao enriquecer com custas: %s", e)
        raise

def gerar_layout_final(df_enriquecido: pd.DataFrame, data_pagamento: str) -> pd.DataFrame:
    """
    Gera o layout final de recebimento conforme especificao.
    
    Args:
        df_enriquecido: DataFrame enriquecido com dados de custas
        data_pagamento: Data a ser utilizada em "DT. PAGAMENTO" (DataExtracao da base Tabelionato tratada)
    
    Returns:
        DataFrame no layout final
    """
    try:
        # Criar DataFrame com layout final
        df_final = pd.DataFrame()
        
        # NOME CLIENTE  da base Max Tratada
        df_final['NOME CLIENTE'] = df_enriquecido['NOME_RAZAO_SOCIAL']
        
        # CPF/CNPJ CLIENTE  da base Max Tratada
        df_final['CPF/CNPJ CLIENTE'] = df_enriquecido['CPFCNPJ_CLIENTE']
        
        # CNPJ CREDOR  da base Max Tratada
        df_final['CNPJ CREDOR'] = df_enriquecido['CNPJ_CREDOR']
        
        # NUMERO DOC  CHAVE (protocolo)
        df_final['NUMERO DOC'] = df_enriquecido['CHAVE']
        
        # VALOR DA PARCELA  valor da base Max Tratada (sem formatao adicional)
        df_final['VALOR DA PARCELA'] = df_enriquecido['VALOR']
        
        # DT. VENCIMENTO  data da base Max Tratada
        df_final['DT. VENCIMENTO'] = df_enriquecido['VENCIMENTO']
        
        # STATUS ACORDO  fixo = 2
        df_final['STATUS ACORDO'] = 2

        # DT. PAGAMENTO  DataExtracao da base Tabelionato tratada (fail-fast se ausente)
        if not data_pagamento:
            raise ValueError("DT. PAGAMENTO ausente: 'data_pagamento' nao informado (origem esperada DataExtracao)")
        df_final['DT. PAGAMENTO'] = data_pagamento
        
        # VALOR RECEBIDO  da base Custas (Valor Total Pago) sem formatao adicional
        df_final['VALOR RECEBIDO'] = df_enriquecido['Valor Total Pago']

        # Formatar colunas de valor com separador decimal configurado
        # Mantemos duas casas decimais e sem separador de milhares
        colunas_valor = ['VALOR DA PARCELA', 'VALOR RECEBIDO']

        def _fmt(v: float) -> str:
            return "" if pd.isna(v) else ("%.2f" % v).replace(".", DECIMAL_SEP)

        def _to_numeric_brazil(s: pd.Series) -> pd.Series:
            # Normaliza strings monetárias com vírgula/ponto para float
            s = s.astype(str).str.replace('R$', '', regex=False).str.replace(' ', '', regex=False)
            mask_comma = s.str.contains(',')
            s_br = s.copy()
            s_br[mask_comma] = s_br[mask_comma].str.replace('.', '', regex=False).str.replace(',', '.', regex=False)
            s_br[~mask_comma] = s_br[~mask_comma].str.replace(',', '.', regex=False)
            return pd.to_numeric(s_br, errors='coerce')

        for col in colunas_valor:
            if col in df_final.columns:
                df_final[col] = _to_numeric_brazil(df_final[col]).map(_fmt)
        
        logger.info("Layout final gerado: %s registros", len(df_final))
        return df_final

    except Exception as e:
        logger.error("Erro ao gerar layout final: %s", e)
        raise

def salvar_checagem(df_checagem: pd.DataFrame) -> Optional[str]:
    """
    Registra informacoes sobre registros nao exportados sem gerar arquivos.

    Antes de registrar as estatisticas em log, remove quaisquer artefatos antigos
    de checagem para evitar que execucoes anteriores deixem arquivos residuais
    como ``checagem_nao_exportados_*.csv`` no diretorio de baixa.
    """

    # Remover arquivos de checagem de execucoes anteriores (quando existiam).
    limpar_arquivos_antigos(BAIXA_DIR, "checagem_nao_exportados_*")

    total_pendentes = len(df_checagem)
    if total_pendentes == 0:
        logger.info("Nenhum registro pendente para checagem.")
    else:
        logger.info(
            "Registros nao exportados mantidos apenas em memoria: %s",
            total_pendentes,
        )

    # Mantem compatibilidade retornando sempre None, sinalizando ausencia de arquivo.
    return None

def salvar_resultado_baixa(df_final: pd.DataFrame) -> str:
    """
    Salva o resultado final da baixa em formato ZIP.
    
    Args:
        df_final: DataFrame com layout final
        
    Returns:
        Caminho do arquivo ZIP salvo
    """
    try:
        # Criar diretrio se no existir
        BAIXA_DIR.mkdir(parents=True, exist_ok=True)

        limpar_arquivos_antigos(BAIXA_DIR, "baixa_tabelionato_*.zip")

        # Nome do arquivo com timestamp
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        nome_csv = f"baixa_tabelionato_{timestamp}.csv"
        nome_zip = f"baixa_tabelionato_{timestamp}.zip"
        caminho_csv = BAIXA_DIR / nome_csv
        caminho_zip = BAIXA_DIR / nome_zip
        
        # Salvar CSV temporrio
        df_final.to_csv(caminho_csv, index=False, encoding='utf-8', sep=';')
        
        # Criar ZIP
        with zipfile.ZipFile(caminho_zip, 'w', zipfile.ZIP_DEFLATED) as zipf:
            zipf.write(caminho_csv, nome_csv)
        
        # Remover CSV temporrio
        caminho_csv.unlink()
        
        logger.info("Resultado da baixa salvo: %s", caminho_zip)
        return str(caminho_zip)

    except Exception as e:
        logger.error("Erro ao salvar resultado: %s", e)
        raise

def executar_processo_baixa() -> ResultadoBaixa:
    """
    Executa o processo completo de baixa.

    Returns:
        ResultadoBaixa com detalhes sobre a execucao.
    """
    log_session_start("Baixa Tabelionato")
    inicio = datetime.now()
    sucesso = False

    try:
        # 1. Carregar bases tratadas
        logger.info("1. Carregando bases tratadas...")

        # MAX tratada
        caminho_max = OUTPUT_DIR / "max_tratada" / "max_tratada.zip"
        if not caminho_max.exists():
            raise FileNotFoundError(f"Base MAX tratada no encontrada: {caminho_max}")
        df_max = carregar_base_zip(str(caminho_max))

        # Tabelionato tratado
        caminho_tabelionato = OUTPUT_DIR / "tabelionato_tratada" / "tabelionato_tratado.zip"
        if not caminho_tabelionato.exists():
            raise FileNotFoundError(f"Base Tabelionato tratada no encontrada: {caminho_tabelionato}")
        df_tabelionato = carregar_base_zip(str(caminho_tabelionato))

        # Obter DataExtracao e normalizar para YYYY-MM-DD (mapeamento direto)
        if 'DataExtracao' not in df_tabelionato.columns:
            raise KeyError("Base Tabelionato tratada sem 'DataExtracao'.")
        data_pagamento = str(df_tabelionato['DataExtracao'].dropna().iloc[0])[:10]

        # Base de custas
        df_custas = carregar_base_custas()

        # 2. Filtrar MAX por status aberto
        logger.info("2. Filtrando MAX por status aberto...")
        df_max_aberto = filtrar_max_status_aberto(df_max)

        # 3. Identificar diferena (MAX - Tabelionato)
        logger.info("3. Identificando protocolos apenas na MAX...")
        df_diferenca = identificar_diferenca_max_tabelionato(df_max_aberto, df_tabelionato)

        validacao_origem_max = localizar_chaves_ausentes(
            df_diferenca,
            df_max_aberto,
        )
        if getattr(validacao_origem_max, "possui_inconsistencias", False):
            log_validation_presence(
                "Baixa - presenca na base MAX",
                validacao_origem_max.total_verificado,
                validacao_origem_max.amostras_inconsistentes,
            )
            resumo = resumir_amostras(validacao_origem_max.amostras_inconsistentes)
            raise ValueError(
                "Alguns protocolos identificados para baixa no esto mais presentes na base MAX filtrada: "
                f"{resumo}"
            )
        else:
            log_validation_presence(
                "Baixa - presenca na base MAX",
                validacao_origem_max.total_verificado,
                [],
            )

        validacao_chaves = localizar_chaves_presentes(df_diferenca, df_tabelionato)
        if getattr(validacao_chaves, "possui_inconsistencias", False):
            log_validation_result(
                "Baixa - protocolos MAX vs Tabelionato",
                validacao_chaves.total_verificado,
                validacao_chaves.amostras_inconsistentes,
            )
            resumo = resumir_amostras(validacao_chaves.amostras_inconsistentes)
            raise ValueError(
                "Foi identificada sobreposicao entre protocolos da baixa e da base "
                f"Tabelionato: {resumo}"
            )
        else:
            log_validation_result(
                "Baixa - protocolos MAX vs Tabelionato",
                validacao_chaves.total_verificado,
                [],
            )

        if len(df_diferenca) == 0:
            mensagem = "Nenhum protocolo em aberto da MAX ficou sem retorno do Tabelionato."
            logger.warning(mensagem)
            sucesso = True
            return ResultadoBaixa(
                status="sem_registros",
                mensagem=mensagem,
                duracao=(datetime.now() - inicio).total_seconds(),
            )

        # 4. Enriquecer com custas
        logger.info("4. Enriquecendo com dados de custas...")
        df_enriquecido, df_checagem = enriquecer_com_custas(df_diferenca, df_custas)

        total_exportados = len(df_enriquecido)
        total_nao_exportados = len(df_checagem)

        if total_exportados == 0:
            mensagem = (
                "Aps o enriquecimento, nenhum protocolo permaneceu elegvel para baixa. "
                f"Todos os {total_nao_exportados:,} registros esto pendentes para anlise."
            )
            logger.warning(mensagem)
            salvar_checagem(df_checagem)
            log_metrics(
                "Baixa Tabelionato",
                {
                    "Registros MAX carregados": f"{len(df_max):,}",
                    "Registros Tabelionato carregados": f"{len(df_tabelionato):,}",
                    "Diferena MAX - Tabelionato": f"{len(df_diferenca):,}",
                    "Exportados para baixa": f"{total_exportados:,}",
                    "Pendentes por custas": f"{total_nao_exportados:,}",
                },
            )
            logger.info("Registros pendentes de custas: %s", total_nao_exportados)
            sucesso = True
            return ResultadoBaixa(
                status="sem_registros",
                mensagem=mensagem,
                arquivo_checagem=None,
                total_nao_exportados=total_nao_exportados,
                duracao=(datetime.now() - inicio).total_seconds(),
            )

        # 5. Gerar layout final
        logger.info("5. Gerando layout final...")
        df_final = gerar_layout_final(df_enriquecido, data_pagamento)

        # 6. Salvar resultado
        logger.info("6. Salvando resultado...")
        arquivo_final = salvar_resultado_baixa(df_final)

        logger.info("7. Registrando protocolos sem match (sem gerar arquivo)...")
        salvar_checagem(df_checagem)

        resultado = ResultadoBaixa(
            status="sucesso",
            mensagem="Processo de baixa concluido com sucesso.",
            arquivo_final=arquivo_final,
            arquivo_checagem=None,
            total_exportados=total_exportados,
            total_nao_exportados=total_nao_exportados,
            duracao=(datetime.now() - inicio).total_seconds(),
        )

        log_metrics(
            "Baixa Tabelionato",
            {
                "Registros MAX carregados": f"{len(df_max):,}",
                "Registros Tabelionato carregados": f"{len(df_tabelionato):,}",
                "Diferena MAX - Tabelionato": f"{len(df_diferenca):,}",
                "Exportados para baixa": f"{total_exportados:,}",
                "Pendentes por custas": f"{total_nao_exportados:,}",
            },
        )

        logger.info("Arquivo final: %s", resultado.arquivo_final)
        logger.info("Registros sem exportacao mantidos em memoria: %s", total_nao_exportados)
        logger.info("Total de registros exportados: %s", total_exportados)
        logger.info("Registros pendentes de custas: %s", total_nao_exportados)

        sucesso = True

        return resultado

    except Exception as exc:  # pragma: no cover - fluxo crtico
        logger.exception("Erro no processo de baixa")
        raise
    finally:
        log_session_end("Baixa Tabelionato", success=sucesso)

def main() -> int:
    """Funo principal para execucao standalone."""

    try:
        resultado = executar_processo_baixa()
    except Exception as exc:
        logger.error("Falha ao executar a baixa: %s", exc)
        linhas = [
            "[ERRO] Processo de baixa nao concluido.",
            "",
            f"Detalhes: {exc}",
        ]
        print_section("BAIXA - TABELIONATO", linhas, leading_break=False)
        return 1

    if resultado.status == "sucesso":
        linhas = [
            "[STEP] Baixa Tabelionato",
            "",
            f"Registros exportados: {format_int(resultado.total_exportados)}",
        ]
        if resultado.total_nao_exportados:
            linhas.append(
                f"Pendentes registrados em log: {format_int(resultado.total_nao_exportados)}"
            )
        if resultado.arquivo_final:
            linhas.extend(["", f"Arquivo exportado: {resultado.arquivo_final}"])
        if resultado.duracao:
            linhas.append(f"Duracao: {format_duration(resultado.duracao)}")

        print_section("BAIXA - TABELIONATO", linhas, leading_break=False)
        return 0

    if resultado.status == "sem_registros":
        mensagem = resultado.mensagem or "Nenhum protocolo elegivel para baixa."
        linhas = [
            "[STEP] Baixa Tabelionato",
            "",
            mensagem,
        ]
        if resultado.total_nao_exportados:
            linhas.append(
                f"Pendentes registrados em log: {format_int(resultado.total_nao_exportados)}"
            )
        if resultado.duracao:
            linhas.append(f"Duracao: {format_duration(resultado.duracao)}")

        print_section("BAIXA - TABELIONATO", linhas, leading_break=False)
        return 0

    mensagem = resultado.mensagem or "Processo de baixa nao concluido."
    linhas = [
        "[ERRO] Processo de baixa nao concluido.",
        "",
        mensagem,
    ]
    if resultado.duracao:
        linhas.append(f"Duracao: {format_duration(resultado.duracao)}")
    print_section("BAIXA - TABELIONATO", linhas, leading_break=False)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())

