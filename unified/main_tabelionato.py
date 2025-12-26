#!/usr/bin/env python3
"""Orquestra o fluxo completo do projeto Tabelionato."""

from __future__ import annotations

import argparse
import importlib
import logging
import os
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Iterable, List, Sequence, Tuple

BASE_DIR = Path(__file__).resolve().parent.parent
LOG_DIR = BASE_DIR / "data" / "logs"
VENV_DIR = BASE_DIR / ".venv"


def _caminho_python_venv() -> Path:
    """Retorna o caminho do interpretador Python dentro do .venv."""

    if os.name == "nt":
        return VENV_DIR / "Scripts" / "python.exe"
    return VENV_DIR / "bin" / "python"

from src.utils.logger_config import (
    get_logger,
    log_session_end,
    log_session_start,
)


def configurar_logger(verbose: bool) -> Tuple[logging.Logger, Path]:
    """Configura o logger do fluxo completo reutilizando o arquivo central."""

    LOG_DIR.mkdir(parents=True, exist_ok=True)
    log_file = LOG_DIR / "tabelionato.log"

    logger = get_logger("fluxo_completo")
    logger.setLevel(logging.DEBUG if verbose else logging.INFO)

    # Ajusta handlers existentes (principalmente o console) para respeitar o modo verboso
    for handler in logger.handlers:
        if isinstance(handler, logging.StreamHandler):
            handler.setLevel(logging.DEBUG if verbose else logging.INFO)

    return logger, log_file


def _localizar_python(logger: logging.Logger | None = None) -> List[str]:
    """Retorna o comando preferencial para invocar o Python.

    Mantém compatibilidade com ambientes Windows onde o interpretador pode estar
    instalado em caminhos com espaços ou remoções posteriores. Caso o caminho
    reportado pelo ``sys.executable`` não esteja acessível, tenta localizar
    alternativas conhecidas como ``python`` (PATH) ou ``py`` (launcher do
    Windows).
    """

    python_venv = _caminho_python_venv()
    if python_venv.exists():
        return [str(python_venv)]

    if sys.executable:
        exe_path = Path(sys.executable)
        if exe_path.exists():
            return [str(exe_path)]
        if logger:
            logger.warning(
                "Interpretador reportado por sys.executable inexistente: %s",
                exe_path,
            )

    for nome in ("python", "python3"):
        localizado = shutil.which(nome)
        if localizado:
            return [localizado]

    if os.name == "nt":
        launcher = shutil.which("py")
        if launcher:
            return [launcher, "-3"]

    raise RuntimeError(
        "Nenhum interpretador Python acessível foi encontrado. Configure o PATH "
        "ou utilize o launcher do Python para executar o fluxo."
    )


def garantir_venv_disponivel(logger: logging.Logger) -> Path:
    """Garante que o ambiente virtual `.venv` exista e retorne o Python interno."""

    python_venv = _caminho_python_venv()
    if python_venv.exists():
        return python_venv

    logger.info("Preparando ambiente virtual dedicado em %s", VENV_DIR)
    comando_base = _localizar_python(logger)
    resultado = subprocess.run([
        *comando_base,
        "-m",
        "venv",
        str(VENV_DIR),
    ], cwd=BASE_DIR)

    if resultado.returncode != 0 or not python_venv.exists():
        raise RuntimeError(
            "Falha ao criar o ambiente virtual (.venv). Código de saída: %s"
            % resultado.returncode
        )

    logger.info("Ambiente virtual criado com sucesso.")
    return python_venv


def garantir_dependencias_instaladas(logger: logging.Logger) -> None:
    """Certifica-se de que os pacotes do requirements estão prontos para uso."""

    modulos_obrigatorios = {
        "pandas": "pandas",
        "numpy": "numpy",
        "pyodbc": "pyodbc",
        "dotenv": "python-dotenv",
        "py7zr": "py7zr",
        "pyautogui": "pyautogui",
    }

    faltantes = []
    for modulo in modulos_obrigatorios:
        try:
            importlib.import_module(modulo)
        except ImportError:
            faltantes.append(modulos_obrigatorios[modulo])

    if not faltantes:
        return

    logger.info(
        "Dependências ausentes detectadas (%s). Instalando via requirements.txt...",
        ", ".join(sorted(set(faltantes))),
    )

    python_venv = garantir_venv_disponivel(logger)
    resultado = subprocess.run(
        [
            str(python_venv),
            "-m",
            "pip",
            "install",
            "-r",
            str(BASE_DIR / "requirements.txt"),
        ],
        cwd=BASE_DIR,
    )

    if resultado.returncode != 0:
        raise RuntimeError(
            "Falha ao instalar dependências (pip retornou código %s)."
            % resultado.returncode
        )

    for modulo in modulos_obrigatorios:
        importlib.import_module(modulo)

    logger.info("Dependências instaladas e validadas com sucesso.")


def assegurar_execucao_no_venv(
    logger: logging.Logger, argumentos: Sequence[str]
) -> Tuple[int, bool]:
    """Reexecuta o fluxo dentro do `.venv` quando necessário."""

    python_venv = garantir_venv_disponivel(logger)

    try:
        atual = Path(sys.executable).resolve()
    except FileNotFoundError:
        atual = Path(sys.executable)

    if atual == python_venv.resolve():
        return 0, False

    logger.info(
        "Reexecutando fluxo dentro do ambiente virtual dedicado (.venv)."
    )

    env = os.environ.copy()
    env.setdefault("VIRTUAL_ENV", str(VENV_DIR))
    env["PATH"] = str(python_venv.parent) + os.pathsep + env.get("PATH", "")
    env["TABELIONATO_BOOTSTRAP"] = "1"

    resultado = subprocess.run(
        [str(python_venv), str(BASE_DIR / "fluxo_completo.py"), *argumentos],
        cwd=BASE_DIR,
        env=env,
    )

    return resultado.returncode, True


def obter_etapas(logger: logging.Logger) -> List[Tuple[str, str, List[str]]]:
    """Retorna a sequncia padro de etapas do fluxo."""

    comando_python = _localizar_python(logger)

    return [
        (
            "extracao_max",
            "Extrao MAX",
            [*comando_python, "-m", "src.extracao_base_max_tabelionato"],
        ),
        (
            "extracao_tabelionato",
            "Extrao Tabelionato",
            [*comando_python, "-m", "src.extrair_base_tabelionato"],
        ),
        (
            "tratamento_max",
            "Tratamento MAX",
            [*comando_python, "-m", "src.tratamento_max"],
        ),
        (
            "tratamento_tabelionato",
            "Tratamento Tabelionato",
            [*comando_python, "-m", "src.tratamento_tabelionato"],
        ),
        (
            "batimento",
            "Batimento",
            [*comando_python, "-m", "src.batimento_tabelionato"],
        ),
        (
            "baixa",
            "Baixa",
            [*comando_python, "-m", "src.baixa_tabelionato"],
        ),
    ]


def filtrar_etapas(
    etapas: Sequence[Tuple[str, str, List[str]]], *, skip_extraction: bool
) -> List[Tuple[str, str, List[str]]]:
    """Filtra etapas conforme parmetros informados."""

    ids_a_pular = {"extracao_max", "extracao_tabelionato"} if skip_extraction else set()
    return [step for step in etapas if step[0] not in ids_a_pular]


def executar_etapas(
    etapas: Iterable[Tuple[str, str, List[str]]], logger: logging.Logger
) -> Tuple[int, str | None]:
    """Executa as etapas informadas sequencialmente."""

    etapas = list(etapas)
    total = len(etapas)

    if total == 0:
        logger.warning("Nenhuma etapa selecionada para execucao.")
        return 0, None

    for indice, (step_id, descricao, comando) in enumerate(etapas, start=1):
        logger.info("[Passo %s/%s] %s", indice, total, descricao)
        try:
            resultado = subprocess.run(comando, cwd=BASE_DIR)
        except FileNotFoundError as exc:
            logger.error(
                "Falha ao acionar o Python para a etapa %s (%s)",
                descricao,
                comando[0],
            )
            logger.debug("Detalhes do erro: %s", exc)
            return 127, step_id
        if resultado.returncode != 0:
            logger.error(
                "Falha na etapa %s (codigo de saida %s)",
                descricao,
                resultado.returncode,
            )
            return resultado.returncode, step_id

        logger.info("%s concluido com sucesso", descricao)

    logger.info("Fluxo completo finalizado sem erros.")
    return 0, None





def main(argv: Sequence[str] | None = None) -> int:
    """Ponto de entrada do utilitrio de linha de comando."""

    parser = argparse.ArgumentParser(
        description=(
            "Executa todas as etapas do projeto Tabelionato em sequncia, "
            "com logs consolidados e preparo de ambiente."
        )
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Exibe logs detalhados tambm no console.",
    )

    argumentos = list(argv if argv is not None else sys.argv[1:])
    args = parser.parse_args(argumentos)

    logger, log_file = configurar_logger(args.verbose)

    try:
        retorno, reexecutado = assegurar_execucao_no_venv(logger, argumentos)
    except Exception as exc:  # pragma: no cover - cenário operacional
        logger.error("Não foi possível preparar o ambiente virtual: %s", exc)
        return 1
    if reexecutado:
        return retorno

    log_session_start("Fluxo Completo")

    try:
        garantir_dependencias_instaladas(logger)
    except Exception as exc:  # pragma: no cover - cenário operacional
        logger.error("Não foi possível preparar as dependências: %s", exc)
        log_session_end("Fluxo Completo", success=False)
        return 1

    try:
        etapas = obter_etapas(logger)
    except RuntimeError as exc:
        logger.error("Não foi possível localizar o interpretador Python: %s", exc)
        log_session_end("Fluxo Completo", success=False)
        return 1

    codigo_saida = 0
    etapa_com_erro = None
    codigo_saida, etapa_com_erro = executar_etapas(etapas, logger)

    logger.info("Logs consolidados em: %s", log_file)

    try:
        if codigo_saida != 0:
            logger.error(
                "Fluxo interrompido na etapa '%s'. Consulte o log para detalhes.",
                etapa_com_erro,
            )
            return codigo_saida

        return 0
    finally:
        log_session_end("Fluxo Completo", success=codigo_saida == 0)


if __name__ == "__main__":
    raise SystemExit(main())

