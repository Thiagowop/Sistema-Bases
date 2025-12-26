"""Utilitarios para trabalhar com arquivos compactados e o executavel do 7-Zip."""

from __future__ import annotations

import os
import shutil
import subprocess
import zipfile
from pathlib import Path
from typing import Iterable, List, Sequence

from .logger_config import get_logger

PROJECT_ROOT = Path(__file__).resolve().parents[2]
BIN_DIR = PROJECT_ROOT / "bin"

logger = get_logger("utils.archives")


def _locate_existing_7zip(destino: Path) -> Path | None:
    """Retorna o executavel do 7-Zip caso ja exista no diretorio informado."""

    candidatos = [
        destino / "7z.exe",
        destino / "7za.exe",
        destino / "7zz.exe",
        destino / "7_zip_rar" / "7z.exe",
        destino / "7_zip_rar" / "7za.exe",
        destino / "7_zip_rar" / "7zz.exe",
        destino / "7_zip_rar" / "7-Zip" / "7z.exe",
        destino / "7_zip_rar" / "7-Zip" / "7za.exe",
        destino / "7_zip_rar" / "7-Zip" / "7zz.exe",
    ]

    for candidato in candidatos:
        if candidato.exists():
            return candidato

    return None


def ensure_7zip_ready(bin_dir: Path | None = None) -> Path | None:
    """Garante que o executavel do 7-Zip esteja disponivel localmente.

    Retorna o caminho localizado ou ``None`` quando nao for possivel preparar.
    """

    destino = bin_dir or BIN_DIR

    existente = _locate_existing_7zip(destino)
    if existente:
        return existente

    pacote = destino / "7_zip_rar.zip"
    if not pacote.exists():
        logger.debug("Pacote 7_zip_rar.zip nao encontrado em %s", destino)
        return None

    try:
        with zipfile.ZipFile(pacote, "r") as arquivo_zip:
            arquivo_zip.extractall(destino)
        logger.info("7-Zip preparado a partir de %s", pacote)
    except Exception as exc:  # pragma: no cover - operacao de IO
        logger.error("Falha ao extrair pacote do 7-Zip: %s", exc)
        return None

    return _locate_existing_7zip(destino)


def _collect_candidate_paths(extra_paths: Sequence[Path] | None = None) -> List[Path]:
    candidatos: List[Path] = []

    env_path = os.getenv("SEVEN_ZIP_PATH")
    if env_path:
        candidatos.append(Path(env_path))

    for nome in ("7z", "7z.exe", "7za.exe", "7zz.exe", "7zr.exe"):
        localizado = shutil.which(nome)
        if localizado:
            candidatos.append(Path(localizado))

    possiveis_raizes: Iterable[Path] = [
        Path(os.environ.get("ProgramFiles", "")),
        Path(os.environ.get("ProgramFiles(x86)", "")),
        BIN_DIR,
        BIN_DIR / "7_zip_rar",
        PROJECT_ROOT / "bin",
        PROJECT_ROOT / "bin" / "7_zip_rar",
    ]

    for raiz in possiveis_raizes:
        if not raiz:
            continue
        for nome in ("7z.exe", "7za.exe", "7zz.exe", "7zr.exe"):
            candidatos.append(raiz / "7-Zip" / nome)
            candidatos.append(raiz / nome)

    if extra_paths:
        candidatos.extend(extra_paths)

    return candidatos


def find_7zip_executable(extra_paths: Sequence[Path] | None = None) -> Path:
    """Localiza o executavel do 7-Zip, considerando diferentes instalacoes."""

    ensure_7zip_ready()
    vistos: set[Path] = set()

    for candidato in _collect_candidate_paths(extra_paths):
        if not candidato:
            continue
        try:
            caminho = candidato.resolve()
        except FileNotFoundError:
            continue
        if caminho in vistos:
            continue
        vistos.add(caminho)
        if caminho.exists():
            return caminho

    raise FileNotFoundError(
        "7-Zip nao encontrado. Configure SEVEN_ZIP_PATH, disponibilize bin/7_zip_rar.zip "
        "ou instale o utilitario no sistema."
    )


def extract_with_7zip(
    arquivo: Path, destino: Path, *, senha: str | None = None
) -> List[Path]:
    """Extrai ``arquivo`` para ``destino`` utilizando o 7-Zip e retorna os novos arquivos."""

    destino.mkdir(parents=True, exist_ok=True)

    arquivos_preexistentes = {item.resolve() for item in destino.rglob("*") if item.is_file()}

    executavel = find_7zip_executable()
    comando = [str(executavel), "x", str(arquivo), f"-o{destino}", "-y"]
    comando.append(f"-p{senha}" if senha else "-p-")

    resultado = subprocess.run(comando, capture_output=True, text=True)  # pragma: no cover
    if resultado.returncode != 0:
        detalhe = resultado.stderr.strip() or resultado.stdout.strip() or "erro nao informado"
        raise RuntimeError(f"Falha ao extrair {arquivo} com 7-Zip (codigo {resultado.returncode}): {detalhe}")

    novos_arquivos = [
        item
        for item in destino.rglob("*")
        if item.is_file() and item.resolve() not in arquivos_preexistentes
    ]
    return novos_arquivos
