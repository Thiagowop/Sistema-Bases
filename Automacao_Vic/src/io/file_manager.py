"""Gerenciador de arquivos centralizado para o pipeline VIC.

O objetivo é oferecer operações padronizadas de leitura e escrita de
CSV/ZIP usando apenas as configurações fornecidas pelo ``config.yaml``.
As rotinas priorizam validações fail-fast e registram informações úteis
para depuração sem carregar responsabilidades de regra de negócio.
"""

from __future__ import annotations

import logging
import zipfile
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import pandas as pd


class FileManager:
    """Gerencia operações de entrada e saída de arquivos."""

    def __init__(self, config: Dict[str, Any]) -> None:
        self.config = config
        self.logger = logging.getLogger(__name__)

        self._validar_config()

        global_cfg = self.config.get("global", {})
        self.encoding = global_cfg.get("encoding", "utf-8")
        self.csv_separator = global_cfg.get("csv_separator", ";")
        self.paths = self.config.get("paths", {})

    # ------------------------------------------------------------------
    def _validar_config(self) -> None:
        """Garante que as seções obrigatórias existem no ``config``."""

        for section in ("global", "paths"):
            if section not in self.config:
                raise ValueError(
                    f"Seção obrigatória '{section}' ausente no config.yaml"
                )

        paths_cfg = self.config.get("paths", {})
        for key in ("input", "output"):
            if key not in paths_cfg:
                raise ValueError(
                    f"Path obrigatório 'paths.{key}' ausente no config.yaml"
                )

        if "base" not in paths_cfg.get("output", {}):
            raise ValueError(
                "Path obrigatório 'paths.output.base' ausente no config.yaml"
            )

    # ------------------------------------------------------------------
    def obter_path_input(self, tipo: str) -> Path:
        """Retorna o diretório configurado para o tipo de entrada dado."""

        input_paths = self.paths.get("input", {})
        if tipo not in input_paths:
            raise ValueError(f"Path de input para '{tipo}' não configurado")

        path = Path(input_paths[tipo])
        if not path.exists():
            raise FileNotFoundError(f"Diretório de input não existe: {path}")

        return path

    # ------------------------------------------------------------------
    def obter_path_output(self, subdir: Optional[str] = None) -> Path:
        """Retorna o diretório de saída e garante sua existência."""

        base_path = Path(self.paths["output"]["base"])
        path = base_path / subdir if subdir else base_path
        path.mkdir(parents=True, exist_ok=True)
        return path

    # ------------------------------------------------------------------
    def validar_arquivo_existe(self, arquivo: Union[str, Path]) -> Path:
        """Valida se o ``arquivo`` informado existe e é um arquivo."""

        path = Path(arquivo)
        if not path.exists():
            raise FileNotFoundError(f"Arquivo não encontrado: {path}")
        if not path.is_file():
            raise ValueError(f"Path não é um arquivo: {path}")
        return path

    # ------------------------------------------------------------------
    def ler_csv(self, arquivo: Union[str, Path], **kwargs: Any) -> pd.DataFrame:
        """Lê um CSV aplicando as configurações globais padrão."""

        path = self.validar_arquivo_existe(arquivo)
        final_kwargs = {
            "sep": self.csv_separator,
            "encoding": self.encoding,
            "dtype": str,
            **kwargs,
        }

        try:
            df = pd.read_csv(path, **final_kwargs)
            self.logger.debug(
                "CSV carregado: %s (%s registros)", path, f"{len(df):,}"
            )
            return df
        except Exception as exc:  # pragma: no cover - reempacota exceções
            raise ValueError(f"Erro ao ler CSV {path}: {exc}") from exc

    # ------------------------------------------------------------------
    def ler_zip_csv(
        self, arquivo_zip: Union[str, Path], nome_csv: Optional[str] = None
    ) -> pd.DataFrame:
        """Lê um CSV contido em um arquivo ZIP."""

        zip_path = self.validar_arquivo_existe(arquivo_zip)

        try:
            with zipfile.ZipFile(zip_path, "r") as zip_file:
                csv_files = [
                    member
                    for member in zip_file.namelist()
                    if member.lower().endswith(".csv")
                ]

                if not csv_files:
                    raise ValueError(
                        f"Nenhum arquivo CSV encontrado no ZIP: {zip_path}"
                    )

                if nome_csv:
                    if nome_csv not in csv_files:
                        raise ValueError(
                            f"CSV '{nome_csv}' não encontrado no ZIP"
                        )
                    csv_target = nome_csv
                else:
                    csv_target = csv_files[0]
                    if len(csv_files) > 1:
                        self.logger.warning(
                            "Múltiplos CSVs no ZIP %s, usando: %s",
                            zip_path,
                            csv_target,
                        )

                with zip_file.open(csv_target) as csv_file:
                    df = pd.read_csv(
                        csv_file,
                        sep=self.csv_separator,
                        encoding=self.encoding,
                        dtype=str,
                    )

            self.logger.debug(
                "CSV carregado do ZIP: %s/%s (%s registros)",
                zip_path,
                csv_target,
                f"{len(df):,}",
            )
            return df
        except zipfile.BadZipFile as exc:
            raise ValueError(f"Arquivo ZIP inválido: {zip_path}") from exc
        except Exception as exc:  # pragma: no cover - reempacota exceções
            raise ValueError(f"Erro ao ler ZIP {zip_path}: {exc}") from exc

    # ------------------------------------------------------------------
    def ler_csv_ou_zip(self, arquivo: Union[str, Path]) -> pd.DataFrame:
        """Lê automaticamente arquivos CSV ou ZIP."""

        path = self.validar_arquivo_existe(arquivo)
        suffix = path.suffix.lower()

        if suffix == ".zip":
            return self.ler_zip_csv(path)
        if suffix == ".csv":
            return self.ler_csv(path)

        raise ValueError(f"Formato de arquivo não suportado: {suffix}")

    # ------------------------------------------------------------------
    def salvar_csv(
        self, df: pd.DataFrame, arquivo: Union[str, Path], **kwargs: Any
    ) -> Path:
        """Salva ``df`` como CSV garantindo o diretório alvo."""

        path = Path(arquivo)
        path.parent.mkdir(parents=True, exist_ok=True)

        final_kwargs = {
            "sep": self.csv_separator,
            "encoding": self.encoding,
            "index": False,
            **kwargs,
        }

        try:
            df.to_csv(path, **final_kwargs)
            self.logger.info(
                "CSV salvo: %s (%s registros)", path, f"{len(df):,}"
            )
            return path
        except Exception as exc:  # pragma: no cover - reempacota exceções
            raise ValueError(f"Erro ao salvar CSV {path}: {exc}") from exc

    # ------------------------------------------------------------------
    def salvar_zip(
        self, arquivos: Dict[str, Union[pd.DataFrame, Path, str]],
        arquivo_zip: Union[str, Path],
    ) -> Path:
        """Salva múltiplos arquivos em um ZIP."""

        zip_path = Path(arquivo_zip)
        zip_path.parent.mkdir(parents=True, exist_ok=True)

        try:
            with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zip_file:
                for nome_arquivo, conteudo in arquivos.items():
                    if isinstance(conteudo, pd.DataFrame):
                        csv_content = conteudo.to_csv(
                            sep=self.csv_separator,
                            index=False,
                        ).encode(self.encoding)
                        zip_file.writestr(nome_arquivo, csv_content)
                    else:
                        arquivo_path = Path(conteudo)
                        if arquivo_path.exists():
                            zip_file.write(arquivo_path, nome_arquivo)
                        else:
                            self.logger.warning(
                                "Arquivo não encontrado para ZIP: %s",
                                arquivo_path,
                            )

            self.logger.debug(
                "ZIP criado: %s (%s arquivos)",
                zip_path,
                f"{len(arquivos):,}",
            )
            return zip_path
        except Exception as exc:  # pragma: no cover - reempacota exceções
            raise ValueError(f"Erro ao criar ZIP {zip_path}: {exc}") from exc

    # ------------------------------------------------------------------
    def listar_arquivos(
        self,
        diretorio: Union[str, Path],
        extensoes: Optional[List[str]] = None,
    ) -> List[Path]:
        """Lista arquivos do ``diretorio`` filtrando por ``extensoes``."""

        dir_path = Path(diretorio)
        if not dir_path.exists():
            self.logger.warning("Diretório não existe: %s", dir_path)
            return []
        if not dir_path.is_dir():
            raise ValueError(f"Path não é um diretório: {dir_path}")

        extensoes_normalizadas = (
            {ext.lower() for ext in extensoes} if extensoes else None
        )

        arquivos: List[Path] = []
        for arquivo in dir_path.iterdir():
            if arquivo.is_file():
                if (
                    extensoes_normalizadas is None
                    or arquivo.suffix.lower() in extensoes_normalizadas
                ):
                    arquivos.append(arquivo)

        return sorted(arquivos)

    # ------------------------------------------------------------------
    def encontrar_arquivo_mais_recente(
        self, diretorio: Union[str, Path], padrao: str = "*"
    ) -> Optional[Path]:
        """Retorna o arquivo mais recente do diretório informado."""

        dir_path = Path(diretorio)
        if not dir_path.exists():
            return None

        arquivos = [f for f in dir_path.glob(padrao) if f.is_file()]
        if not arquivos:
            return None

        arquivo_mais_recente = max(arquivos, key=lambda f: f.stat().st_mtime)
        self.logger.info(
            "Arquivo mais recente encontrado: %s", arquivo_mais_recente
        )
        return arquivo_mais_recente

    # ------------------------------------------------------------------
    def limpar_diretorio(
        self, diretorio: Union[str, Path], manter_arquivos: Optional[List[str]] = None
    ) -> int:
        """Remove arquivos do diretório, preservando os listados em ``manter_arquivos``."""

        dir_path = Path(diretorio)
        if not dir_path.exists():
            return 0

        manter = set(manter_arquivos or [])
        removidos = 0

        for arquivo in dir_path.iterdir():
            if arquivo.is_file() and arquivo.name not in manter:
                try:
                    arquivo.unlink()
                    removidos += 1
                    self.logger.debug("Arquivo removido: %s", arquivo)
                except Exception as exc:  # pragma: no cover - não é crítico
                    self.logger.warning(
                        "Erro ao remover arquivo %s: %s", arquivo, exc
                    )

        if removidos:
            self.logger.info(
                "Diretório limpo: %s (%s arquivos removidos)",
                dir_path,
                f"{removidos:,}",
            )

        return removidos
