"""Módulo de Exportação - Export genérico CSV/ZIP.

Princípios:
- Single Responsibility: apenas exportação de dados
- Config First: configurações do config.yaml
- Fail-Fast: validação rigorosa
- DRY: reutilização por todos os processadores
"""

import os
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, Optional, Union, List
import pandas as pd
import logging

def clean_old_files(directory: Path, name_prefix: str, extension: str, keep: int = 1) -> None:
    """Remove arquivos antigos mantendo apenas os mais recentes por prefixo.

    Ex.: prefixo 'vic_tratada' e ext '.zip' => mantém os N mais recentes
    entre 'vic_tratada_*.zip'.
    """
    logger = logging.getLogger(__name__)
    try:
        directory = Path(directory)
        if not directory.exists():
            return
        candidates = [
            p for p in directory.glob(f"{name_prefix}_*{extension}") if p.is_file()
        ]
        if len(candidates) <= keep:
            return
        candidates.sort(key=lambda p: p.stat().st_mtime, reverse=True)
        for old in candidates[keep:]:
            try:
                old.unlink()
                logger.debug(f"Removido arquivo antigo: {old}")
            except Exception:
                logger.warning(f"Falha ao remover arquivo antigo: {old}")
    except Exception as e:
        logger.warning(f"Nao foi possivel limpar arquivos antigos: {e}")


class ExportacaoService:
    """Serviço de exportação padronizado."""
    
    def __init__(self, config: Dict[str, Any], file_manager=None):
        """Inicializa o serviço de exportação.
        
        Args:
            config: Configuração carregada do config.yaml
            file_manager: Instância do FileManager (opcional)
        """
        self.config = config
        self.logger = logging.getLogger(__name__)
        self.file_manager = file_manager
        
        # Validação Fail-Fast
        self._validar_config()
        
        # Configurações globais
        global_config = self.config.get('global', {})
        self.timestamp_format = global_config.get('timestamp_format', '%Y%m%d_%H%M%S')
        self.add_timestamp = global_config.get('add_timestamp_to_files', True)
        
        # Configurações de packaging
        self.packaging = self.config.get('packaging', {})
        self.zip_outputs = self.packaging.get('zip_outputs', False)
        # Retenção de arquivos: manter apenas o mais recente por prefixo
        self.retention = self.config.get('retention', {})
        self.keep_latest_only = bool(self.retention.get('keep_latest_only', True))
        
    def _validar_config(self) -> None:
        """Valida configurações obrigatórias (Fail-Fast).
        
        Raises:
            ValueError: Se configuração obrigatória ausente
        """
        required_sections = ['global', 'paths']
        for section in required_sections:
            if section not in self.config:
                raise ValueError(f"Seção obrigatória '{section}' ausente no config.yaml")
    
    def _gerar_nome_arquivo(self, nome_base: str, extensao: str = '.csv', 
                           add_timestamp: Optional[bool] = None) -> str:
        """Gera nome de arquivo com timestamp opcional.
        
        Args:
            nome_base: Nome base do arquivo
            extensao: Extensão do arquivo
            add_timestamp: Se deve adicionar timestamp (None usa config)
            
        Returns:
            Nome do arquivo gerado
        """
        usar_timestamp = self.add_timestamp if add_timestamp is None else add_timestamp
        
        if usar_timestamp:
            timestamp = datetime.now().strftime(self.timestamp_format)
            return f"{nome_base}_{timestamp}{extensao}"
        else:
            return f"{nome_base}{extensao}"
    
    def exportar_csv(self, df: pd.DataFrame, nome_base: str, 
                    subdir: Optional[str] = None, 
                    add_timestamp: Optional[bool] = None,
                    **kwargs) -> Optional[Path]:
        """Exporta DataFrame para CSV.
        
        Args:
            df: DataFrame para exportar
            nome_base: Nome base do arquivo
            subdir: Subdiretório opcional (ex: 'max', 'vic')
            add_timestamp: Se deve adicionar timestamp (None usa config)
            **kwargs: Argumentos adicionais para file_manager.salvar_csv
            
        Returns:
            Path do arquivo exportado ou None se DataFrame vazio
        """
        if len(df) == 0:
            self.logger.warning(f"DataFrame vazio, não exportando: {nome_base}")
            return None
        
        if not self.file_manager:
            raise ValueError("FileManager não configurado para exportação")
        
        # Obter diretório de saída
        diretorio_saida = self.file_manager.obter_path_output(subdir)
        
        # Gerar nome do arquivo
        nome_arquivo = self._gerar_nome_arquivo(nome_base, '.csv', add_timestamp)
        caminho_arquivo = diretorio_saida / nome_arquivo
        
        # Exportar usando FileManager
        try:
            path_exportado = self.file_manager.salvar_csv(df, caminho_arquivo, **kwargs)
            self.logger.debug(f"CSV exportado: {path_exportado} ({len(df)} registros)")
            # Limpeza de arquivos antigos (se timestamp estiver ativo)
            usar_timestamp = self.add_timestamp if add_timestamp is None else add_timestamp
            if usar_timestamp and self.keep_latest_only:
                clean_old_files(diretorio_saida, nome_base, '.csv', keep=1)
            return path_exportado
        except Exception as e:
            self.logger.error(f"Erro ao exportar CSV {nome_base}: {e}")
            raise
    
    def exportar_inconsistencias(self, df: pd.DataFrame, nome_base: str = "inconsistencias",
                               subdir: Optional[str] = None,
                               add_timestamp: Optional[bool] = None,
                               como_zip: bool = False) -> Optional[Path]:
        """Exporta inconsistências para CSV ou ZIP.
        
        Args:
            df: DataFrame com inconsistências
            nome_base: Nome base do arquivo
            subdir: Subdiretório base (ex: 'max', 'vic')
            add_timestamp: Se deve adicionar timestamp (None usa config)
            como_zip: Se deve exportar como ZIP
            
        Returns:
            Path do arquivo exportado ou None se DataFrame vazio
        """
        if len(df) == 0:
            self.logger.warning(f"Nenhuma inconsistência para exportar: {nome_base}")
            return None
        
        # Criar subdiretório para inconsistências
        # Se subdir já é 'inconsistencias', usar diretamente; senão, criar subpasta
        if subdir == 'inconsistencias':
            subdir_inconsistencias = subdir
        else:
            subdir_inconsistencias = f"{subdir}/inconsistencias" if subdir else "inconsistencias"
        
        if como_zip:
            return self.exportar_zip(
                {f"{nome_base}.csv": df},
                nome_base,
                subdir_inconsistencias,
                add_timestamp
            )
        else:
            return self.exportar_csv(
                df, nome_base, subdir_inconsistencias, add_timestamp
            )
    
    def exportar_zip(self, arquivos: Dict[str, Union[pd.DataFrame, Path]], 
                    nome_base: str,
                    subdir: Optional[str] = None,
                    add_timestamp: Optional[bool] = None) -> Optional[Path]:
        """Exporta múltiplos arquivos em ZIP.
        
        Args:
            arquivos: Dict com {nome_no_zip: DataFrame ou Path}
            nome_base: Nome base do arquivo ZIP
            subdir: Subdiretório opcional
            add_timestamp: Se deve adicionar timestamp (None usa config)
            
        Returns:
            Path do arquivo ZIP exportado ou None se vazio
        """
        if not arquivos:
            self.logger.warning(f"Nenhum arquivo para ZIP: {nome_base}")
            return None
        
        if not self.file_manager:
            raise ValueError("FileManager não configurado para exportação")
        
        # Obter diretório de saída
        diretorio_saida = self.file_manager.obter_path_output(subdir)
        
        # Gerar nome do arquivo ZIP
        nome_arquivo = self._gerar_nome_arquivo(nome_base, '.zip', add_timestamp)
        caminho_zip = diretorio_saida / nome_arquivo
        
        # Exportar usando FileManager
        try:
            path_exportado = self.file_manager.salvar_zip(arquivos, caminho_zip)
            self.logger.debug(f"ZIP exportado: {path_exportado} ({len(arquivos)} arquivos)")
            # Limpeza de arquivos antigos (se timestamp estiver ativo)
            usar_timestamp = self.add_timestamp if add_timestamp is None else add_timestamp
            if usar_timestamp and self.keep_latest_only:
                clean_old_files(diretorio_saida, nome_base, '.zip', keep=1)
            return path_exportado
        except Exception as e:
            self.logger.error(f"Erro ao exportar ZIP {nome_base}: {e}")
            raise
    
    def exportar_com_configuracao(self, df: pd.DataFrame, 
                                 config_processador: Dict[str, Any],
                                 tipo_export: str = 'dados') -> Optional[Path]:
        """Exporta usando configurações específicas do processador.
        
        Args:
            df: DataFrame para exportar
            config_processador: Configuração específica do processador
            tipo_export: Tipo de export ('dados' ou 'inconsistencias')
            
        Returns:
            Path do arquivo exportado ou None se vazio
        """
        if len(df) == 0:
            return None
        
        # Obter configurações de export
        export_config = config_processador.get('export', {})
        
        if tipo_export == 'inconsistencias':
            nome_base = export_config.get('inconsistencies_prefix', 'inconsistencias')
            como_zip = export_config.get('inconsistencies_as_zip', True)
        else:
            nome_base = export_config.get('filename_prefix', 'dados')
            como_zip = self.zip_outputs
        
        # Determinar subdiretório
        subdir = config_processador.get('subdir')
        
        if tipo_export == 'inconsistencias':
            return self.exportar_inconsistencias(
                df, nome_base, subdir, como_zip=como_zip
            )
        else:
            if como_zip:
                return self.exportar_zip(
                    {f"{nome_base}.csv": df},
                    nome_base,
                    subdir
                )
            else:
                return self.exportar_csv(df, nome_base, subdir)

    # Limpeza foi movida para src/utils/cleanup.py
    
    def exportar_multiplos(self, dados: Dict[str, pd.DataFrame], 
                          nome_base: str,
                          subdir: Optional[str] = None,
                          como_zip: bool = True,
                          add_timestamp: Optional[bool] = None) -> List[Path]:
        """Exporta múltiplos DataFrames.
        
        Args:
            dados: Dict com {nome: DataFrame}
            nome_base: Nome base dos arquivos
            subdir: Subdiretório opcional
            como_zip: Se deve agrupar em ZIP único
            add_timestamp: Se deve adicionar timestamp
            
        Returns:
            Lista de paths dos arquivos exportados
        """
        if not dados:
            return []
        
        # Filtrar DataFrames não vazios
        dados_validos = {nome: df for nome, df in dados.items() if len(df) > 0}
        
        if not dados_validos:
            self.logger.warning(f"Todos os DataFrames estão vazios: {nome_base}")
            return []
        
        paths_exportados = []
        
        if como_zip:
            # Exportar tudo em um ZIP
            arquivos_zip = {}
            for nome, df in dados_validos.items():
                nome_csv = f"{nome}.csv"
                arquivos_zip[nome_csv] = df
            
            path_zip = self.exportar_zip(arquivos_zip, nome_base, subdir, add_timestamp)
            if path_zip:
                paths_exportados.append(path_zip)
        else:
            # Exportar cada DataFrame separadamente
            for nome, df in dados_validos.items():
                nome_arquivo = f"{nome_base}_{nome}"
                path_csv = self.exportar_csv(df, nome_arquivo, subdir, add_timestamp)
                if path_csv:
                    paths_exportados.append(path_csv)
        
        return paths_exportados
    
    def obter_estatisticas_export(self, path: Path) -> Dict[str, Any]:
        """Obtém estatísticas do arquivo exportado.
        
        Args:
            path: Path do arquivo exportado
            
        Returns:
            Dicionário com estatísticas
        """
        if not path or not path.exists():
            return {}
        
        stats = {
            'arquivo': str(path),
            'nome': path.name,
            'tamanho_bytes': path.stat().st_size,
            'tamanho_mb': round(path.stat().st_size / (1024 * 1024), 2),
            'data_criacao': datetime.fromtimestamp(path.stat().st_ctime),
            'extensao': path.suffix
        }
        
        return stats
    
    def limpar_exports_antigos(self, subdir: Optional[str] = None, 
                              dias_manter: int = 7) -> int:
        """Limpa exports antigos.
        
        Args:
            subdir: Subdiretório para limpar
            dias_manter: Número de dias para manter arquivos
            
        Returns:
            Número de arquivos removidos
        """
        if not self.file_manager:
            return 0
        
        diretorio = self.file_manager.obter_path_output(subdir)
        
        if not diretorio.exists():
            return 0
        
        # Data limite
        limite = datetime.now().timestamp() - (dias_manter * 24 * 60 * 60)
        
        removidos = 0
        for arquivo in diretorio.rglob('*'):
            if arquivo.is_file() and arquivo.stat().st_mtime < limite:
                try:
                    arquivo.unlink()
                    removidos += 1
                    self.logger.debug(f"Arquivo antigo removido: {arquivo}")
                except Exception as e:
                    self.logger.warning(f"Erro ao remover arquivo {arquivo}: {e}")
        
        if removidos > 0:
            self.logger.debug(f"Limpeza concluída: {removidos} arquivos antigos removidos")
        
        return removidos


# Funções utilitárias para compatibilidade
def criar_servico_exportacao(config: Dict[str, Any], file_manager=None) -> ExportacaoService:
    """Cria instância do serviço de exportação.
    
    Args:
        config: Configuração do projeto
        file_manager: Instância do FileManager
        
    Returns:
        Instância do ExportacaoService
    """
    return ExportacaoService(config, file_manager)

