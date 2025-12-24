"""Logger centralizado do projeto VIC.

Configura logging baseado nas configurações do config.yaml.
"""

import logging
import logging.handlers
import os
from pathlib import Path
from typing import Optional, Dict, Any


class ProjectLogger:
    """Logger centralizado configurável."""
    
    _loggers: Dict[str, logging.Logger] = {}
    _configured = False
    
    @classmethod
    def setup_logging(cls, config: Dict[str, Any]) -> None:
        """Configura o sistema de logging baseado no config.
        
        Args:
            config: Configurações de logging do config.yaml
        """
        if cls._configured:
            return
            
        logging_config = config.get('logging', {})
        
        # Configurações padrão
        level_name = os.getenv('VIC_LOG_LEVEL', logging_config.get('level', 'WARNING'))
        level = getattr(logging, level_name, logging.INFO)
        log_format = logging_config.get('format', '%(message)s')
        date_format = logging_config.get('date_format', '%Y-%m-%d %H:%M:%S')

        formatter = logging.Formatter(log_format, date_format)
        
        # Configurar logging básico e limpar handlers anteriores
        logging.basicConfig(level=level, format=log_format, datefmt=date_format, force=True)
        root_logger = logging.getLogger()

        # Console handler adicional (permite múltiplos destinos futuramente)
        console_config = logging_config.get('console_handler', {})
        if console_config.get('enabled', True):
            console_handler = logging.StreamHandler()
            console_handler.setFormatter(formatter)
            console_handler.setLevel(level)
            root_logger.addHandler(console_handler)
        
        # Configurar handler de arquivo
        file_config = logging_config.get('file_handler', {})
        if file_config.get('enabled', False):
            # Diretório de logs relativo à raiz do projeto
            paths_cfg = config.get('paths', {})
            logs_dir = Path(paths_cfg.get('logs', 'data/logs'))
            if not logs_dir.is_absolute():
                projeto_root = Path(paths_cfg.get('projeto_root', '.'))
                logs_dir = projeto_root / logs_dir
            logs_dir.mkdir(parents=True, exist_ok=True)

            # Configurar rotating file handler
            log_file = logs_dir / file_config.get('filename', 'projeto_vic.log')
            max_bytes = file_config.get('max_bytes', 10485760)  # 10MB
            backup_count = file_config.get('backup_count', 5)
            
            file_handler = logging.handlers.RotatingFileHandler(
                log_file, maxBytes=max_bytes, backupCount=backup_count, encoding='utf-8'
            )
            # No arquivo mantemos UTF-8 e nao precisamos normalizar para ASCII
            file_handler.setFormatter(logging.Formatter(log_format, date_format))
            file_handler.setLevel(level)
            
            root_logger.addHandler(file_handler)
        
        cls._configured = True
    
    @classmethod
    def get_logger(cls, name: str, config: Optional[Dict[str, Any]] = None) -> logging.Logger:
        """Retorna logger configurado para o módulo.
        
        Args:
            name: Nome do logger (geralmente __name__)
            config: Configurações opcionais (se não fornecidas, usa configuração padrão)
            
        Returns:
            Logger configurado
        """
        if name not in cls._loggers:
            # Configurar logging se ainda não foi feito
            if not cls._configured and config:
                cls.setup_logging(config)
            
            # Criar logger específico
            logger = logging.getLogger(name)
            cls._loggers[name] = logger
        
        return cls._loggers[name]


def get_logger(name: str, config: Optional[Dict[str, Any]] = None) -> logging.Logger:
    """Função de conveniência para obter logger.
    
    Args:
        name: Nome do logger
        config: Configurações opcionais
        
    Returns:
        Logger configurado
    """
    return ProjectLogger.get_logger(name, config)


def log_section(logger: logging.Logger, title: str) -> None:
    """Imprime um cabeçalho padronizado."""
    print("=" * 60)
    print(title)
    print("=" * 60)
    print("")
