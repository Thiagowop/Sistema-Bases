#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Loader centralizado para configurações do projeto VIC.

Fornece uma API consistente para carregar o `config.yaml` com
expansão de variáveis de ambiente e defaults seguros, além de
helpers para acesso a chaves aninhadas. Mantém compatibilidade
com o helper antigo `load_cfg()`.
"""

from __future__ import annotations

import os
import re
from copy import deepcopy
from pathlib import Path
from typing import Any, Dict, Optional

import yaml


class ConfigLoader:
    """Loader de configuração com API de classe e instância.

    Principais objetivos:
    - API unificada para todo o projeto (`load`, `get_config`)
    - Defaults seguros para seções obrigatórias (global/paths)
    - Expansão opcional de variáveis `${ENV}` em strings
    - Helper utilitário `get_nested_value`
    """

    DEFAULTS: Dict[str, Any] = {
        'global': {
            'encoding': 'utf-8',
            'csv_separator': ';',
            'timestamp_format': '%Y%m%d_%H%M%S',
            # As duas chaves abaixo garantem compatibilidade de nomes usados no código
            'add_timestamp_to_files': True,
            'add_timestamp': True,
            'empresa': {
                'cnpj': '',
            },
        },
        'paths': {
            'input': {
                'max': 'data/input/max',
                'vic': 'data/input/vic',
                'judicial': 'data/input/judicial',
                'blacklist': 'data/input/blacklist',
            },
            'output': {
                'base': 'data/output',
            },
            # opcional, usado em alguns pontos para resolver paths relativos
            'projeto_root': '.',
            'logs': 'data/logs',
        },
        # Estruturas mínimas dos processadores para evitar KeyError
        'max_processor': {
            'validation': {
                'chave_regex': r'^[A-Za-z0-9]+(?:-[A-Za-z0-9]+)+$',
            },
            'columns': {
                'required': ['CPFCNPJ_CLIENTE', 'NUMERO_CONTRATO', 'PARCELA', 'VENCIMENTO', 'VALOR'],
            },
            'export': {
                'filename_prefix': 'max_tratada',
                'inconsistencies_prefix': 'max_inconsistencias',
            },
        },
        'vic_processor': {
            'columns': {},
            'status_em_aberto': 'EM ABERTO',
            'tipos_validos': [],
            'aging_minimo': 90,
        },
        'devolucao_processor': {
            'columns': {
                'required': ['PARCELA', 'VENCIMENTO', 'CPFCNPJ_CLIENTE'],
            },
            'status_devolucao_fixo': '98',
        },
        'batimento_processor': {
            'columns': {
                'required': ['CHAVE', 'CPFCNPJ_CLIENTE', 'VENCIMENTO'],
            },
        },
        'comparacao': {
            'legacy_dir': ''
        },
        'logging': {
            'level': 'INFO',
            'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            'date_format': '%Y-%m-%d %H:%M:%S',
            'console_handler': {'enabled': True},
            'file_handler': {
                'enabled': False,
                'filename': 'projeto_vic.log',
                'max_bytes': 10_485_760,
                'backup_count': 5,
            },
        },
    }

    def __init__(self, config_path: Optional[Path | str] = None, expand_env: bool = True) -> None:
        self.config_path = Path(config_path) if config_path else self._default_config_path()
        self.expand_env = expand_env
        self._config: Optional[Dict[str, Any]] = None

    # -------- API de Classe --------
    @classmethod
    def load(cls, config_path: Optional[Path | str] = None, expand_env: bool = True) -> Dict[str, Any]:
        """Carrega e retorna o dicionário de config com defaults mesclados."""
        loader = cls(config_path=config_path, expand_env=expand_env)
        return loader.get_config()

    @classmethod
    def load_with_env_expansion(cls, config_path: Optional[Path | str] = None) -> Dict[str, Any]:
        """Compat: atalho explícito para carregar com expansão de ${ENV}."""
        return cls.load(config_path=config_path, expand_env=True)

    # -------- API de Instância --------
    def load_config(self) -> Dict[str, Any]:
        """Carrega o arquivo YAML aplicando defaults e expansão opcional."""
        raw = self._read_yaml(self.config_path)
        if raw is None:
            raw = {}
        cfg = self._merge_defaults(raw)
        if self.expand_env:
            cfg = self._expand_env_in_dict(cfg)
        # Duplicar flags de timestamp para compatibilidade entre nomes
        add_ts = cfg.get('global', {}).get('add_timestamp_to_files', True)
        cfg['global']['add_timestamp_to_files'] = bool(add_ts)
        cfg['global']['add_timestamp'] = bool(add_ts)
        self._config = cfg
        return cfg

    # Alias amigável às chamadas existentes no código
    def get_config(self) -> Dict[str, Any]:
        if self._config is None:
            return self.load_config()
        return self._config

    # Compat: alguns pontos chamavam self.config_loader.load()
    # Mantido como alias claro para evitar sombreamento do classmethod
    def load_instance(self) -> Dict[str, Any]:
        return self.get_config()

    # -------- Helpers --------
    @staticmethod
    def get_nested_value(data: Dict[str, Any], key: str, default: Any = None) -> Any:
        """Obtém valor aninhado. Suporta notação simples ou 'a.b.c'."""
        if key in data:
            return data.get(key, default)
        # Fallback para chave com pontos
        cur = data
        for part in key.split('.'):
            if not isinstance(cur, dict) or part not in cur:
                return default
            cur = cur[part]
        return cur

    # -------- Internos --------
    @staticmethod
    def _default_config_path() -> Path:
        # raiz do projeto = três níveis acima deste arquivo
        return Path(__file__).parent.parent.parent / 'config.yaml'

    @staticmethod
    def _read_yaml(path: Path) -> Optional[Dict[str, Any]]:
        if not path.exists():
            # Não lançar aqui; permitir defaults cobrirem o básico
            return None
        try:
            with open(path, 'r', encoding='utf-8') as fh:
                return yaml.safe_load(fh)  # type: ignore[return-value]
        except yaml.YAMLError as e:
            # Erro estrutural no YAML deve ser explícito
            raise yaml.YAMLError(f"Erro ao processar YAML em {path}: {e}")

    @classmethod
    def _merge_defaults(cls, raw: Dict[str, Any]) -> Dict[str, Any]:
        """Mescla recursivamente DEFAULTS -> raw (raw sobrescreve)."""
        def merge(a: Dict[str, Any], b: Dict[str, Any]) -> Dict[str, Any]:
            out = deepcopy(a)
            for k, v in (b or {}).items():
                if isinstance(v, dict) and isinstance(out.get(k), dict):
                    out[k] = merge(out[k], v)
                else:
                    out[k] = v
            return out

        return merge(cls.DEFAULTS, raw or {})

    @classmethod
    def _expand_env_in_dict(cls, data: Dict[str, Any]) -> Dict[str, Any]:
        """Expande padrões ${VAR} em todas as strings do dicionário."""
        pattern = re.compile(r"\$\{([^}]+)\}")

        def expand(value: Any) -> Any:
            if isinstance(value, str):
                def repl(m: re.Match[str]) -> str:
                    var = m.group(1)
                    return os.environ.get(var, m.group(0))
                return pattern.sub(repl, value)
            if isinstance(value, dict):
                return {k: expand(v) for k, v in value.items()}
            if isinstance(value, list):
                return [expand(v) for v in value]
            return value

        return expand(data)  # type: ignore[return-value]


def load_cfg() -> Dict[str, Any]:
    """Compat: helper simples para carregar config com defaults.

    Preferir usar `ConfigLoader.load()`.
    """
    return ConfigLoader.load()
