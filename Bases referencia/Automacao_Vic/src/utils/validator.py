"""Validadores consolidados para MAX e VIC com regras configuráveis.

Consolida funcionalidades de:
- validador_vic.py (ValidadorVicSimplificado)
- validador_consistencia.py (ValidadorConsistencia) 
- inconsistencias.py (InconsistenciaManager)
"""

import re
import logging
from typing import Dict, Any, Tuple, List, Optional, Set
from collections import Counter
from pathlib import Path
import numpy as np

import pandas as pd


class MaxValidator:
    """Validador para dados MAX (CHAVE/PARCELA)."""

    def __init__(self, config: Dict[str, Any], logger: logging.Logger):
        self.config = config
        self.logger = logger

        validation_config = config.get('validation', {})
        self.regex_chave = validation_config.get(
            'chave_regex', r'^[A-Za-z0-9]+(?:-[A-Za-z0-9]+)+$'
        )

    def validar_dados(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
        df = df.copy()
        df['motivo_inconsistencia'] = ''

        coluna = 'CHAVE' if 'CHAVE' in df.columns else 'PARCELA'
        if coluna in df.columns:
            chaves = df[coluna].astype(str).str.strip()
            mask_vazia = chaves.eq('') | df[coluna].isna()
            df.loc[mask_vazia, 'motivo_inconsistencia'] += 'CHAVE_VAZIA;'

            mask_invalida = ~chaves.str.match(self.regex_chave, na=False)
            df.loc[mask_invalida & ~mask_vazia, 'motivo_inconsistencia'] += 'CHAVE_FORMATO_INVALIDO;'

        dados_invalidos = df[df['motivo_inconsistencia'] != '']
        dados_validos = df[df['motivo_inconsistencia'] == ''].drop('motivo_inconsistencia', axis=1)
        return dados_validos, dados_invalidos

    def validar_amostra(self, df: pd.DataFrame, n_amostras: int = 10) -> Dict[str, Any]:
        if len(df) == 0:
            return {'total': 0, 'amostras': []}

        amostra = df.sample(min(n_amostras, len(df)))

        amostras_info = []
        for idx, row in amostra.iterrows():
            info = {
                'index': idx,
                'CHAVE': str(row.get('CHAVE', 'N/A')),
                'VENCIMENTO': str(row.get('VENCIMENTO', 'N/A'))
            }

            if 'CHAVE' in row and pd.notna(row['CHAVE']):
                chave_str = str(row['CHAVE']).strip()
                info['chave_match_regex'] = bool(re.match(self.regex_chave, chave_str))
            else:
                info['chave_match_regex'] = False

            amostras_info.append(info)

        return {
            'total': len(df),
            'amostras': amostras_info,
            'regex_chave': self.regex_chave,
        }


class VicValidator:
    """Validador consolidado para dados VIC (CPF/CNPJ e VENCIMENTO).
    
    Consolida funcionalidades do ValidadorVicSimplificado.
    """

    def __init__(self, config: Dict[str, Any], logger: logging.Logger):
        self.config = config
        self.logger = logger

    def validar_dados(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Valida dados VIC verificando CPF/CNPJ e VENCIMENTO.
        
        Args:
            df: DataFrame com dados VIC
            
        Returns:
            Tuple[pd.DataFrame, pd.DataFrame]: (dados_validos, dados_invalidos)
        """
        df = df.copy()
        df['motivo_inconsistencia'] = ''

        # Validar VENCIMENTO
        if 'VENCIMENTO' in df.columns:
            mask_venc_inval = df['VENCIMENTO'].isna()
            df.loc[mask_venc_inval, 'motivo_inconsistencia'] += 'VENCIMENTO_INVALIDO;'

        # Validar CPF/CNPJ (principal validação VIC)
        if 'CPFCNPJ_CLIENTE' in df.columns:
            docs = df['CPFCNPJ_CLIENTE'].astype(str).str.strip()
            mask_doc_vazio = (
                docs.eq('') | docs.eq('nan') | docs.eq('none') |
                docs.str.lower().eq('nan') | docs.str.lower().eq('none') |
                df['CPFCNPJ_CLIENTE'].isna()
            )
            df.loc[mask_doc_vazio, 'motivo_inconsistencia'] += 'CPF/CNPJ nulo ou vazio;'
        else:
            self.logger.warning("Coluna CPFCNPJ_CLIENTE não encontrada no DataFrame")

        # Separar válidos e inválidos
        dados_invalidos = df[df['motivo_inconsistencia'] != '']
        dados_validos = df[df['motivo_inconsistencia'] == ''].drop(columns=['motivo_inconsistencia'])
        
        # Log das estatísticas
        total = len(df)
        validos = len(dados_validos)
        invalidos = len(dados_invalidos)
        
        self.logger.info(f"Validação VIC: {total:,} total, {validos:,} válidos, {invalidos:,} inválidos")
        
        if invalidos > 0 and not dados_invalidos.empty:
            # Contar motivos de inconsistência
            motivos = dados_invalidos['motivo_inconsistencia'].str.split(';').explode()
            motivos = motivos[motivos.str.strip() != ''].value_counts()
            for motivo, count in motivos.items():
                self.logger.info(f"  - {motivo.strip()}: {count:,} registros")
        
        return dados_validos, dados_invalidos

    def obter_estatisticas_inconsistencias(self, df_inconsistencias: pd.DataFrame) -> Dict[str, int]:
        """Retorna estatísticas detalhadas das inconsistências encontradas.
        
        Args:
            df_inconsistencias: DataFrame com registros inconsistentes
            
        Returns:
            Dict[str, int]: Dicionário com contagem de cada tipo de inconsistência
        """
        if len(df_inconsistencias) == 0:
            return {}
        
        motivos = df_inconsistencias['motivo_inconsistencia'].str.split(';').explode()
        motivos = motivos[motivos != ''].value_counts()
        return motivos.to_dict()


class InconsistenciaManager:
    """Gerenciador consolidado de inconsistências e validações.
    
    Consolida funcionalidades do módulo inconsistencias.py.
    """
    
    def __init__(self, config: Dict[str, Any]):
        """Inicializa o gerenciador de inconsistências.
        
        Args:
            config: Configuração carregada do config.yaml
        """
        self.config = config
        self.logger = logging.getLogger(__name__)
        
        # Lista para armazenar inconsistências
        self.inconsistencias: List[Dict[str, Any]] = []
        
        # Contadores
        self.contadores = {
            'total_registros': 0,
            'registros_validos': 0,
            'registros_invalidos': 0,
            'motivos': Counter()
        }
    
    def adicionar_motivo(self, indice: int, motivo: str, 
                        detalhes: Optional[str] = None, 
                        valor_original: Optional[str] = None) -> None:
        """Adiciona motivo de inconsistência.
        
        Args:
            indice: Índice do registro no DataFrame
            motivo: Motivo da inconsistência
            detalhes: Detalhes adicionais (opcional)
            valor_original: Valor original que causou a inconsistência
        """
        inconsistencia = {
            'indice': indice,
            'motivo': motivo,
            'detalhes': detalhes,
            'valor_original': valor_original
        }
        
        self.inconsistencias.append(inconsistencia)
        self.contadores['motivos'][motivo] += 1
        
        self.logger.debug(f"Inconsistência adicionada: {motivo} (índice {indice})")
    
    def adicionar_motivos_em_lote(self, indices: List[int], motivo: str, 
                                 detalhes: Optional[str] = None) -> None:
        """Adiciona mesmo motivo para múltiplos registros.
        
        Args:
            indices: Lista de índices dos registros
            motivo: Motivo da inconsistência
            detalhes: Detalhes adicionais (opcional)
        """
        for indice in indices:
            self.adicionar_motivo(indice, motivo, detalhes)
        
        self.logger.debug(f"Inconsistências em lote: {motivo} ({len(indices)} registros)")
    
    def dividir_validos_invalidos(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Divide DataFrame em registros válidos e inválidos.
        
        Args:
            df: DataFrame original
            
        Returns:
            Tupla com (DataFrame válidos, DataFrame inválidos)
        """
        if not self.inconsistencias:
            # Todos os registros são válidos
            self.contadores['total_registros'] = len(df)
            self.contadores['registros_validos'] = len(df)
            self.contadores['registros_invalidos'] = 0
            return df.copy(), pd.DataFrame()
        
        # Índices dos registros inválidos
        indices_invalidos = {inc['indice'] for inc in self.inconsistencias}
        
        # Dividir DataFrames
        df_invalidos = df.iloc[list(indices_invalidos)].copy()
        df_validos = df.drop(indices_invalidos).copy()
        
        # Atualizar contadores
        self.contadores['total_registros'] = len(df)
        self.contadores['registros_validos'] = len(df_validos)
        self.contadores['registros_invalidos'] = len(df_invalidos)
        
        self.logger.info(
            f"Divisão concluída: {len(df_validos)} válidos, "
            f"{len(df_invalidos)} inválidos de {len(df)} total"
        )
        
        return df_validos, df_invalidos
    
    def obter_estatisticas(self) -> Dict[str, Any]:
        """Retorna estatísticas das inconsistências.
        
        Returns:
            Dict com estatísticas detalhadas
        """
        return {
            'total_inconsistencias': len(self.inconsistencias),
            'contadores': dict(self.contadores),
            'motivos_detalhados': dict(self.contadores['motivos'])
        }

    def criar_dataframe_inconsistencias(self, df_invalidos: pd.DataFrame) -> pd.DataFrame:
        """Gera um DataFrame padronizado de inconsistências.

        Compatível com chamadas antigas que esperavam um DF pronto para exportação.
        Mantém todas as colunas do DF inválido e garante a presença de
        'motivo_inconsistencia' (se não existir, preenche vazio).
        """
        df_out = df_invalidos.copy()
        if 'motivo_inconsistencia' not in df_out.columns:
            df_out['motivo_inconsistencia'] = ''
        # Opcional: reordenar para dar destaque ao motivo
        cols = list(df_out.columns)
        if 'motivo_inconsistencia' in cols:
            cols = ['motivo_inconsistencia'] + [c for c in cols if c != 'motivo_inconsistencia']
            df_out = df_out[cols]
        return df_out


class ValidadorConsistencia:
    """Validador consolidado para comparação entre dados originais e refatorados.
    
    Consolida funcionalidades do validador_consistencia.py.
    """
    
    def __init__(self, config: Dict[str, Any], logger: Optional[logging.Logger] = None):
        """Inicializa o validador de consistência.
        
        Args:
            config: Configuração carregada do config.yaml
            logger: Logger opcional
        """
        self.config = config
        self.logger = logger or logging.getLogger(__name__)
        
        # Configurações de validação
        self.validacao_config = config.get('validacao_consistencia', {})
        self.tolerancia_numerica = self.validacao_config.get('tolerancia_numerica', 0.01)
        self.colunas_ignorar = set(self.validacao_config.get('colunas_ignorar', []))
        self.colunas_chave = self.validacao_config.get('colunas_chave', [])
        
        # Resultados da validação
        self.resultados = {
            'total_registros_original': 0,
            'total_registros_refatorado': 0,
            'registros_coincidentes': 0,
            'registros_apenas_original': 0,
            'registros_apenas_refatorado': 0,
            'diferencas_por_coluna': {},
            'resumo_validacao': 'PENDENTE'
        }
    
    def comparar_dataframes(self, df_original: pd.DataFrame, df_refatorado: pd.DataFrame,
                          colunas_chave: Optional[List[str]] = None) -> Dict[str, Any]:
        """Compara dois DataFrames e retorna relatório de diferenças.
        
        Args:
            df_original: DataFrame original
            df_refatorado: DataFrame refatorado
            colunas_chave: Colunas para usar como chave de comparação
            
        Returns:
            Dict com relatório de diferenças
        """
        chaves = colunas_chave or self.colunas_chave
        
        if not chaves:
            self.logger.warning("Nenhuma coluna chave definida para comparação")
            return self.resultados
        
        # Atualizar contadores básicos
        self.resultados['total_registros_original'] = len(df_original)
        self.resultados['total_registros_refatorado'] = len(df_refatorado)
        
        # Comparação simples por tamanho
        if len(df_original) == len(df_refatorado):
            self.resultados['resumo_validacao'] = 'TAMANHOS_IGUAIS'
        else:
            self.resultados['resumo_validacao'] = 'TAMANHOS_DIFERENTES'
            
        self.logger.info(
            f"Comparação: Original={len(df_original)}, "
            f"Refatorado={len(df_refatorado)}, Status={self.resultados['resumo_validacao']}"
        )
        
        return self.resultados
    
    def validar_integridade_dados(self, df: pd.DataFrame, nome_dataset: str = "Dataset") -> bool:
        """Valida integridade básica de um DataFrame.
        
        Args:
            df: DataFrame para validar
            nome_dataset: Nome do dataset para logs
            
        Returns:
            True se dados íntegros, False caso contrário
        """
        if df.empty:
            self.logger.error(f"{nome_dataset}: DataFrame vazio")
            return False
            
        # Verificar duplicatas se houver colunas chave
        if self.colunas_chave:
            colunas_existentes = [col for col in self.colunas_chave if col in df.columns]
            if colunas_existentes:
                duplicatas = df.duplicated(subset=colunas_existentes).sum()
                if duplicatas > 0:
                    self.logger.warning(f"{nome_dataset}: {duplicatas} registros duplicados encontrados")
        
        self.logger.info(f"{nome_dataset}: Validação de integridade concluída - {len(df)} registros")
        return True
