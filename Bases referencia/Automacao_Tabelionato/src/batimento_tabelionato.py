"""Script de batimento entre bases Tabelionato e MAX.

Implementa regras especficas:
- Batimento por nmero do protocolo
- Priorizar CNPJ em protocolos duplicados
- Usar CPF para enriquecimento
"""

import os
from pathlib import Path
from datetime import datetime
import pandas as pd
import zipfile

from src.utils.console import format_duration, format_int, print_section, suppress_console_info
from src.utils.formatting import formatar_moeda_serie
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

# Configuracoes
DECIMAL_SEP = os.getenv('CSV_DECIMAL_SEPARATOR', ',')

class TabelionatoBatimento:
    """Processador de batimento Tabelionato x MAX."""
    
    def __init__(self):
        # Configurao bsica
        self.base_dir = Path(__file__).parent.parent
        self.data_dir = self.base_dir / 'data' / 'output'
        self.max_input_dir = self.base_dir / 'data' / 'input' / 'max'
        self.output_dir = self.base_dir / 'data' / 'output' / 'batimento'
        self.output_enriquecimento_dir = self.base_dir / 'data' / 'output' / 'enriquecimento'
        self.logs_dir = self.base_dir / 'data' / 'logs'
        
        # Configuraes
        self.encoding = 'utf-8'
        self.csv_separator = ';'
        self.timestamp_format = '%Y%m%d_%H%M%S'
        
        # Criar diretrios se no existirem
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.output_enriquecimento_dir.mkdir(parents=True, exist_ok=True)
        self.logs_dir.mkdir(parents=True, exist_ok=True)
        
        # Logger padronizado
        self.logger = get_logger("batimento")
        self.documentos_campanha78_abertos: set[str] = set()
        self.metricas_campanha78 = {"documentos_max": 0, "realocados": 0}
        self.contagem_campanhas: dict[str, int] = {}
    
    def carregar_base_tabelionato(self):
        """Carrega base Tabelionato tratada."""
        # Buscar arquivo mais recente
        tabelionato_dir = self.data_dir / 'tabelionato_tratada'
        if not tabelionato_dir.exists():
            raise FileNotFoundError(f"Diretrio no encontrado: {tabelionato_dir}")
        
        # Buscar arquivos ZIP (excluindo pasta inconsistencias)
        arquivos = [f for f in tabelionato_dir.glob('*.zip') 
                   if f.is_file() and 'tabelionato_tratado' in f.name]
        
        if not arquivos:
            raise FileNotFoundError(f"Nenhum arquivo Tabelionato encontrado em {tabelionato_dir}")
        
        arquivo_mais_recente = max(arquivos, key=lambda x: x.stat().st_mtime)
        self.logger.info(f"Carregando Tabelionato: {arquivo_mais_recente.name}")
        
        with zipfile.ZipFile(arquivo_mais_recente, 'r') as zip_file:
            csv_files = [f for f in zip_file.namelist() if f.endswith('.csv')]
            if not csv_files:
                raise ValueError("Nenhum arquivo CSV encontrado no ZIP Tabelionato")
            
            with zip_file.open(csv_files[0]) as csv_data:
                df = pd.read_csv(csv_data, encoding=self.encoding, sep=self.csv_separator, dtype=str)
        
        self.logger.info(f"Tabelionato carregado: {len(df):,} registros")
        self.logger.info(f"Colunas Tabelionato: {list(df.columns)}")
        return df
    
    def carregar_base_max(self):
        """Carrega base MAX tratada."""
        # Buscar arquivo mais recente
        max_dir = self.data_dir / 'max_tratada'
        if not max_dir.exists():
            raise FileNotFoundError(f"Diretrio no encontrado: {max_dir}")
        
        arquivos = sorted(max_dir.glob('max_tratada*.zip'))
        if not arquivos:
            raise FileNotFoundError(f"Nenhum arquivo MAX encontrado em {max_dir}")

        arquivo_mais_recente = max(arquivos, key=lambda x: x.stat().st_mtime)
        self.logger.info(f"Carregando MAX: {arquivo_mais_recente.name}")
        
        with zipfile.ZipFile(arquivo_mais_recente, 'r') as zip_file:
            csv_files = [f for f in zip_file.namelist() if f.endswith('.csv')]
            if not csv_files:
                raise ValueError("Nenhum arquivo CSV encontrado no ZIP MAX")
            
            with zip_file.open(csv_files[0]) as csv_data:
                df = pd.read_csv(csv_data, encoding=self.encoding, sep=self.csv_separator, dtype=str)
        
        self.logger.info(f"MAX carregado: {len(df):,} registros")
        return df

    def carregar_base_max_bruta(self):
        """Carrega base MAX antes do tratamento (direto do input)."""
        if not self.max_input_dir.exists():
            raise FileNotFoundError(f"Diretrio no encontrado: {self.max_input_dir}")

        arquivos = [f for f in self.max_input_dir.glob('*.zip') if f.is_file()]
        if not arquivos:
            raise FileNotFoundError(
                f"Nenhum arquivo MAX bruto encontrado em {self.max_input_dir}"
            )

        arquivo_mais_recente = max(arquivos, key=lambda x: x.stat().st_mtime)
        self.logger.info(f"Carregando MAX bruto: {arquivo_mais_recente.name}")

        with zipfile.ZipFile(arquivo_mais_recente, 'r') as zip_file:
            csv_files = [f for f in zip_file.namelist() if f.endswith('.csv')]
            if not csv_files:
                raise ValueError("Nenhum arquivo CSV encontrado no ZIP MAX bruto")

            with zip_file.open(csv_files[0]) as csv_data:
                df = pd.read_csv(
                    csv_data,
                    encoding='utf-8-sig',
                    sep=';',
                    dtype=str,
                )

        self.logger.info(
            "MAX bruto carregado: %s registros (colunas: %s)",
            f"{len(df):,}",
            list(df.columns),
        )
        return df
    

    def _aplicar_regra_duplicados_tabelionato(self, df):
        """Prioriza CNPJ em protocolos duplicados e separa demais para enriquecimento."""
        self.logger.info("Aplicando regra de priorizao por protocolo...")

        if df.empty:
            return df.copy(), pd.DataFrame()

        df_trabalho = df.copy()

        comprimento_doc = df_trabalho['CPFCNPJ_CLIENTE'].str.len()
        prioridade = pd.Series(2, index=df_trabalho.index, dtype='int64')
        prioridade = prioridade.mask(comprimento_doc == 18, 0)
        prioridade = prioridade.mask(comprimento_doc == 14, 1)
        df_trabalho['PRIORIDADE_DOCUMENTO'] = prioridade

        colunas_ordenacao = ['CHAVE', 'PRIORIDADE_DOCUMENTO']
        ordem = [True, True]
        if 'DtAnuencia' in df_trabalho.columns:
            colunas_ordenacao.append('DtAnuencia')
            ordem.append(False)

        df_ordenado = df_trabalho.sort_values(colunas_ordenacao, ascending=ordem, kind='mergesort')

        df_principal = df_ordenado.drop_duplicates(subset='CHAVE', keep='first').copy()
        df_principal = df_principal.drop(columns=['PRIORIDADE_DOCUMENTO'])

        duplicados = df_ordenado[df_ordenado.duplicated(subset='CHAVE', keep='first')].copy()
        duplicados = duplicados.drop(columns=['PRIORIDADE_DOCUMENTO'])

        if duplicados.empty:
            df_enriquecimento = pd.DataFrame()
        else:
            # Criar tabela de enriquecimento com registros duplicados
            doc_referencia = df_principal.set_index('CHAVE')['CPFCNPJ_CLIENTE']
            duplicados['PROTOCOLO_REFERENCIA'] = duplicados['CHAVE']
            duplicados['DOCUMENTO_REFERENCIA'] = duplicados['CHAVE'].map(doc_referencia)

            tamanho_doc = duplicados['CPFCNPJ_CLIENTE'].str.len()
            tipos = pd.Series('DOCUMENTO_ADICIONAL', index=duplicados.index)
            tipos = tipos.mask(tamanho_doc == 18, 'CNPJ_DUPLICADO')
            tipos = tipos.mask(tamanho_doc == 14, 'CPF_ADICIONAL')
            duplicados['TIPO_ENRIQUECIMENTO'] = tipos
            df_enriquecimento = duplicados

        self.logger.info(
            "Prioridade aplicada: %s protocolos principais, %s registros enviados para enriquecimento",
            f"{len(df_principal):,}",
            f"{len(df_enriquecimento):,}",
        )

        return df_principal, df_enriquecimento

    @staticmethod
    def _normalizar_documentos(serie: pd.Series) -> pd.Series:
        """Remove mascara e mantm apenas dgitos."""
        if serie is None:
            return pd.Series(dtype='string')
        return (
            serie.astype(str)
            .str.strip()
            .str.replace(r'[^0-9]', '', regex=True)
            .replace('', pd.NA)
        )

    def _obter_documentos_campanha78_max(self, df_max: pd.DataFrame) -> set[str]:
        """Retorna conjunto de CPFs/CNPJs da campanha 78 com status em aberto."""
        colunas_necessarias = {'CPFCNPJ_CLIENTE', 'CAMPANHA', 'STATUS_TITULO'}
        if not colunas_necessarias.issubset(df_max.columns):
            faltantes = colunas_necessarias - set(df_max.columns)
            self.logger.warning(
                "No foi possvel calcular campanha 78: colunas faltantes na base MAX: %s",
                ', '.join(sorted(faltantes)),
            )
            return set()

        serie_doc = self._normalizar_documentos(df_max['CPFCNPJ_CLIENTE'])
        mask_doc = serie_doc.str.len().isin({11, 14}).fillna(False)

        campanha_bruta = df_max['CAMPANHA'].astype(str).str.strip()
        mask_campanha = campanha_bruta.str.contains('78', regex=False).fillna(False)

        status_normalizado = (
            df_max['STATUS_TITULO']
            .astype(str)
            .str.strip()
            .str.lower()
        )
        status_validos = {'aberto', 'em aberto', 'a', '0'}
        mask_status = status_normalizado.isin(status_validos)

        mask_final = mask_doc & mask_campanha & mask_status

        documentos_validos = set(serie_doc[mask_final].dropna().tolist())
        if documentos_validos:
            self.logger.info(
                "Campanha 78 (MAX) - %s documentos em aberto identificados",
                format_int(len(documentos_validos)),
            )
        else:
            self.logger.info("Campanha 78 (MAX) - nenhum documento em aberto identificado")

        return documentos_validos

    def _redistribuir_para_campanha78(self, df_pendentes: pd.DataFrame) -> tuple[pd.DataFrame, int]:
        """Realoca pendncias cujo CPF/CNPJ est em aberto na campanha 78 da MAX."""
        if df_pendentes.empty or not self.documentos_campanha78_abertos:
            return df_pendentes, 0

        if 'CpfCnpj' not in df_pendentes.columns:
            self.logger.warning(
                "Coluna 'CpfCnpj' ausente em pendncias - regra da campanha 78 no aplicada."
            )
            return df_pendentes, 0

        serie_documento = self._normalizar_documentos(df_pendentes['CpfCnpj'])
        mask_documento = serie_documento.str.len().isin({11, 14}).fillna(False)
        mask_destino = mask_documento & serie_documento.isin(self.documentos_campanha78_abertos)
        quantidade = int(mask_destino.sum())

        if not quantidade:
            return df_pendentes, 0

        df_resultado = df_pendentes.copy()
        df_resultado.loc[mask_destino, 'Campanha'] = 'Campanha 78'
        self.logger.info(
            "Regra campanha 78 aplicada: %s registros movidos para a nova planilha",
            format_int(quantidade),
        )
        return df_resultado, quantidade
    

    def realizar_cruzamento(self, df_tabelionato: pd.DataFrame, df_max: pd.DataFrame) -> pd.DataFrame:
        """Identifica parcelas no Tabelionato que no esto no MAX (left anti-join)."""
        self.logger.info("PROCV TABELIONATOMAX: iniciando identificao...")

        # Verificar colunas obrigatrias
        if 'CHAVE' not in df_tabelionato.columns:
            raise ValueError(
                "Coluna CHAVE ausente na base Tabelionato tratada para cruzamento com MAX"
            )
        if 'CHAVE' not in df_max.columns:
            raise ValueError(
                "Coluna CHAVE ausente na base MAX tratada para cruzamento com Tabelionato"
            )

        # Implementar anti-join isolado para Tabelionato
        def _procv_tabelionato_menos_max(df_tabelionato, df_max):
            """Anti-join isolado: retorna registros do Tabelionato no presentes no MAX."""
            max_keys = set(df_max['CHAVE'].astype(str).str.strip().dropna())
            mask = ~df_tabelionato['CHAVE'].astype(str).str.strip().isin(max_keys)
            return df_tabelionato.loc[mask].copy()
        
        df_nao_encontradas = _procv_tabelionato_menos_max(df_tabelionato, df_max)
        
        # Registrar informao sobre duplicados APENAS nos registros pendentes
        if not df_nao_encontradas.empty:
            duplicados_count = df_nao_encontradas['CHAVE'].duplicated().sum()
            if duplicados_count > 0:
                self.logger.info(
                    "Registros duplicados nos protocolos pendentes: %s (sero tratados na regra de priorizao)",
                    f"{duplicados_count:,}"
                )

        self.metrics_ultima_execucao = {
            'registros_tabelionato': len(df_tabelionato),
            'registros_max': len(df_max),
            'registros_batimento': len(df_nao_encontradas),
        }

        self.logger.info(
            "PROCV TABELIONATOMAX: %s registros",
            f"{len(df_nao_encontradas):,}",
        )
        return df_nao_encontradas

    def _formatar_layout_saida(self, df: pd.DataFrame) -> pd.DataFrame:
        """Mapeia colunas para o layout final esperado pelo sistema de importação."""

        if df.empty:
            return df.copy()

        df_trabalho = df.copy()

        colunas_obrigatorias = {
            'Protocolo': 'NUMERO CONTRATO',
            'Devedor': 'NOME / RAZAO SOCIAL',
            'DtAnuencia': 'VENCIMENTO',
            'CpfCnpj': 'CPFCNPJ CLIENTE',
            'Custas': 'VALOR',
            'Credor': 'OBSERVACAO CONTRATO',
        }

        ausentes = [col for col in colunas_obrigatorias if col not in df_trabalho.columns]
        if ausentes:
            colunas_disponiveis = ", ".join(df_trabalho.columns)
            raise KeyError(
                "Coluna obrigatória ausente no batimento. "
                f"Faltando: {ausentes}. Disponíveis: [{colunas_disponiveis}]"
            )

        # Criar DataFrame de saída com ordem específica das colunas
        df_saida = pd.DataFrame(index=df_trabalho.index)
        
        # Ordem solicitada das colunas:
        # 1. CPFCNPJ CLIENTE
        df_saida['CPFCNPJ CLIENTE'] = df_trabalho['CpfCnpj'].copy()
        
        # 2. NOME / RAZAO SOCIAL  
        df_saida['NOME / RAZAO SOCIAL'] = df_trabalho['Devedor'].copy()
        
        # 3. VALOR
        df_saida['VALOR'] = formatar_moeda_serie(
            df_trabalho['Custas'], decimal_separator=DECIMAL_SEP
        )
        
        # 4. ID NEGOCIADOR (nova coluna em branco)
        df_saida['ID NEGOCIADOR'] = pd.Series('', index=df_trabalho.index, dtype='string')
        
        # 5. CNPJ CREDOR (valor fixo)
        df_saida['CNPJ CREDOR'] = pd.Series('16.746.133/0001-41', index=df_trabalho.index, dtype='string')
        
        # 6. PARCELA
        df_saida['PARCELA'] = df_trabalho['Protocolo'].copy()
        
        # 7. VENCIMENTO
        df_saida['VENCIMENTO'] = df_trabalho['DtAnuencia'].copy()
        
        # 8. OBSERVACAO CONTRATO
        df_saida['OBSERVACAO CONTRATO'] = df_trabalho['Credor'].copy()
        
        # 9. NUMERO CONTRATO
        df_saida['NUMERO CONTRATO'] = df_trabalho['Protocolo'].copy()

        # Adicionar Campanha no início se existir
        if 'Campanha' in df_trabalho.columns:
            df_saida.insert(0, 'Campanha', df_trabalho['Campanha'].copy())
        else:
            df_saida.insert(0, 'Campanha', pd.Series(pd.NA, index=df_trabalho.index, dtype='string'))

        return df_saida

    def gerar_relatorios(self, df_pendentes, df_enriquecimento):
        """Exporta as trs planilhas previstas no escopo."""
        self.logger.info("Gerando planilhas finais...")

        for arquivo in self.output_dir.glob('batimento_campanha14*.csv'):
            arquivo.unlink(missing_ok=True)
        for arquivo in self.output_dir.glob('batimento_campanha58*.csv'):
            arquivo.unlink(missing_ok=True)
        for arquivo in self.output_dir.glob('batimento_campanha78*.csv'):
            arquivo.unlink(missing_ok=True)
        for arquivo in self.output_dir.glob('batimento_campanha94*.csv'):
            arquivo.unlink(missing_ok=True)
        for arquivo in self.output_enriquecimento_dir.glob('tabela_enriquecimento*.csv'):
            arquivo.unlink(missing_ok=True)

        arquivos_gerados = {}
        contagens = {}

        if not df_pendentes.empty:
            mapa_campanhas = {
                'Campanha 58': 'batimento_campanha58',
                'Campanha 78': 'batimento_campanha78',
                'Campanha 94': 'batimento_campanha94',
            }

            for campanha, nome_base in mapa_campanhas.items():
                df_campanha = df_pendentes[df_pendentes['Campanha'] == campanha].copy()
                if df_campanha.empty:
                    continue
                contagens[campanha] = len(df_campanha)

                # Criar CSV temporário
                csv_temp = self.output_dir / f"{nome_base}.csv"
                df_saida = df_campanha.drop(columns=['Campanha'], errors='ignore')
                df_saida.to_csv(csv_temp, sep=self.csv_separator, encoding=self.encoding, index=False)
                
                # Criar ZIP
                zip_path = self.output_dir / f"{nome_base}.zip"
                with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zip_ref:
                    zip_ref.write(csv_temp, csv_temp.name)
                
                # Remover CSV temporário
                csv_temp.unlink()
                
                arquivos_gerados[campanha] = zip_path
            if contagens:
                for camp, qtd in contagens.items():
                    self.logger.info("%s: %s registros exportados", camp, format_int(qtd))
            self.logger.info(
                "Arquivos de batimento gerados para campanhas 58, 78 e 94.")

        if df_enriquecimento is not None and not df_enriquecimento.empty:
            # Criar CSV temporrio (pasta propria de enriquecimento)
            csv_temp = self.output_enriquecimento_dir / 'tabela_enriquecimento.csv'
            df_enriquecimento_export = df_enriquecimento.copy()
            if 'Custas' in df_enriquecimento_export.columns:
                df_enriquecimento_export['Custas'] = formatar_moeda_serie(
                    df_enriquecimento_export['Custas'], decimal_separator=DECIMAL_SEP
                )
            df_enriquecimento_export.to_csv(csv_temp, sep=self.csv_separator, encoding=self.encoding, index=False)
            
            # Criar ZIP
            zip_path = self.output_enriquecimento_dir / 'tabela_enriquecimento.zip'
            with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zip_ref:
                zip_ref.write(csv_temp, csv_temp.name)
            
            # Remover CSV temporrio
            csv_temp.unlink()
            
            arquivos_gerados['enriquecimento'] = zip_path
            self.logger.info(
                "Tabela de enriquecimento gerada com %s registros",
                f"{len(df_enriquecimento):,}",
            )

        self.contagem_campanhas = contagens
        return arquivos_gerados

    def processar(self):
        """Executa processo completo de batimento."""
        inicio = datetime.now()
        log_session_start("Batimento Tabelionato x MAX")
        sucesso = False

        try:
            # Carregar bases
            df_tabelionato = self.carregar_base_tabelionato()
            df_max_bruto = self.carregar_base_max_bruta()
            df_max = self.carregar_base_max()
            self.documentos_campanha78_abertos = self._obter_documentos_campanha78_max(df_max_bruto)
            self.metricas_campanha78['documentos_max'] = len(self.documentos_campanha78_abertos)
            
            # Verificar colunas obrigatrias
            if 'CHAVE' not in df_tabelionato.columns:
                raise ValueError("A base tratada do Tabelionato deve conter a coluna 'CHAVE'.")
            
            if 'CPFCNPJ_CLIENTE' not in df_tabelionato.columns:
                raise ValueError("A base tratada do Tabelionato deve conter a coluna 'CPFCNPJ_CLIENTE'.")
            
            if 'Campanha' not in df_tabelionato.columns:
                self.logger.warning("Coluna 'Campanha' ausente; os relatrios sero gerados sem essa segmentao.")
                df_tabelionato['Campanha'] = pd.NA

            # Realizar cruzamento (LEFT ANTI-JOIN) - protocolos do Tabelionato ausentes no MAX
            df_pendentes = self.realizar_cruzamento(df_tabelionato, df_max)
            df_pendentes, realocados_c78 = self._redistribuir_para_campanha78(df_pendentes)
            self.metricas_campanha78['realocados'] = realocados_c78

            # Aplicar regra de duplicados APENAS nos registros pendentes
            df_pendentes_principal, df_enriquecimento = self._aplicar_regra_duplicados_tabelionato(df_pendentes)

            validacao_origem = localizar_chaves_ausentes(
                df_pendentes_principal,
                df_tabelionato,
            )
            if getattr(validacao_origem, "possui_inconsistencias", False):
                log_validation_presence(
                    "Batimento - presenca no Tabelionato",
                    validacao_origem.total_verificado,
                    validacao_origem.amostras_inconsistentes,
                )
                resumo = resumir_amostras(validacao_origem.amostras_inconsistentes)
                raise ValueError(
                    "Protocolos pendentes no foram localizados na base Tabelionato tratada: "
                    f"{resumo}"
                )
            else:
                log_validation_presence(
                    "Batimento - presenca no Tabelionato",
                    validacao_origem.total_verificado,
                    [],
                )

            df_pendentes_formatado = self._formatar_layout_saida(df_pendentes_principal)

            arquivos = self.gerar_relatorios(df_pendentes_formatado, df_enriquecimento)

            validacao_pendentes = localizar_chaves_presentes(
                df_pendentes_principal,
                df_max,
            )
            if getattr(validacao_pendentes, "possui_inconsistencias", False):
                log_validation_result(
                    "Batimento - protocolos pendentes x MAX",
                    validacao_pendentes.total_verificado,
                    validacao_pendentes.amostras_inconsistentes,
                )
                resumo = resumir_amostras(validacao_pendentes.amostras_inconsistentes)
                raise ValueError(
                    "Foram encontrados protocolos pendentes presentes na base MAX: "
                    f"{resumo}"
                )
            else:
                log_validation_result(
                    "Batimento - protocolos pendentes x MAX",
                    validacao_pendentes.total_verificado,
                    [],
                )

            if df_enriquecimento is not None and not df_enriquecimento.empty:
                validacao_enriq = localizar_chaves_presentes(df_enriquecimento, df_max)
                if getattr(validacao_enriq, "possui_inconsistencias", False):
                    log_validation_result(
                        "Batimento - enriquecimento x MAX",
                        validacao_enriq.total_verificado,
                        validacao_enriq.amostras_inconsistentes,
                    )
                    resumo = resumir_amostras(validacao_enriq.amostras_inconsistentes)
                    raise ValueError(
                        "Registros de enriquecimento ainda esto presentes na base MAX: "
                        f"{resumo}"
                    )
                else:
                    log_validation_result(
                        "Batimento - enriquecimento x MAX",
                        validacao_enriq.total_verificado,
                        [],
                    )

            # Estatsticas finais
            duracao = (datetime.now() - inicio).total_seconds()

            pendencias_total = len(df_pendentes)
            pendencias_principais = len(df_pendentes_principal)
            enriquecimento_total = len(df_enriquecimento) if df_enriquecimento is not None else 0

            arquivos_gerados = [
                f"  - {tipo.title()}: {arquivo.name}"
                for tipo, arquivo in arquivos.items()
                if arquivo
            ]

            linhas = [
                "[STEP] Batimento Tabelionato x MAX",
                "",
                f"Tabelionato tratado: {format_int(len(df_tabelionato))}",
                f"MAX tratado: {format_int(len(df_max))}",
                f"Pendencias identificadas: {format_int(pendencias_total)}",
                f"Pendencias principais: {format_int(pendencias_principais)}",
                f"Registros de enriquecimento: {format_int(enriquecimento_total)}",
                f"Filtro (MAX com titulos - Em aberto - Campanha 78): {format_int(self.metricas_campanha78['documentos_max'])}",
                f"Campanha 78 - registros realocados: {format_int(self.metricas_campanha78['realocados'])}",
            ]

            if arquivos_gerados:
                linhas.append("")
                linhas.append("Arquivos gerados:")
                linhas.extend(arquivos_gerados)

            if self.contagem_campanhas:
                linhas.append("")
                linhas.append("Distribuio por campanha:")
                for camp, qtd in self.contagem_campanhas.items():
                    linhas.append(f"{camp}: {format_int(qtd)} registros exportados")

            linhas.append("")
            linhas.append(f"Duracao: {format_duration(duracao)}")

            print_section("BATIMENTO - TABELIONATO x MAX", linhas)

            metricas = {
                "Tabelionato tratado": f"{len(df_tabelionato):,}",
                "MAX tratado": f"{len(df_max):,}",
                "Pendencias identificadas": f"{len(df_pendentes):,}",
                "Pendencias principais": f"{len(df_pendentes_principal):,}",
                "Enriquecimento": f"{len(df_enriquecimento):,}",
                "Filtro (MAX com titulos - Em aberto - Campanha 78)": f"{self.metricas_campanha78['documentos_max']:,}",
                "Campanha 78 realocados": f"{self.metricas_campanha78['realocados']:,}",
            }
            for camp, qtd in self.contagem_campanhas.items():
                metricas[f"{camp} exportada"] = f"{qtd:,}"

            log_metrics("Batimento Tabelionato x MAX", metricas)

            sucesso = True

            return df_pendentes_principal

        except Exception:
            self.logger.exception("Erro no batimento")
            raise
        finally:
            log_session_end("Batimento Tabelionato x MAX", success=sucesso)


def main():
    """Funo principal."""

    processor = TabelionatoBatimento()
    processor.processar()


if __name__ == '__main__':
    main()
