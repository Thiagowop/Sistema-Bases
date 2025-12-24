"""Processador Tabelionato - Script isolado para tratamento de dados do tabelionato.

Baseado no processador VIC do projeto principal, adaptado para o contexto isolado.
Aplica filtros por status, aging, blacklist e validaes especficas.
"""

import sys
import os
import zipfile
from datetime import datetime
from pathlib import Path
from typing import Dict, Any

import pandas as pd
from pandas.api.types import is_string_dtype

from src.utils.console import format_duration, format_int, format_percent, print_section, suppress_console_info
from src.utils.formatting import formatar_moeda_serie
from src.utils.logger_config import get_logger, log_session_start, log_session_end

# Configuracao de separador decimal para exportacao CSV
DECIMAL_SEP = os.getenv('CSV_DECIMAL_SEPARATOR', ',')

class TabelionatoProcessor:
    """Processador para dados do Tabelionato com filtros e validaes."""

    def __init__(self):
        # Logger centralizado
        self.logger = get_logger("tratamento_tabelionato")
        suppress_console_info(self.logger)

        # Configuracoes especificas do tabelionato
        self.status_em_aberto = 'EM ABERTO'
        # Regras de aging sero habilitadas em etapa futura
        self.aging_minimo = 1800
        self.data_referencia_aging = None
        self.encoding = 'utf-8'
        self.csv_separator = ';'
        self.zip_password = b"Mf4tab@"
        self.nome_arquivo_prioritario = "tabelionato"  # Prioriza arquivos com "tabelionato" no nome

        # Estatsticas da execucao
        self.stats = {}

        # Diretrios de trabalho dentro do escopo isolado do Tabelionato
        self.base_dir = Path(__file__).parent.parent
        self.data_dir = self.base_dir / 'data'
        self.output_dir = self.data_dir / 'output'
        self.output_tratada_dir = self.output_dir / 'tabelionato_tratada'
        self.output_inconsistencias_dir = self.output_dir / 'inconsistencias'

        # Garantir que a estrutura de diretrios exista antes da exportao
        self.output_tratada_dir.mkdir(parents=True, exist_ok=True)
        self.output_inconsistencias_dir.mkdir(parents=True, exist_ok=True)

    @staticmethod
    def _formatar_documento_numerico(valor: str, formato: str) -> str:
        """Formata um valor numrico conforme o formato informado."""
        try:
            return formato.format(*valor)
        except Exception:
            return valor

    def _formatar_cpf_cnpj(self, valor: Any) -> str:
        """Normaliza CPF/CNPJ removendo espaos extras e reaplicando a mascara."""
        if pd.isna(valor):
            return ''

        texto = str(valor).strip()
        if texto == '' or texto.upper() == 'NAN':
            return ''

        # Normalizar espaos mltiplos e internos antes da validao
        texto_normalizado = ' '.join(texto.split())  # Remove espaos mltiplos
        texto_normalizado = texto_normalizado.replace(' ', '')  # Remove espaos internos
        
        apenas_digitos = ''.join(ch for ch in texto_normalizado if ch.isdigit())

        if len(apenas_digitos) == 11:
            return self._formatar_documento_numerico(
                apenas_digitos,
                "{0}{1}{2}.{3}{4}{5}.{6}{7}{8}-{9}{10}",
            )

        if len(apenas_digitos) == 14:
            return self._formatar_documento_numerico(
                apenas_digitos,
                "{0}{1}.{2}{3}{4}.{5}{6}{7}/{8}{9}{10}{11}-{12}{13}",
            )

        # Caso j esteja com mascara correta aps normalizao
        if len(texto_normalizado) in (14, 18) and all(ch.isdigit() or ch in '.-/' for ch in texto_normalizado):
            return texto_normalizado

        # Retornar valor original se no conseguir formatar
        return texto_normalizado if texto_normalizado else texto

    def carregar_arquivo_zip(self, caminho_zip: Path) -> pd.DataFrame:
        """Carrega dados do arquivo informado, aceitando ZIP protegido ou CSV."""
        self.logger.info(f"Carregando arquivo: {caminho_zip}")

        if not caminho_zip.exists():
            raise FileNotFoundError(f"Arquivo no encontrado: {caminho_zip}")

        # Permitir iniciar o desenvolvimento com arquivos CSV enquanto o ZIP no est disponvel.
        if caminho_zip.suffix.lower() != '.zip':
            self.logger.info("Entrada no est compactada. Lendo diretamente como CSV.")
            df = pd.read_csv(caminho_zip, encoding=self.encoding, sep=self.csv_separator)
            self.logger.info(f"Dados carregados: {len(df):,} registros")
            return df

        with zipfile.ZipFile(caminho_zip, 'r') as zip_file:
            arquivos_txt = [f for f in zip_file.namelist() if f.lower().endswith('.txt')]
            arquivos_csv = [f for f in zip_file.namelist() if f.lower().endswith('.csv')]

            if not arquivos_txt and not arquivos_csv:
                raise ValueError(f"Nenhum arquivo TXT ou CSV encontrado no ZIP: {caminho_zip}")

            candidatos = arquivos_txt or arquivos_csv
            candidatos_ordenados = sorted(
                candidatos,
                key=lambda nome: (
                    self.nome_arquivo_prioritario not in nome.lower(),
                    nome.lower(),
                ),
            )

            arquivo_selecionado = candidatos_ordenados[0]
            self.logger.info(f"Processando arquivo compactado: {arquivo_selecionado}")

            # Alguns anexos chegam protegidos por senha.
            try:
                with zip_file.open(
                    arquivo_selecionado,
                    pwd=self.zip_password if self.zip_password else None,
                ) as arquivo:
                    df = pd.read_csv(arquivo, encoding=self.encoding, sep=self.csv_separator)
            except RuntimeError as exc:
                raise RuntimeError(
                    "No foi possvel abrir o arquivo compactado. Verifique a senha informada."
                ) from exc

        self.logger.info(f"Dados carregados: {len(df):,} registros")
        return df

    def padronizar_campos(self, df: pd.DataFrame) -> pd.DataFrame:
        """Tratamento conforme escopo: normalizar DtAnuencia, tratar CPF/CNPJ e calcular AGING."""
        df = df.copy()

        # Normalizar nomes das colunas (remover espaos extras)
        df.columns = df.columns.str.strip()

        # ETAPA 3: Normalizar DtAnuencia (remover hora se presente)
        if 'DtAnuencia' in df.columns:
            self.logger.info("Normalizando formato da coluna DtAnuencia")
            
            # Converter para datetime se ainda no estiver
            dt_anuencia = pd.to_datetime(df['DtAnuencia'], errors='coerce', dayfirst=True)
            
            # Normalizar para remover a hora (manter apenas a data)
            dt_anuencia = dt_anuencia.dt.normalize()
            df['DtAnuencia'] = dt_anuencia
            
            # Calcular AGING baseado na data normalizada
            referencia = self.data_referencia_aging
            if referencia is None:
                referencia = pd.Timestamp.now().normalize()
            else:
                referencia = pd.to_datetime(referencia, errors='coerce')

            if pd.notna(referencia):
                aging = (referencia - dt_anuencia).dt.days
                aging = aging.where(dt_anuencia.notna(), pd.NA)
                aging = aging.where(aging >= 0, 0)
                df['AGING'] = aging.astype('Int64')
            else:
                df['AGING'] = pd.Series([pd.NA] * len(df), index=df.index, dtype='Int64')
                
            self.logger.info(f"DtAnuencia normalizada e AGING calculado para {len(df):,} registros")
        else:
            self.logger.warning("Coluna DtAnuencia no encontrada - AGING no pde ser calculado")
            df['AGING'] = pd.Series([pd.NA] * len(df), index=df.index, dtype='Int64')

        # ETAPA 4.2: Tratamento do CPF/CNPJ (remover espaamentos)
        coluna_documento = next(
            (coluna for coluna in ('CPFCNPJ_CLIENTE', 'CpfCnpj', 'CPF/CNPJ') if coluna in df.columns),
            None,
        )

        if coluna_documento:
            df[coluna_documento] = df[coluna_documento].apply(self._formatar_cpf_cnpj)

            if coluna_documento != 'CPFCNPJ_CLIENTE':
                df['CPFCNPJ_CLIENTE'] = df[coluna_documento]
        else:
            self.logger.warning(
                "Coluna de CPF/CNPJ no localizada. A etapa de duplicados pode ser impactada."
            )

        if 'CPFCNPJ_CLIENTE' in df.columns:
            df['CPFCNPJ_CLIENTE'] = df['CPFCNPJ_CLIENTE'].astype(str).str.strip()

        # ETAPA 4.3: Criar chave de batimento a partir do Protocolo
        coluna_protocolo = next(
            (coluna for coluna in df.columns if coluna.strip().lower() == 'protocolo'),
            None,
        )

        if coluna_protocolo:
            self.logger.info(f"Criando coluna CHAVE a partir da coluna '{coluna_protocolo}'")
            df[coluna_protocolo] = df[coluna_protocolo].astype(str).str.strip()
            df['CHAVE'] = df[coluna_protocolo]
            self.logger.info(f"Coluna CHAVE criada com {len(df):,} registros")
        elif 'CHAVE' not in df.columns:
            self.logger.warning(
                "Coluna 'Protocolo' no localizada. Criando chave vazia para evitar falhas no batimento."
            )
            df['CHAVE'] = ''

        # ETAPA 5: Classificao da campanha por aging e regra de protocolo misto
        df = self._atribuir_campanha(df)

        return df

    def _atribuir_campanha(self, df: pd.DataFrame) -> pd.DataFrame:
        """Define a campanha conforme aging e regra especial para protocolos com aging misto."""

        if df.empty:
            df['Campanha'] = pd.Series(dtype='string')
            return df

        if 'AGING' not in df.columns:
            df['Campanha'] = pd.Series(pd.NA, index=df.index, dtype='string')
            self.logger.warning("Coluna AGING ausente - Campanha no pde ser atribuda")
            return df

        aging = df['AGING']
        campanha = pd.Series(pd.NA, index=df.index, dtype='string')

        # Aplicar regras bsicas de campanha por aging
        aging_valido = aging.notna()
        
        # Campanha 58: aging <= 1800 dias
        regra_camp58 = aging_valido & (aging <= 1800)
        campanha.loc[regra_camp58] = 'Campanha 58'
        
        # Campanha 94: aging > 1800 dias
        regra_camp94 = aging_valido & (aging > 1800)
        campanha.loc[regra_camp94] = 'Campanha 94'

        # Regra especial: Protocolos com aging misto (tanto <= 1800 quanto > 1800) ficam na Campanha 58
        if 'CHAVE' in df.columns:
            protocolos = df['CHAVE'].fillna('').astype(str).str.strip()
            
            # Identificar protocolos que possuem registros em ambas as faixas de aging
            aging_menor = (aging <= 1800).fillna(False)
            aging_maior = (aging > 1800).fillna(False)

            possui_menor = aging_menor.groupby(protocolos, sort=False).transform('any')
            possui_maior = aging_maior.groupby(protocolos, sort=False).transform('any')

            # Protocolos com aging misto: possuem registros tanto <= 1800 quanto > 1800
            mistura = possui_menor & possui_maior

            if mistura.any():
                registros_afetados = mistura.sum()
                protocolos_afetados = protocolos[mistura].nunique()
                self.logger.info(
                    "Regra de campanha mista aplicada a %s registros (%s protocolos unicos)",
                    f"{registros_afetados:,}",
                    f"{protocolos_afetados:,}",
                )
                # Todos os registros de protocolos com aging misto vao para Campanha 58
                campanha.loc[mistura] = 'Campanha 58'

        df['Campanha'] = campanha

        # Log das estatsticas finais
        if not campanha.isna().all():
            stats_campanha = campanha.value_counts()
            self.logger.info("Distribuio de campanhas:")
            for camp, qtd in stats_campanha.items():
                self.logger.info(f"  {camp}: {qtd:,} registros")

        return df

    def _exportar_inconsistencias(self, df_invalido: pd.DataFrame, caminho_saida: Path) -> str:
        """Exporta as inconsistencias encontradas para analise manual."""
        if df_invalido.empty:
            return ""

        caminho_saida = Path(caminho_saida)
        caminho_saida.mkdir(parents=True, exist_ok=True)

        # Remover arquivos anteriores
        for arquivo_anterior in caminho_saida.glob('tabelionato_inconsistencias*.zip'):
            arquivo_anterior.unlink(missing_ok=True)
            self.logger.info(f"Arquivo anterior de inconsistencias removido: {arquivo_anterior.name}")

        csv_temp = caminho_saida / "tabelionato_inconsistencias.csv"
        df_export = df_invalido.copy()
        if 'Custas' in df_export.columns:
            df_export['Custas'] = formatar_moeda_serie(
                df_export['Custas'], decimal_separator=DECIMAL_SEP
            )
        df_export.to_csv(csv_temp, index=False, encoding=self.encoding, sep=self.csv_separator)

        arquivo_zip = caminho_saida / "tabelionato_inconsistencias.zip"
        # Criar arquivo ZIP
        with zipfile.ZipFile(arquivo_zip, 'w', zipfile.ZIP_DEFLATED) as zip_file:
            zip_file.write(csv_temp, csv_temp.name)
        
        # Remover arquivo CSV temporrio
        csv_temp.unlink()
        
        self.logger.info("Inconsistencias exportadas: %s", arquivo_zip)
        return str(arquivo_zip)

    def validar_dados(self, df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
        """Valida dados conforme escopo: apenas DtAnuencia invlida."""
        if df.empty:
            return df.copy(), pd.DataFrame()

        inconsistentes = set()
        motivos_inconsistencia = {}  # Dicionrio para armazenar os motivos

        # INCONSISTNCIA: DtAnuencia com formato incorreto, fora do padro de data, vazio, nulo ou nat
        if 'DtAnuencia' in df.columns:
            dt_anuencia_valores = df['DtAnuencia']

            # Verificar valores nulos, vazios ou invlidos ANTES da converso
            mask_vazio = (
                dt_anuencia_valores.isna() | 
                (dt_anuencia_valores.astype(str).str.strip() == '') |
                (dt_anuencia_valores.astype(str).str.strip().str.lower() == 'nan') |
                (dt_anuencia_valores.astype(str).str.strip().str.lower() == 'nat') |
                (dt_anuencia_valores.astype(str).str.strip().str.lower() == 'none')
            )

            # Tentar converter para datetime para casos que passaram no filtro anterior
            dt_anuencia_convertida = pd.to_datetime(dt_anuencia_valores, errors='coerce')
            mask_conversao_falhou = dt_anuencia_convertida.isna()

            # Datas anteriores a 1900 so consideradas inconsistentes
            referencia_minima = pd.Timestamp('1900-01-01')
            mask_data_antiga = dt_anuencia_convertida < referencia_minima

            # Combinar todas as mascaras de inconsistncia
            mask_dt_invalida = mask_vazio | mask_conversao_falhou | mask_data_antiga
            
            # Registrar motivos especficos para DtAnuencia
            for idx in df.index[mask_dt_invalida]:
                motivos = []
                if mask_vazio.loc[idx]:
                    motivos.append("DtAnuencia vazia/nula")
                elif mask_conversao_falhou.loc[idx]:
                    motivos.append("DtAnuencia com formato invlido")
                elif mask_data_antiga.loc[idx]:
                    motivos.append("DtAnuencia anterior a 1900")
                motivos_inconsistencia[idx] = "; ".join(motivos)
            
            if mask_data_antiga.any():
                total_antigas = mask_data_antiga.sum()
                self.logger.warning(
                    "Registros com DtAnuencia anterior a 1900 detectados: %s",
                    total_antigas,
                )
            inconsistentes.update(df.index[mask_dt_invalida].tolist())


        # INCONSISTNCIA: Textos com quebras de linha internas indicam registros quebrados
        colunas_textuais = [
            coluna
            for coluna in df.columns
            if df[coluna].dtype == object or is_string_dtype(df[coluna])
        ]
        for coluna in colunas_textuais:
            serie = df[coluna]
            if serie.empty:
                continue

            mask_quebra_linha = serie.astype(str).str.contains(r'[\r\n]', regex=True, na=False)
            if mask_quebra_linha.any():
                total_quebra = mask_quebra_linha.sum()
                self.logger.warning(
                    "Quebras de linha internas detectadas na coluna '%s': %s registros", 
                    coluna,
                    total_quebra,
                )
                # Registrar motivos para quebras de linha
                for idx in df.index[mask_quebra_linha]:
                    motivo_atual = motivos_inconsistencia.get(idx, "")
                    novo_motivo = f"Quebra de linha na coluna '{coluna}'"
                    if motivo_atual:
                        motivos_inconsistencia[idx] = f"{motivo_atual}; {novo_motivo}"
                    else:
                        motivos_inconsistencia[idx] = novo_motivo
                inconsistentes.update(df.index[mask_quebra_linha].tolist())

        # INCONSISTNCIA: qualquer campo textual contendo quebras de linha embutidas
        colunas_textuais = df.select_dtypes(include=['object', 'string']).columns.tolist()
        if colunas_textuais:
            possui_quebra_linha = (
                df[colunas_textuais]
                .apply(lambda coluna: coluna.fillna('').astype(str).str.contains(r'[\r\n]', regex=True))
                .any(axis=1)
            )

            if possui_quebra_linha.any():
                indices_quebra_linha = df.index[possui_quebra_linha].tolist()
                # Registrar motivos para quebras de linha gerais (se ainda no registrado)
                for idx in indices_quebra_linha:
                    if idx not in motivos_inconsistencia:
                        motivos_inconsistencia[idx] = "Quebras de linha em campos textuais"
                inconsistentes.update(indices_quebra_linha)
                self.logger.warning(
                    "Identificados %s registros com quebras de linha em campos textuais", len(indices_quebra_linha)
                )

        if not inconsistentes:
            self.logger.info("Validacao concluda: nenhum registro inconsistente identificado")
            return df.copy(), pd.DataFrame(columns=df.columns)

        inconsistencias_ordenadas = sorted(inconsistentes)
        df_invalido = df.loc[inconsistencias_ordenadas].copy()
        
        # Adicionar coluna de motivo
        df_invalido['Motivo'] = [motivos_inconsistencia.get(idx, "Motivo no especificado") for idx in inconsistencias_ordenadas]
        
        df_valido = df.drop(inconsistencias_ordenadas).copy()

        self.logger.info(
            "Validacao concluda: %s vlidos, %s inconsistentes",
            f"{len(df_valido):,}",
            f"{len(df_invalido):,}",
        )

        return df_valido, df_invalido

    def exportar_resultados(self, df: pd.DataFrame, caminho_saida: Path | None = None) -> str:
        """Exporta os dados tratados para arquivo ZIP."""
        if df.empty:
            self.logger.warning("DataFrame vazio, no ser exportado")
            return ""

        # Definir diretrio de sada
        output_dir = Path(caminho_saida) if caminho_saida else self.output_tratada_dir
        output_dir.mkdir(parents=True, exist_ok=True)

        # Remover arquivos anteriores
        for arquivo_anterior in output_dir.glob('tabelionato_tratado*.zip'):
            arquivo_anterior.unlink(missing_ok=True)
            self.logger.info(f"Arquivo anterior removido: {arquivo_anterior.name}")
        
        # Criar arquivo ZIP
        nome_arquivo = "tabelionato_tratado.zip"
        arquivo_zip = output_dir / nome_arquivo
        
        # Preparar dados para exportao
        df_export = df.copy()
        if 'DtAnuencia' in df_export.columns:
            dt_anuencia = pd.to_datetime(df_export['DtAnuencia'], errors='coerce')
            df_export['DtAnuencia'] = dt_anuencia.dt.strftime('%d/%m/%Y')
        if 'Custas' in df_export.columns:
            df_export['Custas'] = formatar_moeda_serie(
                df_export['Custas'], decimal_separator=DECIMAL_SEP
            )

        # Salvar CSV temporrio
        csv_temp = output_dir / "tabelionato_tratado.csv"
        df_export.to_csv(csv_temp, index=False, encoding=self.encoding, sep=self.csv_separator)
        
        # Criar arquivo ZIP
        with zipfile.ZipFile(arquivo_zip, 'w', zipfile.ZIP_DEFLATED) as zip_file:
            zip_file.write(csv_temp, csv_temp.name)
        
        # Remover arquivo CSV temporrio
        csv_temp.unlink()
        
        self.logger.info(f"Arquivo exportado: {arquivo_zip}")
        return str(arquivo_zip)

    def processar(self, entrada: Path, saida: Path) -> Dict[str, Any]:
        """Executa o processamento completo dos dados do Tabelionato."""
        inicio = datetime.now()
        
        log_session_start("TRATAMENTO TABELIONATO")
        
        # Carregar dados
        df_original = self.carregar_arquivo_zip(entrada)
        registros_originais = len(df_original)
        
        # Padronizar campos
        df_padronizado = self.padronizar_campos(df_original)
        
        # Validar dados
        df_valido, df_invalido = self.validar_dados(df_padronizado)
        inconsistencias_iniciais = len(df_invalido)
        
        # Exportar inconsistencias se houver
        arquivo_inconsistencias = ""
        if not df_invalido.empty:
            destino_inconsistencias = self.output_inconsistencias_dir
            arquivo_inconsistencias = self._exportar_inconsistencias(df_invalido, destino_inconsistencias)
        
        # Exportar dados tratados
        destino_tratado = saida if saida else self.output_tratada_dir
        arquivo_gerado = self.exportar_resultados(df_valido, destino_tratado)
        
        # Calcular estatsticas
        duracao = (datetime.now() - inicio).total_seconds()
        taxa_aproveitamento = (len(df_valido) / registros_originais * 100) if registros_originais > 0 else 0.0
        
        self.stats = {
            'registros_originais': registros_originais,
            'inconsistencias_iniciais': inconsistencias_iniciais,
            'registros_finais': len(df_valido),
            'taxa_aproveitamento': taxa_aproveitamento,
            'arquivo_gerado': arquivo_gerado,
            'duracao': duracao,
        }
        
        if arquivo_inconsistencias:
            self.stats['arquivo_inconsistencias'] = arquivo_inconsistencias
        
        self._exibir_resumo()
        return self.stats

    def _exibir_resumo(self):
        """Exibe resumo das estatsticas do processamento."""
        linhas = [
            "[STEP] Tratamento Tabelionato",
            "",
            f"Registros originais: {format_int(self.stats['registros_originais'])}",
            f"Inconsistencias identificadas: {format_int(self.stats['inconsistencias_iniciais'])}",
            "",
            f"Registros finais tratados: {format_int(self.stats['registros_finais'])}",
            f"Taxa de aproveitamento: {format_percent(self.stats['taxa_aproveitamento'])}",
            "",
            f"Arquivo exportado: {self.stats['arquivo_gerado']}",
        ]
        if self.stats.get('arquivo_inconsistencias'):
            linhas.append(f"Inconsistencias: {self.stats['arquivo_inconsistencias']}")
        linhas.append(f"Duracao: {format_duration(self.stats['duracao'])}")

        print_section("TRATAMENTO - TABELIONATO", linhas, leading_break=False)


def main():
    """Funcao principal para execucao isolada."""
    try:
        # Configurar caminhos
        base_dir = Path(__file__).parent.parent
        entrada = base_dir / "data" / "input" / "tabelionato" / "Tabelionato.zip"
        saida = base_dir / "data" / "output" / "tabelionato_tratada"
        
        # Executar processamento
        processor = TabelionatoProcessor()
        stats = processor.processar(entrada, saida)

        log_session_end("TRATAMENTO TABELIONATO", success=True)

    except Exception as e:
        linhas = [
            "[ERRO] Tratamento Tabelionato nao concluido.",
            "",
            f"Detalhes: {e}",
        ]
        print_section("TRATAMENTO - TABELIONATO", linhas, leading_break=False)
        log_session_end("TRATAMENTO TABELIONATO", success=False)
        raise


if __name__ == "__main__":
    main()

