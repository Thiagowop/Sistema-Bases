#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Processador de arquivos RAR/ZIP do Tabelionato - Converte TXT extraído para CSV/ZIP."""

import os
import sys
import zipfile
import csv
import imaplib
import email
import time
import logging
from dataclasses import dataclass, field
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional, List, Tuple
import re
import unicodedata
from email.header import decode_header

import pandas as pd
from dotenv import load_dotenv

# Configuração de paths
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

# Carregar variáveis de ambiente
load_dotenv(ROOT / ".env")

# Logger simples
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)

# Import do módulo de extração 7-Zip
from src.utils.archives import ensure_7zip_ready, extract_with_7zip

# Diretórios
PROJECT_ROOT = ROOT
INPUT_DIR = PROJECT_ROOT / "data" / "input" / "tabelionato"
INPUT_DIR_CUSTAS = PROJECT_ROOT / "data" / "input" / "tabelionato_custas"
OUTPUT_DIR = PROJECT_ROOT / "data" / "output" / "tabelionato_tratada"


# Funções utilitárias simples
def format_int(value: int) -> str:
    """Formata número com separadores de milhares."""
    return f"{value:,}".replace(",", ".")


def format_duration(seconds: float) -> str:
    """Formata duração em segundos para formato legível."""
    if seconds < 60:
        return f"{seconds:.2f}s"
    minutes = int(seconds // 60)
    secs = seconds % 60
    return f"{minutes}m {secs:.2f}s"


def print_section(title: str, lines: List[str], leading_break: bool = True) -> None:
    """Imprime uma seção formatada."""
    if leading_break:
        print()
    print("=" * 60)
    print(f"  {title}")
    print("=" * 60)
    for line in lines:
        print(f"  {line}")
    print("=" * 60)
    print()


@dataclass
class ExtracaoResumo:
    data_email: Optional[str] = None
    email_id: Optional[str] = None
    anexos_baixados: List[str] = field(default_factory=list)
    cobranca_arquivo: Optional[Path] = None
    cobranca_registros: int = 0
    custas_arquivo: Optional[Path] = None
    custas_registros: int = 0
    mensagem: Optional[str] = None


PROCESSAMENTO_METRICAS: dict[str, object] = {}


class EmailDownloader:
    """Classe para download automtico de emails com anexos do Tabelionato."""
    
    @staticmethod
    def _normalize_text(text: str | None) -> str:
        """Normaliza texto removendo acentos e espacos extras para comparacoes."""
        if not text:
            return ""
        normalized = unicodedata.normalize("NFKD", text)
        without_accents = ''.join(ch for ch in normalized if not unicodedata.combining(ch))
        return re.sub(r"\s+", " ", without_accents).strip().lower()

    def __init__(self):
        """Inicializa o downloader com configuraes especficas do Tabelionato"""
        self.email_user = os.getenv('EMAIL_USER')
        self.email_password = os.getenv('EMAIL_APP_PASSWORD')
        self.imap_server = os.getenv('IMAP_SERVER', 'imap.gmail.com')
        
        # Configuraes especficas do Tabelionato conforme documentao
        self.email_sender = 'adriano@4protestobh.com'
        self.subject_keyword = os.getenv('EMAIL_SUBJECT_KEYWORD', 'Base de Dados e Relatrio de Recebimento de Custas Postergadas do 4 Tabelionato')
        raw_tokens = os.getenv('EMAIL_SUBJECT_TOKENS', 'base de dados;relatorio de recebimento de custas;tabelionato')
        self.subject_tokens = [
            self._normalize_text(token)
            for token in raw_tokens.split(';')
            if token.strip()
        ]
        if not self.subject_tokens:
            self.subject_tokens = [self._normalize_text(self.subject_keyword)]
        self.attachment_filename = 'Cobrana'
        logger.info(f"Conta IMAP utilizada: {self.email_user}")
        
        # Validar configuraes
        if not all([self.email_user, self.email_password]):
            raise ValueError("Credenciais de email no configuradas no .env")
        
        logger.info(f"Configurado para baixar de: {self.email_sender}")
        logger.info(f"Assunto deve conter: {self.subject_keyword}")
        logger.info("Palavras-chave obrigatorias (normalizadas): %s", ', '.join(self.subject_tokens))
        logger.info(f"Nome do anexo: {self.attachment_filename}")
    
    def conectar_imap(self) -> imaplib.IMAP4_SSL:
        """Conecta ao servidor IMAP."""
        try:
            logger.info(f"Conectando ao servidor IMAP: {self.imap_server}")
            mail = imaplib.IMAP4_SSL(self.imap_server)
            mail.login(self.email_user, self.email_password)
            logger.info("Conexo IMAP estabelecida com sucesso")
            return mail
        except Exception as e:
            logger.error(f"Erro ao conectar IMAP: {e}")
            raise
    
    def decodificar_header(self, header_value: str) -> str:
        """Decodifica header de email."""
        if header_value is None:
            return ""
        
        decoded_parts = decode_header(header_value)
        decoded_string = ""
        
        for part, encoding in decoded_parts:
            if isinstance(part, bytes):
                if encoding:
                    decoded_string += part.decode(encoding)
                else:
                    decoded_string += part.decode('utf-8', errors='ignore')
            else:
                decoded_string += part
        
        return decoded_string
    
    def buscar_emails_recentes(self, mail: imaplib.IMAP4_SSL, dias: int = 7) -> List[str]:
        """Busca emails do remetente especificado (sem filtro SINCE para evitar confusao)."""
        try:
            mail.select('INBOX')
            
            # Critrio de busca simplificado: apenas por remetente
            criterio = f'(FROM "{self.email_sender}")'
            logger.info(f"Buscando emails com criterio: {criterio}")
            
            status, messages = mail.search(None, criterio)
            
            if status != 'OK':
                logger.warning("Nenhum email encontrado")
                return []
            
            email_ids = messages[0].split()
            logger.info(f"Encontrados {len(email_ids)} emails do remetente")
            
            return [email_id.decode() for email_id in email_ids]
            
        except Exception as e:
            logger.error(f"Erro ao buscar emails: {e}")
            return []
    
    def extrair_data_hora_assunto(self, assunto: str) -> Optional[str]:
        """Extrai data e hora do assunto do email."""
        try:
            # Padro especfico do Tabelionato:
            # "Base de Dados e Relatrio de Recebimento de Custas Postergadas do 4 Tabelionato de Protestos do dia 25/09/2024"
            padrao_data = r'do dia\s+(\d{1,2}[/-]\d{1,2}[/-]\d{4})'
            match = re.search(padrao_data, assunto, re.IGNORECASE)
            
            if match:
                data_str = match.group(1)
                logger.info(f"Data encontrada no assunto: {data_str}")
                
                # Normalizar formato
                data_str = re.sub(r'[-/]', '/', data_str)
                
                # Converter para formato padro
                partes = data_str.split('/')
                if len(partes) == 3:
                    dia, mes, ano = partes
                    data_formatada = f"{ano}-{mes.zfill(2)}-{dia.zfill(2)}"
                    
                    # Para emails do Tabelionato, usar apenas a data (sem hora)
                    data_hora_completa = data_formatada
                    
                    logger.info(f"Data extrada do assunto: {data_hora_completa}")
                    return data_hora_completa
            
            # Se no encontrou o padro especfico, tentar outros padres
            logger.warning(f"Padro 'do dia DD/MM/AAAA' no encontrado no assunto: {assunto}")
            
        except Exception as e:
            logger.error(f"Erro ao extrair data do assunto: {e}")
            return None
        
        # Se chegou aqui, no conseguiu extrair a data
        logger.error(f"No foi possvel extrair data do assunto: {assunto}")
        return None
    
    def processar_email(self, mail: imaplib.IMAP4_SSL, email_id: str) -> Optional[Tuple[str, str]]:
        """Processa um email especfico e baixa anexos relevantes."""
        try:
            status, msg_data = mail.fetch(email_id, '(RFC822)')
            
            if status != 'OK':
                logger.warning(f"No foi possvel buscar email {email_id}")
                return None
            
            # Parse do email
            email_message = email.message_from_bytes(msg_data[0][1])
            
            # Decodificar assunto
            assunto = self.decodificar_header(email_message['Subject'])
            remetente = self.decodificar_header(email_message['From'])
            data_email = email_message['Date']
            
            logger.info(f"Processando email: {assunto[:50]}...")
            logger.info(f"Remetente: {remetente}")
            logger.info(f"Data: {data_email}")
            
            # Verificar se o assunto contm as palavras obrigatorias (normalizadas)
            assunto_normalizado = self._normalize_text(assunto)
            missing_tokens = [token for token in self.subject_tokens if token and token not in assunto_normalizado]
            if missing_tokens:
                logger.info("Email ignorado - assunto nao contem as palavras obrigatorias: %s", ", ".join(missing_tokens))
                logger.debug("Assunto normalizado recebido: %s", assunto_normalizado)
                return None

            # Usar data/hora EXATA do email (no do assunto)
            from email.utils import parsedate_to_datetime
            try:
                data_hora_obj = parsedate_to_datetime(data_email)
                # Converter para formato brasileiro - APENAS DATA
                data_hora_email = data_hora_obj.strftime("%Y-%m-%d")
                logger.info(f"Data EXATA do email: {data_hora_email}")
            except Exception as e:
                logger.error(f"Erro ao extrair data do email: {e}")
                # Fallback para data do assunto apenas se falhar
                data_hora_email = self.extrair_data_hora_assunto(assunto)
            
            # Processar anexos
            anexo_baixado = self.processar_anexos(email_message, data_hora_email)
            
            if anexo_baixado:
                return anexo_baixado, data_hora_email
            
            return None
            
        except Exception as e:
            logger.error(f"Erro ao processar email {email_id}: {e}")
            return None
    
    def processar_anexos(self, email_message, data_hora_email: str) -> Optional[str]:
        """Processa anexos do email e salva o arquivo relevante."""
        anexos_encontrados = []
        
        for part in email_message.walk():
            if part.get_content_disposition() == 'attachment':
                filename = part.get_filename()
                
                if filename:
                    filename = self.decodificar_header(filename)
                    logger.info(f"Anexo encontrado: {filename}")
                    
                    # Verificar se  anexo do Tabelionato (Cobrana ou RecebimentoCustas)
                    filename_lower = filename.lower()
                    
                    # Anexo principal: Cobrana (ZIP/RAR)
                    if (('cobranca' in filename_lower or 'cobrana' in filename_lower) and
                        'recebimento' not in filename_lower and
                        filename.lower().endswith(('.zip', '.rar'))):
                        
                        anexos_encontrados.append(('cobranca', filename, part))
                        logger.info(f"Anexo Cobrana encontrado: {filename}")
                    
                    # Anexo secundrio: RecebimentoCustas (CSV ou RAR/ZIP)
                    elif ('recebimento' in filename_lower and 'custas' in filename_lower and
                          filename.lower().endswith(('.csv', '.zip', '.rar'))):
                        
                        anexos_encontrados.append(('custas', filename, part))
                        logger.info(f"Anexo RecebimentoCustas encontrado: {filename}")
        
        if not anexos_encontrados:
            logger.warning("Nenhum anexo vlido encontrado (Cobrana ou RecebimentoCustas)")
            return None
        
        # Processar todos os anexos encontrados
        arquivos_salvos = []
        
        for tipo_anexo, filename, part in anexos_encontrados:
            try:
                if tipo_anexo == 'cobranca':
                    # Garantir que o diretrio de cobrana existe
                    INPUT_DIR.mkdir(parents=True, exist_ok=True)
                    
                    # Limpar arquivos anteriores de cobrana
                    for arquivo_antigo in INPUT_DIR.glob("tabelionato_cobranca.*"):
                        arquivo_antigo.unlink()
                        logger.info(f"Arquivo anterior removido: {arquivo_antigo}")
                    
                    # Determinar extenso correta baseada no contedo original
                    extensao_original = Path(filename).suffix.lower()
                    nome_arquivo = f"tabelionato_cobranca{extensao_original}"
                    caminho_arquivo = INPUT_DIR / nome_arquivo
                    
                elif tipo_anexo == 'custas':
                    # Garantir que o diretrio de custas existe
                    INPUT_DIR_CUSTAS.mkdir(parents=True, exist_ok=True)
                    
                    # Limpar arquivos anteriores de custas
                    for arquivo_antigo in INPUT_DIR_CUSTAS.glob("RecebimentoCustas_*.*"):
                        arquivo_antigo.unlink()
                        logger.info(f"Arquivo anterior removido: {arquivo_antigo}")
                    
                    # Manter nome original para custas
                    nome_arquivo = filename
                    caminho_arquivo = INPUT_DIR_CUSTAS / nome_arquivo
                
                # Salvar contedo
                with open(caminho_arquivo, 'wb') as f:
                    f.write(part.get_payload(decode=True))
                
                tamanho_mb = caminho_arquivo.stat().st_size / (1024 * 1024)
                logger.info(f"Anexo '{tipo_anexo}' salvo como: {caminho_arquivo}")
                logger.info(f"Tamanho: {tamanho_mb:.2f} MB")
                
                arquivos_salvos.append(str(caminho_arquivo))
                
            except Exception as e:
                logger.error(f"Erro ao salvar anexo {tipo_anexo}: {e}")
        
        # Retornar o primeiro arquivo salvo (compatibilidade com cdigo existente)
        return arquivos_salvos[0] if arquivos_salvos else None
    
    def baixar_emails_tabelionato(self, dias: int = 7) -> List[Tuple[str, str]]:
        """Baixa emails do Tabelionato dos ltimos N dias."""
        logger.info("=" * 60)
        logger.info("   DOWNLOAD AUTOMTICO DE EMAILS - TABELIONATO")
        logger.info("=" * 60)
        
        arquivos_baixados = []
        
        try:
            # Conectar ao IMAP
            mail = self.conectar_imap()
            
            # Buscar emails recentes
            email_ids = self.buscar_emails_recentes(mail, dias)
            
            if not email_ids:
                logger.info("Nenhum email encontrado para download")
                return arquivos_baixados
            
            # Processar APENAS o email mais recente (ltimo da lista)
            if email_ids:
                email_id = email_ids[-1]  # ltimo email (mais recente)
                logger.info(f"Processando APENAS o email mais recente ID: {email_id}")
                
                resultado = self.processar_email(mail, email_id)
                
                if resultado:
                    arquivo_baixado, data_hora = resultado
                    arquivos_baixados.append((arquivo_baixado, data_hora))
                    logger.info(f"Email mais recente processado: {arquivo_baixado}")
                else:
                    logger.warning("Email mais recente no contm anexo vlido")
            
            # Fechar conexo
            mail.close()
            mail.logout()
            
            logger.info(f"Download concluido: {len(arquivos_baixados)} arquivos baixados")
            
            return arquivos_baixados
            
        except Exception as e:
            logger.error(f"Erro no download de emails: {e}")
            return arquivos_baixados


def processar_arquivo_custas(txt_path: Path, data_hora_email: str, debug: bool = False) -> Optional[Path]:
    """Processa arquivo de custas TXT/CSV e gera arquivo ZIP final."""
    import pandas as pd
    import re
    from datetime import datetime
    
    try:
        logger.info(f"Processando arquivo de custas: {txt_path}")
        
        # Ler o arquivo CSV/TXT
        if txt_path.suffix.lower() == '.csv':
            df = pd.read_csv(txt_path, encoding='utf-8-sig', sep=';')
        else:
            # Assumir que  um arquivo de largura fixa ou delimitado
            df = pd.read_csv(txt_path, encoding='utf-8-sig', sep='\t')
        
        logger.info(f"Arquivo de custas carregado: {len(df)} registros")
        
        # Tratamento da coluna Protocolo: remover pontos, espaos e caracteres especiais
        if 'Protocolo' in df.columns:
            df['Protocolo'] = df['Protocolo'].astype(str).str.replace(r'[^\w]', '', regex=True)
            logger.info("Coluna Protocolo tratada: removidos pontos, espaos e caracteres especiais")
        
        # Criar coluna Valor Total Pago = Vr. Pago Custas Postergadas + Vr. Pago Cancelamento
        custas_col = None
        cancelamento_col = None
        
        # Procurar pelas colunas de valores pagos
        for col in df.columns:
            col_lower = col.lower()
            if 'custas' in col_lower and 'pago' in col_lower:
                custas_col = col
            elif 'cancelamento' in col_lower and 'pago' in col_lower:
                cancelamento_col = col
        
        if custas_col and cancelamento_col:
            # Converter para numrico, tratando valores em formato brasileiro
            def converter_valor(serie):
                from decimal import Decimal, InvalidOperation
                
                def limpar_valor(valor_str):
                    if pd.isna(valor_str) or valor_str == '':
                        return Decimal('0.00')
                    
                    # Converter para string e limpar
                    valor_str = str(valor_str).replace('R$', '').strip()
                    
                    # Remover espaos em branco
                    valor_str = valor_str.replace(' ', '')
                    
                    # Se contm vrgula, assumir formato brasileiro (1.234,56)
                    if ',' in valor_str:
                        # Separar parte inteira e decimal pela vrgula
                        partes = valor_str.split(',')
                        if len(partes) == 2:
                            parte_inteira = partes[0].replace('.', '')  # Remove separadores de milhares
                            parte_decimal = partes[1]
                            # Garantir que parte decimal tenha no mximo 2 dgitos
                            if len(parte_decimal) > 2:
                                parte_decimal = parte_decimal[:2]
                            valor_str = f"{parte_inteira}.{parte_decimal}"
                        else:
                            # Mltiplas vrgulas - tratar como erro, remover tudo
                            valor_str = valor_str.replace(',', '').replace('.', '')
                    else:
                        # Sem vrgula - pode ser formato americano ou apenas inteiro
                        # Se tem apenas um ponto e 2 dgitos aps,  decimal
                        if valor_str.count('.') == 1 and len(valor_str.split('.')[-1]) <= 2:
                            pass  # Manter como est (formato americano)
                        else:
                            # Mltiplos pontos ou formato ambguo - remover pontos
                            valor_str = valor_str.replace('.', '')
                    
                    try:
                        resultado = Decimal(valor_str) if valor_str else Decimal('0.00')
                        return resultado
                    except InvalidOperation as e:
                        logger.warning(f"Erro ao converter valor '{valor_str}': {e}")
                        return Decimal('0.00')
                
                return serie.apply(limpar_valor)
            
            df['Valor Total Pago'] = converter_valor(df[custas_col]) + converter_valor(df[cancelamento_col])
            logger.info(f"Coluna 'Valor Total Pago' criada: {custas_col} + {cancelamento_col}")
        else:
            logger.warning(f"Colunas de valores no encontradas. Disponveis: {list(df.columns)}")
        
        # Adicionar data de extracao como coluna e logar
        if not data_hora_email:
            logger.error("Data/hora do email não disponível; abortando processamento de custas para evitar data incorreta.")
            return None
        df['DataExtracao'] = data_hora_email
        logger.info(f"Data de extracao (custas): {data_hora_email}")
        
        # Gerar nome do arquivo de sada
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_filename = f"RecebimentoCustas_{timestamp}.zip"
        output_path = INPUT_DIR_CUSTAS / output_filename
        
        # Salvar como CSV temporrio
        temp_csv = INPUT_DIR_CUSTAS / f"RecebimentoCustas_{timestamp}.csv"
        df.to_csv(temp_csv, index=False, encoding='utf-8-sig', sep=';')
        
        # Criar arquivo ZIP
        import zipfile
        with zipfile.ZipFile(output_path, 'w', zipfile.ZIP_DEFLATED) as zf:
            zf.write(temp_csv, temp_csv.name)
        
        # Remover CSV temporrio imediatamente aps a gerao do ZIP
        # (o CSV permanece apenas dentro do arquivo compactado)
        temp_csv.unlink(missing_ok=True)
        
        tamanho_mb = output_path.stat().st_size / (1024 * 1024)
        logger.info(f"Arquivo de custas processado e salvo como: {output_path}")
        logger.info(f"Tamanho do arquivo: {tamanho_mb:.2f} MB")

        PROCESSAMENTO_METRICAS["custas_arquivo"] = output_path
        PROCESSAMENTO_METRICAS["custas_registros"] = len(df)

        return output_path
        
    except Exception as e:
        logger.error(f"Erro ao processar arquivo de custas: {e}")
        return None


def processar_arquivo_txt(txt_path: Path, data_hora_email: str, debug: bool = False) -> Optional[Path]:
    """Processa arquivo TXT/CSV do Tabelionato e converte para Tabelionato.zip."""

    column_names = [
        'Protocolo', 'VrTitulo', 'DtAnuencia', 'Devedor',
        'Endereco', 'Cidade', 'Cep', 'CpfCnpj',
        'Intimado', 'Custas', 'Credor'
    ]

    bool_map = {
        'false': 'False',
        'falso': 'False',
        'true': 'True',
        'verdadeiro': 'True'
    }

    cpf_cnpj_pattern = re.compile(
        r'(\d{3}\.\d{3}\.\d{3}-\d{2}'
        r'|\d{2}\.\d{3}\.\d{3}/\d{4}-\d{2}'
        r'|\d{11}'
        r'|\d{14})'
    )

    def normalize_spaces(series: pd.Series) -> pd.Series:
        return series.fillna('').astype(str).str.strip().str.replace(r'\s+', ' ', regex=True)

    def normalize_data(value: str) -> str:
        if not value:
            return ''
        if re.fullmatch(r'\d{2}/\d{2}/\d{4}\s+\d{2}:\d{2}:\d{2}', value):
            return value
        if re.fullmatch(r'\d{2}/\d{2}/\d{4}\s+\d{2}:\d{2}', value):
            return f"{value}:00"
        if re.fullmatch(r'\d{2}/\d{2}/\d{4}', value):
            return f"{value} 00:00:00"
        return value

    def format_cpf_cnpj(value: str) -> str:
        digits = re.sub(r'\D', '', value or '')
        if len(digits) == 11:
            return f"{digits[:3]}.{digits[3:6]}.{digits[6:9]}-{digits[9:]}"
        if len(digits) == 14:
            return f"{digits[:2]}.{digits[2:5]}.{digits[5:8]}/{digits[8:12]}-{digits[12:]}"
        return value.strip()

    def normalize_cep(value: str) -> str:
        digits = re.sub(r'\D', '', value or '')
        return digits if len(digits) == 8 else value.strip()

    def normalize_currency(value: str) -> str:
        value = value.strip()
        if not value:
            return ''
        cleaned = re.sub(r'\s+', '', value.replace('R$', '').upper())
        return cleaned if cleaned else ''

    try:
        logger.info(f"Processando arquivo: {txt_path}")

        column_names = [
            'Protocolo', 'VrTitulo', 'DtAnuencia', 'Devedor',
            'Endereco', 'Cidade', 'Cep', 'CpfCnpj',
            'Intimado', 'Custas', 'Credor'
        ]

        registros: List[List[str]] = []
        inconsistencias: List[dict] = []

        with open(txt_path, 'r', encoding='utf-8-sig', errors='ignore') as f:
            linhas = f.readlines()

        if not linhas:
            logger.error('Arquivo vazio ou ilegvel: nenhuma linha encontrada')
            return None

        header_line = linhas[0].rstrip('\r\n') if linhas else ''
        start_idx = 1 if header_line and 'Protocolo' in header_line else 0

        space_re = re.compile(r'\s+')
        bool_pattern = re.compile(r'\b(false|true|falso|verdadeiro)\b', re.IGNORECASE)
        currency_pattern = re.compile(r'R\$\s*[0-9.\s]+,\d{2}')
        non_digit_re = re.compile(r'\D')
        bool_map = {
            'false': 'False',
            'falso': 'False',
            'true': 'True',
            'verdadeiro': 'True'
        }

        def normalize_text(value: str) -> str:
            return space_re.sub(' ', value.strip()) if value else ''

        def normalize_data(value: str) -> str:
            value = normalize_text(value)
            if not value:
                return value
            if re.fullmatch(r'\d{2}/\d{2}/\d{4}\s+\d{2}:\d{2}:\d{2}', value):
                return value
            if re.fullmatch(r'\d{2}/\d{2}/\d{4}\s+\d{2}:\d{2}', value):
                return f"{value}:00"
            if re.fullmatch(r'\d{2}/\d{2}/\d{4}', value):
                return f"{value} 00:00:00"
            return value

        def normalize_cep(value: str) -> str:
            digits = non_digit_re.sub('', value or '')
            if len(digits) == 8:
                return digits
            return normalize_text(value)

        def normalize_currency(value: str) -> str:
            if not value:
                return ''
            cleaned = value.upper().replace('R$', '').strip()
            cleaned = cleaned.replace(' ', '')
            return cleaned if cleaned else ''

        def normalize_bool(value: str) -> str:
            if not value:
                return ''
            return bool_map.get(value.strip().lower(), value.strip())

        def compute_fixed_width_specs(header: str) -> Optional[List[Tuple[int, int]]]:
            if not header:
                return None
            try:
                positions = [header.index(col) for col in column_names]
            except ValueError:
                return None
            specs: List[Tuple[int, int]] = []
            for idx, start in enumerate(positions):
                end = positions[idx + 1] if idx + 1 < len(positions) else len(header)
                specs.append((start, end))
            return specs

        fixed_width_specs = None
        if header_line and ';' not in header_line:
            fixed_width_specs = compute_fixed_width_specs(header_line)
            if fixed_width_specs:
                logger.info('Formato detectado: colunas de largura fixa')

        def parse_fixed_width_line(raw_line: str) -> Optional[List[str]]:
            if not fixed_width_specs:
                return None
            line = raw_line.rstrip('\r\n')
            if not line.strip():
                return None
            last_end = fixed_width_specs[-1][1]
            if len(line) < last_end:
                line = line.ljust(last_end)
            return [normalize_text(line[start:end]) for start, end in fixed_width_specs]

        def parse_semicolon_line(raw_line: str) -> Optional[List[str]]:
            if ';' not in raw_line:
                return None
            try:
                row = next(csv.reader([raw_line], delimiter=';'))
            except Exception:
                return None
            if len(row) != len(column_names):
                return None
            return [normalize_text(value) for value in row]

        total_linhas_validas = 0
        for idx, raw in enumerate(linhas[start_idx:], start=1 + start_idx):
            if not raw.strip():
                continue

            total_linhas_validas += 1
            parsed_values = parse_fixed_width_line(raw) if fixed_width_specs else None
            if parsed_values is None:
                parsed_values = parse_semicolon_line(raw)

            if parsed_values is None:
                if debug:
                    inconsistencias.append({
                        'linha': idx,
                        'motivo': 'formato_nao_reconhecido',
                        'conteudo': raw.strip()
                    })
                continue

            if len(parsed_values) != len(column_names):
                if debug:
                    inconsistencias.append({
                        'linha': idx,
                        'motivo': 'colunas_incompletas',
                        'conteudo': raw.strip()
                    })
                continue

            protocolo = parsed_values[0]
            vr_titulo = parsed_values[1]
            dt_anuencia = normalize_data(parsed_values[2])
            devedor = parsed_values[3]
            endereco = parsed_values[4]
            cidade = parsed_values[5]
            cep = normalize_cep(parsed_values[6])
            cpf_cnpj = normalize_text(parsed_values[7])
            intimado = normalize_bool(parsed_values[8])
            custas = normalize_currency(parsed_values[9])
            credor = normalize_text(parsed_values[10])

            if not protocolo:
                if debug:
                    inconsistencias.append({
                        'linha': idx,
                        'motivo': 'protocolo_vazio',
                        'conteudo': raw.strip()
                    })
                continue

            registros.append([
                protocolo,
                vr_titulo,
                dt_anuencia,
                devedor,
                endereco,
                cidade,
                cep,
                cpf_cnpj,
                intimado,
                custas,
                credor
            ])

        if not registros:
            logger.error('Nenhum registro vlido foi gerado a partir do arquivo de entrada')
            return None

        df = pd.DataFrame(registros, columns=column_names)
        df = df.fillna('')
        df['Intimado'] = df['Intimado'].apply(normalize_bool)
        df['Custas'] = df['Custas'].apply(normalize_currency)
        df['Credor'] = df['Credor'].apply(normalize_text)
        df['Endereco'] = df['Endereco'].apply(normalize_text)
        df['Cidade'] = df['Cidade'].apply(normalize_text)
        df['CpfCnpj'] = df['CpfCnpj'].apply(normalize_text)
        df['Cep'] = df['Cep'].apply(normalize_cep)
        df['DtAnuencia'] = df['DtAnuencia'].apply(normalize_data)

        mask_intimado_missing = df['Intimado'].eq('')
        bool_from_credor = df.loc[mask_intimado_missing, 'Credor'].str.extract(
            r'\b(False|True|FALSO|VERDADEIRO)\b', flags=re.IGNORECASE
        )[0]
        if bool_from_credor is not None:
            bool_mask = bool_from_credor.notna()
            if bool_mask.any():
                idxs = bool_from_credor[bool_mask].index
                df.loc[idxs, 'Intimado'] = bool_from_credor[bool_mask].apply(normalize_bool)
                df.loc[idxs, 'Credor'] = df.loc[idxs, 'Credor'].str.replace(
                    r'\b(False|True|FALSO|VERDADEIRO)\b', '', n=1, regex=True
                ).str.strip()

        mask_cpf_missing = df['CpfCnpj'].eq('')
        cpf_from_credor = df.loc[mask_cpf_missing, 'Credor'].str.extract(
            r'(\d{3}\.\d{3}\.\d{3}-\d{2}|\d{2}\.\d{3}\.\d{3}/\d{4}-\d{2})'
        )[0]
        if cpf_from_credor is not None:
            cpf_mask = cpf_from_credor.notna()
            if cpf_mask.any():
                idxs = cpf_from_credor[cpf_mask].index
                df.loc[idxs, 'CpfCnpj'] = cpf_from_credor[cpf_mask].apply(normalize_text)
                df.loc[idxs, 'Credor'] = df.loc[idxs, 'Credor'].str.replace(
                    r'(\d{3}\.\d{3}\.\d{3}-\d{2}|\d{2}\.\d{3}\.\d{3}/\d{4}-\d{2})',
                    '', n=1, regex=True
                ).str.strip()

        mask_custas_in_credor = df['Credor'].str.contains(r'R\$', na=False)
        mask_custas_in_credor |= df['Credor'].str.contains(r'R\$\s*[0-9.,]+', na=False)
        mask_custas_missing = df['Custas'].eq('')
        mask_fix_custas = mask_custas_in_credor & mask_custas_missing
        if mask_fix_custas.any():
            valores_custas = df.loc[mask_fix_custas, 'Credor'].str.extract(
                r'(R\$\s*[0-9.,]+)', flags=re.IGNORECASE
            )[0]
            df.loc[mask_fix_custas, 'Custas'] = valores_custas.apply(normalize_currency)
            df.loc[mask_fix_custas, 'Credor'] = df.loc[mask_fix_custas, 'Credor'].str.replace(
                r'R\$\s*[0-9.,]+\s*', '', regex=True
            ).str.strip()

        total_processadas = len(df)
        total_linhas = total_linhas_validas
        logger.info(f"Linhas no TXT (sem cabecalho): {total_linhas:,}")
        logger.info(f"Registros parseados: {total_processadas:,}")
        logger.info(f"Ignoradas: {total_linhas - total_processadas:,}")

        # Adicionar data de extracao como coluna e logar
        if not data_hora_email:
            logger.error("Data/hora do email não disponível; abortando processamento de cobrança para evitar data incorreta.")
            return None
        df['DataExtracao'] = data_hora_email
        logger.info(f"Data de extracao (cobranca): {data_hora_email}")

        csv_name = 'Tabelionato.csv'
        zip_name = 'Tabelionato.zip'
        zip_path = INPUT_DIR / zip_name
        INPUT_DIR.mkdir(parents=True, exist_ok=True)
        temp_csv = INPUT_DIR / csv_name
        df.to_csv(temp_csv, index=False, sep=';', encoding='utf-8-sig')

        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zf:
            zf.write(temp_csv, csv_name)

        temp_csv.unlink(missing_ok=True)

        if debug and inconsistencias:
            inc_df = pd.DataFrame(inconsistencias)
            inc_path = INPUT_DIR / 'Tabelionato_inconsistencias.csv'
            inc_df.to_csv(inc_path, index=False, sep=';', encoding='utf-8-sig')
            logger.info(f"Inconsistencias salvas em: {inc_path} ({len(inc_df):,} linhas)")

        logger.info(f"Arquivo processado e salvo como: {zip_path}")
        logger.info(f"Tamanho do arquivo: {zip_path.stat().st_size / (1024*1024):.2f} MB")

        PROCESSAMENTO_METRICAS["cobranca_arquivo"] = zip_path
        PROCESSAMENTO_METRICAS["cobranca_registros"] = len(df)

        return zip_path
    except Exception as exc:
        logger.error(f"Falha ao ler TXT: {exc}")
        return None


def extrair_zip_com_senha(zip_path: Path, senha: str = "Mf4tab@") -> Optional[Path]:
    """Extrai ZIP protegido usando 7-Zip e retorna o TXT/CSV extraído."""
    zip_path = Path(zip_path)
    if not zip_path.exists():
        logger.error("Arquivo ZIP nao encontrado: %s", zip_path)
        return None

    try:
        novos_arquivos = extract_with_7zip(zip_path, INPUT_DIR, senha=senha)
    except FileNotFoundError as exc:
        logger.error("Ferramenta 7-Zip nao localizada: %s", exc)
        return None
    except RuntimeError as exc:
        logger.error("Falha ao extrair arquivo ZIP: %s", exc)
        return None

    for arquivo in novos_arquivos:
        if arquivo.suffix.lower() in {'.txt', '.csv'}:
            logger.info("Arquivo de cobranca extraido: %s", arquivo)
            return arquivo

    logger.error("Nenhum arquivo TXT/CSV de cobranca foi identificado apos a extracao.")
    return None


def extrair_zip_com_senha_custas(zip_path: Path, senha: str = "Mf4tab@") -> Optional[Path]:
    """Extrai ZIP de custas usando 7-Zip e retorna o TXT/CSV extraído."""
    zip_path = Path(zip_path)
    if not zip_path.exists():
        logger.error("Arquivo ZIP de custas nao encontrado: %s", zip_path)
        return None

    try:
        novos_arquivos = extract_with_7zip(zip_path, INPUT_DIR_CUSTAS, senha=senha)
    except FileNotFoundError as exc:
        logger.error("Ferramenta 7-Zip nao localizada: %s", exc)
        return None
    except RuntimeError as exc:
        logger.error("Falha ao extrair arquivo ZIP de custas: %s", exc)
        return None

    for arquivo in novos_arquivos:
        if arquivo.suffix.lower() in {'.txt', '.csv'}:
            logger.info("Arquivo de custas extraido: %s", arquivo)
            return arquivo

    logger.error("Nenhum arquivo TXT/CSV de custas foi identificado apos a extracao.")
    return None

def extrair_rar_com_senha_custas(rar_path: Path, senha: str = "Mf4tab@") -> Optional[Path]:
    """Extrai arquivo RAR de custas usando o 7-Zip detectado automaticamente."""
    rar_path = Path(rar_path)
    if not rar_path.exists():
        logger.error("Arquivo de custas nao encontrado: %s", rar_path)
        return None

    try:
        novos_arquivos = extract_with_7zip(rar_path, INPUT_DIR_CUSTAS, senha=senha)
    except FileNotFoundError as exc:
        logger.error("Ferramenta 7-Zip nao localizada: %s", exc)
        return None
    except RuntimeError as exc:
        logger.error("Falha ao extrair arquivo de custas: %s", exc)
        return None

    for arquivo in novos_arquivos:
        if arquivo.suffix.lower() in {'.txt', '.csv'}:
            logger.info("Arquivo de custas extraido: %s", arquivo)
            return arquivo

    logger.error("Nenhum arquivo TXT/CSV de custas foi identificado apos a extracao.")
    return None





def extrair_rar_com_senha(rar_path: Path, senha: str = "Mf4tab@") -> Optional[Path]:
    """Extrai arquivo RAR de cobranca usando o 7-Zip detectado automaticamente."""
    rar_path = Path(rar_path)
    if not rar_path.exists():
        logger.error("Arquivo de cobranca nao encontrado: %s", rar_path)
        return None

    try:
        novos_arquivos = extract_with_7zip(rar_path, INPUT_DIR, senha=senha)
    except FileNotFoundError as exc:
        logger.error("Ferramenta 7-Zip nao localizada: %s", exc)
        return None
    except RuntimeError as exc:
        logger.error("Falha ao extrair arquivo de cobranca: %s", exc)
        return None

    for arquivo in novos_arquivos:
        if arquivo.suffix.lower() in {'.txt', '.csv'}:
            logger.info("Arquivo de cobranca extraido: %s", arquivo)
            return arquivo

    logger.error("Nenhum arquivo TXT/CSV de cobranca foi identificado apos a extracao.")
    return None


def processar_cobranca(
    debug_mode: bool,
    data_hora_email: str | None = None,
    *,
    resumo: ExtracaoResumo | None = None,
) -> None:
    """Processa arquivos de cobranca do tabelionato."""

    logger.info("=" * 60)
    logger.info("   PROCESSANDO ARQUIVOS DE COBRANCA")
    logger.info("=" * 60)

    arquivos_baixados_files = sorted(INPUT_DIR.glob("tabelionato_cobranca.*"))

    if not arquivos_baixados_files:
        logger.info("Nenhum arquivo no formato padrao encontrado. Procurando arquivos TXT/CSV/ZIP no diretorio...")
        fallback_padroes = ['*.txt', '*.csv', '*.zip', '*.rar']
        for padrao in fallback_padroes:
            candidatos = sorted(INPUT_DIR.glob(padrao))
            if candidatos:
                arquivos_baixados_files = candidatos
                logger.info(f"Arquivo(s) encontrado(s) com padrao {padrao}: {arquivos_baixados_files[:1]}")
                break

    if not arquivos_baixados_files:
        logger.warning("Nenhum arquivo de cobranca encontrado para processamento.")
        return

    arquivo = arquivos_baixados_files[0]
    logger.info(f"Arquivo encontrado: {arquivo}")
    logger.info(f"Tamanho: {arquivo.stat().st_size / (1024*1024):.2f} MB")

    suffix = arquivo.suffix.lower()
    if suffix == '.zip':
        txt_path = extrair_zip_com_senha(arquivo)
    elif suffix == '.rar':
        txt_path = extrair_rar_com_senha(arquivo)
    elif suffix in {'.txt', '.csv'}:
        txt_path = arquivo
    else:
        logger.error(f"Formato de arquivo nao suportado: {arquivo.suffix}")
        return

    if not txt_path or not txt_path.exists():
        logger.error("Nao foi possivel extrair o arquivo de cobranca.")
        return

    logger.info(f"Processando arquivo: {txt_path}")

    if not data_hora_email:
        logger.error("Data/hora do email nao disponivel; interrompendo processamento de cobranca para evitar data incorreta.")
        return

    resultado = processar_arquivo_txt(txt_path, data_hora_email, debug=debug_mode)

    if not debug_mode and txt_path.exists() and txt_path != arquivo:
        txt_path.unlink()
        logger.info(f"Arquivo temporario removido: {txt_path}")

    if resultado and arquivo.exists() and not debug_mode and arquivo != resultado:
        arquivo.unlink()
        logger.info(f"Arquivo original removido: {arquivo}")

    if resultado:
        logger.info("Processamento de cobranca concluido com sucesso!")
        logger.info(f"Arquivo final: {resultado}")
        if resumo is not None:
            resumo.cobranca_arquivo = Path(resultado)
            resumo.cobranca_registros = int(PROCESSAMENTO_METRICAS.get("cobranca_registros", 0))
    else:
        logger.error("Falha no processamento de cobranca.")


def processar_custas(
    debug_mode: bool,
    data_hora_email: str | None = None,
    *,
    resumo: ExtracaoResumo | None = None,
) -> None:
    """Processa arquivos de custas do tabelionato."""

    logger.info("=" * 60)
    logger.info("   PROCESSANDO ARQUIVOS DE CUSTAS")
    logger.info("=" * 60)

    arquivos_custas = sorted(INPUT_DIR_CUSTAS.glob("RecebimentoCustas_*.*"))

    if not arquivos_custas:
        logger.warning("Nenhum arquivo de custas encontrado para processamento.")
        return

    arquivo = arquivos_custas[0]
    logger.info(f"Arquivo de custas encontrado: {arquivo}")
    logger.info(f"Tamanho: {arquivo.stat().st_size / (1024*1024):.2f} MB")

    suffix = arquivo.suffix.lower()
    if suffix == '.zip':
        txt_path = extrair_zip_com_senha_custas(arquivo)
    elif suffix == '.rar':
        txt_path = extrair_rar_com_senha_custas(arquivo)
    elif suffix in {'.txt', '.csv'}:
        txt_path = arquivo
    else:
        logger.error(f"Formato de arquivo nao suportado: {arquivo.suffix}")
        return

    if not txt_path or not txt_path.exists():
        logger.error("Nao foi possivel extrair o arquivo de custas.")
        return

    logger.info(f"Processando arquivo de custas: {txt_path}")

    if not data_hora_email:
        logger.error("Data/hora do email nao disponivel; interrompendo processamento de custas para evitar data incorreta.")
        return

    resultado = processar_arquivo_custas(txt_path, data_hora_email, debug=debug_mode)

    if not debug_mode and txt_path.exists() and txt_path != arquivo:
        txt_path.unlink()
        logger.info(f"Arquivo temporario de custas removido: {txt_path}")

    if resultado and arquivo.exists() and not debug_mode and arquivo != resultado:
        arquivo.unlink()
        logger.info(f"Arquivo original de custas removido: {arquivo}")

    if resultado:
        logger.info("Processamento de custas concluido com sucesso!")
        logger.info(f"Arquivo final de custas: {resultado}")
        if resumo is not None:
            resumo.custas_arquivo = Path(resultado)
            resumo.custas_registros = int(PROCESSAMENTO_METRICAS.get("custas_registros", 0))
    else:
        logger.error("Falha no processamento de custas.")




def main() -> None:
    """Funcao principal."""

    inicio = time.perf_counter()
    resumo = ExtracaoResumo()

    logger.info("=" * 60)
    logger.info("   PROCESSADOR DE ARQUIVOS RAR/ZIP TABELIONATO")
    logger.info("=" * 60)

    ensure_7zip_ready()

    debug_mode = ('--debug' in sys.argv) or (os.getenv('TABELIONATO_DEBUG', '0') == '1')
    if debug_mode:
        logger.info("DEBUG ativado: manter TXT/CSV e gerar relatorio de inconsistencias")

    data_hora_email: Optional[str] = None

    try:
        logger.info("Tentando baixar emails automaticamente...")
        downloader = EmailDownloader()
        arquivos_baixados = downloader.baixar_emails_tabelionato(dias=7)

        if arquivos_baixados:
            resumo.anexos_baixados = [Path(arquivo).name for arquivo, _ in arquivos_baixados]
            _, data_hora_email = arquivos_baixados[0]
            resumo.data_email = data_hora_email
        else:
            resumo.mensagem = "Nenhum email novo encontrado."
            logger.error("Nenhum email novo encontrado; bloqueando extracao para evitar data incorreta.")

    except Exception as exc:
        resumo.mensagem = f"Falha no download de emails: {exc}"
        logger.warning(resumo.mensagem)
        logger.info("Continuando com processamento de arquivos existentes...")

    if not data_hora_email:
        linhas = [
            "[ERRO] Extracao Tabelionato nao concluida.",
            "",
            resumo.mensagem or "Data do email nao encontrada.",
        ]
        print_section("EXTRACAO - TABELIONATO", linhas, leading_break=False)
        return

    processar_cobranca(debug_mode, data_hora_email, resumo=resumo)
    processar_custas(debug_mode, data_hora_email, resumo=resumo)

    resumo.cobranca_arquivo = resumo.cobranca_arquivo or PROCESSAMENTO_METRICAS.get("cobranca_arquivo")
    resumo.cobranca_registros = resumo.cobranca_registros or int(
        PROCESSAMENTO_METRICAS.get("cobranca_registros", 0)
    )
    resumo.custas_arquivo = resumo.custas_arquivo or PROCESSAMENTO_METRICAS.get("custas_arquivo")
    resumo.custas_registros = resumo.custas_registros or int(
        PROCESSAMENTO_METRICAS.get("custas_registros", 0)
    )

    duracao = time.perf_counter() - inicio

    anexos = ", ".join(resumo.anexos_baixados) if resumo.anexos_baixados else "-"

    linhas = [
        "[STEP] Extracao Tabelionato",
        "",
        f"Email processado: {resumo.data_email or '-'}",
        f"Anexos baixados: {anexos}",
        "",
        f"Cobranca: {format_int(resumo.cobranca_registros)} registros",
        f"Arquivo cobranca: {resumo.cobranca_arquivo or '-'}",
        "",
        f"Custas: {format_int(resumo.custas_registros)} registros",
        f"Arquivo custas: {resumo.custas_arquivo or '-'}",
        "",
        f"Duracao: {format_duration(duracao)}",
    ]

    print_section("EXTRACAO - TABELIONATO", linhas, leading_break=False)

if __name__ == "__main__":
    main()


