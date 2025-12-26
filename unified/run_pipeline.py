#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Pipeline Unificado - Executa processamento para qualquer cliente.
Uso: python run_pipeline.py <cliente> [--output-dir <dir>] [--dry-run]

Clientes suportados:
  - emccamp: Carrega de arquivos locais
  - vic: Carrega de email (requer .env com EMAIL_USER, EMAIL_APP_PASSWORD)
  - tabelionato: Carrega de email com RAR protegido (requer 7-Zip)
"""

import argparse
import email
import imaplib
import os
import subprocess
import sys
import zipfile
from dataclasses import dataclass
from datetime import datetime, timedelta
from email.header import decode_header
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, Dict, List, Optional, Set, Tuple

import pandas as pd
import yaml

# Carregar .env se disponível
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass


@dataclass
class PipelineResult:
    """Resultado da execução do pipeline."""
    success: bool
    client: str
    duration: float
    novos: int = 0
    baixas: int = 0
    devolucao: int = 0
    outputs: Dict[str, Path] = None
    error: Optional[str] = None

    def __post_init__(self):
        if self.outputs is None:
            self.outputs = {}


class UnifiedPipeline:
    """Pipeline unificado para processamento de carteiras."""

    def __init__(self, config_path: Path, output_dir: Path):
        self.config_path = config_path
        self.output_dir = output_dir
        self.config = self._load_config()
        self.client_name = self.config.get('name', 'unknown')

    def _load_config(self) -> Dict[str, Any]:
        """Carrega configuração YAML do cliente."""
        with open(self.config_path, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f)

    def _load_file(self, path: str, password: str = None) -> pd.DataFrame:
        """Carrega dados de arquivo (CSV, Excel, ZIP)."""
        file_path = Path(path)

        # Resolver caminho relativo
        if not file_path.is_absolute():
            # Determinar projeto base pelo nome do cliente
            project_map = {
                'emccamp': 'Emccamp',
                'vic': 'Automacao_Vic',
                'tabelionato': 'Automacao_Tabelionato',
            }
            project_dir = project_map.get(self.client_name.lower(), '')

            # Tentar vários caminhos base
            script_dir = Path(__file__).resolve().parent
            project_root = script_dir.parent  # /home/user/Sistema-Bases

            bases = [
                Path.cwd(),
                Path.cwd() / 'unified',
                self.config_path.parent.parent.parent,
                project_root / project_dir if project_dir else None,
                script_dir,
            ]

            for base in bases:
                if base is None:
                    continue
                candidate = base / path
                if candidate.exists():
                    file_path = candidate
                    break

        if not file_path.exists():
            raise FileNotFoundError(f"Arquivo não encontrado: {path}")

        return self._read_file(file_path, password)

    def _read_file(self, file_path: Path, password: str = None) -> pd.DataFrame:
        """Lê dados de arquivo (CSV, Excel, ZIP, RAR)."""
        suffix = file_path.suffix.lower()
        encoding = 'utf-8-sig'
        separator = ';'

        if suffix == '.zip':
            with zipfile.ZipFile(file_path, 'r') as zf:
                for name in zf.namelist():
                    if name.lower().endswith('.csv'):
                        with zf.open(name) as f:
                            df = pd.read_csv(f, sep=separator, encoding=encoding, dtype=str, low_memory=False)
                            df.columns = [str(c).strip().upper() for c in df.columns]
                            return df
            raise ValueError(f"Nenhum CSV encontrado no ZIP: {file_path}")

        elif suffix == '.csv':
            df = pd.read_csv(file_path, sep=separator, encoding=encoding, dtype=str, low_memory=False)
            df.columns = [str(c).strip().upper() for c in df.columns]
            return df

        elif suffix in ('.xlsx', '.xls'):
            df = pd.read_excel(file_path, dtype=str)
            df.columns = [str(c).strip().upper() for c in df.columns]
            return df

        elif suffix == '.rar':
            return self._extract_rar(file_path, password)

        else:
            raise ValueError(f"Tipo de arquivo não suportado: {suffix}")

    def _extract_rar(self, rar_path: Path, password: str = None) -> pd.DataFrame:
        """Extrai e carrega dados de arquivo RAR (requer 7-Zip)."""
        seven_zip_paths = [
            os.getenv('SEVEN_ZIP_PATH', ''),
            '/usr/bin/7z',
            '/usr/local/bin/7z',
            'C:\\Program Files\\7-Zip\\7z.exe',
            str(Path(__file__).parent.parent / 'Automacao_Tabelionato' / 'bin' / '7z.exe'),
        ]

        seven_zip = None
        for p in seven_zip_paths:
            if p and Path(p).exists():
                seven_zip = p
                break

        if not seven_zip:
            raise RuntimeError("7-Zip não encontrado. Instale 7-Zip ou configure SEVEN_ZIP_PATH.")

        with TemporaryDirectory() as temp_dir:
            cmd = [seven_zip, 'x', str(rar_path), f'-o{temp_dir}', '-y']
            if password:
                cmd.append(f'-p{password}')

            result = subprocess.run(cmd, capture_output=True, text=True)
            if result.returncode != 0:
                raise RuntimeError(f"Erro ao extrair RAR: {result.stderr}")

            # Procurar CSV ou Excel no diretório extraído
            for ext in ['*.csv', '*.xlsx', '*.xls']:
                for f in Path(temp_dir).rglob(ext):
                    return self._read_file(f)

            raise ValueError(f"Nenhum arquivo de dados encontrado no RAR: {rar_path}")

    def _load_email(self, params: Dict) -> pd.DataFrame:
        """Carrega dados de anexo de email via IMAP."""
        server = params.get('server', os.getenv('IMAP_SERVER', 'imap.gmail.com'))
        email_user = params.get('email', os.getenv('EMAIL_USER', ''))
        password = params.get('password', os.getenv('EMAIL_APP_PASSWORD', ''))
        folder = params.get('folder', 'INBOX')
        sender_filter = params.get('sender_filter', '')
        subject_filter = params.get('subject_filter', '')
        days_back = params.get('days_back', 30)
        attachment_pattern = params.get('attachment_pattern', '*.zip')
        file_password = params.get('file_password')

        if not email_user or not password:
            raise ValueError("Credenciais de email não configuradas (EMAIL_USER, EMAIL_APP_PASSWORD)")

        try:
            mail = imaplib.IMAP4_SSL(server)
            mail.login(email_user, password)
            mail.select(folder)

            # Construir critério de busca
            criteria = []
            if sender_filter:
                criteria.append(f'FROM "{sender_filter}"')
            if subject_filter:
                criteria.append(f'SUBJECT "{subject_filter}"')
            if days_back > 0:
                since = (datetime.now() - timedelta(days=days_back)).strftime('%d-%b-%Y')
                criteria.append(f'SINCE "{since}"')

            search_query = ' '.join(criteria) if criteria else 'ALL'

            status, msg_ids = mail.search(None, search_query)
            if status != 'OK' or not msg_ids[0]:
                mail.logout()
                raise ValueError(f"Nenhum email encontrado: {search_query}")

            # Pegar email mais recente
            latest_id = msg_ids[0].split()[-1]
            status, msg_data = mail.fetch(latest_id, '(RFC822)')
            mail.logout()

            if status != 'OK':
                raise ValueError("Falha ao buscar email")

            msg = email.message_from_bytes(msg_data[0][1])

            # Extrair anexo
            with TemporaryDirectory() as temp_dir:
                import fnmatch

                for part in msg.walk():
                    if part.get_content_maintype() == 'multipart':
                        continue

                    filename = part.get_filename()
                    if not filename:
                        continue

                    # Decodificar nome
                    decoded = decode_header(filename)
                    if decoded:
                        text, enc = decoded[0]
                        if isinstance(text, bytes):
                            filename = text.decode(enc or 'utf-8', errors='replace')
                        else:
                            filename = str(text)

                    if fnmatch.fnmatch(filename.lower(), attachment_pattern.lower()):
                        filepath = Path(temp_dir) / filename
                        with open(filepath, 'wb') as f:
                            f.write(part.get_payload(decode=True))

                        return self._read_file(filepath, file_password)

                raise ValueError(f"Nenhum anexo encontrado com padrão: {attachment_pattern}")

        except imaplib.IMAP4.error as e:
            raise ValueError(f"Erro IMAP: {e}")

    def _execute_loader(self, loader_config: Dict, name: str) -> pd.DataFrame:
        """Executa o loader apropriado baseado na configuração."""
        loader_type = loader_config.get('type', 'file')
        params = loader_config.get('params', {})

        try:
            if loader_type == 'file':
                path = params.get('path', '')
                password = params.get('password')
                df = self._load_file(path, password)
                print(f"    {name}: {len(df)} registros")
                return df

            elif loader_type == 'email':
                df = self._load_email(params)
                print(f"    {name}: {len(df)} registros (via email)")
                return df

            elif loader_type == 'sql':
                print(f"    [SKIP] {name}: Loader SQL requer conexão de banco de dados")
                return pd.DataFrame()

            elif loader_type == 'api':
                print(f"    [SKIP] {name}: Loader API não implementado no script standalone")
                return pd.DataFrame()

            else:
                print(f"    [SKIP] {name}: Loader tipo '{loader_type}' não suportado")
                return pd.DataFrame()

        except Exception as e:
            print(f"    [ERRO] {name}: {e}")
            return pd.DataFrame()

    def _generate_key(self, df: pd.DataFrame, key_config: Dict) -> pd.DataFrame:
        """Gera coluna CHAVE baseado na configuração."""
        df = df.copy()
        key_type = key_config.get('type', 'composite')
        output_col = key_config.get('output_column', 'CHAVE')

        if key_type == 'composite':
            components = key_config.get('components', [])
            separator = key_config.get('separator', '-')
            df[output_col] = df[components[0]].astype(str)
            for comp in components[1:]:
                df[output_col] = df[output_col] + separator + df[comp].astype(str)

        elif key_type == 'column':
            source_col = key_config.get('column', 'CHAVE')
            if source_col != output_col:
                df[output_col] = df[source_col]

        return df

    def _apply_validators(self, df: pd.DataFrame, validators: list) -> tuple:
        """Aplica validadores e retorna (df_valido, df_invalido)."""
        valid_mask = pd.Series(True, index=df.index)

        for validator in validators:
            if not validator.get('enabled', True):
                continue

            v_type = validator.get('type')
            params = validator.get('params', {})

            if v_type == 'required':
                columns = params.get('columns', [])
                for col in columns:
                    if col in df.columns:
                        valid_mask &= df[col].notna() & (df[col] != '')

            elif v_type == 'type_filter':
                column = params.get('column')
                exclude = params.get('exclude', [])
                include = params.get('include', [])

                if column in df.columns:
                    if exclude:
                        valid_mask &= ~df[column].isin(exclude)
                    if include:
                        valid_mask &= df[column].isin(include)

            elif v_type == 'status':
                column = params.get('column')
                include = params.get('include', [])
                exclude = params.get('exclude', [])

                if column in df.columns:
                    if include:
                        valid_mask &= df[column].str.upper().isin([v.upper() for v in include])
                    if exclude:
                        valid_mask &= ~df[column].str.upper().isin([v.upper() for v in exclude])

        return df[valid_mask].copy(), df[~valid_mask].copy()

    def _clean_cpf(self, df: pd.DataFrame, columns: list) -> pd.DataFrame:
        """Limpa colunas de CPF/CNPJ."""
        df = df.copy()
        for col in columns:
            if col in df.columns:
                df[f'{col}_LIMPO'] = df[col].str.replace(r'[.\-/\s]', '', regex=True)
        return df

    def _batimento(self, cliente_df: pd.DataFrame, max_df: pd.DataFrame,
                   cliente_key: str, max_key: str) -> tuple:
        """Executa batimento (anti-join)."""
        chaves_cliente = set(cliente_df[cliente_key])
        chaves_max = set(max_df[max_key])

        novos_chaves = chaves_cliente - chaves_max
        baixas_chaves = chaves_max - chaves_cliente

        novos_df = cliente_df[cliente_df[cliente_key].isin(novos_chaves)].copy()
        baixas_df = max_df[max_df[max_key].isin(baixas_chaves)].copy()

        return novos_df, baixas_df

    def _separar_judicial(self, df: pd.DataFrame, judicial_df: pd.DataFrame,
                          df_cpf_col: str, judicial_cpf_col: str) -> tuple:
        """Separa registros judiciais dos demais."""
        # Limpar CPFs
        df = df.copy()
        df['_CPF_LIMPO'] = df[df_cpf_col].str.replace(r'[.\-/\s]', '', regex=True)

        judicial_cpfs = set(
            judicial_df[judicial_cpf_col].str.replace(r'[.\-/\s]', '', regex=True)
        )

        mask_judicial = df['_CPF_LIMPO'].isin(judicial_cpfs)

        judicial_out = df[mask_judicial].copy()
        normal_out = df[~mask_judicial].copy()

        # Remover coluna temporária
        judicial_out = judicial_out.drop(columns=['_CPF_LIMPO'])
        normal_out = normal_out.drop(columns=['_CPF_LIMPO'])

        return normal_out, judicial_out

    def _export(self, df: pd.DataFrame, name: str, columns: list = None) -> Path:
        """Exporta DataFrame para CSV."""
        output_path = self.output_dir / self.client_name
        output_path.mkdir(parents=True, exist_ok=True)

        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"{name}_{timestamp}.csv"
        filepath = output_path / filename

        if columns:
            df = df[[c for c in columns if c in df.columns]]

        df.to_csv(filepath, sep=';', encoding='utf-8-sig', index=False)
        return filepath

    def run(self, dry_run: bool = False) -> PipelineResult:
        """Executa o pipeline completo."""
        start_time = datetime.now()

        try:
            print(f"\n{'='*60}")
            print(f"PIPELINE: {self.client_name.upper()}")
            print(f"{'='*60}")

            # 1. EXTRAÇÃO
            print("\n[1] EXTRAÇÃO")
            client_source = self.config.get('client_source', {})
            max_source = self.config.get('max_source', {})

            # Carregar cliente
            client_loader = client_source.get('loader', {})
            cliente_df = self._execute_loader(client_loader, 'Cliente')

            # Carregar MAX
            max_loader = max_source.get('loader', {})
            max_df = self._execute_loader(max_loader, 'MAX')

            if cliente_df.empty or max_df.empty:
                return PipelineResult(
                    success=False,
                    client=self.client_name,
                    duration=(datetime.now() - start_time).total_seconds(),
                    error="Dados não disponíveis (loader não suportado ou arquivo não encontrado)"
                )

            # 2. TRATAMENTO
            print("\n[2] TRATAMENTO")

            # Gerar CHAVEs
            client_key_config = client_source.get('key', {})
            max_key_config = max_source.get('key', {})

            # Remover coluna PARCELA duplicada se existir (bug conhecido)
            if 'PARCELA' in cliente_df.columns and 'ID_PARCELA' in cliente_df.columns:
                cliente_df = cliente_df.drop(columns=['PARCELA'])

            cliente_df = self._generate_key(cliente_df, client_key_config)
            max_df = self._generate_key(max_df, max_key_config)
            print(f"    CHAVEs geradas: OK")

            # Limpar CPF
            cpf_cols = ['CPF', 'CPFCNPJ_CLIENTE', 'CPF_CNPJ']
            cliente_df = self._clean_cpf(cliente_df, cpf_cols)
            max_df = self._clean_cpf(max_df, cpf_cols)

            # 3. VALIDAÇÃO
            print("\n[3] VALIDAÇÃO")
            validators = client_source.get('validators', [])
            cliente_df, invalidos = self._apply_validators(cliente_df, validators)
            print(f"    Válidos: {len(cliente_df)}, Inválidos: {len(invalidos)}")

            # 4. BATIMENTO
            print("\n[4] BATIMENTO")
            client_key_col = client_key_config.get('output_column', 'CHAVE')
            max_key_col = max_key_config.get('output_column', 'CHAVE')

            novos_df, baixas_df = self._batimento(cliente_df, max_df, client_key_col, max_key_col)
            print(f"    NOVOS: {len(novos_df)}")
            print(f"    BAIXAS: {len(baixas_df)}")

            # 5. DEVOLUÇÃO (se houver arquivo judicial)
            print("\n[5] DEVOLUÇÃO")
            devolucao_df = pd.DataFrame()
            judicial_path = self.config.get('inputs', {}).get('clientes_judiciais_path')

            if judicial_path:
                try:
                    judicial_df = self._load_file(judicial_path)

                    # Determinar coluna CPF
                    cpf_col_baixas = next((c for c in ['CPFCNPJ_CLIENTE', 'CPF_CNPJ', 'CPF'] if c in baixas_df.columns), None)
                    cpf_col_judicial = next((c for c in ['CPF_CNPJ', 'CPFCNPJ', 'CPF', 'DOCUMENTO'] if c in judicial_df.columns), None)

                    if cpf_col_baixas and cpf_col_judicial:
                        baixas_df, devolucao_df = self._separar_judicial(
                            baixas_df, judicial_df, cpf_col_baixas, cpf_col_judicial
                        )
                        print(f"    DEVOLUÇÃO: {len(devolucao_df)}")
                        print(f"    BAIXAS (final): {len(baixas_df)}")
                    else:
                        print(f"    [SKIP] Colunas CPF não encontradas")
                except Exception as e:
                    print(f"    [SKIP] Erro ao carregar judicial: {e}")
            else:
                print(f"    [SKIP] Arquivo judicial não configurado")

            # 6. ENRIQUECIMENTO
            print("\n[6] ENRIQUECIMENTO")
            timestamp = datetime.now().strftime('%Y-%m-%d')

            # Enriquecer NOVOS
            novos_df['DATA_CADASTRO'] = timestamp

            # Enriquecer BAIXAS
            baixas_df['STATUS_BAIXA'] = '98'
            baixas_df['DATA_BAIXA'] = timestamp

            # Enriquecer DEVOLUÇÃO
            if not devolucao_df.empty:
                devolucao_df['MOTIVO_DEVOLUCAO'] = 'CLIENTE JUDICIAL'
                devolucao_df['DATA_DEVOLUCAO'] = timestamp

            print(f"    Campos adicionados: OK")

            # 7. EXPORTAÇÃO
            if dry_run:
                print("\n[7] EXPORTAÇÃO (dry-run)")
                print(f"    [SKIP] Modo dry-run ativado")
                outputs = {}
            else:
                print("\n[7] EXPORTAÇÃO")
                outputs = {}

                if not novos_df.empty:
                    outputs['novos'] = self._export(novos_df, 'novos')
                    print(f"    NOVOS: {outputs['novos']}")

                if not baixas_df.empty:
                    outputs['baixas'] = self._export(baixas_df, 'baixas')
                    print(f"    BAIXAS: {outputs['baixas']}")

                if not devolucao_df.empty:
                    outputs['devolucao'] = self._export(devolucao_df, 'devolucao')
                    print(f"    DEVOLUÇÃO: {outputs['devolucao']}")

            duration = (datetime.now() - start_time).total_seconds()

            print(f"\n{'='*60}")
            print(f"✓ PIPELINE CONCLUÍDO EM {duration:.2f}s")
            print(f"{'='*60}")

            return PipelineResult(
                success=True,
                client=self.client_name,
                duration=duration,
                novos=len(novos_df),
                baixas=len(baixas_df),
                devolucao=len(devolucao_df),
                outputs=outputs
            )

        except Exception as e:
            duration = (datetime.now() - start_time).total_seconds()
            print(f"\n[ERRO] {e}")
            return PipelineResult(
                success=False,
                client=self.client_name,
                duration=duration,
                error=str(e)
            )


def find_config(client_name: str) -> Optional[Path]:
    """Encontra o arquivo de configuração do cliente."""
    # Caminhos possíveis
    paths = [
        Path(f"unified/configs/clients/{client_name}.yaml"),
        Path(f"configs/clients/{client_name}.yaml"),
        Path(f"{client_name}.yaml"),
    ]

    for path in paths:
        if path.exists():
            return path

    return None


def list_clients() -> list:
    """Lista clientes disponíveis."""
    config_dirs = [
        Path("unified/configs/clients"),
        Path("configs/clients"),
    ]

    clients = []
    for config_dir in config_dirs:
        if config_dir.exists():
            for yaml_file in config_dir.glob("*.yaml"):
                clients.append(yaml_file.stem)

    return sorted(set(clients))


def main():
    parser = argparse.ArgumentParser(description='Pipeline Unificado de Processamento')
    parser.add_argument('client', nargs='?', help='Nome do cliente (ex: emccamp, vic, tabelionato)')
    parser.add_argument('--output-dir', '-o', default='unified/data/output', help='Diretório de saída')
    parser.add_argument('--dry-run', '-n', action='store_true', help='Executar sem exportar')
    parser.add_argument('--list', '-l', action='store_true', help='Listar clientes disponíveis')

    args = parser.parse_args()

    if args.list:
        print("Clientes disponíveis:")
        for client in list_clients():
            print(f"  - {client}")
        return

    if not args.client:
        parser.print_help()
        print("\nClientes disponíveis:")
        for client in list_clients():
            print(f"  - {client}")
        sys.exit(1)

    config_path = find_config(args.client)
    if not config_path:
        print(f"[ERRO] Configuração não encontrada para cliente: {args.client}")
        print("\nClientes disponíveis:")
        for client in list_clients():
            print(f"  - {client}")
        sys.exit(1)

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    pipeline = UnifiedPipeline(config_path, output_dir)
    result = pipeline.run(dry_run=args.dry_run)

    if result.success:
        print(f"\nResumo:")
        print(f"  NOVOS: {result.novos}")
        print(f"  BAIXAS: {result.baixas}")
        print(f"  DEVOLUÇÃO: {result.devolucao}")
        sys.exit(0)
    else:
        print(f"\n[FALHA] {result.error}")
        sys.exit(1)


if __name__ == '__main__':
    main()
