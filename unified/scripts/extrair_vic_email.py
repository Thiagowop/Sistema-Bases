#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Extração VIC - Email
Extrai a base VIC do anexo de email (candiotto.zip).
"""
import os
import sys
import time
import zipfile
from datetime import datetime
from pathlib import Path

# Setup path
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv
load_dotenv(ROOT / ".env")

from src.loaders.email_loader import EmailLoader
from src.core.schemas import LoaderConfig, LoaderType


def main():
    print("=" * 60)
    print("  EXTRAÇÃO VIC - BASE CLIENTE (EMAIL)")
    print("=" * 60)
    print()
    
    inicio = time.time()
    
    # Configuração
    output_dir = ROOT / "data" / "input" / "vic"
    output_dir.mkdir(parents=True, exist_ok=True)
    output_file = output_dir / "VicCandiotto.zip"
    
    print(f"[INFO] Output: {output_file}")
    print()
    
    # Verificar variáveis de ambiente
    email_user = os.getenv("EMAIL_USER")
    email_password = os.getenv("EMAIL_APP_PASSWORD")
    imap_server = os.getenv("IMAP_SERVER")
    sender = os.getenv("VIC_EMAIL_SENDER")
    subject = os.getenv("VIC_EMAIL_SUBJECT")
    attachment = os.getenv("VIC_ATTACHMENT_FILENAME")
    
    if not all([email_user, email_password]):
        print("[ERRO] Variáveis EMAIL_USER/EMAIL_APP_PASSWORD não configuradas no .env")
        sys.exit(1)
    
    print("[INFO] Configuração:")
    print(f"  Email: {email_user}")
    print(f"  IMAP: {imap_server}")
    print(f"  Remetente: {sender}")
    print(f"  Assunto: {subject}")
    print(f"  Anexo: {attachment}")
    print()
    
    # Usar EmailLoader
    config = LoaderConfig(
        type=LoaderType.EMAIL,
        params={
            "server": imap_server,
            "email": email_user,
            "password": email_password,
            "sender_filter": sender,
            "subject_filter": subject,
            "attachment_pattern": attachment,
            "days_back": 30,
            "encoding": "utf-8-sig",
            "separator": ";",
        }
    )
    
    print("[INFO] Conectando ao servidor IMAP...")
    loader = EmailLoader(config, None)
    result = loader.load()
    
    if "error" in result.metadata:
        print(f"\n[ERRO] {result.metadata['error']}")
        sys.exit(1)
    
    if result.data.empty:
        print("[ERRO] Nenhum dado extraído")
        sys.exit(1)
    
    # Salvar como ZIP
    print(f"\n[INFO] Salvando {len(result.data):,} registros...")
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_name = f"VicCandiotto_{timestamp}.csv"
    
    with zipfile.ZipFile(output_file, 'w', zipfile.ZIP_DEFLATED) as zf:
        csv_content = result.data.to_csv(index=False, sep=';', encoding='utf-8-sig')
        zf.writestr(csv_name, csv_content)
    
    tempo = time.time() - inicio
    
    print()
    print("[RESULTADO] Extração concluída:")
    print(f"  Data: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}")
    print(f"  Arquivo: {output_file}")
    print(f"  Registros: {len(result.data):,}")
    print(f"  Colunas: {len(result.data.columns)}")
    print(f"  Tempo: {tempo:.2f}s")
    print()
    print("[OK] VIC Email extraído com sucesso!")


if __name__ == "__main__":
    main()
