#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Extração de anexos do Gmail via IMAP com relatório amigável."""

from __future__ import annotations

import argparse
import email
import imaplib
import os
import sys
import time
import unicodedata
import zipfile
from datetime import datetime
from email.header import decode_header, make_header
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import pandas as pd
from dotenv import load_dotenv

BASE = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BASE))
sys.path.insert(0, str(BASE / "src"))
load_dotenv(BASE / ".env")

from src.config.loader import load_cfg  # type: ignore

PROJECT_ROOT = Path(__file__).parent.parent


def _dec(header: Optional[str]) -> str:
    if not header:
        return ""
    try:
        return str(make_header(decode_header(header)))
    except Exception:
        try:
            return header.encode("latin1").decode("utf-8")
        except Exception:
            return str(header)


def _contar_registros(arquivo: Path) -> Tuple[Optional[int], Optional[str]]:
    try:
        if arquivo.suffix.lower() == ".zip":
            with zipfile.ZipFile(arquivo) as zf:
                csvs = [name for name in zf.namelist() if name.lower().endswith(".csv")]
                if not csvs:
                    return None, None
                alvo = csvs[0]
                with zf.open(alvo) as fh:
                    df = pd.read_csv(fh, sep=";", encoding="utf-8-sig")
                return len(df), alvo
        if arquivo.suffix.lower() in {".csv", ".txt"}:
            df = pd.read_csv(arquivo, sep=";", encoding="utf-8-sig")
            return len(df), arquivo.name
    except Exception:
        return None, None
    return None, None


def _normalize_text(value: Optional[str]) -> str:
    if not value:
        return ""
    normalized = unicodedata.normalize("NFKD", value)
    stripped = "".join(ch for ch in normalized if not unicodedata.combining(ch))
    return stripped.lower().strip()


def _coerce_extensions(raw: Any) -> set[str]:
    if not raw:
        return set()
    if isinstance(raw, str):
        items = [raw]
    else:
        try:
            items = list(raw)
        except TypeError:
            items = [str(raw)]
    cleaned = set()
    for item in items:
        if not item:
            continue
        ext = str(item).strip().lower()
        if not ext:
            continue
        if not ext.startswith('.'):
            ext = f'.{ext}'
        cleaned.add(ext)
    return cleaned


def _available_email_profiles(config: Dict[str, Any]) -> list[str]:
    perfis = []
    for chave, valor in config.items():
        if isinstance(valor, dict) and ('imap_server' in valor or 'email_sender' in valor):
            perfis.append(chave)
    return sorted(perfis)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Extração de anexos via IMAP (Gmail).')
    parser.add_argument(
        '--profile',
        default='email',
        help='Seção do config.yaml com as configurações de e-mail (default: email).',
    )
    parser.add_argument(
        '--output',
        help='Sobrescreve o nome do arquivo de saída definido no config.yaml.',
    )
    return parser.parse_args()


def _load_config() -> Dict[str, Any]:
    try:
        return load_cfg()
    except Exception as exc:  # pragma: no cover - falha de bootstrap
        print(f"[ERRO] Falha ao carregar config: {exc}")
        sys.exit(1)


def main() -> None:
    print("=" * 60)
    print("     EXTRACAO DE ANEXOS – GMAIL IMAP (READ-ONLY)")
    print("=" * 60)
    print()

    args = parse_args()
    config = _load_config()
    email_cfg = config.get(args.profile, {})

    if not isinstance(email_cfg, dict) or 'imap_server' not in email_cfg:
        perfis = _available_email_profiles(config)
        print(
            f"[ERRO] Configuração de e-mail '{args.profile}' não encontrada em config.yaml. "
            f"Perfis disponíveis: {', '.join(perfis) or 'nenhum perfil com parâmetros de e-mail'}"
        )
        sys.exit(1)

    email_user = os.getenv("EMAIL_USER")
    email_app_password = os.getenv("EMAIL_APP_PASSWORD")

    imap_server = email_cfg.get("imap_server", "imap.gmail.com")
    imap_folder = email_cfg.get("imap_folder", "INBOX")
    email_sender = email_cfg.get("email_sender", "").strip()
    email_subject_keyword = email_cfg.get("email_subject_keyword", "").strip()
    attachment_filename = email_cfg.get("attachment_filename", "").strip()
    attachment_keyword = email_cfg.get("attachment_keyword", "").strip()
    attachment_extensions = _coerce_extensions(email_cfg.get("attachment_extensions"))
    download_dir = PROJECT_ROOT / email_cfg.get("download_dir", "data/input/vic")
    output_filename = args.output or email_cfg.get("output_filename", "").strip()

    if not output_filename:
        output_filename = "anexo_email.zip"

    download_dir.mkdir(parents=True, exist_ok=True)

    inicio = time.time()

    if not email_user or not email_app_password:
        print("[ERRO] Variáveis EMAIL_USER/EMAIL_APP_PASSWORD ausentes no .env")
        sys.exit(1)
    if len(email_app_password.strip()) != 16:
        print("[AVISO] App Password deve ter 16 caracteres (sem espaços).")

    print(f"[INFO] Perfil de configuração utilizado: {args.profile}")
    print(f"[INFO] Diretório de download: {download_dir}")

    mail: Optional[imaplib.IMAP4_SSL] = None
    anexos_encontrados = 0
    anexos_baixados = 0
    arquivo_final: Optional[Path] = None
    email_info: dict[str, str] = {}

    sender_filter_norm = _normalize_text(email_sender)
    subject_filter_norm = _normalize_text(email_subject_keyword)
    filename_norm = _normalize_text(attachment_filename)
    keyword_norm = _normalize_text(attachment_keyword)

    try:
        mail = imaplib.IMAP4_SSL(imap_server, 993)
        mail.login(email_user, email_app_password)

        typ, _ = mail.select(imap_folder)
        if typ != "OK":
            print(f"[ERRO] Não foi possível selecionar a pasta {imap_folder}")
            sys.exit(1)

        print(
            "[INFO] Filtros aplicados | FROM='{}' SUBJECT~='{}'".format(
                email_sender or '*', email_subject_keyword or '*'
            )
        )

        def buscar(*criteria: str) -> list[bytes]:
            if not criteria:
                return []
            status, data = mail.search("UTF-8", *criteria)
            if status != "OK" or not data:
                return []
            blob = data[0]
            return blob.split() if blob else []

        ids_sender = buscar("FROM", f'"{email_sender}"') if email_sender else []
        ids_subject = buscar("SUBJECT", f'"{email_subject_keyword}"') if email_subject_keyword else []
        ids_combined: list[bytes] = []
        if email_sender and email_subject_keyword:
            ids_combined = buscar(f'(FROM "{email_sender}" SUBJECT "{email_subject_keyword}")')

        sorter = lambda items: sorted(items, key=lambda b: int(b), reverse=True)
        ids_sender = sorter(ids_sender)
        ids_subject = sorter(ids_subject)
        ids_combined = sorter(ids_combined)

        print("[INFO] Resultado das buscas:")
        print(f"        Remetente: {len(ids_sender)} e-mails")
        print(f"        Assunto  : {len(ids_subject)} e-mails")
        print(f"        Combinada: {len(ids_combined)} e-mails")

        if ids_combined:
            ids_to_process = [ids_combined[0]]
            print("[INFO] Usando a busca combinada (mais recente).")
        elif ids_sender:
            ids_to_process = [ids_sender[0]]
            print("[INFO] Usando a busca por remetente (mais recente).")
        elif ids_subject:
            ids_to_process = [ids_subject[0]]
            print("[INFO] Usando a busca por assunto (mais recente).")
        else:
            print("[ERRO] Nenhum e-mail encontrado com os critérios informados.")
            return

        for eid in ids_to_process:
            status, msg_data = mail.fetch(eid, "(RFC822)")
            if status != "OK":
                continue
            msg_bytes = next((part[1] for part in msg_data if isinstance(part, tuple)), None)
            if not msg_bytes:
                continue

            msg = email.message_from_bytes(msg_bytes)
            from_h = _dec(msg.get("From", ""))
            subj_h = _dec(msg.get("Subject", ""))
            date_h = _dec(msg.get("Date", ""))

            email_info = {
                "remetente": from_h,
                "assunto": subj_h,
                "data": date_h,
            }

            print("\n[EMAIL] Selecionado:")
            for chave, valor in email_info.items():
                print(f"        {chave.title()}: {valor or '-'}")

            from_norm = _normalize_text(from_h)
            if sender_filter_norm and sender_filter_norm not in from_norm:
                print("        [AVISO] Remetente não confere com o filtro. Ignorando.")
                continue
            subj_norm = _normalize_text(subj_h)
            if subject_filter_norm and subject_filter_norm not in subj_norm:
                print("        [AVISO] Assunto não contém a palavra-chave. Ignorando.")
                continue

            for part in msg.walk():
                if part.get_content_maintype() == "multipart":
                    continue
                if part.get("Content-Disposition") is None:
                    continue

                filename = _dec(part.get_filename() or "")
                if not filename:
                    continue
                filename_candidate_norm = _normalize_text(filename)
                if filename_norm and filename_candidate_norm != filename_norm:
                    continue
                if keyword_norm and keyword_norm not in filename_candidate_norm:
                    continue
                if attachment_extensions:
                    extensao = Path(filename).suffix.lower()
                    if extensao not in attachment_extensions:
                        continue

                anexos_encontrados += 1
                payload = part.get_payload(decode=True)
                if payload is None:
                    continue

                filepath = download_dir / output_filename
                with open(filepath, "wb") as fh:
                    fh.write(payload)
                anexos_baixados += 1
                arquivo_final = filepath
                print(f"        [OK] Anexo salvo em: {filepath}")

        mail.logout()

    except imaplib.IMAP4.error as exc:
        elapsed = time.time() - inicio
        print(f"[ERRO] IMAP: {exc}")
        print(f"[INFO] Tempo de execução: {elapsed:.2f} segundos")
        sys.exit(1)
    except Exception as exc:
        elapsed = time.time() - inicio
        print(f"[ERRO] Falha durante a extração: {exc}")
        print(f"[INFO] Tempo de execução: {elapsed:.2f} segundos")
        sys.exit(1)
    finally:
        try:
            if mail is not None:
                mail.logout()
        except Exception:
            pass

    elapsed = time.time() - inicio

    print("\n[RESUMO] Extração de anexos:")
    print(f"        Anexos encontrados: {anexos_encontrados}")
    print(f"        Anexos baixados   : {anexos_baixados}")

    if anexos_baixados == 0 or not arquivo_final:
        print(f"[INFO] Tempo de execução: {elapsed:.2f} segundos")
        print("[AVISO] Nenhum anexo foi salvo.")
        return

    # Validação de tamanho do arquivo
    tamanho_mb = arquivo_final.stat().st_size / (1024 * 1024)
    validation_cfg = email_cfg.get('validation', {})
    min_size_mb = validation_cfg.get('min_file_size_mb', 0)
    
    if min_size_mb > 0 and tamanho_mb < min_size_mb:
        print(f"\n{'='*60}")
        print("[ERRO CRÍTICO] BASE COM INCONFORMIDADE DETECTADA")
        print(f"{'='*60}")
        print(f"[ERRO] O arquivo baixado possui tamanho MUITO ABAIXO do esperado!")
        print(f"[ERRO] Tamanho recebido: {tamanho_mb:.2f} MB")
        print(f"[ERRO] Tamanho mínimo esperado: {min_size_mb:.2f} MB")
        print(f"[ERRO] Arquivo: {arquivo_final}")
        print(f"[ERRO] ")
        print(f"[ERRO] POSSÍVEIS CAUSAS:")
        print(f"[ERRO]   - Base enviada com formato incorreto")
        print(f"[ERRO]   - Arquivo corrompido ou incompleto")
        print(f"[ERRO]   - Email com anexo errado")
        print(f"[ERRO] ")
        print(f"[ERRO] AÇÃO NECESSÁRIA:")
        print(f"[ERRO]   - Verificar manualmente o arquivo baixado")
        print(f"[ERRO]   - Contatar o remetente: {email_info.get('remetente', 'N/A')}")
        print(f"[ERRO]   - Solicitar reenvio da base correta")
        print(f"{'='*60}")
        
        # Registrar no log de erros
        logs_dir = PROJECT_ROOT / "data" / "logs"
        logs_dir.mkdir(parents=True, exist_ok=True)
        log_file = logs_dir / "extracao_email_erros.log"
        
        with open(log_file, 'a', encoding='utf-8-sig') as f:
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            f.write(f"\n{'='*80}\n")
            f.write(f"[{timestamp}] ERRO CRITICO - BASE COM INCONFORMIDADE\n")
            f.write(f"{'='*80}\n")
            f.write(f"Arquivo: {arquivo_final}\n")
            f.write(f"Tamanho recebido: {tamanho_mb:.4f} MB\n")
            f.write(f"Tamanho minimo esperado: {min_size_mb:.2f} MB\n")
            f.write(f"Diferenca: {min_size_mb - tamanho_mb:.4f} MB abaixo do esperado\n")
            f.write(f"\nInformacoes do e-mail:\n")
            f.write(f"  Remetente: {email_info.get('remetente', 'N/A')}\n")
            f.write(f"  Assunto: {email_info.get('assunto', 'N/A')}\n")
            f.write(f"  Data: {email_info.get('data', 'N/A')}\n")
            f.write(f"\nAcao necessaria: Verificar arquivo e solicitar reenvio da base\n")
            f.write(f"{'='*80}\n")
        
        print(f"\n[INFO] Erro registrado em: {log_file}")
        print(f"[INFO] Tempo de execução: {elapsed:.2f} segundos")
        print("\n[FALHA] Extração concluída COM ERRO - Base com tamanho inválido.")
        sys.exit(1)

    registros, detalhe = _contar_registros(arquivo_final)
    mtime = datetime.fromtimestamp(arquivo_final.stat().st_mtime)

    print("\n[RESULTADO] Arquivo salvo:")
    print(f"        Caminho : {arquivo_final}")
    print(f"        Tamanho : {tamanho_mb:.2f} MB")
    print(f"        Atualizado em : {mtime:%d/%m/%Y %H:%M:%S}")
    if detalhe:
        print(f"        Conteúdo analisado: {detalhe}")
    if registros is not None:
        print(f"        Registros encontrados: {registros:,}")
    else:
        print("        Registros encontrados: não foi possível calcular")

    if email_info:
        print("\n[RESULTADO] E-mail de origem:")
        print(f"        Remetente: {email_info.get('remetente', '-')}")
        print(f"        Assunto  : {email_info.get('assunto', '-')}")
        print(f"        Data/hora: {email_info.get('data', '-')}")

    print(f"\n[INFO] Tempo de execução: {elapsed:.2f} segundos")
    print("[OK] Extração concluída com sucesso.")


if __name__ == "__main__":
    main()
