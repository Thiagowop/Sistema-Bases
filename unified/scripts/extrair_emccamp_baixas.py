#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Extração EMCCAMP Baixas - API TOTVS
Extrai dados de baixas/pagamentos via API TOTVS (endpoint CANDIOTTO.002).
"""
import os
import sys
import time
import zipfile
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests

# Setup path
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv
load_dotenv(ROOT / ".env")


def main():
    print("=" * 60)
    print("  EXTRAÇÃO EMCCAMP BAIXAS - API TOTVS")
    print("=" * 60)
    print()
    
    inicio = time.time()
    
    # Configuração
    output_dir = ROOT / "data" / "input" / "baixas"
    output_dir.mkdir(parents=True, exist_ok=True)
    output_file = output_dir / "baixa_emccamp.zip"
    
    print(f"[INFO] Output: {output_file}")
    print()
    
    # Verificar variáveis de ambiente
    base_url = os.getenv("TOTVS_BASE_URL")
    user = os.getenv("TOTVS_USER")
    password = os.getenv("TOTVS_PASS")
    
    if not base_url:
        print("[ERRO] Variável TOTVS_BASE_URL não configurada no .env")
        sys.exit(1)
    
    if not user or not password:
        print("[ERRO] Variáveis TOTVS_USER/TOTVS_PASS não configuradas no .env")
        sys.exit(1)
    
    # Montar URL completa (endpoint diferente: CANDIOTTO.002)
    endpoint = "/api/framework/v1/consultaSQLServer/RealizaConsulta/CANDIOTTO.002/0/X"
    full_url = f"{base_url}{endpoint}"
    
    print("[INFO] Configuração:")
    print(f"  API URL: {full_url[:60]}...")
    print(f"  User: {user}")
    print()
    
    print("[INFO] Conectando à API TOTVS...")
    
    try:
        resp = requests.get(
            full_url, 
            auth=(user, password), 
            timeout=(15, 300)
        )
        resp.raise_for_status()
        
        data = resp.json()
        df = pd.DataFrame(data)
        
        if df.empty:
            print("[AVISO] API retornou nenhum registro")
            df = pd.DataFrame(columns=["NUM_VENDA", "ID_PARCELA", "HONORARIO_BAIXADO", "DATA_RECEBIMENTO", "VALOR_RECEBIDO"])
        
        # Normalizar colunas
        df.columns = [str(col).upper() for col in df.columns]
        
        # Filtrar registros com HONORARIO_BAIXADO != 0
        if "HONORARIO_BAIXADO" in df.columns:
            df["HONORARIO_BAIXADO"] = pd.to_numeric(df["HONORARIO_BAIXADO"], errors="coerce").fillna(0)
            df_filtrado = df[df["HONORARIO_BAIXADO"] != 0].copy()
        else:
            df_filtrado = df.copy()
        
        # Gerar CHAVE
        if "NUM_VENDA" in df_filtrado.columns and "ID_PARCELA" in df_filtrado.columns:
            df_filtrado["CHAVE"] = (
                df_filtrado["NUM_VENDA"].astype(str).str.strip()
                + "-"
                + df_filtrado["ID_PARCELA"].astype(str).str.strip()
            )
        
        # Salvar como ZIP
        print(f"\n[INFO] Salvando {len(df_filtrado):,} registros (filtrados de {len(df):,})...")
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        csv_name = f"baixa_emccamp_{timestamp}.csv"
        
        with zipfile.ZipFile(output_file, 'w', zipfile.ZIP_DEFLATED) as zf:
            csv_content = df_filtrado.to_csv(index=False, sep=';', encoding='utf-8-sig')
            zf.writestr(csv_name, csv_content)
        
        tempo = time.time() - inicio
        
        print()
        print("[RESULTADO] Extração concluída:")
        print(f"  Data: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}")
        print(f"  Arquivo: {output_file}")
        print(f"  Registros originais: {len(df):,}")
        print(f"  Registros filtrados: {len(df_filtrado):,}")
        print(f"  Colunas: {len(df_filtrado.columns)}")
        print(f"  Tempo: {tempo:.2f}s")
        print()
        print("[OK] EMCCAMP Baixas extraídas com sucesso!")
        
    except requests.exceptions.RequestException as e:
        print(f"\n[ERRO] Falha na requisição HTTP: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"\n[ERRO] Falha na extração: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
