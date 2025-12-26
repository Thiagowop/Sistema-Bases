#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Extração EMCCAMP - API TOTVS
Extrai a base EMCCAMP via API TOTVS.
"""
import os
import sys
import time
import zipfile
from datetime import datetime, date, timedelta
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
    print("  EXTRAÇÃO EMCCAMP - BASE API TOTVS")
    print("=" * 60)
    print()
    
    inicio = time.time()
    
    # Configuração
    output_dir = ROOT / "data" / "input" / "emccamp"
    output_dir.mkdir(parents=True, exist_ok=True)
    output_file = output_dir / "Emccamp.zip"
    
    print(f"[INFO] Output: {output_file}")
    print()
    
    # Verificar variáveis de ambiente
    base_url = os.getenv("EMCCAMP_API_URL")
    user = os.getenv("EMCCAMP_API_USER")
    password = os.getenv("EMCCAMP_API_PASSWORD")
    data_inicio = os.getenv("EMCCAMP_DATA_VENCIMENTO_INICIAL")
    data_fim_env = os.getenv("EMCCAMP_DATA_VENCIMENTO_FINAL", "AUTO").strip()
    
    if not base_url:
        print("[ERRO] Variável EMCCAMP_API_URL não configurada no .env")
        sys.exit(1)
    
    if not user or not password:
        print("[ERRO] Variáveis EMCCAMP_API_USER/PASSWORD não configuradas no .env")
        sys.exit(1)
        
    if not data_inicio:
        print("[ERRO] Variável EMCCAMP_DATA_VENCIMENTO_INICIAL não configurada no .env")
        sys.exit(1)
    
    # Calcular data final
    if not data_fim_env or data_fim_env.upper() == "AUTO":
        data_fim_dt = date.today() - timedelta(days=6)
        data_fim = data_fim_dt.strftime("%Y-%m-%d")
        data_fim_info = f"{data_fim} (AUTO: hoje-6)"
    else:
        data_fim = data_fim_env
        data_fim_info = data_fim
    
    print("[INFO] Configuração:")
    print(f"  API URL: {base_url[:50]}...")
    print(f"  User: {user}")
    print(f"  Data Inicial: {data_inicio}")
    print(f"  Data Final: {data_fim_info}")
    print()
    
    # Montar parâmetros
    parametros = [f"DATA_VENCIMENTO_INICIAL={data_inicio}"]
    if data_fim:
        parametros.append(f"DATA_VENCIMENTO_FINAL={data_fim}")
    parametros_str = ";".join(parametros)
    
    print("[INFO] Conectando à API TOTVS...")
    
    try:
        resp = requests.get(
            base_url, 
            params={"parameters": parametros_str}, 
            auth=(user, password), 
            timeout=(15, 300)
        )
        resp.raise_for_status()
        
        data = resp.json()
        df = pd.DataFrame(data)
        
        if df.empty:
            print("[AVISO] API retornou nenhum registro")
        
        # Salvar como ZIP
        print(f"\n[INFO] Salvando {len(df):,} registros...")
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        csv_name = f"Emccamp_{timestamp}.csv"
        
        with zipfile.ZipFile(output_file, 'w', zipfile.ZIP_DEFLATED) as zf:
            csv_content = df.to_csv(index=False, sep=';', encoding='utf-8-sig')
            zf.writestr(csv_name, csv_content)
        
        tempo = time.time() - inicio
        
        print()
        print("[RESULTADO] Extração concluída:")
        print(f"  Data: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}")
        print(f"  Arquivo: {output_file}")
        print(f"  Registros: {len(df):,}")
        print(f"  Colunas: {len(df.columns)}")
        print(f"  Tempo: {tempo:.2f}s")
        print()
        print("[OK] EMCCAMP Base extraída com sucesso!")
        
    except requests.exceptions.RequestException as e:
        print(f"\n[ERRO] Falha na requisição HTTP: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"\n[ERRO] Falha na extração: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
