#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Extração Judicial - AUTOJUR + MAX Smart
Extrai dados judiciais de dois bancos diferentes e combina em um único arquivo.
"""
import os
import sys
import time
import zipfile
from datetime import datetime
from pathlib import Path

import pandas as pd
import pyodbc

# Setup path
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv
load_dotenv(ROOT / ".env")


# Query AUTOJUR - Banco Candiotto/Autojur
SQL_AUTOJUR = """
SELECT DISTINCT 
    [cpf_cnpj_parte_contraria_principal] as CPF_CNPJ,
    'AUTOJUR' as ORIGEM
FROM [Autojur].[dbo].[Pastas_New] 
WHERE 
    grupo_empresarial = 'Vic Engenharia' 
    AND numero_cnj <> '' 
    AND numero_cnj <> 'none' 
    AND cpf_cnpj_parte_contraria_principal <> '' 
    AND cpf_cnpj_parte_contraria_principal IS NOT NULL
"""

# Query MAX Smart Judicial - Banco STD
SQL_MAXSMART_JUDICIAL = """
SELECT DISTINCT 
    dbo.RetornaCPFCNPJ(MoInadimplentesID,1) as CPF_CNPJ,
    'MAX_SMART' as ORIGEM
FROM Movimentacoes 
WHERE 
    MoCampanhasID = 4
    AND dbo.RetornaCPFCNPJ(MoInadimplentesID,1) IS NOT NULL
    AND dbo.RetornaCPFCNPJ(MoInadimplentesID,1) <> ''
"""


def executar_consulta(server: str, database: str, username: str, password: str, query: str, descricao: str) -> pd.DataFrame:
    """Executa consulta SQL e retorna DataFrame."""
    print(f"[EXEC] {descricao}...")
    
    try:
        conn_str = (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={server};"
            f"DATABASE={database};"
            f"UID={username};"
            f"PWD={password};"
            f"TrustServerCertificate=yes;"
        )
        conn = pyodbc.connect(conn_str, timeout=30)
        df = pd.read_sql(query, conn, dtype=str)
        conn.close()
        
        print(f"[OK] {descricao}: {len(df):,} registros")
        return df
        
    except Exception as e:
        print(f"[ERRO] {descricao}: {e}")
        return pd.DataFrame(columns=["CPF_CNPJ", "ORIGEM"])


def main():
    print("=" * 60)
    print("  EXTRAÇÃO DE DADOS JUDICIAIS")
    print("=" * 60)
    print()
    
    inicio = time.time()
    
    # Configuração
    output_dir = ROOT / "data" / "input" / "judicial"
    output_dir.mkdir(parents=True, exist_ok=True)
    output_file = output_dir / "ClientesJudiciais.zip"
    
    print(f"[INFO] Output: {output_file}")
    print()
    
    # Configurações dos bancos
    # Banco STD (principal)
    server_std = os.getenv("MSSQL_SERVER_STD")
    database_std = os.getenv("MSSQL_DATABASE_STD")
    user_std = os.getenv("MSSQL_USER_STD")
    pass_std = os.getenv("MSSQL_PASSWORD_STD")
    
    # Banco Candiotto (Autojur)
    server_candiotto = os.getenv("MSSQL_SERVER_CANDIOTTO")
    database_candiotto = os.getenv("MSSQL_DATABASE_CANDIOTTO")
    user_candiotto = os.getenv("MSSQL_USER_CANDIOTTO")
    pass_candiotto = os.getenv("MSSQL_PASSWORD_CANDIOTTO")
    
    # Extrair AUTOJUR
    df_autojur = executar_consulta(
        server_candiotto, database_candiotto, user_candiotto, pass_candiotto,
        SQL_AUTOJUR, "Consulta AUTOJUR"
    )
    
    # Extrair MAX Smart Judicial
    df_max = executar_consulta(
        server_std, database_std, user_std, pass_std,
        SQL_MAXSMART_JUDICIAL, "Consulta MAX Smart Judicial"
    )
    
    # Combinar e remover duplicatas
    print()
    print("[INFO] Combinando resultados...")
    print(f"       AUTOJUR:   {len(df_autojur):,} registros")
    print(f"       MAX_SMART: {len(df_max):,} registros")
    
    combinados = pd.concat([df_autojur, df_max], ignore_index=True)
    
    if 'CPF_CNPJ' in combinados.columns:
        combinados['_CPF_DIGITO'] = combinados['CPF_CNPJ'].astype(str).str.replace(r"[^0-9]", "", regex=True)
        unicos = combinados.drop_duplicates(subset=['_CPF_DIGITO'], keep='first')
        unicos = unicos.drop(columns=['_CPF_DIGITO'])
    else:
        unicos = combinados.drop_duplicates(subset=['CPF_CNPJ'], keep='first')
    
    removidos = len(combinados) - len(unicos)
    print(f"[INFO] Duplicatas removidas: {removidos:,}")
    print(f"[INFO] Total único: {len(unicos):,} registros")
    
    if unicos.empty:
        print("\n[AVISO] Nenhum dado judicial encontrado para extrair.")
        return
    
    # Salvar como ZIP
    print(f"\n[INFO] Salvando {len(unicos):,} registros...")
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_name = f"ClientesJudiciais_{timestamp}.csv"
    
    with zipfile.ZipFile(output_file, 'w', zipfile.ZIP_DEFLATED) as zf:
        csv_content = unicos.to_csv(index=False, sep=';', encoding='utf-8-sig')
        zf.writestr(csv_name, csv_content)
    
    tempo = time.time() - inicio
    
    print()
    print("[RESULTADO] Extração concluída:")
    print(f"  Data: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}")
    print(f"  Arquivo: {output_file}")
    print(f"  Registros únicos: {len(unicos):,}")
    print(f"  Tempo: {tempo:.2f}s")
    print()
    print("[OK] Dados judiciais extraídos com sucesso!")


if __name__ == "__main__":
    main()
