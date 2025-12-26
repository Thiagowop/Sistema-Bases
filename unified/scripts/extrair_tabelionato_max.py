#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Extração MAX Tabelionato - SQL Server
Extrai a base MAX do SQL Server específica para Tabelionato.
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

from src.loaders.sql_loader import SQLLoader
from src.core.schemas import LoaderConfig, LoaderType

# Query SQL para extração Tabelionato MAX
# MoClientesID = 2746 (Tabelionato)
# Campanhas: 60, 80, 96
SQL_TABELIONATO_MAX = """
SELECT DISTINCT
    dbo.RetornaNomeCampanha(MoCampanhasID,1) AS 'CAMPANHA',
    dbo.RetornaNomeRazaoSocial(MoClientesID) AS 'CREDOR',
    dbo.RetornaCPFCNPJ(MoClientesID,1) AS 'CNPJ_CREDOR',
    dbo.RetornaCPFCNPJ(MoInadimplentesID,1) AS 'CPFCNPJ_CLIENTE',
    dbo.RetornaNomeRazaoSocial(MoInadimplentesID) AS 'NOME_RAZAO_SOCIAL',
    MoContrato AS 'NUMERO_CONTRATO',
    MoNumeroDocumento AS 'PARCELA',
    Movimentacoes_ID AS 'Movimentacoes_ID',
    MoDataVencimento AS 'VENCIMENTO',
    MoValorDocumento AS 'VALOR',
    dbo.RetornaStatusMovimentacao(MoStatusMovimentacao) AS 'STATUS_TITULO'
FROM Movimentacoes
    INNER JOIN Pessoas ON MoInadimplentesID = Pessoas_ID
WHERE
    (MoStatusMovimentacao = 0 OR MoStatusMovimentacao = 1)
    AND MoClientesID = 2746
    AND MoOrigemMovimentacao in ('C', 'I')
    AND MoCampanhasID IN (60,80,96)
ORDER BY dbo.RetornaCPFCNPJ(MoInadimplentesID,1), MoDataVencimento ASC
"""


def main():
    print("=" * 60)
    print("  EXTRAÇÃO TABELIONATO MAX - BASE SQL SERVER")
    print("=" * 60)
    print()
    
    inicio = time.time()
    
    # Configuração
    output_dir = ROOT / "data" / "input" / "max"
    output_dir.mkdir(parents=True, exist_ok=True)
    output_file = output_dir / "MaxSmart_Tabelionato.zip"
    
    print(f"[INFO] Output: {output_file}")
    print()
    
    # Verificar variáveis de ambiente
    server = os.getenv("MSSQL_SERVER_STD")
    database = os.getenv("MSSQL_DATABASE_STD")
    username = os.getenv("MSSQL_USER_STD")
    password = os.getenv("MSSQL_PASSWORD_STD")
    
    if not all([server, database]):
        print("[ERRO] Variáveis MSSQL_SERVER_STD/MSSQL_DATABASE_STD não configuradas no .env")
        sys.exit(1)
    
    print("[INFO] Configuração:")
    print(f"  Server: {server}")
    print(f"  Database: {database}")
    print(f"  User: {username}")
    print(f"  Cliente: TABELIONATO (MoClientesID = 2746)")
    print(f"  Campanhas: 60, 80, 96")
    print()
    
    # Usar SQLLoader
    config = LoaderConfig(
        type=LoaderType.SQL,
        params={
            "server": server,
            "database": database,
            "username": username,
            "password": password,
            "query": SQL_TABELIONATO_MAX,
        }
    )
    
    print("[INFO] Conectando ao SQL Server...")
    loader = SQLLoader(config, None)
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
    csv_name = f"MaxSmart_Tabelionato_{timestamp}.csv"
    
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
    print("[OK] TABELIONATO MAX extraído com sucesso!")


if __name__ == "__main__":
    main()
