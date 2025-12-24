#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Conexões SQL Server - Projeto VIC

Classes e funções para conectar com bancos SQL Server.
Implementa Fail-Fast para credenciais ausentes.
"""

import pyodbc
import pandas as pd
from pathlib import Path
import os
from datetime import datetime
from typing import Optional
from dotenv import load_dotenv
import warnings

# Suprimir avisos do pandas sobre conexões DBAPI2
warnings.filterwarnings('ignore', message='pandas only supports SQLAlchemy connectable')

def get_std_connection():
    """Retorna conexão com banco STD2016."""
    # Carregar variáveis de ambiente
    env_path = Path(__file__).parent.parent.parent / '.env'
    if env_path.exists():
        load_dotenv(env_path)
    
    # Fail-Fast: sem defaults perigosos
    server = os.getenv('MSSQL_SERVER_STD')
    if not server:
        raise RuntimeError("MSSQL_SERVER_STD ausente (.env)")
        
    database = os.getenv('MSSQL_DATABASE_STD')
    if not database:
        raise RuntimeError("MSSQL_DATABASE_STD ausente (.env)")
        
    username = os.getenv('MSSQL_USER_STD')
    if not username:
        raise RuntimeError("MSSQL_USER_STD ausente (.env)")
        
    password = os.getenv('MSSQL_PASSWORD_STD')
    if not password:
        raise RuntimeError("MSSQL_PASSWORD_STD ausente (.env)")
    
    return SQLServerConnection(server, database, username, password)

def get_candiotto_connection():
    """Retorna conexão com banco CANDIOTTO."""
    # Carregar variáveis de ambiente
    env_path = Path(__file__).parent.parent.parent / '.env'
    if env_path.exists():
        load_dotenv(env_path)
    
    # Fail-Fast: sem defaults perigosos
    server = os.getenv('MSSQL_SERVER_CANDIOTTO')
    if not server:
        raise RuntimeError("MSSQL_SERVER_CANDIOTTO ausente (.env)")
        
    database = os.getenv('MSSQL_DATABASE_CANDIOTTO')
    if not database:
        raise RuntimeError("MSSQL_DATABASE_CANDIOTTO ausente (.env)")
        
    username = os.getenv('MSSQL_USER_CANDIOTTO')
    if not username:
        raise RuntimeError("MSSQL_USER_CANDIOTTO ausente (.env)")
        
    password = os.getenv('MSSQL_PASSWORD_CANDIOTTO')
    if not password:
        raise RuntimeError("MSSQL_PASSWORD_CANDIOTTO ausente (.env)")
    
    return SQLServerConnection(server, database, username, password)

class SQLServerConnection:
    """Classe para gerenciar conexões com SQL Server."""
    
    def __init__(self, server: str, database: str, username: str, password: str):
        self.server = server
        self.database = database
        self.username = username
        self.password = password
        self.connection = None
    
    def connect(self) -> bool:
        """Estabelece conexão com o banco de dados."""
        # Lista de drivers ODBC para tentar
        drivers = [
            "ODBC Driver 17 for SQL Server",
            "ODBC Driver 13 for SQL Server",
            "ODBC Driver 11 for SQL Server",
            "SQL Server Native Client 11.0",
            "SQL Server"
        ]
        
        for driver in drivers:
            try:
                connection_string = (
                    f"DRIVER={{{driver}}};"
                    f"SERVER={self.server};"
                    f"DATABASE={self.database};"
                    f"UID={self.username};"
                    f"PWD={self.password};"
                    f"TrustServerCertificate=yes;"
                    f"Timeout=30;"
                )
                self.connection = pyodbc.connect(connection_string, timeout=30)
                return True
            except Exception:
                continue
        
        return False
    
    def execute_query(self, query: str) -> Optional[pd.DataFrame]:
        """Executa uma consulta SQL e retorna um DataFrame."""
        if not self.connection:
            return None
        
        try:
            df = pd.read_sql(query, self.connection)
            return df
        except Exception:
            return None
    
    def close(self):
        """Fecha a conexão com o banco."""
        if self.connection:
            self.connection.close()
            self.connection = None