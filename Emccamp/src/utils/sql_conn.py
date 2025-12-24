from __future__ import annotations

import os
from pathlib import Path
from typing import Optional
import warnings

import pandas as pd
import pyodbc
from dotenv import load_dotenv

warnings.filterwarnings(
    "ignore",
    message="pandas only supports SQLAlchemy connectable",
    category=UserWarning,
)


class SQLServerConnection:
    def __init__(self, server: str, database: str, username: str, password: str) -> None:
        self.server = server
        self.database = database
        self.username = username
        self.password = password
        self.connection: Optional[pyodbc.Connection] = None

    def connect(self) -> bool:
        drivers = [
            "ODBC Driver 18 for SQL Server",
            "ODBC Driver 17 for SQL Server",
            "ODBC Driver 13 for SQL Server",
            "SQL Server"
        ]
        for driver in drivers:
            try:
                conn_string = (
                    f"DRIVER={{{driver}}};"
                    f"SERVER={self.server};"
                    f"DATABASE={self.database};"
                    f"UID={self.username};"
                    f"PWD={self.password};"
                    f"TrustServerCertificate=yes;"
                )
                self.connection = pyodbc.connect(conn_string, timeout=30)
                return True
            except Exception:
                continue
        return False

    def execute_query(self, query: str) -> pd.DataFrame:
        if not self.connection:
            raise RuntimeError('Conexao SQL nao inicializada')
        return pd.read_sql(query, self.connection)

    def close(self) -> None:
        if self.connection:
            self.connection.close()
            self.connection = None

def _load_env(config_base: Path) -> None:
    """Carrega variáveis de ambiente a partir dos caminhos padrão do projeto."""

    candidates = [
        config_base / ".env",
        config_base / "src" / "config" / ".env",
        config_base / "config" / ".env",
    ]

    for env_path in candidates:
        if env_path.exists():
            load_dotenv(env_path)


def get_std_connection(config_base: Path) -> SQLServerConnection:
    _load_env(config_base)
    server = os.getenv('MSSQL_SERVER_STD')
    database = os.getenv('MSSQL_DATABASE_STD')
    user = os.getenv('MSSQL_USER_STD')
    password = os.getenv('MSSQL_PASSWORD_STD')
    if not all([server, database, user, password]):
        raise RuntimeError('Credenciais STD ausentes no arquivo .env')
    return SQLServerConnection(server, database, user, password)

def get_candiotto_connection(config_base: Path) -> SQLServerConnection:
    _load_env(config_base)
    server = os.getenv('MSSQL_SERVER_CANDIOTTO')
    database = os.getenv('MSSQL_DATABASE_CANDIOTTO')
    user = os.getenv('MSSQL_USER_CANDIOTTO')
    password = os.getenv('MSSQL_PASSWORD_CANDIOTTO')
    if not all([server, database, user, password]):
        raise RuntimeError('Credenciais CANDIOTTO ausentes no arquivo .env')
    return SQLServerConnection(server, database, user, password)
