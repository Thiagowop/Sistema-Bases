"""
SQL loader.
Loads data from SQL Server databases.
"""
from __future__ import annotations

import os
from typing import TYPE_CHECKING

import pandas as pd

from ..core.base import BaseLoader, LoaderResult

if TYPE_CHECKING:
    from ..core.schemas import ClientConfig, LoaderConfig


class SQLLoader(BaseLoader):
    """Loads data from SQL Server databases."""

    @property
    def name(self) -> str:
        return "sql"

    def load(self) -> LoaderResult:
        # Get connection parameters (from params or environment)
        server = self.params.get("server", os.getenv("SQL_SERVER", ""))
        database = self.params.get("database", os.getenv("SQL_DATABASE", ""))
        username = self.params.get("username", os.getenv("SQL_USER", ""))
        password = self.params.get("password", os.getenv("SQL_PASSWORD", ""))
        driver = self.params.get("driver", "{ODBC Driver 17 for SQL Server}")

        # Query configuration
        query = self.params.get("query", "")
        table = self.params.get("table", "")
        schema = self.params.get("schema", "dbo")

        if not all([server, database]):
            return LoaderResult(
                data=pd.DataFrame(),
                metadata={"error": "SQL Server connection not configured"},
            )

        if not query and not table:
            return LoaderResult(
                data=pd.DataFrame(),
                metadata={"error": "No query or table specified"},
            )

        try:
            # Try pyodbc first, fall back to pymssql
            conn = self._get_connection(server, database, username, password, driver)

            if not conn:
                return LoaderResult(
                    data=pd.DataFrame(),
                    metadata={"error": "Failed to connect to SQL Server"},
                )

            # Build query if table specified
            if not query:
                query = f"SELECT * FROM [{schema}].[{table}]"

            # Execute query
            df = pd.read_sql(query, conn, dtype=str)
            conn.close()

            # Normalize column names
            df.columns = [str(c).strip().upper() for c in df.columns]

            return LoaderResult(
                data=df,
                metadata={
                    "rows": len(df),
                    "columns": list(df.columns),
                    "source": f"sql:{database}.{schema}.{table}" if table else f"sql:{database}",
                    "query": query[:100] + "..." if len(query) > 100 else query,
                },
            )

        except Exception as e:
            return LoaderResult(
                data=pd.DataFrame(),
                metadata={"error": f"SQL query failed: {e}"},
            )

    def _get_connection(
        self,
        server: str,
        database: str,
        username: str,
        password: str,
        driver: str,
    ):
        """Get database connection using available driver."""
        # Try pyodbc first
        try:
            import pyodbc

            if username and password:
                conn_str = (
                    f"DRIVER={driver};"
                    f"SERVER={server};"
                    f"DATABASE={database};"
                    f"UID={username};"
                    f"PWD={password};"
                    "TrustServerCertificate=yes;"
                )
            else:
                conn_str = (
                    f"DRIVER={driver};"
                    f"SERVER={server};"
                    f"DATABASE={database};"
                    "Trusted_Connection=yes;"
                    "TrustServerCertificate=yes;"
                )
            return pyodbc.connect(conn_str)
        except ImportError:
            pass
        except Exception:
            pass

        # Fall back to pymssql
        try:
            import pymssql

            return pymssql.connect(
                server=server,
                database=database,
                user=username,
                password=password,
            )
        except ImportError:
            pass
        except Exception:
            pass

        return None


def create_sql_loader(config: LoaderConfig, client_config: ClientConfig) -> SQLLoader:
    """Factory function to create a SQLLoader."""
    return SQLLoader(config, client_config)
