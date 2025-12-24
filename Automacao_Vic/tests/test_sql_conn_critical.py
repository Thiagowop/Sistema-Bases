import os
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from src.utils.sql_conn import (
    SQLServerConnection,
    get_candiotto_connection,
    get_std_connection,
)


@pytest.fixture()
def clean_env(monkeypatch):
    """Ensure database env vars are cleared before each test."""
    monkeypatch.setattr("src.utils.sql_conn.load_dotenv", lambda *_, **__: False)
    for key in [
        "MSSQL_SERVER_STD",
        "MSSQL_DATABASE_STD",
        "MSSQL_USER_STD",
        "MSSQL_PASSWORD_STD",
        "MSSQL_SERVER_CANDIOTTO",
        "MSSQL_DATABASE_CANDIOTTO",
        "MSSQL_USER_CANDIOTTO",
        "MSSQL_PASSWORD_CANDIOTTO",
    ]:
        monkeypatch.delenv(key, raising=False)


def _set_std_env(monkeypatch):
    monkeypatch.setenv("MSSQL_SERVER_STD", "std_server")
    monkeypatch.setenv("MSSQL_DATABASE_STD", "std_db")
    monkeypatch.setenv("MSSQL_USER_STD", "std_user")
    monkeypatch.setenv("MSSQL_PASSWORD_STD", "std_pass")


def _set_candiotto_env(monkeypatch):
    monkeypatch.setenv("MSSQL_SERVER_CANDIOTTO", "cand_server")
    monkeypatch.setenv("MSSQL_DATABASE_CANDIOTTO", "cand_db")
    monkeypatch.setenv("MSSQL_USER_CANDIOTTO", "cand_user")
    monkeypatch.setenv("MSSQL_PASSWORD_CANDIOTTO", "cand_pass")


@patch("src.utils.sql_conn.SQLServerConnection")
def test_get_std_connection_uses_env(mock_sql_conn, monkeypatch, clean_env):
    _set_std_env(monkeypatch)

    mock_instance = MagicMock()
    mock_sql_conn.return_value = mock_instance

    result = get_std_connection()

    assert result is mock_instance
    mock_sql_conn.assert_called_once_with("std_server", "std_db", "std_user", "std_pass")


def test_get_std_connection_missing_env_raises(monkeypatch, clean_env):
    _set_std_env(monkeypatch)
    monkeypatch.delenv("MSSQL_PASSWORD_STD")

    with pytest.raises(RuntimeError):
        get_std_connection()


@patch("src.utils.sql_conn.SQLServerConnection")
def test_get_candiotto_connection_uses_env(mock_sql_conn, monkeypatch, clean_env):
    _set_candiotto_env(monkeypatch)

    mock_instance = MagicMock()
    mock_sql_conn.return_value = mock_instance

    result = get_candiotto_connection()

    assert result is mock_instance
    mock_sql_conn.assert_called_once_with(
        "cand_server", "cand_db", "cand_user", "cand_pass"
    )


def test_get_candiotto_connection_missing_env(monkeypatch, clean_env):
    _set_candiotto_env(monkeypatch)
    monkeypatch.delenv("MSSQL_SERVER_CANDIOTTO")

    with pytest.raises(RuntimeError):
        get_candiotto_connection()


@patch("src.utils.sql_conn.pyodbc.connect")
def test_sql_server_connection_connect_success(mock_connect):
    mock_connect.return_value = MagicMock()
    conn = SQLServerConnection("srv", "db", "user", "pwd")

    assert conn.connect() is True
    mock_connect.assert_called()
    assert conn.connection is mock_connect.return_value


@patch("src.utils.sql_conn.pyodbc.connect", side_effect=Exception("driver fail"))
def test_sql_server_connection_connect_failure(mock_connect):
    conn = SQLServerConnection("srv", "db", "user", "pwd")

    assert conn.connect() is False
    assert conn.connection is None


def test_sql_server_connection_execute_without_connection():
    conn = SQLServerConnection("srv", "db", "user", "pwd")

    assert conn.execute_query("SELECT 1") is None


@patch("src.utils.sql_conn.pd.read_sql")
def test_sql_server_connection_execute_query_success(mock_read_sql):
    mock_read_sql.return_value = pd.DataFrame({"col": [1, 2]})
    conn = SQLServerConnection("srv", "db", "user", "pwd")
    conn.connection = MagicMock()

    df = conn.execute_query("SELECT 1")

    assert isinstance(df, pd.DataFrame)
    assert list(df.columns) == ["col"]
    mock_read_sql.assert_called_once_with("SELECT 1", conn.connection)


@patch("src.utils.sql_conn.pd.read_sql", side_effect=Exception("boom"))
def test_sql_server_connection_execute_query_failure(mock_read_sql):
    conn = SQLServerConnection("srv", "db", "user", "pwd")
    conn.connection = MagicMock()

    assert conn.execute_query("SELECT 1") is None
    mock_read_sql.assert_called_once()


def test_sql_server_connection_close():
    mock_conn = MagicMock()
    conn = SQLServerConnection("srv", "db", "user", "pwd")
    conn.connection = mock_conn

    conn.close()

    mock_conn.close.assert_called_once()
    assert conn.connection is None
