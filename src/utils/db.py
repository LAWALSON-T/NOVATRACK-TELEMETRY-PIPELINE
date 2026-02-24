"""
Database utility functions for NovaTrack pipeline.

Provides helper functions for database operations.
"""

import logging
from typing import Any, Dict, List, Optional

import psycopg2
from psycopg2 import pool
from psycopg2.extensions import connection as Connection

logger = logging.getLogger(__name__)


def get_connection(
    host: str,
    port: int,
    database: str,
    user: str,
    password: str,
) -> Connection:
    """
    Create a database connection.

    Args:
        host: Database hostname.
        port: Database port.
        database: Database name.
        user: Database username.
        password: Database password.

    Returns:
        PostgreSQL connection object.

    Example:
        >>> conn = get_connection(
        ...     host="localhost",
        ...     port=5432,
        ...     database="novatrack",
        ...     user="user",
        ...     password="pass"
        ... )
        >>> cursor = conn.cursor()
        >>> cursor.execute("SELECT 1")
    """
    try:
        conn = psycopg2.connect(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password,
            connect_timeout=10,  # Fail fast if can't connect
        )
        logger.info(f"Connected to database: {database}@{host}:{port}")
        return conn
    except psycopg2.Error as e:
        logger.error(f"Failed to connect to database: {e}")
        raise


def create_table_if_not_exists(conn: Connection, table_sql: str) -> None:
    """
    Create table if it doesn't exist.

    Args:
        conn: Database connection.
        table_sql: CREATE TABLE SQL statement.

    Example:
        >>> conn = get_connection(...)
        >>> sql = '''
        ... CREATE TABLE IF NOT EXISTS users (
        ...     id SERIAL PRIMARY KEY,
        ...     name VARCHAR(100)
        ... )
        ... '''
        >>> create_table_if_not_exists(conn, sql)
    """
    try:
        cursor = conn.cursor()
        cursor.execute(table_sql)
        conn.commit()
        cursor.close()
        logger.info("Table creation check completed")
    except psycopg2.Error as e:
        conn.rollback()
        logger.error(f"Failed to create table: {e}")
        raise


def execute_sql_file(conn: Connection, filepath: str) -> None:
    """
    Execute SQL statements from a file.

    Args:
        conn: Database connection.
        filepath: Path to SQL file.

    Example:
        >>> conn = get_connection(...)
        >>> execute_sql_file(conn, "sql/init.sql")
    """
    try:
        with open(filepath, "r") as f:
            sql = f.read()

        cursor = conn.cursor()
        cursor.execute(sql)
        conn.commit()
        cursor.close()

        logger.info(f"Executed SQL file: {filepath}")
    except FileNotFoundError:
        logger.error(f"SQL file not found: {filepath}")
        raise
    except psycopg2.Error as e:
        conn.rollback()
        logger.error(f"Failed to execute SQL file: {e}")
        raise


def test_connection(
    host: str,
    port: int,
    database: str,
    user: str,
    password: str,
) -> bool:
    """
    Test if database connection works.

    Args:
        host: Database hostname.
        port: Database port.
        database: Database name.
        user: Database username.
        password: Database password.

    Returns:
        True if connection successful, False otherwise.

    Example:
        >>> if test_connection("localhost", 5432, "db", "user", "pass"):
        ...     print("Connection OK")
        ... else:
        ...     print("Connection failed")
    """
    try:
        conn = get_connection(host, port, database, user, password)
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        cursor.fetchone()
        cursor.close()
        conn.close()
        logger.info("Database connection test successful")
        return True
    except Exception as e:
        logger.error(f"Database connection test failed: {e}")
        return False


def get_table_row_count(conn: Connection, table_name: str) -> int:
    """
    Get the number of rows in a table.

    Args:
        conn: Database connection.
        table_name: Name of the table.

    Returns:
        Number of rows.

    Example:
        >>> conn = get_connection(...)
        >>> count = get_table_row_count(conn, "telemetry_events")
        >>> print(f"Table has {count} rows")
    """
    try:
        cursor = conn.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        count = cursor.fetchone()[0]
        cursor.close()
        return count
    except psycopg2.Error as e:
        logger.error(f"Failed to get row count: {e}")
        return 0


def get_table_columns(
    conn: Connection, table_name: str, schema: str = "public"
) -> List[Dict[str, Any]]:
    """
    Get column information for a table.

    Args:
        conn: Database connection.
        table_name: Name of the table.
        schema: Schema name (default: "public").

    Returns:
        List of dictionaries with column information.

    Example:
        >>> conn = get_connection(...)
        >>> columns = get_table_columns(conn, "telemetry_events")
        >>> for col in columns:
        ...     print(f"{col['name']}: {col['type']}")
    """
    try:
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT 
                column_name,
                data_type,
                is_nullable,
                column_default
            FROM information_schema.columns
            WHERE table_schema = %s
              AND table_name = %s
            ORDER BY ordinal_position
        """,
            (schema, table_name),
        )

        columns = []
        for row in cursor.fetchall():
            columns.append(
                {"name": row[0], "type": row[1], "nullable": row[2] == "YES", "default": row[3]}
            )

        cursor.close()
        return columns
    except psycopg2.Error as e:
        logger.error(f"Failed to get table columns: {e}")
        return []


# Advanced: Connection pooling for high-performance applications
class ConnectionPool:
    """
    Connection pool for reusing database connections.

    Useful in production for better performance.

    Example:
        >>> pool = ConnectionPool(
        ...     host="localhost",
        ...     port=5432,
        ...     database="novatrack",
        ...     user="user",
        ...     password="pass",
        ...     minconn=2,
        ...     maxconn=10
        ... )
        >>>
        >>> # Get connection from pool
        >>> conn = pool.get_connection()
        >>> cursor = conn.cursor()
        >>> cursor.execute("SELECT * FROM events")
        >>>
        >>> # Return to pool (don't close!)
        >>> pool.return_connection(conn)
        >>>
        >>> # When done with pool
        >>> pool.close_all()
    """

    def __init__(
        self,
        host: str,
        port: int,
        database: str,
        user: str,
        password: str,
        minconn: int = 2,
        maxconn: int = 10,
    ):
        """Initialize connection pool."""
        self.pool = pool.SimpleConnectionPool(
            minconn=minconn,
            maxconn=maxconn,
            host=host,
            port=port,
            database=database,
            user=user,
            password=password,
        )
        logger.info(f"Connection pool created: {minconn}-{maxconn} connections")

    def get_connection(self) -> Connection:
        """Get connection from pool."""
        return self.pool.getconn()

    def return_connection(self, conn: Connection) -> None:
        """Return connection to pool."""
        self.pool.putconn(conn)

    def close_all(self) -> None:
        """Close all connections in pool."""
        self.pool.closeall()
        logger.info("Connection pool closed")
