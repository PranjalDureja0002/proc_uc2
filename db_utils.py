"""
RAS Data Profiler — Database Connection Utility
=================================================
"""

import pyodbc
import pandas as pd
from config import DB_CONFIG, DB_CONFIG_SECONDARY


def get_connection(use_secondary=False):
    """Get a pyodbc connection to the RAS SQL Server."""
    cfg = DB_CONFIG_SECONDARY if use_secondary else DB_CONFIG

    if cfg["trusted_connection"]:
        conn_str = (
            f"DRIVER={cfg['driver']};"
            f"SERVER={cfg['server']};"
            f"DATABASE={cfg['database']};"
            f"Trusted_Connection=yes;"
        )
    else:
        conn_str = (
            f"DRIVER={cfg['driver']};"
            f"SERVER={cfg['server']};"
            f"DATABASE={cfg['database']};"
            f"UID={cfg['username']};"
            f"PWD={cfg['password']};"
        )

    try:
        conn = pyodbc.connect(conn_str, timeout=30)
        print(f"[OK] Connected to {cfg['server']} / {cfg['database']}")
        return conn
    except Exception as e:
        print(f"[ERROR] Connection failed: {e}")
        print(f"  Server: {cfg['server']}")
        print(f"  Database: {cfg['database']}")
        print(f"  Driver: {cfg['driver']}")
        raise


def run_query(query, conn=None, use_secondary=False):
    """Run a SQL query and return a pandas DataFrame."""
    close_conn = False
    if conn is None:
        conn = get_connection(use_secondary)
        close_conn = True
    try:
        df = pd.read_sql(query, conn)
        return df
    finally:
        if close_conn:
            conn.close()


def run_scalar(query, conn):
    """Run a query and return a single scalar value."""
    cursor = conn.cursor()
    cursor.execute(query)
    result = cursor.fetchone()
    return result[0] if result else None
