"""
Script 1: Schema Discovery
============================
Discovers all tables, views, columns, data types, and row counts.
Outputs: schema_discovery.xlsx

Run: python 01_schema_discovery.py
"""

import os
import pandas as pd
from db_utils import get_connection, run_query, run_scalar
from config import TABLES_TO_PROFILE, VIEWS_TO_PROFILE, OUTPUT_DIR

os.makedirs(OUTPUT_DIR, exist_ok=True)


def discover_all_objects(conn):
    """List all tables and views in the database."""
    query = """
    SELECT 
        TABLE_SCHEMA as [schema],
        TABLE_NAME as [name],
        TABLE_TYPE as [type]
    FROM INFORMATION_SCHEMA.TABLES
    ORDER BY TABLE_TYPE, TABLE_NAME
    """
    return run_query(query, conn)


def get_columns(conn, table_name, schema="dbo"):
    """Get column details for a specific table/view."""
    query = f"""
    SELECT 
        COLUMN_NAME as [column_name],
        DATA_TYPE as [data_type],
        CHARACTER_MAXIMUM_LENGTH as [max_length],
        IS_NULLABLE as [nullable],
        COLUMN_DEFAULT as [default_value],
        ORDINAL_POSITION as [position]
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_NAME = '{table_name}'
      AND TABLE_SCHEMA = '{schema}'
    ORDER BY ORDINAL_POSITION
    """
    return run_query(query, conn)


def get_row_count(conn, table_name):
    """Get row count for a table/view."""
    try:
        return run_scalar(f"SELECT COUNT(*) FROM [{table_name}]", conn)
    except Exception as e:
        print(f"  [WARN] Could not count rows for {table_name}: {e}")
        return None


def get_date_range(conn, table_name, date_col):
    """Get min and max dates for a date column."""
    try:
        query = f"""
        SELECT 
            MIN([{date_col}]) as min_date,
            MAX([{date_col}]) as max_date
        FROM [{table_name}]
        WHERE [{date_col}] IS NOT NULL
        """
        df = run_query(query, conn)
        return df.iloc[0]["min_date"], df.iloc[0]["max_date"]
    except Exception:
        return None, None


def get_sample_rows(conn, table_name, n=5):
    """Get top N rows from a table."""
    try:
        return run_query(f"SELECT TOP {n} * FROM [{table_name}]", conn)
    except Exception as e:
        print(f"  [WARN] Could not sample {table_name}: {e}")
        return pd.DataFrame()


def main():
    print("=" * 60)
    print("SCRIPT 1: SCHEMA DISCOVERY")
    print("=" * 60)

    conn = get_connection()
    writer = pd.ExcelWriter(f"{OUTPUT_DIR}/01_schema_discovery.xlsx", engine="openpyxl")

    # 1. All objects in database
    print("\n[1/4] Discovering all tables and views...")
    all_objects = discover_all_objects(conn)
    print(f"  Found {len(all_objects)} objects")
    all_objects.to_excel(writer, sheet_name="All Objects", index=False)

    # 2. Column details for each target table/view
    print("\n[2/4] Profiling column schemas...")
    all_columns = []
    for table in TABLES_TO_PROFILE + VIEWS_TO_PROFILE:
        print(f"  Profiling: {table}")
        cols = get_columns(conn, table)
        if len(cols) > 0:
            cols.insert(0, "table_name", table)
            all_columns.append(cols)
            # Also write individual sheet
            sheet_name = table[:31]  # Excel sheet name max 31 chars
            cols.to_excel(writer, sheet_name=f"Cols_{sheet_name}", index=False)

    if all_columns:
        combined = pd.concat(all_columns, ignore_index=True)
        combined.to_excel(writer, sheet_name="All Columns", index=False)

    # 3. Row counts
    print("\n[3/4] Counting rows...")
    counts = []
    for table in TABLES_TO_PROFILE + VIEWS_TO_PROFILE:
        count = get_row_count(conn, table)
        print(f"  {table}: {count:,} rows" if count else f"  {table}: ERROR")
        counts.append({"table_name": table, "row_count": count})

    pd.DataFrame(counts).to_excel(writer, sheet_name="Row Counts", index=False)

    # 4. Sample rows
    print("\n[4/4] Extracting sample rows...")
    for table in TABLES_TO_PROFILE + VIEWS_TO_PROFILE:
        print(f"  Sampling: {table}")
        sample = get_sample_rows(conn, table, n=10)
        if len(sample) > 0:
            sheet_name = f"Sample_{table[:24]}"
            sample.to_excel(writer, sheet_name=sheet_name, index=False)

    writer.close()
    conn.close()

    print(f"\n[DONE] Output saved to: {OUTPUT_DIR}/01_schema_discovery.xlsx")
    print("  Sheets: All Objects, All Columns, Row Counts, per-table columns, per-table samples")


if __name__ == "__main__":
    main()
