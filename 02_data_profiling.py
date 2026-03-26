"""
Script 2: Data Profiling (Last 6 Months)
==========================================
For each column in each target table: null %, distinct count,
top values, min/max for numerics, pattern analysis for text.
Filters to last 6 months based on date column.

Outputs: 02_data_profiling.xlsx

Run: python 02_data_profiling.py
"""

import os
import pandas as pd
from datetime import datetime, timedelta
from db_utils import get_connection, run_query, run_scalar
from config import (
    TABLES_TO_PROFILE, VIEWS_TO_PROFILE, DATE_COLUMNS,
    OUTPUT_DIR, MONTHS_LOOKBACK, MAX_DISTINCT_VALUES
)

os.makedirs(OUTPUT_DIR, exist_ok=True)


def get_date_filter(table_name):
    """Build a WHERE clause for last N months."""
    date_col = DATE_COLUMNS.get(table_name)
    if not date_col:
        return "", ""  # No date filter
    cutoff = (datetime.now() - timedelta(days=MONTHS_LOOKBACK * 30)).strftime("%Y-%m-%d")
    where = f"WHERE [{date_col}] >= '{cutoff}'"
    return where, date_col


def profile_column(conn, table_name, col_name, col_type, where_clause):
    """Profile a single column: nulls, distinct values, top values, stats."""
    result = {
        "table": table_name,
        "column": col_name,
        "data_type": col_type,
    }

    # Total rows (with date filter)
    total = run_scalar(
        f"SELECT COUNT(*) FROM [{table_name}] {where_clause}", conn
    )
    result["total_rows"] = total

    if total == 0:
        return result, pd.DataFrame()

    # Null count and %
    null_count = run_scalar(
        f"SELECT COUNT(*) FROM [{table_name}] {where_clause} "
        f"{'AND' if where_clause else 'WHERE'} [{col_name}] IS NULL",
        conn
    )
    result["null_count"] = null_count
    result["null_pct"] = round((null_count / total) * 100, 1) if total > 0 else 0

    # Blank/empty count (for string columns)
    if col_type in ("varchar", "nvarchar", "char", "nchar", "text", "ntext"):
        blank_count = run_scalar(
            f"SELECT COUNT(*) FROM [{table_name}] {where_clause} "
            f"{'AND' if where_clause else 'WHERE'} "
            f"(LTRIM(RTRIM([{col_name}])) = '' OR [{col_name}] IS NULL)",
            conn
        )
        result["blank_or_null_count"] = blank_count
        result["blank_or_null_pct"] = round((blank_count / total) * 100, 1)
    else:
        result["blank_or_null_count"] = null_count
        result["blank_or_null_pct"] = result["null_pct"]

    # Distinct count
    distinct = run_scalar(
        f"SELECT COUNT(DISTINCT [{col_name}]) FROM [{table_name}] {where_clause}",
        conn
    )
    result["distinct_count"] = distinct

    # Top N values by frequency
    try:
        top_values = run_query(
            f"SELECT TOP {MAX_DISTINCT_VALUES} "
            f"  [{col_name}] as [value], "
            f"  COUNT(*) as [count], "
            f"  CAST(ROUND(COUNT(*) * 100.0 / {total}, 1) AS DECIMAL(5,1)) as [pct] "
            f"FROM [{table_name}] {where_clause} "
            f"{'AND' if where_clause else 'WHERE'} [{col_name}] IS NOT NULL "
            f"GROUP BY [{col_name}] "
            f"ORDER BY COUNT(*) DESC",
            conn
        )
        top_values.insert(0, "table", table_name)
        top_values.insert(1, "column", col_name)
    except Exception:
        top_values = pd.DataFrame()

    # Min/max for numeric and date columns
    if col_type in ("int", "bigint", "decimal", "numeric", "float", "money", "smallint", "tinyint"):
        try:
            stats = run_query(
                f"SELECT "
                f"  MIN([{col_name}]) as min_val, "
                f"  MAX([{col_name}]) as max_val, "
                f"  AVG(CAST([{col_name}] AS FLOAT)) as avg_val "
                f"FROM [{table_name}] {where_clause}",
                conn
            )
            result["min_value"] = stats.iloc[0]["min_val"]
            result["max_value"] = stats.iloc[0]["max_val"]
            result["avg_value"] = round(stats.iloc[0]["avg_val"], 2) if stats.iloc[0]["avg_val"] else None
        except Exception:
            pass
    elif col_type in ("datetime", "datetime2", "date", "smalldatetime"):
        try:
            stats = run_query(
                f"SELECT "
                f"  MIN([{col_name}]) as min_val, "
                f"  MAX([{col_name}]) as max_val "
                f"FROM [{table_name}] {where_clause}",
                conn
            )
            result["min_value"] = stats.iloc[0]["min_val"]
            result["max_value"] = stats.iloc[0]["max_val"]
        except Exception:
            pass

    # Average string length for text columns
    if col_type in ("varchar", "nvarchar", "char", "nchar", "text", "ntext"):
        try:
            avg_len = run_scalar(
                f"SELECT AVG(LEN([{col_name}])) FROM [{table_name}] {where_clause} "
                f"{'AND' if where_clause else 'WHERE'} [{col_name}] IS NOT NULL",
                conn
            )
            result["avg_text_length"] = round(avg_len, 1) if avg_len else 0
        except Exception:
            pass

    return result, top_values


def profile_table(conn, table_name):
    """Profile all columns of a table."""
    # Get columns
    cols_df = run_query(
        f"SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS "
        f"WHERE TABLE_NAME = '{table_name}' ORDER BY ORDINAL_POSITION",
        conn
    )

    where_clause, date_col = get_date_filter(table_name)
    if where_clause:
        total = run_scalar(f"SELECT COUNT(*) FROM [{table_name}] {where_clause}", conn)
        print(f"  Rows in last {MONTHS_LOOKBACK} months: {total:,}")
    else:
        total = run_scalar(f"SELECT COUNT(*) FROM [{table_name}]", conn)
        print(f"  Total rows (no date filter): {total:,}")

    profiles = []
    all_top_values = []

    for _, row in cols_df.iterrows():
        col_name = row["COLUMN_NAME"]
        col_type = row["DATA_TYPE"]
        print(f"    Profiling column: {col_name} ({col_type})")

        try:
            profile, top_vals = profile_column(conn, table_name, col_name, col_type, where_clause)
            profiles.append(profile)
            if len(top_vals) > 0:
                all_top_values.append(top_vals)
        except Exception as e:
            print(f"    [WARN] Error profiling {col_name}: {e}")
            profiles.append({"table": table_name, "column": col_name, "error": str(e)})

    return pd.DataFrame(profiles), pd.concat(all_top_values, ignore_index=True) if all_top_values else pd.DataFrame()


def main():
    print("=" * 60)
    print("SCRIPT 2: DATA PROFILING (Last 6 Months)")
    print("=" * 60)

    conn = get_connection()
    writer = pd.ExcelWriter(f"{OUTPUT_DIR}/02_data_profiling.xlsx", engine="openpyxl")

    all_profiles = []
    all_top_values = []

    for table in TABLES_TO_PROFILE + VIEWS_TO_PROFILE:
        print(f"\n[PROFILING] {table}")
        profiles, top_values = profile_table(conn, table)

        all_profiles.append(profiles)
        if len(top_values) > 0:
            all_top_values.append(top_values)

        # Individual sheets
        sheet = table[:28]
        profiles.to_excel(writer, sheet_name=f"Prof_{sheet}", index=False)
        if len(top_values) > 0:
            top_values.to_excel(writer, sheet_name=f"TopV_{sheet}", index=False)

    # Combined summary
    if all_profiles:
        combined = pd.concat(all_profiles, ignore_index=True)
        combined.to_excel(writer, sheet_name="All Profiles", index=False)

        # Data quality summary: columns sorted by null % descending
        quality = combined[["table", "column", "data_type", "total_rows",
                           "null_pct", "blank_or_null_pct", "distinct_count"]].copy()
        quality = quality.sort_values("blank_or_null_pct", ascending=False)
        quality.to_excel(writer, sheet_name="Quality Summary", index=False)

    writer.close()
    conn.close()

    print(f"\n[DONE] Output saved to: {OUTPUT_DIR}/02_data_profiling.xlsx")


if __name__ == "__main__":
    main()
