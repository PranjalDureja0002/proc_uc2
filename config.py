"""
RAS Data Profiler — Configuration
==================================
Update the connection settings below before running any scripts.
"""

# ============================================================
# DATABASE CONNECTION
# ============================================================
# Primary: Azure SQL Database (rasmaster)
DB_CONFIG = {
    "server": "rasmastertable.database.windows.net",
    "database": "rasmaster",
    "driver": "{ODBC Driver 17 for SQL Server}",
    "trusted_connection": False,             # Azure SQL requires SQL Auth
    "username": "YOUR_USERNAME",             # <-- UPDATE THIS
    "password": "YOUR_PASSWORD",             # <-- UPDATE THIS
}

# Secondary: On-prem PRODTEST (if needed)
DB_CONFIG_SECONDARY = {
    "server": "10.193.10.111",
    "port": "1528",
    "database": "PRODTEST",                  # Adjust if different
    "driver": "{ODBC Driver 17 for SQL Server}",
    "trusted_connection": True,              # On-prem may use Windows Auth
    "username": "",
    "password": "",
}

# ============================================================
# ANALYSIS SETTINGS
# ============================================================
# Only analyze data from the last N months (based on created/originated date)
MONTHS_LOOKBACK = 6

# Maximum rows to sample for display (keeps output manageable)
SAMPLE_ROWS = 20

# Maximum distinct values to show per column in profiling
MAX_DISTINCT_VALUES = 30

# ============================================================
# KNOWN TABLES & VIEWS (from our ADD analysis)
# ============================================================
TABLES_TO_PROFILE = [
    "purchase_req_mst",
    "purchase_req_detail",
    "purchase_attachments",
    "doc_clasification_metadata",    # NEW — may contain existing doc classification
    "EXCHANGE_RATE",                 # NEW — useful for price normalization
    "currency_mst",                  # NEW — currency master data
    "tbl_get_ras_data_for_bidashboard",  # NEW — materialized version of BI view
]

VIEWS_TO_PROFILE = [
    "vw_get_ras_data_for_bidashboard",
]

# Date columns to use for filtering last N months per table
DATE_COLUMNS = {
    "purchase_req_mst": "ORIGINATED_ON",
    "purchase_req_detail": "ORIGINATED_ON",
    "purchase_attachments": "UPLOADED_ON",
    "doc_clasification_metadata": None,      # Unknown — script will skip date filter
    "EXCHANGE_RATE": None,                    # Unknown — script will skip date filter
    "currency_mst": None,                     # Small table — no filter needed
    "tbl_get_ras_data_for_bidashboard": "Originated_On",
    "vw_get_ras_data_for_bidashboard": "Originated_On",
}

# ============================================================
# OUTPUT
# ============================================================
OUTPUT_DIR = "./profiler_output"
