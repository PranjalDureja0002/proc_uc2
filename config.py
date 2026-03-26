"""
RAS Data Profiler — Configuration
==================================
Update the connection settings below before running any scripts.
"""

# ============================================================
# DATABASE CONNECTION
# ============================================================
# Option 1: Windows Authentication (if running on a domain-joined machine)
DB_CONFIG = {
    "server": "YOUR_SQL_SERVER_HOST",       # e.g., "10.0.0.1" or "sqlserver.company.com"
    "database": "YOUR_DATABASE_NAME",        # e.g., "RAS_DB"
    "driver": "{ODBC Driver 17 for SQL Server}",
    "trusted_connection": True,              # True = Windows Auth, False = SQL Auth
    # Only needed if trusted_connection = False:
    "username": "",
    "password": "",
}

# Option 2: If you have a second database (Arpita mentioned two databases)
DB_CONFIG_SECONDARY = {
    "server": "YOUR_SECONDARY_SQL_SERVER",
    "database": "YOUR_SECONDARY_DB",
    "driver": "{ODBC Driver 17 for SQL Server}",
    "trusted_connection": True,
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
]

VIEWS_TO_PROFILE = [
    "vw_get_ras_data_for_bidashboard",
]

# Date columns to use for filtering last N months per table
DATE_COLUMNS = {
    "purchase_req_mst": "ORIGINATED_ON",       # Adjust if different
    "purchase_req_detail": "ORIGINATED_ON",     # Adjust if different
    "purchase_attachments": "UPLOADED_ON",
    "vw_get_ras_data_for_bidashboard": "Originated_On",  # Adjust if different
}

# ============================================================
# OUTPUT
# ============================================================
OUTPUT_DIR = "./profiler_output"
