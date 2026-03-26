# RAS Data Profiler

Data profiling toolkit for the Motherson RAS Procurement Benchmarking project.
Generates comprehensive data quality analysis across all key tables and views.

## Setup

### Prerequisites
```bash
pip install pyodbc pandas openpyxl
```

You also need **ODBC Driver 17 for SQL Server** installed:
- Windows: Usually pre-installed, or download from Microsoft
- Linux: `sudo apt-get install msodbcsql17`

### Configuration
Edit `config.py` and update:
1. `DB_CONFIG` — your SQL Server connection (server, database, auth method)
2. `DATE_COLUMNS` — verify the date column names match your actual schema
3. `MONTHS_LOOKBACK` — defaults to 6 months, adjust if needed

## Running

### Run everything at once:
```bash
python run_all.py
```

### Run individual scripts:
```bash
python 01_schema_discovery.py      # Tables, columns, types, samples
python 02_data_profiling.py        # Null rates, distinct values, quality
python 03_item_quality.py          # Item_Name garbage/vague/rich analysis
python 04_attachment_analysis.py   # ATT_TYPE, file formats, docs per RAS
python 05_category_analysis.py     # Category consistency check
python 06_cross_validation.py      # DB vs enriched JSON coverage
```

## Output

All outputs are in `./profiler_output/` as Excel files:

| File | What it tells you |
|------|-------------------|
| `01_schema_discovery.xlsx` | All tables/views in the DB, columns with data types, row counts, sample rows |
| `02_data_profiling.xlsx` | Per-column: null %, blank %, distinct values, top values, min/max, avg text length |
| `03_item_quality.xlsx` | Item_Name classified as GARBAGE/VAGUE/MODERATE/RICH with examples and % breakdown |
| `04_attachment_analysis.xlsx` | ATT_TYPE labels, file extensions, attachments per RAS, completeness patterns |
| `05_category_analysis.xlsx` | Category distribution, hierarchy, inconsistency detection (same item in multiple categories) |
| `06_cross_validation.xlsx` | JSON coverage %, attachment coverage %, price anomalies, RAS with no attachments |

## What to look for in the results

### From 03_item_quality.xlsx:
- **Quality Summary sheet**: What % of your data is GARBAGE vs RICH? This determines how much your ingestion pipeline depends on enriched JSONs.
- **Garbage Examples sheet**: The actual garbage descriptions — helps define the patterns for the ingestion skip logic.
- **Quality By Category sheet**: Are some categories worse than others? (e.g., "Infrastructure" might have more garbage than "IT Equipment")

### From 04_attachment_analysis.xlsx:
- **ATT_TYPE_Distribution sheet**: What document type labels exist? How many distinct values? Are there typos/variations?
- **ATT_TYPE_vs_Extension sheet**: Does a "Quotation" ATT_TYPE always have a .pdf/.xlsx extension? Or are there .msg (emails) labelled as quotations?
- **Attachments_Per_RAS sheet**: How many attachments does a typical RAS have? This affects background processor load.

### From 05_category_analysis.xlsx:
- **Category_Inconsistency sheet**: Are "injection moulding" items found in multiple categories? This validates our decision to use categories as soft signals.

### From 06_cross_validation.xlsx:
- **JSON_Coverage sheet**: What % of recent RAS records have enriched JSON? If it's high (>70%), the primary path in ingestion will handle most records. If low, the fallback path matters more.
- **RAS_Without_Attachments sheet**: Are there RAS records with zero attachments? These are the ones where the dashboard will show limited KPIs.
