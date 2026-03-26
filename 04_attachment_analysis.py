"""
Script 4: Attachment & Document Type Analysis
===============================================
Analyzes the purchase_attachments table:
  - ATT_TYPE distribution (what labels exist, how frequent)
  - File format distribution (PDF, Excel, images, etc.)
  - Number of attachments per RAS
  - Document completeness patterns (which RAS have all required docs)
  - ATT_TYPE vs file extension mismatches

This directly informs the Document Verification Agent design.

Outputs: 04_attachment_analysis.xlsx

Run: python 04_attachment_analysis.py
"""

import os
import re
import pandas as pd
from datetime import datetime, timedelta
from db_utils import get_connection, run_query
from config import OUTPUT_DIR, MONTHS_LOOKBACK

os.makedirs(OUTPUT_DIR, exist_ok=True)


def main():
    print("=" * 60)
    print("SCRIPT 4: ATTACHMENT & DOCUMENT TYPE ANALYSIS")
    print("=" * 60)

    conn = get_connection()
    cutoff = (datetime.now() - timedelta(days=MONTHS_LOOKBACK * 30)).strftime("%Y-%m-%d")
    writer = pd.ExcelWriter(f"{OUTPUT_DIR}/04_attachment_analysis.xlsx", engine="openpyxl")

    # ── 1. ATT_TYPE distribution ──
    print("\n[1/7] ATT_TYPE value distribution (last 6 months)...")
    att_type_dist = run_query(f"""
        SELECT
            ATT_TYPE as [doc_type_label],
            COUNT(*) as [count],
            CAST(ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM purchase_attachments
                WHERE UPLOADED_ON >= '{cutoff}'), 1) AS DECIMAL(5,1)) as [pct]
        FROM purchase_attachments
        WHERE UPLOADED_ON >= '{cutoff}'
        GROUP BY ATT_TYPE
        ORDER BY COUNT(*) DESC
    """, conn)
    print(f"  Found {len(att_type_dist)} distinct ATT_TYPE values")
    for _, row in att_type_dist.head(15).iterrows():
        print(f"    {str(row['doc_type_label']):30s} {row['count']:>6,} ({row['pct']}%)")
    att_type_dist.to_excel(writer, sheet_name="ATT_TYPE_Distribution", index=False)

    # ── 2. File format distribution (from file names) ──
    print("\n[2/7] File format distribution...")
    file_formats = run_query(f"""
        SELECT
            LOWER(
                CASE
                    WHEN CHARINDEX('.', REVERSE(FILES_NAME)) > 0
                    THEN RIGHT(FILES_NAME, CHARINDEX('.', REVERSE(FILES_NAME)) - 1)
                    ELSE 'no_extension'
                END
            ) as [file_extension],
            COUNT(*) as [count]
        FROM purchase_attachments
        WHERE UPLOADED_ON >= '{cutoff}'
          AND FILES_NAME IS NOT NULL
        GROUP BY LOWER(
            CASE
                WHEN CHARINDEX('.', REVERSE(FILES_NAME)) > 0
                THEN RIGHT(FILES_NAME, CHARINDEX('.', REVERSE(FILES_NAME)) - 1)
                ELSE 'no_extension'
            END
        )
        ORDER BY COUNT(*) DESC
    """, conn)
    print(f"  File types found:")
    for _, row in file_formats.head(15).iterrows():
        print(f"    .{row['file_extension']:10s} {row['count']:>6,}")
    file_formats.to_excel(writer, sheet_name="File_Formats", index=False)

    # ── 3. ATT_TYPE vs file extension cross-tab ──
    print("\n[3/7] ATT_TYPE vs file extension cross-tab...")
    cross_tab = run_query(f"""
        SELECT TOP 5000
            ATT_TYPE as [doc_type_label],
            LOWER(
                CASE
                    WHEN CHARINDEX('.', REVERSE(FILES_NAME)) > 0
                    THEN RIGHT(FILES_NAME, CHARINDEX('.', REVERSE(FILES_NAME)) - 1)
                    ELSE 'no_ext'
                END
            ) as [file_extension],
            COUNT(*) as [count]
        FROM purchase_attachments
        WHERE UPLOADED_ON >= '{cutoff}'
          AND FILES_NAME IS NOT NULL
        GROUP BY ATT_TYPE, LOWER(
            CASE
                WHEN CHARINDEX('.', REVERSE(FILES_NAME)) > 0
                THEN RIGHT(FILES_NAME, CHARINDEX('.', REVERSE(FILES_NAME)) - 1)
                ELSE 'no_ext'
            END
        )
        ORDER BY ATT_TYPE, COUNT(*) DESC
    """, conn)
    cross_tab.to_excel(writer, sheet_name="ATT_TYPE_vs_Extension", index=False)

    # ── 4. Attachments per RAS ──
    print("\n[4/7] Attachments per RAS...")
    per_ras = run_query(f"""
        SELECT
            PURCHASE_ID,
            COUNT(*) as [num_attachments],
            COUNT(DISTINCT ATT_TYPE) as [num_distinct_types],
            COUNT(DISTINCT SUPPLIER_ID) as [num_distinct_suppliers]
        FROM purchase_attachments
        WHERE UPLOADED_ON >= '{cutoff}'
        GROUP BY PURCHASE_ID
    """, conn)
    if len(per_ras) > 0:
        print(f"  RAS records with attachments: {len(per_ras):,}")
        print(f"  Attachments per RAS: min={per_ras['num_attachments'].min()}, "
              f"avg={per_ras['num_attachments'].mean():.1f}, "
              f"max={per_ras['num_attachments'].max()}")
        print(f"  Distinct doc types per RAS: avg={per_ras['num_distinct_types'].mean():.1f}")
        print(f"  Distinct suppliers per RAS: avg={per_ras['num_distinct_suppliers'].mean():.1f}")

        # Distribution of attachment counts
        att_count_dist = per_ras["num_attachments"].value_counts().sort_index().head(20)
        att_count_dist = att_count_dist.reset_index()
        att_count_dist.columns = ["num_attachments", "num_ras"]
        att_count_dist.to_excel(writer, sheet_name="Attachments_Per_RAS_Dist", index=False)

        per_ras.describe().to_excel(writer, sheet_name="Attachments_Per_RAS_Stats")

    # ── 5. ATT_TYPE NULL / blank analysis ──
    print("\n[5/7] ATT_TYPE null/blank analysis...")
    null_analysis = run_query(f"""
        SELECT
            CASE
                WHEN ATT_TYPE IS NULL THEN 'NULL'
                WHEN LTRIM(RTRIM(ATT_TYPE)) = '' THEN 'BLANK'
                ELSE 'HAS_VALUE'
            END as [att_type_status],
            COUNT(*) as [count]
        FROM purchase_attachments
        WHERE UPLOADED_ON >= '{cutoff}'
        GROUP BY
            CASE
                WHEN ATT_TYPE IS NULL THEN 'NULL'
                WHEN LTRIM(RTRIM(ATT_TYPE)) = '' THEN 'BLANK'
                ELSE 'HAS_VALUE'
            END
    """, conn)
    for _, row in null_analysis.iterrows():
        print(f"    {row['att_type_status']:12s} {row['count']:>6,}")
    null_analysis.to_excel(writer, sheet_name="ATT_TYPE_Null_Analysis", index=False)

    # ── 6. Document completeness patterns ──
    print("\n[6/7] Document completeness patterns (which doc types per RAS)...")
    completeness = run_query(f"""
        SELECT TOP 500
            pa.PURCHASE_ID,
            STRING_AGG(DISTINCT pa.ATT_TYPE, ', ') as [doc_types_present],
            COUNT(*) as [total_attachments],
            COUNT(DISTINCT pa.ATT_TYPE) as [distinct_doc_types]
        FROM purchase_attachments pa
        WHERE pa.UPLOADED_ON >= '{cutoff}'
          AND pa.ATT_TYPE IS NOT NULL
        GROUP BY pa.PURCHASE_ID
        ORDER BY COUNT(DISTINCT pa.ATT_TYPE) DESC
    """, conn)
    if len(completeness) > 0:
        completeness.to_excel(writer, sheet_name="Doc_Completeness", index=False)
        print(f"  Sampled {len(completeness)} RAS records for completeness analysis")

    # ── 7. Sample attachments with file names ──
    print("\n[7/7] Sample attachment records...")
    samples = run_query(f"""
        SELECT TOP 100
            PURCHASE_ID,
            PURCHASE_DTL_ID,
            ATTACHMENT_ID,
            FILES_NAME,
            ATT_TYPE,
            SUPPLIER_ID,
            UPLOADED_ON
        FROM purchase_attachments
        WHERE UPLOADED_ON >= '{cutoff}'
        ORDER BY UPLOADED_ON DESC
    """, conn)
    samples.to_excel(writer, sheet_name="Sample_Attachments", index=False)

    writer.close()
    conn.close()

    print(f"\n[DONE] Output saved to: {OUTPUT_DIR}/04_attachment_analysis.xlsx")
    print("  Key sheets: ATT_TYPE_Distribution, File_Formats, ATT_TYPE_vs_Extension,")
    print("              Attachments_Per_RAS, Doc_Completeness, Sample_Attachments")


if __name__ == "__main__":
    main()
