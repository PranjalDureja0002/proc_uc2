"""
Script 5: Category Consistency Analysis
==========================================
Analyzes how consistently items are categorized:
  - Same item described differently in different categories
  - Category distribution
  - Items that appear in multiple categories
  - Category_LVL1 → Category_LVL2 → COMMODITY_ID hierarchy

This validates our decision to use categories as soft signals, not hard filters.

Outputs: 05_category_analysis.xlsx

Run: python 05_category_analysis.py
"""

import os
import pandas as pd
from datetime import datetime, timedelta
from db_utils import get_connection, run_query
from config import OUTPUT_DIR, MONTHS_LOOKBACK

os.makedirs(OUTPUT_DIR, exist_ok=True)


def main():
    print("=" * 60)
    print("SCRIPT 5: CATEGORY CONSISTENCY ANALYSIS")
    print("=" * 60)

    conn = get_connection()
    cutoff = (datetime.now() - timedelta(days=MONTHS_LOOKBACK * 30)).strftime("%Y-%m-%d")
    writer = pd.ExcelWriter(f"{OUTPUT_DIR}/05_category_analysis.xlsx", engine="openpyxl")

    # ── 1. Category distribution from BI view ──
    print("\n[1/5] Purchase_Category distribution...")
    cat_dist = run_query(f"""
        SELECT
            Purchase_Category,
            COUNT(*) as [count],
            CAST(ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM vw_get_ras_data_for_bidashboard
                WHERE Originated_On >= '{cutoff}'), 1) AS DECIMAL(5,1)) as [pct]
        FROM vw_get_ras_data_for_bidashboard
        WHERE Originated_On >= '{cutoff}'
        GROUP BY Purchase_Category
        ORDER BY COUNT(*) DESC
    """, conn)
    print(f"  Found {len(cat_dist)} distinct Purchase_Category values")
    for _, row in cat_dist.head(15).iterrows():
        print(f"    {str(row['Purchase_Category']):35s} {row['count']:>6,} ({row['pct']}%)")
    cat_dist.to_excel(writer, sheet_name="Category_Distribution", index=False)

    # ── 2. Sub_Category_Type distribution ──
    print("\n[2/5] Sub_Category_Type distribution...")
    subcat_dist = run_query(f"""
        SELECT
            Purchase_Category,
            Sub_Category_Type,
            COUNT(*) as [count]
        FROM vw_get_ras_data_for_bidashboard
        WHERE Originated_On >= '{cutoff}'
        GROUP BY Purchase_Category, Sub_Category_Type
        ORDER BY Purchase_Category, COUNT(*) DESC
    """, conn)
    print(f"  Found {len(subcat_dist)} distinct Category → SubCategory combinations")
    subcat_dist.to_excel(writer, sheet_name="SubCategory_Distribution", index=False)

    # ── 3. Category hierarchy: LVL1 → LVL2 → COMMODITY_ID from detail table ──
    print("\n[3/5] Category hierarchy from purchase_req_detail...")
    try:
        hierarchy = run_query(f"""
            SELECT
                Category_LVL1_ID,
                Category_LVL2_ID,
                COMMODITY_ID,
                COUNT(*) as [count]
            FROM purchase_req_detail
            WHERE ORIGINATED_ON >= '{cutoff}'
            GROUP BY Category_LVL1_ID, Category_LVL2_ID, COMMODITY_ID
            ORDER BY Category_LVL1_ID, Category_LVL2_ID, COUNT(*) DESC
        """, conn)
        print(f"  Found {len(hierarchy)} distinct LVL1 → LVL2 → COMMODITY combinations")
        hierarchy.to_excel(writer, sheet_name="Category_Hierarchy", index=False)
    except Exception as e:
        print(f"  [WARN] Category hierarchy query failed: {e}")

    # ── 4. Inconsistency detection: same-sounding items in different categories ──
    print("\n[4/5] Inconsistency detection: similar items in different categories...")

    # Find items where the same keyword appears in multiple categories
    keyword_categories = run_query(f"""
        SELECT TOP 2000
            Item_Name,
            Purchase_Category,
            Sub_Category_Type,
            Supplier,
            Negotiated_Item_Value_INR
        FROM vw_get_ras_data_for_bidashboard
        WHERE Originated_On >= '{cutoff}'
          AND Item_Name IS NOT NULL
          AND LEN(Item_Name) > 10
        ORDER BY NEWID()
    """, conn)

    if len(keyword_categories) > 0:
        keyword_categories.to_excel(writer, sheet_name="Items_With_Categories", index=False)

        # Find items that have "injection" or "moulding" in name — are they all in same category?
        sample_keywords = ["injection", "moulding", "laptop", "dell", "hp", "machine",
                          "transport", "furniture", "construction", "cable", "motor"]
        inconsistency_rows = []
        for kw in sample_keywords:
            matches = keyword_categories[
                keyword_categories["Item_Name"].str.contains(kw, case=False, na=False)
            ]
            if len(matches) > 0:
                cats = matches["Purchase_Category"].nunique()
                subcats = matches["Sub_Category_Type"].nunique()
                inconsistency_rows.append({
                    "keyword": kw,
                    "matching_items": len(matches),
                    "distinct_categories": cats,
                    "distinct_subcategories": subcats,
                    "categories_found": ", ".join(matches["Purchase_Category"].dropna().unique()[:5]),
                    "is_inconsistent": "YES" if cats > 1 else "NO"
                })

        if inconsistency_rows:
            df_incon = pd.DataFrame(inconsistency_rows)
            df_incon.to_excel(writer, sheet_name="Category_Inconsistency", index=False)
            print(f"  Analyzed {len(sample_keywords)} keywords:")
            for _, row in df_incon.iterrows():
                flag = " *** INCONSISTENT" if row["is_inconsistent"] == "YES" else ""
                print(f"    '{row['keyword']}': {row['matching_items']} items, "
                      f"{row['distinct_categories']} categories{flag}")

    # ── 5. NULL / blank category analysis ──
    print("\n[5/5] Category null/blank analysis...")
    null_cats = run_query(f"""
        SELECT
            CASE
                WHEN Purchase_Category IS NULL OR LTRIM(RTRIM(Purchase_Category)) = '' THEN 'MISSING'
                ELSE 'PRESENT'
            END as [category_status],
            CASE
                WHEN Sub_Category_Type IS NULL OR LTRIM(RTRIM(Sub_Category_Type)) = '' THEN 'MISSING'
                ELSE 'PRESENT'
            END as [subcategory_status],
            COUNT(*) as [count]
        FROM vw_get_ras_data_for_bidashboard
        WHERE Originated_On >= '{cutoff}'
        GROUP BY
            CASE WHEN Purchase_Category IS NULL OR LTRIM(RTRIM(Purchase_Category)) = '' THEN 'MISSING' ELSE 'PRESENT' END,
            CASE WHEN Sub_Category_Type IS NULL OR LTRIM(RTRIM(Sub_Category_Type)) = '' THEN 'MISSING' ELSE 'PRESENT' END
    """, conn)
    null_cats.to_excel(writer, sheet_name="Category_Null_Analysis", index=False)
    for _, row in null_cats.iterrows():
        print(f"    Category={row['category_status']}, SubCategory={row['subcategory_status']}: {row['count']:,}")

    writer.close()
    conn.close()

    print(f"\n[DONE] Output saved to: {OUTPUT_DIR}/05_category_analysis.xlsx")


if __name__ == "__main__":
    main()
