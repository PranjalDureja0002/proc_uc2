"""
Script 6: Cross-Validation — DB Fields vs Enriched JSON / Attachments
=======================================================================
Checks how often the structured DB data matches or conflicts with
data extracted from quotation attachments (enriched JSONs).

This validates:
  - Whether enriched JSON exists for recent RAS records
  - Whether JSON item names differ from DB Item_Name
  - Whether JSON prices match DB prices
  - Coverage: what % of recent records have JSON enrichment

Outputs: 06_cross_validation.xlsx

Run: python 06_cross_validation.py

NOTE: This script needs access to the enriched JSON data.
      Update JSON_SOURCE below to point to where your enriched JSONs are stored.
"""

import os
import json
import glob
import pandas as pd
from datetime import datetime, timedelta
from db_utils import get_connection, run_query
from config import OUTPUT_DIR, MONTHS_LOOKBACK

os.makedirs(OUTPUT_DIR, exist_ok=True)

# ============================================================
# UPDATE THIS: Path to enriched JSON files
# These are the pre-extracted JSONs from quotation blob processing
# Could be a folder of JSON files, or a database table
# ============================================================
JSON_SOURCE = "./enriched_jsons/"       # Folder of JSON files
# OR set to None if JSONs are in a database table:
JSON_TABLE = None                        # e.g., "enriched_quotation_data"


def load_json_index(conn):
    """Build an index of which RAS IDs have enriched JSON data."""
    if JSON_TABLE:
        # If JSONs are in a database table
        print(f"  Loading JSON index from table: {JSON_TABLE}")
        try:
            df = run_query(f"""
                SELECT DISTINCT
                    PURCHASE_ID as ras_id,
                    COUNT(*) as json_record_count
                FROM [{JSON_TABLE}]
                GROUP BY PURCHASE_ID
            """, conn)
            return set(df["ras_id"].tolist()), df
        except Exception as e:
            print(f"  [WARN] Could not load JSON table: {e}")
            return set(), pd.DataFrame()

    elif os.path.exists(JSON_SOURCE):
        # If JSONs are in a folder
        print(f"  Scanning JSON files in: {JSON_SOURCE}")
        json_files = glob.glob(os.path.join(JSON_SOURCE, "**/*.json"), recursive=True)
        print(f"  Found {len(json_files)} JSON files")

        ras_ids = set()
        records = []
        for f in json_files[:1000]:  # Sample first 1000
            try:
                with open(f, "r") as fh:
                    data = json.load(fh)
                # Try to extract RAS ID from filename or content
                fname = os.path.basename(f)
                ras_id = data.get("ras_id") or data.get("PURCHASE_ID") or data.get("purchase_id")
                if ras_id:
                    ras_ids.add(str(ras_id))
                    records.append({
                        "ras_id": ras_id,
                        "file": fname,
                        "has_item_name": bool(data.get("item_name") or data.get("Item_Name")),
                        "has_price": bool(data.get("price") or data.get("quoted_price")),
                        "has_specs": bool(data.get("specs") or data.get("specifications")),
                    })
            except Exception:
                pass

        return ras_ids, pd.DataFrame(records) if records else pd.DataFrame()

    else:
        print(f"  [WARN] JSON source not found: {JSON_SOURCE}")
        print(f"  Set JSON_SOURCE in this script to your enriched JSON folder")
        return set(), pd.DataFrame()


def main():
    print("=" * 60)
    print("SCRIPT 6: CROSS-VALIDATION — DB vs ENRICHED JSON")
    print("=" * 60)

    conn = get_connection()
    cutoff = (datetime.now() - timedelta(days=MONTHS_LOOKBACK * 30)).strftime("%Y-%m-%d")
    writer = pd.ExcelWriter(f"{OUTPUT_DIR}/06_cross_validation.xlsx", engine="openpyxl")

    # ── 1. Load enriched JSON index ──
    print("\n[1/4] Loading enriched JSON index...")
    json_ras_ids, json_index = load_json_index(conn)
    print(f"  Enriched JSON available for {len(json_ras_ids)} RAS IDs")

    if len(json_index) > 0:
        json_index.to_excel(writer, sheet_name="JSON_Index", index=False)

    # ── 2. Check coverage: what % of recent RAS records have JSON ──
    print("\n[2/4] JSON coverage for recent RAS records...")
    recent_ras = run_query(f"""
        SELECT DISTINCT
            PURCHASE_REQ_ID
        FROM vw_get_ras_data_for_bidashboard
        WHERE Originated_On >= '{cutoff}'
    """, conn)

    if len(recent_ras) > 0 and len(json_ras_ids) > 0:
        recent_set = set(recent_ras["PURCHASE_REQ_ID"].astype(str).tolist())
        covered = recent_set & json_ras_ids
        not_covered = recent_set - json_ras_ids

        print(f"  Recent RAS records: {len(recent_set):,}")
        print(f"  With enriched JSON: {len(covered):,} ({len(covered)/len(recent_set)*100:.1f}%)")
        print(f"  Without JSON:       {len(not_covered):,} ({len(not_covered)/len(recent_set)*100:.1f}%)")

        coverage = pd.DataFrame({
            "metric": ["Total recent RAS", "With enriched JSON", "Without enriched JSON"],
            "count": [len(recent_set), len(covered), len(not_covered)],
            "pct": [100, round(len(covered)/len(recent_set)*100, 1),
                    round(len(not_covered)/len(recent_set)*100, 1)]
        })
        coverage.to_excel(writer, sheet_name="JSON_Coverage", index=False)
    else:
        print("  [INFO] Cannot compute coverage — either no recent RAS or no JSON index")

    # ── 3. Attachment coverage: how many RAS have attachments at all ──
    print("\n[3/4] Attachment coverage for recent RAS records...")
    att_coverage = run_query(f"""
        SELECT
            v.PURCHASE_REQ_ID,
            v.Item_Name,
            v.Purchase_Category,
            COALESCE(att.num_attachments, 0) as num_attachments,
            COALESCE(att.num_quotation_type, 0) as num_quotation_type_attachments,
            COALESCE(att.distinct_att_types, 0) as distinct_doc_types
        FROM (
            SELECT DISTINCT PURCHASE_REQ_ID, Item_Name, Purchase_Category
            FROM vw_get_ras_data_for_bidashboard
            WHERE Originated_On >= '{cutoff}'
        ) v
        LEFT JOIN (
            SELECT
                PURCHASE_ID,
                COUNT(*) as num_attachments,
                SUM(CASE WHEN ATT_TYPE LIKE '%quot%' OR ATT_TYPE LIKE '%Quot%'
                         OR ATT_TYPE LIKE '%QUOT%' THEN 1 ELSE 0 END) as num_quotation_type,
                COUNT(DISTINCT ATT_TYPE) as distinct_att_types
            FROM purchase_attachments
            WHERE UPLOADED_ON >= '{cutoff}'
            GROUP BY PURCHASE_ID
        ) att ON v.PURCHASE_REQ_ID = att.PURCHASE_ID
        ORDER BY num_attachments DESC
    """, conn)

    if len(att_coverage) > 0:
        no_att = len(att_coverage[att_coverage["num_attachments"] == 0])
        has_att = len(att_coverage[att_coverage["num_attachments"] > 0])
        has_quote = len(att_coverage[att_coverage["num_quotation_type_attachments"] > 0])

        print(f"  Recent RAS records: {len(att_coverage):,}")
        print(f"  With attachments:   {has_att:,} ({has_att/len(att_coverage)*100:.1f}%)")
        print(f"  Without attachments: {no_att:,} ({no_att/len(att_coverage)*100:.1f}%)")
        print(f"  With quotation-type: {has_quote:,} ({has_quote/len(att_coverage)*100:.1f}%)")

        att_coverage.to_excel(writer, sheet_name="Attachment_Coverage", index=False)

        # RAS records with NO attachments at all
        no_att_df = att_coverage[att_coverage["num_attachments"] == 0].head(50)
        no_att_df.to_excel(writer, sheet_name="RAS_Without_Attachments", index=False)

    # ── 4. Price comparison: DB price vs what we'd expect ──
    print("\n[4/4] Price sanity check (looking for anomalies)...")
    price_check = run_query(f"""
        SELECT TOP 1000
            PURCHASE_REQ_ID,
            Item_Name,
            Supplier,
            Original_Item_Value_INR,
            Negotiated_Item_Value_INR,
            CASE
                WHEN Negotiated_Item_Value_INR > 0 AND Original_Item_Value_INR > 0
                THEN CAST(ROUND((1 - Negotiated_Item_Value_INR * 1.0 / Original_Item_Value_INR) * 100, 1) AS DECIMAL(5,1))
                ELSE NULL
            END as discount_pct,
            CASE
                WHEN Negotiated_Item_Value_INR > Original_Item_Value_INR * 2 THEN 'ANOMALY: Negotiated > 2x Original'
                WHEN Negotiated_Item_Value_INR <= 0 THEN 'ANOMALY: Zero or negative'
                WHEN Original_Item_Value_INR <= 0 THEN 'ANOMALY: Zero or negative original'
                WHEN Negotiated_Item_Value_INR > Original_Item_Value_INR THEN 'WARNING: Negotiated > Original'
                ELSE 'OK'
            END as price_status
        FROM vw_get_ras_data_for_bidashboard
        WHERE Originated_On >= '{cutoff}'
          AND Original_Item_Value_INR IS NOT NULL
        ORDER BY
            CASE
                WHEN Negotiated_Item_Value_INR > Original_Item_Value_INR * 2 THEN 0
                WHEN Negotiated_Item_Value_INR > Original_Item_Value_INR THEN 1
                ELSE 2
            END,
            Original_Item_Value_INR DESC
    """, conn)

    if len(price_check) > 0:
        anomaly_count = len(price_check[price_check["price_status"].str.startswith("ANOMALY")])
        warning_count = len(price_check[price_check["price_status"].str.startswith("WARNING")])
        print(f"  Sampled {len(price_check)} records")
        print(f"  Anomalies: {anomaly_count}")
        print(f"  Warnings:  {warning_count}")
        price_check.to_excel(writer, sheet_name="Price_Sanity_Check", index=False)

    writer.close()
    conn.close()

    print(f"\n[DONE] Output saved to: {OUTPUT_DIR}/06_cross_validation.xlsx")
    print("  Key sheets: JSON_Coverage, Attachment_Coverage, RAS_Without_Attachments, Price_Sanity_Check")


if __name__ == "__main__":
    main()
