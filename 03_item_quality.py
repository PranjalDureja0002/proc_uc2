"""
Script 3: Item Name & Description Quality Analysis
=====================================================
Classifies every Item_Name / ITEMDESCRIPTION into:
  - GARBAGE: blank, "as per PO attached", "refer attachment", etc.
  - VAGUE: single generic words like "Laptop", "Machine", "Infra"
  - MODERATE: some useful info but incomplete
  - RICH: brand, model, specs embedded

This directly informs our ingestion pipeline:
  - GARBAGE + no JSON → SKIP (excluded from vectorization)
  - GARBAGE/VAGUE + JSON exists → BOOSTED path (use JSON canonical name)
  - MODERATE/RICH → FALLBACK path (use Item_Name, but JSON still preferred)

Outputs: 03_item_quality.xlsx

Run: python 03_item_quality.py
"""

import os
import re
import pandas as pd
from datetime import datetime, timedelta
from db_utils import get_connection, run_query
from config import OUTPUT_DIR, MONTHS_LOOKBACK

os.makedirs(OUTPUT_DIR, exist_ok=True)

# Patterns that indicate garbage descriptions
GARBAGE_PATTERNS = [
    r"(?i)^as\s+per\s+(po|purchase|annexure|attachment|sheet|quote|mail|req)",
    r"(?i)^refer\s+(attachment|annexure|po|mail|document|sheet)",
    r"(?i)^see\s+(attachment|annexure|enclosed|po)",
    r"(?i)^attached\s+(herewith|file|document|po)",
    r"(?i)^as\s+discussed",
    r"(?i)^as\s+mentioned",
    r"(?i)^as\s+agreed",
    r"(?i)^please\s+refer",
    r"(?i)^please\s+see",
    r"(?i)^details?\s+in\s+(attachment|annexure|po)",
    r"(?i)^same\s+as\s+(above|before|previous)",
    r"(?i)^-+$",               # Just dashes
    r"(?i)^\.+$",               # Just dots
    r"(?i)^n/?a$",              # NA or N/A
    r"(?i)^nil$",
    r"(?i)^none$",
    r"(?i)^test\s*$",
    r"(?i)^xx+$",               # xxx or similar
]

# Words that indicate vague descriptions (too generic to be useful)
VAGUE_WORDS = {
    "laptop", "desktop", "pc", "computer", "machine", "machinery",
    "equipment", "material", "materials", "infra", "infrastructure",
    "furniture", "services", "service", "work", "works", "items",
    "item", "goods", "consumables", "consumable", "spare", "spares",
    "tools", "tool", "parts", "part", "supplies", "supply",
    "miscellaneous", "misc", "general", "other", "others",
    "various", "sundry", "charges", "expenses",
}

# Patterns that indicate rich descriptions (brand, model, specs)
RICH_INDICATORS = [
    r"\d+\s*(T|ton|GB|MB|TB|kg|mm|inch|HP|kW|RPM|PSI|bar|volt|V|amp|A)\b",  # specs with units
    r"[A-Z]{2,}\s*[-/]?\s*\d{3,}",      # Model numbers like "XPS 1210", "DT-500"
    r"(?i)(make|brand|model|type)\s*:?\s*\w+",  # Explicit brand/model mentions
    r"[A-Z][a-z]+\s+[A-Z][a-z]+.*\d",   # Mixed case with numbers (product names)
    r"\b[A-Z0-9]{2,}-[A-Z0-9]{2,}\b",   # Part numbers like "C9723A", "DS-66081"
]


def classify_item_name(text):
    """Classify an item name into GARBAGE, VAGUE, MODERATE, or RICH."""
    if text is None or str(text).strip() == "":
        return "GARBAGE", "blank/null"

    text = str(text).strip()

    # Check if it's too short to be meaningful
    if len(text) <= 2:
        return "GARBAGE", f"too short ({len(text)} chars)"

    # Check garbage patterns
    for pattern in GARBAGE_PATTERNS:
        if re.search(pattern, text):
            return "GARBAGE", f"matches pattern: {pattern[:40]}"

    # Check if it's a single vague word
    words = text.lower().split()
    if len(words) <= 2 and all(w.strip(".,;:-()") in VAGUE_WORDS for w in words):
        return "VAGUE", f"generic word(s): {text}"

    # Check for rich indicators
    rich_count = sum(1 for p in RICH_INDICATORS if re.search(p, text))
    if rich_count >= 2:
        return "RICH", f"{rich_count} rich indicators (specs, model numbers, brands)"
    elif rich_count == 1:
        return "MODERATE", f"1 rich indicator, partial specs"

    # Default: check word count and length
    if len(words) >= 4 and len(text) >= 20:
        return "MODERATE", "multi-word description, some context"
    elif len(words) >= 2:
        return "VAGUE", f"short description ({len(words)} words, {len(text)} chars)"
    else:
        return "VAGUE", f"single word: {text}"


def main():
    print("=" * 60)
    print("SCRIPT 3: ITEM NAME & DESCRIPTION QUALITY ANALYSIS")
    print("=" * 60)

    conn = get_connection()
    cutoff = (datetime.now() - timedelta(days=MONTHS_LOOKBACK * 30)).strftime("%Y-%m-%d")
    writer = pd.ExcelWriter(f"{OUTPUT_DIR}/03_item_quality.xlsx", engine="openpyxl")

    # ── Analysis 1: Item_Name from BI view ──
    print("\n[1/4] Analyzing Item_Name from BI view (last 6 months)...")
    try:
        df_view = run_query(f"""
            SELECT TOP 50000
                Item_Name,
                Purchase_Category,
                Sub_Category_Type,
                Supplier,
                Originated_On
            FROM vw_get_ras_data_for_bidashboard
            WHERE Originated_On >= '{cutoff}'
            ORDER BY Originated_On DESC
        """, conn)
    except Exception as e:
        print(f"  [WARN] BI view query failed, trying alternative: {e}")
        df_view = pd.DataFrame()

    if len(df_view) > 0:
        print(f"  Fetched {len(df_view):,} rows")
        df_view["quality"], df_view["reason"] = zip(
            *df_view["Item_Name"].apply(classify_item_name)
        )
        quality_summary = df_view["quality"].value_counts()
        quality_pct = df_view["quality"].value_counts(normalize=True).mul(100).round(1)

        print("\n  ITEM NAME QUALITY DISTRIBUTION (BI View):")
        for q in ["RICH", "MODERATE", "VAGUE", "GARBAGE"]:
            count = quality_summary.get(q, 0)
            pct = quality_pct.get(q, 0)
            bar = "█" * int(pct / 2)
            print(f"    {q:10s}: {count:>6,} ({pct:>5.1f}%) {bar}")

        df_view.to_excel(writer, sheet_name="BI_View_ItemNames", index=False)

        # Quality summary
        summary = pd.DataFrame({
            "quality": quality_summary.index,
            "count": quality_summary.values,
            "percentage": quality_pct.values
        })
        summary.to_excel(writer, sheet_name="BI_View_Quality_Summary", index=False)

        # Garbage examples
        garbage = df_view[df_view["quality"] == "GARBAGE"].head(50)
        garbage.to_excel(writer, sheet_name="BI_View_Garbage_Examples", index=False)

        # Rich examples
        rich = df_view[df_view["quality"] == "RICH"].head(50)
        rich.to_excel(writer, sheet_name="BI_View_Rich_Examples", index=False)

    # ── Analysis 2: ITEMDESCRIPTION from purchase_req_detail ──
    print("\n[2/4] Analyzing ITEMDESCRIPTION from purchase_req_detail...")
    try:
        df_detail = run_query(f"""
            SELECT TOP 50000
                ITEMDESCRIPTION,
                PRICE,
                SUPPLIER_NAME,
                Category_LVL1_ID,
                Category_LVL2_ID,
                COMMODITY_ID
            FROM purchase_req_detail
            WHERE ORIGINATED_ON >= '{cutoff}'
            ORDER BY ORIGINATED_ON DESC
        """, conn)
    except Exception as e:
        print(f"  [WARN] purchase_req_detail query failed: {e}")
        df_detail = pd.DataFrame()

    if len(df_detail) > 0:
        print(f"  Fetched {len(df_detail):,} rows")
        df_detail["quality"], df_detail["reason"] = zip(
            *df_detail["ITEMDESCRIPTION"].apply(classify_item_name)
        )
        quality_summary2 = df_detail["quality"].value_counts()
        quality_pct2 = df_detail["quality"].value_counts(normalize=True).mul(100).round(1)

        print("\n  ITEM DESCRIPTION QUALITY DISTRIBUTION (purchase_req_detail):")
        for q in ["RICH", "MODERATE", "VAGUE", "GARBAGE"]:
            count = quality_summary2.get(q, 0)
            pct = quality_pct2.get(q, 0)
            bar = "█" * int(pct / 2)
            print(f"    {q:10s}: {count:>6,} ({pct:>5.1f}%) {bar}")

        df_detail.to_excel(writer, sheet_name="Detail_ItemDesc", index=False)

    # ── Analysis 3: Quality by category ──
    print("\n[3/4] Quality breakdown by Purchase Category...")
    if len(df_view) > 0 and "Purchase_Category" in df_view.columns:
        cat_quality = df_view.groupby(["Purchase_Category", "quality"]).size().unstack(fill_value=0)
        cat_quality["total"] = cat_quality.sum(axis=1)
        if "GARBAGE" in cat_quality.columns:
            cat_quality["garbage_pct"] = (cat_quality["GARBAGE"] / cat_quality["total"] * 100).round(1)
        cat_quality = cat_quality.sort_values("total", ascending=False)
        cat_quality.to_excel(writer, sheet_name="Quality_By_Category")
        print(f"  {len(cat_quality)} categories analyzed")

    # ── Analysis 4: Description length distribution ──
    print("\n[4/4] Description length analysis...")
    if len(df_view) > 0:
        df_view["name_length"] = df_view["Item_Name"].apply(lambda x: len(str(x)) if x else 0)
        length_stats = df_view.groupby("quality")["name_length"].describe()
        length_stats.to_excel(writer, sheet_name="Length_Stats")

    writer.close()
    conn.close()

    print(f"\n[DONE] Output saved to: {OUTPUT_DIR}/03_item_quality.xlsx")
    print("  Key sheets: BI_View_Quality_Summary, Garbage_Examples, Rich_Examples, Quality_By_Category")


if __name__ == "__main__":
    main()
