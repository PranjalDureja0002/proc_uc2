"""
RAS Data Profiler — Master Runner
====================================
Runs all profiling scripts in sequence.

Run: python run_all.py
"""

import subprocess
import sys
import os
import time

SCRIPTS = [
    ("01_schema_discovery.py",  "Schema Discovery — tables, columns, row counts, samples"),
    ("02_data_profiling.py",    "Data Profiling — null rates, distinct values, data quality per column"),
    ("03_item_quality.py",      "Item Name Quality — garbage/vague/moderate/rich classification"),
    ("04_attachment_analysis.py","Attachment Analysis — ATT_TYPE, file formats, docs per RAS"),
    ("05_category_analysis.py", "Category Consistency — inconsistency detection, hierarchy analysis"),
    ("06_cross_validation.py",  "Cross-Validation — DB fields vs enriched JSON coverage"),
]


def run_script(script, description):
    print(f"\n{'='*70}")
    print(f"RUNNING: {script}")
    print(f"  {description}")
    print(f"{'='*70}\n")

    start = time.time()
    result = subprocess.run(
        [sys.executable, script],
        capture_output=False
    )
    elapsed = time.time() - start

    if result.returncode == 0:
        print(f"\n[OK] {script} completed in {elapsed:.1f}s")
    else:
        print(f"\n[ERROR] {script} failed (exit code {result.returncode})")

    return result.returncode == 0


def main():
    print("=" * 70)
    print("RAS DATA PROFILER — MASTER RUNNER")
    print("=" * 70)
    print(f"Running {len(SCRIPTS)} scripts in sequence...")
    print(f"Output directory: ./profiler_output/")
    print()

    # Check dependencies
    try:
        import pyodbc
        import openpyxl
        print("[OK] pyodbc installed")
        print("[OK] openpyxl installed")
    except ImportError as e:
        print(f"[ERROR] Missing dependency: {e}")
        print("  Run: pip install pyodbc pandas openpyxl")
        sys.exit(1)

    print("\n[IMPORTANT] Before running:")
    print("  1. Update config.py with your SQL Server connection details")
    print("  2. Ensure you have network access to the SQL Server")
    print("  3. Ensure ODBC Driver 17 for SQL Server is installed")
    print()

    input("Press Enter to start, or Ctrl+C to cancel...")

    results = {}
    for script, desc in SCRIPTS:
        if os.path.exists(script):
            success = run_script(script, desc)
            results[script] = "OK" if success else "FAILED"
        else:
            print(f"\n[SKIP] {script} not found")
            results[script] = "NOT FOUND"

    # Summary
    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)
    for script, status in results.items():
        icon = "[OK]   " if status == "OK" else "[FAIL] " if status == "FAILED" else "[SKIP] "
        print(f"  {icon} {script}")

    print(f"\n  Output files in: ./profiler_output/")
    print(f"  Share these Excel files for review.")


if __name__ == "__main__":
    main()
