"""
Test script for csv_parser.py
Purpose:
- Run CSV cleaning pipeline
- Print sample rows and cleaning summary
- Verify that cleaned file is written successfully
- Verify that cleaned file is readable by our parser and has same schema
"""

import os
import sys

# ---------------------------------------------------------------
# Ensure Python can find the 'src' directory
# ---------------------------------------------------------------
current_dir = os.path.dirname(__file__)
project_root = os.path.abspath(os.path.join(current_dir, ".."))
src_dir = os.path.join(project_root, "src")

sys.path.insert(0, src_dir)
sys.path.insert(0, project_root)

# ---------------------------------------------------------------
# Import from src/
# ---------------------------------------------------------------
from src.csv_parser import read_csv, write_clean_csv, print_stats

# ---------------------------------------------------------------
# Define file paths
# ---------------------------------------------------------------
csv_path = os.path.join(project_root, "data", "spotify_songs.csv")
clean_path = os.path.join(project_root, "data", "spotify_clean.csv")

# ---------------------------------------------------------------
# Check if input file exists
# ---------------------------------------------------------------
if not os.path.exists(csv_path):
    print(f"[ERROR] Could not find input file: {csv_path}")
    exit()

# ---------------------------------------------------------------
# Read and parse CSV
# ---------------------------------------------------------------
print(f"Reading CSV file from: {csv_path}")
rows = read_csv(csv_path)
print(f"Successfully parsed {len(rows):,} rows.")

# ---------------------------------------------------------------
# Preview sample rows
# ---------------------------------------------------------------
if rows:
    print("\nExample Dataset of the first 3 rows:")
    for i, row in enumerate(rows[:3], start=1):
        print(f"\nRow {i}:")
        for key, value in list(row.items())[:10]:
            print(f"  {key}: {value}")

# ---------------------------------------------------------------
# Write cleaned CSV
# ---------------------------------------------------------------
write_clean_csv(rows, clean_path)

# ---------------------------------------------------------------
# Print cleaning stats
# ---------------------------------------------------------------
print_stats(rows)

# ---------------------------------------------------------------
# Confirm cleaned file creation
# ---------------------------------------------------------------
if os.path.exists(clean_path):
    print(f"Cleaned file successfully saved at: {clean_path}")
else:
    print(f"[WARNING] Cleaned file not found. Check write_clean_csv() function.")

# ---------------------------------------------------------------
# Verify that cleaned CSV is readable and has same schema
# ---------------------------------------------------------------
if os.path.exists(clean_path):
    print("\nRe-reading cleaned CSV to verify schema...")
    cleaned_rows = read_csv(clean_path)

    if not isinstance(cleaned_rows, list):
        raise AssertionError("Cleaned file should load as a list of dicts.")

    if not cleaned_rows:
        raise AssertionError("Cleaned file appears to be empty.")

    original_keys = set(rows[0].keys())
    cleaned_keys = set(cleaned_rows[0].keys())

    if original_keys != cleaned_keys:
        raise AssertionError(
            f"Schema mismatch between original and cleaned CSV.\n"
            f"Original keys: {sorted(original_keys)}\n"
            f"Cleaned keys:  {sorted(cleaned_keys)}"
        )

    print("Cleaned CSV is readable and has the same schema as original data.")
