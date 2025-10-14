"""
src/clean_spotify_subset.py

Creates a lightweight version of spotify_clean.csv that keeps only
columns relevant for downstream analysis and modeling.

Usage (from repo root):
    python -m src.clean_spotify_subset
"""

import os
import csv

# ---------------------------------------------------------------
# CONFIGURATION
# ---------------------------------------------------------------
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DATA_DIR = os.path.join(PROJECT_ROOT, "data")

INPUT_FILE = os.path.join(DATA_DIR, "spotify_clean.csv")
OUTPUT_FILE = os.path.join(DATA_DIR, "spotify_subset.csv")

# Columns you care about
KEEP_COLS = [
    "track_name",
    "track_artist",
    "track_popularity",
    "track_album_name",
    "track_album_release_date",
    "playlist_genre",
    "playlist_subgenre",
    "danceability",
    "energy",
    "tempo",
    "liveness",
]


# ---------------------------------------------------------------
# FUNCTION: clean_csv
# ---------------------------------------------------------------
def clean_csv(input_path: str, output_path: str, keep_cols):
    """
    Reads a CSV, filters to specific columns, and writes a new CSV.

    - Skips columns not in KEEP_COLS
    - Warns if expected columns are missing
    """
    if not os.path.exists(input_path):
        raise FileNotFoundError(f"Input CSV not found: {input_path}")

    with open(input_path, "r", encoding="utf-8", newline="") as infile:
        reader = csv.DictReader(infile)
        available_cols = reader.fieldnames or []
        missing = [c for c in keep_cols if c not in available_cols]
        if missing:
            print(f"⚠️ Warning: missing columns: {missing}")

        cols_to_use = [c for c in keep_cols if c in available_cols]
        rows = [ {col: row.get(col, "") for col in cols_to_use} for row in reader ]

    # Write subset CSV
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w", encoding="utf-8", newline="") as outfile:
        writer = csv.DictWriter(outfile, fieldnames=cols_to_use)
        writer.writeheader()
        writer.writerows(rows)

    print(f"✅ Wrote subset CSV: {output_path}")
    print(f"   Rows: {len(rows):,} | Columns kept: {len(cols_to_use)} ({cols_to_use})")


# ---------------------------------------------------------------
# MAIN EXECUTION
# ---------------------------------------------------------------
if __name__ == "__main__":
    clean_csv(INPUT_FILE, OUTPUT_FILE, KEEP_COLS)
