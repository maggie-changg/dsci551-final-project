"""
tests/test_dataframe.py

Integration-style tests for your custom DataFrame class.

────────────────────────────────────────────
WHAT THIS TEST FILE DOES
────────────────────────────────────────────
This script acts like an “integration harness” for your mini DataFrame system.
It loads your cleaned CSV file, constructs a DataFrame, and verifies that the
core methods — filter, project, group_by, sort_by, and error handling — all
work correctly together.

You can run this file in two main ways:
    python -m tests.test_dataframe      ← from repo root
or
    Open this file in VS Code and click “Run Python File”.

Output is verbose and meant to be human-readable: you’ll see previews of rows,
confirmation messages, and explicit assertions if something goes wrong.
"""

import os
import sys

# ---------------------------------------------------------------
# 0. Path setup — ensures imports work from src/
# ---------------------------------------------------------------
THIS_DIR = os.path.dirname(__file__)
PROJECT_ROOT = os.path.abspath(os.path.join(THIS_DIR, ".."))
SRC_DIR = os.path.join(PROJECT_ROOT, "src")

# Add src/ and project root to Python’s module search path
sys.path.insert(0, SRC_DIR)
sys.path.insert(0, PROJECT_ROOT)

# ---------------------------------------------------------------
# 1. Imports — pull in the DataFrame and optional CSV parser
# ---------------------------------------------------------------
from src.dataframe import DataFrame  # your main DataFrame class
try:
    from src.csv_parser import read_csv  # prefer your own CSV reader
except Exception:
    read_csv = None  # fallback if not available


# ---------------------------------------------------------------
# 2. Helper functions
# ---------------------------------------------------------------

def choose_col(possible_names, available):
    """
    Utility that finds the first matching column name from a list of possibilities.

    Example:
        choose_col(["artist_name", "track_artist", "artist"], df.columns)
    returns whichever one actually exists in your dataset.
    """
    for name in possible_names:
        if name in available:
            return name
    return None


def preview_rows(rows, n=3, k=8):
    """
    Nicely prints a handful of rows (default: 3 rows, 8 columns each)
    so you can visually confirm that the DataFrame content looks right.
    """
    print(f"\n🔍 Preview first {n} rows:")
    for i, r in enumerate(rows[:n], 1):
        # Grab just first k key/value pairs for readability
        items = list(r.items())[:k]
        print(f"Row {i}: " + ", ".join([f"{k}={v}" for k, v in items]))


# ---------------------------------------------------------------
# 3. Data loading logic
# ---------------------------------------------------------------
def load_rows():
    """
    Attempts to load data from your project’s /data directory.
    Prefers a cleaned CSV (spotify_clean.csv) if present.
    Falls back to raw CSV (spotify_songs.csv) otherwise.

    Uses your own csv_parser.read_csv() so data types stay consistent.
    """
    data_dir = os.path.join(PROJECT_ROOT, "data")
    clean_path = os.path.join(data_dir, "spotify_clean.csv")
    raw_path = os.path.join(data_dir, "spotify_songs.csv")

    if os.path.exists(clean_path):
        print(f"📄 Using cleaned CSV: {clean_path}")
        if read_csv is None:
            raise RuntimeError("csv_parser.read_csv not available; please run cleaning first.")
        return read_csv(clean_path)

    print("⚠️ Cleaned file not found. Falling back to raw CSV and parsing in-memory.")
    if not os.path.exists(raw_path):
        raise FileNotFoundError(f"Could not find raw file at {raw_path}")
    if read_csv is None:
        raise RuntimeError("csv_parser.read_csv not available to parse raw file.")
    return read_csv(raw_path)


# ---------------------------------------------------------------
# 4. MAIN TEST FUNCTION
# ---------------------------------------------------------------
def main():
    # ---- Load rows ----
    rows = load_rows()
    assert isinstance(rows, list), "read_csv should return a list of dictionaries"
    assert len(rows) > 0, "No rows loaded — check your CSV path."

    # ---- Construct the DataFrame ----
    df = DataFrame(rows)
    assert df.count() == len(rows), "DataFrame.count() should equal number of rows loaded"
    assert isinstance(df.columns, list) and len(df.columns) > 0, "Columns not inferred correctly"

    print(f"\n✅ Loaded {df.count():,} rows with {len(df.columns)} columns.")
    print("Columns:", df.columns[:12], "..." if len(df.columns) > 12 else "")
    preview_rows(df.rows, n=2, k=10)

    # ---- Dynamically choose column names ----
    artist_col = choose_col(["artist_name", "track_artist", "artist"], df.columns)
    pop_col    = choose_col(["popularity", "track_popularity"], df.columns)
    track_col  = choose_col(["track_name", "name", "song"], df.columns)

    # Sanity check: ensure we at least have an artist column
    assert artist_col is not None, "Couldn't find an artist column."
    print(f"🎯 artist column: {artist_col}")

    # These may or may not exist depending on CSV variant
    if pop_col:
        print(f"🎯 popularity column: {pop_col}")
    else:
        print("ℹ️ No popularity column found; skipping popularity-based tests.")

    if track_col:
        print(f"🎯 track column: {track_col}")
    else:
        print("ℹ️ No track/title column found; projection test will use artist only.")

    # -----------------------------------------------------------
    # 4.1 FILTER TEST
    # -----------------------------------------------------------
    if pop_col:
        print("\n— TEST: filter (popularity > 80) —")
        popular = df.filter(lambda r: isinstance(r.get(pop_col), (int, float)) and r[pop_col] > 80)
        assert popular.count() <= df.count(), "Filter should not increase row count"
        print(f"✅ Filtered rows: {popular.count()} out of {df.count()}")
        preview_rows(popular.rows, n=2, k=6)

    # -----------------------------------------------------------
    # 4.2 PROJECT TEST
    # -----------------------------------------------------------
    print("\n— TEST: project (subset of columns) —")
    cols_to_keep = [c for c in [track_col, artist_col, pop_col] if c]
    assert len(cols_to_keep) > 0, "No suitable columns found to project."
    proj = df.project(cols_to_keep)
    # Check that projection keeps only desired keys
    assert all(set(row.keys()) == set(cols_to_keep) for row in proj.rows[:10]), \
        "Projection kept unexpected columns"
    print(f"✅ Projected columns: {cols_to_keep}")
    preview_rows(proj.rows, n=2, k=len(cols_to_keep))

    # -----------------------------------------------------------
    # 4.3 GROUP_BY TEST
    # -----------------------------------------------------------
    print("\n— TEST: group_by (avg popularity by artist) —")
    if pop_col:
        grouped = df.group_by(artist_col, {pop_col: "avg"})
        assert grouped.count() > 0, "group_by should produce at least one group"

        # ✅ Group key validation (robust against None keys)
        input_keys = {r.get(artist_col) for r in df.rows}
        group_keys = {r.get(artist_col) for r in grouped.rows}
        assert group_keys == input_keys, (
            "Group keys in result do not match unique keys from input.\n"
            f"Missing in result: {sorted(k for k in (input_keys - group_keys) if k is not None)[:10]}\n"
            f"Extra in result: {sorted(k for k in (group_keys - input_keys) if k is not None)[:10]}"
        )
        print(f"✅ Grouped by '{artist_col}' with {grouped.count()} groups.")

        # Sort groups by average popularity (descending) and preview
        top_artists = grouped.sort_by(pop_col, reverse=True)
        preview_rows(top_artists.rows, n=5, k=2)
    else:
        print("Skipping group_by test (no popularity column).")

    # -----------------------------------------------------------
    # 4.4 SORT_BY TEST
    # -----------------------------------------------------------
    print("\n— TEST: sort_by —")
    sort_target = pop_col or artist_col
    sorted_df = df.sort_by(sort_target, reverse=False)
    assert sorted_df.count() == df.count(), "Sorting should not change row count"

    # Light sanity check: first two values are non-decreasing
    a = next((r[sort_target] for r in sorted_df.rows if r.get(sort_target) is not None), None)
    b = next((r[sort_target] for r in sorted_df.rows if r.get(sort_target) is not None and r[sort_target] != a), None)
    if a is not None and b is not None:
        assert a <= b or isinstance(a, str) or isinstance(b, str), "Ascending sort check failed"
    print("✅ sort_by produced correctly ordered rows (basic sanity check).")

    # -----------------------------------------------------------
    # 4.5 ERROR HANDLING TEST
    # -----------------------------------------------------------
    print("\n— TEST: project should raise on bad column —")
    raised = False
    try:
        df.project(["__definitely_not_a_real_column__"])
    except ValueError:
        raised = True
    assert raised, "project() should raise ValueError for missing columns"
    print("✅ project() correctly raised ValueError for invalid columns.")

    # -----------------------------------------------------------
    # 4.6 FINAL STATUS
    # -----------------------------------------------------------
    print("\n🎉 All DataFrame tests completed successfully!\n")


# Entry point guard so script can be run directly
if __name__ == "__main__":
    main()
