"""
tests/test_dataframe.py

Integration-style tests for your custom DataFrame class.

────────────────────────────────────────────
WHAT THIS TEST FILE DOES
────────────────────────────────────────────
This script acts like an “integration harness” for your mini DataFrame system.
It loads your cleaned CSV file, constructs a DataFrame, and verifies that the
core methods — filter, project, group_by, sort_by, join, and error handling —
all work correctly together.
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
# 1. Imports
# ---------------------------------------------------------------
from src.dataframe import DataFrame
try:
    from src.csv_parser import read_csv
except Exception:
    read_csv = None


# ---------------------------------------------------------------
# 2. Helper functions
# ---------------------------------------------------------------

def choose_col(possible_names, available):
    for name in possible_names:
        if name in available:
            return name
    return None


def preview_rows(rows, n=3, k=8):
    print(f"\nPreview first {n} rows:")
    for i, r in enumerate(rows[:n], 1):
        items = list(r.items())[:k]
        print(f"Row {i}: " + ", ".join([f"{k}={v}" for k, v in items]))


# ---------------------------------------------------------------
# 3. Data loading logic
# ---------------------------------------------------------------
def load_rows():
    data_dir = os.path.join(PROJECT_ROOT, "data")
    clean_path = os.path.join(data_dir, "spotify_clean.csv")
    raw_path = os.path.join(data_dir, "spotify_songs.csv")

    if os.path.exists(clean_path):
        print(f"Using cleaned CSV: {clean_path}")
        if read_csv is None:
            raise RuntimeError("csv_parser.read_csv not available.")
        return read_csv(clean_path)

    print("Cleaned file not found. Falling back to raw CSV.")
    if not os.path.exists(raw_path):
        raise FileNotFoundError(f"Could not find raw file at {raw_path}")
    if read_csv is None:
        raise RuntimeError("csv_parser.read_csv not available.")
    return read_csv(raw_path)


# ---------------------------------------------------------------
# 4. MAIN TEST FUNCTION
# ---------------------------------------------------------------
def main():
    # ---- Load rows ----
    rows = load_rows()
    assert isinstance(rows, list)
    assert len(rows) > 0

    # ---- Build DataFrame ----
    df = DataFrame(rows)
    assert df.count() == len(rows)
    assert isinstance(df.columns, list)

    print(f"\nLoaded {df.count():,} rows with {len(df.columns)} columns.")
    print("Columns:", df.columns[:12], "..." if len(df.columns) > 12 else "")
    preview_rows(df.rows, n=2, k=10)

    # ---- Column detection ----
    artist_col = choose_col(["artist_name", "track_artist", "artist"], df.columns)
    pop_col    = choose_col(["popularity", "track_popularity"], df.columns)
    track_col  = choose_col(["track_name", "name", "song"], df.columns)

    assert artist_col is not None, "Couldn't find an artist column."
    print(f"artist column: {artist_col}")
    if pop_col:
        print(f"popularity column: {pop_col}")
    if track_col:
        print(f"track column: {track_col}")

    # -----------------------------------------------------------
    # 4.1 FILTER TEST
    # -----------------------------------------------------------
    if pop_col:
        print("\n— TEST: filter (popularity > 80) —")
        popular = df.filter(lambda r: isinstance(r.get(pop_col), (int, float)) and r[pop_col] > 80)
        assert popular.count() <= df.count()
        print(f"Filtered rows: {popular.count()}")
        preview_rows(popular.rows, n=2, k=6)

    # -----------------------------------------------------------
    # 4.2 PROJECT TEST
    # -----------------------------------------------------------
    print("\n— TEST: project —")
    cols_to_keep = [c for c in [track_col, artist_col, pop_col] if c]
    proj = df.project(cols_to_keep)
    assert all(set(r.keys()) == set(cols_to_keep) for r in proj.rows[:10])
    print(f"Projected columns: {cols_to_keep}")
    preview_rows(proj.rows, n=2, k=len(cols_to_keep))

    # -----------------------------------------------------------
    # 4.3 GROUP BY TEST
    # -----------------------------------------------------------
    print("\n— TEST: group_by —")
    if pop_col:
        grouped = df.group_by(artist_col, {pop_col: "avg"})
        assert grouped.count() > 0
        print(f"Grouped into {grouped.count()} artist groups.")
        preview_rows(grouped.rows, n=3, k=3)

    # -----------------------------------------------------------
    # 4.4 SORT TEST
    # -----------------------------------------------------------
    print("\n— TEST: sort_by —")
    sort_target = pop_col or artist_col
    sorted_df = df.sort_by(sort_target)
    assert sorted_df.count() == df.count()
    print("sort_by works.")

    # -----------------------------------------------------------
    # 4.5 ERROR TEST
    # -----------------------------------------------------------
    print("\n— TEST: bad projection —")
    raised = False
    try:
        df.project(["__not_real__"])
    except ValueError:
        raised = True
    assert raised
    print("project raised ValueError correctly.")

       # -----------------------------------------------------------
    # 4.6 JOIN TEST (NEW)
    # -----------------------------------------------------------
    print("\n— TEST: join —")

    # Build grouped table (left side)
    if pop_col:
        grouped_for_join = df.group_by(artist_col, {pop_col: "avg"})
    else:
        grouped_for_join = df.group_by(artist_col, {})

    # Build small right-hand table with fake metadata
    # Only keep artist values that are strings to avoid type issues
    artist_values = {
        r.get(artist_col)
        for r in df.rows
        if isinstance(r.get(artist_col), str) and r.get(artist_col)
    }

    # Take a small sample of artists for the join test
    sample_artists = sorted(artist_values)[:5]
    if not sample_artists:
        raise AssertionError("No valid string artist values found for join test.")

    right_rows = [{artist_col: a, "dummy_meta": "Testland"} for a in sample_artists]
    right_df = DataFrame(right_rows)

    # Perform join
    joined = grouped_for_join.join(right_df, left_on=artist_col, right_on=artist_col)

    # Basic assertions
    assert joined.count() <= len(sample_artists), "Join produced more rows than sample artists."
    assert "dummy_meta" in joined.columns, "Joined DataFrame missing metadata column."

    joined_artists = {r.get(artist_col) for r in joined.rows}
    assert joined_artists.issubset(set(sample_artists)), \
        "Joined artist keys are not a subset of the sample artists."

    print(f"join produced {joined.count()} rows.")
    preview_rows(joined.rows, n=3, k=4)

    # -----------------------------------------------------------
    # 4.7 FINAL STATUS
    # -----------------------------------------------------------
    print("\nAll DataFrame tests completed successfully!\n")


# ---------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------
if __name__ == "__main__":
    main()
