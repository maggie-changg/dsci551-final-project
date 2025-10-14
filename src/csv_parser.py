"""
CSV parser v4:
- Handles quoted fields and commas inside quotes
- Converts ints/floats and nulls
- Validates column structure and skips malformed rows
- Automatically removes duplicate rows
- Writes cleaned CSV to data/spotify_clean.csv
- Tracks and reports cleaning statistics
"""

from typing import List, Dict, Any
import os
import csv

# ---------------------------------------------------------------
# CONSTANTS
# ---------------------------------------------------------------
_NULLS = {"", "na", "n/a", "null", "none"}  # Strings considered as null/empty

# ---------------------------------------------------------------
# GLOBAL COUNTERS (for stats)
# ---------------------------------------------------------------
stats = {
    "total_rows": 0,
    "total_fields": 0,
    "num_nulls": 0,
    "num_ints": 0,
    "num_floats": 0,
    "num_strings": 0,
    "malformed_rows": 0,
    "duplicates_removed": 0,
}

# ---------------------------------------------------------------
# FUNCTION: _coerce
# PURPOSE: Converts each field (string) from the CSV into its proper Python type.
#          - Turns numbers into int/float
#          - Turns null-like strings into None
#          - Keeps other text fields as strings
# ---------------------------------------------------------------
def _coerce(tok: str):
    t = tok.strip()
    stats["total_fields"] += 1  # Count every processed field

    if t.lower() in _NULLS:
        stats["num_nulls"] += 1
        return None

    # Try integer
    if t and (t.isdigit() or (t[0] == "-" and t[1:].isdigit())):
        try:
            stats["num_ints"] += 1
            return int(t)
        except ValueError:
            pass

    # Try float
    try:
        val = float(t)
        stats["num_floats"] += 1
        return val
    except ValueError:
        stats["num_strings"] += 1
        return t


# ---------------------------------------------------------------
# FUNCTION: _split_csv_line
# PURPOSE: Splits one line of text from a CSV into separate fields.
#          It handles tricky cases like commas inside quotes and escaped quotes.
# ---------------------------------------------------------------
def _split_csv_line(line: str, delimiter: str = ",") -> List[str]:
    fields, cur, in_quotes = [], [], False
    i, n = 0, len(line)

    while i < n:
        ch = line[i]
        if ch == '"':
            if in_quotes and i + 1 < n and line[i + 1] == '"':
                cur.append('"')
                i += 2
                continue
            in_quotes = not in_quotes
            i += 1
            continue

        if ch == delimiter and not in_quotes:
            fields.append("".join(cur))
            cur = []
            i += 1
            continue

        if ch == "\n" and not in_quotes:
            i += 1
            continue

        cur.append(ch)
        i += 1

    fields.append("".join(cur))
    return fields


# ---------------------------------------------------------------
# FUNCTION: read_csv
# PURPOSE: Reads the entire CSV file line by line, converts each field using
#          _coerce(), and builds a list of dictionaries (rows).
#          Skips malformed lines that have a mismatched number of fields.
# ---------------------------------------------------------------
def read_csv(path: str, delimiter: str = ",") -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    with open(path, "r", encoding="utf-8") as f:
        header_line = f.readline()
        if not header_line:
            return rows

        header = _split_csv_line(header_line.rstrip("\n"), delimiter)

        for line in f:
            parts = _split_csv_line(line.rstrip("\n"), delimiter)
            if len(parts) != len(header):
                stats["malformed_rows"] += 1
                continue  # skip bad line

            row = {h: _coerce(v) for h, v in zip(header, parts)}
            rows.append(row)
            stats["total_rows"] += 1

    return rows


# ---------------------------------------------------------------
# FUNCTION: remove_duplicates
# PURPOSE: Removes duplicate rows (keeps first occurrence).
# ---------------------------------------------------------------
def remove_duplicates(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    unique_rows = []
    seen = set()
    for row in rows:
        row_tuple = tuple(sorted(row.items()))
        if row_tuple not in seen:
            seen.add(row_tuple)
            unique_rows.append(row)
        else:
            stats["duplicates_removed"] += 1
    return unique_rows


# ---------------------------------------------------------------
# FUNCTION: write_clean_csv
# PURPOSE: Takes parsed/cleaned rows and writes them to a new CSV file.
# ---------------------------------------------------------------
def write_clean_csv(rows: List[Dict[str, Any]], output_path: str):
    if not rows:
        print("[WARNING] No data to write.")
        return

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        for row in rows:
            clean_row = {k: ("" if v is None else v) for k, v in row.items()}
            writer.writerow(clean_row)
    print(f"Cleaned CSV written to: {output_path}")


# ---------------------------------------------------------------
# FUNCTION: print_stats
# PURPOSE: Prints a summary of how many fields were cleaned or converted.
# ---------------------------------------------------------------
def print_stats(rows: List[Dict[str, Any]]):
    print("-" * 40)
    print("\n--Cleaning Summary--")
    print(f"Total rows processed: {stats['total_rows']:,}")
    print(f"Total fields processed: {stats['total_fields']:,}")
    print(f"Malformed rows skipped: {stats['malformed_rows']:,}")
    print(f"Null values replaced: {stats['num_nulls']:,}")
    print(f"Converted to int: {stats['num_ints']:,}")
    print(f"Converted to float: {stats['num_floats']:,}")
    print(f"Kept as string: {stats['num_strings']:,}")
    print(f"Duplicate rows removed: {stats['duplicates_removed']:,}")
    clean_pct = (1 - stats["num_nulls"] / stats["total_fields"]) * 100 if stats["total_fields"] else 0
    print(f"Clean percentage (non-null): {clean_pct:.2f}%")
    print()


# ---------------------------------------------------------------
# MAIN EXECUTION
# ---------------------------------------------------------------
if __name__ == "__main__":
    csv_path = os.path.join("data", "spotify_songs.csv")
    clean_path = os.path.join("data", "spotify_clean.csv")

    if not os.path.exists(csv_path):
        print(f"[ERROR] CSV file not found at: {csv_path}")
    else:
        print(f"Reading CSV from: {csv_path}")

        rows = read_csv(csv_path)
        print(f"Parsed {len(rows)} rows successfully.")

        if rows:
            print("Sample row:")
            for k, v in list(rows[0].items())[:10]:
                print(f"  {k}: {v}")

            # Remove duplicates before writing
            rows = remove_duplicates(rows)

            write_clean_csv(rows, clean_path)
            print_stats(rows)
