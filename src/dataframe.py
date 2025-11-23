"""
DataFrame: A lightweight in-memory data manipulation engine
Implements core SQL-like operations:
- SELECT (projection)
- WHERE (filtering)
- GROUP BY with aggregation
- SORT and COUNT
"""

from typing import List, Dict, Any, Callable, Optional
from collections import defaultdict
import statistics

# ---------------------------------------------------------------
# CLASS: DataFrame
# PURPOSE: Provides SQL-like operations on parsed CSV data
# ---------------------------------------------------------------
class DataFrame:
    def __init__(self, rows: List[Dict[str, Any]]):
        """
        Initialize with a list of dictionaries (rows),
        each representing one record in the dataset.
        """
        self.rows = rows
        self.columns = list(rows[0].keys()) if rows else []

    # -----------------------------------------------------------
    # METHOD: head
    # PURPOSE: Display the first N rows for inspection.
    # -----------------------------------------------------------
    def head(self, n: int = 5):
        print(f"\nShowing first {n} rows:")
        for row in self.rows[:n]:
            print(row)
        print("-" * 40)

    # -----------------------------------------------------------
    # METHOD: filter
    # PURPOSE: Keep only rows that satisfy a condition (WHERE clause).
    # Example:
    #   df.filter(lambda r: r["popularity"] > 80)
    # -----------------------------------------------------------
    def filter(self, condition: Callable[[Dict[str, Any]], bool]):
        filtered_rows = [row for row in self.rows if condition(row)]
        print(f"Filtered rows: {len(filtered_rows)} out of {len(self.rows)}")
        return DataFrame(filtered_rows)

    # -----------------------------------------------------------
    # METHOD: project
    # PURPOSE: Select only certain columns (SELECT clause).
    # Example:
    #   df.project(["track_name", "artist_name"])
    # -----------------------------------------------------------
    def project(self, columns: List[str]):
        for col in columns:
            if col not in self.columns:
                raise ValueError(f"Column '{col}' not found in dataset.")
        projected = [{col: row[col] for col in columns} for row in self.rows]
        print(f"Projected columns: {columns}")
        return DataFrame(projected)

    # -----------------------------------------------------------
    # METHOD: group_by
    # PURPOSE: Group rows by one column and aggregate others.
    # Example:
    #   df.group_by("artist_name", {"popularity": "avg"})
    # -----------------------------------------------------------
    def group_by(self, group_col: str, agg_map: Dict[str, str]):
        groups = defaultdict(list)
        for row in self.rows:
            key = row[group_col]
            groups[key].append(row)

        result = []
        for key, rows in groups.items():
            agg_result = {group_col: key}
            for col, func in agg_map.items():
                values = [r[col] for r in rows if isinstance(r[col], (int, float))]
                if not values:
                    agg_result[col] = None
                    continue

                if func == "avg":
                    agg_result[col] = statistics.mean(values)
                elif func == "sum":
                    agg_result[col] = sum(values)
                elif func == "max":
                    agg_result[col] = max(values)
                elif func == "min":
                    agg_result[col] = min(values)
                elif func == "count":
                    agg_result[col] = len(values)
                else:
                    raise ValueError(f"Unsupported aggregation: {func}")

            result.append(agg_result)

        print(f"Grouped by '{group_col}' with {len(result)} groups.")
        return DataFrame(result)

    # -----------------------------------------------------------
    # METHOD: sort_by
    # PURPOSE: Sort the rows by one column (ASC or DESC).
    # Example:
    #   df.sort_by("popularity", reverse=True)
    # -----------------------------------------------------------
    def sort_by(self, col: str, reverse: bool = False):
        if col not in self.columns:
            raise ValueError(f"Column '{col}' not found.")
        sorted_rows = sorted(self.rows, key=lambda r: (r[col] is None, r[col]), reverse=reverse)
        print(f"Sorted by '{col}' ({'DESC' if reverse else 'ASC'})")
        return DataFrame(sorted_rows)

    # -----------------------------------------------------------
    # METHOD: count
    # PURPOSE: Return the total number of rows in the dataset.
    # -----------------------------------------------------------
    def count(self):
        return len(self.rows)

    # -----------------------------------------------------------
    # METHOD: join
    # PURPOSE: Inner join with another DataFrame on key columns.
    #
    # This is an INNER JOIN:
    #   - Only rows with matching key values on both sides appear
    #     in the result.
    # -----------------------------------------------------------
    def join(
        self,
        other: "DataFrame",
        left_on: str,
        right_on: Optional[str] = None,
        suffixes=("_left", "_right"),
    ) -> "DataFrame":
        if right_on is None:
            right_on = left_on

        if not self.rows or not other.rows:
            return DataFrame([])

        if left_on not in self.columns:
            raise ValueError(f"Column '{left_on}' not found in left DataFrame.")
        if right_on not in other.columns:
            raise ValueError(f"Column '{right_on}' not found in right DataFrame.")

        # Build lookup from right-hand key to list of rows
        index: Dict[Any, List[Dict[str, Any]]] = {}
        for r in other.rows:
            key = r.get(right_on)
            index.setdefault(key, []).append(r)

        joined_rows: List[Dict[str, Any]] = []

        for l in self.rows:
            key = l.get(left_on)
            matches = index.get(key, [])
            for r in matches:
                combined: Dict[str, Any] = {}

                # copy left row
                for col in self.columns:
                    combined[col] = l.get(col)

                # copy right row (avoid duplicate key col; handle collisions)
                for col in other.columns:
                    if col == right_on:
                        # key already represented from left
                        continue
                    if col in combined:
                        combined[col + suffixes[1]] = r.get(col)
                    else:
                        combined[col] = r.get(col)

                joined_rows.append(combined)

        print(
            f"Joined {len(self.rows)} left rows with "
            f"{len(other.rows)} right rows; result has {len(joined_rows)} rows."
        )
        return DataFrame(joined_rows)
