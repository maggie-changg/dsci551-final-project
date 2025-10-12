"""
services.py
Business logic using your csv_parser and DataFrame:
- load rows
- compute UI options (genres, subgenres, tempo buckets, month/year lists)
- apply filters via DataFrame.filter
- project via DataFrame.project
- group_by via DataFrame.group_by
- sort_by via DataFrame.sort_by
"""

import os
from typing import Dict, Any, List, Optional, Tuple

from csv_parser import read_csv
from dataframe import DataFrame
from filters import (
    unique_non_null,
    subgenres_for_genre,
    parse_range_or_any,
    month_to_int_or_any,
    parse_year_or_any,
    build_predicate,
)

PROJECT_COLS = ["track_name", "track_artist", "track_album_name", "track_album_release_date"]

def load_rows(path: str) -> List[Dict[str, Any]]:
    if not os.path.exists(path):
        raise FileNotFoundError(f"Missing file: {path}")
    return read_csv(path)

def build_options(rows: List[Dict[str, Any]], genre_choice: Optional[str]):
    """Compute dropdown options for the UI."""
    # Popularity buckets 0-10 ... 90-100
    pop_options = ["ANY"] + [f"{i}-{i+10}" for i in range(0, 100, 10)]

    # Float buckets 0.0-0.1 ... 0.9-1.0
    float_buckets = ["ANY"] + [f"{i/10:.1f}-{(i+1)/10:.1f}" for i in range(0, 10)]

    # Genres & subgenres
    genres = ["ANY"] + unique_non_null(rows, "playlist_genre")
    if (genre_choice and genre_choice != "ANY"):
        sub_opts = ["ANY"] + subgenres_for_genre(rows, genre_choice)
    else:
        sub_opts = ["ANY"] + unique_non_null(rows, "playlist_subgenre")

    # Tempo buckets (20 BPM steps) based on dataset min/max
    tempos = [r.get("tempo") for r in rows if isinstance(r.get("tempo"), (int, float))]
    if tempos:
        tmin, tmax = int(min(tempos)), int(max(tempos))
        tempo_opts = ["ANY"]
        start = (tmin // 20) * 20
        end = ((tmax + 19) // 20) * 20
        for a in range(start, end, 20):
            tempo_opts.append(f"{a}-{a+20}")
    else:
        tempo_opts = ["ANY"]

    # Month labels
    month_labels = ["ANY",
        "Jan (1)","Feb (2)","Mar (3)","Apr (4)","May (5)","Jun (6)",
        "Jul (7)","Aug (8)","Sep (9)","Oct (10)","Nov (11)","Dec (12)"
    ]

    # Year options from data
    years = sorted({int(r["track_album_release_date"][:4]) for r in rows
                    if isinstance(r.get("track_album_release_date"), str)
                    and len(r["track_album_release_date"]) >= 4
                    and r["track_album_release_date"][:4].isdigit()})
    year_opts = ["ANY"] + [str(y) for y in years]

    return {
        "pop_options": pop_options,
        "float_buckets": float_buckets,
        "genres": genres,
        "subgenres": sub_opts,
        "tempo_options": tempo_opts,
        "month_labels": month_labels,
        "year_options": year_opts,
    }

def apply_pipeline(
    rows: List[Dict[str, Any]],
    pop_bucket: Optional[str],
    genre_choice: Optional[str],
    subgenre_choice: Optional[str],
    dance_bucket: Optional[str],
    energy_bucket: Optional[str],
    tempo_bucket: Optional[str],
    live_bucket: Optional[str],
    month_label: Optional[str],
    year_label: Optional[str],
    sort_choice: Optional[str],  # 'popularity' | 'danceability' | None
):
    """
    Full pipeline using your DataFrame:
      - filter (WHERE)
      - project (SELECT)
      - group_by artist (GROUP BY ... AVG)
      - sort_by chosen aggregate (ORDER BY ASC)
    Returns (df_raw, df_filtered, df_projected, df_grouped, df_sorted)
    """
    df_raw = DataFrame(rows)

    # Resolve UI selections to typed filters (None == ANY)
    pop_range   = parse_range_or_any(pop_bucket)
    dance_range = parse_range_or_any(dance_bucket)
    energy_range= parse_range_or_any(energy_bucket)
    tempo_range = parse_range_or_any(tempo_bucket)
    live_range  = parse_range_or_any(live_bucket)
    month_val   = month_to_int_or_any(month_label)
    year_val    = parse_year_or_any(year_label)

    genre_val    = None if (not genre_choice or genre_choice == "ANY") else genre_choice
    subgenre_val = None if (not subgenre_choice or subgenre_choice == "ANY") else subgenre_choice

    # WHERE
    predicate = build_predicate(
        pop_range, genre_val, subgenre_val,
        dance_range, energy_range, tempo_range, live_range,
        month_val, year_val
    )
    df_filtered = df_raw.filter(predicate)

    # SELECT
    df_projected = df_filtered.project(PROJECT_COLS)

    # GROUP BY (avg popularity & avg danceability per artist)
    agg_map = {"track_popularity": "avg", "danceability": "avg"}
    df_grouped = df_filtered.group_by("track_artist", agg_map)

    # ORDER BY (ascending) on selected aggregate
    sort_key = None
    if sort_choice == "popularity":
        sort_key = "track_popularity"
    elif sort_choice == "danceability":
        sort_key = "danceability"

    df_sorted = df_grouped.sort_by(sort_key, reverse=False) if sort_key else df_grouped

    return df_raw, df_filtered, df_projected, df_grouped, df_sorted
