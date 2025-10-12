"""
app_streamlit.py — UI-only Streamlit app
- Imports functions from your modules (no data logic here).
Run:
    streamlit run src/app_streamlit.py
"""

import os
import sys
import streamlit as st

# Make src importable
THIS_DIR = os.path.dirname(__file__)
PROJECT_ROOT = os.path.abspath(os.path.join(THIS_DIR, ".."))
sys.path.insert(0, os.path.join(PROJECT_ROOT, "src"))

from services import (
    load_rows, build_options, apply_pipeline, PROJECT_COLS
)
from utils_io import to_csv_string

DATA_DIR = os.path.join(PROJECT_ROOT, "data")
SUBSET_PATH = os.path.join(DATA_DIR, "spotify_subset.csv")

st.set_page_config(page_title="Spotify Mini-DB App (Your Functions)", layout="wide")
st.title("🎵 Spotify Mini-DB App (UI only)")
st.caption("This UI calls your parser and DataFrame functions via modular services. No data logic in the UI.")

# 1) Load
if not os.path.exists(SUBSET_PATH):
    st.error(f"Missing file: {SUBSET_PATH}. Run your subset script first.")
    st.stop()

rows = load_rows(SUBSET_PATH)  # csv_parser.read_csv (under the hood in services)

# 2) Sidebar filters (options provided by services)
st.sidebar.header("Filters (ANY = no filter)")
# Build options (genres/subgenres/tempos are computed off dataset)
opt = build_options(rows, genre_choice=None)

pop_bucket   = st.sidebar.selectbox("Track popularity", opt["pop_options"], index=0)
dance_bucket = st.sidebar.selectbox("Danceability", opt["float_buckets"], index=0)
energy_bucket= st.sidebar.selectbox("Energy", opt["float_buckets"], index=0)
live_bucket  = st.sidebar.selectbox("Liveness", opt["float_buckets"], index=0)
tempo_bucket = st.sidebar.selectbox("Tempo (BPM)", opt["tempo_options"], index=0)

genre_choice = st.sidebar.selectbox("Playlist genre", opt["genres"], index=0)
# refresh subgenre options based on chosen genre
opt2 = build_options(rows, genre_choice=genre_choice)
subgenre_choice = st.sidebar.selectbox("Playlist subgenre", opt2["subgenres"], index=0)

month_label = st.sidebar.selectbox("Release month", opt["month_labels"], index=0)
year_label  = st.sidebar.selectbox("Release year", opt["year_options"], index=0)

sort_choice = st.sidebar.selectbox("Sort grouped summary (ascending)", ["None", "popularity", "danceability"], index=0)
sort_choice = None if sort_choice == "None" else sort_choice

# 3) Apply pipeline
df_raw, df_filtered, df_projected, df_grouped, df_sorted = apply_pipeline(
    rows,
    pop_bucket, genre_choice, subgenre_choice,
    dance_bucket, energy_bucket, tempo_bucket, live_bucket,
    month_label, year_label, sort_choice
)

# 4) Show data loading & counts (uses your DataFrame)
st.header("1) Data Loading & Parsing")
st.success(f"Loaded & parsed: **{SUBSET_PATH}**")
st.write(f"Rows: **{df_raw.count()}**  |  Columns: **{len(df_raw.columns)}**")
with st.expander("Preview schema & first 5 rows"):
    st.write("Columns:", df_raw.columns)
    st.json(df_raw.rows[:5])

# 5) WHERE (filter)
st.header("2) WHERE (Filter)")
st.write(f"Rows after filter: **{df_filtered.count()}** / {df_raw.count()}")
with st.expander("Show example filtered rows (first 10)"):
    st.json(df_filtered.rows[:10])

# 6) SELECT (project)
st.header("3) SELECT (Project)")
st.caption("Always showing: track_name, track_artist, track_album_name, track_album_release_date")
st.dataframe(df_projected.rows, use_container_width=True, height=420)
st.write(f"Projected rows: **{df_projected.count()}**")

csv_data = to_csv_string(df_projected.rows, PROJECT_COLS)
st.download_button("⬇️ Download projected results (CSV)", data=csv_data.encode("utf-8"),
                   file_name="spotify_filtered_projected.csv", mime="text/csv")

# 7) GROUP BY + ORDER BY
st.header("4) GROUP BY (by track_artist) + Aggregation")
st.caption("Aggregations: avg(track_popularity), avg(danceability) per artist")
st.dataframe(df_grouped.rows, use_container_width=True, height=420)
st.write(f"Artist groups: **{df_grouped.count()}**")

st.header("5) ORDER BY (Sort)")
if sort_choice:
    st.caption(f"Sorted ascending by avg {sort_choice}")
st.dataframe(df_sorted.rows, use_container_width=True, height=420)

# 8) Trace (for your report)
st.header("6) Operation Trace (for your report)")
st.markdown("""
- **Load & Parse**: `rows = read_csv('data/spotify_subset.csv')` (via `services.load_rows`)
- **Construct**: `df_raw = DataFrame(rows)`
- **WHERE**: `df_filtered = df_raw.filter(predicate)` (predicate built in `filters.build_predicate`)
- **SELECT**: `df_projected = df_filtered.project([...])`
- **GROUP BY**: `df_grouped = df_filtered.group_by('track_artist', {'track_popularity':'avg','danceability':'avg'})`
- **ORDER BY**: `df_sorted = df_grouped.sort_by('<chosen_agg>', reverse=False)`
- **COUNT**: `df_raw.count(), df_filtered.count(), df_grouped.count()`
""")
