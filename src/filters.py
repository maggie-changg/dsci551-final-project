"""
filters.py
Parsing utilities and predicate builder for filtering rows.
No Streamlit or I/O here.
"""

from typing import Dict, Any, Optional, Tuple, List

# ---------- range parsing ----------

def parse_range_or_any(s: Optional[str]) -> Optional[Tuple[float, float]]:
    """
    Accepts "a-b" (ints/floats) → (low, high); ANY/blank/invalid → None (no filter).
    """
    if not s:
        return None
    s = s.strip()
    if s.lower() == "any":
        return None
    try:
        a, b = s.split("-")
        low, high = float(a.strip()), float(b.strip())
        if high < low:
            return None
        return (low, high)
    except Exception:
        return None

def in_bucket(val: Optional[float], rng: Tuple[float, float], top_inclusive=False) -> bool:
    """
    Default [low, high). If top_inclusive=True use [low, high].
    """
    if val is None:
        return False
    low, high = rng
    if top_inclusive:
        return (val >= low) and (val <= high)
    return (val >= low) and (val < high)

# ---------- month/year parsing ----------

def month_to_int_or_any(m: Optional[str]) -> Optional[int]:
    """
    Accept 1-12 or labels like "Jan (1)". ANY/blank/invalid → None.
    """
    if not m:
        return None
    s = str(m).strip().lower()
    if s == "any":
        return None
    if s.isdigit():
        n = int(s)
        return n if 1 <= n <= 12 else None
    if "(" in s and s.endswith(")"):
        try:
            return int(s[s.rfind("(")+1:-1])
        except Exception:
            return None
    # also accept short/long names
    name_to_num = {
        "jan": 1, "january": 1,
        "feb": 2, "february": 2,
        "mar": 3, "march": 3,
        "apr": 4, "april": 4,
        "may": 5,
        "jun": 6, "june": 6,
        "jul": 7, "july": 7,
        "aug": 8, "august": 8,
        "sep": 9, "sept": 9, "september": 9,
        "oct": 10, "october": 10,
        "nov": 11, "november": 11,
        "dec": 12, "december": 12,
    }
    return name_to_num.get(s)

def parse_year_or_any(y: Optional[str]) -> Optional[int]:
    """
    Accept 4-digit year; ANY/blank/invalid → None.
    """
    if not y:
        return None
    s = str(y).strip().lower()
    if s == "any":
        return None
    return int(s) if s.isdigit() and len(s) == 4 else None

# ---------- options helpers ----------

def unique_non_null(rows: List[Dict[str, Any]], col: str) -> List[str]:
    return sorted({r.get(col) for r in rows if r.get(col) not in (None, "")})

def subgenres_for_genre(rows: List[Dict[str, Any]], genre: Optional[str]) -> List[str]:
    if not genre:
        return []
    subs = {
        r.get("playlist_subgenre")
        for r in rows
        if r.get("playlist_genre") == genre and r.get("playlist_subgenre") not in (None, "")
    }
    return sorted(subs)

# ---------- predicate builder ----------

def build_predicate(
    pop_range: Optional[Tuple[float, float]],
    genre: Optional[str],
    subgenre: Optional[str],
    dance_range: Optional[Tuple[float, float]],
    energy_range: Optional[Tuple[float, float]],
    tempo_range: Optional[Tuple[float, float]],
    live_range: Optional[Tuple[float, float]],
    month: Optional[int],
    year: Optional[int],
):
    """
    Returns a function(row) -> bool that applies AND logic across all filters.
    None means ANY (skip that check).
    """
    top_pop = pop_range is not None and abs(pop_range[1] - 100.0) < 1e-9
    top_01  = lambda rng: (rng is not None) and abs(rng[1] - 1.0) < 1e-9

    def _pred(row: Dict[str, Any]) -> bool:
        # 1) popularity
        if pop_range is not None and not in_bucket(row.get("track_popularity"), pop_range, top_inclusive=top_pop):
            return False
        # 2) genre
        if genre and row.get("playlist_genre") != genre:
            return False
        # 3) subgenre
        if subgenre and row.get("playlist_subgenre") != subgenre:
            return False
        # 4) danceability
        if dance_range is not None and not in_bucket(row.get("danceability"), dance_range, top_inclusive=top_01(dance_range)):
            return False
        # 5) energy
        if energy_range is not None and not in_bucket(row.get("energy"), energy_range, top_inclusive=top_01(energy_range)):
            return False
        # 6) tempo (inclusive upper bound for convenience)
        if tempo_range is not None and not in_bucket(row.get("tempo"), tempo_range, top_inclusive=True):
            return False
        # 7) liveness
        if live_range is not None and not in_bucket(row.get("liveness"), live_range, top_inclusive=top_01(live_range)):
            return False
        # 8) month/year — expects "YYYY-MM-DD"
        if (month is not None) or (year is not None):
            ds = row.get("track_album_release_date")
            m2 = y2 = None
            if isinstance(ds, str) and len(ds) >= 7 and ds[4] == "-":
                parts = ds.split("-")
                if len(parts) >= 2 and parts[0].isdigit() and parts[1].isdigit():
                    y2, m2 = int(parts[0]), int(parts[1])
            if (month is not None) and (m2 != month):
                return False
            if (year is not None) and (y2 != year):
                return False
        return True

    return _pred
