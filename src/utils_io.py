"""
utils_io.py
Lightweight CSV string writer for download (no pandas).
"""

import io
from typing import List, Dict, Any

def to_csv_string(rows: List[Dict[str, Any]], fields: List[str]) -> str:
    out = io.StringIO()
    out.write(",".join(fields) + "\n")
    for r in rows:
        vals = []
        for f in fields:
            s = "" if r.get(f) is None else str(r.get(f))
            if ("," in s) or ('"' in s):
                s = '"' + s.replace('"', '""') + '"'
            vals.append(s)
        out.write(",".join(vals) + "\n")
    return out.getvalue()
