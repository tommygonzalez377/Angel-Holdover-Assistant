#!/usr/bin/env python3
"""
Mica Booking Updater
Automates updating Hold/Final statuses (and screening types) in demo.mica.co.

Usage:
  python mica_update.py --production "FILM (2026)" --contact "Ashley Hensley" booking.csv
"""

import argparse
import csv
import os
import re
import sys
import time
from pathlib import Path
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout

try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).parent / ".env")
except ImportError:
    pass

MICA_URLS = {
    "demo": "https://demo.mica.co/bookings/holdovers",
    "prod": "https://app.mica.co/bookings/holdovers",
}
OUTPUT_DIR = Path(__file__).parent / "output"

# Set by run_mica_update() so helper functions can reference the active URL/auth file
_active_mica_url:  str  = MICA_URLS["demo"]
_active_auth_file: Path = OUTPUT_DIR / "mica_auth_demo.json"

MICA_USER = os.getenv("MICA_USERNAME", "")
MICA_PASS = os.getenv("MICA_PASSWORD", "")

_SERVER_MODE  = bool(os.getenv("SERVER_MODE"))
_HEADLESS     = _SERVER_MODE
_SLOW_MO      = 0 if _SERVER_MODE else 150
_BROWSER_ARGS = [
    "--disable-gpu", "--no-sandbox",
    "--disable-blink-features=AutomationControlled",
    "--disable-dev-shm-usage",
    "--disable-web-security",
    "--no-first-run",
    "--no-default-browser-check",
    "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
] if _SERVER_MODE else ["--start-maximized", "--disable-gpu"]

# Known booking-name → Mica venue name aliases (case-insensitive).
# Add entries here whenever a booking uses a shortened or different name than Mica.
VENUE_ALIASES: dict[str, str] = {
    "west chester 18":               "amc west chester township 18",
    "fairfield stm 16 & imax":       "regal edwards fairfield 16",
    "stockton cty ctr stm 16 & imax": "regal stockton city centre 16",
    "oviedo mall stm 22":            "regal oviedo marketplace 22",
    "regal naples 4dx & imax":       "regal hollywood cinema naples 20",
    "la habra stm 16":               "regal la habra marketplace 16",
    # ── Andy Anderson (Bay Area Cinemark circuit) ────────────────────────────
    "san mateo 12":      "Cinemark Century Downtown San Mateo 12",
    # ── Owen Simonds ──────────────────────────────────────────────────────────
    "stars cinema 6":    "Stars Theater 7",
    # ── Diane Johnson (Cinergy circuit old names) ─────────────────────────────
    "driftwood 6":       "Cinergy Granbury 6",
    "driftwood 8":       "Cinergy Marble Falls 8",
    # ── Mary Ann B. Silk (Golden Ticket / mixed independent circuit) ─────────
    "aberdeen":          "Golden Ticket Cinemas Aberdeen 5",
    "ale house":         "Golden Ticket Cinemas Greensboro Ale House 10",
    "ale house plf":     "Golden Ticket Cinemas Greensboro Ale House 10",
    "bloomington":       "Golden Ticket Bloomington Ale House 10",
    "bluefield":         "Golden Ticket Cinemas Bluefield 8",
    "clarion":           "Golden Ticket Clarion 5",
    "cloquet":           "Premiere Cloquet 6",
    "dickinson":         "Golden Ticket Dickinson 3",
    "dublin":            "Golden Ticket Cinemas Dublin 6",
    "dubois":            "Golden Ticket Cinemas DuBois 5",
    "greenville":        "Golden Ticket Cinemas Greenville Grande 14",
    "greenville plf":    "Golden Ticket Cinemas Greenville Grande 14",
    "harrison":          "Golden Ticket Cinemas Harrison 8",
    "hastings":          "Golden Ticket Cinemas Hastings 3",
    "jamestown":         "Bison 6 Cinema",
    "kearney":           "Golden Ticket Cinemas Hilltop 4",
    "lenoir":            "Golden Ticket Cinemas Twin 2",
    "madisonville":      "Golden Ticket Cinemas Capitol 8",
    "meridian":          "Golden Ticket Cinemas Meridian 6",
    "middlesboro":       "Golden Ticket Cinemas Middlesboro 4",
    "north platte":      "Golden Ticket Cinemas Platte River 6",
    "onamia":            "Grand Makwa Cinema Onamia 4",
    "rapid city":        "Golden Ticket Cinemas Rushmore 7",
    "rhinelander":       "Rouman Cinema Rhinelander 6",
    "scottsbluff":       "Golden Ticket Cinemas Reel Lux 6 *temp 4*",
    "shawnee":           "Golden Ticket Cinemas Shawnee 6",
    "shawnee plf":       "Golden Ticket Cinemas Shawnee 6",
    "sioux falls":       "West Mall 7 Theatres",
    "st. clairsville":   "Golden Tickets St. Clairsville 5",
    "waynesville":       "Smoky Mountain Cinema 3",
    "willmar":           "Golden Ticket Cinemas Kandi 6",
    "worthington":       "New Grand Theatre",
    # Other Mary Ann venues (for future bookings using city name)
    "minot":             "Oak Park Theater 1",
    "spooner":           "Palace Spooner 2",
    "valley city":       "Valley Twin Cinema 2",
    "luverne":           "Verne Drive-in Luverne 1",
}

# Booking phrase substring → Mica screening type label
# Checked longest-match first so "hold/shows" beats "hold", "1 mat" beats "mat"
PHRASE_TO_SCREENING: list[tuple[str, str]] = [
    ("hold/shows",  "Alternating"),
    ("mats+ee",     "Alternating"),
    ("lm+ee",       "Alternating"),
    ("em+le",       "Alternating"),
    ("em+ee",       "Alternating"),   # Early Mats + Early Evenings
    ("em + ee",     "Alternating"),
    ("matinee shows","Alternating"),
    ("1 mat",       "Single Matinee"),
    ("mats",        "Multiple Matinees"),
    ("em",          "Multiple Matinees"),
    ("lm",          "Late"),
    ("mat",         "Single Matinee"),
    ("prime",       "Prime"),
    ("split",       "Alternating"),
    ("alt",         "Alternating"),
    ("shows",       "Alternating"),
]


def log(msg: str):
    print(msg, flush=True)


def get_screening_type(phrase: str) -> str | None:
    """Map booking phrase → Mica screening type label, or None if default (Clean)."""
    pl = (phrase or "").lower().strip()
    for key, val in PHRASE_TO_SCREENING:
        if key in pl:
            return val
    return None  # default is Clean — no change needed


# ── Exhibitor Ref ID → Venue lookup (Gundrum ID# grid format) ──────────────
_MASTER_REF_LOOKUP: dict[tuple[str, str], str] = {}

def _load_master_ref_lookup() -> dict[tuple[str, str], str]:
    global _MASTER_REF_LOOKUP
    if _MASTER_REF_LOOKUP:
        return _MASTER_REF_LOOKUP
    import csv as _csv_ml
    master_path = Path(__file__).parent / "master_list_cache.csv"
    if not master_path.exists():
        log("  [ref-lookup] master_list_cache.csv not found — ID lookup unavailable")
        return _MASTER_REF_LOOKUP
    with open(master_path, newline="", encoding="utf-8-sig") as _f:
        for _row in _csv_ml.DictReader(_f):
            _ref  = _row.get("Exhibitor's Ref ID", "").strip()
            _vn   = _row.get("Venue", "").strip()
            _city = _row.get("City", "").strip().lower()
            if _ref and _vn and _city:
                _MASTER_REF_LOOKUP[(_ref, _city)] = _vn
    log(f"  [ref-lookup] loaded {len(_MASTER_REF_LOOKUP)} entries")
    return _MASTER_REF_LOOKUP


# ── City+State → [venue names] lookup (Glen Parham / GTC format) ────────────
_MASTER_CITY_STATE_LOOKUP: dict[tuple[str, str], list[str]] = {}

# US state full name → 2-letter abbreviation (lowercase)
_STATE_FULL_TO_ABBREV: dict[str, str] = {
    'alabama': 'al', 'alaska': 'ak', 'arizona': 'az', 'arkansas': 'ar',
    'california': 'ca', 'colorado': 'co', 'connecticut': 'ct', 'delaware': 'de',
    'florida': 'fl', 'georgia': 'ga', 'hawaii': 'hi', 'idaho': 'id',
    'illinois': 'il', 'indiana': 'in', 'iowa': 'ia', 'kansas': 'ks',
    'kentucky': 'ky', 'louisiana': 'la', 'maine': 'me', 'maryland': 'md',
    'massachusetts': 'ma', 'michigan': 'mi', 'minnesota': 'mn', 'mississippi': 'ms',
    'missouri': 'mo', 'montana': 'mt', 'nebraska': 'ne', 'nevada': 'nv',
    'new hampshire': 'nh', 'new jersey': 'nj', 'new mexico': 'nm', 'new york': 'ny',
    'north carolina': 'nc', 'north dakota': 'nd', 'ohio': 'oh', 'oklahoma': 'ok',
    'oregon': 'or', 'pennsylvania': 'pa', 'rhode island': 'ri', 'south carolina': 'sc',
    'south dakota': 'sd', 'tennessee': 'tn', 'texas': 'tx', 'utah': 'ut',
    'vermont': 'vt', 'virginia': 'va', 'washington': 'wa', 'west virginia': 'wv',
    'wisconsin': 'wi', 'wyoming': 'wy', 'district of columbia': 'dc',
    'puerto rico': 'pr',
}

# City name corrections for venues whose master-list city differs from booking city
_BOOKING_CITY_CORRECTIONS: dict[str, str] = {
    "fort benning": "fort benning south  (historical)",
    "st. augustine": "saint augustine",
}

def _load_city_state_lookup() -> dict[tuple[str, str], list[str]]:
    global _MASTER_CITY_STATE_LOOKUP
    if _MASTER_CITY_STATE_LOOKUP:
        return _MASTER_CITY_STATE_LOOKUP
    import csv as _csv_cs
    master_path = Path(__file__).parent / "master_list_cache.csv"
    if not master_path.exists():
        return _MASTER_CITY_STATE_LOOKUP
    with open(master_path, newline="", encoding="utf-8-sig") as _f:
        for _row in _csv_cs.DictReader(_f):
            _vn        = _row.get("Venue", "").strip()
            _city      = _row.get("City",  "").strip().lower()
            _state_raw = _row.get("State", "").strip().lower()
            # Normalise full state name → 2-letter abbreviation
            _state = _STATE_FULL_TO_ABBREV.get(_state_raw, _state_raw[:2])
            if _vn and _city:
                _MASTER_CITY_STATE_LOOKUP.setdefault((_city, _state), []).append(_vn)
    log(f"  [city-state-lookup] loaded {len(_MASTER_CITY_STATE_LOOKUP)} city+state keys")
    return _MASTER_CITY_STATE_LOOKUP


def _fuzzy_venue_match(name: str, candidates: list[str], cutoff: float = 0.35) -> str:
    """Return best fuzzy match for name from candidates, or '' if none good enough."""
    import difflib as _dl
    # Normalise: lowercase, strip format suffixes, collapse spaces
    _strip_re = re.compile(
        r'\bw/gtx\b|\bwith pdx\b|\bwith gtx\b|\bplf\b|\bstadium\b|\bcinemas?\b'
        r'|\bcineplex\b|\bcinema\b|\bw/\w+\b|\s+', re.I
    )
    def _norm(s):
        return _strip_re.sub(' ', s.lower()).strip()
    _nm = _norm(name)
    _best, _best_r = '', 0.0
    for _c in candidates:
        _r = _dl.SequenceMatcher(None, _nm, _norm(_c)).ratio()
        if _r > _best_r:
            _best_r, _best = _r, _c
    return _best if _best_r >= cutoff else ''


def _parse_one_per_line_to_dicts(raw: str) -> list[dict]:
    """Parse booking where each cell is on its own line (email copy-paste format).
    Handles standard Action/Policy format, Cinemark __COLUMN__ format, and bare
    Cinemark format (where email clients strip the __ underscores).
    """
    import re as _re

    _NAME_MAP = {'SALES': 'Buyer', 'THEATRE': 'Theatre', 'THEATER': 'Theatre',
                 'SCR': 'Screens', '#': 'Unit', 'DMA': 'DMA', 'BRCH': 'Branch',
                 'BRANCH': 'Branch', 'SCREENS': 'Screens',
                 'BOOK': 'Action',    # Michael Eiff / Cinemark single-film format
                 'PRINTS': 'Action'}  # Andy Anderson SF Bay Area format
    _CINEMARK_BARE = {'DMA', 'SALES', '#', 'THEATRE', 'THEATER', 'SCR', 'SCREENS',
                      'CHAIN', 'CIRCUIT', 'BRCH', 'BRANCH', 'BOOK', 'PRINTS'}

    # Parse preserving space-only lines as empty cell values.
    # Blank separator lines (truly empty after strip) are skipped.
    # Space-only lines (" ") represent blank column values (e.g. Action/Terms).
    cell_values = []
    for line in raw.splitlines():
        stripped = line.strip()
        if stripped:
            cell_values.append(stripped)
        elif len(line) > 0:   # has chars (spaces) but strips to empty → blank cell
            cell_values.append('')
        # else: truly empty line → row separator, skip

    # Also maintain non-empty list for dunder/standard detection
    values = [v for v in cell_values if v]

    if not values:
        return []

    # ── Cinemark DB export format (snake_case headers: theater_name / status) ─
    _SNAKE_KEYS = {'dma_name', 'city', 'state', 'theater_name', 'theatre_name',
                   'title', 'status', 'account_name', 'circuit'}
    _SNAKE_MAP  = {'theater_name': 'Theatre', 'theatre_name': 'Theatre',
                   'dma_name': 'DMA', 'status': 'Action', 'title': 'Film',
                   'city': 'City', 'state': 'State', 'circuit': 'Circuit',
                   'account_name': 'Buyer'}
    _th_name_idx = next((i for i, v in enumerate(values)
                         if v.lower() in ('theater_name', 'theatre_name')), None)
    if _th_name_idx is not None:
        # Walk back to find start of contiguous snake header block
        _ss = _th_name_idx
        while _ss > 0 and values[_ss - 1].lower() in _SNAKE_KEYS:
            _ss -= 1
        _snake_headers = []
        _si = _ss
        while _si < len(values) and values[_si].lower() in _SNAKE_KEYS:
            _snake_headers.append(_SNAKE_MAP.get(values[_si].lower(), values[_si]))
            _si += 1
        _n_sn = len(_snake_headers)
        _data_sn = values[_si:]
        rows = []
        for _sj in range(0, len(_data_sn), _n_sn):
            _chunk = list(_data_sn[_sj : _sj + _n_sn])
            if len(_chunk) < _n_sn:
                _chunk += [''] * (_n_sn - len(_chunk))
            _row = dict(zip(_snake_headers, _chunk))
            _al = _row.get('Action', '').lower()
            if 'final' in _al:
                _row['Action'] = 'Final'
            elif 'hold' in _al:
                _row['Action'] = 'Hold'
            rows.append(_row)
        return rows

    # ── Cinemark __COLUMN__ format (with underscores) ────────────────────────
    dunder_start = next((i for i, v in enumerate(values) if _re.fullmatch(r'__.*__', v)), None)
    if dunder_start is not None:
        action_count = 0
        headers = []
        for v in values[dunder_start:]:
            m = _re.fullmatch(r'__(.*?)__', v)
            if not m:
                break
            inner = m.group(1).strip()
            if not inner:
                action_count += 1
                headers.append('Action' if action_count == 1 else 'Terms')
            else:
                headers.append(_NAME_MAP.get(inner.upper(), inner))
        n_cols    = len(headers)
        data_vals = values[dunder_start + n_cols:]
        rows = []
        for i in range(0, len(data_vals), n_cols):
            chunk = data_vals[i:i + n_cols]
            if len(chunk) < n_cols:
                chunk += [''] * (n_cols - len(chunk))
            row = dict(zip(headers, chunk))
            if row.get('Action', '') == '':
                row['Action'] = 'Final'  # blank = confirmed
            rows.append(row)
        return rows

    # ── Cinemark DMA / City / Theatre / Title / Print / Attributes / Status / Detail ─
    # Detected when the first 4 non-blank values are: DMA, City, Theatre, Title.
    # The CSV has a blank line between every individual value (cell separator), so
    # record boundaries are detected by matching DMA-pattern values in cell_values.
    # DMA values contain " - " (e.g. "Dallas - Ft. Worth") or are "City,ST" style.
    if (len(values) >= 4
            and values[0].lower() == 'dma'
            and values[1].lower() == 'city'
            and values[2].lower() in ('theatre', 'theater')
            and values[3].lower() == 'title'):
        _DMA_PAT = _re.compile(r'.+ - .+|.+,\s*[A-Z]{2}$')
        # cell_values already skips blank separator lines; space-only → ''
        _data = cell_values[8:]              # skip the 8 header values
        # find record-start positions (where a DMA value appears)
        _dma_pos = [_i for _i, _v in enumerate(_data) if _v and _DMA_PAT.match(_v)]
        # fallback: treat first value as DMA and find all repetitions
        if not _dma_pos and _data:
            _dma0 = next((_v for _v in _data if _v), '')
            _dma_pos = [_i for _i, _v in enumerate(_data) if _v == _dma0]
        rows = []
        for _ri, _dp in enumerate(_dma_pos):
            _rend = _dma_pos[_ri + 1] if _ri + 1 < len(_dma_pos) else len(_data)
            _rv = _data[_dp:_rend]
            _nb = [_v for _v in _rv if _v]
            if len(_nb) < 3:
                continue
            _dma, _city, _th = _nb[0], _nb[1], _nb[2]
            # skip past the 3 non-blank DMA/City/Theatre values
            _nbc2, _skip2 = 0, len(_rv)
            for _k2, _cv2 in enumerate(_rv):
                if _cv2:
                    _nbc2 += 1
                    if _nbc2 == 3:
                        _skip2 = _k2 + 1
                        break
            _fv = list(_rv[_skip2:])
            while len(_fv) % 5:             # pad to multiple of 5
                _fv.append('')
            for _fi in range(0, len(_fv), 5):
                _ttl, _, _, _sta, _dtl = _fv[_fi:_fi + 5]
                if not _ttl and not _sta:
                    continue                # all-blank filler row
                _al = _sta.lower()
                if 'final' in _al:
                    _act = 'Final'
                elif 'hold' in _al:
                    _act = 'Hold'
                else:
                    continue
                rows.append({'Theatre': _th, 'DMA': _dma, 'City': _city,
                             'Film': _ttl, 'Action': _act, 'Terms': _dtl})
        return rows

    # ── ComScore booking: Theatre # / ComScore Name / City / ST / Screens / DMA ──
    # The "Theatre #" value IS the Comscore unit number — use it for direct lookup.
    # Theatre names may include "(City, ST)" / "(date)" suffixes — stripped below.
    # Blank Action = confirmed (Final); any "hold" variant = Hold.
    if (len(values) >= 4
            and values[0].lower() in ('theatre #', 'theater #')
            and values[2].lower() == 'city'
            and values[3].lower() == 'st'):
        _hdrs_csc = ['Unit', 'Theatre', 'City', 'State', 'Screens', 'DMA', 'Action']
        _data_csc = cell_values[6:]   # skip 6 explicit header lines
        # Each row is anchored by a 3+ digit unit number
        _id_pos_csc = [_i for _i, _v in enumerate(_data_csc)
                       if _re.fullmatch(r'\d{3,}', _v)]
        rows = []
        for _idx_csc, _pos_csc in enumerate(_id_pos_csc):
            _end_csc = (_id_pos_csc[_idx_csc + 1] if _idx_csc + 1 < len(_id_pos_csc)
                        else len(_data_csc))
            _row_csc = list(_data_csc[_pos_csc:_end_csc])
            if len(_row_csc) < 7:
                _row_csc += [''] * (7 - len(_row_csc))
            _d_csc = dict(zip(_hdrs_csc, _row_csc[:7]))
            # Strip "(City, ST)" and "(date)" parentheticals from theatre name
            _th_csc = _re.sub(r'\s*\([^)]*\)', '', _d_csc['Theatre']).strip()
            _al_csc = _d_csc['Action'].lower()
            _act_csc = 'Hold' if 'hold' in _al_csc else 'Final'
            rows.append({'Theatre': _th_csc, 'Unit': _d_csc['Unit'],
                         'City': _d_csc['City'], 'Action': _act_csc, 'Film': ''})
        return rows

    # ── Landmark "Location" format: 2-column (Theatre / Status) ────────────────
    # Film title may appear as preamble before the "Location" header.
    # Storage: either one-value-per-line (alternating pairs) OR tab/comma-separated
    # rows where each line is "Theatre\tStatus" or "Theatre,Status".
    # "finished" → Final; permanently "closed" → skip; everything else → Hold.
    _loc_idx_lm = next((i for i, v in enumerate(values[:8]) if v.lower() == 'location'), None)
    if _loc_idx_lm is not None:
        _film_lm  = values[0] if _loc_idx_lm > 0 else ''
        _data_lm  = values[_loc_idx_lm + 1:]
        rows = []
        # Detect if data is inline-separated (tab or comma in the value itself)
        _sep_lm = None
        for _sv in _data_lm[:4]:
            if '\t' in _sv:
                _sep_lm = '\t'; break
            if ',' in _sv:
                _sep_lm = ','; break
        def _lm_row(th, st):
            _al = st.lower()
            if 'closed' in _al and 'no opening' in _al:
                return None
            _act = 'Final' if 'finished' in _al else 'Hold'
            return {'Theatre': th, 'Film': _film_lm, 'Action': _act, 'Phrase': st}
        if _sep_lm:
            # Each value is "Theatre<sep>Status" on one line
            for _entry_lm in _data_lm:
                _parts_lm = _entry_lm.split(_sep_lm, 1)
                _r = _lm_row(_parts_lm[0].strip(),
                              _parts_lm[1].strip() if len(_parts_lm) > 1 else '')
                if _r:
                    rows.append(_r)
        else:
            # One-per-line: alternating Theatre / Status pairs
            for _fi_lm in range(0, len(_data_lm), 2):
                _r = _lm_row(_data_lm[_fi_lm],
                              _data_lm[_fi_lm + 1] if _fi_lm + 1 < len(_data_lm) else '')
                if _r:
                    rows.append(_r)
        return rows

    # ── Bare Cinemark format (no underscores — email clients strip __ markers) ─
    # Detect by finding known Cinemark column names in the non-empty values.
    bare_start = None
    for i, v in enumerate(cell_values):
        if v.upper() in _CINEMARK_BARE:
            subsequent = [cell_values[j] for j in range(i + 1, min(i + 6, len(cell_values)))]
            if any(s.upper() in _CINEMARK_BARE for s in subsequent):
                bare_start = i
                break

    if bare_start is not None:
        headers = []
        blank_count = 0
        _book_action = False  # True when 'BOOK' col is explicit → blank means skip, not Final
        i = bare_start
        while i < len(cell_values):
            v = cell_values[i]
            if v.upper() in _CINEMARK_BARE or v == '#':
                mapped = _NAME_MAP.get(v.upper(), v)
                if v.upper() == 'BOOK':
                    _book_action = True
                headers.append(mapped)
                blank_count = 0
                i += 1
            elif v == '' and blank_count < 2:
                headers.append('Action' if blank_count == 0 else 'Terms')
                blank_count += 1
                i += 1
            else:
                break
        # Always ensure Action and Terms columns exist
        if 'Action' not in headers:
            headers.append('Action')
        if 'Terms' not in headers:
            headers.append('Terms')
        n_cols    = len(headers)
        data_vals = cell_values[i:]

        # Find Theatre column offset for row-boundary detection
        _th_col   = next((h for h in headers if h in ('Theatre', 'Theater')), None)
        _th_off   = headers.index(_th_col) if _th_col is not None else None
        _THEATRE_RE = _re.compile(r'\([^)]*,\s*[A-Z]{2}\)', _re.IGNORECASE)

        if _th_off is not None and blank_count == 0:
            # No blank separators → variable-length rows; anchor on Theatre "(City, ST)"
            th_positions = [j for j, v in enumerate(data_vals) if _THEATRE_RE.search(v)]
            rows = []
            for idx, th_pos in enumerate(th_positions):
                row_start = th_pos - _th_off
                if row_start < 0:
                    continue
                if idx + 1 < len(th_positions):
                    row_end = th_positions[idx + 1] - _th_off
                else:
                    row_end = len(data_vals)  # last row: include all remaining
                row_data = list(data_vals[row_start : row_end])
                if len(row_data) < n_cols:
                    row_data += [''] * (n_cols - len(row_data))
                row = dict(zip(headers, row_data[:n_cols]))
                # Blank action: Final for dunder formats; skip for explicit BOOK column
                if row.get('Action', '') == '' and not _book_action:
                    row['Action'] = 'Final'
                # Strip "(City, ST)" from Theatre and populate City if missing
                _th_val = row.get('Theatre', '')
                _city_m = _THEATRE_RE.search(_th_val)
                if _city_m:
                    _cs = _city_m.group(0)[1:-1]  # e.g. "Cuyahoga Falls, OH"
                    row['Theatre'] = (_th_val[:_city_m.start()] + _th_val[_city_m.end():]).strip()
                    if not row.get('City'):
                        row['City'] = _cs.split(',')[0].strip()
                rows.append(row)
            return rows
        else:
            # Blank separators present → fixed-length rows
            rows = []
            for j in range(0, len(data_vals), n_cols):
                chunk = data_vals[j:j + n_cols]
                if len(chunk) < n_cols:
                    chunk += [''] * (n_cols - len(chunk))
                row = dict(zip(headers, chunk))
                if row.get('Action', '') == '':
                    row['Action'] = 'Final'
                rows.append(row)
            return rows

    # ── Small-exhibitor city+state format: "City, State   HOLD/Final" ──────────
    # e.g. "Ark City, KS       HOLD"  or  "Florence, SC        Final"
    _CS_RE = _re.compile(r'^(.*\S)\s+(HOLD|FINAL|OPEN|CONFIRMED)\s*$', _re.IGNORECASE)
    _SS_RE = _re.compile(r'^(.*?),?\s*([A-Z]{2})\s*$')
    _nonempty_lines = [l.strip() for l in raw.splitlines() if l.strip()]
    if _nonempty_lines:
        _cs_hits = sum(1 for _l in _nonempty_lines if _CS_RE.match(_l))
        if _cs_hits / len(_nonempty_lines) >= 0.70:
            rows = []
            for _line in _nonempty_lines:
                _cm = _CS_RE.match(_line)
                if not _cm:
                    continue
                _loc    = _cm.group(1).strip()
                _stat   = _cm.group(2).strip()
                _action = 'Final' if 'final' in _stat.lower() else 'Hold'
                rows.append({'Theatre': _loc, 'Action': _action})  # "City, ST" gives 2 match words
            return rows

    # ── Standard Action/Policy format ────────────────────────────────────────
    action_idx = None
    for i, v in enumerate(values):
        if v.lower() in ("action", "policy"):
            action_idx = i
            break
    if action_idx is None:
        return []
    # Skip any leading non-column values (e.g. "Angel Studios Inc.")
    KNOWN_COLS = {"buyer","br","unit","theatre","theater","attraction","film","title","type","media","prt"}
    header_start = 0
    for i in range(action_idx + 1):
        if values[i].lower() in KNOWN_COLS:
            header_start = i
            break
    headers   = values[header_start:action_idx + 1]
    n_cols    = len(headers)
    remainder = values[action_idx + 1:]
    if not remainder:
        return []
    # ID-based detection: if any of the first 5 cols is Unit/# (3+ digit numbers anchor rows)
    # This handles cell-wrapping pastes where theatre names split across lines, throwing off
    # fixed-size chunking.  Unit IDs are always 3-4 digit numbers; screen counts are ≤2 digits.
    _unit_col_idx = next(
        (i for i, h in enumerate(headers[:5])
         if any(p in h.lower() for p in ("unit", "comscore", "#"))),
        None
    )
    if _unit_col_idx is None and any(p in headers[0].lower() for p in ("unit", "comscore", "#")):
        _unit_col_idx = 0
    if _unit_col_idx is not None:
        id_pos = [i for i, v in enumerate(remainder) if _re.fullmatch(r'\d{3,}', v)]
        if id_pos:
            rows = []
            for idx, pos in enumerate(id_pos):
                row_start = pos - _unit_col_idx
                if row_start < 0:
                    continue
                next_id = id_pos[idx + 1] if idx + 1 < len(id_pos) else len(remainder)
                row_end = next_id - _unit_col_idx
                row = list(remainder[row_start:row_end])
                if len(row) < n_cols:
                    row += [""] * (n_cols - len(row))
                rows.append(dict(zip(headers, row[:n_cols])))
            return rows
    # Fixed-size chunking fallback
    rows = []
    for i in range(0, len(remainder), n_cols):
        chunk = remainder[i:i + n_cols]
        if len(chunk) < n_cols:
            chunk += [""] * (n_cols - len(chunk))
        rows.append(dict(zip(headers, chunk)))
    return rows


def parse_booking_csv(path: Path) -> list[dict]:
    """
    Parse booking CSV → list of {theatre, action, phrase, screening_type}.
    Returns only Hold and Final rows.
    """
    results = []
    try:
        with open(path, newline="", encoding="utf-8-sig") as f:
            lines = f.readlines()

        import io, re as _re_pbc
        # Detect Cinemark __COLUMN__ format (may have preamble before headers)
        stripped_lines = [l.strip() for l in lines if l.strip()]
        _dunder_idx = next((i for i, v in enumerate(stripped_lines[:15])
                            if _re_pbc.fullmatch(r'__.*__', v)), None)
        _is_dunder_fmt = _dunder_idx is not None

        if _is_dunder_fmt and _dunder_idx > 0:
            # Trim to the first __COLUMN__ line
            target = stripped_lines[_dunder_idx]
            header_idx = next((i for i, l in enumerate(lines) if l.strip() == target), 0)
        elif not _is_dunder_fmt:
            # Skip title rows — find first line with a known column name
            HEADER_KEYS = {"theatre", "theater", "buyer", "film", "attraction",
                           "action", "unit", "dma_name", "status", "dma"}
            header_idx = 0
            for i, line in enumerate(lines[:10]):
                # Split on whitespace/comma/tab so "dma" inside "landmark" doesn't fire
                _words = set(_re_pbc.split(r'[\s,\t]+', line.lower().strip()))
                if _words & HEADER_KEYS:
                    header_idx = i
                    break
        else:
            header_idx = 0

        content = "".join(lines[header_idx:])

        # One-per-line format: check max tabs/commas on any single line (not total)
        # (data values like "Houston, TX" add commas but each line has at most 1)
        _lines_sample = [l for l in content.splitlines() if l.strip()][:10]
        _max_tabs   = max((l.count("\t") for l in _lines_sample), default=0)
        _max_commas = max((l.count(",")  for l in _lines_sample), default=0)
        # ComScore booking format has "ComScore Name, City, State" header (2 commas)
        # but is still a one-per-line format — force one-per-line path for it.
        _is_comscore_hdr = any(
            l.strip().lower() in ('theatre #', 'theater #') for l in _lines_sample[:4]
        )
        log(f"  [debug] header_idx={header_idx} max_tabs={_max_tabs} max_commas={_max_commas} comscore={_is_comscore_hdr}")

        # ── AMC Holdover Report format ────────────────────────────────────────
        # PDF copy-paste from AMC booking system. Each data line contains an
        # anchor: "<gross> [Split screen. ]?<Final|Holdover|Opening>".
        # Film title tracks across lines (blank merged cell in PDF → same film).
        # "Split screen. Holdover" → Hold/Alternating; "Holdover" → Hold/Clean.
        _is_amc_hdr = (
            any('AMC Film Programmer' in l for l in stripped_lines[:15])
            or any(re.search(r'Split\s+[Ss]creen\.\s+(?:Final|Holdover)', l) for l in stripped_lines)
            or any(re.search(r'\b[\d,]+[ ]+(?:Split[ ]+[Ss]creen\.[ ]+)?(?:Final|Holdover)\b', l) for l in stripped_lines)
        )
        if _is_amc_hdr:
            _DMA_RE_amc = re.compile(
                r'\b([A-Z]{2,}[A-Z0-9\-&\/]*(?:\s+[A-Z]{2,}[A-Z0-9\-&\/]*)*)'
                r'(?:\s*\([^)]*\))?(?:,\s*[A-Z]{2})?\s+(?=[A-Z][a-z])'
            )
            _anchor_amc = re.compile(
                r'\b[\d,]+\s+((?:Split\s+[Ss]creen\.\s+)?(?:Final|Holdover))\b'
            )
            _cur_film_amc = ''
            _full_text_amc = ''.join(lines)
            for _ln in _full_text_amc.splitlines():
                _ln = _ln.strip()
                if not _ln:
                    continue
                _am = _anchor_amc.search(_ln)
                if not _am:
                    continue
                _action_str = _am.group(1)
                _is_split   = 'split' in _action_str.lower()
                if 'final' in _action_str.lower():
                    _act_amc, _phrase_amc = 'Final', ''
                else:
                    _act_amc   = 'Hold'
                    _phrase_amc = 'shows' if _is_split else ''
                _before = _ln[:_am.start()].strip()
                # Use the LAST DMA match — first may be the distributor name (ALL-CAPS)
                _all_dma_m = list(_DMA_RE_amc.finditer(_before))
                _dma_m     = _all_dma_m[-1] if _all_dma_m else None
                if _dma_m:
                    _film_part = _before[:_dma_m.start()].strip()
                    _th_amc    = _before[_dma_m.end():].strip()
                    # Extract film title: rightmost mixed-case word(s) in film_part
                    _fp_clean = re.sub(r'\b[A-Z]{2,}\b[\s]*', ' ', _film_part).strip()
                    _fp_clean = re.sub(r'\s+', ' ', _fp_clean).strip()
                    if _fp_clean:
                        _cur_film_amc = _fp_clean
                else:
                    _th_amc = _before
                # Split off embedded film title after screen-count number
                _tf_m = re.match(r'^(.+?\b\d+)\s+([A-Z][A-Za-z].+)$', _th_amc)
                if _tf_m and re.search(r'[a-z]', _tf_m.group(2)):
                    _clean = re.sub(r'\s*[-–]\s*(2D|3D|OC|IMAX|XD|Combo).*', '', _tf_m.group(2), flags=re.I).strip()
                    if _clean:
                        _cur_film_amc = _clean
                    _th_amc = _tf_m.group(1).strip()
                if not _th_amc or not re.search(r'\d', _th_amc):
                    continue
                _st_amc = get_screening_type(_phrase_amc) if _act_amc == 'Hold' else None
                results.append({'theatre': _th_amc, 'city': '', 'action': _act_amc,
                                'film': _cur_film_amc, 'phrase': _phrase_amc,
                                'screening_type': _st_amc})
            log(f"  [amc-holdover] parsed {len(results)} results")
            return results
        # ── End AMC Holdover Report format ────────────────────────────────────

        # ── Holdover grid format (David Saunders / indie circuits) ───────────
        # Detected by "PRELIMINARY HOLD OVERS" anywhere in the text.
        # Tab-delimited: THEATRE | FILM | hold_x | F | S | S | M | T | W | T | undecided_x
        # col[2]='x' → Hold; cols[3-9] any 'x' → Final; col[10]='x' → skip.
        _full_text_hg = ''.join(lines)
        if 'preliminary hold overs' in _full_text_hg.lower():
            _hg_results: list[dict] = []
            _SKIP_HG = {'theatre', 'theater', 'theatres', 'theaters', 'film', 'title', 'attraction'}
            for _hl in _full_text_hg.splitlines():
                _hc = _hl.split('\t')
                if len(_hc) < 3:
                    continue
                _th_hg  = _hc[0].strip()
                _film_hg = _hc[1].strip() if len(_hc) > 1 else ''
                if not _th_hg or not _film_hg:
                    continue
                if _th_hg.lower() in _SKIP_HG or _film_hg.lower() in _SKIP_HG:
                    continue
                _hold_x_hg     = _hc[2].strip().lower() if len(_hc) > 2 else ''
                _day_xs_hg     = [_hc[i].strip().lower() for i in range(3, min(10, len(_hc)))]
                _undecided_hg  = _hc[10].strip().lower() if len(_hc) > 10 else ''
                if _undecided_hg == 'x':
                    continue
                elif _hold_x_hg == 'x':
                    _act_hg, _phrase_hg = 'Hold', ''
                elif any(x == 'x' for x in _day_xs_hg):
                    _act_hg, _phrase_hg = 'Final', ''
                else:
                    continue
                _st_hg = get_screening_type(_phrase_hg) if _act_hg == 'Hold' else None
                _hg_results.append({'theatre': _th_hg, 'city': '', 'action': _act_hg,
                                    'film': _film_hg, 'phrase': _phrase_hg,
                                    'screening_type': _st_hg})
            if _hg_results:
                log(f"  [holdover-grid] parsed {len(_hg_results)} results")
                return _hg_results
        # ── End holdover grid format ───────────────────────────────────────────

        # ── Cinemark "Theater # / Name (City, State)" TSV format ─────────────
        # Header row starts with "Theater #\t...".  Preamble lines before the
        # header (e.g. "David", "Solo Mio") are the film/production names.
        # Two duplicate "Regular" columns → one per film.
        # Values: "Final" → Final, "Clean"/any non-dash → Hold, "-"/blank → skip.
        _first_content_line = next((l.rstrip('\n\r') for l in content.splitlines() if l.strip()), "")
        _is_theater_hash_tsv = (
            _first_content_line.split('\t')[0].strip().lower() in ('theater #', 'theatre #')
            and '\t' in _first_content_line
        )
        if _is_theater_hash_tsv:
            _preamble_films = [l.strip() for l in lines[:header_idx] if l.strip()]
            _tsv_raw_headers = [c.strip() for c in _first_content_line.split('\t')]
            # Deduplicate column names (two "Regular" → "Regular", "Regular.1")
            _seen_th = {}
            _deduped_th = []
            for _h in _tsv_raw_headers:
                _hl = _h.lower()
                if _hl in _seen_th:
                    _seen_th[_hl] += 1
                    _deduped_th.append(f"{_h}.{_seen_th[_hl]}")
                else:
                    _seen_th[_hl] = 0
                    _deduped_th.append(_h)
            # Film columns = anything not in the standard info set
            _INFO_COLS_TH = {'theater #', 'theatre #', 'name (city, state)', 'dma',
                             'screens', 'contact', 'chain', 'circuit', 'branch'}
            _film_col_idxs = [i for i, h in enumerate(_deduped_th)
                               if h.split('.')[0].strip().lower() not in _INFO_COLS_TH]
            log(f"  [1b-tsv] preamble_films={_preamble_films} film_col_idxs={_film_col_idxs} headers={_deduped_th}")
            _city_pat_th = _re_pbc.compile(r'\(([^,)]+),\s*[A-Z]{2}\)\s*$')
            for _dl in content.splitlines()[1:]:
                if not _dl.strip():
                    continue
                _cells = [c.strip() for c in _dl.split('\t')]
                _raw_nm = _cells[1].strip() if len(_cells) > 1 else ""
                _cm = _city_pat_th.search(_raw_nm)
                _city_th = _cm.group(1).strip() if _cm else ""
                _theatre_th = _city_pat_th.sub("", _raw_nm).strip()
                if not _theatre_th:
                    continue
                for _fi, _ci in enumerate(_film_col_idxs):
                    _val = _cells[_ci].strip().lower() if _ci < len(_cells) else ""
                    _film_th = _preamble_films[_fi] if _fi < len(_preamble_films) else ""
                    if _val == 'final':
                        _a_th = 'Final'
                    elif _val and _val not in ('-',):
                        _a_th = 'Hold'   # "clean" = clean hold
                    else:
                        continue         # "-" or blank = not booked
                    _phrase_th = "" if _val in ('final', 'clean') else _val
                    _st_th = get_screening_type(_phrase_th) if _a_th == 'Hold' else None
                    results.append({"theatre": _theatre_th, "city": _city_th,
                                    "action": _a_th, "film": _film_th,
                                    "phrase": _phrase_th, "screening_type": _st_th})
            log(f"  [1b-tsv] parsed {len(results)} results")
            return results
        # ── End Cinemark Theater # TSV ────────────────────────────────────────

        # ── Cinemark "Theater #" one-per-line variant ─────────────────────────
        # Same format as TSV above but each cell is on its own line (no tabs).
        # Detected when the first non-empty content line is exactly "Theater #".
        _is_theater_hash_opl = (
            _first_content_line.strip().lower() in ('theater #', 'theatre #')
            and '\t' not in _first_content_line
        )
        if _is_theater_hash_opl:
            _preamble_films_opl = [l.strip() for l in lines[:header_idx] if l.strip()]
            _all_vals_opl = [l.strip() for l in content.splitlines() if l.strip()]
            # Collect headers: consecutive non-numeric non-empty values at start
            _opl_hdrs = []
            _opl_ds = 0
            for _oi, _ov in enumerate(_all_vals_opl):
                if _re_pbc.fullmatch(r'\d{3,}', _ov):
                    _opl_ds = _oi
                    break
                _opl_hdrs.append(_ov)
            # Deduplicate column names
            _seen_opl = {}
            _deduped_opl = []
            for _h in _opl_hdrs:
                _hl = _h.lower()
                if _hl in _seen_opl:
                    _seen_opl[_hl] += 1
                    _deduped_opl.append(f"{_h}.{_seen_opl[_hl]}")
                else:
                    _seen_opl[_hl] = 0
                    _deduped_opl.append(_h)
            _INFO_COLS_OPL = {'theater #', 'theatre #', 'name (city, state)', 'dma',
                               'screens', 'contact', 'chain', 'circuit', 'branch'}
            _film_idxs_opl = [i for i, h in enumerate(_deduped_opl)
                               if h.split('.')[0].strip().lower() not in _INFO_COLS_OPL]
            log(f"  [1b-opl-mica] preamble={_preamble_films_opl} film_idxs={_film_idxs_opl} headers={_deduped_opl}")
            _opl_data_vals = _all_vals_opl[_opl_ds:]
            _id_pos_opl = [i for i, v in enumerate(_opl_data_vals) if _re_pbc.fullmatch(r'\d{3,}', v)]
            _cpat_opl = _re_pbc.compile(r'\(([^,)]+),\s*[A-Z]{2}\)\s*$')
            for _ri, _rpos in enumerate(_id_pos_opl):
                _rnxt = _id_pos_opl[_ri + 1] if _ri + 1 < len(_id_pos_opl) else len(_opl_data_vals)
                _row_opl = _opl_data_vals[_rpos:_rnxt]
                _raw_nm_opl = _row_opl[1] if len(_row_opl) > 1 else ""
                _cm_opl = _cpat_opl.search(_raw_nm_opl)
                _city_opl = _cm_opl.group(1).strip() if _cm_opl else ""
                _theatre_opl = _cpat_opl.sub("", _raw_nm_opl).strip()
                if not _theatre_opl:
                    continue
                for _fi, _ci in enumerate(_film_idxs_opl):
                    _val_opl = _row_opl[_ci].strip().lower() if _ci < len(_row_opl) else ""
                    _film_opl = _preamble_films_opl[_fi] if _fi < len(_preamble_films_opl) else ""
                    if _val_opl == 'final':
                        _a_opl = 'Final'
                    elif _val_opl and _val_opl not in ('-',):
                        _a_opl = 'Hold'   # "clean" = clean hold
                    else:
                        continue
                    _phrase_opl = "" if _val_opl in ('final', 'clean') else _val_opl
                    _st_opl = get_screening_type(_phrase_opl) if _a_opl == 'Hold' else None
                    results.append({"theatre": _theatre_opl, "city": _city_opl,
                                    "action": _a_opl, "film": _film_opl,
                                    "phrase": _phrase_opl, "screening_type": _st_opl})
            log(f"  [1b-opl-mica] parsed {len(results)} results")
            return results
        # ── End Cinemark Theater # one-per-line ───────────────────────────────

        # ── Gundrum "ID # grid" format ────────────────────────────────────────
        # Tab-delimited. Columns: [row#] | ID # | Screens | Theatre (City, ST) | DMA | [film cols...]
        # Film names are the column headers. Actions: "Hold Clean", "Hold Mats", "Final", "-"
        _gundrum_hdrs = [h.strip().lower() for h in _first_content_line.split('\t')]
        _is_gundrum = (
            '\t' in _first_content_line
            and 'id #' in _gundrum_hdrs
            and any(h in ('theatre', 'theater') for h in _gundrum_hdrs)
            and 'screens' in _gundrum_hdrs
        )
        if _is_gundrum:
            _INFO_G      = {'', 'id #', 'screens', 'theatre', 'theater', 'dma'}
            _raw_hdrs_g  = [c.strip() for c in _first_content_line.split('\t')]
            _film_col_idxs_g = [i for i, h in enumerate(_gundrum_hdrs) if h not in _INFO_G]
            _film_names_g    = [_raw_hdrs_g[i] for i in _film_col_idxs_g]
            _th_col_g  = next(i for i, h in enumerate(_gundrum_hdrs) if h in ('theatre', 'theater'))
            _id_col_g  = _gundrum_hdrs.index('id #')
            _cpat_g    = _re_pbc.compile(r'\(([^,)]+),\s*[A-Z]{2}\)\s*$')
            _date_g    = _re_pbc.compile(r'\s*\(\d{1,2}/\d{1,2}\)')
            _ref_lkp_g = _load_master_ref_lookup()
            log(f"  [gundrum] films={_film_names_g} th_col={_th_col_g} id_col={_id_col_g}")
            for _dl in content.splitlines()[1:]:
                if not _dl.strip():
                    continue
                _cells = [c.strip() for c in _dl.split('\t')]
                _raw_id = _cells[_id_col_g].strip() if _id_col_g < len(_cells) else ""
                _raw_nm = _cells[_th_col_g].strip() if _th_col_g < len(_cells) else ""
                if not _raw_nm:
                    continue
                _raw_nm = _date_g.sub('', _raw_nm)          # strip embedded dates like "(4/18)"
                _cm = _cpat_g.search(_raw_nm)
                _city_g    = _cm.group(1).strip() if _cm else ""
                _theatre_g = _cpat_g.sub('', _raw_nm).strip()
                _lookup_nm = _ref_lkp_g.get((_raw_id, _city_g.lower()), "")
                _final_nm  = _lookup_nm or _theatre_g
                for _fi, _ci in enumerate(_film_col_idxs_g):
                    _val = _cells[_ci].strip().lower() if _ci < len(_cells) else ""
                    _film_g = _film_names_g[_fi] if _fi < len(_film_names_g) else ""
                    if _val == 'final':
                        _act_g, _phrase_g = 'Final', ''
                    elif _val.startswith('hold'):
                        _act_g  = 'Hold'
                        _mod    = _val[4:].strip()           # e.g. "clean", "mats"
                        _phrase_g = '' if _mod in ('', 'clean') else _mod
                    elif _val and _val != '-':
                        _act_g, _phrase_g = 'Hold', _val
                    else:
                        continue                              # "-" or blank = not booked
                    _st_g = get_screening_type(_phrase_g) if _act_g == 'Hold' else None
                    results.append({"theatre": _final_nm, "city": _city_g,
                                    "action": _act_g, "film": _film_g,
                                    "phrase": _phrase_g, "screening_type": _st_g})
            log(f"  [gundrum] parsed {len(results)} results")
            return results
        # ── End Gundrum ID# grid format ───────────────────────────────────────

        # ── Diane Johnson "circuit grid with date headers" format ─────────────
        # Tab-delimited. Columns: CIRCUIT | THEATRE | CITY | STATE | [Film - M/D] ...
        # Film names + dates are the column headers. Actions: "Hold Clean",
        # "Hold Mats", "Hold Shows", "Final", "Opening", blank/skip.
        # Distinct from Glen Parham: has "theatre" (not "theatre name"), no "status"
        # column, and film headers contain a date suffix (" - M/D").
        _dj_raw_hdrs = [c.strip() for c in _first_content_line.split('\t')]
        _dj_hdrs     = [h.lower() for h in _dj_raw_hdrs]
        _is_dj = (
            '\t' in _first_content_line
            and 'circuit'  in _dj_hdrs
            and 'theatre'  in _dj_hdrs
            and 'theatre name' not in _dj_hdrs
            and any(_re_pbc.search(r'-\s*\d{1,2}/\d{1,2}', h) for h in _dj_hdrs)
        )
        if _is_dj:
            _DJ_INFO = {'circuit', 'theatre', 'city', 'state', 'st'}
            _dj_film_cols = [
                i for i, h in enumerate(_dj_hdrs)
                if _re_pbc.search(r'-\s*\d{1,2}/\d{1,2}', h)
            ]
            # Extract film name by stripping " - M/D" suffix from raw header
            _dj_film_names = [
                _re_pbc.sub(r'\s*-\s*\d{1,2}/\d{1,2}.*', '', _dj_raw_hdrs[i]).strip()
                for i in _dj_film_cols
            ]
            _ci_dj_circuit = _dj_hdrs.index('circuit') if 'circuit' in _dj_hdrs else -1
            _ci_dj_theatre = _dj_hdrs.index('theatre') if 'theatre' in _dj_hdrs else -1
            _ci_dj_city    = _dj_hdrs.index('city')    if 'city'    in _dj_hdrs else -1
            _ci_dj_state   = next((i for i, h in enumerate(_dj_hdrs) if h in ('state', 'st')), -1)
            _cs_lkp_dj     = _load_city_state_lookup()
            log(f"  [diane-j] films={_dj_film_names} film_cols={_dj_film_cols}")
            for _dl in content.splitlines()[1:]:
                if not _dl.strip():
                    continue
                _cells = [c.strip() for c in _dl.split('\t')]
                def _dj(i): return _cells[i] if 0 <= i < len(_cells) else ""
                _circuit_dj = _dj(_ci_dj_circuit)
                _thtr_dj    = _dj(_ci_dj_theatre)
                _city_dj    = _dj(_ci_dj_city)
                _st_dj      = _dj(_ci_dj_state).lower()[:2]
                if not _thtr_dj:
                    continue
                # Venue lookup: try theatre alone, then circuit+theatre
                _city_key_dj = _BOOKING_CITY_CORRECTIONS.get(_city_dj.lower(), _city_dj.lower())
                _cands_dj    = _cs_lkp_dj.get((_city_key_dj, _st_dj), [])
                _matched_dj  = _fuzzy_venue_match(_thtr_dj, _cands_dj) if _cands_dj else ''
                if not _matched_dj and _circuit_dj:
                    _combined_dj = f"{_circuit_dj} {_thtr_dj}"
                    _matched_dj  = _fuzzy_venue_match(_combined_dj, _cands_dj) if _cands_dj else ''
                if _matched_dj:
                    _venue_dj = _matched_dj
                elif _circuit_dj:
                    # Use circuit+theatre so Mica's scorer has more signal than bare name
                    _venue_dj = f"{_circuit_dj} {_thtr_dj}"
                    log(f"  [diane-j] no match for '{_thtr_dj}' ({_city_dj}, {_st_dj.upper()}) — using '{_venue_dj}'")
                else:
                    _venue_dj = _thtr_dj
                    log(f"  [diane-j] no match for '{_thtr_dj}' ({_city_dj}, {_st_dj.upper()}) — using raw name")
                for _fi, _ci in enumerate(_dj_film_cols):
                    _val  = _dj(_ci)
                    _vl   = _val.lower().strip()
                    _film_dj = _dj_film_names[_fi]
                    if 'final' in _vl:
                        _act_dj, _phrase_dj = 'Final', ''
                    elif _vl.startswith('hold'):
                        _act_dj  = 'Hold'
                        _mod_dj  = _vl[4:].strip().lstrip('(*').strip()
                        _phrase_dj = '' if _mod_dj in ('', 'clean') else _mod_dj
                    elif 'open' in _vl:
                        continue  # opening — handled by booking_plan_update
                    else:
                        continue  # blank or unrecognised
                    _st_type_dj = get_screening_type(_phrase_dj) if _act_dj == 'Hold' else None
                    results.append({"theatre": _venue_dj, "city": _city_dj,
                                    "action": _act_dj, "film": _film_dj,
                                    "phrase": _phrase_dj, "screening_type": _st_type_dj})
            log(f"  [diane-j] parsed {len(results)} results")
            return results
        # ── End Diane Johnson circuit grid format ─────────────────────────────

        # ── Andy Anderson "THEATRE/SCR/City/State" grid format ───────────────
        # Tab-delimited. Preamble = film names. Header: THEATRE | SCR | City | State | [film cols].
        # Film cols labeled (e.g. "Prints") — one per preamble film.
        # Actions: Hold, Hold Shows, Hold a show, Final, blank.
        _aa_raw_hdrs = [c.strip() for c in _first_content_line.split('\t')]
        _aa_hdrs     = [h.lower() for h in _aa_raw_hdrs]
        _is_aa = (
            '\t' in _first_content_line
            and _aa_hdrs[0] in ('theatre', 'theater')
            and 'scr' in _aa_hdrs
            and 'city' in _aa_hdrs
            and 'state' in _aa_hdrs
        )
        if _is_aa:
            _preamble_aa  = [l.strip() for l in lines[:header_idx] if l.strip()]
            _state_idx_aa = _aa_hdrs.index('state')
            _city_idx_aa  = _aa_hdrs.index('city')
            _film_idxs_aa = list(range(_state_idx_aa + 1, len(_aa_hdrs)))
            _film_names_aa = (_preamble_aa[:len(_film_idxs_aa)]
                              if len(_preamble_aa) >= len(_film_idxs_aa)
                              else _preamble_aa + [''] * (len(_film_idxs_aa) - len(_preamble_aa)))
            _cpat_aa  = _re_pbc.compile(r'\(([^,)]+),\s*[A-Z]{2}\)\s*$')
            _state_col_aa = next((i for i, h in enumerate(_aa_hdrs) if h in ('state', 'st')), -1)
            _cs_lkp_aa = _load_city_state_lookup()
            log(f"  [aa] preamble={_preamble_aa} film_idxs={_film_idxs_aa} films={_film_names_aa}")
            for _dl in content.splitlines()[1:]:
                if not _dl.strip():
                    continue
                _cells_aa = [c.strip() for c in _dl.split('\t')]
                _raw_nm_aa = _cells_aa[0].strip() if _cells_aa else ''
                if not _raw_nm_aa:
                    continue
                _city_aa = (_cells_aa[_city_idx_aa].strip()
                            if _city_idx_aa < len(_cells_aa) else '')
                if not _city_aa:
                    _cm = _cpat_aa.search(_raw_nm_aa)
                    _city_aa = _cm.group(1).strip() if _cm else ''
                _st_aa = (_cells_aa[_state_col_aa].strip().lower()[:2]
                          if _state_col_aa >= 0 and _state_col_aa < len(_cells_aa) else '')
                _theatre_aa = _cpat_aa.sub('', _raw_nm_aa).strip()
                # Check VENUE_ALIASES first (before fuzzy), then city+state fuzzy match
                _alias_key_aa = _theatre_aa.lower().strip()
                if _alias_key_aa in VENUE_ALIASES:
                    _venue_aa = VENUE_ALIASES[_alias_key_aa]
                else:
                    _city_key_aa = _BOOKING_CITY_CORRECTIONS.get(_city_aa.lower(), _city_aa.lower())
                    _cands_aa    = _cs_lkp_aa.get((_city_key_aa, _st_aa), [])
                    _matched_aa  = _fuzzy_venue_match(_theatre_aa, _cands_aa) if _cands_aa else ''
                    _venue_aa    = _matched_aa or _theatre_aa
                    if not _matched_aa:
                        log(f"  [aa] no master match for '{_theatre_aa}' ({_city_aa}, {_st_aa.upper()}) — using raw")
                for _fi, _ci in enumerate(_film_idxs_aa):
                    _val = _cells_aa[_ci].strip() if _ci < len(_cells_aa) else ''
                    _vl  = _val.lower()
                    _film_aa = _film_names_aa[_fi] if _fi < len(_film_names_aa) else ''
                    if 'final' in _vl:
                        _act_aa, _phrase_aa = 'Final', ''
                    elif _vl.startswith('hold'):
                        _act_aa = 'Hold'
                        _mod = _vl[4:].strip().lstrip('(*').strip()
                        if _mod == 'a show':
                            _mod = 'shows'
                        _phrase_aa = '' if _mod in ('', '1', 'clean') else _mod
                    elif not _vl:
                        continue
                    else:
                        continue
                    _scr_aa = get_screening_type(_phrase_aa) if _act_aa == 'Hold' else None
                    results.append({'theatre': _venue_aa, 'city': _city_aa,
                                    'action': _act_aa, 'film': _film_aa,
                                    'phrase': _phrase_aa, 'screening_type': _scr_aa})
            log(f"  [aa] parsed {len(results)} results")
            return results
        # ── End Andy Anderson THEATRE/SCR/City/State format ───────────────────

        # ── Jennifer Solorzano "THEATRE/SCR" grid format ──────────────────────
        # Tab-delimited. Preamble lines = film names. Header: THEATRE | SCR | [blank...]
        # Film columns have blank headers; actions in cells.
        # Actions: "Hold Shows", "Hold 1", "Hold Thru Prime", "Final [date]", blank.
        _jen_raw_hdrs = [c.strip() for c in _first_content_line.split('\t')]
        _jen_hdrs     = [h.lower() for h in _jen_raw_hdrs]
        _is_jen = (
            '\t' in _first_content_line
            and _jen_hdrs[0] in ('theatre', 'theater')
            and 'scr' in _jen_hdrs
        )
        if _is_jen:
            _preamble_jen = [l.strip() for l in lines[:header_idx] if l.strip()]
            # Film columns = blank-header columns after position of 'scr'
            _scr_idx_jen  = _jen_hdrs.index('scr')
            _film_idxs_jen = [i for i in range(_scr_idx_jen + 1, len(_jen_hdrs))
                               if not _jen_hdrs[i]]
            _film_names_jen = (_preamble_jen[:len(_film_idxs_jen)]
                               if len(_preamble_jen) >= len(_film_idxs_jen)
                               else _preamble_jen + [''] * (len(_film_idxs_jen) - len(_preamble_jen)))
            _cpat_jen = _re_pbc.compile(r'\(([^,)]+),\s*[A-Z]{2}\)\s*$')
            _date_jen = _re_pbc.compile(r'\s*(thu|fri|sat|sun|mon|tue|wed)?\s*\d{1,2}/\d{1,2}(/\d{2,4})?', _re_pbc.I)
            log(f"  [jen] preamble={_preamble_jen} film_idxs={_film_idxs_jen} films={_film_names_jen}")
            for _dl in content.splitlines()[1:]:
                if not _dl.strip():
                    continue
                _cells = [c.strip() for c in _dl.split('\t')]
                _raw_nm = _cells[0].strip() if _cells else ""
                if not _raw_nm:
                    continue
                _cm = _cpat_jen.search(_raw_nm)
                _city_jen   = _cm.group(1).strip() if _cm else ""
                _theatre_jen = _cpat_jen.sub('', _raw_nm).strip()
                for _fi, _ci in enumerate(_film_idxs_jen):
                    _val = _cells[_ci].strip() if _ci < len(_cells) else ""
                    _vl  = _val.lower()
                    _film_jen = _film_names_jen[_fi] if _fi < len(_film_names_jen) else ""
                    if 'final' in _vl:
                        _act_jen, _phrase_jen = 'Final', ''
                    elif _vl.startswith('hold'):
                        _act_jen = 'Hold'
                        _mod = _vl[4:].strip().lstrip('(*').strip()
                        _phrase_jen = '' if _mod in ('', '1', 'clean') else _mod
                    elif not _vl:
                        continue
                    else:
                        continue
                    _st_jen = get_screening_type(_phrase_jen) if _act_jen == 'Hold' else None
                    results.append({"theatre": _theatre_jen, "city": _city_jen,
                                    "action": _act_jen, "film": _film_jen,
                                    "phrase": _phrase_jen, "screening_type": _st_jen})
            log(f"  [jen] parsed {len(results)} results")
            return results
        # ── End Jennifer Solorzano THEATRE/SCR grid format ────────────────────

        # ── Glen Parham / GTC "Circuit + Theatre Name" format ────────────────
        # Tab-delimited. Columns: Circuit | Theatre Name | City | ST | Title |
        #   DIST | Playwk | Status | WK# | FSS
        # One film per row. Status: "Hold [* qualifier]", "Final", "New..." (skip).
        _gp_hdrs = [h.strip().lower() for h in _first_content_line.split('\t')]
        _is_gp = (
            '\t' in _first_content_line
            and 'circuit' in _gp_hdrs
            and 'theatre name' in _gp_hdrs
            and 'status' in _gp_hdrs
            and 'title' in _gp_hdrs
        )
        if _is_gp:
            _idx = {h: i for i, h in enumerate(_gp_hdrs)}
            _ci_thtr = _idx.get('theatre name', -1)
            _ci_city = _idx.get('city', -1)
            _ci_st   = _idx.get('st',   -1)
            _ci_film = _idx.get('title', -1)
            _ci_stat = _idx.get('status', -1)
            _cs_lkp  = _load_city_state_lookup()
            log(f"  [glen-parham] headers={_gp_hdrs}")
            for _dl in content.splitlines()[1:]:
                if not _dl.strip():
                    continue
                _cells = [c.strip() for c in _dl.split('\t')]
                def _gc(i): return _cells[i].strip() if 0 <= i < len(_cells) else ""
                _thtr_raw = _gc(_ci_thtr)
                _city_raw = _gc(_ci_city)
                _st_raw   = _gc(_ci_st).lower()[:2]
                _film_gp  = _gc(_ci_film)
                _stat_raw = _gc(_ci_stat)
                if not _thtr_raw or not _stat_raw:
                    continue
                _sl = _stat_raw.lower()
                if _sl.startswith('new') or _sl == '-' or not _sl:
                    continue  # opening / unbooked — not a holdover
                if _sl.startswith('final'):
                    _act_gp, _phrase_gp = 'Final', ''
                elif _sl.startswith('hold'):
                    _act_gp = 'Hold'
                    # qualifier after "hold" (strip leading * chars and spaces)
                    _qual = _re_pbc.sub(r'^[\s*]+', '', _sl[4:]).strip()
                    _phrase_gp = '' if _qual in ('', 'schedule') else _qual
                else:
                    continue
                _city_key = _BOOKING_CITY_CORRECTIONS.get(_city_raw.lower(), _city_raw.lower())
                _cands    = _cs_lkp.get((_city_key, _st_raw), [])
                _matched  = _fuzzy_venue_match(_thtr_raw, _cands) if _cands else ''
                _final_gp = _matched or _thtr_raw
                if not _matched:
                    log(f"  [glen-parham] no master match for '{_thtr_raw}' ({_city_raw}, {_st_raw.upper()}) — using raw name")
                _st_gp = get_screening_type(_phrase_gp) if _act_gp == 'Hold' else None
                results.append({"theatre": _final_gp, "city": _city_raw,
                                 "action": _act_gp, "film": _film_gp,
                                 "phrase": _phrase_gp, "screening_type": _st_gp})
            log(f"  [glen-parham] parsed {len(results)} results")
            return results
        # ── End Glen Parham / GTC format ──────────────────────────────────────

        # ── "THEATRE" single-header + alternating name/action format ──────────
        # Preamble lines (e.g. "David", "Solo Mio") are film names.
        # Data follows as: TheatreName(City,ST) / Action [/ Action2 for film2] ...
        # Theatre name lines are identified by "(City, ST)" at the end.
        # If a theatre has 2 action lines and 2 preamble films, first action = film 1.
        # If only 1 action line, apply to all films.
        _is_theatre_hdr = (
            _first_content_line.strip().lower() in ('theatre', 'theater')
            and '\t' not in _first_content_line
        )
        if _is_theatre_hdr:
            _preamble_films_th = [l.strip() for l in lines[:header_idx] if l.strip()]
            _CITY_ST_th  = _re_pbc.compile(r'\([^,)]+,\s*[A-Z]{2}\)\s*$')
            _CITY_EX_th  = _re_pbc.compile(r'\(([^,)]+,\s*[A-Z]{2})\)\s*$')
            # DMA "City, ST" lines (no parens) that follow each theatre row
            _DMA_PAT_th  = _re_pbc.compile(r'^[^()]+,\s*[A-Z]{2}\s*$')
            # Date column headers like "2/6", "12/19"
            _DATE_PAT_th = _re_pbc.compile(r'^\d{1,2}/\d{1,2}$')
            _all_th = [l.strip() for l in content.splitlines() if l.strip()][1:]
            # Detect date-column headers appearing before the first theatre line.
            # e.g. ["DMA", "2/6", "12/19"] → 2 date columns, groups preamble films.
            _pre_th_vals = []
            for _v in _all_th:
                if _CITY_ST_th.search(_v):
                    break
                _pre_th_vals.append(_v)
            _date_cols_th = [v for v in _pre_th_vals if _DATE_PAT_th.match(v)]
            _ndcols_th    = len(_date_cols_th)
            _grp_th       = max(1, len(_preamble_films_th) // _ndcols_th) if _ndcols_th > 0 else 0
            # Collect blocks of (theatre_name, [action, ...]).
            # DMA "City, ST" lines (no parens) are skipped.
            _blocks_th, _cur_nm, _cur_ac = [], None, []
            for _v in _all_th:
                if _CITY_ST_th.search(_v):
                    if _cur_nm is not None:
                        _blocks_th.append((_cur_nm, _cur_ac))
                    _cur_nm, _cur_ac = _v, []
                elif _cur_nm is not None and not _DMA_PAT_th.match(_v):
                    _cur_ac.append(_v)
            if _cur_nm is not None:
                _blocks_th.append((_cur_nm, _cur_ac))
            log(f"  [theatre-hdr] preamble={_preamble_films_th} blocks={len(_blocks_th)} ndcols={_ndcols_th} grp={_grp_th}")
            for _nm, _acts in _blocks_th:
                if not _acts:
                    continue
                _cme = _CITY_EX_th.search(_nm)
                _city_th2 = _cme.group(1).strip() if _cme else ""
                _clean_th = _CITY_EX_th.sub("", _nm).strip()
                def _emit_th(film, act):
                    _al = act.lower()
                    if 'final' in _al:
                        _a = 'Final'
                    elif 'hold' in _al:
                        _a = 'Hold'
                    else:
                        return
                    _ph = act if _a == 'Hold' else ""
                    results.append({"theatre": _clean_th, "city": _city_th2,
                                    "action": _a, "film": film,
                                    "phrase": _ph, "screening_type": get_screening_type(_ph) if _a == 'Hold' else None})
                if _ndcols_th > 0 and _grp_th > 0 and _preamble_films_th:
                    # Date-column grouping: action[i] → preamble_films[i*grp : (i+1)*grp]
                    for _ai, _act_th in enumerate(_acts):
                        _fs = _ai * _grp_th
                        _fe = (_ai + 1) * _grp_th if _ai < _ndcols_th - 1 else len(_preamble_films_th)
                        for _film_th2 in _preamble_films_th[_fs:_fe]:
                            _emit_th(_film_th2, _act_th)
                elif len(_preamble_films_th) >= 2 and len(_acts) >= 2:
                    for _film_th2, _act_th in zip(_preamble_films_th, _acts):
                        _emit_th(_film_th2, _act_th)
                else:
                    for _film_th2 in (_preamble_films_th or [""]):
                        _emit_th(_film_th2, _acts[0])
            log(f"  [theatre-hdr] parsed {len(results)} results")
            return results
        # ── End THEATRE header alternating format ─────────────────────────────

        # ── Headerless "Theatre  Film  Action" 3-column format ──────────────────
        # Some bookers send a plain 3-column sheet with no headers:
        # short theatre/city name | film title | Hold/Final/Open
        # Columns separated by 2+ spaces or a tab.
        _SPLIT_MA = _re_pbc.compile(r'\t|\s{2,}')
        _HFO_MA   = {'hold', 'final', 'open'}
        _ma_sample = [_SPLIT_MA.split(l.strip()) for l in stripped_lines[:8] if l.strip()]
        _ma_hits   = [c for c in _ma_sample if len(c) == 3 and c[2].strip().lower() in _HFO_MA]
        _is_ma_fmt = len(_ma_hits) >= 2 and len(_ma_hits) >= len(_ma_sample) * 0.6
        if _is_ma_fmt:
            log(f"  [ma-3col] detected headerless 3-col format, {len(stripped_lines)} lines")
            for _line in stripped_lines:
                _cols = _SPLIT_MA.split(_line.strip())
                if len(_cols) < 3:
                    continue
                _theatre_ma = _cols[0].strip()
                _film_ma    = _cols[1].strip()
                _act_raw_ma = _cols[2].strip().lower()
                if _act_raw_ma == 'final':
                    _action_ma = 'Final'
                elif _act_raw_ma == 'hold':
                    _action_ma = 'Hold'
                else:
                    continue
                results.append({"theatre": _theatre_ma, "city": "", "action": _action_ma,
                                 "film": _film_ma, "phrase": "", "screening_type": None})
            log(f"  [ma-3col] parsed {len(results)} results")
            return results
        # ── End headerless 3-column format ───────────────────────────────────────

        # ── Clark Film Buying PDF holdover format ─────────────────────────────
        # PDF copy-paste: CITY, ST header lines separate venue groups.
        # Each venue row: Theatre Film Dist Format Week Rank 3Day H|F * [Yes] [Notes]
        # H = Hold; F = Final; Split col Yes → Alternating screening type.
        # Detection: ≥2 all-caps "CITY, ST" lines + ≥2 rows matching the anchor pattern.
        _CFB_CITY_RE = _re_pbc.compile(r'^[A-Z][A-Z\s]+,\s*[A-Z]{2}$')
        _CFB_ROW_RE  = _re_pbc.compile(
            r'^(.+?)\s+([A-Z]{2,5})\s+(\S+)\s+(\d+)\s+(\d+/\d+)\s+(\d+)\s+(H|F)\s+\*\s*(Yes)?\s*(.*)$'
        )
        _full_cfb    = ''.join(lines)
        _cfb_all     = _full_cfb.splitlines()
        _n_cfb_city  = sum(1 for l in _cfb_all if _CFB_CITY_RE.match(l.strip()))
        _n_cfb_rows  = sum(1 for l in _cfb_all if _CFB_ROW_RE.match(l.strip()))
        _is_cfb      = _n_cfb_city >= 2 and _n_cfb_rows >= 2

        if _is_cfb:
            from collections import Counter as _Counter_cfb
            # Pre-pass: collect all "Theatre Film" prefix strings
            _cfb_prefixes = [
                _CFB_ROW_RE.match(l.strip()).group(1).strip()
                for l in _cfb_all if _CFB_ROW_RE.match(l.strip())
            ]
            # Film titles = 2-word suffixes appearing in ≥2 rows
            _cfb_film_freq = _Counter_cfb(
                ' '.join(p.split()[-2:]) for p in _cfb_prefixes if len(p.split()) >= 2
            )
            _cfb_known_films = {s for s, c in _cfb_film_freq.items() if c >= 2}

            _cfb_results: list[dict] = []
            _city_cfb = ''
            for _ln in _cfb_all:
                _ls = _ln.strip()
                if not _ls:
                    continue
                if _CFB_CITY_RE.match(_ls):
                    _city_cfb = _ls.split(',')[0].strip().title()
                    continue
                _m = _CFB_ROW_RE.match(_ls)
                if not _m:
                    continue
                _th_film    = _m.group(1).strip()
                _action_cfb = 'Final' if _m.group(7).upper() == 'F' else 'Hold'
                _split_yes  = bool(_m.group(8))
                # Separate theatre from film: try 2-word suffix first, else fallback
                _words = _th_film.split()
                _theatre_cfb = _th_film
                _film_cfb    = ''
                for _nf in (2, 3):
                    if len(_words) > _nf and ' '.join(_words[-_nf:]) in _cfb_known_films:
                        _theatre_cfb = ' '.join(_words[:-_nf])
                        _film_cfb    = ' '.join(_words[-_nf:])
                        break
                else:
                    # Fallback: last 2 words = film title
                    if len(_words) >= 3:
                        _theatre_cfb = ' '.join(_words[:-2])
                        _film_cfb    = ' '.join(_words[-2:])
                _phrase_cfb = 'shows' if _split_yes and _action_cfb == 'Hold' else ''
                _st_cfb     = get_screening_type(_phrase_cfb) if _action_cfb == 'Hold' else None
                _cfb_results.append({
                    'theatre':        _theatre_cfb,
                    'city':           _city_cfb,
                    'action':         _action_cfb,
                    'film':           _film_cfb,
                    'phrase':         _phrase_cfb,
                    'screening_type': _st_cfb,
                })

            if _cfb_results:
                log(f"  [clark-cfb] {_n_cfb_city} cities, films={sorted(_cfb_known_films)}, parsed {len(_cfb_results)} results")
                return _cfb_results
        # ── End Clark Film Buying PDF format ──────────────────────────────────

        _opl_rows = _parse_one_per_line_to_dicts(content)
        log(f"  [debug] one-per-line returned {len(_opl_rows)} rows; first values: {[l.strip() for l in content.splitlines() if l.strip()][:5]}")
        if (_max_tabs < 2 and _max_commas < 2) or _is_comscore_hdr:
            for row in _opl_rows:
                fl = {k.lower().strip(): v for k, v in row.items()}
                theatre = fl.get("theatre") or fl.get("theater") or ""
                city    = fl.get("city", "").strip()
                action  = fl.get("action")  or fl.get("policy")  or ""
                phrase  = fl.get("phrase")  or fl.get("terms")   or ""
                film    = fl.get("attraction") or fl.get("film") or fl.get("title") or ""
                # ComScore format: theatre is under the film-title column (unknown col)
                if not theatre.strip():
                    _skip = {"buyer","br","unit","attraction","film","title","type","media",
                             "prt","action","policy","status","phrase","comscore #","comscore","#"}
                    for _k, _v in fl.items():
                        if _k not in _skip and _v and not _v.strip().isdigit():
                            theatre = _v
                            break
                if not theatre.strip():
                    continue
                al = action.lower()
                if "final" in al:
                    a = "Final"
                elif not al and _is_dunder_fmt:
                    # Cinemark __COLUMN__ format: blank action = confirmed booking
                    a = "Final"
                elif "hold" in al:
                    a = "Hold"
                else:
                    continue
                st = get_screening_type(phrase or action) if a == "Hold" else None
                results.append({"theatre": theatre.strip(), "city": city, "action": a,
                                 "film": film.strip(), "phrase": phrase, "screening_type": st})
            return results

        delim = "\t" if content.count("\t") > content.count(",") else ","
        reader = csv.DictReader(io.StringIO(content), delimiter=delim)
        if not reader.fieldnames:
            log("ERROR: Empty or invalid CSV")
            return results

        fl = {k: k.lower().strip() for k in reader.fieldnames}

        def col(*names: str) -> str | None:
            for n in names:
                for orig, low in fl.items():
                    if low == n:
                        return orig
            return None

        theatre_col    = col("theatre", "theater", "theater_name", "theatre_name", "location", "venue", "screen")
        action_col     = col("action", "status", "policy", "booking type", "type")
        phrase_col     = col("phrase", "booking phrase", "screening type", "notes")
        film_col       = col("attraction", "film", "title", "production", "picture")
        city_col       = col("city")

        if not theatre_col:
            log("  WARNING: Could not find theatre column — tried: theatre, theater, location, venue, screen")
        if not action_col:
            log("  WARNING: Could not find action column — tried: action, status, policy, booking type, type")

        for row in reader:
            theatre = (row.get(theatre_col) or "").strip() if theatre_col else ""
            action  = (row.get(action_col)  or "").strip() if action_col  else ""
            phrase  = (row.get(phrase_col)  or "").strip() if phrase_col  else ""
            film    = (row.get(film_col)    or "").strip() if film_col    else ""
            city    = (row.get(city_col)    or "").strip() if city_col    else ""

            if not theatre:
                continue

            al = action.lower()
            if "final" in al:
                a = "Final"
            elif "hold" in al:
                a = "Hold"
            else:
                continue  # skip Offer, Request, etc.

            st = get_screening_type(phrase or action) if a == "Hold" else None

            results.append({
                "theatre":        theatre,
                "city":           city,
                "action":         a,
                "film":           film,
                "phrase":         phrase,
                "screening_type": st,  # None = default Clean, no update needed
            })

    except Exception as e:
        log(f"ERROR parsing CSV: {e}")

    return results


# ---------------------------------------------------------------------------
# Playwright automation
# ---------------------------------------------------------------------------

def run_mica_update(contact: str, theatres: list[dict], mode: str = "demo", filter_type: str = "contact_person"):
    """Main Playwright automation entry point. mode: 'demo' or 'prod'."""
    global _active_mica_url, _active_auth_file
    mica_url = MICA_URLS.get(mode, MICA_URLS["demo"])
    auth_file = OUTPUT_DIR / f"mica_auth_{mode}.json"
    _active_mica_url  = mica_url
    _active_auth_file = auth_file
    # Apply booking-name aliases before any processing
    for t in theatres:
        key = t["theatre"].lower().strip()
        if key in VENUE_ALIASES:
            t["theatre"] = VENUE_ALIASES[key]

    finals = [t for t in theatres if t["action"] == "Final"]
    holds  = [t for t in theatres if t["action"] == "Hold"]

    # Deduplicate by (theatre, film) — booking sheets have one row per film per theatre,
    # so the same venue+film combination can appear multiple times. Last entry wins.
    _seen: dict[tuple, dict] = {}
    for t in finals:
        _seen[(t["theatre"], t.get("film", ""))] = t
    finals = list(_seen.values())

    _seen = {}
    for t in holds:
        _seen[(t["theatre"], t.get("film", ""))] = t
    holds = list(_seen.values())

    # Holds take precedence over Finals for the same theatre.
    # Normalize film title (strip "- OC", "- Dub:...", "- 2D/OC" etc.) so that a venue
    # appearing as Hold for the main film AND Final for an OC/dub variant is treated as
    # one row — the Hold wins (it's still running this week).
    def _base_film(film: str) -> str:
        return re.sub(r'\s*[-–]\s*(OC|2D|3D|IMAX|XD|Dub.*|Combo.*).*', '', film, flags=re.I).strip().lower()

    _hold_base_keys = {(t["theatre"].lower(), _base_film(t.get("film", ""))) for t in holds}
    finals = [t for t in finals if (t["theatre"].lower(), _base_film(t.get("film", ""))) not in _hold_base_keys]

    log(f"Mode       : {mode.upper()}")
    log(f"Contact    : {contact}")
    log(f"Filter type: {filter_type}")
    log(f"Finals     : {len(finals)}")
    log(f"Holds      : {len(holds)}")
    log("")

    OUTPUT_DIR.mkdir(exist_ok=True)

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=_HEADLESS, slow_mo=_SLOW_MO,
            args=_BROWSER_ARGS,
        )
        ctx_kwargs = {"viewport": {"width": 1440, "height": 900}}
        if auth_file.exists():
            ctx_kwargs["storage_state"] = str(auth_file)
            log("Using saved Mica session ...")
        ctx  = browser.new_context(**ctx_kwargs)
        page = ctx.new_page()
        if not _SERVER_MODE:
            page.bring_to_front()

        try:
            log(f"Opening {mica_url} ...")
            page.goto(mica_url, wait_until="domcontentloaded", timeout=60_000)
            # Wait for Angular SPA to settle — either the holdovers table or the login form
            try:
                page.wait_for_selector(
                    'table, input[placeholder="Email"], input[type="password"]',
                    timeout=15_000,
                )
            except PlaywrightTimeout:
                pass
            _dismiss_popups(page)

            def _on_login_page(pg) -> bool:
                url = pg.url.lower()
                if _on_auth_url(url):
                    return True
                try:
                    return pg.locator('input[type="password"]').count() > 0
                except Exception:
                    return False

            if _on_login_page(page):
                # Stale session — delete auth file so next run starts clean
                if auth_file.exists():
                    auth_file.unlink()
                if MICA_USER and MICA_PASS:
                    log("Session expired — auto-logging in ...")
                    _auto_login(page)
                else:
                    log("Login required — please log in to Mica in the browser window ...")
                    log("Tip: add MICA_USERNAME and MICA_PASSWORD to your .env file to skip this step.")
                    log("Waiting up to 3 minutes ...")
                    try:
                        page.wait_for_url(
                            lambda url: "login" not in url.lower() and "sign-in" not in url.lower() and "authentication" not in url.lower(),
                            timeout=180_000
                        )
                    except PlaywrightTimeout:
                        log("ERROR: Login timeout — please re-run and log in within 3 minutes.")
                        sys.exit(1)

                log("Logged in! Saving session for future runs ...")
                auth_file.parent.mkdir(exist_ok=True)
                ctx.storage_state(path=str(auth_file))
                page.goto(mica_url, wait_until="domcontentloaded", timeout=60_000)
                _dismiss_popups(page)

            log("Applying filters ...")
            _dismiss_popups(page)
            _apply_filters(page, contact, filter_type=filter_type)
            _screenshot(page, "mica_filtered.png")
            # Dismiss any popup (e.g. Numero error) that appeared during filter application
            _dismiss_popups(page)
            _dismiss_any_dialog(page)

            # ---------- Finals ----------
            if finals:
                final_entries = [{"theatre": t["theatre"], "film": t.get("film", ""), "city": t.get("city", "")} for t in finals]
                log(f"\n--- Finals ({len(finals)}) ---")
                for t in finals:
                    film_label = f"  [{t['film']}]" if t.get("film") else ""
                    log(f"  {t['theatre']}{film_label}")

                n = _set_status_per_row(page, final_entries, "Final", contact=contact)
                if n == 0:
                    log("  WARNING: No matching rows updated for Finals")
                else:
                    log(f"  Status -> Final  OK ({n} rows)")

            # ---------- Holds (one row at a time: status → screening type) ----------
            if holds:
                log(f"\n--- Holds ({len(holds)}) ---")
                for t in holds:
                    film_label = f"  [{t['film']}]" if t.get("film") else ""
                    label = t["screening_type"] or "Clean"
                    log(f"  {t['theatre']}{film_label}  [{t['phrase']}]  -> {label}")

                hold_updated = 0
                for t in holds:
                    entry = [{"theatre": t["theatre"], "film": t.get("film", ""), "city": t.get("city", "")}]
                    n = _set_status_per_row(page, entry, "Hold", contact=contact)
                    if n > 0:
                        hold_updated += n
                        if t["screening_type"]:
                            _set_screening_type_per_row(page, entry, t["screening_type"], contact=contact)
                if hold_updated == 0:
                    log("  WARNING: No matching rows updated for Holds")
                else:
                    log(f"  Holds updated: {hold_updated} rows")

            log("\nMica update complete!")
            _screenshot(page, "mica_done.png")

        except PlaywrightTimeout as exc:
            log(f"\nERROR: Timeout — {exc}")
            _screenshot(page, "mica_error.png")
            raise
        except SystemExit:
            raise
        except Exception as exc:
            log(f"\nERROR: {exc}")
            _screenshot(page, "mica_error.png")
            raise


# ---------------------------------------------------------------------------
# UI helpers — based on observed Mica demo UI behaviour
# ---------------------------------------------------------------------------

def _on_auth_url(url: str) -> bool:
    return any(k in url.lower() for k in ("auth/login", "authentication", "sign-in"))


def _auto_login(page):
    """Fill email + password and click Sign in on the Mica login page."""
    try:
        log(f"  Login page URL: {page.url}")
        log("  Waiting for email field ...")
        page.wait_for_selector('input[placeholder="Email"], input[type="email"]', timeout=20_000)
        log("  Filling credentials ...")
        page.locator('input[placeholder="Email"], input[type="email"]').first.click(force=True)
        page.locator('input[placeholder="Email"], input[type="email"]').first.fill(MICA_USER)
        page.locator('input[placeholder="Password"], input[type="password"]').first.click(force=True)
        page.locator('input[placeholder="Password"], input[type="password"]').first.fill(MICA_PASS)
        log("  Clicking Sign in ...")
        page.locator('button:has-text("Sign in"), button[type="submit"]').first.click()
        # Wait until we leave the login page
        page.wait_for_url(
            lambda url: not _on_auth_url(url),
            timeout=30_000
        )
        log("Auto-login successful.")
    except (PlaywrightTimeout, Exception) as e:
        if _SERVER_MODE:
            log(f"ERROR: Auto-login failed ({e.__class__.__name__}: {e})")
            log("Check your Mica credentials in your Profile settings.")
            sys.exit(1)
        log(f"WARNING: Auto-login failed ({e.__class__.__name__}: {e}) — please log in manually in the browser window ...")
        log("Waiting up to 3 minutes for manual login ...")
        try:
            page.bring_to_front()
            page.wait_for_url(
                lambda url: not _on_auth_url(url),
                timeout=180_000,
            )
            log("Logged in manually.")
        except PlaywrightTimeout:
            log("ERROR: Login timed out — please re-run and log in within 3 minutes.")
            sys.exit(1)


def _screenshot(page, name: str):
    try:
        page.screenshot(path=str(OUTPUT_DIR / name))
    except Exception:
        pass


def _dismiss_popups(page):
    """Dismiss any visible modal dialogs or toast notifications (benign demo-env popups)."""
    for selector in [
        '.modal-header button.btn-close',
        'button.btn-close',
        '[class*="toast"] button',
        '[class*="alert"] button.close',
        '[class*="notification"] button',
        'button[aria-label="Close"]',
        'button[aria-label="close"]',
    ]:
        try:
            btn = page.locator(selector)
            if btn.count() > 0:
                btn.first.click(timeout=800)
                page.wait_for_timeout(300)
        except Exception:
            pass


def _set_ng_select(page, label_text: str, value: str) -> bool:
    """
    Set an ng-select dropdown by finding whichever ng-select element is
    geometrically closest to a label whose text matches label_text.
    This avoids fragile parent-path CSS selectors that break whenever
    Mica changes its DOM layout.
    """
    # JS returns the 0-based index of the ng-select nearest to the label
    idx = page.evaluate("""
    (labelText) => {
        const allNg = Array.from(document.querySelectorAll('ng-select'));
        if (allNg.length === 0) return -1;

        // Prefer searching within the visible modal/dialog
        const dialogs = Array.from(document.querySelectorAll(
            '[role="dialog"], .modal-content, .modal-dialog, .modal.show, .modal.fade.show'
        ));
        const root = dialogs.length > 0 ? dialogs[0] : document;

        // Labels can be <label>, <span>, <p>, <a>, <h*>, <div> — cast wide net
        const candidates = Array.from(root.querySelectorAll(
            'label, .label, span, p, a, h1, h2, h3, h4, h5, h6'
        ));
        const target = candidates.find(
            el => el.textContent.trim().toLowerCase().includes(labelText.toLowerCase())
        );
        if (!target) return -1;

        const lr = target.getBoundingClientRect();
        let best = -1, bestDist = Infinity;
        allNg.forEach((ns, i) => {
            const r = ns.getBoundingClientRect();
            // Manhattan distance from bottom-left of label to top-left of ng-select
            const dist = Math.abs(r.top - lr.bottom) + Math.abs(r.left - lr.left);
            if (dist < bestDist) { bestDist = dist; best = i; }
        });
        return best;
    }
    """, label_text)

    if idx < 0:
        log(f"  WARNING: Could not find ng-select for '{label_text}'")
        return False

    ng_sel = page.locator('ng-select').nth(idx)

    # Clear existing selection
    try:
        clr = ng_sel.locator('.ng-clear-wrapper, .ng-value-icon').first
        if clr.count() > 0:
            clr.click(timeout=500)
            page.wait_for_timeout(300)
    except Exception:
        pass

    # Open the dropdown
    ng_sel.click()
    page.wait_for_timeout(400)

    # Try to type to search (may be disabled in some Mica environments)
    inp = ng_sel.locator('input').first
    input_disabled = False
    if inp.count() > 0:
        try:
            if inp.is_disabled(timeout=300):
                input_disabled = True
            else:
                inp.fill(value)
        except Exception:
            input_disabled = True
    else:
        page.keyboard.type(value)

    if not input_disabled:
        page.wait_for_timeout(600)

    # Click first matching option (works for both searchable and list-only dropdowns)
    opt = page.locator(f'.ng-option:has-text("{value}"), [role="option"]:has-text("{value}")').first
    if opt.count() > 0:
        opt.click()
        page.wait_for_timeout(300)
        return True

    log(f"  WARNING: No option '{value}' in '{label_text}' dropdown — pressing Enter")
    page.keyboard.press("Enter")
    page.wait_for_timeout(300)
    return False


def _set_ng_select_by_locator(page, ng_sel, value: str) -> bool:
    """Set an ng-select given a direct Playwright locator (positional fallback)."""
    try:
        # Clear existing selection
        try:
            clr = ng_sel.locator('.ng-clear-wrapper, .ng-value-icon').first
            if clr.count() > 0:
                clr.click(timeout=500)
                page.wait_for_timeout(300)
        except Exception:
            pass

        ng_sel.click()
        page.wait_for_timeout(400)

        inp = ng_sel.locator('input').first
        if inp.count() > 0 and not inp.is_disabled(timeout=300):
            inp.fill(value)
            page.wait_for_timeout(600)

        opt = page.locator(f'.ng-option:has-text("{value}"), [role="option"]:has-text("{value}")').first
        if opt.count() > 0:
            opt.click()
            page.wait_for_timeout(300)
            return True

        page.keyboard.press("Enter")
        page.wait_for_timeout(300)
        return False
    except Exception as e:
        log(f"  WARNING: _set_ng_select_by_locator failed: {e}")
        return False


def _add_ng_select_value(page, ng_sel, value: str) -> bool:
    """
    Add one value to an already-located ng-select without clearing first.
    Used for multi-select dropdowns where multiple contacts need to be chosen.
    """
    try:
        ng_sel.click()
        page.wait_for_timeout(400)

        inp = ng_sel.locator('input').first
        if inp.count() > 0:
            try:
                if not inp.is_disabled(timeout=300):
                    inp.fill(value)
                    page.wait_for_timeout(600)
            except Exception:
                pass
        else:
            page.keyboard.type(value)
            page.wait_for_timeout(600)

        opt = page.locator(f'.ng-option:has-text("{value}"), [role="option"]:has-text("{value}")').first
        if opt.count() > 0:
            opt.click()
            page.wait_for_timeout(300)
            return True

        page.keyboard.press("Escape")
        page.wait_for_timeout(200)
        return False
    except Exception as e:
        log(f"  WARNING: _add_ng_select_value('{value}') failed: {e}")
        return False


_FILTER_TYPE_LABELS: dict[str, list[str]] = {
    "contact_person": ["Contact(s)", "Contact"],
    "booker":         ["Booker", "Booker(s)"],
    "venue_group":    ["Venue Group", "Venue Group(s)"],
    "venue":          ["Venue(s)", "Venue"],
    "tv_market":      ["TV Market", "TV Market(s)"],
    "capabilities":   ["Capabilities", "Capability"],
}

# Contact name normalisation — maps what users type → what Mica has on file
_CONTACT_NAME_MAP: dict[str, str] = {
    "joshua wymer": "Josh Wymer",
}

def _normalize_contact(name: str) -> str:
    return _CONTACT_NAME_MAP.get(name.strip().lower(), name.strip())

def _apply_filters(page, contact: str, filter_type: str = "contact_person"):
    """
    Apply a filter via Mica's Filter modal.
    The green '+ Add' button opens a modal; we target the ng-select matching filter_type.
    If contact is empty, just clear existing filters and return (show all holdovers).
    """
    # Clear any existing filters first via the 'Clear filters' link
    log("  Clearing existing filters ...")
    clear_link = page.locator('a:has-text("Clear filters"), button:has-text("Clear filters")').first
    if clear_link.count() > 0:
        clear_link.click()
        page.wait_for_timeout(600)

    # No contact specified → show all holdovers (multi-buyer sheets like Clark Film Buying)
    if not contact or not contact.strip():
        log("  No contact specified — running against all holdovers (no filter)")
        return

    # Wait for the table to stabilise before looking for the Add button
    try:
        page.wait_for_selector('table', timeout=15_000)
    except PlaywrightTimeout:
        pass
    # Extra settle time in server/headless mode — overlays may linger longer
    page.wait_for_timeout(2000 if _SERVER_MODE else 500)
    _screenshot(page, "mica_before_add_filter.png")

    # Click the green '+ Add' button to open the Filter modal.
    # Try text selectors first, then fall back to JS DOM search.
    log("  Opening Filter modal via '+ Add' ...")
    add_btn = page.locator('button:has-text("+ Add")').first
    if add_btn.count() == 0:
        add_btn = page.locator('button:has-text("Add")').first  # + is an icon, not text
    if add_btn.count() == 0:
        btn_idx = page.evaluate("""
        () => {
            const btns = Array.from(document.querySelectorAll('button'));
            return btns.findIndex(b => b.textContent.trim().toLowerCase().includes('add'));
        }
        """)
        if btn_idx >= 0:
            add_btn = page.locator('button').nth(btn_idx)
        else:
            log("  WARNING: '+ Add' button not found — skipping contact filter")
            _screenshot(page, "mica_no_add_btn.png")
            return
    # Use JS click to bypass any overlay/backdrop that blocks direct interaction
    try:
        add_btn.click(timeout=10_000)
    except PlaywrightTimeout:
        log("  Direct click timed out — falling back to JS click")
        try:
            page.evaluate("""() => {
                const btn = Array.from(document.querySelectorAll('button'))
                    .find(b => b.textContent.trim().toLowerCase().includes('add'));
                if (btn) btn.click();
            }""")
        except Exception as _e_js:
            log(f"  JS click also failed: {_e_js}")
    page.wait_for_timeout(800)
    _screenshot(page, "mica_filter_modal.png")

    # Wait for the modal to appear
    try:
        page.wait_for_selector('[role="dialog"], .modal-content, .modal', timeout=5_000)
    except PlaywrightTimeout:
        log("  WARNING: Filter modal did not appear after clicking '+ Add'")
        return

    # Wait for ng-select elements inside the modal to fully render
    try:
        page.wait_for_selector('[role="dialog"] ng-select, .modal-content ng-select, .modal ng-select', timeout=5_000)
        page.wait_for_timeout(400)
    except PlaywrightTimeout:
        log("  WARNING: ng-select not visible in filter modal yet — proceeding anyway")

    # Support comma-separated contacts for multi-buyer sheets (e.g. Clark Film Buying)
    contacts = [_normalize_contact(c) for c in contact.split(',') if c.strip()]
    labels = _FILTER_TYPE_LABELS.get(filter_type, _FILTER_TYPE_LABELS["contact_person"])
    log(f"  Setting {labels[0]}: {contacts}")

    # Find the ng-select index once (by label proximity), then add each contact value
    ng_idx = -1
    for lbl in labels:
        ng_idx = page.evaluate("""
        (labelText) => {
            const allNg = Array.from(document.querySelectorAll('ng-select'));
            if (allNg.length === 0) return -1;
            const dialogs = Array.from(document.querySelectorAll(
                '[role="dialog"], .modal-content, .modal-dialog, .modal.show, .modal.fade.show'
            ));
            const root = dialogs.length > 0 ? dialogs[0] : document;
            const candidates = Array.from(root.querySelectorAll(
                'label, .label, span, p, a, h1, h2, h3, h4, h5, h6'
            ));
            const target = candidates.find(
                el => el.textContent.trim().toLowerCase().includes(labelText.toLowerCase())
            );
            if (!target) return -1;
            const lr = target.getBoundingClientRect();
            let best = -1, bestDist = Infinity;
            allNg.forEach((ns, i) => {
                const r = ns.getBoundingClientRect();
                const dist = Math.abs(r.top - lr.bottom) + Math.abs(r.left - lr.left);
                if (dist < bestDist) { bestDist = dist; best = i; }
            });
            return best;
        }
        """, lbl)
        if ng_idx >= 0:
            break

    if ng_idx < 0:
        # Positional fallback: Contact(s) is the 3rd ng-select in the modal (0-indexed: 2)
        log(f"  Trying positional fallback for {labels[0]} (3rd ng-select in modal)...")
        modal_ng = page.locator('[role="dialog"] ng-select, .modal-content ng-select, .modal ng-select')
        if modal_ng.count() >= 3:
            ng_idx_fallback = 2  # absolute index in page, not modal
            # resolve absolute index
            all_ng_count = page.locator('ng-select').count()
            modal_ng_el = modal_ng.nth(2)
            # use _set_ng_select_by_locator for first value, _add_ng_select_value for rest
            first_ok = _set_ng_select_by_locator(page, modal_ng_el, contacts[0]) if contacts else False
            if first_ok:
                for val in contacts[1:]:
                    _add_ng_select_value(page, modal_ng_el, val)
            else:
                log(f"  WARNING: Could not set {labels[0]} filter for {contacts}")
        else:
            log(f"  WARNING: Could not set {labels[0]} filter — ng-select not found")
    else:
        ng_sel = page.locator('ng-select').nth(ng_idx)
        # Clear any existing selection before setting the first value
        try:
            clr = ng_sel.locator('.ng-clear-wrapper, .ng-value-icon').first
            if clr.count() > 0:
                clr.click(timeout=500)
                page.wait_for_timeout(300)
        except Exception:
            pass
        # Select each contact in sequence
        for val in contacts:
            ok = _add_ng_select_value(page, ng_sel, val)
            if not ok:
                log(f"  WARNING: Could not select '{val}' in {labels[0]} filter")

    _screenshot(page, "mica_filter_contact_set.png")

    # Click Save — scope to dialog to avoid the disabled toolbar Save button
    save_btn = page.locator(
        '[role="dialog"] button:has-text("Save"), '
        '.modal-content button:has-text("Save")'
    ).first
    if save_btn.count() > 0:
        save_btn.click()
        page.wait_for_timeout(1500)
        log("  Filter saved.")
    else:
        log("  WARNING: Save button not found in modal")
        page.keyboard.press("Escape")

    # Wait for the filtered table to reload
    try:
        page.wait_for_load_state("networkidle", timeout=10_000)
    except PlaywrightTimeout:
        pass  # networkidle may never fire on SPA — that's OK
    try:
        page.wait_for_selector('table tbody tr', timeout=10_000)
        row_count = page.locator('table tbody tr').count()
        log(f"  Filter applied — table loaded ({row_count} rows).")
    except PlaywrightTimeout:
        log("  WARNING: Timed out waiting for filtered table rows")


def _select_rows(page, theatre_names: list[str]) -> int:
    """
    Check the row checkboxes for rows whose text best matches any of the theatre names.
    Uses word-level scoring so abbreviated booking names (e.g. "Grand Teton Stm 14")
    can match Mica's full venue names (e.g. "Regal Edwards Grand Teton 14").

    Algorithm (runs in JS):
      - For each booking name, extract significant words (length >= 3, no pure numbers).
      - Score every table row by how many of those words appear in its text.
      - Select the highest-scoring row, provided it meets the minimum threshold
        (at least ceil(40% of sig words), minimum 1).
      - Deduplicate: if two booking names resolve to the same row, only click once.

    Returns count of rows checked.
    """
    # First deselect any currently selected rows so we start clean
    page.evaluate("""
    () => {
        document.querySelectorAll('table tbody tr').forEach(row => {
            const cb = row.querySelector('input[type="checkbox"]');
            if (cb && cb.checked) cb.click();
        });
    }
    """)
    page.wait_for_timeout(300)

    js = """
    (theatreNames) => {
        // Common abbreviations used in booking sheets → full words used in Mica
        const ABBREVS = {stm: 'stadium', ctr: 'center', blvd: 'boulevard'};
        // Significant words: split CamelCase, expand abbreviations, drop pure numbers
        function sigWords(name) {
            return name
                .replace(/([a-z])([A-Z])/g, '$1 $2')
                .toLowerCase()
                .replace(/[&\\/\\#,+()$~%.'\"!?@*]/g, ' ')
                .split(/\\s+/)
                .filter(w => w.length >= 3 && !/^\\d+$/.test(w))
                .map(w => ABBREVS[w] || w);
        }

        const rows = Array.from(document.querySelectorAll('table tbody tr'));
        const selectedIndices = new Set();
        const matchLog = [];

        theatreNames.forEach(name => {
            const words = sigWords(name);
            if (words.length === 0) {
                matchLog.push({name, matched: false, reason: 'no sig words'});
                return;
            }

            let bestIdx = -1, bestScore = 0;
            rows.forEach((row, i) => {
                if (!row.querySelector('input[type="checkbox"]')) return; // data rows only
                const text = row.textContent.toLowerCase();
                const matched = words.filter(w => text.includes(w));
                // Primary: count; tie-break by length of longest matching word
                const score = matched.length * 1000 +
                    (matched.length > 0 ? Math.max(...matched.map(w => w.length)) : 0);
                if (score > bestScore) { bestScore = score; bestIdx = i; }
            });

            const matchCount = Math.floor(bestScore / 1000);
            const threshold = Math.max(1, Math.ceil(words.length * 0.4));
            if (bestIdx >= 0 && matchCount >= threshold) {
                selectedIndices.add(bestIdx);
                matchLog.push({
                    name, matched: true, score: matchCount, words: words.length,
                    rowText: rows[bestIdx].textContent.trim().slice(0, 80)
                });
            } else {
                matchLog.push({name, matched: false, score: matchCount,
                                threshold, words: words.join(',')});
            }
        });

        return {indices: [...selectedIndices], log: matchLog};
    }
    """
    result = page.evaluate(js, theatre_names)

    for entry in result.get("log", []):
        if entry["matched"]:
            log(f"    MATCH  '{entry['name']}' -> score {entry['score']}/{entry['words']} -- {entry['rowText'][:60]}")
        else:
            reason = entry.get("reason") or f"best score {entry.get('score',0)}/{entry.get('words','')} < threshold {entry.get('threshold','')}"
            log(f"    NO MATCH  '{entry['name']}' ({reason})")

    # Deselect all currently selected rows (JS is fine here — just unchecking)
    page.evaluate("""
    () => {
        document.querySelectorAll('table tbody tr').forEach(row => {
            const cb = row.querySelector('input[type="checkbox"]');
            if (cb && cb.checked) cb.click();
        });
    }
    """)
    page.wait_for_timeout(300)

    # Select matching rows — use page.mouse for hover+click so CSS :hover state
    # stays active while clicking, which properly triggers Angular change detection.
    count = 0
    for idx in result.get("indices", []):
        row = page.locator("table tbody tr").nth(idx)
        try:
            row.scroll_into_view_if_needed(timeout=3_000)
            row_box = row.bounding_box()
            if not row_box:
                log(f"    WARNING: No bounding box for row {idx}")
                continue
            # Move mouse onto the row to trigger CSS :hover (reveals the checkbox)
            page.mouse.move(
                row_box['x'] + row_box['width'] * 0.05,
                row_box['y'] + row_box['height'] / 2,
            )
            page.wait_for_timeout(200)
            # Click the checkbox at its actual screen coordinates
            cb = row.locator('input[type="checkbox"]').first
            if cb.count() == 0:
                log(f"    WARNING: No checkbox found for row {idx}")
                continue
            cb_box = cb.bounding_box()
            if cb_box:
                page.mouse.click(
                    cb_box['x'] + cb_box['width'] / 2,
                    cb_box['y'] + cb_box['height'] / 2,
                )
            else:
                # Fallback: force-click if checkbox has no visible bounding box
                cb.click(force=True)
            count += 1
            page.wait_for_timeout(100)
        except Exception as e:
            log(f"    WARNING: Could not check row {idx}: {e}")

    page.wait_for_timeout(400)
    return count


def _click_bulk_change(page) -> bool:
    """
    Click the 'Bulk Change' button in the toolbar.
    Returns True if clicked AND the Bulk Change modal opened, False otherwise.
    """
    # Wait up to 3s for the button to be present
    try:
        page.wait_for_function(
            "() => Array.from(document.querySelectorAll('button, a, [role=\"button\"]'))"
            ".some(b => /^bulk\\s*change/i.test(b.textContent.trim()))",
            timeout=3_000,
        )
    except PlaywrightTimeout:
        pass

    # Only match elements whose trimmed text STARTS WITH "Bulk Change" to avoid
    # accidentally clicking "Bulk updates" or other Bulk-prefixed buttons.
    clicked = False
    for sel in [
        'button:has-text("Bulk Change")',
        'a:has-text("Bulk Change")',
        '[role="button"]:has-text("Bulk Change")',
    ]:
        candidates = page.locator(sel).all()
        for btn in candidates:
            try:
                txt = btn.text_content() or ""
                if txt.strip().lower().startswith("bulk change"):
                    btn.click()
                    clicked = True
                    break
            except Exception:
                continue
        if clicked:
            break

    if not clicked:
        log("  WARNING: Bulk Change button not found")
        return False

    # Verify the Bulk Change modal actually opened
    try:
        page.wait_for_selector('[role="dialog"]', timeout=4_000)
        page.wait_for_timeout(400)
        return True
    except PlaywrightTimeout:
        log("  WARNING: Bulk Change modal did not open after button click")
        return False


def _bulk_set_status(page, status: str):
    """
    Set status via the toolbar 'Status ▼' dropdown (NOT the Bulk Change modal).

    Observed UI flow (from walkthrough):
      1. Rows selected → toolbar shows 'Status ▼' button
      2. Click 'Status ▼' → dropdown appears with Hold / Final / To Do etc.
      3. Click the desired status option
      4. Confirmation dialog: 'Change status to `Hold`' → click Continue
    """
    # Click the Status dropdown button in the toolbar
    status_btn = page.locator('button:has-text("Status")').first
    if status_btn.count() == 0:
        log(f"  WARNING: 'Status' dropdown button not found in toolbar")
        return
    status_btn.click()
    page.wait_for_timeout(500)

    # Click the matching status option.
    # Dropdown options show "Hold (N)" / "Final (N)" — table row buttons show "Hold -" / "Final -".
    # Matching on "{status} (" avoids accidentally clicking a table row button.
    opt = page.locator(f'button:has-text("{status} (")').first
    if opt.count() == 0:
        # Fallback: try any element with the status text
        opt = page.locator(
            f'[role="option"]:has-text("{status}"), '
            f'li:has-text("{status}"), '
            f'button:has-text("{status}"), '
            f'a:has-text("{status}")'
        ).first
    if opt.count() == 0:
        log(f"  WARNING: Status option '{status}' not found in dropdown")
        page.keyboard.press("Escape")
        return
    opt.click()
    page.wait_for_timeout(500)

    # Confirmation dialog: "Change status to `Hold`" → click Continue
    confirm = page.locator('button:has-text("Continue")').first
    if confirm.count() > 0:
        page.wait_for_timeout(300)
        confirm.click()
        page.wait_for_timeout(1500)
    else:
        for label in ("OK", "Yes", "Confirm"):
            alt = page.locator(f'button:has-text("{label}")').first
            if alt.count() > 0:
                alt.click()
                page.wait_for_timeout(1500)
                break

    _dismiss_error_popups(page)


def _bulk_set_screening_type(page, screening_type: str, contact: str = ""):
    """
    Bulk Change → check Screening Types checkbox → pick type from ng-select → Apply.

    Bulk Change modal structure (from observed UI):
      [checkbox]  Screening Types   [ng-select "Select: All"]
      [checkbox]  Showcodes         [select "Standard"]
      ...
    The ng-select is disabled until its checkbox is checked.
    Confirmed options: Clean, Single Matinee, Multiple Matinees, Prime, Late, Alternating.
    """
    _ensure_holdovers_page(page, contact)
    if not _click_bulk_change(page):
        return
    try:
        page.wait_for_selector('[role="dialog"]', timeout=5_000)
    except PlaywrightTimeout:
        pass
    page.wait_for_timeout(600)

    # Step 1: Find the "Screening Types" checkbox and click it with bubbling events
    # so Angular change detection fires.
    #
    # Strategy A: scan every checkbox in the modal; walk UP from it and check if
    # that container's text mentions "Screening" — handles sibling label layouts.
    # Strategy B: TreeWalker finds any text node containing "Screening", then walks
    # UP to find a checkbox in an ancestor.
    # Strategy C: fallback — click the first unchecked checkbox in the modal
    # (Screening Types is typically first).
    cb_result = page.evaluate("""
        () => {
            const modal = document.querySelector('[role="dialog"]') ||
                          document.querySelector('.modal-content') ||
                          document.querySelector('.modal');
            if (!modal) return 'no_modal';

            function clickCb(cb) {
                if (!cb.checked) {
                    cb.click();
                    cb.dispatchEvent(new MouseEvent('click', {bubbles: true}));
                    cb.dispatchEvent(new Event('change', {bubbles: true}));
                }
            }

            // Strategy A: find checkboxes, check container text
            const checkboxes = Array.from(modal.querySelectorAll('input[type="checkbox"]'));
            for (const cb of checkboxes) {
                let container = cb.parentElement;
                for (let i = 0; i < 8 && container && container !== modal; i++) {
                    const txt = container.innerText || container.textContent || '';
                    if (/screening/i.test(txt)) {
                        clickCb(cb);
                        return 'strat_a_clicked';
                    }
                    container = container.parentElement;
                }
            }

            // Strategy B: TreeWalker finds "Screening" text node, walks up for checkbox
            const walker = document.createTreeWalker(modal, NodeFilter.SHOW_TEXT, null);
            let textNode;
            while ((textNode = walker.nextNode())) {
                if (/screening/i.test(textNode.textContent)) {
                    let el = textNode.parentElement;
                    for (let i = 0; i < 10 && el && el !== modal; i++) {
                        const cb = el.querySelector('input[type="checkbox"]');
                        if (cb) { clickCb(cb); return 'strat_b_clicked'; }
                        el = el.parentElement;
                    }
                }
            }

            // Strategy C: first unchecked checkbox
            const first = checkboxes.find(cb => !cb.checked);
            if (first) { clickCb(first); return 'strat_c_first_of_' + checkboxes.length; }

            return 'not_found_' + checkboxes.length + '_cbs';
        }
    """)
    log(f"  Screening Types checkbox JS: {cb_result}")
    page.wait_for_timeout(800)

    # Step 2: Open the ng-select for "Screening Types".
    # After the checkbox is checked, the ng-select for that row should be enabled.
    # Strategy: find the ng-select in the same container as the "Screening" text;
    # fallback to the first ng-select in the modal.
    ns_opened = page.evaluate("""
        () => {
            const modal = document.querySelector('[role="dialog"]') ||
                          document.querySelector('.modal-content') ||
                          document.querySelector('.modal');
            if (!modal) return false;

            // Find a text node containing "Screening", walk up to find ng-select sibling
            const walker = document.createTreeWalker(modal, NodeFilter.SHOW_TEXT, null);
            let textNode;
            while ((textNode = walker.nextNode())) {
                if (/screening/i.test(textNode.textContent)) {
                    let el = textNode.parentElement;
                    for (let i = 0; i < 10 && el && el !== modal; i++) {
                        const ns = el.querySelector('ng-select');
                        if (ns) {
                            ns.dispatchEvent(new MouseEvent('click', {bubbles: true}));
                            return true;
                        }
                        el = el.parentElement;
                    }
                }
            }
            // Fallback: first ng-select in modal
            const first = modal.querySelector('ng-select');
            if (first) { first.dispatchEvent(new MouseEvent('click', {bubbles: true})); return true; }
            return false;
        }
    """)
    page.wait_for_timeout(500)

    # Step 3: Select the option from the open dropdown
    opt = page.locator(f'.ng-option:has-text("{screening_type}")').first
    if opt.count() > 0:
        opt.click()
        log(f"  Selected '{screening_type}' from Bulk Change ng-select")
    else:
        # Type to filter then click
        page.keyboard.type(screening_type[:3])
        page.wait_for_timeout(400)
        opt = page.locator(f'.ng-option:has-text("{screening_type}")').first
        if opt.count() > 0:
            opt.click()
            log(f"  Selected '{screening_type}' via type+click")
        else:
            log(f"  WARNING: '{screening_type}' not found in Bulk Change dropdown")
            page.keyboard.press("Escape")
            page.wait_for_timeout(300)
            return
    page.wait_for_timeout(300)

    # Step 4: Click Apply
    if page.locator('[role="dialog"]').count() == 0:
        log("  WARNING: Bulk Change modal closed unexpectedly — skipping Apply")
        return

    apply_btn = page.locator(
        '[role="dialog"] button:has-text("Apply"), [role="dialog"] button:has-text("Save")'
    ).first
    if apply_btn.count() > 0:
        apply_btn.click()
        page.wait_for_timeout(1500)
        log(f"  Applied Bulk Change → {screening_type}")
    else:
        log("  WARNING: Apply/Save button not found in Bulk Change modal")
        page.keyboard.press("Escape")
        return

    _dismiss_any_dialog(page)


# ---------------------------------------------------------------------------
# City+state → actual venue name for small-exhibitor "City, ST  HOLD" bookings.
# Applied before _FIND_ONE_JS so the JS word-scorer can match by venue name.
# ---------------------------------------------------------------------------
_CITY_VENUE_ALIASES: dict[str, str] = {
    # ── Cinemark DFW local-name → Mica master name ────────────────────────────
    "cinemark central plano 10": "cinemark movies plano 10",
    "cut! by cinemark":          "cinemark cut! 10",
    "cinemark 17":               "cinemark 17 + imax",
    "rave ridgmar 13":           "cinemark ridgmar mall 13 + xd",
    "rave north east mall 18":   "cinemark northeast mall 18 + xd",
    "cinemark cleburne":         "cinemark cinema cleburne 6",
    "cinemark 12 and xd":        "cinemark mansfield 12 + xd",
    "tinseltown grapevine and xd": "cinemark tinseltown grapevine 17 + xd",
    "cinemark 17 + imax":          "cinemark tulsa 17",
    # City-qualified (key = "booking name, city" — resolved before plain name)
    "cinemark 14, cedar hill":   "cinemark cedar hill 14",
    "movies 14, lancaster":      "cinemark movies lancaster 14",
    "cinemark 14, denton":       "cinemark denton 14",
    "cinemark 12, sherman":      "cinemark sherman 12",
    "movies 8, paris":           "cinemark movies paris 8",
    "cinemark west plano, plano":             "cinemark movies plano 10",
    "cinemark west plano":                    "cinemark movies plano 10",
    "cinemark (the legacy), plano":           "Cinemark Legacy 24 + XD",
    "cinemark (the legacy)":                  "Cinemark Legacy 24 + XD",
    "cinemark allen 16 and xd, allen":        "Cinemark Allen 16 + XD",
    "cinemark allen 16 and xd":               "Cinemark Allen 16 + XD",
    "cinemark roanoke 14, roanoke":           "Cinemark Roanoke 14 + XD",
    "cinemark roanoke 14":                    "Cinemark Roanoke 14 + XD",
    "cinemark rockwall 14 and xd, rockwall":  "Cinemark Rockwall 14 + XD",
    "cinemark rockwall 14 and xd":            "Cinemark Rockwall 14 + XD",
    # ── Brad Bills small-exhibitor city+state → actual venue name ─────────────
    "espanola, nm":      "dreamcatcher 10",
    "espanola nm":       "dreamcatcher 10",
    "espanola":          "dreamcatcher 10",
    "independence, mo":  "pharaoh independence 4",
    "independence mo":   "pharaoh independence 4",
    "guymon, ok":        "northridge guymon 8",
    "guymon ok":         "northridge guymon 8",
    "florence, sc":      "julia florence 4",
    "florence sc":       "julia florence 4",
    "tulsa, ok":         "eton tulsa 6",
    "tulsa ok":          "eton tulsa 6",
    "kirksville, mo":    "downtown kirksville 8",
    "kirksville mo":     "downtown kirksville 8",
    "marion, nc":        "hometown cinemas marion 2",
    "marion nc":         "hometown cinemas marion 2",
    "fulton, mo":        "fulton cinema 8",
    "fulton mo":         "fulton cinema 8",
    "lumberton, nc":     "hometown lumberton 4",
    "lumberton nc":      "hometown lumberton 4",
    "marshall, mo":      "cinema marshall 3",
    "marshall mo":       "cinema marshall 3",
    "milford, ia":       "pioneer milford 1",
    "milford ia":        "pioneer milford 1",
    "parsons, ks":       "the parsons theatre",
    "parsons ks":        "the parsons theatre",
    "norton, ks":        "norton theatre",
    "norton ks":         "norton theatre",
    # City-qualified overrides (booking name, city) → master list name
    "cinemark 22 + imax":                    "cinemark lancaster 22",
    "cinemark 22 + imax, lancaster":          "cinemark lancaster 22",
    "cinemark 16 +xd, victorville":           "cinemark victorville 16 + xd",
    "landmark 12 surrey":                     "landmark guildford 12",
    # ── Cinemark national shorthand → Mica full name ─────────────────────────
    "tinseltown usa, jacksonville":           "cinemark tinseltown jacksonville 20 + xd",
    "tinseltown usa, fayetteville":           "cinemark tinseltown fayetteville 17 + xd",
    "tinseltown usa, north aurora":           "cinemark tinseltown north aurora 17 usa",
    "cinemark orlando and xd":               "cinemark festival bay orlando 20 + xd",
    "cinemark orlando and xd, orlando":      "cinemark festival bay orlando 20 + xd",
    "cinemark west dundee, il":               "cinemark spring hill mall 8 + xd",
    "cinemark west dundee":                   "cinemark spring hill mall 8 + xd",
    "movies 8 ladson oakbrook ii":            "cinemark movies summerville 8",
    "movies 8 ladson oakbrook ii, summerville": "cinemark movies summerville 8",
    "movies 10, bourbonnais":                 "cinemark movies bourbonnais 10",
    "movies 10":                              "cinemark movies bourbonnais 10",
    "cinemark louis joliet mall":             "cinemark louis joliet mall 14",
    "deer park 16":                           "cinemark century deer park 16",
    "deer park 16, deer park":                "cinemark century deer park 16",
    "valparaiso commons shopping center":     "cinemark at valparaiso 12",
    "cinemark palace 20, boca raton":         "Cinemark Palace 20 + XD",
    "cinemark palace 20":                     "Cinemark Palace 20 + XD",
    "cinemark paradise 24, davie":            "Cinemark Paradise 24 + XD",
    "cinemark paradise 24":                   "Cinemark Paradise 24 + XD",
    "cinemark bluffton, bluffton":            "Cinemark Bluffton 12",
    "cinemark bluffton":                      "Cinemark Bluffton 12",
    "cinemark seven bridges":                 "cinemark 7 bridges woodridge 16 imax",
    "cinemark seven bridges, woodridge":      "cinemark 7 bridges woodridge 16 imax",
    # ── Cinemark Taylor Reynolds circuit (THEATRE-header format) ────────────────
    "cinemark 16, mesa":                      "cinemark mesa 16",
    "cinemark 16, provo":                     "cinemark provo 16",
    "cinemark 24 + xd, west jordan":          "cinemark west jordan 24+xd",
    "imperial valley 14, el centro":          "cinemark century imperial valley mall 14",
    "imperial valley 14":                     "cinemark century imperial valley mall 14",
    "sierra vista 10, sierra vista":          "cinemark sierra vista 10",
    "sierra vista 10":                        "cinemark sierra vista 10",
    "cinemark spanish fork + xd, spanish fork": "cinemark spanish fork 8+xd",
    "cinemark spanish fork + xd":             "cinemark spanish fork 8+xd",
    "henderson 12, henderson":                "Cinemark Cinedome 12 (Henderson)",
    "henderson 12":                           "Cinemark Cinedome 12 (Henderson)",
    "cinemark draper + xd, draper":           "Cinemark Draper 12 + XD",
    "cinemark draper + xd":                   "Cinemark Draper 12 + XD",
    "cinemark farmington + xd, farmington":   "Cinemark Farmington 14+ XD",
    "cinemark farmington + xd":               "Cinemark Farmington 14+ XD",
    "cinemark riverton + xd, riverton":       "Cinemark Riverton Ridgewood 14 +XD",
    "cinemark riverton + xd":                 "Cinemark Riverton Ridgewood 14 +XD",
    "century el con + xd, tucson":            "Cinemark Century 20 El Con and XD (Tucson)",
    "century el con + xd":                    "Cinemark Century 20 El Con and XD (Tucson)",
    "cinemark 12, american fork":             "Cinemark American Fork 12",
    # Kaitlin Privitera (AMC KC/Midwest) — report short names differ from Mica names
    "studio 28":               "amc studio olathe 30",
    "independence 20":         "amc independence commons 20",
    "prairiefire 17":          "amc dine-in prairie fire 17",
    "legends 14":              "amc legends kansas city 14",
    "northrock 14":            "amc northrock wichita 14",
    "springfield 11":          "amc springfield 11",
    # James Douglas (AMC SE/Florida) — report short names differ from Mica names
    "regency 24":            "amc regency sq jacksonville 24",
    "aventura mall 24":      "amc aventura 24",
    "sunset place 24":       "amc sunset s miami 24",
    "weston 8":              "amc weston cinema sunrise 8",
    "bayou 15":              "amc bayou pensacola 15",
    "avenue 16":             "amc avenue melbourne 16",
    "tallahassee 20":        "amc tallahassee mall 20",
    "veterans expressway 24":"amc veterans tampa 24",
    "regency 20":            "amc regency sq brandon 20",
    "woodlands square 20":   "amc woodland sq oldsmar 20",
    "riverview 14":          "amc riverview gibsonton 14",
    "westshore plaza 14":    "amc westshore tampa 14",
    "sundial 12":            "amc sundial st petersburg",
    "coral ridge 10":        "amc coral ridge ft lauderdale 10",
    "pensacola 18":          "amc classic pensacola 18",
    # Sergio Candelas (AMC Central/Southwest) — report short names differ from Mica names
    "quail springs mall 24": "amc quail springs oklahoma city 24",
    "town square 18":        "amc town square las vegas 18",
    "decatur 10":            "amc classic decatur 10",
    "springfield 12":        "amc classic springfield 12",
    "springfield 8":         "amc springfield 8",
    "esplanade 14":          "amc dine-in esplanade 14",
    # Ron Wooley (AMC NE) — report short names differ from Mica names
    "plainville 20":   "amc plainville cinema 20",
    "palisades 21":    "amc palisades center 21",
    "freehold 14":     "amc freehold metroplex 14",
    "aviation 12":     "amc aviation linden 12",
    "levittown 10":    "amc dine-in levittown 10",
    "landmark 8":      "amc landmark 8",
    # Dave Glass (AMC) — report short names differ from Mica names
    "southlands 16":        "amc southlands aurora 16",
    "flatiron crossing 14": "amc flatiron broomfield 14",
    "orchard 12":           "amc orchard town center westminster 12",
    "mayfair 18":           "amc mayfair mall wauwatosa 18",
    "southdale center 16":  "amc southdale edina 16",
    "southgate 9":          "amc dine-in southgate 9",
    "southcenter 16":       "amc southcenter tukwila 16",
    "factoria 8":           "amc factoria bellevue 8",
    "alderwood 16":         "amc alderwood lynnwood 16",
    # Kathryn Wintermyer (AMC Detroit/OH/PA) — report short names differ from Mica names
    "forum 30":             "amc forum sterling heights 17",
    "gratiot 15":           "amc star gratiot clinton township 15",
    "john r 15":            "amc john r theatre 15",
    "stonybrook 20":        "amc stonybrook louisville 20",
    "309 cinema 9":         "amc 309 cinemas north wales 9",
    "berkshire 8":          "amc berkshire wyomissing 8",
    "marlton 8":            "amc marlton cinemas 8",
    "westmoreland 15":      "amc westmoreland greensburg 15",
    # David Saunders (Pacific NW / OR / WA indie circuit)
    "bremerton":            "seefilm cinema",
    "cinestars":            "hood river cinemas 5",
    "coast":                "coast fort bragg 4",
    "milwaukie":            "milwaukie portland 2",
    "living room pdx":      "living room theatres portland",
    "living room indy":     "living room theaters indianapolis",
    "battle ground":        "battle ground cinema 8",
    "canby":                "canby cinema 8",
    "independence":         "independence cinema 8",
    "oak grove":            "oak grove portland 8",
    "roseburg":             "roseburg cinema",
    "sandy":                "sandy cinema 8",
    "scappoose":            "scappoose cinema 7",
    # ── Small Indies / Diane Johnson circuit ──────────────────────────────────
    "lamar, mo":            "plaza lamar 1",
    "lamar mo":             "plaza lamar 1",
    "borger, tx":           "morley borger 5",
    "borger tx":            "morley borger 5",
    "mountain grove, mo":   "fun city 5 cinemas",
    "mountain grove mo":    "fun city 5 cinemas",
    "mountain grove":       "fun city 5 cinemas",
    "mt vernon 8":          "Mount Vernon 8",
    "mesa grand 14":        "Mesa Grande 14",
    "southern hill 12":     "Southern Hills 12",
    "arrowhead town center 14": "Arrowhead 14",
    "tulsa 12":             "Tulsa Hills 12",
    "southroads 20":        "Southroads Tulsa 20",
    "foothills 15":         "Foothills Tucson 15",
    "surprise 14":          "Surprise Pointe 14",
    # ── Regal / STM format aliases ────────────────────────────────────────────
    # Brandon Corrier (Regal PA/NJ/NY circuit)
    "hadley theatre stm 16":            "Regal Hadley Cinemas South Plainfield 16",
    "washington township 14":           "Regal Washington Township Sewell 14",
    "reg king of prussia 4dx & imax":   "Regal King Of Prussia 16",
    # Mark Waring (Regal MD/VA/DC circuit)
    "fox stm 16 & imax":                "Regal Fox Ashburn 16",
    "kingstowne stm 16 & rpx":          "Regal Kingstowne Cinema 16",
    "laurel towne centre 12":           "Regal Laurel Town Center 12",
    "valley mall stm 16":               "Regal Valley Mall Hagerstown 16",
    # Ashley Hensley (Regal Mountain West/CO/HI circuit)
    "boise stm 22 & imax":              "Regal Edwards Boise 21 ScreenX, 4DX & IMAX",
    # Alanna Peffley (Regal FL/VA/NC circuit)
    "regal dania point 4dx rpx &vip":   "Regal Dania Pointe 16",
    "regal dania point 4dx rpx & vip":  "Regal Dania Pointe 16",
    "regal bistro at the falls":         "Regal Falls Miami 12",
    "kendall vlg stm 16 imax & rpx":    "Regal Kendall Village Miami 16",
    "pavilion stm 14 & rpx":            "Regal Pavilion Port Orange 14",
    "champlain centre stm 8":           "Champlain Plattsburgh 8",
    "e. greenbush 8":                   "East Greenbush 8",
    "aviation mall 9":                  "Aviation Mall Queensbury 9",
    "natomas mktplace stm 16 & rpx":    "Natomas Marketplace 16",
    "stockton cty ctr stm 16 & imax":   "Stockton City Center 16",
    "auburn stm 17":                    "Auburn Stadium 17",
    "bridgeport stm 18 & imax":         "Bridgeport Tigard 18",
    "cascade stm 16 imax & rpx":        "Cascade Vancouver 16",
    "cinema 99 stm 11":                 "Cinema 99 Vancouver",
    "city center stm 12":               "City Vancouver 12",
    "movies on tv stm 16":              "Movies Hillsboro 16",
    "santiam stm 11":                   "Santiam Salem 11",
    "stark street stm 10":              "Stark Gresham 10",
    # ── Celebration Cinema aliases ────────────────────────────────────────────
    "cinema carousel 16":               "Celebration Cinema Carousel",
    "crossroads 15 + imax":             "Celebration Crossroads Imax",
    "rivertown 13 + c premium":         "Celebration Cinema Rivertown",
    "grand rapids north 17 + imax":     "Celebration Cinema GR North",
    "grand rapids south 15 + c premium":"Celebration South",
    "lansing 19 + c premium xl":        "Celebration Lansing Imax",
    "mt. pleasant 11":                  "Celebration Mt Pleasant",
    "benton harbor 14 + dbox":          "Celebration Benton Harbor",
    # ── Michael Eiff (Cinemark OH / IA circuit) ──────────────────────────────
    "cinemark 14, strongsville":         "cinemark strongsville 14",
    "cinemark 14, mansfield":            "cinemark mansfield 14",
    "cinemark 12, zanesville":           "cinemark colony square mall",
    "tinseltown usa + xd, north canton": "cinemark tinseltown n canton 24+ xd",
    "movies 16, gahanna":                "cinemark stoneridge plaza movies 16",
    "w. des moines jordan creek + xd":   "cinemark century 20 jordan creek and xd",
    # ── Jennifer Solorzano (Cinemark CO / NM / TX West circuit) ─────────────
    "cinemark 12, greeley":               "Cinemark Greeley 12",
    "cinemark 16, fort collins":          "Cinemark Fort Collins 16",
    "main place 6, mcallen":              "Cinemark Movies 6",
    "main place 6":                       "Cinemark Movies 6",
    "cinemark 16 + xd, brownsville":      "Cinemark Brownsville 16 + XD",
    "cinemark 16 + xd, harlingen":        "Cinemark Harlingen 16 + XD",
    "cinemark abilene and xd, abilene":   "Cinemark Abilene 12",
    "cinemark abilene and xd":            "Cinemark Abilene 12",
    # ── Andy Anderson (Cinemark SF Bay Area) ─────────────────────────────────
    "century at hayward, hayward":        "Cinemark Century At Hayward 12",
    "century at hayward":                 "Cinemark Century At Hayward 12",
    # ── Beth Teal (Cinemark East / Midwest) ──────────────────────────────────
    "cinemark towson + xd, towson":           "Cinemark Towson 15 + XD",
    "cinemark towson + xd":                   "Cinemark Towson 15 + XD",
    "cinemark 15 + xd, hadley":               "Cinemark Hadley 15 + XD",
    "cinemark tinseltown + xd, louisville":   "Cinemark Tinseltown Louisville + XD",
    "cinemark paducah, paducah":              "Cinemark Paducah 12",
    "cinemark paducah":                       "Cinemark Paducah 12",
    # ── Jennifer Hernandez (Cinemark SoCal — LA / Palm Springs) ─────────────
    "cinemark 12 and xd, los angeles":        "Cinemark 12 Howard Hughes LA and XD",
    "cinemark 16, palmdale":                  "Cinemark Antelope Valley Mall Palmdale 16",
    "century la quinta + xd, la quinta":      "Cinemark Century La Quinta 12 + XD",
    # ── Allie Fullmer (Cinemark TX — Houston / Austin / SA / Corpus) ─────────
    "tinseltown 17 + xd, the woodlands":      "Cinemark The Woodlands 17 + XD",
    "cinemark 18 + xd, webster":              "Cinemark Webster 18 + XD",
    "hollywood usa 20, pasadena":             "Cinemark Hollywood Pasadena 20",
    "cinemark 19 + xd, katy":                 "Cinemark Katy 19 + XD",
    "cinemark 12 + xd, pearland":             "Cinemark Pearland 12 + XD",
    "cinemark 12 + xd, cypress":              "Cinemark Cypress 12 + XD",
    "cut! by cinemark cypress, cypress":      "Cinemark Cut 8!",
    "cut! by cinemark cypress":               "Cinemark Cut 8!",
    "cinemark 12, rosenberg":                 "Cinemark Rosenberg 12",
    "cinemark 12, victoria":                  "Cinemark Victoria 12",
    "century 16 + imax, corpus christi":      "Cinemark Century Corpus Christi 16 + XD and IMAX",
    "college station + xd, college station":  "Cinemark College Station 18 + XD",
    "stone hill town center, pflugerville":   "Cinemark Stone Hill Town Ctr Pflugerville 9 *TEMP 5*",
    "stone hill town center":                 "Cinemark Stone Hill Town Ctr Pflugerville 9 *TEMP 5*",
    "cinemark 14, round rock":                "Cinemark Round Rock 14",
    "southpark meadows 14, austin":           "Cinemark Southpark Mall Austin 14",
    "southpark meadows 14":                   "Cinemark Southpark Mall Austin 14",
    "movies 8, del rio":                      "Cinemark Movies Del Rio 8",
    "cinemark 7, eagle pass":                 "Cinemark Eagle Pass 7",
    # ── Tammy Flores / Hooky Entertainment ───────────────────────────────────
    "hooky entertainment + sdx + imax, hutto": "Hooky Entertainment + SDX + IMAX Hutto 8",
    "hooky entertainment + sdx + imax":        "Hooky Entertainment + SDX + IMAX Hutto 8",
    "redstone 14 cinemas w/pdx":               "Red Stone 14 Cinemas",
    "redstone 14 cinemas":                     "Red Stone 14 Cinemas",
    # ── Tom McCauley (AMC New England / Mid-Atlantic) ─────────────────────────
    "methuen 20":               "AMC Methuen at the Loop 20",
    "hampton 24":               "AMC Hampton Towne Centre 24",
    "loudoun 11":               "AMC Loudoun Station Ashburn 11",
    "worldgate 9":              "AMC Worldgate Herndon 9",
    "columbia mall 14":         "AMC Columbia Maryland 14",
    "mj capital center 12":     "AMC Magic Johnson Capital Center 12",
    "st charles towne center 9":"AMC St. Charles Waldorf 9",
    "academy 8":                "AMC Academy Greenbelt 8",
    # ── Justin Johnson (AMC Chicago / Midwest / Indiana) ─────────────────────
    "yorktown 18":              "AMC Yorktown Lombard 18",
    "galewood 14":              "AMC Galewood Crossings 14",
    "hawthorn 12":              "AMC Hawthorn Vernon Hills 12",
    "randhurst 12":             "AMC Randhurst Mount Prospect 12",
    "peru 8":                   "AMC Peru Mall 8",
    "castleton square 14":      "AMC Castleton Indianapolis 14",
    "washington sq 12":         "AMC Washington Square 12",
    # ── Dan Cammarata (AMC Texas / SE) ───────────────────────────────────────
    "northpark 15":             "AMC North Park Dallas 15 & IMAX",
    "lakeline 9":               "AMC Lakeline Mall Cedar Park 9",
    "fountains 18":             "AMC Fountains Stafford 18 & IMAX",
    "rivercenter 11":           "AMC Rivercenter San Antonio 9",
    "sikes 10":                 "AMC Sikes Senter 10",
    "corpus 16":                "AMC Corpus Christi 16",
    "stonebriar mall 24":       "AMC Stonebriar Frisco 24 & IMAX",
    "firewheel town center 18": "AMC Firewheel Garland 18",
    "eastchase 9":              "AMC Eastchase Ft Worth 9",
    "willowbrook 24":           "AMC Willowbrook Houston 24",
    "brazos 14":                "AMC Brazos Stadium Lake Jackson 14",
    # ── Devan Tolbert (AMC SE / Louisiana / Carolinas) ───────────────────────
    "hanes 12":                 "AMC Hanes Winston Salem 12",
    # ── Brandon Ferguson (AMC LA / San Diego / Sacramento / SF Bay Area) ──────
    "orange 30":                "AMC Block Orange 30 & IMAX",
    "tyler 16":                 "AMC Tyler Riverside 16 & IMAX",
    "mercado 20":               "AMC Mercado Santa Clara 20 & IMAX",
    "metreon 16":               "AMC Metreon San Francisco 16 & IMAX",
    "eastridge 15":             "AMC Eastridge Mall San Jose 15 & IMAX",
    "saratoga 14":              "AMC Saratoga San Jose 14 & IMAX",
    # ── Kelsey Kash (AMC AL / TN / Chattanooga / Knoxville / Tri-Cities) ──────
    "dothan pavilion 12":       "AMC CLASSIC Dothan Pavillion 12",
    "vestavia 10":              "AMC DINE-IN Vestavia Hills 10",
    "battlefield 10":           "AMC CLASSIC Battlefield Ft Oglethorpe 10",
    "thoroughbred 20":          "AMC Thoroughbred Franklin 20 & IMAX",
    "stones river 9":           "AMC Stone River 9",
    # ── Blue Smiley / CFB Epic + GTC (Rentrak-grid fallback) ─────────────────
    "regency square 8":         "Epic Regency Cinema Stuart 8",
    "riverwatch":               "GTC Riverwatch Cinemas 12",
    "gateway 7":                "GTC Gateway 7",
    "island 7":                 "GTC Island 7",
    "smithfield cinemas 10":    "GTC Smithfield 10",
    "valdosta stadium 15 w/gtx":"GTC Valdosta 15",
    "pooler stadium 14 w/gtx":  "GTC Pooler 14",
    "mall 7":                   "GTC Mall Cinemas 7",
    "evans 14":                 "GTC Evans 14",
    "liberty 9":                "GTC Liberty 9",
    "mountain cinemas 8":       "GTC Mountain Cinemas 8",
    "university 16 cinemas w/gtx":"GTC University 16",
    "moultrie stadium 6 cinemas":"GTC Moultrie 6",
    "danville stadium 12":      "GTC Danville 12",
    # ── Cinemark Pacific NW (Josh Wymer) ─────────────────────────────────────
    "lincoln square cinema with imax":    "cinemark lincoln square cinemas imax 16",
    "lincoln square cinema bistro 6":     "cinemark reserve lincoln square dine-in 6",
    "cinemark totem lake + xd":           "cinemark village at totem lake 8",
    "century walla walla grand cinema 12":"cinemark walla walla grand cinema12",
    "century arden and xd, sacramento":   "Cinemark Century Arden + XD 14",
    "century arden and xd":               "Cinemark Century Arden + XD 14",
    "century marina + xd, marina":        "Cinemark Century Marina + XD 5",
    "century marina + xd":                "Cinemark Century Marina + XD 5",
    "cinemark 17, springfield":           "Cinemark Springfield 17",
    # ── Watson / Imagine Cinemas / CineStarz Canada ───────────────────────────
    "waikoloa 3":               "waikoloa village cinema 3",
    "promenade mall":           "imagine cinemas promenade 6",
    "london":                   "imagine cinemas london",
    "lakeshore windsor":        "imagine cinemas lakeshore",
    "alliston":                 "imagine cinemas alliston",
    "cote des nieges":          "cine starz cote-des-neiges 7",
    "cote-des-nieges":          "cine starz cote-des-neiges 7",
    "deluxe taschereau":        "cine starz taschereau 12",
    "deluxe longueuil":         "cinestarz longueuil 14",
    "burlington":               "cine starz burlington",
    "st. laurent":              "cine starz st laurent centre",
    "st laurent":               "cine starz st laurent centre",
    # ── Culbertson / IBS Indiana ─────────────────────────────────────────────
    "goshen":                   "linway plaza goshen 14",
    "jerseyville":              "the stadium theater 3",
    "litchfield":               "westside litchfield 3",
    "rensselaer":               "fountain stone theaters rensselaer 5",
    "nappanee theatre":         "nappanee theatre 1",
    "goshen, in":               "linway plaza goshen 14",
    "jerseyville, il":          "the stadium theater 3",
    "litchfield, il":           "westside litchfield 3",
    "rensselaer, in":           "fountain stone theaters rensselaer 5",
}


def _apply_city_alias(name: str, city: str = "") -> str:
    """Translate booking theatre name to actual venue name if known.
    Tries city-qualified key first ("name, city") for disambiguation.
    When city is "City, ST" (includes state), also tries just the city part."""
    if city:
        city_l = city.lower().strip()
        combined = f"{name.lower().strip()}, {city_l}"
        if combined in _CITY_VENUE_ALIASES:
            return _CITY_VENUE_ALIASES[combined]
        # Also try just the city name without the state abbreviation
        city_only = city_l.split(",")[0].strip()
        if city_only and city_only != city_l:
            combined2 = f"{name.lower().strip()}, {city_only}"
            if combined2 in _CITY_VENUE_ALIASES:
                return _CITY_VENUE_ALIASES[combined2]
    return _CITY_VENUE_ALIASES.get(name.lower().strip(), name)


# ---------------------------------------------------------------------------
# JS helper: find ONE table row by fuzzy theatre name match.
# Used by both _set_screening_type_per_row and _set_status_per_row.
# ---------------------------------------------------------------------------
_FIND_ONE_JS = """
    ({name, film}) => {
        const ABBREVS = {stm: 'stadium', ctr: 'center', blvd: 'boulevard'};
        // Chain/brand words that are too generic to count toward the match threshold
        const CHAIN_WORDS = new Set(['regal', 'amc', 'cinemark', 'harkins', 'marcus',
                                     'showcase', 'cineworld', 'amstar', 'imax']);
        function sigWords(n) {
            // Strip diacritics so accented names (e.g. "Española") match plain ASCII
            const plain = (n || '').normalize('NFD').replace(/[\\u0300-\\u036f]/g, '');
            return plain.replace(/([a-z])([A-Z])/g, '$1 $2').toLowerCase()
                .replace(/[&\\/\\#,+()$~%.'\"!?@*]/g, ' ')
                .split(/\\s+/).filter(w => w.length >= 3 && !/^\\d+$/.test(w))
                .map(w => ABBREVS[w] || w);
        }
        const tWords = sigWords(name);
        // Non-chain words are used for threshold — must contain at least a location word
        const tCore = tWords.filter(w => !CHAIN_WORDS.has(w));
        const effectiveWords = tCore.length > 0 ? tCore : tWords;
        const fWords = film ? sigWords(film) : [];
        if (tWords.length === 0) return {idx: -1, reason: 'no sig words'};
        const rows = Array.from(document.querySelectorAll('table tbody tr'));
        let bestIdx = -1, bestScore = 0;
        rows.forEach((row, i) => {
            if (!row.querySelector('[ngbdropdowntoggle]')) return;  // skip header/detail rows
            const text = row.textContent.toLowerCase();
            // Core (non-chain) word matches drive the threshold check
            const coreMatched = effectiveWords.filter(w => text.includes(w));
            // All word matches (incl. chain) drive ranking + longest-word tiebreaker
            const tMatched = tWords.filter(w => text.includes(w));
            const tLen = tMatched.length > 0 ? Math.max(...tMatched.map(w => w.length)) : 0;
            // Film words: tiebreaker bonus — used when same theatre has 2+ films
            const fCount = fWords.length > 0 ? fWords.filter(w => text.includes(w)).length : 0;
            const score = coreMatched.length * 100000 + tLen * 100 + fCount * 10;
            if (score > bestScore) { bestScore = score; bestIdx = i; }
        });
        const matchCount = Math.floor(bestScore / 100000);
        // Threshold: 50% of non-chain words must match (prevents generic regal+imax false positives)
        const threshold = Math.max(1, Math.ceil(effectiveWords.length * 0.5));
        if (bestIdx < 0 || matchCount < threshold) {
            const bestText = bestIdx >= 0 ? rows[bestIdx].textContent.trim().replace(/\\s+/g,' ').slice(0, 300) : 'no rows';
            return {idx: -1, reason: `score ${matchCount}/${effectiveWords.length} < threshold ${threshold} — best candidate: "${bestText}"`};
        }
        return {idx: bestIdx, rowText: rows[bestIdx].textContent.trim().slice(0, 70)};
    }
    """


def _set_screening_type_per_row(page, entries: list[dict], screening_type: str, contact: str = "") -> int:
    """
    Set screening type for specific rows using checkbox selection + Bulk Change.
    Checks the checkbox on each matching row, then applies Bulk Change → Screening Types.
    """
    # Clear any existing row selections before starting
    page.evaluate("""() => {
        document.querySelectorAll('table tbody tr input[type="checkbox"]:checked')
            .forEach(cb => cb.click());
    }""")
    page.wait_for_timeout(200)

    count = 0
    seen: set[tuple] = set()

    for entry in entries:
        name = entry["theatre"] if isinstance(entry, dict) else entry
        film = entry.get("film", "") if isinstance(entry, dict) else ""
        city = entry.get("city", "") if isinstance(entry, dict) else ""
        key  = (name, film, city)
        if key in seen:
            continue
        seen.add(key)

        lookup_name = _apply_city_alias(name, city)
        info = page.evaluate(_FIND_ONE_JS, {"name": lookup_name, "film": film})
        idx = info["idx"]
        if idx < 0:
            label = f"'{name}'" + (f" / '{film}'" if film else "")
            log(f"    NO MATCH  {label} ({info.get('reason', '')})")
            continue
        alias_note = f" [alias->{lookup_name}]" if lookup_name != name else ""
        label = f"'{name}'{alias_note}" + (f" / '{film}'" if film else "")
        log(f"    MATCH  {label} -> row {idx} -- {info.get('rowText','')[:60]}")

        # Check the checkbox for this row via JS
        checked = page.evaluate("""
            (idx) => {
                const rows = document.querySelectorAll('table tbody tr');
                if (idx >= rows.length) return false;
                const cb = rows[idx].querySelector('input[type="checkbox"]');
                if (!cb) return false;
                if (!cb.checked) cb.click();
                return true;
            }
        """, idx)
        if checked:
            count += 1
        else:
            log(f"    WARNING: Checkbox not found for '{name}'")

    if count == 0:
        return 0

    # Apply Bulk Change → Screening Types for all checked rows
    log(f"  Applying Bulk Change → {screening_type} for {count} checked rows ...")
    _bulk_set_screening_type(page, screening_type, contact="")

    # Uncheck all rows after bulk change
    page.evaluate("""() => {
        document.querySelectorAll('table tbody tr input[type="checkbox"]:checked')
            .forEach(cb => cb.click());
    }""")

    return count


def _set_status_per_row(page, entries: list[dict], status: str, contact: str = "") -> int:
    """
    Update status by clicking each matching row's individual status button.
    Finds each row fresh per iteration so table reordering after each click is handled.
    entries: list of {theatre, film} dicts — film is used as a tiebreaker when the
    same theatre has rows for two different films.
    contact: if provided, used to re-apply filter if the page navigates away mid-run.
    """
    count = 0
    seen: set[tuple] = set()

    for entry in entries:
        name = entry["theatre"] if isinstance(entry, dict) else entry
        film = entry.get("film", "") if isinstance(entry, dict) else ""
        city = entry.get("city", "") if isinstance(entry, dict) else ""
        key  = (name, film, city)  # include city so same-name theatres in diff cities aren't deduped
        if key in seen:
            continue
        seen.add(key)

        # Guard: if a prior status change redirected the browser, go back and re-filter
        _ensure_holdovers_page(page, contact)

        # Re-find the row fresh — accounts for table reordering after previous status changes
        lookup_name = _apply_city_alias(name, city)
        info = page.evaluate(_FIND_ONE_JS, {"name": lookup_name, "film": film})
        idx = info["idx"]
        if idx < 0:
            label = f"'{name}'" + (f" / '{film}'" if film else "")
            log(f"    NO MATCH  {label} ({info.get('reason', '')})")
            continue
        alias_note = f" [alias->{lookup_name}]" if lookup_name != name else ""
        label = f"'{name}'{alias_note}" + (f" / '{film}'" if film else "")
        log(f"    MATCH  {label} -> row {idx} -- {info.get('rowText','')[:60]}")

        row = page.locator("table tbody tr").nth(idx)
        try:
            row.scroll_into_view_if_needed(timeout=3_000)
            page.wait_for_timeout(500)
        except Exception:
            pass

        # Dismiss any lingering popup before clicking the status toggle
        _dismiss_any_dialog(page)

        # Force-remove any ngb-modal-window that intercepts pointer events.
        # The filter modal sometimes stays in the DOM after Save and blocks all clicks.
        page.evaluate("""() => {
            document.querySelectorAll('ngb-modal-window').forEach(m => {
                if (!m.textContent.includes('Edit Screenings')) m.remove();
            });
            document.querySelectorAll('.modal-backdrop').forEach(b => b.remove());
            document.body.classList.remove('modal-open');
        }""")
        page.wait_for_timeout(200)

        # Click the ng-bootstrap TOGGLE button — force=True bypasses overlay checks
        status_btn = row.locator('[ngbdropdowntoggle]').first
        _clicked = False
        try:
            page.evaluate("el => el.scrollIntoView({block:'center'})",
                          status_btn.element_handle())
            page.wait_for_timeout(300)
            status_btn.click(force=True, timeout=5_000)
            _clicked = True
            log(f"    Clicked status toggle for '{name}' (direct)")
        except Exception as _e1:
            log(f"    Direct click failed for '{name}': {_e1} — trying JS dispatch")
        if not _clicked:
            try:
                page.evaluate(
                    "el => el.dispatchEvent(new MouseEvent('click', {bubbles:true, cancelable:true}))",
                    status_btn.element_handle())
                _clicked = True
                log(f"    Clicked status toggle for '{name}' (JS dispatch)")
            except Exception as e:
                log(f"    WARNING: Could not click status toggle for '{name}': {e}")
                continue

        # Wait for dropdown to open (up to 3s) instead of blind sleep
        dropdown_opened = False
        try:
            page.wait_for_selector('.dropdown-menu.show', timeout=3_000)
            dropdown_opened = True
        except PlaywrightTimeout:
            log(f"    WARNING: Dropdown did not open for '{name}' — trying keyboard approach")
            # Last resort: keyboard Enter on the focused toggle
            try:
                page.keyboard.press("Enter")
                page.wait_for_selector('.dropdown-menu.show', timeout=2_000)
                dropdown_opened = True
                log(f"    Dropdown opened via keyboard for '{name}'")
            except PlaywrightTimeout:
                log(f"    WARNING: Dropdown still not open for '{name}' — skipping")
                page.keyboard.press("Escape")
                continue

        # Find option scoped to the open dropdown (.dropdown-menu.show)
        opt = page.locator(
            f'.dropdown-menu.show [ngbdropdownitem]:has-text("{status}"), '
            f'.dropdown-menu.show button:has-text("{status}")'
        ).first
        if opt.count() == 0:
            opt = page.locator(f'[ngbdropdownitem]:has-text("{status}")').first
        if opt.count() == 0:
            log(f"    WARNING: '{status}' option not found in open dropdown for '{name}'")
            page.keyboard.press("Escape")
            continue
        log(f"    Found '{status}' option — clicking")
        try:
            opt.click(timeout=3_000)
        except Exception as _e2:
            log(f"    WARNING: Option click failed for '{name}': {_e2}")
            page.keyboard.press("Escape")
            continue
        page.wait_for_timeout(500)

        # Confirmation dialog → Continue
        confirm = page.locator('button:has-text("Continue")').first
        if confirm.count() > 0:
            log(f"    Confirming status change for '{name}'")
            confirm.click()
            page.wait_for_timeout(400)
        else:
            log(f"    No confirmation dialog for '{name}' (may already be set or demo env)")

        # Dismiss the Numero error dialog (btn-close ✕) and any other benign popups.
        # _dismiss_any_dialog is scoped to [role="dialog"] so it cannot navigate away.
        _dismiss_any_dialog(page)

        # Wait for table to stabilise before finding the next row
        page.wait_for_timeout(1_500)
        count += 1
        log(f"    OK — status '{status}' set for '{name}' ({count} done so far)")

    return count


def _dismiss_error_popups(page):
    """
    Dismiss benign error/info popups that appear in the demo environment.
    Tries common close button patterns silently.
    """
    for text in ("OK", "Dismiss", "Close", "Got it"):
        try:
            btn = page.locator(f'[role="dialog"] button:has-text("{text}")').first
            if btn.count() > 0:
                btn.click()
                page.wait_for_timeout(300)
                return
        except Exception:
            pass


def _dismiss_any_dialog(page):
    """
    Close any open dialog/toast — including the benign Numero error popup whose
    close button is an ✕ (btn-close) rather than a labelled button.
    Never closes the Edit Screenings modal (that is handled intentionally elsewhere).
    """
    # Do not touch the Edit Screenings modal
    if page.locator('[role="dialog"]:has-text("Edit Screenings")').count() > 0:
        return

    for sel in [
        # [role="dialog"] modals (scoped — safe)
        '[role="dialog"] button.btn-close',
        '[role="dialog"] button[aria-label="Close"]',
        '[role="dialog"] button[aria-label="close"]',
        '[role="dialog"] button:has-text("OK")',
        '[role="dialog"] button:has-text("Dismiss")',
        '[role="dialog"] button:has-text("Close")',
        # Angular/Bootstrap toasts (Numero error popup) — scoped to toast containers
        'ngb-toast button.btn-close',
        '[class*="toast"] button.btn-close',
        '[class*="alert"] button.btn-close',
        '[class*="notification"] button.btn-close',
    ]:
        try:
            btn = page.locator(sel).first
            if btn.count() > 0:
                btn.click(timeout=800)
                page.wait_for_timeout(400)
                return
        except Exception:
            pass


def _ensure_holdovers_page(page, contact: str = ""):
    """
    If the script has navigated away from the Holdovers tab (e.g. a status-change
    confirmation link or toast redirected the browser), navigate back and re-apply
    the Contact filter so subsequent row operations work correctly.
    """
    mica_url  = _active_mica_url
    auth_file = _active_auth_file

    url = page.url.lower().rstrip('/')
    # Must be the holdovers LIST page — not a detail page (/holdovers/123)
    # or a completely different section.
    if url.endswith('/holdovers') or '/holdovers?' in url:
        return  # already on the list page

    log(f"  NOTE: Navigated away to '{page.url}' — returning to Holdovers ...")
    page.goto(mica_url, wait_until="domcontentloaded", timeout=60_000)
    # Wait for Angular SPA to settle — either the table or the login form
    try:
        page.wait_for_selector(
            'table, input[placeholder="Email"], input[type="password"]',
            timeout=15_000,
        )
    except PlaywrightTimeout:
        pass

    # Session may have expired mid-run — re-login if redirected to auth page
    if _on_auth_url(page.url) or page.locator('input[type="password"]').count() > 0:
        log("  Session expired mid-run — re-logging in ...")
        if auth_file.exists():
            auth_file.unlink()
        _auto_login(page)
        auth_file.parent.mkdir(exist_ok=True)
        page.context.storage_state(path=str(auth_file))
        log("  Session refreshed. Returning to Holdovers ...")
        page.goto(mica_url, wait_until="domcontentloaded", timeout=60_000)
        try:
            page.wait_for_selector('table', timeout=15_000)
        except PlaywrightTimeout:
            pass

    _dismiss_popups(page)
    page.wait_for_timeout(500)

    if contact:
        log("  Re-applying Contact filter after navigation ...")
        _apply_filters(page, contact)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Update Mica booking statuses")
    parser.add_argument("csv_file",  help="Path to booking CSV")
    parser.add_argument("--contact",     required=True, help='Contact/booker name in Mica (e.g. "Ashley Hensley")')
    parser.add_argument("--mode",        choices=["demo", "prod"], default="demo", help="demo or prod (default: demo)")
    parser.add_argument("--filter-type", dest="filter_type",
                        choices=["contact_person", "booker", "venue_group", "tv_market", "capabilities"],
                        default="contact_person", help="Which Mica filter dropdown to use (default: contact_person)")
    args = parser.parse_args()

    csv_path = Path(args.csv_file)
    if not csv_path.exists():
        log(f"ERROR: File not found: {csv_path}")
        sys.exit(1)

    log(f"Parsing booking: {csv_path.name}")
    theatres = parse_booking_csv(csv_path)

    if not theatres:
        log("INFO: No Hold or Final bookings found in this booking — nothing to update in Mica.")
        sys.exit(0)

    log(f"Found {len(theatres)} Hold/Final booking(s):")
    for t in theatres:
        st = f"-> {t['screening_type']}" if t["screening_type"] else ""
        log(f"  [{t['action']:5s}] {t['theatre']}  {st}")
    log("")

    run_mica_update(args.contact, theatres, mode=args.mode, filter_type=args.filter_type)


if __name__ == "__main__":
    main()
