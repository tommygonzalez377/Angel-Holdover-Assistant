#!/usr/bin/env python3
"""
Mica Sales Plan Booking Updater

Parses a booking sheet for "Open" rows, groups them by film title, then for
each film:
  1. Navigates to its Sales Plan (description "US, CA, PR")
  2. Filters venues by the specified Buyer/Booker
  3. Selects only the venues matching the booking sheet theatres
  4. Sets their status to "Agreed"
  5. For any venue whose opening date differs from the plan's default start
     date, clicks the Start Date cell → Edit Playweeks modal → updates the
     date → saves

Usage:
  python booking_plan_update.py --contact "Rich Motzer" --booking booking.txt
  python booking_plan_update.py --contact "Rich Motzer" --title "Animal Farm" --booking booking.txt
  # --title is optional; omit to process all Open films found in the sheet
"""

import argparse
import csv
import io
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

MICA_BASE_URLS = {
    "demo": "https://demo.mica.co",
    "prod": "https://app.mica.co",
}
OUTPUT_DIR = Path(__file__).parent / "output"

# Set by main() via --mode; used throughout the script
MICA_PLANS_URL = "https://demo.mica.co/plans"
MICA_LOGIN_URL = "https://demo.mica.co/auth/login"
AUTH_FILE      = OUTPUT_DIR / "mica_auth.json"

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


def log(msg: str):
    print(msg, flush=True)


# ---------------------------------------------------------------------------
# Booking sheet parsing — "Open" rows only
# ---------------------------------------------------------------------------

def _parse_action_date(action: str) -> str | None:
    """
    Extract a date from an action string like 'Open 02/06' or 'Open 05/08/2026'.
    Returns 'MM/DD' (two-digit month and day, no year) or None.
    """
    m = re.search(r"(\d{1,2})/(\d{1,2})", action)
    if not m:
        return None
    return f"{int(m.group(1)):02d}/{int(m.group(2)):02d}"


def _parse_one_per_line(raw: str) -> list[dict]:
    """Parse one-cell-per-line booking format (email copy-paste).
    Handles standard format (Action/Policy header), Cinemark __COLUMN__ format,
    and bare Cinemark format (where email clients strip the __ underscores).
    """
    _CINEMARK_BARE = {'DMA', 'SALES', '#', 'THEATRE', 'THEATER', 'SCR', 'SCREENS',
                      'CHAIN', 'CIRCUIT', 'BRCH', 'BRANCH'}
    _NAME_MAP = {'SALES': 'buyer', 'THEATRE': 'theatre', 'THEATER': 'theatre',
                 'SCR': 'screens', '#': 'unit', 'DMA': 'dma', 'BRCH': 'branch',
                 'BRANCH': 'branch', 'SCREENS': 'screens'}

    # Parse preserving space-only lines as empty cell values.
    # Blank separator lines (truly empty after strip) are skipped.
    cell_values = []
    for line in raw.splitlines():
        stripped = line.strip()
        if stripped:
            cell_values.append(stripped)
        elif len(line) > 0:   # space-only → blank cell
            cell_values.append('')
        # truly empty → separator, skip

    values = [v for v in cell_values if v]
    if not values:
        return []

    # ── Cinemark __COLUMN__ format (with underscores) ────────────────────────
    cinemark_hdr_idxs = [i for i, v in enumerate(values) if re.fullmatch(r'__.*__', v)]
    if cinemark_hdr_idxs:
        headers = []
        for i in cinemark_hdr_idxs:
            h = values[i].strip('_').strip().lower()
            headers.append(h if h else f"_col{len(headers)}")
        n_cols    = len(headers)
        data_vals = values[cinemark_hdr_idxs[-1] + 1:]
        rows = []
        for i in range(0, len(data_vals), n_cols):
            chunk = data_vals[i:i + n_cols]
            if len(chunk) < n_cols:
                chunk += [""] * (n_cols - len(chunk))
            rows.append(dict(zip(headers, chunk)))
        return rows

    # ── Cinemark DB export format (snake_case headers: theater_name / status) ─
    _SNAKE_KEYS_BP = {'dma_name', 'city', 'state', 'theater_name', 'theatre_name',
                      'title', 'status', 'account_name', 'circuit'}
    _SNAKE_MAP_BP  = {'theater_name': 'theatre', 'theatre_name': 'theatre',
                      'dma_name': 'dma', 'status': 'action', 'title': 'film',
                      'city': 'city', 'state': 'state', 'circuit': 'circuit',
                      'account_name': 'buyer'}
    _th_name_idx_bp = next((i for i, v in enumerate(values)
                            if v.lower() in ('theater_name', 'theatre_name')), None)
    if _th_name_idx_bp is not None:
        _ss_bp = _th_name_idx_bp
        while _ss_bp > 0 and values[_ss_bp - 1].lower() in _SNAKE_KEYS_BP:
            _ss_bp -= 1
        _snake_hdrs_bp = []
        _si_bp = _ss_bp
        while _si_bp < len(values) and values[_si_bp].lower() in _SNAKE_KEYS_BP:
            _snake_hdrs_bp.append(_SNAKE_MAP_BP.get(values[_si_bp].lower(), values[_si_bp].lower()))
            _si_bp += 1
        _n_sn_bp = len(_snake_hdrs_bp)
        _data_sn_bp = values[_si_bp:]
        rows = []
        for _sj in range(0, len(_data_sn_bp), _n_sn_bp):
            _chunk = list(_data_sn_bp[_sj : _sj + _n_sn_bp])
            if len(_chunk) < _n_sn_bp:
                _chunk += [''] * (_n_sn_bp - len(_chunk))
            _row = dict(zip(_snake_hdrs_bp, _chunk))
            _al = _row.get('action', '').lower()
            if 'final' in _al:
                _row['action'] = 'Final'
            elif 'hold' in _al:
                _row['action'] = 'Hold'
            rows.append(_row)
        return rows

    # ── Cinemark DMA / City / Theatre / Title / Print / Attributes / Status / Detail ─
    # Detected when the first 4 non-blank values are: DMA, City, Theatre, Title.
    # The CSV has a blank line between every individual value (cell separator), so
    # record boundaries are detected by matching DMA-pattern values in cell_values.
    if (len(values) >= 4
            and values[0].lower() == 'dma'
            and values[1].lower() == 'city'
            and values[2].lower() in ('theatre', 'theater')
            and values[3].lower() == 'title'):
        import re as _re_bp2
        _DMA_PAT_BP = _re_bp2.compile(r'.+ - .+|.+,\s*[A-Z]{2}$')
        _data_bp = cell_values[8:]
        _dma_pos_bp = [_i for _i, _v in enumerate(_data_bp) if _v and _DMA_PAT_BP.match(_v)]
        if not _dma_pos_bp and _data_bp:
            _dma0_bp = next((_v for _v in _data_bp if _v), '')
            _dma_pos_bp = [_i for _i, _v in enumerate(_data_bp) if _v == _dma0_bp]
        rows = []
        for _ri_b, _dp_b in enumerate(_dma_pos_bp):
            _rend_b = _dma_pos_bp[_ri_b + 1] if _ri_b + 1 < len(_dma_pos_bp) else len(_data_bp)
            _rv_b = _data_bp[_dp_b:_rend_b]
            _nb_b = [_v for _v in _rv_b if _v]
            if len(_nb_b) < 3:
                continue
            _dma_b, _city_b, _th_b = _nb_b[0], _nb_b[1], _nb_b[2]
            _nbc_b, _skip_b = 0, len(_rv_b)
            for _k_b, _cv_b in enumerate(_rv_b):
                if _cv_b:
                    _nbc_b += 1
                    if _nbc_b == 3:
                        _skip_b = _k_b + 1
                        break
            _fv_b = list(_rv_b[_skip_b:])
            while len(_fv_b) % 5:
                _fv_b.append('')
            for _fi_b in range(0, len(_fv_b), 5):
                _ttl_b, _, _, _sta_b, _dtl_b = _fv_b[_fi_b:_fi_b + 5]
                if not _ttl_b and not _sta_b:
                    continue
                rows.append({'theatre': _th_b, 'dma': _dma_b, 'film': _ttl_b,
                             'action': _sta_b, 'terms': _dtl_b})
        return rows

    # ── ComScore booking: Theatre # / ComScore Name / City / ST / Screens / DMA ──
    # Theatre # = Comscore unit#; unit-based master lookup. Strip parentheticals from name.
    if (len(values) >= 4
            and values[0].lower() in ('theatre #', 'theater #')
            and values[2].lower() == 'city'
            and values[3].lower() == 'st'):
        _hdrs_csc_b = ['unit', 'theatre', 'city', 'state', 'screens', 'dma', 'action']
        _data_csc_b = cell_values[6:]
        _id_pos_csc_b = [_i for _i, _v in enumerate(_data_csc_b)
                         if re.fullmatch(r'\d{3,}', _v)]
        rows = []
        for _idx_csc_b, _pos_csc_b in enumerate(_id_pos_csc_b):
            _end_csc_b = (_id_pos_csc_b[_idx_csc_b + 1] if _idx_csc_b + 1 < len(_id_pos_csc_b)
                          else len(_data_csc_b))
            _row_csc_b = list(_data_csc_b[_pos_csc_b:_end_csc_b])
            if len(_row_csc_b) < 7:
                _row_csc_b += [''] * (7 - len(_row_csc_b))
            _d_csc_b = dict(zip(_hdrs_csc_b, _row_csc_b[:7]))
            _th_csc_b = re.sub(r'\s*\([^)]*\)', '', _d_csc_b['theatre']).strip()
            rows.append({'unit': _d_csc_b['unit'], 'theatre': _th_csc_b,
                         'city': _d_csc_b['city'], 'action': _d_csc_b['action']})
        return rows

    # ── Landmark "Location" format: 2-column vertical (Theatre / Status) ────────
    # Data may be one-per-line alternating pairs OR tab/comma-separated rows.
    _loc_idx_bp = next((i for i, v in enumerate(values[:8]) if v.lower() == 'location'), None)
    if _loc_idx_bp is not None:
        _film_bp = values[0] if _loc_idx_bp > 0 else ''
        _data_bp_lm = values[_loc_idx_bp + 1:]
        rows = []
        # Detect inline separator (tab or comma embedded in values)
        _sep_bp = None
        for _sv_bp in _data_bp_lm[:4]:
            if '\t' in _sv_bp:
                _sep_bp = '\t'; break
            if ',' in _sv_bp:
                _sep_bp = ','; break
        def _lm_row_bp(th, st):
            _al = st.lower()
            if 'closed' in _al and 'no opening' in _al:
                return None
            _act = 'Final' if 'finished' in _al else 'Hold'
            return {'theatre': th, 'film': _film_bp, 'action': _act, 'phrase': st}
        if _sep_bp:
            for _entry_bp in _data_bp_lm:
                _parts_bp = _entry_bp.split(_sep_bp, 1)
                _r = _lm_row_bp(_parts_bp[0].strip(),
                                 _parts_bp[1].strip() if len(_parts_bp) > 1 else '')
                if _r:
                    rows.append(_r)
        else:
            for _fi_bp in range(0, len(_data_bp_lm), 2):
                _r = _lm_row_bp(_data_bp_lm[_fi_bp],
                                 _data_bp_lm[_fi_bp + 1] if _fi_bp + 1 < len(_data_bp_lm) else '')
                if _r:
                    rows.append(_r)
        return rows

    # ── Bare Cinemark format (no underscores — email clients strip __ markers) ─
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
        i = bare_start
        while i < len(cell_values):
            v = cell_values[i]
            if v.upper() in _CINEMARK_BARE or v == '#':
                headers.append(_NAME_MAP.get(v.upper(), v.lower()))
                blank_count = 0
                i += 1
            elif v == '' and blank_count < 2:
                headers.append('action' if blank_count == 0 else 'terms')
                blank_count += 1
                i += 1
            else:
                break
        # Always ensure action and terms columns exist
        if 'action' not in headers:
            headers.append('action')
        if 'terms' not in headers:
            headers.append('terms')
        n_cols    = len(headers)
        data_vals = cell_values[i:]

        # Find theatre column offset for row-boundary detection
        _th_col = next((h for h in headers if h in ('theatre', 'theater')), None)
        _th_off = headers.index(_th_col) if _th_col is not None else None
        _THEATRE_RE = re.compile(r'\([^)]*,\s*[A-Z]{2}\)', re.IGNORECASE)

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
                    row_end = len(data_vals)
                row_data = list(data_vals[row_start : row_end])
                if len(row_data) < n_cols:
                    row_data += [''] * (n_cols - len(row_data))
                rows.append(dict(zip(headers, row_data[:n_cols])))
            return rows
        else:
            # Blank separators present → fixed-length rows
            rows = []
            for j in range(0, len(data_vals), n_cols):
                chunk = data_vals[j:j + n_cols]
                if len(chunk) < n_cols:
                    chunk += [''] * (n_cols - len(chunk))
                rows.append(dict(zip(headers, chunk)))
            return rows

    # ── Small-exhibitor city+state format: "City, State   HOLD/Final" ──────────
    # e.g. "Ark City, KS       HOLD"  or  "Florence, SC        Final"
    _CS_RE_BP = re.compile(r'^(.*\S)\s+(HOLD|FINAL|OPEN|CONFIRMED)\s*$', re.IGNORECASE)
    _SS_RE_BP = re.compile(r'^(.*?),?\s*([A-Z]{2})\s*$')
    _nonempty_bp = [l.strip() for l in raw.splitlines() if l.strip()]
    if _nonempty_bp:
        _cs_hits_bp = sum(1 for _l in _nonempty_bp if _CS_RE_BP.match(_l))
        if _cs_hits_bp / len(_nonempty_bp) >= 0.70:
            rows = []
            for _line in _nonempty_bp:
                _cm = _CS_RE_BP.match(_line)
                if not _cm:
                    continue
                _loc    = _cm.group(1).strip()
                _stat   = _cm.group(2).strip()
                _action = 'Final' if 'final' in _stat.lower() else 'Hold'
                rows.append({'theatre': _loc, 'action': _action})  # "City, ST" gives 2 match words
            return rows

    # ── Standard Action/Policy format ────────────────────────────────────────
    action_idx = next(
        (i for i, v in enumerate(values) if v.lower() in ("action", "policy")), None
    )
    if action_idx is None:
        return []
    KNOWN = {"buyer","br","unit","theatre","theater","attraction","film",
             "title","type","media","prt","comscore","comscore #","#"}
    header_start = next(
        (i for i in range(action_idx + 1) if values[i].lower() in KNOWN), 0
    )
    headers   = values[header_start:action_idx + 1]
    n_cols    = len(headers)
    remainder = values[action_idx + 1:]
    if not remainder:
        return []
    if any(p in headers[0].lower() for p in ("unit", "comscore", "#")):
        id_pos = [i for i, v in enumerate(remainder) if re.fullmatch(r"\d{3,}", v)]
        if id_pos:
            rows = []
            for idx, pos in enumerate(id_pos):
                end = id_pos[idx + 1] if idx + 1 < len(id_pos) else len(remainder)
                row = list(remainder[pos:end])
                if len(row) < n_cols:
                    row += [""] * (n_cols - len(row))
                rows.append(dict(zip(headers, row[:n_cols])))
            return rows
    rows = []
    for i in range(0, len(remainder), n_cols):
        chunk = remainder[i:i + n_cols]
        if len(chunk) < n_cols:
            chunk += [""] * (n_cols - len(chunk))
        rows.append(dict(zip(headers, chunk)))
    return rows


def _parse_delimited(text: str, delim: str) -> list[dict]:
    """Parse tab/comma delimited booking format."""
    import re as _re_pd
    lines = text.splitlines()
    HEADER_KEYS = {"theatre","theater","buyer","film","attraction","action","unit","policy"}
    header_idx = 0
    for i, line in enumerate(lines[:5]):
        # Word-token split so "dma" inside "landmark" doesn't fire
        _words = set(_re_pd.split(r'[\s,\t]+', line.lower().strip()))
        if _words & HEADER_KEYS:
            header_idx = i
            break
    content = "\n".join(lines[header_idx:])
    reader  = csv.DictReader(io.StringIO(content), delimiter=delim)
    return list(reader)


def _preamble_film_title(text: str) -> str:
    """
    Extract the film title from lines that appear BEFORE the first column header.
    Works for Cinemark-style exports that put 'Film Name' before __DMA__ etc.
    Returns the last non-empty preamble line (skipping generic labels like 'David').
    """
    import re as _re
    _CINEMARK_BARE = {'DMA', 'SALES', '#', 'THEATRE', 'THEATER', 'SCR', 'SCREENS',
                      'BRCH', 'BRANCH', 'CHAIN', 'CIRCUIT'}
    # Collect preamble lines (before first column header, dunder or bare)
    preamble = []
    stripped_lines = [l.strip() for l in text.splitlines()]
    for stripped in stripped_lines:
        if _re.fullmatch(r'__.*__', stripped):
            break  # reached dunder header section
        if stripped.upper() in _CINEMARK_BARE:
            break  # reached bare Cinemark header section
        if stripped:
            preamble.append(stripped)
    # The film title is usually the last meaningful preamble line (not a person's name)
    # If there are 2+ lines, take the second one (first is often an account name)
    if len(preamble) >= 2:
        return preamble[1]
    if preamble:
        return preamble[0]
    return ""


def _is_active_action(action: str) -> bool:
    """
    Return True if the booking row should be treated as active/open.
    Accepts: blank (Cinemark format), "Open ...", "Final" (Cinemark confirmed).
    Rejects: Cancelled, Hold, Declined, etc.
    """
    al = action.strip().lower()
    if not al:
        return True          # blank = active in Cinemark format
    if "open" in al:
        return True
    if "final" in al:
        return True
    if "confirm" in al:
        return True
    if "tentative" in al:
        return True
    return False             # Cancelled, Hold, Declined, etc.


def _parse_email_booking(text: str) -> dict[str, list[dict]]:
    """
    Parse informal email-style booking where the film title appears as
    'on FILM TITLE:' or 'for FILM TITLE:' and theatres are listed one per line
    after the mention, before any paragraph text.

    Example:
        I've worked out the best grossing situations for you on ANIMAL FARM:

        CineLux - Watsonville
        CineLux - Scotts Valley
        CineLux - Morgan Hill

        We will be running our normal Full Schedule...
    """
    _PROSE_STARTS = {
        'i', "i've", "i'll", 'we', 'none', 'please', 'all', 'kindest', 'thank',
        'the', 'this', 'our', 'your', 'they', 'it', 'if', 'as', 'my', 'no',
        'not', 'any', 'and', 'but', 'so', 'in', 'at', 'best', 'will', 'would',
        'can', 'could', 'should', 'may', 'might', 'also', 'with', 'from',
        'that', 'there', 'here', 'these', 'those', 'sincerely', 'regards',
        'hi', 'hello', 'dear', 'hey', 'attached', 'see', 'per', 'as',
    }

    # Match "on FILM TITLE:" or "for FILM TITLE:"
    m = re.search(
        r'\b(?:on|for)\s+([A-Z][A-Za-z0-9 \'\-\(\)&\.]+?)\s*:',
        text,
        re.MULTILINE,
    )
    if not m:
        return {}

    film = m.group(1).strip()
    remaining = text[m.end():]

    theatres = []
    in_block = False
    for line in remaining.splitlines():
        stripped = line.strip()
        if not stripped:
            if in_block:
                break   # blank line ends the theatre block
            continue

        first_word = stripped.split()[0].lower().rstrip('.,;:')
        if first_word in _PROSE_STARTS:
            if in_block:
                break
            continue
        if len(stripped) > 70:   # paragraph text
            if in_block:
                break
            continue

        in_block = True
        theatres.append(stripped)

    if not theatres:
        return {}

    log(f"  [email format] Film: '{film}', {len(theatres)} theatre(s): {theatres}")
    return {film: [{"theatre": t, "date": None} for t in theatres]}


def _parse_caribbean_booking(text: str) -> dict[str, list[dict]]:
    """
    Parse Caribbean/Puerto Rico booking format (Caribbean Cinemas).

    Structure:
        APR 30'26 WK 18             (date header)
            animal farm  animal farm (film names — tab-indented)
                         spa         (version sub-header)
            angel        angel
        El Distrito    1            (theatre + booking cols)
        Guaynabo       combo
        Rio Hondo            1
        Metro                       (blank = not playing)
        Theater Count  3            (section footer — skip)

    A theatre is booked if ANY column value is "1" or "combo".
    Blank = not playing.
    """
    full_lower = text.lower()
    if 'combo' not in full_lower and not re.search(r'\bspa\b|\bov\b', full_lower):
        return {}

    # Extract play date from header (e.g. "APR 30'26 WK 18" → "04/30")
    _month_map = {'jan':'01','feb':'02','mar':'03','apr':'04','may':'05','jun':'06',
                  'jul':'07','aug':'08','sep':'09','oct':'10','nov':'11','dec':'12'}
    play_date = None
    for line in text.splitlines()[:8]:
        m = re.search(r'\b(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)\s+(\d{1,2})', line, re.I)
        if m:
            play_date = f"{_month_map[m.group(1).lower()]}/{int(m.group(2)):02d}"
            break

    # Extract film title from tab-indented header rows
    _skip_words = {'spa', 'ov', 'angel', 'wk', ''}
    film = 'Unknown'
    for line in text.splitlines()[:15]:
        if not line.startswith('\t'):
            continue
        parts = [p.strip().lower() for p in line.split('\t') if p.strip()]
        for p in parts:
            if p and p not in _skip_words and not re.match(r'^\d|^wk', p) and len(p) > 2:
                film = p.title()
                break
        if film != 'Unknown':
            break

    # Parse theatre rows — lines that start with a letter (not tab-indented)
    results: dict[str, list[dict]] = {}
    _skip_theatre = re.compile(
        r'^(theater\s*count|theatre\s*count|spa|ov|angel|combo|\d)', re.I
    )
    for line in text.splitlines():
        if not line or line[0] in (' ', '\t'):
            continue
        parts = line.split('\t')
        theatre = parts[0].strip()
        if not theatre or not re.search(r'[a-zA-Z]', theatre):
            continue
        if _skip_theatre.match(theatre):
            continue
        values = [p.strip().lower() for p in parts[1:]]
        if any(v in ('1', 'combo') for v in values):
            results.setdefault(film, []).append({'theatre': theatre, 'date': play_date})

    if results:
        for f, rows in results.items():
            log(f"  [Caribbean format] Film: '{f}', {len(rows)} theatre(s): "
                f"{[r['theatre'] for r in rows]}")
    return results


def _parse_amc_booking(text: str) -> dict[str, list[dict]]:
    """
    Parse AMC Theatres booking email format.

    Each theatre row contains an anchor like:
        Albany 16  0  Opening - 04/30/2026  1  1 total ...
    or with a leading DMA name:
        ALBANY, GA  Albany 16  0  Opening - 04/30/2026  ...
    or with both film title + DMA:
        Animal Farm  ALBANY, GA  Albany 16  0  Opening - 04/30/2026  ...

    Detection: first ~400 chars contain "AMC Film Programmer" or
               a column header with "Theatre Name" + "Change Type".

    Returns { film_title: [{"theatre": str, "date": "MM/DD"}, ...] }
    """
    header_sample = '\n'.join(text.splitlines()[:15])
    _amc_opening_pat = re.compile(r'\b\d+\s+(?:Split\s+screen\.\s+)?(?:Opening\s*[-–]\s*\d{1,2}/\d{1,2}/\d{4}|Holdover)\b')
    is_amc = (
        'AMC Film Programmer' in header_sample
        or ('Theatre Name' in header_sample and 'Change Type' in header_sample)
        or bool(_amc_opening_pat.search(text))
    )
    if not is_amc:
        return {}

    # Extract "Film Week: MM/DD/YYYY" date as fallback for Holdover rows (no explicit date)
    _fw_m = re.search(r'Film\s+Week[:\s]+(\d{1,2}/\d{1,2}/\d{4})', text)
    _fw_dm = re.search(r'(\d{1,2})/(\d{1,2})', _fw_m.group(1)) if _fw_m else None
    film_week_date = (f"{int(_fw_dm.group(1)):02d}/{int(_fw_dm.group(2)):02d}"
                      if _fw_dm else None)

    # DMA pattern: 3+ consecutive ALL-CAPS chars per word (e.g. "ALBANY", "DALLAS-FORT"),
    # optional parenthetical (e.g. "(LAS CRUCES)") and ", ST" suffix,
    # followed by a mixed-case theatre name word.
    _DMA_RE = re.compile(
        r'\b([A-Z]{3,}[A-Z0-9\-&\/]*(?:\s+[A-Z]{2,}[A-Z0-9\-&\/]*)*)'
        r'(?:\s*\([^)]*\))?'    # optional parenthetical
        r'(?:,\s*[A-Z]{2})?'   # optional ", ST"
        r'\s+(?=[A-Z][a-z])'   # followed by Mixed-Case word (theatre name)
    )

    results: dict[str, list[dict]] = {}
    current_film = ''

    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue

        # Anchor: gross + Opening (with date) OR Holdover (date from film week header)
        m = re.search(
            r'\b[\d,]+\s+(?:Split\s+screen\.\s+)?(?:Opening\s*[-–]\s*(\d{1,2}/\d{1,2}/\d{4})|Holdover)\b',
            line)
        if not m:
            continue

        before = line[:m.start()].strip()
        date_raw = m.group(1)  # None for Holdover rows
        if date_raw:
            dm = re.search(r'(\d{1,2})/(\d{1,2})', date_raw)
            action_date = f"{int(dm.group(1)):02d}/{int(dm.group(2)):02d}" if dm else film_week_date
        else:
            action_date = film_week_date  # Holdover: use film week date

        # Find DMA in "before": use LAST match so distributor (ANGEL STUDIOS INC)
        # doesn't shadow the real DMA (ALBANY, DALLAS-FORT WORTH, etc.).
        all_dma_m = list(_DMA_RE.finditer(before))
        dma_m = all_dma_m[-1] if all_dma_m else None
        if dma_m:
            film_part  = before[:dma_m.start()].strip()
            theatre    = before[dma_m.end():].strip()
            # Strip leading ALL-CAPS distributor prefix from film_part
            # e.g. "ANGEL STUDIOS INC  Animal Farm" → "Animal Farm"
            film_part = re.sub(r'^(?:[A-Z][A-Z\s]*[A-Z])\s+(?=[A-Z][a-z])', '', film_part).strip()
            if film_part and re.search(r'[a-z]', film_part):
                clean = re.sub(r'\s*[-–]\s*(2D|3D|OC|IMAX|XD|Combo|Dub:|Sub:|Dubbed|Subtitled).*',
                               '', film_part, flags=re.I).strip()
                if clean:
                    current_film = clean
        else:
            # No DMA on this line — entire "before" is the theatre name (possibly + film title)
            theatre = before

        # Handle format where film title comes AFTER the theatre number:
        #   "Albany 16 Animal Farm" or "Barton Creek 14 Animal Farm - 2D/OC"
        # Split on the screen-count number: everything up to + including the
        # first number is the theatre; anything after is the film title.
        th_film_m = re.match(r'^(.+?\b\d+)\s+([A-Z][A-Za-z].+)$', theatre)
        if th_film_m:
            film_suffix = th_film_m.group(2).strip()
            if re.search(r'[a-z]', film_suffix):   # has lowercase → likely a film title
                clean = re.sub(r'\s*[-–]\s*(2D|3D|OC|IMAX|XD|Combo|Dub:|Sub:|Dubbed|Subtitled).*',
                               '', film_suffix, flags=re.I).strip()
                if clean:
                    current_film = clean
                theatre = th_film_m.group(1).strip()

        if not theatre or not re.search(r'\d', theatre):
            continue   # theatre name should contain a screen-count number

        film = current_film or 'Unknown'
        results.setdefault(film, []).append({'theatre': theatre, 'date': action_date})

    if results:
        for film, rows in results.items():
            log(f"  [AMC format] Film: '{film}', {len(rows)} theatre(s): "
                f"{[r['theatre'] for r in rows]}")
    return results


# ── Glen Parham / GTC "Circuit + Theatre Name" format ───────────────────────
# Shared state abbreviation map for city+state venue lookup
_GP_STATE_FULL_TO_ABBREV: dict[str, str] = {
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
    'wisconsin': 'wi', 'wyoming': 'wy', 'district of columbia': 'dc', 'puerto rico': 'pr',
}
_GP_CITY_CORRECTIONS: dict[str, str] = {
    "fort benning": "fort benning south  (historical)",
}
_GP_CITY_STATE_LOOKUP: dict[tuple[str, str], list[str]] = {}

def _load_gp_city_state_lookup() -> dict[tuple[str, str], list[str]]:
    global _GP_CITY_STATE_LOOKUP
    if _GP_CITY_STATE_LOOKUP:
        return _GP_CITY_STATE_LOOKUP
    master_path = Path(__file__).parent / "master_list_cache.csv"
    if not master_path.exists():
        return _GP_CITY_STATE_LOOKUP
    with open(master_path, newline="", encoding="utf-8-sig") as _f:
        for _row in csv.DictReader(_f):
            _vn    = _row.get("Venue", "").strip()
            _city  = _row.get("City",  "").strip().lower()
            _state = _GP_STATE_FULL_TO_ABBREV.get(_row.get("State", "").strip().lower(),
                                                   _row.get("State", "").strip().lower()[:2])
            if _vn and _city:
                _GP_CITY_STATE_LOOKUP.setdefault((_city, _state), []).append(_vn)
    log(f"  [gp-city-lookup] loaded {len(_GP_CITY_STATE_LOOKUP)} city+state keys")
    return _GP_CITY_STATE_LOOKUP

_GP_STRIP_RE = re.compile(
    r'\bw/gtx\b|\bwith pdx\b|\bwith gtx\b|\bplf\b|\bstadium\b|\bcinemas?\b'
    r'|\bcineplex\b|\bw/\w+\b', re.I
)

def _gp_fuzzy_match(name: str, candidates: list[str], cutoff: float = 0.35) -> str:
    import difflib as _dl
    def _norm(s):
        return re.sub(r'\s+', ' ', _GP_STRIP_RE.sub(' ', s.lower())).strip()
    _nm = _norm(name)
    _best, _best_r = '', 0.0
    for _c in candidates:
        _r = _dl.SequenceMatcher(None, _nm, _norm(_c)).ratio()
        if _r > _best_r:
            _best_r, _best = _r, _c
    return _best if _best_r >= cutoff else ''


def _parse_glen_parham_booking(text: str) -> dict[str, list[dict]]:
    """
    Parse Glen Parham / GTC format:
      Circuit | Theatre Name | City | ST | Title | DIST | Playwk | Status | WK# | FSS
    Returns opening rows (Status starts with "New") as {film: [{"theatre", "date"}]}.
    """
    lines = [l for l in text.splitlines() if l.strip()]
    if not lines:
        return {}
    _hdrs = [h.strip().lower() for h in lines[0].split('\t')]
    if not ('\t' in lines[0] and 'circuit' in _hdrs
            and 'theatre name' in _hdrs and 'title' in _hdrs and 'status' in _hdrs):
        return {}
    _idx = {h: i for i, h in enumerate(_hdrs)}
    _ci_thtr  = _idx.get('theatre name', -1)
    _ci_city  = _idx.get('city', -1)
    _ci_st    = _idx.get('st', -1)
    _ci_film  = _idx.get('title', -1)
    _ci_stat  = _idx.get('status', -1)
    _ci_playwk = _idx.get('playwk', -1)
    _cs_lkp   = _load_gp_city_state_lookup()
    results: dict[str, list[dict]] = {}
    for _dl in lines[1:]:
        _cells = [c.strip() for c in _dl.split('\t')]
        def _gc(i): return _cells[i] if 0 <= i < len(_cells) else ""
        _stat = _gc(_ci_stat).lower()
        if not _stat.startswith('new'):
            continue
        _thtr = _gc(_ci_thtr)
        _city = _gc(_ci_city)
        _st   = _gc(_ci_st).lower()
        _film = _gc(_ci_film)
        _playwk = _gc(_ci_playwk)  # "MM/DD/YYYY" → extract "MM/DD"
        if not _thtr or not _film:
            continue
        _date_m = re.match(r'(\d{1,2}/\d{1,2})', _playwk)
        _date = _date_m.group(1) if _date_m else None
        _city_key = _GP_CITY_CORRECTIONS.get(_city.lower(), _city.lower())
        _cands = _cs_lkp.get((_city_key, _st), [])
        _matched = _gp_fuzzy_match(_thtr, _cands) if _cands else ''
        _venue = _matched or _thtr
        if not _matched:
            log(f"  [glen-parham-open] no master match for '{_thtr}' ({_city}, {_st.upper()}) — using raw name")
        results.setdefault(_film, []).append({"theatre": _venue, "date": _date})
    if results:
        for film, rows in results.items():
            log(f"  [glen-parham-open] Film: '{film}', {len(rows)} theatre(s): "
                f"{[r['theatre'] for r in rows]}")
    return results


def _parse_diane_johnson_booking(text: str) -> dict[str, list[dict]]:
    """
    Parse Diane Johnson circuit-grid format:
      CIRCUIT | THEATRE | CITY | STATE | [Film - M/D] ...
    Returns opening rows (action contains 'open') as {film: [{"theatre", "date"}]}.
    Date comes from the column header (' - M/D' suffix), not from the action cell.
    """
    lines = [l for l in text.splitlines() if l.strip()]
    if not lines:
        return {}
    _raw_hdrs = [c.strip() for c in lines[0].split('\t')]
    _hdrs     = [h.lower() for h in _raw_hdrs]
    if not ('\t' in lines[0] and 'circuit' in _hdrs and 'theatre' in _hdrs
            and 'theatre name' not in _hdrs
            and any(re.search(r'-\s*\d{1,2}/\d{1,2}', h) for h in _hdrs)):
        return {}
    _film_cols = [i for i, h in enumerate(_hdrs) if re.search(r'-\s*\d{1,2}/\d{1,2}', h)]
    _film_names = [re.sub(r'\s*-\s*\d{1,2}/\d{1,2}.*', '', _raw_hdrs[i]).strip()
                   for i in _film_cols]
    _film_dates = []
    for i in _film_cols:
        m = re.search(r'(\d{1,2}/\d{1,2})', _raw_hdrs[i])
        _film_dates.append(m.group(1) if m else None)
    _ci_circuit = _hdrs.index('circuit') if 'circuit' in _hdrs else -1
    _ci_theatre = _hdrs.index('theatre') if 'theatre' in _hdrs else -1
    _ci_city    = _hdrs.index('city')    if 'city'    in _hdrs else -1
    _ci_state   = next((i for i, h in enumerate(_hdrs) if h in ('state', 'st')), -1)
    _cs_lkp     = _load_gp_city_state_lookup()
    results: dict[str, list[dict]] = {}
    for _dl in lines[1:]:
        _cells = [c.strip() for c in _dl.split('\t')]
        def _g(i): return _cells[i] if 0 <= i < len(_cells) else ""
        _circuit = _g(_ci_circuit)
        _thtr    = _g(_ci_theatre)
        _city    = _g(_ci_city)
        _st      = _g(_ci_state).lower()[:2]
        if not _thtr:
            continue
        _city_key = _GP_CITY_CORRECTIONS.get(_city.lower(), _city.lower())
        _cands    = _cs_lkp.get((_city_key, _st), [])
        _matched  = _gp_fuzzy_match(_thtr, _cands) if _cands else ''
        if not _matched and _circuit:
            _matched = _gp_fuzzy_match(f"{_circuit} {_thtr}", _cands) if _cands else ''
        _venue = _matched or (f"{_circuit} {_thtr}".strip() if _circuit else _thtr)
        for _fi, _ci in enumerate(_film_cols):
            _val = _g(_ci)
            if 'open' not in _val.lower():
                continue
            _film = _film_names[_fi]
            _date = _film_dates[_fi]
            results.setdefault(_film, []).append({"theatre": _venue, "date": _date})
    if results:
        for film, rows in results.items():
            log(f"  [diane-j-open] Film: '{film}', {len(rows)} theatre(s): "
                f"{[r['theatre'] for r in rows]}")
    return results


def parse_open_bookings(text: str) -> dict[str, list[dict]]:
    """
    Parse booking text and return:
      { film_title: [ {"theatre": str, "date": str|None}, ... ] }
    where "date" is "MM/DD" extracted from the action (e.g. "Open 05/08" → "05/08"),
    or None if no date was found.
    Accepts rows whose Action contains "open", "final", or is blank (Cinemark format).
    """
    if not text or not text.strip():
        return {}

    # ── Diane Johnson circuit-grid format: early detection ───────────────────
    _dj_result = _parse_diane_johnson_booking(text)
    if _dj_result:
        return _dj_result
    # ── Glen Parham / GTC format: early detection before generic parsers ──────
    _gp_result = _parse_glen_parham_booking(text)
    if _gp_result:
        return _gp_result
    # ──────────────────────────────────────────────────────────────────────────

    lines    = [l for l in text.splitlines() if l.strip()]
    max_tabs  = max((l.count("\t") for l in lines[:5]), default=0)
    max_commas = max((l.count(",") for l in lines[:5]), default=0)
    _is_csc_hdr = any(l.strip().lower() in ('theatre #', 'theater #') for l in lines[:4])

    if (max_tabs < 2 and max_commas < 2) or _is_csc_hdr:
        raw_rows = _parse_one_per_line(text)
    else:
        delim    = "\t" if max_tabs > max_commas else ","
        raw_rows = _parse_delimited(text, delim)

    # Try to extract film title from preamble (Cinemark format)
    preamble_film = _preamble_film_title(text)

    _skip = {"buyer","br","unit","attraction","film","title","type","media",
             "prt","action","policy","status","phrase","comscore #","comscore","#"}
    results: dict[str, list[dict]] = {}

    for row in raw_rows:
        fl     = {k.lower().strip(): (v.strip() if v else "") for k, v in row.items()}
        action = fl.get("action") or fl.get("policy") or ""
        if not _is_active_action(action):
            continue

        theatre = fl.get("theatre") or fl.get("theater") or ""
        film    = (fl.get("attraction") or fl.get("film") or fl.get("title") or "")

        # ComScore format: theatre under the film-title column (unknown key)
        if not theatre.strip():
            for k, v in fl.items():
                if k not in _skip and v and not v.strip().isdigit():
                    theatre = v
                    break

        if not theatre.strip():
            continue

        date = _parse_action_date(action)   # "MM/DD" or None
        film = film.strip() or preamble_film or "Unknown"
        results.setdefault(film, []).append({"theatre": theatre.strip(), "date": date})

    # Fallback: try Caribbean/Puerto Rico booking format
    if not results:
        results = _parse_caribbean_booking(text)

    # Fallback: try AMC booking format
    if not results:
        results = _parse_amc_booking(text)

    # Fallback: try email-style booking format if nothing was found
    if not results:
        results = _parse_email_booking(text)

    return results


def parse_bare_theatre_list(text: str, film: str) -> list[dict]:
    """
    Parse a plain list of theatre names (one per line) when no booking format
    is detected. Used when the user pastes just the theatre names and fills in
    the film title separately in the Title field.

    Skips blank lines, very long lines (paragraph text), and common prose words.
    Returns [{"theatre": str, "date": None}, ...]
    """
    _SKIP_STARTS = {
        'i', 'we', 'please', 'all', 'the', 'this', 'our', 'your', 'none',
        'kindest', 'thank', 'regards', 'sincerely', 'hi', 'hello', 'dear',
        'no', 'not', 'any', 'and', 'but', 'so', 'will', 'would', 'can',
    }
    theatres = []
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped or len(stripped) > 80:
            continue
        first = stripped.split()[0].lower().rstrip('.,;:')
        if first in _SKIP_STARTS:
            continue
        theatres.append(stripped)
    return [{"theatre": t, "date": None} for t in theatres]


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def _navigate_to_plans(page, ctx):
    """Navigate to Mica Sales → Plans, handling login and Angular lazy-loading."""
    log("Navigating to Sales → Plans ...")
    base_url = MICA_PLANS_URL.replace("/plans", "")
    page.goto(f"{base_url}/", wait_until="domcontentloaded", timeout=30_000)
    try:
        page.wait_for_selector(
            'nav, input[placeholder="Email"], input[type="password"]',
            timeout=15_000,
        )
    except PlaywrightTimeout:
        pass

    if _is_login_page(page):
        _do_login(page, ctx)
        try:
            page.wait_for_selector('nav, [class*="navbar"], [class*="sidebar"]', timeout=15_000)
        except PlaywrightTimeout:
            log(f"  WARNING: Nav not found after login (url={page.url})")
        log(f"  Post-login URL: {page.url}")

    page.wait_for_timeout(2_000)
    _screenshot(page, "bp_after_root_load.png")

    nav_items = page.evaluate("""
    () => {
        const els = Array.from(document.querySelectorAll('a, button, [routerlink]'));
        return els
            .filter(el => el.textContent.trim().length > 0 && el.textContent.trim().length < 60)
            .map(el => el.textContent.trim().replace(/\\s+/g, ' ').substring(0, 50))
            .filter((v, i, a) => a.indexOf(v) === i)
            .slice(0, 40);
    }
    """)
    log(f"Page links/buttons: {nav_items}")

    plans_loaded = False
    log("Clicking Sales nav item ...")
    sales_count = page.locator(
        'a:has-text("Sales"), button:has-text("Sales"), [routerlink*="sales"]'
    ).count()
    log(f"Sales locator count: {sales_count}")

    if sales_count > 0:
        page.locator(
            'a:has-text("Sales"), button:has-text("Sales"), [routerlink*="sales"]'
        ).first.click()
        try:
            page.wait_for_url(lambda u: "sales" in u.lower(), timeout=8_000)
        except PlaywrightTimeout:
            pass
        log(f"URL after clicking Sales: {page.url}")
        page.wait_for_timeout(1_500)

        after_sales = page.evaluate("""
        () => {
            const els = Array.from(document.querySelectorAll(
                'a, button, li, .dropdown-item, [routerlink], .nav-link, .tab'
            ));
            return els
                .filter(el => {
                    const t = el.textContent.trim();
                    return t.length > 0 && t.length < 60;
                })
                .map(el => el.textContent.trim().replace(/\\s+/g, ' ').substring(0, 50))
                .filter((v, i, a) => a.indexOf(v) === i)
                .slice(0, 50);
        }
        """)
        log(f"After clicking Sales: {after_sales}")

        plans_count = page.locator(
            'a:has-text("Plans"), [routerlink*="plans"], '
            '.nav-link:has-text("Plans"), .tab:has-text("Plans"), '
            'button:has-text("Plans")'
        ).count()
        log(f"Plans locator count: {plans_count}")
        if plans_count > 0:
            page.locator(
                'a:has-text("Plans"), [routerlink*="plans"], '
                '.nav-link:has-text("Plans"), .tab:has-text("Plans"), '
                'button:has-text("Plans")'
            ).first.click()
            try:
                page.wait_for_selector("table", timeout=15_000)
                plans_loaded = True
                log(f"Plans tab clicked — URL: {page.url}")
            except PlaywrightTimeout:
                log(f"Table not found after Plans click (url={page.url})")

        if not plans_loaded:
            log("Plans tab click failed — trying direct goto ...")
            page.goto(MICA_PLANS_URL, wait_until="domcontentloaded", timeout=30_000)
            page.wait_for_timeout(2_000)
            log(f"URL after goto: {page.url}")

    if not plans_loaded and sales_count == 0:
        log("Sales nav not found — trying direct goto ...")
        page.goto(MICA_PLANS_URL, wait_until="domcontentloaded", timeout=30_000)
        page.wait_for_timeout(2_000)
        log(f"URL after goto: {page.url}")

    _dismiss_popups(page)

    try:
        page.wait_for_selector("table", timeout=10_000)
    except PlaywrightTimeout:
        _screenshot(page, "bp_plans_load_failed.png")
        log(f"ERROR: Plans page did not load (url={page.url})")
        log("  Screenshot saved to output/bp_plans_load_failed.png")
        sys.exit(1)


def _run_films_in_browser(page, ctx, films_theatres: dict, contact: str, mode: str = "demo", filter_type: str = "contact_person"):
    """Execute booking plan updates for all films using an existing page/ctx."""
    for film, entries in films_theatres.items():
        theatre_names = [e["theatre"] for e in entries]

        log(f"\n{'='*50}")
        log(f"Film: {film}")
        log(f"{'='*50}")
        _screenshot(page, f"bp_{_safe(film)}_start.png")

        # Navigate back to plans list if on a detail page
        if page.url.rstrip("/") != MICA_PLANS_URL.rstrip("/"):
            page.goto(MICA_PLANS_URL, wait_until="domcontentloaded", timeout=30_000)
            _dismiss_popups(page)
            try:
                page.wait_for_selector("table", timeout=10_000)
            except PlaywrightTimeout:
                pass

        log(f"Looking for plan: '{film}' ...")
        _search_plans_for_title(page, film)
        if not _find_and_click_plan(page, film, mode=mode):
            log(f"  ERROR: Plan not found for '{film}'")
            log(f"  Tip: Verify the title matches exactly in Mica → Sales → Plans")
            _screenshot(page, f"bp_{_safe(film)}_not_found.png")
            continue

        try:
            page.wait_for_url(
                lambda url: "/plans/" in url
                    and url.rstrip("/") != MICA_PLANS_URL.rstrip("/"),
                timeout=15_000,
            )
            page.wait_for_selector("table tbody tr", timeout=15_000)
        except PlaywrightTimeout:
            log("  WARNING: Plan detail page may not have fully loaded")

        log(f"  Plan opened: {page.url}")
        _dismiss_popups(page)
        page.wait_for_timeout(800)

        plan_default_date = _get_plan_release_date(page)
        log(f"  Plan default start date: {plan_default_date or 'unknown'}")
        _screenshot(page, f"bp_{_safe(film)}_detail.png")

        log(f"  Filtering by {filter_type}: {contact!r} ...")
        _filter_by_buyer(page, contact, filter_type=filter_type)
        _screenshot(page, f"bp_{_safe(film)}_filtered.png")

        _expand_table_page_size(page)
        count = _count_table_rows(page)
        log(f"  Venues for {contact!r}: {count}")
        if count == 0:
            log(f"  WARNING: No venues found for '{contact}' — skipping")
            continue

        if theatre_names:
            log(f"  Matching {len(theatre_names)} theatre(s) from booking sheet ...")
            mr = _select_matching_venues(page, theatre_names)
            n  = mr["selected"]
            log(f"  Selected {n} matching venue(s)")
            if n == 0:
                log("  WARNING: No venue matches found — skipping to avoid updating all venues")
                log("  Tip: check that theatre names in the booking match venues in the Mica plan")
                continue
        else:
            log("  WARNING: No theatre list found in booking — skipping to avoid updating all venues")
            log("  Tip: make sure the booking text includes theatre names")
            continue

        page.wait_for_timeout(800)
        _screenshot(page, f"bp_{_safe(film)}_selected.png")

        page.wait_for_timeout(1_200)
        log("  Setting status → Agreed ...")
        _set_agreed(page, n)
        _screenshot(page, f"bp_{_safe(film)}_agreed.png")

        if not plan_default_date:
            log("  WARNING: Plan release date unknown — skipping playweek date updates")
        else:
            non_default = [
                e for e in entries
                if e.get("date") and _full_date(e["date"], plan_default_date) != plan_default_date
            ]
            if non_default:
                log(f"  Updating playweek dates for {len(non_default)} venue(s) ...")
                for e in non_default:
                    full = _full_date(e["date"], plan_default_date)
                    log(f"    {e['theatre']}  →  {full}")
                    _update_venue_playweek(page, e["theatre"], full)
            else:
                log("  All venues open on the default date — no playweek updates needed")

        _screenshot(page, f"bp_{_safe(film)}_done.png")


def run_daemon(mode: str = "demo"):
    """
    Persistent mode: launch browser once, keep it open, read JSON jobs from
    stdin and process them one by one without restarting or re-logging in.

    Each job line: {"title": "...", "contact": "...", "booking_text": "..."}
    Responses: __JOB_DONE__ or __JOB_ERROR__ <message>
    Send __QUIT__ to close.
    """
    import json as _json

    global MICA_PLANS_URL, MICA_LOGIN_URL, AUTH_FILE
    base = MICA_BASE_URLS.get(mode, MICA_BASE_URLS["demo"])
    MICA_PLANS_URL = f"{base}/plans"
    MICA_LOGIN_URL = f"{base}/auth/login"
    AUTH_FILE      = OUTPUT_DIR / f"mica_auth_booking_{mode}.json"
    log(f"[daemon] Mode: {mode.upper()} ({base})")

    OUTPUT_DIR.mkdir(exist_ok=True)
    _pw = sync_playwright().start()
    browser = _pw.chromium.launch(
        headless=_HEADLESS, slow_mo=_SLOW_MO, args=_BROWSER_ARGS,
    )
    ctx_kwargs: dict = {"viewport": {"width": 1440, "height": 900}}
    if AUTH_FILE.exists():
        ctx_kwargs["storage_state"] = str(AUTH_FILE)
        log("[daemon] Using saved Mica session ...")
    ctx  = browser.new_context(**ctx_kwargs)
    page = ctx.new_page()
    if not _HEADLESS:
        page.bring_to_front()

    try:
        _navigate_to_plans(page, ctx)
        # Signal to launcher that browser is ready for jobs
        print("__READY__", flush=True)
        log("[daemon] Ready — waiting for jobs ...")

        for raw_line in sys.stdin:
            raw_line = raw_line.strip()
            if not raw_line:
                continue
            if raw_line == "__QUIT__":
                log("[daemon] Quit received — closing.")
                break
            try:
                job = _json.loads(raw_line)
                title        = job.get("title", "")
                contact      = job.get("contact", "")
                booking_text = job.get("booking_text", "")
                filter_type  = job.get("filter_type", "contact_person")

                films_theatres = parse_open_bookings(booking_text)
                if not films_theatres:
                    if title and booking_text.strip():
                        # Plain list of theatre names — use title from the Title field
                        bare = parse_bare_theatre_list(booking_text, title)
                        if bare:
                            log(f"  Detected plain theatre list — {len(bare)} theatre(s)")
                            films_theatres = {title: bare}
                        else:
                            films_theatres = {title: []}
                    elif title:
                        films_theatres = {title: []}
                    else:
                        log("ERROR: No films found in booking text")
                        print("__JOB_ERROR__ No films found", flush=True)
                        continue

                if title:
                    match = next(
                        (k for k in films_theatres
                         if title.lower() in k.lower() or k.lower() in title.lower()), None
                    )
                    films_theatres = {match: films_theatres[match]} if match else {title: films_theatres.get(title, [])}

                # Make sure we're on the plans page before each job
                if page.url.rstrip("/") != MICA_PLANS_URL.rstrip("/"):
                    page.goto(MICA_PLANS_URL, wait_until="domcontentloaded", timeout=30_000)
                    try:
                        page.wait_for_selector("table", timeout=10_000)
                    except PlaywrightTimeout:
                        pass

                _run_films_in_browser(page, ctx, films_theatres, contact, mode=mode, filter_type=filter_type)
                log("\n✓ Booking plan update complete!")
                print("__JOB_DONE__", flush=True)

            except Exception as exc:
                err_msg = str(exc)
                log(f"[daemon] ERROR: {err_msg}")
                # If the browser was closed externally, relaunch it and retry the job once
                if "closed" in err_msg.lower() or "target" in err_msg.lower():
                    log("[daemon] Browser appears closed — relaunching and retrying ...")
                    try:
                        browser = _pw.chromium.launch(
                            headless=_HEADLESS, slow_mo=_SLOW_MO, args=_BROWSER_ARGS,
                        )
                        ctx_kwargs2: dict = {"viewport": {"width": 1440, "height": 900}}
                        if AUTH_FILE.exists():
                            ctx_kwargs2["storage_state"] = str(AUTH_FILE)
                        ctx  = browser.new_context(**ctx_kwargs2)
                        page = ctx.new_page()
                        if not _HEADLESS:
                            page.bring_to_front()
                        _navigate_to_plans(page, ctx)
                        log("[daemon] Browser relaunched — retrying job ...")
                        try:
                            _run_films_in_browser(page, ctx, films_theatres, contact, mode=mode, filter_type=filter_type)
                            log("\n✓ Booking plan update complete!")
                            print("__JOB_DONE__", flush=True)
                        except Exception as retry_exc:
                            log(f"[daemon] Retry failed: {retry_exc}")
                            print(f"__JOB_ERROR__ {retry_exc}", flush=True)
                    except Exception as relaunch_exc:
                        log(f"[daemon] Relaunch failed: {relaunch_exc}")
                        print(f"__JOB_ERROR__ {exc}", flush=True)
                else:
                    print(f"__JOB_ERROR__ {exc}", flush=True)

    except Exception as exc:
        log(f"[daemon] Fatal error: {exc}")
    finally:
        try:
            _pw.stop()
        except Exception:
            pass


def run_booking_plan_update(title: str, contact: str, booking_text: str = "", filter_type: str = "contact_person", mode: str = "demo"):
    """
    For each film found in booking_text with 'Open' actions:
      - If `title` is given, process only that film.
      - Navigate to its Sales Plan (description = 'US, CA, PR').
      - Filter venues by `contact`.
      - Select only venues that match booking-sheet theatres.
      - Set status → Agreed.
      - Update playweek start dates for any venue opening on a different Friday.
    """
    log(f"Contact : {contact}")
    log(f"Title   : {title or '(all films in booking sheet)'}")
    log("")

    films_theatres = parse_open_bookings(booking_text)

    if not films_theatres:
        if title and booking_text.strip():
            # Try plain list of theatre names (pasted without email header)
            bare = parse_bare_theatre_list(booking_text, title)
            if bare:
                log(f"Detected plain theatre list — {len(bare)} theatre(s) for '{title}'")
                films_theatres = {title: bare}
            else:
                log("WARNING: No booking data found — no theatre names detected")
                films_theatres = {title: []}
        elif title:
            log("WARNING: No booking text provided")
            films_theatres = {title: []}
        else:
            log("ERROR: Nothing to process. Provide a booking sheet or specify --title")
            sys.exit(1)

    if title:
        match = next(
            (k for k in films_theatres
             if title.lower() in k.lower() or k.lower() in title.lower()), None
        )
        if match:
            films_theatres = {match: films_theatres[match]}
        else:
            log(f"WARNING: '{title}' not found in parsed rows — using theatre list as-is")
            films_theatres = {title: films_theatres.get(title, [])}

    log(f"Films to process: {list(films_theatres.keys())}")
    for film, entries in films_theatres.items():
        log(f"  {film}: {len(entries)} theatre(s)")
    log("")

    OUTPUT_DIR.mkdir(exist_ok=True)

    _pw = sync_playwright().start()
    browser = _pw.chromium.launch(
        headless=_HEADLESS, slow_mo=_SLOW_MO,
        args=_BROWSER_ARGS,
    )
    ctx_kwargs: dict = {"viewport": {"width": 1440, "height": 900}}
    if AUTH_FILE.exists():
        ctx_kwargs["storage_state"] = str(AUTH_FILE)
        log("Using saved Mica session ...")
    ctx  = browser.new_context(**ctx_kwargs)
    page = ctx.new_page()
    if not _SERVER_MODE:
        page.bring_to_front()

    try:
        _navigate_to_plans(page, ctx)
        _run_films_in_browser(page, ctx, films_theatres, contact, mode=mode, filter_type=filter_type)
        log("\n✓ Booking plan update complete!")

        # Keep browser open for review (local mode only)
        if not _HEADLESS:
            log("Browser is open for review — close the browser window when finished.")
            try:
                browser.wait_for_event("disconnected", timeout=3_600_000)
            except Exception:
                pass

    except PlaywrightTimeout as exc:
        log(f"\nERROR: Timeout — {exc}")
        _screenshot(page, "bp_error.png")
        raise
    except SystemExit:
        raise
    except Exception as exc:
        log(f"\nERROR: {exc}")
        _screenshot(page, "bp_error.png")
        raise
    finally:
        try:
            _pw.stop()
        except Exception:
            pass


def _safe(name: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", name.lower()).strip("_")[:30]


def _full_date(mm_dd: str, reference_date: str) -> str:
    """
    Given 'MM/DD' from the booking sheet and the plan's full date 'MM/DD/YYYY',
    return the full 'MM/DD/YYYY' using the plan's year.
    If reference_date is empty, returns mm_dd unchanged.
    """
    if not mm_dd:
        return ""
    if len(mm_dd) > 5:        # already has year
        return mm_dd
    year = reference_date[-4:] if reference_date and len(reference_date) >= 4 else ""
    return f"{mm_dd}/{year}" if year else mm_dd


# ---------------------------------------------------------------------------
# Auth helpers
# ---------------------------------------------------------------------------

def _on_auth_url(url: str) -> bool:
    return any(k in url.lower() for k in ("auth/login", "authentication", "sign-in"))


def _is_login_page(page) -> bool:
    url = page.url.lower()
    if _on_auth_url(url):
        return True
    # Also detect by presence of login form (in case URL doesn't indicate login)
    try:
        return page.locator('input[type="password"]').count() > 0
    except Exception:
        return False


def _do_login(page, ctx):
    # Delete stale auth file so next run starts clean
    if AUTH_FILE.exists():
        AUTH_FILE.unlink()
    # If not already on the login page, navigate there
    if not _on_auth_url(page.url):
        page.goto(MICA_LOGIN_URL, wait_until="domcontentloaded", timeout=20_000)
    if MICA_USER and MICA_PASS:
        log("Session expired — auto-logging in ...")
        try:
            _screenshot(page, "bp_login_before.png")
            # Wait longer for Angular SPA to render the form
            page.wait_for_selector(
                'input[placeholder="Email"], input[type="email"], input[type="text"]',
                timeout=20_000,
            )
            _screenshot(page, "bp_login_form.png")
            page.fill('input[placeholder="Email"], input[type="email"]', MICA_USER)
            page.fill('input[placeholder="Password"], input[type="password"]', MICA_PASS)
            page.click('button:has-text("Sign in"), button[type="submit"]')
            page.wait_for_url(
                lambda url: not _on_auth_url(url),
                timeout=30_000,
            )
            log("Auto-login successful. Saving session ...")
            AUTH_FILE.parent.mkdir(exist_ok=True)
            ctx.storage_state(path=str(AUTH_FILE))
        except (PlaywrightTimeout, Exception) as e:
            _screenshot(page, "bp_login_failed.png")
            if _SERVER_MODE:
                log(f"ERROR: Auto-login failed ({e.__class__.__name__}: {e})")
                log("Check your Mica credentials in your Profile settings.")
                sys.exit(1)
            log(f"WARNING: Auto-login failed ({e.__class__.__name__}) — please log in manually in the browser window ...")
            log("Waiting up to 3 minutes for manual login ...")
            try:
                page.bring_to_front()
                page.wait_for_url(
                    lambda url: not _on_auth_url(url),
                    timeout=180_000,
                )
                log("Logged in! Saving session ...")
                AUTH_FILE.parent.mkdir(exist_ok=True)
                ctx.storage_state(path=str(AUTH_FILE))
            except PlaywrightTimeout:
                log("ERROR: Login timed out — please re-run and log in within 3 minutes.")
                sys.exit(1)
    else:
        log("Please log in to Mica in the browser window ...")
        log("Tip: add MICA_USERNAME / MICA_PASSWORD to .env to skip this step.")
        log("Waiting up to 3 minutes ...")
        try:
            page.wait_for_url(
                lambda url: not _on_auth_url(url),
                timeout=180_000,
            )
        except PlaywrightTimeout:
            log("ERROR: Login timeout — re-run and log in within 3 minutes.")
            sys.exit(1)
        log("Logged in! Saving session ...")
        AUTH_FILE.parent.mkdir(exist_ok=True)
        ctx.storage_state(path=str(AUTH_FILE))


# ---------------------------------------------------------------------------
# UI helpers
# ---------------------------------------------------------------------------

def _screenshot(page, name: str):
    try:
        page.screenshot(path=str(OUTPUT_DIR / name))
    except Exception:
        pass


def _dismiss_popups(page):
    for selector in [
        ".modal-header button.btn-close", "button.btn-close",
        '[class*="toast"] button', '[class*="alert"] button.close',
        'button[aria-label="Close"]', 'button[aria-label="close"]',
    ]:
        try:
            btn = page.locator(selector)
            if btn.count() > 0:
                btn.first.click(timeout=800)
                page.wait_for_timeout(300)
        except Exception:
            pass


def _get_plan_release_date(page) -> str:
    """
    Read the plan's release/start date from the page.
    Returns 'MM/DD/YYYY' string or '' if not found.
    """
    try:
        date_str: str = page.evaluate("""
        () => {
            // Method 1: element whose text is exactly "Release Date" → look for date nearby
            const all = Array.from(document.querySelectorAll('*'));
            for (const el of all) {
                const t = (el.childNodes.length === 1 && el.childNodes[0].nodeType === 3)
                    ? el.textContent.trim().toLowerCase() : '';
                if (t === 'release date') {
                    // Check next sibling element
                    let sib = el.nextElementSibling;
                    while (sib) {
                        const m = sib.textContent.match(/(\\d{2}\\/\\d{2}\\/\\d{4})/);
                        if (m) return m[1];
                        sib = sib.nextElementSibling;
                    }
                    // Check parent element text
                    const parentText = el.parentElement ? el.parentElement.textContent : '';
                    const m2 = parentText.match(/(\\d{2}\\/\\d{2}\\/\\d{4})/);
                    if (m2) return m2[1];
                }
            }
            // Method 2: scan full page text for "release date" followed by a date
            const body = document.body ? document.body.innerText : '';
            const idx = body.toLowerCase().indexOf('release date');
            if (idx >= 0) {
                const nearby = body.substring(idx, idx + 120);
                const m = nearby.match(/(\\d{1,2}\\/\\d{1,2}\\/\\d{4})/);
                if (m) {
                    const parts = m[1].split('/');
                    return parts[0].padStart(2,'0') + '/' + parts[1].padStart(2,'0') + '/' + parts[2];
                }
            }
            // Method 3: look for date range header like "Fr, May 1, 2026"
            const headings = Array.from(document.querySelectorAll('h1,h2,h3,h4,p,[class*="title"],[class*="header"],[class*="date"]'));
            for (const h of headings) {
                const m = h.textContent.match(/([A-Za-z]+\\.?\\s+\\d{1,2},\\s*\\d{4})/);
                if (m) {
                    const d = new Date(m[1].replace('.',''));
                    if (!isNaN(d.getTime())) {
                        const mo = String(d.getMonth()+1).padStart(2,'0');
                        const dy = String(d.getDate()).padStart(2,'0');
                        return mo + '/' + dy + '/' + d.getFullYear();
                    }
                }
            }
            return '';
        }
        """)
        return date_str or ""
    except Exception:
        return ""


def _search_plans_for_title(page, title: str):
    """
    On the Plans list page, use the Production(s) filter + calendar button + Search
    to load the plans for a specific film title.  This ensures plans outside the
    default date window (e.g. future or past releases) are visible.
    """
    try:
        # 1. Find the Production(s) ng-select via JS (same approach as _filter_by_buyer)
        idx: int = page.evaluate(
            """
            (hints) => {
                const allNg = Array.from(document.querySelectorAll('ng-select'));
                for (let i = 0; i < allNg.length; i++) {
                    const ph = allNg[i].querySelector('.ng-placeholder');
                    const phText = (ph ? ph.textContent : '').trim().toLowerCase();
                    if (hints.some(h => phText.includes(h))) return i;
                    const inp = allNg[i].querySelector('input');
                    const inpPh = ((inp && inp.placeholder) || '').toLowerCase();
                    if (hints.some(h => inpPh.includes(h))) return i;
                }
                return 0;  // fallback: first ng-select
            }
            """,
            ["production", "select"],
        )
        prod_sel = page.locator("ng-select").nth(idx)

        prod_sel.click()
        page.wait_for_timeout(400)
        search_input = prod_sel.locator('input').first
        if search_input.count() > 0:
            search_input.type(title, delay=60)  # keystroke events trigger ng-select search
        else:
            page.keyboard.type(title, delay=60)
        page.wait_for_timeout(900)

        # Click the first visible option (same pattern as _filter_by_buyer)
        opt = page.locator('.ng-option:visible, [role="option"]:visible').first
        if opt.count() > 0:
            opt_text = (opt.inner_text() or "").strip()
            if opt_text.lower() not in ("no items found", ""):
                opt.click()
                page.wait_for_timeout(800)
                sel_label = (prod_sel.locator('.ng-value-label, .ng-value').first.text_content() or '').strip()
                log(f"  Production filter set: '{sel_label}'" if sel_label else "  WARNING: Production filter may not have applied")
            else:
                page.keyboard.press("Escape")
                log(f"  WARNING: Production '{title}' not found in dropdown — searching without filter")
        else:
            page.keyboard.press("Escape")
            log(f"  WARNING: No options found for '{title}' — searching without filter")

        # 2. Widen the date range to cover all history (past + future plans).
        #    Use the Angular-compatible native setter so change events fire correctly.
        dates_set = page.evaluate("""
        () => {
            const setAngularInput = (el, val) => {
                const setter = Object.getOwnPropertyDescriptor(
                    window.HTMLInputElement.prototype, 'value'
                ).set;
                setter.call(el, val);
                el.dispatchEvent(new Event('input',  { bubbles: true }));
                el.dispatchEvent(new Event('change', { bubbles: true }));
                el.dispatchEvent(new KeyboardEvent('keyup', { bubbles: true }));
            };
            // Find inputs whose current value looks like MM/DD/YYYY
            const dateInputs = Array.from(document.querySelectorAll('input'))
                .filter(inp => /\\d{2}\\/\\d{2}\\/\\d{4}/.test(inp.value));
            if (dateInputs.length >= 2) {
                setAngularInput(dateInputs[0], '01/01/2010');   // start: far past
                setAngularInput(dateInputs[1], '12/31/2035');   // end:   far future
                return { start: dateInputs[0].value, end: dateInputs[1].value };
            }
            return null;
        }
        """)
        if dates_set:
            log(f"  Date range expanded: {dates_set['start']} → {dates_set['end']}")
        else:
            log("  WARNING: Date inputs not found — trying calendar button fallback")
            # Fallback: click the small icon button between week-nav and Search
            page.evaluate("""
            () => {
                const btns = Array.from(document.querySelectorAll('button'));
                const searchIdx = btns.findIndex(b =>
                    b.textContent.trim().toLowerCase().includes('search')
                );
                const limit = searchIdx >= 0 ? searchIdx : btns.length;
                const NAV = new Set(['<', '>', '‹', '›', '«', '»']);
                for (let i = limit - 1; i >= Math.max(0, limit - 6); i--) {
                    const b = btns[i];
                    if (!b.offsetParent) continue;
                    const txt = b.textContent.trim();
                    if (NAV.has(txt) || txt.toLowerCase().includes('search')) continue;
                    const rect = b.getBoundingClientRect();
                    if (rect.width > 0) { b.click(); return; }
                }
            }
            """)
            page.wait_for_timeout(300)

        # 3. Click the Search button
        search_btn = page.locator(
            'button:has-text("Search"), button[type="submit"]:has-text("Search")'
        ).first
        if search_btn.count() > 0:
            search_btn.click()
            log("  Search clicked")
            try:
                page.wait_for_selector("table tbody tr", timeout=10_000)
            except PlaywrightTimeout:
                pass
            page.wait_for_timeout(800)
        else:
            log("  WARNING: Search button not found — table may not refresh")

    except Exception as e:
        log(f"  WARNING: Plans filter setup failed ({e}) — will search current table")


def _find_and_click_plan(page, title: str, mode: str = "demo") -> bool:
    """Find the best matching plan row for a title.
    - Searches ALL cells in each row (not just first).
    - Production: prefer plans with 'US, CA, PR' description.
    - Demo: prefer plans with 'Demo' description.
    - If only one plan exists, use it regardless of description.
    """
    title_lower = title.lower().strip()
    result: dict = page.evaluate(
        """
        ([titleLower, isDemo]) => {
            const rows = Array.from(document.querySelectorAll('table tbody tr'));
            const sample = rows.slice(0, 5).map(r =>
                r.textContent.trim().replace(/\\s+/g, ' ').substring(0, 80)
            );

            // Score each matching row — higher = better
            // Demo mode:  +15 description contains "demo"
            // Prod mode:  +10 description contains "us" AND "can/ca", +5 "us, ca", +2 "pr"
            // -20  description is "Mystery Movie" (hidden/blind title — never the right plan)
            // -10  description contains "test"
            const PREFER = isDemo ? [
                d => d.includes('demo')                           ? 15 : 0,
            ] : [
                // US/CAN ONLY, US/CAN/PR, US, CA, PR — all territory indicators
                d => (d.includes('us') && (d.includes('can') || d.includes('ca'))) ? 10 : 0,
                d => d.includes('pr')                             ?   2 : 0,
                d => d.includes('us, ca') || d.includes('us,ca') ?   5 : 0,
            ];
            const AVOID = [
                d => d.includes('mystery movie')              ? -20 : 0,
                d => d.includes('test')                       ? -10 : 0,
            ];

            let bestScore = -Infinity, bestIdx = -1, firstMatch = -1;
            for (let i = 0; i < rows.length; i++) {
                const cells = Array.from(rows[i].querySelectorAll('td'));
                if (!cells.length) continue;
                const texts = cells.map(c => c.textContent.trim().toLowerCase());
                if (!texts.some(t => t.includes(titleLower))) continue;
                if (firstMatch < 0) firstMatch = i;

                // Find description cell (usually 3rd or 4th — not title, date, or checkbox)
                const desc = texts.find(t =>
                    t && !t.includes(titleLower) && !/^(mo|tu|we|th|fr|sa|su),/.test(t) && !/^\\d/.test(t)
                ) || '';

                let score = 0;
                PREFER.forEach(fn => score += fn(desc));
                AVOID.forEach(fn  => score += fn(desc));

                if (score > bestScore) { bestScore = score; bestIdx = i; }
            }
            const chosen = bestIdx >= 0 ? bestIdx : firstMatch;
            const chosenDesc = chosen >= 0
                ? Array.from(rows[chosen].querySelectorAll('td'))
                    .map(c => c.textContent.trim()).filter(Boolean).join(' | ')
                    .substring(0, 120)
                : '';
            return { idx: chosen, sample, chosenDesc };
        }
        """,
        [title_lower, mode == "demo"],
    )
    log(f"  Plans table sample rows: {result.get('sample', [])}")
    log(f"  Selected plan row: {result.get('chosenDesc', '(none)')}")
    row_idx = result.get("idx", -1)
    if row_idx < 0:
        return False
    row = page.locator("table tbody tr").nth(row_idx)
    # Find the cell that contains the title text and click its link
    # (first cell is often a checkbox — don't click that)
    title_link_clicked = False
    cells = row.locator("td")
    for i in range(cells.count()):
        cell = cells.nth(i)
        cell_text = (cell.text_content() or "").strip().lower()
        if title_lower in cell_text:
            link = cell.locator("a")
            if link.count() > 0:
                link.first.click()
            else:
                cell.click()
            title_link_clicked = True
            break
    if not title_link_clicked:
        # Fallback: click first link in the row
        any_link = row.locator("a")
        if any_link.count() > 0:
            any_link.first.click()
        else:
            row.locator("td").nth(1).click()  # skip checkbox col
    return True


def _filter_by_circuit(page, circuit: str) -> bool:
    """
    Set the Circuit ng-select on the plan detail page to narrow venues by chain.
    Returns True if the filter was applied successfully.
    Tries multiple detection strategies since the placeholder label varies.
    """
    if not circuit:
        return False
    log(f"  Filtering by Circuit: {circuit!r} ...")

    # Log all ng-select placeholders to help diagnose
    placeholders = page.evaluate("""
        () => Array.from(document.querySelectorAll('ng-select')).map((ng, i) => {
            const ph  = ng.querySelector('.ng-placeholder');
            const inp = ng.querySelector('input');
            return { i, ph: (ph ? ph.textContent : '').trim(),
                     inpPh: (inp ? inp.placeholder : '').trim() };
        })
    """)
    log(f"  ng-selects found: {placeholders}")

    # Strategy 1: match placeholder "circuit", "venue group", or "chain"
    CIRCUIT_LABELS = {"circuit", "venue group", "chain", "exhibitor group"}
    idx = next(
        (d["i"] for d in placeholders
         if any(lbl in d["ph"].lower() or lbl in d["inpPh"].lower()
                for lbl in CIRCUIT_LABELS)),
        -1,
    )
    if idx >= 0:
        log(f"  Using circuit ng-select (idx {idx}, ph='{placeholders[idx]['ph']}')")

    # Strategy 2: fallback — the FIRST ng-select that is NOT a known non-circuit filter
    KNOWN = {"contact", "booker", "buyer", "market", "tv", "country",
             "region", "version", "screening", "status", "capacity",
             "capabilities"}
    if idx < 0:
        for d in placeholders:
            combined = (d["ph"] + " " + d["inpPh"]).lower()
            if not any(k in combined for k in KNOWN):
                idx = d["i"]
                log(f"  Using first unknown ng-select as circuit filter (idx {idx}, ph='{d['ph']}')")
                break

    if idx < 0:
        log("  WARNING: Circuit ng-select not found — skipping circuit filter")
        return False

    ng_sel = page.locator("ng-select").nth(idx)
    try:
        clr = ng_sel.locator(".ng-clear-wrapper, .ng-value-icon").first
        if clr.count() > 0:
            clr.click(timeout=500)
            page.wait_for_timeout(300)
    except Exception:
        pass
    ng_sel.click()
    page.wait_for_timeout(400)
    inp = ng_sel.locator("input").first
    try:
        inp.fill(circuit)
    except Exception:
        page.keyboard.type(circuit)
    page.wait_for_timeout(800)

    # Check if any matching option exists — bail if "No items found"
    opt = page.locator(
        f'.ng-option:has-text("{circuit}"), [role="option"]:has-text("{circuit}")'
    ).first
    if opt.count() > 0:
        opt.click()
        log(f"  Circuit filter set: {circuit!r}")
        page.wait_for_timeout(1_500)
        return True

    # No exact match — check what's visible
    opt_any = page.locator('.ng-option:visible, [role="option"]:visible').first
    if opt_any.count() > 0:
        opt_text = (opt_any.text_content() or "").strip().lower()
        if "no items" in opt_text or "not found" in opt_text or opt_text == "":
            # Filter returned nothing — wrong spelling or value not in this plan
            page.keyboard.press("Escape")
            log(f"  WARNING: Circuit '{circuit}' not found in dropdown "
                f"(got '{opt_text}') — check spelling. Filter NOT applied.")
            page.wait_for_timeout(500)
            return False
        # Something matched — click the best visible option
        full_text = (opt_any.text_content() or "").strip()
        opt_any.click()
        log(f"  Circuit filter: selected '{full_text}'")
        page.wait_for_timeout(1_500)
        return True

    page.keyboard.press("Escape")
    log(f"  WARNING: No dropdown options appeared for circuit '{circuit}'")
    page.wait_for_timeout(500)
    return False


_FILTER_TYPE_HINTS: dict = {
    "contact_person": ["contact person"],
    "booker":         ["booker"],
    "venue_group":    ["venue group"],
    "tv_market":      ["tv market"],
    "capabilities":   ["capabilities"],
}


def _filter_by_buyer(page, contact: str, filter_type: str = "contact_person"):
    """Set the specified ng-select filter on the plan detail page."""
    hints = _FILTER_TYPE_HINTS.get(filter_type, ["contact person"])
    idx: int = page.evaluate(
        """
        (hints) => {
            const allNg = Array.from(document.querySelectorAll('ng-select'));
            for (let i = 0; i < allNg.length; i++) {
                const ph = allNg[i].querySelector('.ng-placeholder');
                const phText = (ph ? ph.textContent : '').trim().toLowerCase();
                if (hints.some(h => phText.includes(h))) return i;
                const inp = allNg[i].querySelector('input');
                const inpPh = ((inp && inp.placeholder) || '').toLowerCase();
                if (hints.some(h => inpPh.includes(h))) return i;
            }
            return -1;
        }
        """,
        hints,
    )
    if idx < 0:
        log(f"  WARNING: '{filter_type}' ng-select not found — venues may not be filtered")
        return
    ng_sel = page.locator("ng-select").nth(idx)
    try:
        clr = ng_sel.locator(".ng-clear-wrapper, .ng-value-icon").first
        if clr.count() > 0:
            clr.click(timeout=500)
            page.wait_for_timeout(300)
    except Exception:
        pass
    ng_sel.click()
    page.wait_for_timeout(400)
    inp = ng_sel.locator("input").first
    if inp.count() > 0:
        inp.fill(contact)
    else:
        page.keyboard.type(contact)
    page.wait_for_timeout(900)
    # Click the first visible dropdown option (case-insensitive partial match)
    opt = page.locator('.ng-option:visible, [role="option"]:visible').first
    if opt.count() > 0:
        opt_text = opt.inner_text().strip()
        opt.click()
        log(f"  Venue filter set: '{opt_text}'")
    else:
        log(f"  WARNING: '{contact}' not found in Contact dropdown — pressing Enter")
        page.keyboard.press("Enter")
    page.wait_for_timeout(1_500)


def _expand_table_page_size(page):
    """
    Try to set the table's per-page size to its maximum option so all filtered
    rows are rendered in the DOM before we try to match/select them.
    """
    try:
        # Look for a per-page select (common patterns: select, ng-select near pagination)
        expanded = page.evaluate("""
        () => {
            // Try a native <select> that contains large numbers (50, 100, 200, All)
            const selects = Array.from(document.querySelectorAll('select'));
            for (const sel of selects) {
                const opts = Array.from(sel.options).map(o => o.value);
                const big = opts.filter(v => parseInt(v) >= 50 || v.toLowerCase() === 'all');
                if (big.length > 0) {
                    sel.value = big[big.length - 1];
                    sel.dispatchEvent(new Event('change', { bubbles: true }));
                    return 'native-select:' + big[big.length - 1];
                }
            }
            return null;
        }
        """)
        if expanded:
            log(f"  Page size expanded via {expanded}")
            page.wait_for_timeout(1_200)
        else:
            # No per-page control found — table may already show all rows
            log("  No per-page size control found (table may show all rows)")
    except Exception as e:
        log(f"  WARNING: Could not expand page size ({e})")


def _count_table_rows(page) -> int:
    try:
        return page.locator("table tbody tr").count()
    except Exception:
        return 0


# City+state → actual venue name aliases for exhibitors that send booking sheets
# with only "City, ST  HOLD/Final" (no theatre name).  Key = normalized city+state.
# Espanola is the critical one — "dreamcatcher" has no city word so word-scoring fails.
_CITY_VENUE_ALIASES: dict[str, str] = {
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
    "lamar, mo":         "plaza lamar 1",
    "lamar mo":          "plaza lamar 1",
    "borger, tx":        "morley borger 5",
    "borger tx":         "morley borger 5",
    "mountain grove, mo": "fun city 5 cinemas",
    "mountain grove mo":  "fun city 5 cinemas",
    "mountain grove":     "fun city 5 cinemas",
    # AMC booking-name → Mica venue name (name differs from what parser extracts)
    "mt vernon 8":              "Mount Vernon 8",
    "mesa grand 14":            "Mesa Grande 14",
    "southern hill 12":         "Southern Hills 12",
    "arrowhead town center 14": "Arrowhead 14",
    "tulsa 12":                 "Tulsa Hills 12",
    "southroads 20":            "Southroads Tulsa 20",
    "foothills 15":             "Foothills Tucson 15",
    "surprise 14":              "Surprise Pointe 14",
    # Regal/Becky Williams — "Stm" suffix stripped by STOP, but these need city disambiguation
    "champlain centre stm 8":        "Champlain Plattsburgh 8",
    "e. greenbush 8":                "East Greenbush 8",
    "aviation mall 9":               "Aviation Mall Queensbury 9",
    # Regal/Rich Motzer — abbreviated venue names
    "natomas mktplace stm 16 & rpx": "Natomas Marketplace 16",
    "stockton cty ctr stm 16 & imax":"Stockton City Center 16",
    # Regal/Christopher Lauderdale — stm-only sig-word venues need city disambiguation
    "auburn stm 17":                "Auburn Stadium 17",
    "bridgeport stm 18 & imax":     "Bridgeport Tigard 18",
    "cascade stm 16 imax & rpx":    "Cascade Vancouver 16",
    "cinema 99 stm 11":             "Cinema 99 Vancouver",
    "city center stm 12":           "City Vancouver 12",
    "movies on tv stm 16":          "Movies Hillsboro 16",
    "santiam stm 11":               "Santiam Salem 11",
    "stark street stm 10":          "Stark Gresham 10",
    # Celebration! Cinema (Zach Righetti) — booking names differ from Mica abbreviated names
    "cinema carousel 16":               "Celebration Cinema Carousel",
    "crossroads 15 + imax":             "Celebration Crossroads Imax",
    "rivertown 13 + c premium":         "Celebration Cinema Rivertown",
    "grand rapids north 17 + imax":     "Celebration Cinema GR North",
    "grand rapids south 15 + c premium":"Celebration South",
    "lansing 19 + c premium xl":        "Celebration Lansing Imax",
    "mt. pleasant 11":                  "Celebration Mt Pleasant",
    "benton harbor 14 + dbox":          "Celebration Benton Harbor",
    # Hooky Entertainment / Tammy Flores — Hutto has no city in venue name; RedStone spelling
    "hooky entertainment + sdx + imax, hutto": "Hooky Entertainment + SDX + IMAX Hutto 8",
    "hooky entertainment + sdx + imax":        "Hooky Entertainment + SDX + IMAX Hutto 8",
    "redstone 14 cinemas w/pdx":               "Red Stone 14 Cinemas",
    "redstone 14 cinemas":                     "Red Stone 14 Cinemas",
}


def _select_matching_venues(page, theatre_names: list[str], dry_run: bool = False) -> dict:
    """
    Check the checkbox for each venue row that best matches a booking-sheet
    theatre name using significant-word scoring.
    If dry_run=True, computes matches but does NOT click any checkboxes.
    Returns dict: { selected: int, missed: list[str], unselected: list[str] }
    """
    # Pre-translate known city+state aliases → actual venue names so the JS
    # word-scorer can find them (e.g. "Espanola, NM" → "Dreamcatcher 10").
    translated = [_CITY_VENUE_ALIASES.get(n.lower().strip(), n) for n in theatre_names]
    result = page.evaluate(
        """
        ([bookingNames, dryRun]) => {
            // Require at least 2 meaningful words to match — prevents single-word
            // false positives (e.g. "Park Royal" hitting "Park North").
            const MIN_SCORE = 2;
            // Generic words present in nearly all venue names — strip before scoring
            // so only location-specific words drive the match.
            const STOP = new Set([
                'cinemas','cinema','theatre','theater','theatres','theaters',
                'odeon','vip','and','xscape','entertainment','centre','center',
                'et','avec','les','des','cineplex','with','stm','temp','screens'
            ]);
            // Circuit/format brand words that inflate AMC/Cinemark venue name word counts.
            // Stripped from the DENOMINATOR when computing ratio so that a booking like
            // "Albany 16" (1 sig word: "albany") gets ratio 1/1 = 1.0 against
            // "AMC CLASSIC Albany 16" instead of 1/3 = 0.33.
            const CIRCUIT_WORDS = new Set([
                'amc','classic','imax','dolby','plf','dine','prime','luxe',
                'max','rpx','xd','btx','alc','epl','cinemark','regal','showcase',
                'landmark','harkins','reading','marcus','epic','theatres'
            ]);
            function normalize(s) {
                // Strip diacritics so French names (é, è, â, etc.) match ASCII versions
                return s.normalize('NFD').replace(/[\\u0300-\\u036f]/g, '').toLowerCase();
            }
            // US/CA 2-letter state/province codes — kept even though < 3 chars
            // so city+state bookings ("Florence, SC") disambiguate by state.
            const STATE_CODES = new Set([
                'al','ak','az','ar','ca','co','ct','de','fl','ga','hi','id','il','in',
                'ia','ks','ky','la','me','md','ma','mi','mn','ms','mo','mt','ne','nv',
                'nh','nj','nm','ny','nc','nd','oh','ok','or','pa','ri','sc','sd','tn',
                'tx','ut','vt','va','wa','wv','wi','wy','dc','pr','vi',
                'ab','bc','mb','nb','nl','ns','on','pe','qc','sk'
            ]);
            function sigWords(s) {
                // Min length 3 prevents "st" from matching "strawberry" etc.
                // Exception: 2-letter US/CA state codes are kept for city+state matching.
                return normalize(s).split(/[^a-z0-9]+/)
                        .filter(w => (w.length >= 3 || STATE_CODES.has(w))
                                     && !/^\\d+$/.test(w) && !STOP.has(w));
            }
            // Returns { matched: count, ratio: matched/venueWordCount }
            // Exact word match only — no startsWith to prevent "st" → "strawberry" bugs.
            // Ratio uses venue words MINUS circuit brand words so AMC/CLASSIC don't inflate
            // the denominator and sink single-word bookings like "Albany 16".
            function scoreInfo(venueText, bookingWords) {
                const rw = sigWords(venueText);
                const rwForRatio = rw.filter(w => !CIRCUIT_WORDS.has(w));
                const denom = rwForRatio.length > 0 ? rwForRatio.length : rw.length;
                const m = bookingWords.filter(w => rw.includes(w)).length;
                return { matched: m, ratio: denom > 0 ? m / denom : 0 };
            }
            const headers = Array.from(document.querySelectorAll('table thead th'))
                                  .map(h => h.textContent.trim().toLowerCase());
            const venueColIdx = headers.findIndex(h => h.startsWith('venue'));
            const rows = Array.from(document.querySelectorAll('table tbody tr'));
            const venueTexts = rows.map(row => {
                const cells = Array.from(row.querySelectorAll('td'));
                const cell = venueColIdx >= 0 ? cells[venueColIdx] : null;
                return (cell ? cell.textContent : '').trim().replace(/\\s+/g, ' ');
            });
            // Track which rows are already claimed to avoid double-matching
            const usedIdx = new Set();
            let selected = 0;
            const matched = [], missed = [];
            for (const booking of bookingNames) {
                // Detect "City, ST" or "City ST" format (e.g. "Lamar, MO", "Borger TX")
                const cityStateMatch = normalize(booking).match(/^([a-z][a-z\\s]+?)\\s*,?\\s*([a-z]{2})$/);
                const isCityState = cityStateMatch && STATE_CODES.has(cityStateMatch[2]);

                let bestMatched = 0, bestRatio = 0, bestRowIdx = -1, bestText = '';

                if (isCityState) {
                    // City+state input: match any venue whose name contains the city name
                    const city = cityStateMatch[1].trim();
                    rows.forEach((row, i) => {
                        if (usedIdx.has(i)) return;
                        if (normalize(venueTexts[i]).includes(city)) {
                            // Score by how much of the venue name is the city
                            const ratio = city.split(' ').length / (sigWords(venueTexts[i]).length || 1);
                            if (bestRowIdx < 0 || ratio > bestRatio) {
                                bestMatched = 1; bestRatio = ratio; bestRowIdx = i; bestText = venueTexts[i];
                            }
                        }
                    });
                } else {
                const bw = sigWords(booking);
                // For city+state bookings (e.g. "Independence, MO"), strip state codes
                // before matching — state codes don't appear in venue names and block match.
                const bwNoState = bw.filter(w => !STATE_CODES.has(w));
                const bwForScore = bwNoState.length >= 1 ? bwNoState : bw;
                // Allow single-word match when:
                //   a) booking reduces to 1 word after state-code stripping, OR
                //   b) booking reduces to 1 word after stop-word removal (e.g. "Clarion Theatre")
                // In both cases require a high ratio (≥0.8) to avoid false positives on
                // large plans where one common word matches many venues.
                const singleWord = bwForScore.length <= 1;
                const minScore = singleWord ? 1 : MIN_SCORE;
                const minRatio = singleWord ? 0.8 : 0;
                rows.forEach((row, i) => {
                    if (usedIdx.has(i)) return;  // skip already-matched rows
                    const { matched: m, ratio } = scoreInfo(venueTexts[i], bwForScore);
                    // Prefer higher word count match; break ties by word-density ratio
                    if (m > bestMatched || (m === bestMatched && m > 0 && ratio > bestRatio)) {
                        bestMatched = m; bestRatio = ratio; bestRowIdx = i; bestText = venueTexts[i];
                    }
                });
                if (!(bestRowIdx >= 0 && bestMatched >= minScore && bestRatio >= minRatio)) {
                    bestRowIdx = -1;
                }
                } // end non-city-state branch

                if (bestRowIdx >= 0) {
                    if (!dryRun) {
                        const cb = rows[bestRowIdx].querySelector('input[type="checkbox"]');
                        if (cb && !cb.checked) { cb.click(); selected++; }
                    } else {
                        selected++;
                    }
                    usedIdx.add(bestRowIdx);
                    matched.push({ booking, matched: bestText, score: bestMatched });
                } else {
                    missed.push({ booking, bestScore: bestMatched, bestText });
                }
            }
            // Mica venues that were not claimed by any CSV name — capture full row data
            const cityColIdx    = headers.findIndex(h => h.includes('city'));
            const stateColIdx   = headers.findIndex(h => h.includes('state') || h.includes('province'));
            const screensColIdx = headers.findIndex(h => h.includes('screen'));
            function rowData(row, i) {
                const cells = Array.from(row.querySelectorAll('td'));
                const venue = (venueColIdx >= 0 ? cells[venueColIdx]?.textContent : '').trim().replace(/\\s+/g, ' ');
                if (!venue) return null;
                const city    = (cityColIdx    >= 0 ? cells[cityColIdx]?.textContent    : '').trim();
                const state   = (stateColIdx   >= 0 ? cells[stateColIdx]?.textContent   : '').trim();
                const screens = (screensColIdx >= 0 ? cells[screensColIdx]?.textContent : '').trim();
                return { venue, city, state, screens };
            }
            // Rows already checked (Agreed) in Mica BEFORE our run, and not matched by booking sheet
            const alreadyAgreed = rows
                .map((row, i) => {
                    if (usedIdx.has(i)) return null;
                    const cb = row.querySelector('input[type="checkbox"]');
                    if (!cb || !cb.checked) return null;
                    return rowData(row, i);
                })
                .filter(t => t !== null);
            const unselected = rows
                .map((row, i) => {
                    if (usedIdx.has(i)) return null;
                    return rowData(row, i);
                })
                .filter(t => t !== null);
            return { selected, matched, missed, venueTexts, unselected, alreadyAgreed };
        }
        """,
        [translated, dry_run],
    )
    for m in result.get("matched", []):
        orig = theatre_names[translated.index(m['booking'])] if m['booking'] in translated else m['booking']
        label = f"{orig} → {m['booking']}" if orig != m['booking'] else orig
        log(f"    MATCH  '{label}' → '{m['matched']}' (score {m['score']})")
    for m in result.get("missed", []):
        log(f"    MISS   '{m['booking']}' — best score {m['bestScore']} ('{m['bestText']}')")
    missed_list    = result.get("missed", [])
    unselected     = result.get("unselected", [])
    already_agreed = result.get("alreadyAgreed", [])
    total_csv     = len(theatre_names)
    total_matched = result.get("selected", 0)
    log(f"\n  --- Venue Match Summary ---")
    log(f"  CSV theatres   : {total_csv}")
    log(f"  Matched        : {total_matched}")
    if missed_list:
        log(f"  NOT matched ({len(missed_list)}):")
        for m in missed_list:
            closest = f" (closest: '{m['bestText']}', score {m['bestScore']})" if m.get("bestText") else ""
            log(f"    - '{m['booking']}'{closest}")
    if unselected:
        log(f"  Mica venues not selected ({len(unselected)}):")
        for v in unselected:
            log(f"    - '{v.get('venue', v) if isinstance(v, dict) else v}'")
    if already_agreed:
        log(f"  Already Agreed in Mica (not on booking sheet) ({len(already_agreed)}):")
        for v in already_agreed:
            log(f"    - '{v.get('venue', v) if isinstance(v, dict) else v}'")
    # Emit machine-readable lists for the UI panels
    import json as _json_sel
    log(f"__UNBOOKED__:{_json_sel.dumps(unselected)}")
    log(f"__ALREADY_BOOKED__:{_json_sel.dumps(already_agreed)}")
    # Missed = on booking sheet but not found in Mica plan
    missed_simple = [{'venue': m['booking'], 'bestMatch': m.get('bestText',''), 'score': m.get('bestScore',0)} for m in missed_list]
    log(f"__MISSED__:{_json_sel.dumps(missed_simple)}")
    return {
        "selected":   total_matched,
        "missed":     [m["booking"] for m in missed_list],
        "unselected": unselected,
    }


def _select_all(page):
    """Click all visible row checkboxes via JS (bypasses disabled/visibility guards)."""
    result = page.evaluate("""
    () => {
        // Prefer the header 'select all' checkbox
        const hdr = document.querySelector('table thead input[type="checkbox"]');
        if (hdr) {
            hdr.click();
            return { via: 'header', count: 1 };
        }
        // Fall back: click every body-row checkbox that is not yet checked
        const cbs = Array.from(
            document.querySelectorAll('table tbody input[type="checkbox"]')
        );
        let clicked = 0;
        cbs.forEach(cb => { if (!cb.checked) { cb.click(); clicked++; } });
        return { via: 'rows', count: clicked };
    }
    """)
    via   = result.get("via",   "?") if result else "?"
    count = result.get("count", 0)   if result else 0
    if count:
        log(f"  Checkboxes selected via JS ({via}): {count}")
    else:
        log("  WARNING: No checkboxes found in table — Status button may stay disabled")
    # Give Angular time to react and enable the bulk-Status button
    page.wait_for_timeout(1_000)


def _set_agreed(page, expected: int):
    """Click bulk Set Status → Agreed → Continue."""
    btn_idx: int = page.evaluate(
        """
        () => {
            const candidates = Array.from(document.querySelectorAll(
                '[ngbdropdowntoggle], button.dropdown-toggle'
            ));
            for (let i = 0; i < candidates.length; i++) {
                const btn = candidates[i];
                if (!btn.textContent.trim().toLowerCase().includes('status')) continue;
                let el = btn.parentElement;
                let inRow = false;
                while (el) {
                    if (el.tagName === 'TR') { inRow = true; break; }
                    el = el.parentElement;
                }
                if (!inRow) return i;
            }
            return -1;
        }
        """
    )
    if btn_idx < 0:
        log("  ERROR: Bulk 'Status' dropdown not found in toolbar")
        return
    all_toggles = page.locator("[ngbdropdowntoggle], button.dropdown-toggle")
    # Remove disabled, then JS-click — Mica sometimes lags enabling the button
    # after checkboxes are ticked; forcing the click is safe here.
    toggle_handle = all_toggles.nth(btn_idx).element_handle()
    page.evaluate("""el => {
        el.removeAttribute('disabled');
        el.classList.remove('disabled');
        el.dispatchEvent(new MouseEvent('click', { bubbles: true, cancelable: true }));
    }""", toggle_handle)
    page.wait_for_timeout(1_000)
    agreed = page.locator(
        '.dropdown-menu.show .dropdown-item:has-text("Agreed"), '
        '.dropdown-menu.show button:has-text("Agreed")'
    ).first
    if agreed.count() == 0:
        agreed = page.locator(
            '.dropdown-item:has-text("Agreed"), button:has-text("Agreed")'
        ).first
    if agreed.count() == 0:
        log("  ERROR: 'Agreed' option not found in Status dropdown")
        return
    # Use JS click to bypass 'disabled' class and pointer-event interception
    page.evaluate("el => el.click()", agreed.element_handle())
    page.wait_for_timeout(800)
    try:
        page.wait_for_selector(
            '[role="dialog"]:has-text("Agreed"), .modal-content:has-text("Agreed")',
            timeout=6_000,
        )
        log(f"  Confirming for {expected} venue(s) ...")
        cont = page.locator(
            '[role="dialog"] button:has-text("Continue"), '
            '.modal-content button:has-text("Continue")'
        ).first
        if cont.count() > 0:
            cont.click()
            page.wait_for_timeout(2_500)
            log(f"  Status → Agreed ✓  ({expected} venues)")
        else:
            log("  WARNING: 'Continue' button not found in confirmation dialog")
    except PlaywrightTimeout:
        log("  WARNING: Confirmation dialog did not appear")


def _update_venue_playweek(page, theatre_name: str, new_date: str):
    """
    For the venue row matching theatre_name, click its Start Date cell to open
    the Edit Playweeks modal, update the Start Date to new_date (MM/DD/YYYY),
    let End Date auto-populate, then click Save.
    """
    # Find the "Start Date" column index
    col_idx: int = page.evaluate("""
    () => {
        const headers = Array.from(document.querySelectorAll('table thead th'));
        const idx = headers.findIndex(h =>
            h.textContent.trim().toLowerCase().includes('start date')
        );
        return idx;
    }
    """)

    if col_idx < 0:
        log(f"  WARNING: 'Start Date' column not found — cannot update playweek")
        return

    # Find the row that best matches the theatre name
    row_idx: int = page.evaluate(
        """
        (theatreName) => {
            function sigWords(s) {
                return s.toLowerCase().split(/[^a-z0-9]+/)
                        .filter(w => w.length >= 2 && !/^\\d+$/.test(w));
            }
            function scoreInfo(venueText, bw) {
                const rw = sigWords(venueText);
                const m = bw.filter(w => rw.some(r =>
                    r === w || r.startsWith(w) || w.startsWith(r)
                )).length;
                return { m, ratio: rw.length > 0 ? m / rw.length : 0 };
            }
            // Find the Venue column by header text
            const headers = Array.from(document.querySelectorAll('table thead th'))
                                  .map(h => h.textContent.trim().toLowerCase());
            const venueColIdx = headers.findIndex(h => h.startsWith('venue'));
            const rows = Array.from(document.querySelectorAll('table tbody tr'));
            const bw = sigWords(theatreName);
            let bestM = 0, bestRatio = 0, bestIdx = -1;
            rows.forEach((row, i) => {
                const cells = Array.from(row.querySelectorAll('td'));
                const vCell = venueColIdx >= 0 ? cells[venueColIdx] : (cells[2] || cells[1]);
                const text = (vCell ? vCell.textContent : row.textContent).trim();
                const { m, ratio } = scoreInfo(text, bw);
                if (m > bestM || (m === bestM && m > 0 && ratio > bestRatio)) {
                    bestM = m; bestRatio = ratio; bestIdx = i;
                }
            });
            return bestM >= 1 ? bestIdx : -1;
        }
        """,
        theatre_name,
    )

    if row_idx < 0:
        log(f"  WARNING: Row not found for playweek update: {theatre_name}")
        return

    # Click the Start Date cell via JS to bypass sticky nav/pagination overlays
    page.evaluate(
        """
        ([rowIdx, colIdx]) => {
            const rows = document.querySelectorAll('table tbody tr');
            const row  = rows[rowIdx];
            if (!row) return;
            const cells = row.querySelectorAll('td');
            const cell  = cells[colIdx];
            if (!cell) return;
            const target = cell.querySelector('a') || cell;
            target.dispatchEvent(new MouseEvent('click', {bubbles: true, cancelable: true}));
        }
        """,
        [row_idx, col_idx],
    )

    # Wait for Edit Playweeks modal
    try:
        page.wait_for_selector(
            '[role="dialog"]:has-text("Playweek"), .modal:has-text("Playweek"), '
            '[role="dialog"]:has-text("Play"), .modal:has-text("Play")',
            timeout=6_000,
        )
    except PlaywrightTimeout:
        log(f"  WARNING: Edit Playweeks modal did not open for: {theatre_name}")
        return

    modal = page.locator(
        '[role="dialog"]:has-text("Playweek"), .modal-content:has-text("Playweek"), '
        '[role="dialog"]:has-text("Play"), .modal-content:has-text("Play")'
    ).first

    # Fill the Start Date input (first text/date input in the modal)
    start_input = modal.locator('input[type="text"], input[type="date"]').first
    if start_input.count() == 0:
        start_input = modal.locator("input").first

    if start_input.count() > 0:
        start_input.click(click_count=3)
        start_input.fill(new_date)
        page.wait_for_timeout(200)
        page.keyboard.press("Tab")           # trigger End Date auto-populate
        page.wait_for_timeout(800)
        log(f"  Start date set to {new_date}")
    else:
        log(f"  WARNING: Start date input not found in Edit Playweeks modal")
        page.keyboard.press("Escape")
        return

    # Click Save
    save_btn = modal.locator('button:has-text("Save")').first
    if save_btn.count() > 0:
        save_btn.click()
        page.wait_for_timeout(1_500)
        log(f"  Playweek saved ✓  ({theatre_name})")
    else:
        log(f"  WARNING: Save button not found in Edit Playweeks modal")
        page.keyboard.press("Escape")


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def parse_open_bookings_by_contact(text: str) -> dict[str, dict[str, list[dict]]]:
    """
    Parse booking text and return:
      { film: { contact: [ {"theatre": str, "date": str|None}, ... ] } }
    Accepts rows whose Action contains "open", "final", or is blank (Cinemark format).
    Contact is read from the Buyer or Br column.
    """
    if not text or not text.strip():
        return {}

    lines     = [l for l in text.splitlines() if l.strip()]
    max_tabs  = max((l.count("\t") for l in lines[:5]), default=0)
    max_commas= max((l.count(",") for l in lines[:5]), default=0)
    _is_csc_hdr2 = any(l.strip().lower() in ('theatre #', 'theater #') for l in lines[:4])

    if (max_tabs < 2 and max_commas < 2) or _is_csc_hdr2:
        raw_rows = _parse_one_per_line(text)
    else:
        delim    = "\t" if max_tabs > max_commas else ","
        raw_rows = _parse_delimited(text, delim)

    # Film title from preamble (Cinemark puts it before __DMA__ etc.)
    preamble_film = _preamble_film_title(text)

    _skip = {"buyer","br","unit","attraction","film","title","type","media",
             "prt","action","policy","status","phrase","comscore #","comscore","#"}
    results: dict[str, dict[str, list[dict]]] = {}

    for row in raw_rows:
        fl     = {k.lower().strip(): (v.strip() if v else "") for k, v in row.items()}
        action = fl.get("action") or fl.get("policy") or ""
        if not _is_active_action(action):
            continue

        theatre = fl.get("theatre") or fl.get("theater") or ""
        film    = (fl.get("attraction") or fl.get("film") or fl.get("title") or "")
        # "sales" is the Cinemark __SALES__ column (buyer name)
        contact = fl.get("buyer") or fl.get("br") or fl.get("sales") or ""

        if not theatre.strip():
            for k, v in fl.items():
                if k not in _skip and v and not v.strip().isdigit():
                    theatre = v
                    break

        if not theatre.strip() or not contact.strip():
            continue

        date = _parse_action_date(action)
        film = film.strip() or preamble_film or "Unknown"
        contact = contact.strip()
        results.setdefault(film, {}).setdefault(contact, []).append(
            {"theatre": theatre.strip(), "date": date}
        )

    return results


def parse_participating_theatres_csv(text: str) -> list[str]:
    """
    Parse a Cineplex-style participating theatres CSV.
    Finds the header row containing 'Theatre Name' and extracts all theatre names.
    Returns empty list if format is not recognised.
    """
    if not text or not text.strip():
        return []
    lines = text.splitlines()
    header_idx = None
    for i, line in enumerate(lines):
        ll = line.lower()
        if "theatre name" in ll and ("thr." in ll or "region" in ll or "location id" in ll):
            header_idx = i
            break
    if header_idx is None:
        return []

    csv_text = "\n".join(lines[header_idx:])
    reader   = csv.DictReader(io.StringIO(csv_text))
    theatre_col = next(
        (c for c in (reader.fieldnames or []) if "theatre name" in c.lower()),
        None,
    )
    if not theatre_col:
        return []

    theatres = []
    for row in reader:
        name = row.get(theatre_col, "").strip()
        if name:
            theatres.append(name)
    return theatres


def run_participation_update(title: str, booking_text: str = "", circuit: str = "", dry_run: bool = False):
    """
    Participation CSV mode: parse all theatre names from a Cineplex-style
    participating-theatres CSV, open the Mica Sales Plan for the given film,
    optionally filter by circuit, select matching venues, and set status to Agreed.
    No contact/buyer filtering — processes every venue in the plan.
    """
    theatre_names = parse_participating_theatres_csv(booking_text)
    if not theatre_names:
        log("ERROR: No theatre names found in participation CSV")
        sys.exit(1)

    log(f"Title      : {title}")
    log(f"Mode       : PARTICIPATION (no contact filter)")
    log(f"Circuit    : {circuit or '(none)'}")
    log(f"Dry run    : {'YES — no changes will be made' if dry_run else 'no'}")
    log(f"CSV theatres: {len(theatre_names)}")
    log("")

    OUTPUT_DIR.mkdir(exist_ok=True)

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=_HEADLESS, slow_mo=_SLOW_MO,
            args=_BROWSER_ARGS,
        )
        ctx_kwargs: dict = {"viewport": {"width": 1440, "height": 900}}
        if AUTH_FILE.exists():
            ctx_kwargs["storage_state"] = str(AUTH_FILE)
            log("Using saved Mica session ...")
        ctx  = browser.new_context(**ctx_kwargs)
        page = ctx.new_page()
        if not _SERVER_MODE:
            page.bring_to_front()

        try:
            _navigate_to_plans(page, ctx)

            log(f"Looking for plan: '{title}' ...")
            _search_plans_for_title(page, title)
            if not _find_and_click_plan(page, title, mode=args.mode):
                log(f"ERROR: Plan not found for '{title}'")
                log("Tip: Verify the title matches exactly in Mica → Sales → Plans")
                sys.exit(1)

            try:
                page.wait_for_url(
                    lambda url: "/plans/" in url
                        and url.rstrip("/") != MICA_PLANS_URL.rstrip("/"),
                    timeout=15_000,
                )
                page.wait_for_selector("table tbody tr", timeout=15_000)
            except PlaywrightTimeout:
                log("WARNING: Plan detail page may not have fully loaded")

            log(f"Plan opened: {page.url}")
            _dismiss_popups(page)
            page.wait_for_timeout(800)

            # Optionally narrow the table to one circuit before matching
            circuit_applied = False
            if circuit:
                circuit_applied = _filter_by_circuit(page, circuit)

            _expand_table_page_size(page)
            count = _count_table_rows(page)
            log(f"Total venues in plan: {count}")
            if circuit and not circuit_applied:
                log("ERROR: Circuit filter could not be applied.")
                log("  Check the circuit name spelling — it must match exactly what appears in Mica.")
                log(f"  You entered: '{circuit}'")
                log("  Stopping to avoid matching against wrong venues.")
                sys.exit(1)
            if count == 0:
                log("ERROR: No venues found in plan")
                sys.exit(1)

            log(f"{'[DRY RUN] ' if dry_run else ''}Matching {len(theatre_names)} theatre(s) from CSV against {count} Mica venues ...")
            mr = _select_matching_venues(page, theatre_names, dry_run=dry_run)
            n  = mr["selected"]
            log(f"{'[DRY RUN] ' if dry_run else ''}{'Would match' if dry_run else 'Selected'} {n} of {len(theatre_names)} CSV theatres")
            if n == 0:
                log("WARNING: No matches found — check theatre names in CSV vs Mica")
                sys.exit(1)

            if not dry_run:
                page.wait_for_timeout(500)
                log("Setting status → Agreed ...")
                _set_agreed(page, n)
            else:
                log("[DRY RUN] Skipping status change — no venues modified")

            # ── Final summary ────────────────────────────────────────────────
            missed_csv    = mr.get("missed", [])
            not_in_plan   = mr.get("unselected", [])
            log("\n" + "="*60)
            log(f"  {'[DRY RUN] ' if dry_run else ''}PARTICIPATION SUMMARY — {title}")
            log("="*60)
            log(f"  CSV locations      : {len(theatre_names)}")
            log(f"  {'Would match' if dry_run else 'Matched / Agreed'} : {n}")
            log(f"  NOT matched (CSV)  : {len(missed_csv)}")
            if missed_csv:
                for loc in missed_csv:
                    log(f"    ✗  {loc}")
            log(f"  Mica plan venues not in CSV : {len(not_in_plan)}")
            if not_in_plan:
                for loc in not_in_plan:
                    log(f"    –  {loc}")
            log("="*60)

            # Save unmatched list to a text file for easy review
            report_path = OUTPUT_DIR / f"participation_unmatched_{_safe(title)}.txt"
            with open(report_path, "w", encoding="utf-8") as fh:
                fh.write(f"Participation Update — {title}\n")
                fh.write(f"CSV locations: {len(theatre_names)}  |  Matched: {n}\n\n")
                fh.write(f"CSV locations NOT found in Mica plan ({len(missed_csv)}):\n")
                for loc in missed_csv:
                    fh.write(f"  - {loc}\n")
                fh.write(f"\nMica plan venues NOT in CSV ({len(not_in_plan)}):\n")
                for loc in not_in_plan:
                    fh.write(f"  - {loc}\n")
            log(f"\n  Report saved → {report_path}")
            log("\nMass participation update complete!")

        except PlaywrightTimeout as exc:
            log(f"\nERROR: Timeout — {exc}")
            raise
        except SystemExit:
            raise
        except Exception as exc:
            log(f"\nERROR: {exc}")
            raise


def run_mass_booking_plan_update(title: str, booking_text: str = ""):
    """
    Mass version: automatically processes ALL contacts found in the booking sheet.
    For each film → for each unique contact → filter plan by contact, select matching
    venues, set to Agreed, update playweek dates.
    """
    log(f"Title   : {title or '(all films in booking sheet)'}")
    log(f"Mode    : MASS (all contacts)")
    log("")

    films_by_contact = parse_open_bookings_by_contact(booking_text)

    if not films_by_contact:
        log("WARNING: No 'Open' rows with buyer/contact found in booking sheet")
        sys.exit(1)

    if title:
        match = next(
            (k for k in films_by_contact
             if title.lower() in k.lower() or k.lower() in title.lower()), None
        )
        if match:
            films_by_contact = {match: films_by_contact[match]}
        else:
            log(f"WARNING: '{title}' not found in Open rows — found: {list(films_by_contact.keys())}")
            films_by_contact = {title: {}}

    # Summary
    for film, contacts in films_by_contact.items():
        log(f"  {film}:")
        for contact, entries in contacts.items():
            log(f"    {contact}: {len(entries)} theatre(s)")
    log("")

    OUTPUT_DIR.mkdir(exist_ok=True)

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=_HEADLESS, slow_mo=_SLOW_MO,
            args=_BROWSER_ARGS,
        )
        ctx_kwargs: dict = {"viewport": {"width": 1440, "height": 900}}
        if AUTH_FILE.exists():
            ctx_kwargs["storage_state"] = str(AUTH_FILE)
            log("Using saved Mica session ...")
        ctx  = browser.new_context(**ctx_kwargs)
        page = ctx.new_page()
        if not _SERVER_MODE:
            page.bring_to_front()

        try:
            _navigate_to_plans(page, ctx)

            for film, contacts_map in films_by_contact.items():
                log("=" * 50)
                log(f"Film: {film}")
                log("=" * 50)

                for contact, entries in contacts_map.items():
                    theatre_names = [e["theatre"] for e in entries]
                    log(f"\n  Contact: {contact} ({len(entries)} theatre(s))")

                    # Navigate back to plans list if needed
                    if page.url.rstrip("/") != MICA_PLANS_URL.rstrip("/"):
                        page.goto(MICA_PLANS_URL, wait_until="domcontentloaded", timeout=30_000)
                        _dismiss_popups(page)
                        try:
                            page.wait_for_selector("table", timeout=10_000)
                        except PlaywrightTimeout:
                            pass

                    log(f"  Looking for plan: '{film}' ...")
                    _search_plans_for_title(page, film)
                    if not _find_and_click_plan(page, film, mode=args.mode):
                        log(f"  ERROR: Plan not found for '{film}' — skipping")
                        continue

                    try:
                        page.wait_for_url(
                            lambda url: "/plans/" in url
                                and url.rstrip("/") != MICA_PLANS_URL.rstrip("/"),
                            timeout=15_000,
                        )
                        page.wait_for_selector("table tbody tr", timeout=15_000)
                    except PlaywrightTimeout:
                        log("  WARNING: Plan detail page may not have fully loaded")

                    log(f"  Plan opened: {page.url}")
                    _dismiss_popups(page)
                    page.wait_for_timeout(800)

                    plan_default_date = _get_plan_release_date(page)
                    log(f"  Plan default start date: {plan_default_date or 'unknown'}")
                    _screenshot(page, f"bp_mass_{_safe(film)}_{_safe(contact)}_detail.png")

                    log(f"  Filtering by Venue Group / Contact: {contact!r} ...")
                    _filter_by_buyer(page, contact)
                    _screenshot(page, f"bp_mass_{_safe(film)}_{_safe(contact)}_filtered.png")

                    _expand_table_page_size(page)
                    count = _count_table_rows(page)
                    log(f"  Venues for {contact!r}: {count}")
                    if count == 0:
                        log(f"  WARNING: No venues found for '{contact}' — skipping")
                        continue

                    if theatre_names:
                        log(f"  Matching {len(theatre_names)} theatre(s) from booking sheet ...")
                        mr = _select_matching_venues(page, theatre_names)
                        n  = mr["selected"]
                        log(f"  Selected {n} matching venue(s)")
                        if n == 0:
                            log("  WARNING: No venue matches found — skipping to avoid updating all venues")
                            log("  Tip: check that theatre names in the booking match venues in the Mica plan")
                            continue
                    else:
                        log("  WARNING: No theatre list found in booking — skipping to avoid updating all venues")
                        log("  Tip: make sure the booking text includes theatre names")
                        continue

                    page.wait_for_timeout(800)
                    _screenshot(page, f"bp_mass_{_safe(film)}_{_safe(contact)}_selected.png")

                    log("  Setting status → Agreed ...")
                    _set_agreed(page, n)
                    _screenshot(page, f"bp_mass_{_safe(film)}_{_safe(contact)}_agreed.png")

                    if not plan_default_date:
                        log("  WARNING: Plan release date unknown — skipping playweek updates")
                    else:
                        non_default = [
                            e for e in entries
                            if e.get("date") and _full_date(e["date"], plan_default_date) != plan_default_date
                        ]
                        if non_default:
                            log(f"  Updating playweek dates for {len(non_default)} venue(s) ...")
                            for e in non_default:
                                full = _full_date(e["date"], plan_default_date)
                                log(f"    {e['theatre']}  →  {full}")
                                _update_venue_playweek(page, e["theatre"], full)
                        else:
                            log("  All venues open on the default date — no playweek updates needed")

                    _screenshot(page, f"bp_mass_{_safe(film)}_{_safe(contact)}_done.png")

            log("\n✓ Mass booking plan update complete!")

        except PlaywrightTimeout as exc:
            log(f"\nERROR: Timeout — {exc}")
            _screenshot(page, "bp_mass_error.png")
            raise
        except SystemExit:
            raise
        except Exception as exc:
            log(f"\nERROR: {exc}")
            _screenshot(page, "bp_mass_error.png")
            raise


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Update Mica Sales Plan venues to Agreed and fix start dates"
    )
    parser.add_argument("--contact", default="",
                        help='Buyer/Booker name (e.g. "Rich Motzer"); omit when using --mass')
    parser.add_argument("--title", default="",
                        help='Film title filter (optional; omit to process all Open films)')
    parser.add_argument("--booking", default="",
                        help="Path to booking text file (one-per-line or CSV format)")
    parser.add_argument("--mass", action="store_true",
                        help="Mass mode: auto-process all contacts found in booking sheet")
    parser.add_argument("--mode", choices=["demo", "prod"], default="demo",
                        help="demo or prod Mica environment (default: demo)")
    parser.add_argument("--circuit", default="",
                        help="Optional circuit/chain filter (e.g. 'Cineplex') applied before matching")
    parser.add_argument("--dry-run", action="store_true",
                        help="Preview matches without making any changes in Mica")
    parser.add_argument("--filter-type",
                        choices=["contact_person", "booker", "venue_group", "tv_market", "capabilities"],
                        default="contact_person",
                        help="Which Mica plan dropdown to filter by (default: contact_person)")
    parser.add_argument("--daemon", action="store_true",
                        help="Persistent mode: read JSON jobs from stdin, keep browser open")
    args = parser.parse_args()

    # Apply mode — update module-level globals so all functions use the right URLs
    base = MICA_BASE_URLS.get(args.mode, MICA_BASE_URLS["demo"])
    MICA_PLANS_URL = f"{base}/plans"
    MICA_LOGIN_URL = f"{base}/auth/login"
    AUTH_FILE      = OUTPUT_DIR / f"mica_auth_booking_{args.mode}.json"
    log(f"Mode: {args.mode.upper()} ({base})")

    if args.daemon:
        run_daemon(mode=args.mode)
        sys.exit(0)

    if not args.mass and not args.contact:
        parser.error("--contact is required unless --mass is specified")

    booking_text = ""
    if args.booking:
        p = Path(args.booking)
        if p.exists():
            booking_text = p.read_text(encoding="utf-8-sig", errors="replace")
        else:
            log(f"WARNING: Booking file not found: {args.booking}")

    if args.mass:
        # Auto-detect: participation CSV (Theatre Name column) vs regular booking sheet
        if parse_participating_theatres_csv(booking_text):
            if not args.title:
                parser.error("--title is required for participation CSV mode")
            run_participation_update(args.title, booking_text, circuit=args.circuit,
                                     dry_run=args.dry_run)
        else:
            run_mass_booking_plan_update(args.title, booking_text)
    else:
        run_booking_plan_update(args.title, args.contact, booking_text,
                                filter_type=args.filter_type, mode=args.mode)
