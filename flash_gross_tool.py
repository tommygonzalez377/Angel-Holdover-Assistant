"""
Flash Gross Tool
-----------------
1. Reads a booking sheet CSV and finds FINAL locations (Action col contains "FINAL")
2. Fetches the master list from Google Sheets to map Unit # → Rentrak ID
3. Builds Comscore (boxofficeessentials.com) URLs using the Rentrak ID
4. Scrapes ALL films per theatre from the flash gross table
5. Outputs an interactive HTML dashboard (theatre tabs, sortable table,
   Angel Studios films highlighted)

Match key: booking sheet "Unit" == master list "Exhibitor's Ref ID"
"""

import os
import re
import sys
import csv
import io
import json
import base64
import argparse
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from datetime import datetime, date, timedelta

import pandas as pd
from dotenv import load_dotenv
from jinja2 import Environment, FileSystemLoader
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError

# Optional DB alias lookup (available when running inside the launcher server)
try:
    import db as _fgt_db
except ImportError:
    _fgt_db = None

load_dotenv()

BASE_DIR      = Path(__file__).parent

_SERVER_MODE  = bool(os.getenv("SERVER_MODE"))
_BROWSER_ARGS = [
    "--disable-gpu", "--no-sandbox",
    "--disable-blink-features=AutomationControlled",
    "--disable-dev-shm-usage",
    "--no-first-run",
    "--no-default-browser-check",
] if _SERVER_MODE else []
TEMPLATE_DIR  = BASE_DIR / "templates"
OUTPUT_DIR    = BASE_DIR / "output"
ML_CACHE_PATH = BASE_DIR / "master_list_cache.csv"
OUTPUT_DIR.mkdir(exist_ok=True)

COMSCORE_BASE  = "https://beta.boxofficeessentials.com"
COMSCORE_FLASH = f"{COMSCORE_BASE}/reports/flash/theater_films_by_rank"
COMSCORE_USER  = os.getenv("COMSCORE_USERNAME", "")
COMSCORE_PASS  = os.getenv("COMSCORE_PASSWORD", "")

# Parallel scraping: number of simultaneous browser tabs.
# Sequential (1 worker) on server — keeps the main session active and avoids
# Comscore's 2-minute idle timeout that kills copied-session parallel browsers.
MAX_SCRAPE_WORKERS = 2 if _SERVER_MODE else 5
_LOGIN_LOCK = threading.Lock()   # prevents simultaneous re-login across worker tabs

ANTHROPIC_KEY  = os.getenv("ANTHROPIC_API_KEY", "")

MASTER_LIST_URL = (
    "https://docs.google.com/spreadsheets/d/"
    "14VKWNE_oCsjPJ2my_EKAO8A9HdW2L4ZA/export?format=csv&gid=845899698"
)

# Distributor name → short code used in the dashboard
DIST_ABBREV = {
    "angel studios":            "ANGEL",
    "sony":                     "SNY",
    "sony/crunchyroll":         "SNY",
    "paramount":                "PAR",
    "20th century studios":     "20TH",
    "disney":                   "DIS",
    "warner bros.":             "WB",
    "warner bros":              "WB",
    "lionsgate":                "LION",
    "a24":                      "A24",
    "neon":                     "NEON",
    "neon rated":               "NEON",
    "universal":                "UNI",
    "universal pictures":       "UNI",
    "briarcliff":               "BCLF",
    "independent films":        "IndeFilms",
    "fathom entertainment":     "FTHM",
    "trafalgar releasing":      "TRAFR",
    "focus features":           "FOC",
    "amazon mgm studios":       "AMZMGM",
    "amazon mgm":               "AMZMGM",
    "amazon":                   "AMZMGM",
    "vertical entertainment":   "VERT",
    "vertical":                 "VERT",
    "seismic pictures":         "SEISMIC",
    "gkids":                    "GKIDS",
    "well go usa entertainment":"WLGO",
    "well go usa":              "WLGO",
    "cmc pictures":             "CMC",
    "sony pictures classics":   "SPC",
    "specialty":                "SPC",
}

def abbrev_dist(full_name: str) -> str:
    key = full_name.lower().strip()
    if key in DIST_ABBREV:
        return DIST_ABBREV[key]
    # Fallback: first word, up to 6 chars, uppercase
    return full_name.split()[0][:6].upper() if full_name else "?"


# ---------------------------------------------------------------------------
# Date helpers
# ---------------------------------------------------------------------------

def most_recent_friday(today: date = None) -> date:
    today = today or date.today()
    days_since_friday = (today.weekday() - 4) % 7
    # If today IS Friday, flash data for this week isn't ready yet — use last Friday instead
    if days_since_friday == 0:
        days_since_friday = 7
    return today - timedelta(days=days_since_friday)

def parse_week_arg(week_str: str) -> str:
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%m-%d-%Y"):
        try:
            return datetime.strptime(week_str, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    raise ValueError(f"Cannot parse date '{week_str}'. Use YYYY-MM-DD or MM/DD/YYYY.")


# ---------------------------------------------------------------------------
# Screenshot parsing — extract FINAL locations using Claude vision
# ---------------------------------------------------------------------------

IMAGE_EXTS = {".png", ".jpg", ".jpeg", ".webp", ".gif"}

def parse_screenshot(image_path: str) -> list[dict]:
    """Extract FINAL locations from a booking sheet screenshot using Windows OCR."""
    path = Path(image_path)
    print(f"Reading screenshot with Windows OCR: {path.name}")

    text = _ocr_image(path)
    if not text:
        sys.exit("ERROR: Windows OCR returned no text. Make sure the image is clear and readable.")

    locations = _parse_ocr_text(text)

    if not locations:
        print("No FINAL locations found in screenshot.")
        print("--- Full OCR text (for debugging) ---")
        print(text)
        return []

    print(f"Found {len(locations)} FINAL location(s) from screenshot:")
    for loc in locations:
        print(f"  Unit {loc.get('unit', '?')}  {loc.get('theatre', '?')}  [{loc.get('action', '?')}]")

    return locations


def _ocr_image(path: Path) -> str:
    """Run Windows 11 built-in OCR on an image file. Returns the full text."""
    import asyncio
    try:
        from winrt.windows.media.ocr import OcrEngine
        from winrt.windows.globalization import Language
        from winrt.windows.graphics.imaging import BitmapDecoder
        from winrt.windows.storage.streams import InMemoryRandomAccessStream, DataWriter
    except ImportError:
        sys.exit(
            "ERROR: Windows OCR packages not installed.\n"
            "Run: pip install winrt-Windows.Media.Ocr winrt-Windows.Graphics.Imaging "
            "winrt-Windows.Storage.Streams winrt-Windows.Globalization Pillow"
        )

    # Convert to PNG bytes via Pillow so Windows OCR can always decode it
    # Upscale small images — Windows OCR accuracy drops significantly on low-res images
    from PIL import Image
    import io
    img = Image.open(path).convert("RGBA")
    # Scale up to at least 1600px wide for reliable OCR
    min_width = 1600
    if img.width < min_width:
        scale = max(2, min_width // img.width)
        img = img.resize((img.width * scale, img.height * scale), Image.LANCZOS)
        print(f"  Upscaled image {scale}x for OCR ({img.width}x{img.height})")
    buf = io.BytesIO()
    img.save(buf, format="PNG")
    img_bytes = buf.getvalue()

    async def run():
        stream = InMemoryRandomAccessStream()
        writer = DataWriter(stream)
        writer.write_bytes(img_bytes)
        await writer.store_async()
        stream.seek(0)

        decoder = await BitmapDecoder.create_async(stream)
        bitmap  = await decoder.get_software_bitmap_async()

        engine = OcrEngine.try_create_from_language(Language("en-US"))
        if not engine:
            engine = OcrEngine.try_create_from_user_profile_languages()

        result = await engine.recognize_async(bitmap)
        # Join lines preserving structure
        return "\n".join(line.text for line in result.lines)

    return asyncio.run(run())


def _parse_ocr_text(text: str) -> list[dict]:
    """
    Parse raw OCR text from a booking sheet to find FINAL rows.

    Windows OCR on a table screenshot typically reads column-by-column, not row-by-row.
    So unit numbers appear on separate lines from the FINAL markers.

    Strategy:
    1. Try row-based parsing first (if OCR happened to read left-to-right)
    2. Fall back to column-based: collect all unit numbers + count FINAL rows,
       match them positionally.
    """
    import re
    lines = [l.strip() for l in text.splitlines() if l.strip()]

    # --- Strategy 1: FINAL on same line as unit number (row-based OCR) ---
    row_results = []
    seen = set()
    for line in lines:
        if not re.search(r'\bfin[ai]?[ln]\b', line, re.IGNORECASE):  # "FINAL" or OCR garble
            continue
        if not re.search(r'\d', line):
            continue
        m = re.search(r'\b(\d{3,6})\b', line)
        if m:
            unit = m.group(1).lstrip("0") or "0"
            if unit not in seen:
                seen.add(unit)
                action_m = re.search(r'(fin\S*\s*[\d/\-]*)', line, re.IGNORECASE)
                action = action_m.group(1).strip() if action_m else "FINAL"
                row_results.append({"unit": unit, "theatre": f"Unit {unit}",
                                    "attraction": "", "action": action})
    if row_results:
        return row_results

    # --- Strategy 2: column-based OCR — units and FINAL on separate lines ---
    # Collect standalone unit numbers (lines that are just digits, 3-6 chars)
    unit_lines = []
    for line in lines:
        if re.fullmatch(r'\d{3,6}', line):
            unit_lines.append(line)

    # Count lines that look like FINAL (including garbled variants: FNAL, FiNAL, etc.)
    final_lines = [l for l in lines if re.search(r'\bfin[ai]?[ln]\b', l, re.IGNORECASE)
                   or re.search(r'\bfnal\b', l, re.IGNORECASE)]
    # Also count bare date lines near end as potential extra FINALs
    # e.g. "03/12" on its own often means a FINAL row whose "FINAL" was garbled
    date_only = [l for l in lines if re.fullmatch(r'\d{1,2}[/\-]\d{1,2}', l)]

    final_count = len(final_lines) + len(date_only)

    if not unit_lines:
        return []

    # Match first `final_count` units to FINAL rows
    # (units appear in same order as rows in the table)
    units_for_final = unit_lines[:final_count] if final_count < len(unit_lines) else unit_lines

    results = []
    seen = set()
    for unit_str in units_for_final:
        unit = unit_str.lstrip("0") or "0"
        if unit in seen:
            continue
        seen.add(unit)
        results.append({"unit": unit, "theatre": f"Unit {unit}",
                        "attraction": "", "action": "FINAL"})
    return results


# ---------------------------------------------------------------------------
# Step 1: Parse booking sheet — find FINAL rows
# ---------------------------------------------------------------------------

def _parse_one_per_line(raw: str):
    """
    Handle booking system copy-paste formats where each cell is on its own line.

    Two formats supported:
    - Original booking system: blank/space line between every cell (separator format)
    - ComScore booking table: no separators; empty cells may be absent entirely

    Strategy:
    1. Strip blank lines (they are separators in the original format, and absent
       cells don't produce blank lines in the ComScore format).
    2. Find "Action" or "Policy" header; include any trailing known headers
       (e.g. "Showtimes") to determine the full column list.
    3. If the first column is a numeric ID column (ComScore #, Unit, etc.),
       use ID-based row detection: each row starts with a 3+ digit number.
       This handles rows where trailing empty cells are simply absent.
    4. Otherwise fall back to fixed-size chunking (original booking format
       where every row has exactly n_cols cells).
    """
    import re as _re

    # ── __COLUMN__ format detection ────────────────────────────────────────
    # Some booking exports use headers like __DMA__, __THEATRE__, __ __ etc.
    # Preamble lines (account name, film title) may appear before the headers.
    first_vals = [l.strip() for l in raw.splitlines() if l.strip()]
    _dunder_start = next((i for i, v in enumerate(first_vals)
                          if _re.fullmatch(r'__.*__', v)), None)
    if _dunder_start is not None:
        first_vals = first_vals[_dunder_start:]  # skip preamble
    if first_vals and _re.fullmatch(r'__.*__', first_vals[0]):
        _NAME_MAP = {'SALES': 'Buyer', 'THEATRE': 'Theatre', 'THEATER': 'Theatre',
                     'SCR': 'Screens', '#': 'Unit', 'DMA': 'DMA'}
        action_count = 0
        header_values = []
        for v in first_vals:
            m = _re.fullmatch(r'__(.*?)__', v)
            if not m:
                break
            inner = m.group(1).strip()
            if not inner:           # blank __ __ column
                action_count += 1
                header_values.append('Action' if action_count == 1 else 'Terms')
            else:
                header_values.append(_NAME_MAP.get(inner.upper(), inner))

        if header_values:
            n_cols = len(header_values)
            # Re-parse raw keeping blank/space lines as empty-cell markers
            all_stripped = [l.strip() for l in raw.splitlines()]
            data_lines, skipped, in_data = [], 0, False
            for v in all_stripped:
                if not in_data:
                    if _re.fullmatch(r'__.*__', v):
                        skipped += 1
                        if skipped == n_cols:
                            in_data = True
                else:
                    data_lines.append(v)
            rows = []
            for i in range(0, len(data_lines), n_cols):
                chunk = data_lines[i : i + n_cols]
                if len(chunk) < n_cols:
                    chunk += [''] * (n_cols - len(chunk))
                rows.append(chunk)
            if rows:
                return pd.DataFrame(rows, columns=header_values, dtype=str)

    # ── Cinemark DB export format (snake_case headers: theater_name / status) ─
    _SNAKE_KEYS_FGT = {'dma_name', 'city', 'state', 'theater_name', 'theatre_name',
                       'title', 'status', 'account_name', 'circuit'}
    _SNAKE_MAP_FGT  = {'theater_name': 'Theatre', 'theatre_name': 'Theatre',
                       'dma_name': 'DMA', 'status': 'Action', 'title': 'Film',
                       'city': 'City', 'state': 'State'}
    _th_idx_fgt = next((i for i, v in enumerate(first_vals)
                        if v.lower() in ('theater_name', 'theatre_name')), None)
    if _th_idx_fgt is not None:
        _ss_fgt = _th_idx_fgt
        while _ss_fgt > 0 and first_vals[_ss_fgt - 1].lower() in _SNAKE_KEYS_FGT:
            _ss_fgt -= 1
        _snake_hdrs_fgt = []
        _si_fgt = _ss_fgt
        while _si_fgt < len(first_vals) and first_vals[_si_fgt].lower() in _SNAKE_KEYS_FGT:
            _snake_hdrs_fgt.append(_SNAKE_MAP_FGT.get(first_vals[_si_fgt].lower(), first_vals[_si_fgt]))
            _si_fgt += 1
        _n_sn_fgt = len(_snake_hdrs_fgt)
        _data_sn_fgt = first_vals[_si_fgt:]
        _rows_sn_fgt = []
        for _sj in range(0, len(_data_sn_fgt), _n_sn_fgt):
            _chunk = list(_data_sn_fgt[_sj : _sj + _n_sn_fgt])
            if len(_chunk) < _n_sn_fgt:
                _chunk += [''] * (_n_sn_fgt - len(_chunk))
            _rows_sn_fgt.append(_chunk)
        if _rows_sn_fgt:
            return pd.DataFrame(_rows_sn_fgt, columns=_snake_hdrs_fgt, dtype=str)

    # ── Cinemark DMA / City / Theatre / Title / Print / Attributes / Status / Detail ─
    # Detected when the first 4 non-blank values are: DMA, City, Theatre, Title.
    # The CSV has a blank line between every individual value (cell separator), so
    # record boundaries are detected by matching DMA-pattern values in cell_values.
    if (len(first_vals) >= 4
            and first_vals[0].lower() == 'dma'
            and first_vals[1].lower() == 'city'
            and first_vals[2].lower() in ('theatre', 'theater')
            and first_vals[3].lower() == 'title'):
        _DMA_PAT_FGT = _re.compile(r'.+ - .+|.+,\s*[A-Z]{2}$')
        _cv_fgt = []
        for _ln_fgt in raw.splitlines():
            _s_fgt = _ln_fgt.strip()
            if _s_fgt:
                _cv_fgt.append(_s_fgt)
            elif len(_ln_fgt) > 0:
                _cv_fgt.append('')
        _data_fgt = _cv_fgt[8:]
        _dma_pos_fgt = [_i for _i, _v in enumerate(_data_fgt)
                        if _v and _DMA_PAT_FGT.match(_v)]
        if not _dma_pos_fgt and _data_fgt:
            _dma0_fgt = next((_v for _v in _data_fgt if _v), '')
            _dma_pos_fgt = [_i for _i, _v in enumerate(_data_fgt) if _v == _dma0_fgt]
        _rows_d2: list[list[str]] = []
        for _ri2, _dp2 in enumerate(_dma_pos_fgt):
            _rend2 = _dma_pos_fgt[_ri2 + 1] if _ri2 + 1 < len(_dma_pos_fgt) else len(_data_fgt)
            _rv2 = _data_fgt[_dp2:_rend2]
            _nb2 = [_v for _v in _rv2 if _v]
            if len(_nb2) < 3:
                continue
            _dma2, _city2, _th2 = _nb2[0], _nb2[1], _nb2[2]
            _nbc2, _skip2 = 0, len(_rv2)
            for _k2, _cv2 in enumerate(_rv2):
                if _cv2:
                    _nbc2 += 1
                    if _nbc2 == 3:
                        _skip2 = _k2 + 1
                        break
            _fv2 = list(_rv2[_skip2:])
            while len(_fv2) % 5:
                _fv2.append('')
            for _fi2 in range(0, len(_fv2), 5):
                _ttl2, _, _, _sta2, _dtl2 = _fv2[_fi2:_fi2 + 5]
                if not _ttl2 and not _sta2:
                    continue
                _rows_d2.append([_th2, _city2, _ttl2, _sta2, _dtl2])
        if _rows_d2:
            return pd.DataFrame(_rows_d2,
                                columns=['Theatre', 'City', 'Film', 'Action', 'Terms'],
                                dtype=str)

    # ── ComScore booking: Theatre # / ComScore Name / City / ST / Screens / DMA ──
    # The "Theatre #" value IS the Comscore unit number — used for direct master lookup.
    # Theatre names include "(City, ST)" / "(date)" suffixes — stripped before lookup.
    if (len(first_vals) >= 4
            and first_vals[0].lower() in ('theatre #', 'theater #')
            and first_vals[2].lower() == 'city'
            and first_vals[3].lower() == 'st'):
        _cv_csc: list[str] = []
        for _ln_csc in raw.splitlines():
            if _ln_csc.strip():
                _cv_csc.append(_ln_csc.strip())
            elif len(_ln_csc) > 0:
                _cv_csc.append('')
        _hdrs_csc2 = ['Unit', 'Theatre', 'City', 'State', 'Screens', 'DMA', 'Action']
        _data_csc2 = _cv_csc[6:]
        _id_pos_csc2 = [_i for _i, _v in enumerate(_data_csc2)
                        if _re.fullmatch(r'\d{3,}', _v)]
        _rows_csc2: list[list[str]] = []
        for _idx_csc2, _pos_csc2 in enumerate(_id_pos_csc2):
            _end_csc2 = (_id_pos_csc2[_idx_csc2 + 1] if _idx_csc2 + 1 < len(_id_pos_csc2)
                         else len(_data_csc2))
            _row_csc2 = list(_data_csc2[_pos_csc2:_end_csc2])
            if len(_row_csc2) < 7:
                _row_csc2 += [''] * (7 - len(_row_csc2))
            _d_csc2 = dict(zip(_hdrs_csc2, _row_csc2[:7]))
            _th_csc2 = _re.sub(r'\s*\([^)]*\)', '', _d_csc2['Theatre']).strip()
            _rows_csc2.append([_d_csc2['Unit'], _th_csc2, _d_csc2['City'], _d_csc2['Action']])
        if _rows_csc2:
            return pd.DataFrame(_rows_csc2,
                                columns=['Unit', 'Theatre', 'City', 'Action'],
                                dtype=str)

    # ── Bare Cinemark format (no underscores — email clients strip __ markers) ─
    _CINEMARK_BARE_FGT = {'DMA', 'SALES', '#', 'THEATRE', 'THEATER', 'SCR', 'SCREENS',
                          'CHAIN', 'CIRCUIT', 'BRCH', 'BRANCH'}
    _NAME_MAP_FGT = {'SALES': 'Buyer', 'THEATRE': 'Theatre', 'THEATER': 'Theatre',
                     'SCR': 'Screens', '#': 'Unit', 'DMA': 'DMA', 'BRCH': 'Branch',
                     'BRANCH': 'Branch', 'SCREENS': 'Screens'}
    # ── Landmark "Location" format: 2-column vertical (Theatre / Status) ────────
    # Film title may appear as preamble before the "Location" header.
    # Data may be one-per-line alternating pairs OR tab/comma-separated rows.
    _loc_idx_fgt = next((i for i, v in enumerate(first_vals[:8]) if v.lower() == 'location'), None)
    if _loc_idx_fgt is not None:
        _film_fgt = first_vals[0] if _loc_idx_fgt > 0 else ''
        _data_fgt_lm = first_vals[_loc_idx_fgt + 1:]
        _rows_fgt_lm: list[list[str]] = []
        # Detect inline separator (tab or comma embedded in values)
        _sep_fgt = None
        for _sv_fgt in _data_fgt_lm[:4]:
            if '\t' in _sv_fgt:
                _sep_fgt = '\t'; break
            if ',' in _sv_fgt:
                _sep_fgt = ','; break
        def _lm_row_fgt(th, st):
            _al = st.lower()
            if 'closed' in _al and 'no opening' in _al:
                return None
            _act = 'Final' if 'finished' in _al else 'Hold'
            return [th, _film_fgt, _act, st]
        if _sep_fgt:
            for _entry_fgt in _data_fgt_lm:
                _parts_fgt = _entry_fgt.split(_sep_fgt, 1)
                _r = _lm_row_fgt(_parts_fgt[0].strip(),
                                  _parts_fgt[1].strip() if len(_parts_fgt) > 1 else '')
                if _r:
                    _rows_fgt_lm.append(_r)
        else:
            for _fi_fgt in range(0, len(_data_fgt_lm), 2):
                _r = _lm_row_fgt(_data_fgt_lm[_fi_fgt],
                                  _data_fgt_lm[_fi_fgt + 1] if _fi_fgt + 1 < len(_data_fgt_lm) else '')
                if _r:
                    _rows_fgt_lm.append(_r)
        if _rows_fgt_lm:
            return pd.DataFrame(_rows_fgt_lm,
                                columns=['Theatre', 'Film', 'Action', 'Phrase'],
                                dtype=str)

    # Parse preserving space-only lines as empty cell values
    cell_values_fgt = []
    for line in raw.splitlines():
        stripped = line.strip()
        if stripped:
            cell_values_fgt.append(stripped)
        elif len(line) > 0:
            cell_values_fgt.append('')
        # else: truly empty → separator, skip
    bare_start_fgt = None
    for _i, _v in enumerate(cell_values_fgt):
        if _v.upper() in _CINEMARK_BARE_FGT:
            _subsequent = [cell_values_fgt[_j] for _j in range(_i + 1, min(_i + 6, len(cell_values_fgt)))]
            if any(_s.upper() in _CINEMARK_BARE_FGT for _s in _subsequent):
                bare_start_fgt = _i
                break
    if bare_start_fgt is not None:
        _headers_fgt = []
        _blank_count = 0
        _i = bare_start_fgt
        while _i < len(cell_values_fgt):
            _v = cell_values_fgt[_i]
            if _v.upper() in _CINEMARK_BARE_FGT or _v == '#':
                _headers_fgt.append(_NAME_MAP_FGT.get(_v.upper(), _v))
                _blank_count = 0
                _i += 1
            elif _v == '' and _blank_count < 2:
                _headers_fgt.append('Action' if _blank_count == 0 else 'Terms')
                _blank_count += 1
                _i += 1
            else:
                break
        # Always ensure Action and Terms columns exist
        if 'Action' not in _headers_fgt:
            _headers_fgt.append('Action')
        if 'Terms' not in _headers_fgt:
            _headers_fgt.append('Terms')
        _n_fgt = len(_headers_fgt)
        _data_fgt = cell_values_fgt[_i:]

        # Find theatre column offset for row-boundary detection
        _th_col_fgt = next((h for h in _headers_fgt if h in ('Theatre', 'Theater')), None)
        _th_off_fgt = _headers_fgt.index(_th_col_fgt) if _th_col_fgt is not None else None
        _THEATRE_RE_FGT = _re.compile(r'\([^)]*,\s*[A-Z]{2}\)', _re.IGNORECASE)

        if _th_off_fgt is not None and _blank_count == 0:
            # No blank separators → variable-length rows; anchor on Theatre "(City, ST)"
            _th_pos_list = [_j for _j, _v2 in enumerate(_data_fgt) if _THEATRE_RE_FGT.search(_v2)]
            _rows_fgt = []
            for _idx, _th_p in enumerate(_th_pos_list):
                _rs = _th_p - _th_off_fgt
                if _rs < 0:
                    continue
                if _idx + 1 < len(_th_pos_list):
                    _re_end = _th_pos_list[_idx + 1] - _th_off_fgt
                else:
                    _re_end = len(_data_fgt)
                _row_data = list(_data_fgt[_rs : _re_end])
                if len(_row_data) < _n_fgt:
                    _row_data += [''] * (_n_fgt - len(_row_data))
                _rows_fgt.append(_row_data[:_n_fgt])
        else:
            # Blank separators present → fixed-length rows
            _rows_fgt = []
            for _j in range(0, len(_data_fgt), _n_fgt):
                _chunk = list(_data_fgt[_j:_j + _n_fgt])
                if len(_chunk) < _n_fgt:
                    _chunk += [''] * (_n_fgt - len(_chunk))
                _rows_fgt.append(_chunk)
        if _rows_fgt:
            return pd.DataFrame(_rows_fgt, columns=_headers_fgt, dtype=str)

    # ── Small-exhibitor city+state format: "City, State   HOLD/Final" ──────────
    _CS_RE_FGT = _re.compile(r'^(.*\S)\s+(HOLD|FINAL|OPEN|CONFIRMED)\s*$', _re.IGNORECASE)
    _SS_RE_FGT = _re.compile(r'^(.*?),?\s*([A-Z]{2})\s*$')
    _nonempty_fgt = [l.strip() for l in raw.splitlines() if l.strip()]
    if _nonempty_fgt:
        _cs_hits_fgt = sum(1 for _l in _nonempty_fgt if _CS_RE_FGT.match(_l))
        if _cs_hits_fgt / len(_nonempty_fgt) >= 0.70:
            _cs_rows = []
            for _line in _nonempty_fgt:
                _cm = _CS_RE_FGT.match(_line)
                if not _cm:
                    continue
                _loc    = _cm.group(1).strip()
                _stat   = _cm.group(2).strip()
                _action = 'Final' if 'final' in _stat.lower() else 'Hold'
                _cs_rows.append([_loc, _action])   # full "City, ST" → 2 lookup words
            if _cs_rows:
                return pd.DataFrame(_cs_rows,
                                    columns=['Theatre', 'Action'],
                                    dtype=str)

    # ── "THEATRE" single-header + alternating name/action format ──────────────
    # e.g. preamble lines "David", "Solo Mio" then "THEATRE" header, then
    # alternating: TheatreName(City,ST) / Action [/ Action2 for film2] ...
    # Theatre lines identified by "(City, ST)" suffix; everything else = action.
    # Variant: after "THEATRE" there may be extra header cols like "DMA", "2/6",
    # "12/19" (date columns grouping preamble films); DMA "City, ST" rows skipped.
    import re as _re_th
    _th_hdr_idx_fgt = None
    _fv_fgt = [l.strip() for l in raw.splitlines() if l.strip()]
    _CSTP_fgt = _re_th.compile(r'\([^,)]+,\s*[A-Z]{2}\)\s*$')
    for _thi_fgt, _thv_fgt in enumerate(_fv_fgt[:8]):
        if _thv_fgt.lower() in ('theatre', 'theater'):
            # Look ahead up to 8 values for a (City, ST) line
            if any(_CSTP_fgt.search(_v) for _v in _fv_fgt[_thi_fgt + 1:_thi_fgt + 10]):
                _th_hdr_idx_fgt = _thi_fgt
                break
    if _th_hdr_idx_fgt is not None:
        _preamble_fgt  = _fv_fgt[:_th_hdr_idx_fgt]
        _data_fgt2     = _fv_fgt[_th_hdr_idx_fgt + 1:]
        _CSEX_fgt      = _re_th.compile(r'\(([^,)]+,\s*[A-Z]{2})\)\s*$')
        # DMA "City, ST" lines (no parens) that follow each theatre row — skip them
        _DMA_PAT_fgt   = _re_th.compile(r'^[^()]+,\s*[A-Z]{2}\s*$')
        # Date column headers like "2/6", "12/19" that precede first theatre line
        _DATE_PAT_fgt  = _re_th.compile(r'^\d{1,2}/\d{1,2}$')
        _pre_th_fgt = []
        for _v in _data_fgt2:
            if _CSTP_fgt.search(_v):
                break
            _pre_th_fgt.append(_v)
        _date_cols_fgt = [v for v in _pre_th_fgt if _DATE_PAT_fgt.match(v)]
        _ndcols_fgt    = len(_date_cols_fgt)
        _grp_fgt       = max(1, len(_preamble_fgt) // _ndcols_fgt) if _ndcols_fgt > 0 else 0
        _blocks_fgt, _cn_fgt, _ca_fgt = [], None, []
        for _v in _data_fgt2:
            if _CSTP_fgt.search(_v):
                if _cn_fgt is not None:
                    _blocks_fgt.append((_cn_fgt, _ca_fgt))
                _cn_fgt, _ca_fgt = _v, []
            elif _cn_fgt is not None and not _DMA_PAT_fgt.match(_v):
                _ca_fgt.append(_v)
        if _cn_fgt is not None:
            _blocks_fgt.append((_cn_fgt, _ca_fgt))
        _rows_fgt2 = []
        for _nm_f, _acts_f in _blocks_fgt:
            if not _acts_f:
                continue
            _cme_f   = _CSEX_fgt.search(_nm_f)
            _city_f  = _cme_f.group(1).strip() if _cme_f else ""
            _clean_f = _CSEX_fgt.sub("", _nm_f).strip()
            def _emit_fgt(film, act):
                _al = act.lower()
                if 'final' in _al:
                    _af = 'Final'
                elif 'hold' in _al:
                    _af = 'Hold'
                else:
                    return
                _rows_fgt2.append([_clean_f, _city_f, film, _af, act])
            if _ndcols_fgt > 0 and _grp_fgt > 0:
                for _ai, _act_f in enumerate(_acts_f):
                    _fs = _ai * _grp_fgt
                    _fe = (_ai + 1) * _grp_fgt if _ai < _ndcols_fgt - 1 else len(_preamble_fgt)
                    for _film_f in _preamble_fgt[_fs:_fe]:
                        _emit_fgt(_film_f, _act_f)
            elif len(_preamble_fgt) >= 2 and len(_acts_f) >= 2:
                for _film_f, _act_f in zip(_preamble_fgt, _acts_f):
                    _emit_fgt(_film_f, _act_f)
            else:
                for _film_f in (_preamble_fgt or [""]):
                    _emit_fgt(_film_f, _acts_f[0])
        if _rows_fgt2:
            print(f"  [theatre-hdr] parsed {len(_rows_fgt2)} rows", flush=True)
            return pd.DataFrame(_rows_fgt2,
                                columns=['Theatre', 'City', 'Film', 'Action', 'Terms'],
                                dtype=str)

    # ── Headerless "Theatre  Film  Action" 3-column format ─────────────────
    # Some bookers send a plain 3-column sheet with no headers:
    # short theatre/city name | film title | Hold/Final/Open
    # Columns separated by 2+ spaces or a tab.
    import re as _re_ma
    _SPLIT_MA_FGT = _re_ma.compile(r'\t|\s{2,}')
    _HFO_MA_FGT   = {'hold', 'final', 'open'}
    _ma_samp_fgt  = [_SPLIT_MA_FGT.split(v.strip()) for v in first_vals[:8]]
    _ma_hits_fgt  = [c for c in _ma_samp_fgt if len(c) == 3 and c[2].strip().lower() in _HFO_MA_FGT]
    if len(_ma_hits_fgt) >= 2 and len(_ma_hits_fgt) >= len(_ma_samp_fgt) * 0.6:
        _rows_ma_fgt = []
        for _v in first_vals:
            _cols_ma = _SPLIT_MA_FGT.split(_v.strip())
            if len(_cols_ma) < 3:
                continue
            _act_ma = _cols_ma[2].strip().lower()
            if _act_ma not in ('hold', 'final'):
                continue
            _rows_ma_fgt.append([_cols_ma[0].strip(), _cols_ma[1].strip(),
                                  'Final' if _act_ma == 'final' else 'Hold'])
        if _rows_ma_fgt:
            print(f"  [ma-3col] parsed {len(_rows_ma_fgt)} rows", flush=True)
            return pd.DataFrame(_rows_ma_fgt, columns=['Theatre', 'Film', 'Action'], dtype=str)

    # ── standard formats below ─────────────────────────────────────────────
    values = [l.strip() for l in raw.splitlines() if l.strip()]
    if not values:
        return None

    # Find "Action" or "Policy" and include any trailing known column headers
    TRAILING_HEADERS = {"showtimes", "showtime", "notes", "comments", "format",
                        "screens", "auditorium"}
    n_cols = None
    for i, v in enumerate(values):
        if v.lower() in ("action", "policy"):
            n_cols = i + 1
            j = i + 1
            while j < len(values) and values[j].lower() in TRAILING_HEADERS:
                n_cols = j + 1
                j += 1
            break
    if n_cols is None:
        return None

    headers   = values[:n_cols]
    remainder = values[n_cols:]
    if not remainder:
        return None

    # If the first column is a numeric ID (ComScore #, Unit #, etc.) use
    # ID-based row detection: each new row starts with a standalone 3+ digit number.
    # This correctly handles rows where trailing empty cells (e.g. Showtimes)
    # are absent rather than represented by a blank line.
    first_col_is_id = any(
        p in headers[0].lower() for p in ("comscore", "unit", "#")
    )
    if first_col_is_id:
        id_positions = [
            i for i, v in enumerate(remainder)
            if _re.fullmatch(r'\d{3,}', v)
        ]
        if id_positions:
            rows = []
            for idx, pos in enumerate(id_positions):
                next_pos = id_positions[idx + 1] if idx + 1 < len(id_positions) else len(remainder)
                row = list(remainder[pos:next_pos])
                if len(row) < n_cols:
                    row += [""] * (n_cols - len(row))
                rows.append(row[:n_cols])
            return pd.DataFrame(rows, columns=headers, dtype=str)

    # Fallback: fixed-size chunking (original booking format — all rows full)
    rows = []
    for i in range(0, len(remainder), n_cols):
        chunk = remainder[i : i + n_cols]
        if len(chunk) < n_cols:
            chunk += [""] * (n_cols - len(chunk))
        rows.append(chunk)

    return pd.DataFrame(rows, columns=headers, dtype=str)


def load_final_locations(csv_path: str) -> list[dict]:
    raw = Path(csv_path).read_text(encoding="utf-8", errors="replace")

    # Skip title rows like "Angel Studios Inc." before the real header.
    # Exception: if the file starts with __COLUMN__ format, don't trim at all —
    # the one-per-line parser handles that format from the very first header.
    import re as _re_lfl
    first_stripped = [l.strip() for l in raw.splitlines() if l.strip()]
    _CINEMARK_BARE_LFL = {'DMA', 'SALES', '#', 'THEATRE', 'THEATER', 'SCR', 'SCREENS',
                          'CHAIN', 'CIRCUIT', 'BRCH', 'BRANCH'}
    # Dunder format may have preamble lines (account name, film title) before headers
    _dunder_idx = next((i for i, v in enumerate(first_stripped[:15])
                        if _re_lfl.fullmatch(r'__.*__', v)), None)
    _is_dunder = _dunder_idx is not None
    # Bare Cinemark: detect by known column names (counts as dunder for action-blank handling)
    if not _is_dunder:
        _bare_idx = next((i for i, v in enumerate(first_stripped[:15])
                          if v.upper() in _CINEMARK_BARE_LFL), None)
        if _bare_idx is not None:
            _subs = [first_stripped[j] for j in range(_bare_idx + 1, min(_bare_idx + 6, len(first_stripped)))]
            if any(s.upper() in _CINEMARK_BARE_LFL for s in _subs):
                _is_dunder = True   # treat bare Cinemark same as dunder for blank-action handling
    if _is_dunder and _dunder_idx is not None and _dunder_idx > 0:
        # Trim raw to start at the first __COLUMN__ line
        target = first_stripped[_dunder_idx]
        lines_all = raw.splitlines(keepends=True)
        for _li, _ln in enumerate(lines_all):
            if _ln.strip() == target:
                raw = "".join(lines_all[_li:])
                break
    # Save raw BEFORE preamble stripping — AMC preamble contains the format marker
    # ('AMC Film Programmer') which gets stripped before df parsing begins.
    _raw_pre_strip = raw

    if not _is_dunder:
        HEADER_KEYS = {"action", "policy", "theatre", "theater", "buyer", "br", "film",
                       "attraction", "unit", "comscore", "dma_name", "status"}
        lines = raw.splitlines(keepends=True)
        for i, line in enumerate(lines[:5]):
            # Word-token split so "dma" inside "landmark" doesn't fire
            _words = set(_re_lfl.split(r'[\s,\t]+', line.lower().strip()))
            if _words & HEADER_KEYS:
                if i > 0:
                    raw = "".join(lines[i:])
                break

    df = None

    # 1. Try tab-separated (most common from copying browser tables)
    # 2. Try comma-separated (CSV exports)
    for sep in ["\t", ","]:
        try:
            candidate = pd.read_csv(io.StringIO(raw), dtype=str, sep=sep)
            candidate.columns = [c.strip() for c in candidate.columns]
            cols_lower = [c.lower() for c in candidate.columns]
            if "action" in cols_lower or "policy" in cols_lower or "status" in cols_lower:
                df = candidate
                break
        except Exception:
            continue

    # 1b. Cinemark "Theater # / Name (City, State)" format with film-title preamble.
    # The real header row is preceded by 1-4 preamble lines (film names, blanks).
    # Detected when a row contains "Theater #" or "Theatre #" as its first cell.
    # Also works on raw data before preamble stripping (uses skiprows loop).
    if df is None:
        import re as _re2
        # First, search ALL lines for a "Theater #" header in case preamble stripping
        # moved the starting position away from the real header.
        _raw_lines_1b = raw.splitlines()
        _hdr_skip_1b = None
        for _li_1b, _ln_1b in enumerate(_raw_lines_1b[:12]):
            _first_cell = _ln_1b.split('\t')[0].strip().lower()
            if _first_cell in ("theater #", "theatre #"):
                _hdr_skip_1b = _li_1b
                break
        # If not found via line scan, also try the raw file directly (preamble intact)
        _raw_for_1b = Path(csv_path).read_text(encoding="utf-8", errors="replace")
        if _hdr_skip_1b is None:
            for _li_1b, _ln_1b in enumerate(_raw_for_1b.splitlines()[:12]):
                _first_cell = _ln_1b.split('\t')[0].strip().lower()
                if _first_cell in ("theater #", "theatre #"):
                    _hdr_skip_1b = _li_1b
                    # Use the original raw for parsing (preamble still intact)
                    _raw_lines_1b = _raw_for_1b.splitlines()
                    break
        if _hdr_skip_1b is not None:
            try:
                _raw_1b = "\n".join(_raw_lines_1b[_hdr_skip_1b:])
                _cand = pd.read_csv(io.StringIO(_raw_1b), dtype=str, sep="\t", header=0)
                _cand.columns = [c.strip() for c in _cand.columns]
                print(f"  [1b] found Theater# header at line {_hdr_skip_1b}, cols={list(_cand.columns)}", flush=True)

                # ── Detect one-per-line variant ──────────────────────────────
                # When data has no tabs, read_csv gives only 1 column.
                # In that case parse as one-value-per-line (each cell on its own row).
                if len(_cand.columns) == 1:
                    print(f"  [1b] single-column → switching to one-per-line parse", flush=True)
                    # All non-empty values from the header row onward
                    _opl_vals = [l.strip() for l in _raw_lines_1b[_hdr_skip_1b:]
                                 if l.strip()]
                    # Collect header names: consecutive non-numeric lines at the start
                    _opl_headers = []
                    _opl_data_start = 0
                    for _oi, _ov in enumerate(_opl_vals):
                        # First purely-numeric value signals start of data rows
                        if _re2.fullmatch(r'\d+', _ov):
                            _opl_data_start = _oi
                            break
                        _opl_headers.append(_ov)
                    else:
                        _opl_data_start = len(_opl_vals)  # no numeric found
                    # Deduplicate column names (e.g. two "Regular" cols → "Regular", "Regular.1")
                    _seen_h = {}
                    _deduped = []
                    for _h in _opl_headers:
                        if _h in _seen_h:
                            _seen_h[_h] += 1
                            _deduped.append(f"{_h}.{_seen_h[_h]}")
                        else:
                            _seen_h[_h] = 0
                            _deduped.append(_h)
                    _opl_headers = _deduped
                    print(f"  [1b-opl] headers={_opl_headers}, data_start={_opl_data_start}", flush=True)
                    _n_1b = len(_opl_headers)
                    _opl_data = _opl_vals[_opl_data_start:]
                    if _n_1b >= 2 and _opl_data:
                        _rows_1b = []
                        # Prefer position-based chunking when values divide evenly —
                        # avoids confusing screen-count numbers (8, 14, 16…) with
                        # theater unit IDs (always 3+ digits).
                        if len(_opl_data) % _n_1b == 0:
                            _rows_1b = [list(_opl_data[i:i + _n_1b])
                                        for i in range(0, len(_opl_data), _n_1b)]
                            print(f"  [1b-opl] position-based: {len(_rows_1b)} rows", flush=True)
                        else:
                            # Fallback: ID-based using 3+ digit numbers only (skips
                            # small values like screen counts that are ≤2 digits).
                            _id_pos_1b = [i for i, v in enumerate(_opl_data)
                                          if _re2.fullmatch(r'\d{3,}', v)]
                            for _ri, _rpos in enumerate(_id_pos_1b):
                                _next = (_id_pos_1b[_ri + 1] if _ri + 1 < len(_id_pos_1b)
                                         else len(_opl_data))
                                _row = list(_opl_data[_rpos:_next])
                                _row += [''] * max(0, _n_1b - len(_row))
                                _rows_1b.append(_row[:_n_1b])
                            print(f"  [1b-opl] id-based (3+ digits): {len(_rows_1b)} rows", flush=True)
                        if _rows_1b:
                            _cand = pd.DataFrame(_rows_1b, columns=_opl_headers, dtype=str)
                            print(f"  [1b-opl] parsed {len(_cand)} rows, cols={list(_cand.columns)}", flush=True)
                        else:
                            print(f"  [1b-opl] no rows found", flush=True)
                    else:
                        print(f"  [1b-opl] not enough headers or data", flush=True)
                # ── End one-per-line variant ─────────────────────────────────

                # Rename Theater # → _cinemark_id (NOT "Unit" — Cinemark's internal
                # theater numbers are NOT Comscore/Rentrak IDs; using them as unit keys
                # causes wrong-theatre lookups, e.g. #348 → AMC Montgomery 16).
                # Name-based lookup (_name_lookup_fallback) handles these correctly.
                _rename_1b = {}
                for _c in _cand.columns:
                    _cl = _c.lower().strip()
                    if _cl in ("theater #", "theatre #"):
                        _rename_1b[_c] = "_cinemark_id"
                # Rename the venue-name column → Theatre
                # Accept: starts with "name", "theatre", "theater", "location" OR use col[1]
                _theatre_col_found_1b = False
                for _c in _cand.columns:
                    if _rename_1b.get(_c):
                        continue
                    _cl = _c.lower().strip()
                    if (_cl.startswith(("name", "theatre", "theater", "location"))
                            or "city" in _cl or "venue" in _cl):
                        _rename_1b[_c] = "Theatre"
                        _theatre_col_found_1b = True
                        break
                if not _theatre_col_found_1b and len(_cand.columns) > 1:
                    _col1 = _cand.columns[1]
                    if _col1 not in _rename_1b.values():
                        _rename_1b[_col1] = "Theatre"
                _cand = _cand.rename(columns=_rename_1b)
                print(f"  [1b] after rename, cols={list(_cand.columns)}", flush=True)
                # Extract city from "(City, ST)" suffix in Theatre column
                _city_pat_1b = _re2.compile(r'\(([^,)]+),\s*[A-Z]{2}\)\s*$')
                if "Theatre" in _cand.columns:
                    _cand["City"] = _cand["Theatre"].fillna("").apply(
                        lambda v: (_city_pat_1b.search(v).group(1).strip()
                                   if _city_pat_1b.search(v) else ""))
                    _cand["Theatre"] = _cand["Theatre"].apply(
                        lambda v: _city_pat_1b.sub("", v).strip())
                # Identify film status columns: any col not in the standard info set
                _SKIP_COLS_1b = {"Unit", "Theatre", "City", "DMA", "Screens", "Contact",
                                  "Screen", "Chain", "Circuit", "Branch"}
                _BOOKING_VALS_1b = {'final', 'hold', 'clean', 'confirmed', 'mat', '-', ''}
                _film_cols_1b = []
                for _ac in _cand.columns:
                    if _ac in _SKIP_COLS_1b:
                        continue
                    _cl_ac = _ac.lower().strip()
                    if any(_cl_ac.startswith(p) for p in ("unnamed", "_film")):
                        continue
                    _v = _cand[_ac].fillna("").str.strip().str.lower()
                    if _v.isin(_BOOKING_VALS_1b).mean() >= 0.5:
                        _film_cols_1b.append(_ac)
                    elif _v[_v != ''].isin(_BOOKING_VALS_1b).all():
                        _film_cols_1b.append(_ac)
                print(f"  [1b] film_cols={_film_cols_1b}", flush=True)
                if _film_cols_1b:
                    def _combine_film_action_1b(row):
                        vals = [str(row[c]).strip().lower() for c in _film_cols_1b]
                        if any(v == 'final' for v in vals):
                            return 'Final'
                        if any(v not in ('', '-') for v in vals):
                            return 'Hold'
                        return '-'
                    _cand['Action'] = _cand.apply(_combine_film_action_1b, axis=1)
                    _cand['_film_cols'] = ','.join(_film_cols_1b)
                    df = _cand
                    print(f"  [1b] df set with {len(df)} rows", flush=True)
                else:
                    print(f"  [1b] no film cols found — skipping format 1b", flush=True)
            except Exception as _e_1b:
                import traceback
                print(f"  [1b] error: {_e_1b}\n{traceback.format_exc()}", flush=True)

    # 3. Try "one value per line" format (this booking system's copy format)
    if df is None:
        df = _parse_one_per_line(raw)

    # 3.5. Try AMC Theatres booking format (before pandas which can't handle variable columns).
    # Use _raw_pre_strip for detection — preamble stripper above removes 'AMC Film Programmer'.
    # Use raw (stripped) for parsing — data rows with Opening anchors are still present.
    _amc_opening_pat = re.compile(r'\b\d+\s+Opening\s*[-–]\s*\d{1,2}/\d{1,2}/\d{4}')
    if df is None and (
        'AMC Film Programmer' in _raw_pre_strip
        or _amc_opening_pat.search(_raw_pre_strip)
    ):
        import re as _re_amc
        _DMA_RE_amc = _re_amc.compile(
            r'\b([A-Z]{3}[A-Z0-9\-&\/]*(?:\s+[A-Z]{3}[A-Z0-9\-&\/]*)*)'
            r'(?:\s*\([^)]*\))?'
            r'(?:,\s*[A-Z]{2})?'
            r'\s+(?=[A-Z][a-z])'
        )
        _amc_rows = []
        for _line in _raw_pre_strip.splitlines():
            _line = _line.strip()
            if not _line:
                continue
            # Match any AMC change type: gross (may have commas) then action keyword.
            # Handles: "476 Split screen. Final", "565 Holdover", "0 Opening - 04/30/2026",
            #          "1,166 Split screen. Holdover", "151 Final - 02/26/2026"
            _om = _re_amc.search(
                r'\b[\d,]+\s+(?:Split\s+[Ss]creen\.\s+)?(?:Final|Holdover|Opening)\b',
                _line)
            if not _om:
                continue
            _action_str = _om.group(0)
            if _re_amc.search(r'Final', _action_str):
                _action = 'Final'
            elif _re_amc.search(r'Holdover|Split', _action_str, _re_amc.I):
                _action = 'Hold'
            else:
                _action = 'Open'
            _before = _line[:_om.start()].strip()
            _dma_m = _DMA_RE_amc.search(_before)
            if _dma_m:
                _theatre = _before[_dma_m.end():].strip()
            else:
                _theatre = _before
            # Split off any embedded film title after the screen-count number
            _th_film = _re_amc.match(r'^(.+?\b\d+)\s+([A-Z][A-Za-z].+)$', _theatre)
            if _th_film and _re_amc.search(r'[a-z]', _th_film.group(2)):
                _theatre = _th_film.group(1).strip()
            if _theatre and _re_amc.search(r'\d', _theatre):
                _amc_rows.append({'Theatre': _theatre, 'Action': _action})
        if _amc_rows:
            df = pd.DataFrame(_amc_rows)
            print(f"  [AMC format] parsed {len(df)} theatres", flush=True)

    # 4. Last resort: pandas auto-detect
    if df is None:
        try:
            df = pd.read_csv(io.StringIO(raw), dtype=str, sep=None, engine="python")
            df.columns = [c.strip() for c in df.columns]
        except Exception as e:
            sys.exit(f"ERROR: Could not parse pasted data: {e}\n"
                     "Make sure you copy the full table including the header row.")

    # Normalize column aliases → standard names
    _COL_RENAME = {"policy": "Action", "status": "Action",
                   "theater_name": "Theatre", "theatre_name": "Theatre"}
    df = df.rename(columns={c: _COL_RENAME[c.lower()] for c in df.columns if c.lower() in _COL_RENAME})

    col = {c.lower(): c for c in df.columns}

    action_col = col.get("action")
    if not action_col:
        sys.exit(
            f"ERROR: No 'Action' or 'Policy' column found.\nColumns detected: {list(df.columns)}\n"
            "Tip: Copy the full table (including header row) from your booking system."
        )

    action_vals = df[action_col].fillna("").str.strip()
    if _is_dunder:
        # Cinemark __COLUMN__ / bare Cinemark format: blank Action = confirmed booking;
        # also include explicit Hold bookings (on-hold = planned location).
        mask = (action_vals.str.upper().str.contains(r'FINAL|HOLD', na=False)
                | (action_vals == ""))
    else:
        mask = action_vals.str.upper().str.contains(r'FINAL|HOLD', na=False)
    finals = df[mask].copy()

    if finals.empty:
        print("No FINAL/HOLD locations found.")
        return []

    unit_col    = col.get("unit")
    theatre_col = col.get("theatre") or col.get("theater") or col.get("theater_name") or col.get("theatre_name")
    comscore_col = next((c for c in df.columns if "comscore" in c.lower()), None)

    # In ComScore booking format the theatre names are in an "unknown" column
    # (the column named after the film title, e.g. "SOLO MIO")
    KNOWN_COLS = {"unit", "action", "policy", "buyer", "br", "attraction",
                  "showtimes", "showtime", "booking", "week", "date", "format", "notes",
                  "dma", "screens", "screen", "contact", "city", "state", "chain",
                  "circuit", "branch", "_film_cols"}
    if not theatre_col:
        for c in df.columns:
            cl = c.lower().strip()
            if cl not in KNOWN_COLS and not any(p in cl for p in ("comscore", "rentrak", "_film")):
                theatre_col = c
                break

    print(f"  [parse] cols={list(df.columns)}, unit_col={unit_col}, theatre_col={theatre_col}, action_col={action_col}", flush=True)

    # Deduplicate by theatre name — multi-film bookings have one row per film,
    # so the same theatre can appear multiple times (e.g. David=Final + SoloMio=Hold).
    # Keep 'Final' if ANY row for that theatre is Final, otherwise keep first 'Hold'.
    _seen_th: dict[str, dict] = {}
    _city_col_dedup = col.get("city")
    for _rec in finals.to_dict(orient="records"):
        _th_n = (_rec.get(theatre_col) or "").strip().lower() if theatre_col else ""
        _th_c = (_rec.get(_city_col_dedup) or "").strip().lower() if _city_col_dedup else ""
        _th_k = f"{_th_n}|{_th_c}" if _th_n else str(id(_rec))
        _act  = (_rec.get(action_col)  or "").strip().upper() if action_col else ""
        if _th_k not in _seen_th:
            _seen_th[_th_k] = _rec
        elif _act == "FINAL":
            _seen_th[_th_k] = _rec
    _deduped_locs = list(_seen_th.values())

    print(f"Found {len(_deduped_locs)} FINAL location(s):")
    for _rec in _deduped_locs:
        _u = _rec.get(unit_col, "?") if unit_col else (_rec.get(comscore_col, "?") if comscore_col else "?")
        _t = _rec.get(theatre_col, "?") if theatre_col else "?"
        print(f"  Unit {_u}  {_t}  [{_rec.get(action_col, '?')}]")
    return _deduped_locs


# ---------------------------------------------------------------------------
# Step 2: Master list — Unit → Rentrak ID
# ---------------------------------------------------------------------------

def load_master_list(page=None, force_refresh: bool = False) -> dict:
    if not force_refresh and ML_CACHE_PATH.exists():
        import time as _time
        cache_age_days = (_time.time() - ML_CACHE_PATH.stat().st_mtime) / 86400
        if cache_age_days > 7:
            print(f"WARNING: Master list cache is {cache_age_days:.0f} days old — consider running with --refresh-master-list")
        else:
            print(f"Using cached master list: {ML_CACHE_PATH} ({cache_age_days:.1f} days old)")
        return _parse_master_list_csv(ML_CACHE_PATH.read_text(encoding="utf-8"))

    print("Fetching master list from Google Sheets...")
    csv_text = None

    if page is not None:
        try:
            resp = page.request.get(MASTER_LIST_URL)
            if resp.ok:
                csv_text = resp.text()
                print(f"  Fetched via Playwright ({len(csv_text):,} bytes)")
        except Exception as e:
            print(f"  Playwright fetch failed: {e}")

    if csv_text is None:
        try:
            import requests
            r = requests.get(MASTER_LIST_URL, timeout=30)
            if r.ok and "Venue Rentrak ID" in r.text:
                csv_text = r.text
                print(f"  Fetched via requests ({len(csv_text):,} bytes)")
        except Exception as e:
            print(f"  requests fetch failed: {e}")

    if csv_text is None:
        sys.exit(
            "ERROR: Could not fetch master list.\n"
            "Place master_list_cache.csv in the project folder, or run with --no-headless."
        )

    ML_CACHE_PATH.write_text(csv_text, encoding="utf-8")
    print(f"  Cached to {ML_CACHE_PATH}")
    return _parse_master_list_csv(csv_text)

# Known booking-name → master-list-name aliases (exact, case-insensitive after strip).
# Add entries here whenever a booking uses a shortened or different name than Comscore.
VENUE_ALIASES: dict[str, str] = {
    "west chester 18":         "amc west chester township 18",
    "tysons corner center 16": "amc tysons corner 16",
    # ── Cinemark DFW local-name → Comscore master-list name ──────────────────
    "cinemark central plano 10":  "cinemark movies plano 10",
    "cut! by cinemark":           "cinemark cut! 10",
    "cinemark 17":                "cinemark 17 + imax",
    "rave ridgmar 13":            "cinemark ridgmar mall 13 + xd",
    "rave north east mall 18":    "cinemark northeast mall 18 + xd",
    "cinemark cleburne":          "cinemark cinema cleburne 6",
    "cinemark 12 and xd":         "cinemark mansfield 12 + xd",
    "tinseltown grapevine and xd": "cinemark tinseltown grapevine 17 + xd",
    "cinemark 17 + imax":          "cinemark tulsa 17",
    # City-qualified: same booking name used in multiple cities (key = "name, city")
    "cinemark 14, cedar hill":    "cinemark cedar hill 14",
    "movies 14, lancaster":       "cinemark movies lancaster 14",
    "cinemark 14, denton":        "cinemark denton 14",
    "cinemark 12, sherman":       "cinemark sherman 12",
    "movies 8, paris":            "cinemark movies paris 8",
    # ── Brad Bills small-exhibitor city+state → actual venue name ─────────────
    "espanola, nm":            "dreamcatcher 10",
    "espanola nm":             "dreamcatcher 10",
    "espanola":                "dreamcatcher 10",
    "independence, mo":        "pharaoh independence 4",
    "independence mo":         "pharaoh independence 4",
    "guymon, ok":              "northridge guymon 8",
    "guymon ok":               "northridge guymon 8",
    "florence, sc":            "julia florence 4",
    "florence sc":             "julia florence 4",
    "tulsa, ok":               "eton tulsa 6",
    "tulsa ok":                "eton tulsa 6",
    "kirksville, mo":          "downtown kirksville 8",
    "kirksville mo":           "downtown kirksville 8",
    "marion, nc":              "hometown cinemas marion 2",
    "marion nc":               "hometown cinemas marion 2",
    "fulton, mo":              "fulton cinema 8",
    "fulton mo":               "fulton cinema 8",
    "lumberton, nc":           "hometown lumberton 4",
    "lumberton nc":            "hometown lumberton 4",
    "marshall, mo":            "cinema marshall 3",
    "marshall mo":             "cinema marshall 3",
    "milford, ia":             "pioneer milford 1",
    "milford ia":              "pioneer milford 1",
    "parsons, ks":             "the parsons theatre",
    "parsons ks":              "the parsons theatre",
    # City-qualified overrides (raw booking name, city) → master list name
    "cinemark 22 + imax":                 "cinemark lancaster 22",
    "cinemark 22 + imax, lancaster":      "cinemark lancaster 22",
    "cinemark 16 +xd, victorville":       "cinemark victorville 16 + xd",
    "landmark 12 surrey":                 "landmark guildford 12",
    # ── Cinemark Taylor Reynolds circuit (grayed-out venues) ─────────────────
    "las vegas samstown 18":              "cinemark century 18 sam's town (las vegas)",
    "las vegas santa fe station 16 + xd": "cinemark century las vegas santa fe station 16 + xd",
    "sugarhouse movies 10":               "cinemark sugarhouse salt lake city 10",
    "cinemark layton and xd":             "cinemark layton 7 + xd",
    "cinemark west valley + xd":          "cinemark west valley 10 + xd",
    "tucson park place 20 + xd":          "cinemark century park place 20 + xd",
    "century tucson marketplace and xd":  "cinemark century tucson marketplace  14+ xd",
    "imperial valley 14":                 "cinemark century imperial valley mall 14 (elcentro)",
    "cinemark 16, mesa":                  "cinemark mesa 16",
    "cinemark 16, provo":                 "cinemark provo 16",
    "reno parklane 16":                   "cinemark century park lane 16 (reno)",
    # ── Cinemark Pacific NW ───────────────────────────────────────────────────
    "lincoln square cinema with imax":    "cinemark lincoln square cinemas imax 16",
    "lincoln square cinema bistro 6":     "cinemark reserve lincoln square dine-in 6",
    "cinemark totem lake + xd":           "cinemark village at totem lake 8",
    "century walla walla grand cinema 12":"cinemark walla walla grand cinema12",
}

# Direct Rentrak ID overrides — booking name → {rentrak_id, venue}.
# Bypasses the alias+normalize chain for theatres whose names don't resolve cleanly.
# Rentrak IDs verified against master_list_cache.csv.
_RENTRAK_DIRECT: dict[str, dict] = {
    "las vegas samstown 18":              {"rentrak_id": "9023",   "venue": "Cinemark Century 18 Sam's Town (Las Vegas)"},
    "reno parklane 16":                   {"rentrak_id": "8313",   "venue": "Cinemark Century Park Lane 16 (Reno)"},
    "sugarhouse movies 10":               {"rentrak_id": "7922",   "venue": "Cinemark Sugarhouse Salt Lake City 10"},
    "cinemark layton and xd":             {"rentrak_id": "5944",   "venue": "Cinemark Layton 7 + XD"},
    "cinemark west valley + xd":          {"rentrak_id": "991544", "venue": "Cinemark West Valley 10 + XD"},
    "tucson park place 20 + xd":          {"rentrak_id": "9149",   "venue": "Cinemark Century Park Place 20 + XD"},
    "century tucson marketplace and xd":  {"rentrak_id": "991804", "venue": "Cinemark Century Tucson Marketplace  14+ XD"},
    "imperial valley 14":                 {"rentrak_id": "989910", "venue": "Cinemark Century Imperial Valley Mall 14 (ElCentro)"},
    "las vegas santa fe station 16 + xd": {"rentrak_id": "989919", "venue": "Cinemark Century Las Vegas Santa Fe Station 16 + XD"},
    "cinemark 16":                        {"rentrak_id": "990186", "venue": "Cinemark Mesa 16"},   # disambiguated by city below
    "lincoln square cinema with imax":    {"rentrak_id": "990937", "venue": "Cinemark Lincoln Square Cinemas IMAX 16"},
    "lincoln square cinema bistro 6":     {"rentrak_id": "991855", "venue": "Cinemark Reserve Lincoln Square Dine-In 6"},
    "cinemark totem lake + xd":           {"rentrak_id": "992368", "venue": "Cinemark Village at Totem Lake 8"},
    "century walla walla grand cinema 12":{"rentrak_id": "8836",   "venue": "Cinemark Walla Walla Grand Cinema12"},
}
# City-qualified overrides for ambiguous names (e.g. "Cinemark 16" appears in multiple cities)
_RENTRAK_DIRECT_CITY: dict[str, dict] = {
    "cinemark 16|mesa":   {"rentrak_id": "990186", "venue": "Cinemark Mesa 16"},
    "cinemark 16|provo":  {"rentrak_id": "8707",   "venue": "Cinemark Provo 16"},
}


def _normalize_venue(name: str) -> str:
    """Lowercase, strip common chain prefix variants, punctuation, collapse whitespace."""
    n = name.lower().strip()
    n = re.sub(r'^(amc dine-in\s+theatres?|amc dine-in|amc star|amc classic|amc)\s+', '', n)
    n = re.sub(r'^(regal|cinemark|harkins|marcus|showcase|cineworld|amstar|b&b)\s+', '', n)
    n = re.sub(r'[^a-z0-9 ]', ' ', n)   # strip parentheses, commas, +, etc.
    return re.sub(r'\s+', ' ', n).strip()


def _parse_master_list_csv(csv_text: str) -> dict:
    reader = csv.DictReader(io.StringIO(csv_text))
    lookup = {}
    # Also build a name-based lookup for theatres without a Ref ID (e.g. AMC bookings)
    # Key: normalized venue name → entry  (all rentrak entries included)
    _name_lookup: dict[str, dict] = {}
    _name_words:  list[tuple[set, dict, bool]] = []  # (word_set, entry, is_amc)

    for row in reader:
        ref_id  = str(row.get("Exhibitor's Ref ID", "")).strip()
        rentrak = str(row.get("Venue Rentrak ID", "")).strip()
        venue   = str(row.get("Venue", "")).strip()
        if not rentrak or rentrak in ("", "nan"):
            continue
        is_amc = venue.upper().startswith("AMC")
        entry = {"rentrak_id": rentrak, "venue": venue}

        if ref_id and ref_id not in ("", "nan"):
            # Store under exact value, stripped, and zero-padded (4-digit) forms
            lookup[ref_id] = entry
            stripped = ref_id.lstrip("0") or "0"
            lookup[stripped] = entry
            lookup[stripped.zfill(4)] = entry

        # Name-based index (normalized, prefix stripped)
        norm = _normalize_venue(venue)
        _name_lookup[norm] = entry
        _name_words.append((set(norm.split()), entry, is_amc))

    # Attach name indexes so callers can do fallback lookups
    lookup["__name_lookup__"] = _name_lookup  # type: ignore[assignment]
    lookup["__name_words__"]  = _name_words   # type: ignore[assignment]
    print(f"  Master list: {len(_name_lookup)} theatres with Rentrak IDs (name index)")
    return lookup


def _name_lookup_fallback(master_lookup: dict, theatre: str, city: str = "") -> dict | None:
    """
    Try to find a master list entry by venue name when the Unit # is unknown.
    0. Direct Rentrak override dict (city-qualified first, then plain).
    1. Exact normalized match (after stripping AMC prefix variants).
    2. Word-overlap: all words in the booking name appear in the master list name.
    Returns the entry dict or None.
    """
    # 0. Direct Rentrak ID overrides — fastest, most reliable path
    _bk = theatre.lower().strip()
    # Strip trailing "(City, ST)" suffix if still embedded in the theatre name
    # (happens when the booking is parsed as TSV with Theatre/DMA/Action cols
    #  rather than being split into separate Theatre + City columns)
    _bk_clean = re.sub(r'\s*\([^)]+,\s*[a-z]{2}\)\s*$', '', _bk).strip()
    # Use embedded city if no explicit city arg was provided
    _city_from_name = ''
    _city_match = re.search(r'\(([^)]+),\s*([a-z]{2})\)\s*$', _bk)
    if _city_match:
        _city_from_name = _city_match.group(1).strip()
    _effective_city = city.lower().split(',')[0].strip() or _city_from_name
    _city_key = f"{_bk_clean}|{_effective_city}"
    if _city_key in _RENTRAK_DIRECT_CITY:
        return _RENTRAK_DIRECT_CITY[_city_key]
    if _bk_clean in _RENTRAK_DIRECT:
        return _RENTRAK_DIRECT[_bk_clean]
    if _bk in _RENTRAK_DIRECT:
        return _RENTRAK_DIRECT[_bk]

    name_lookup: dict = master_lookup.get("__name_lookup__", {})
    name_words: list  = master_lookup.get("__name_words__", [])

    # Check aliases: database first (user-managed), then hardcoded fallback.
    # Try city-qualified key first for disambiguation, then plain name.
    alias_key = theatre.lower().strip()
    _resolved = False
    if _fgt_db:
        try:
            db_alias = _fgt_db.get_alias(theatre, city)
            if db_alias:
                theatre = db_alias
                _resolved = True
        except Exception:
            pass
    if not _resolved:
        if city:
            city_key = f"{alias_key}, {city.lower().strip()}"
            if city_key in VENUE_ALIASES:
                theatre = VENUE_ALIASES[city_key]
                _resolved = True
        if not _resolved and alias_key in VENUE_ALIASES:
            theatre = VENUE_ALIASES[alias_key]

    norm = _normalize_venue(theatre)

    # 1. Exact normalized match
    if norm in name_lookup:
        return name_lookup[norm]

    # 2. Word overlap: require ALL booking words appear in a master list venue name
    booking_words = set(norm.split())
    if len(booking_words) < 2:
        return None  # Too short to match reliably

    best_entry = None
    best_extra = float("inf")  # prefer fewer extra master words (more specific match)
    for master_words, entry, _is_amc in name_words:
        if booking_words <= master_words:   # all booking words in master name
            extra = len(master_words - booking_words)
            if extra < best_extra:
                best_extra = extra
                best_entry = entry

    if best_entry:
        return best_entry

    # 2.5. State-code-neutral match: strip US/CA 2-letter state codes from booking words.
    #      City+state bookings ("Florence, SC") include a state code that small-exhibitor
    #      master entries omit.  e.g. "Marion, NC" → {"marion"} ⊆ {"hometown","cinemas","marion"}
    _STATE_CODES = frozenset({
        'al','ak','az','ar','ca','co','ct','de','fl','ga','hi','id','il','in','ia','ks',
        'ky','la','me','md','ma','mi','mn','ms','mo','mt','ne','nv','nh','nj','nm','ny',
        'nc','nd','oh','ok','or','pa','ri','sc','sd','tn','tx','ut','vt','va','wa','wv',
        'wi','wy','dc','pr','vi','ab','bc','mb','nb','nl','ns','on','pe','qc','sk',
    })
    no_state_words = {w for w in booking_words if w not in _STATE_CODES}
    if 1 <= len(no_state_words) < len(booking_words):  # only try if state was actually removed
        best25, best_extra25 = None, float("inf")
        for master_words, entry, _ in name_words:
            if no_state_words <= master_words:
                extra = len(master_words - no_state_words)
                if extra < best_extra25:
                    best_extra25 = extra
                    best25 = entry
        if best25:
            return best25

    # 3. Number-stripped word overlap: AMC booking PDFs use internal circuit numbers
    #    e.g. "Forum 30" where "30" is AMC's circuit code, not screen count.
    #    Strip numbers from booking words, match on text words only.
    #    When multiple candidates, prefer AMC venues (since AMC PDFs only list AMC theatres).
    text_words = {w for w in booking_words if not w.isdigit()}
    if len(text_words) < 1:
        return None

    candidates = [
        (entry, is_amc) for master_words, entry, is_amc in name_words
        if text_words <= {w for w in master_words if not w.isdigit()}
    ]
    if len(candidates) == 1:
        return candidates[0][0]
    # Multiple candidates: prefer AMC venues; if exactly one AMC match, use it
    amc_candidates = [e for e, is_amc in candidates if is_amc]
    if len(amc_candidates) == 1:
        return amc_candidates[0]

    # 4. Significant-word match: words with 4+ chars (handles "St" vs "Saint", number variants).
    #    All significant booking words must appear in master name's significant words.
    sig_booking = {w for w in booking_words if len(w) >= 4 and not w.isdigit()}
    if len(sig_booking) >= 2:
        sig_candidates = []
        for master_words, entry, is_amc in name_words:
            sig_master = {w for w in master_words if len(w) >= 4 and not w.isdigit()}
            if sig_booking <= sig_master:
                extra = len(sig_master - sig_booking)
                sig_candidates.append((extra, is_amc, entry))
        if sig_candidates:
            sig_candidates.sort(key=lambda x: (x[0], not x[1]))  # fewest extras first, AMC preferred
            # Only return if unambiguous or clearly best
            if len(sig_candidates) == 1 or sig_candidates[0][0] < sig_candidates[1][0]:
                return sig_candidates[0][2]
            # Tie: prefer AMC if only one AMC match
            amc_sig = [e for _, is_amc, e in sig_candidates if is_amc]
            if len(amc_sig) == 1:
                return amc_sig[0]

    # 5. Chain-neutral match: strip circuit brand names from BOTH sets.
    #    Handles "Universal Cinemark at Citywalk" vs "Cinemark Universal Citywalk 20 + XD"
    #    where the chain name appears mid-string in the booking but as a prefix in master.
    _CIRCUIT_BRANDS = frozenset({
        'cinemark', 'amc', 'regal', 'harkins', 'marcus', 'showcase',
        'cineworld', 'amstar', 'bb', 'universal', 'at', 'and', 'the', 'of',
    })
    neutral_booking = {w for w in booking_words if w not in _CIRCUIT_BRANDS and not w.isdigit()}
    if len(neutral_booking) >= 1:
        best5, best_extra5 = None, float("inf")
        for master_words_5, entry5, _ in name_words:
            neutral_master = {w for w in master_words_5
                              if w not in _CIRCUIT_BRANDS and not w.isdigit()}
            if neutral_booking and neutral_booking <= neutral_master:
                extra = len(neutral_master - neutral_booking)
                if extra < best_extra5:
                    best_extra5 = extra
                    best5 = entry5
        if best5:
            return best5

    print(f"    [name-match MISS] tried: '{norm}'  (words: {sorted(booking_words)})")
    # Print top 3 closest partial matches for diagnostics
    scored = []
    for master_words, entry, _ in name_words:
        overlap = len(booking_words & master_words)
        if overlap > 0:
            scored.append((overlap, entry["venue"]))
    for _, venue in sorted(scored, reverse=True)[:3]:
        print(f"      closest: '{venue}'")

    return None


# ---------------------------------------------------------------------------
# Step 3: Comscore login
# ---------------------------------------------------------------------------

def login_to_comscore(page) -> bool:
    if not COMSCORE_USER or not COMSCORE_PASS:
        sys.exit(
            "ERROR: Comscore credentials not set.\n"
            "Copy .env.example to .env and fill in COMSCORE_USERNAME / COMSCORE_PASSWORD."
        )

    print("\nLogging into Comscore...")
    try:
        page.goto(COMSCORE_BASE, wait_until="domcontentloaded", timeout=60_000)

        # Already logged in?
        if page.locator('text="Logout"').count() > 0 or "login" not in page.url.lower():
            if page.locator('a[href*="manage_account"]').count() > 0:
                print("  Already logged in.")
                return True

        page.fill(
            'input[name="login_id"], input[name="user_name"], input[name="username"], input[type="email"]',
            COMSCORE_USER
        )
        page.fill('input[name="password"], input[type="password"]', COMSCORE_PASS)
        page.click('#beta-supported, input[type="submit"], button[type="submit"]')
        page.wait_for_load_state("domcontentloaded", timeout=60_000)

        # Check if we're still on the login page (login failed) or moved on (success)
        if "login" in page.url.lower():
            print("  ERROR: Login may have failed — check credentials.")
            return False

        print("  Login successful.")
        return True

    except PlaywrightTimeoutError as e:
        print(f"  ERROR: Login timed out. Current URL: {page.url}")
        print(f"  Detail: {e}")
        return False
    except Exception as e:
        print(f"  ERROR: Unexpected login error: {e}")
        return False


# ---------------------------------------------------------------------------
# Step 4: Scrape ALL films per theatre
# ---------------------------------------------------------------------------

def build_flash_url(rentrak_id: str, week_date: str) -> str:
    return (
        f"{COMSCORE_FLASH}"
        f"?theater_no={rentrak_id}"
        f"&pct_change_same_theater_or_total_gross=same_theater"
        f"&day_range_rev={week_date}"
    )

# JavaScript that extracts every film row from the Comscore flash table
_EXTRACT_JS = """
() => {
    // Detect "No data is available at this time." — Comscore shows this when
    // flash data hasn't been published yet (typically before Saturday).
    const pageText = (document.body && document.body.innerText) || '';
    const noData = pageText.includes('No data is available at this time');

    // Headers: find the <table> that owns the .boet-tr-body rows, then read
    // its <thead>.  This is robust regardless of the outer wrapper class name.
    const firstRow = document.querySelector('.boet-tr-body');
    const parentTable = firstRow
        ? firstRow.closest('table')
        : (document.querySelector('.boet-sub-table') || document.querySelector('table'));
    const headers = [];
    if (parentTable) {
        parentTable.querySelectorAll('thead th').forEach(th => {
            const clone = th.cloneNode(true);
            clone.querySelectorAll('button, a').forEach(n => n.remove());
            headers.push(clone.textContent.trim());
        });
    }

    // Data rows — Comscore's flash table uses .boet-tr-body class
    const rows = [];
    document.querySelectorAll('.boet-tr-body').forEach(tr => {
        const cells = Array.from(tr.querySelectorAll('td')).map(td => {
            const link = td.querySelector('a[href*="run_of_engagement"]');
            return link ? link.textContent.trim() : td.textContent.trim();
        });
        if (cells.length > 0 && cells.some(c => c !== '')) rows.push(cells);
    });

    // Data-valid timestamp
    const validEl = document.querySelector('[class*="valid-as-of"], [class*="valid_as_of"], .boet-valid, h2, h3, h4');
    const validText = validEl ? validEl.textContent.trim() : '';

    return { headers, rows, valid_as_of: validText, no_data: noData };
}
"""

def scrape_all_films(page, rentrak_id: str, week_date: str, angel_title: str) -> dict:
    """
    Navigate to a theatre's flash page and return all films as a list.
    Returns:
      {
        films: [{rank, title, dist, gross, weekend_gross, cume, is_angel}, ...],
        valid_as_of: str,
        status: 'ok' | 'error' | 'timeout',
        error: str | None,
        url: str,
      }
    """
    url = build_flash_url(rentrak_id, week_date)
    out = {"films": [], "valid_as_of": "", "status": "error", "error": None, "url": url}

    try:
        page.goto(url, wait_until="domcontentloaded", timeout=45_000)

        # If session expired mid-run, Comscore redirects to the login page.
        # Use a lock so only one worker thread re-logs in at a time.
        if "login" in page.url.lower():
            with _LOGIN_LOCK:
                if "login" in page.url.lower():   # re-check under lock
                    print(f"  Session expired — re-logging in...")
                    if not login_to_comscore(page):
                        out["error"] = "Session expired and re-login failed"
                        return out
            page.goto(url, wait_until="domcontentloaded", timeout=45_000)

        print(f"  Page URL: {page.url}")

        # Poll until Comscore finishes "Preparing Data" (can take 30-45s server-side).
        # IMPORTANT: check "Preparing Data" FIRST — the preparing page may contain
        # hidden "No data available" text that would cause an early exit otherwise.
        raw = {"headers": [], "rows": [], "valid_as_of": "", "no_data": False}
        import time as _time
        _deadline = _time.time() + 90
        _last_row_count = -1
        while _time.time() < _deadline:
            snippet = page.evaluate("() => document.body.innerText.slice(0, 500)")
            if "Preparing Data" in snippet or "Loading" in snippet:
                print(f"  Comscore still preparing, waiting 3s...")
                page.wait_for_timeout(3_000)
                continue
            raw = page.evaluate(_EXTRACT_JS)
            if raw.get("no_data"):
                break
            _cur_count = len(raw.get("rows", []))
            if _cur_count > 0:
                if _cur_count == _last_row_count:
                    break  # row count stable — full table loaded
                _last_row_count = _cur_count
                # Wait briefly and re-check: Comscore may render rows incrementally
                page.wait_for_timeout(1_500)
                continue
            # Page loaded but 0 rows — table may still be rendering; wait and retry
            print(f"  Page loaded but 0 rows — waiting 3s for table to render...")
            page.wait_for_timeout(3_000)
        # One short retry if rows appeared but headers haven't rendered yet
        if raw.get("rows") and not raw.get("headers"):
            page.wait_for_timeout(1000)
            raw = page.evaluate(_EXTRACT_JS)
        if raw.get("no_data"):
            print(f"  Comscore: No data available for this week")

        headers = raw.get("headers", [])
        rows    = raw.get("rows", [])
        out["valid_as_of"] = raw.get("valid_as_of", "")
        print(f"  Page rows extracted: {len(rows)}  headers={headers}")

        if not rows:
            try:
                debug_path = OUTPUT_DIR / f"debug_{rentrak_id}.png"
                page.screenshot(path=str(debug_path))
                print(f"  Debug screenshot saved: {debug_path}")
            except Exception:
                pass
            if raw.get("no_data"):
                out["status"] = "no_data"
                out["error"]  = "No flash data yet — Comscore typically posts Saturday/Sunday"
            else:
                page_snippet = page.evaluate("() => (document.body.innerText || '').slice(0, 500)")
                print(f"  Page text: {page_snippet!r}")
                out["status"] = "no_data"
                out["error"]  = "No table rows found — check debug screenshot"
            return out

        # Map header names → column indices
        h = [s.lower() for s in headers]
        def idx(keyword):
            for i, name in enumerate(h):
                if keyword in name:
                    return i
            return None

        title_idx      = idx("title")
        dist_idx       = idx("distributor") or idx("dist")
        weekend_idx    = idx("weekend gross")
        week_gross_idx = idx("week gross")
        cume_idx       = idx("cume")
        week_num_idx   = idx("week #")

        angel_lower = angel_title.lower().strip()
        films = []
        rank = 0

        for row in rows:
            def get(i):
                if i is not None and i < len(row):
                    val = row[i].replace(",", "").replace("$", "").strip()
                    try:
                        return int(float(val))
                    except ValueError:
                        return row[i].strip() if i < len(row) else None
                return None

            title_raw = row[title_idx].strip() if title_idx is not None and title_idx < len(row) else ""
            dist_raw  = row[dist_idx].strip()  if dist_idx  is not None and dist_idx  < len(row) else ""

            # Skip summary/total rows (no title, or title is "TOTAL")
            if not title_raw or title_raw.upper() == "TOTAL":
                continue

            rank += 1

            # Use Week Gross as primary; fall back to Weekend Gross
            gross          = get(week_gross_idx) or get(weekend_idx) or 0
            weekend_gross  = get(weekend_idx) or 0
            cume           = get(cume_idx) or 0
            week_num       = get(week_num_idx)

            if not isinstance(gross, int):
                gross = 0
            if not isinstance(weekend_gross, int):
                weekend_gross = 0
            if not isinstance(cume, int):
                cume = 0

            films.append({
                "rank":          rank,
                "title":         title_raw,
                "dist":          abbrev_dist(dist_raw),
                "gross":         gross,
                "weekend_gross": weekend_gross,
                "cume":          cume,
                "week_num":      week_num,
                "is_angel":      "angel" in dist_raw.lower() or (bool(angel_lower) and (angel_lower in title_raw.lower() or title_raw.lower() in angel_lower)),
            })

        out["films"]  = films
        out["status"] = "ok"

    except PlaywrightTimeoutError:
        out["status"] = "timeout"
        out["error"]  = "Page load timed out"
    except Exception as e:
        out["status"] = "error"
        out["error"]  = str(e)

    return out


# ---------------------------------------------------------------------------
# Step 5: Orchestrate all FINAL theatres
# ---------------------------------------------------------------------------

def pull_all_theatre_data(
    locations: list[dict],
    master_lookup: dict,
    week_date: str,
    page,
    headless: bool = True,
    is_cdp: bool = False,
) -> list[dict]:

    if not locations:
        return []

    col = lambda key: next((k for k in locations[0] if k.lower() == key), None)
    unit_col          = col("unit")
    theatre_col_named = col("theatre") or col("theater")
    theatre_col       = theatre_col_named
    city_col          = col("city")
    attraction_col    = col("attraction") or col("film") or col("title") or col("movie") or col("production")
    action_col        = col("action")
    buyer_col         = col("buyer")
    br_col            = col("br")
    comscore_col      = next(
        (k for k in locations[0] if "comscore" in k.lower()),
        None
    )
    KNOWN_COLS = {"unit", "action", "policy", "buyer", "br", "attraction",
                  "showtimes", "showtime", "booking", "week", "date", "format", "notes"}
    if not theatre_col:
        for k in locations[0]:
            kl = k.lower().strip()
            if kl not in KNOWN_COLS and not any(p in kl for p in ("comscore", "rentrak")):
                theatre_col = k
                break
    unknown_theatre_col    = theatre_col if not theatre_col_named else None
    film_title_from_header = unknown_theatre_col if (unknown_theatre_col and not attraction_col) else None

    # ── Pass 1: build entries and resolve Rentrak IDs (fast, no network) ──
    entries = []   # (index, entry, needs_scrape)
    for i, loc in enumerate(locations):
        theatre    = str(loc.get(theatre_col, "")).strip() if theatre_col else ""
        city       = str(loc.get(city_col, "")).strip() if city_col else ""
        attraction = str(loc.get(attraction_col, "")).strip() if attraction_col else (film_title_from_header or "")
        action     = str(loc.get(action_col, "")).strip() if action_col else ""
        buyer      = str(loc.get(buyer_col, "")).strip() if buyer_col else ""
        br         = str(loc.get(br_col, "")).strip() if br_col else ""

        comscore_direct = str(loc.get(comscore_col, "")).strip() if comscore_col else ""
        if comscore_direct:
            rentrak_id = comscore_direct
            ml_venue   = theatre
        else:
            unit_key = str(loc.get(unit_col, "")).strip() if unit_col else ""
            ml_entry = master_lookup.get(unit_key)
            if not ml_entry and theatre:
                ml_entry = _name_lookup_fallback(master_lookup, theatre, city=city)
            rentrak_id = ml_entry["rentrak_id"] if ml_entry else None
            ml_venue   = ml_entry["venue"] if ml_entry else theatre

        unit = str(loc.get(unit_col, "")).strip() if unit_col else (comscore_direct or "")

        entry = {
            "unit":           unit,
            "theatre":        theatre,
            "ml_venue":       ml_venue,
            "city":           city,
            "attraction":     attraction,
            "booking_action": action,
            "buyer":          buyer,
            "br":             br,
            "rentrak_id":     rentrak_id or "NOT FOUND",
            "url":            build_flash_url(rentrak_id, week_date) if rentrak_id else None,
        }

        if not rentrak_id:
            src = "Comscore column empty" if comscore_col else "not in master list"
            norm_name = _normalize_venue(theatre) if not comscore_col else ""
            norm_suffix = f'  (normalized: "{norm_name}")' if norm_name else ''
            print(f"  [{unit}] {theatre} — Rentrak ID {src}{norm_suffix}")
            entry.update({
                "films": [], "valid_as_of": "",
                "status": "no_rentrak_id", "error": f"Unit {unit} {src}",
            })
            entries.append((i, entry, False))
        else:
            src = "from Comscore col" if comscore_direct else "from master list"
            print(f"  [{unit}] {theatre}  (Rentrak {rentrak_id}, {src})")
            entries.append((i, entry, True))

    # ── Dedup by Rentrak ID ── same theatre may appear twice if booking sheet
    # has one row per film. Keep entry with 'Final' booking_action if present.
    _rid_idx: dict[str, int] = {}
    _deduped: list = []
    for _et in entries:
        _i2, _e2, _n2 = _et
        _rid2 = _e2.get("rentrak_id") or ""
        if not _rid2 or _rid2 == "NOT FOUND":
            _deduped.append(_et)
        elif _rid2 in _rid_idx:
            _prev = _deduped[_rid_idx[_rid2]][1]
            if (_e2.get("booking_action") or "").upper() == "FINAL":
                _prev["booking_action"] = "Final"
        else:
            _rid_idx[_rid2] = len(_deduped)
            _deduped.append(_et)
    if len(_deduped) < len(entries):
        print(f"  [dedup] {len(entries)} → {len(_deduped)} unique theatres by Rentrak ID", flush=True)
    entries = _deduped

    # ── Pass 2: parallel scrape ───────────────────────────────────────────
    scrape_entries = [(i, entry) for i, entry, needs in entries if needs]
    n_workers = min(MAX_SCRAPE_WORKERS, len(scrape_entries)) if scrape_entries else 1

    # CDP mode shares an existing browser — can't spawn separate instances.
    # Also fall back to sequential when there's only one theatre to scrape.
    if is_cdp or n_workers <= 1:
        print(f"\nScraping {len(scrape_entries)} theatre(s) (sequential)...")
        for i, entry in scrape_entries:
            try:
                scraped = scrape_all_films(
                    page, entry["rentrak_id"], week_date, entry["attraction"]
                )
                entry.update(scraped)
            except Exception as exc:
                entry.update({"films": [], "valid_as_of": "",
                               "status": "error", "error": str(exc)})
        results = []
        for i, entry, needs in entries:
            results.append(entry)
        return results

    # Save the authenticated session so each worker thread can restore it
    # inside its own sync_playwright() instance (Playwright's sync API is
    # greenlet-based and cannot be shared across Python threads).
    storage_state = page.context.storage_state()

    print(f"\nScraping {len(scrape_entries)} theatre(s) with {n_workers} parallel browser(s)...")

    # Distribute entries round-robin across workers so load is balanced.
    batches: list[list[tuple]] = [[] for _ in range(n_workers)]
    for k, item in enumerate(scrape_entries):
        batches[k % n_workers].append(item)

    out_map: dict[int, dict] = {}
    out_lock = threading.Lock()

    def _worker(batch):
        from playwright.sync_api import sync_playwright as _swp
        with _swp() as pw:
            _browser = pw.chromium.launch(headless=headless, args=_BROWSER_ARGS)
            _ctx = _browser.new_context(storage_state=storage_state)
            _pg  = _ctx.new_page()
            try:
                for i, entry in batch:
                    try:
                        scraped = scrape_all_films(
                            _pg, entry["rentrak_id"], week_date, entry["attraction"]
                        )
                        entry.update(scraped)
                    except Exception as exc:
                        entry.update({"films": [], "valid_as_of": "",
                                       "status": "error", "error": str(exc)})
                    with out_lock:
                        out_map[i] = entry
            finally:
                try:
                    _browser.close()
                except Exception:
                    pass

    with ThreadPoolExecutor(max_workers=n_workers) as pool:
        futures = [pool.submit(_worker, batch) for batch in batches if batch]
        for f in as_completed(futures):
            f.result()   # re-raise worker exceptions

    # Reconstruct results in original booking-sheet order.
    results = []
    for i, entry, needs in entries:
        results.append(out_map.get(i, entry))
    return results


# ---------------------------------------------------------------------------
# Step 6: Render dashboard
# ---------------------------------------------------------------------------

def render_dashboard(results: list[dict], week_date: str, output_path: Path):
    env      = Environment(loader=FileSystemLoader(str(TEMPLATE_DIR)))
    template = env.get_template("dashboard.html")

    # Build the theatre data object that gets embedded as JSON in the template
    theatre_json = json.dumps(results, ensure_ascii=False)

    html = template.render(
        theatre_data_json = theatre_json,
        week_date         = week_date,
        generated_at      = datetime.now().strftime("%B %d, %Y  %I:%M %p"),
        total_theatres    = len(results),
        ok_count          = sum(1 for r in results if r.get("status") == "ok"),
    )

    output_path.write_text(html, encoding="utf-8")
    print(f"\nDashboard saved -> {output_path}")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Pull Comscore flash grosses for FINAL locations in a booking sheet."
    )
    parser.add_argument("input_file", help="Booking sheet CSV or screenshot image (PNG, JPG, etc.)")
    parser.add_argument(
        "--week", default=None,
        help="Week date (YYYY-MM-DD or MM/DD/YYYY). Defaults to most recent Friday."
    )
    parser.add_argument(
        "--output",
        default=str(OUTPUT_DIR / "flash_gross_dashboard.html"),
        help="Output HTML path (default: output/flash_gross_dashboard.html)"
    )
    parser.add_argument(
        "--refresh-master-list", action="store_true",
        help="Force re-download of master list from Google Sheets"
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Parse CSV and show Rentrak ID matches without scraping Comscore"
    )
    parser.add_argument(
        "--no-headless", action="store_true",
        help="Show browser window (useful for debugging login)"
    )
    parser.add_argument(
        "--cdp", metavar="URL", nargs="?", const="http://localhost:9222",
        help="Connect to an existing Chrome via CDP (default: http://localhost:9222). "
             "Launch Chrome with: --remote-debugging-port=9222"
    )
    args = parser.parse_args()

    week_date = parse_week_arg(args.week) if args.week else str(most_recent_friday())
    print(f"Week date: {week_date}")

    input_path = Path(args.input_file)
    if input_path.suffix.lower() in IMAGE_EXTS:
        locations = parse_screenshot(str(input_path))
    else:
        locations = load_final_locations(str(input_path))

    if not locations:
        print("INFO: No Final locations found in booking — nothing to scrape from Comscore.")
        sys.exit(0)

    headless = not args.no_headless

    with sync_playwright() as p:
        if args.cdp:
            print(f"Connecting to existing Chrome at {args.cdp} ...")
            browser = p.chromium.connect_over_cdp(args.cdp)
            context = browser.contexts[0] if browser.contexts else browser.new_context()
            page    = context.pages[0] if context.pages else context.new_page()
            print("  Connected — using existing session (no login needed).")
        else:
            browser = p.chromium.launch(headless=headless, args=_BROWSER_ARGS)
            context = browser.new_context()
            page    = context.new_page()

        master_lookup = load_master_list(
            page=page,
            force_refresh=args.refresh_master_list
        )

        if args.dry_run:
            print("\n--- Dry run: Rentrak ID lookup ---")
            col = lambda key: next((k for k in locations[0] if k.lower() == key), None)
            unit_col    = col("unit")
            theatre_col = col("theatre")
            for loc in locations:
                unit    = str(loc.get(unit_col, "")).strip() if unit_col else ""
                theatre = str(loc.get(theatre_col, "")).strip() if theatre_col else ""
                ml      = master_lookup.get(unit)
                rentrak = ml["rentrak_id"] if ml else "NOT FOUND"
                url     = build_flash_url(rentrak, week_date) if ml else "N/A"
                print(f"  Unit {unit:>6}  {theatre:<45}  Rentrak: {rentrak:>8}  {url}")
            if not args.cdp:
                browser.close()
            return

        if not args.cdp and not login_to_comscore(page):
            browser.close()
            sys.exit(1)

        print(f"\nScraping flash grosses for {len(locations)} theatre(s)...")
        results = pull_all_theatre_data(
            locations, master_lookup, week_date, page,
            headless=headless,
            is_cdp=bool(args.cdp),
        )
        if not args.cdp:
            browser.close()

    render_dashboard(results, week_date, Path(args.output))


if __name__ == "__main__":
    main()
