#!/usr/bin/env python3
"""
O Canada Weekly Update
======================
Pulls Canadian weekly grosses for every active Angel title from Comscore Box
Office Essentials and writes them into the "O Canada" Google Sheet.

Run modes
---------
- Manual / cron:         python o_canada_update.py
- Dry run (no writes):   python o_canada_update.py --dry-run
- Force overwrite cells: python o_canada_update.py --force

Expects env vars:
  COMSCORE_USERNAME / COMSCORE_PASSWORD  — for Comscore login
  GSHEETS_SERVICE_ACCOUNT_JSON           — service-account JSON contents (Fly secret)
                                            falls back to creds/sheets-service-account.json
"""

import argparse
import json
import os
import re
import sys
import tempfile
from datetime import date, datetime, timedelta
from pathlib import Path

# Force UTF-8 stdout on Windows (cp1252 default can't render Unicode arrows / em-dashes)
try:
    sys.stdout.reconfigure(encoding='utf-8')
    sys.stderr.reconfigure(encoding='utf-8')
except Exception:
    pass

try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).parent / '.env')
except ImportError:
    pass

import gspread
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError

# Reuse Comscore login from the flash gross tool — single source of truth
from flash_gross_tool import login_to_comscore, COMSCORE_BASE

SHEET_ID = "1E5H7pP-YFZmGQqcGoWN4aNeNBhdVRGKyATVh90AOsUw"
SHEET_TAB = "Box Office Locations/Gross"
CREDS_PATH = Path(__file__).parent / "creds" / "sheets-service-account.json"
# Cache of {normalized_title: title_no} so we only search Comscore once per title.
# Lives on the Fly volume (/app/output) so it survives redeploys.
TITLE_CACHE_PATH = Path(__file__).parent / "output" / "o_canada_title_cache.json"

# Row 1 = title headers (every other col B,D,F...)
# Row 2 = "Location Count" / "Gross $" pair labels
# Row 3+ = WEEK 1, WEEK 2, ...  →  sheet row for week N = N + 2
TITLE_ROW = 1
WEEK_BASE_ROW = 2  # WEEK N is at row WEEK_BASE_ROW + N

# Only process titles at this column or further right (skip older "done" titles).
# Change this string when a newer title becomes the new starting point.
START_FROM_TITLE = "Animal Farm"

# Titles that exist in the sheet but should be skipped — no Canadian rights,
# not applicable, etc.  Compared via normalize_title() (lowercase, alphanum).
SKIP_TITLES = {
    "runner",  # Angel does not have Canadian distribution rights for Runner
}

# Manual release-date overrides for titles where Comscore's date is wrong
# or "(Unset)".  Keys are normalize_title() output; values are real release
# dates that get normalized to the opening Friday of the film week.
# Add entries here as Tommy confirms dates ahead of Comscore.
RELEASE_DATE_OVERRIDES = {
    "angelandthebadman": date(2026, 10, 9),  # Comscore shows "Oct (Unset) 2026"
}


# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────

def log(msg: str) -> None:
    print(msg, flush=True)


def last_completed_friday(today: date) -> date:
    """Return the Friday of the most recently completed Fri-Thu film week.

    On a Friday, the week that just ended last night (Thu) is the target,
    so target_friday = today - 7 days.
    On any other weekday, return the Friday of the previous fully complete week.
    """
    days_back = (today.weekday() - 4) % 7 + 7  # 4 = Friday
    return today - timedelta(days=days_back)


def friday_of_week(d: date) -> date:
    """Return the Friday that starts the Fri-Thu film week containing d."""
    return d - timedelta(days=(d.weekday() - 4) % 7)


_MONTHS = {
    'jan': 1, 'feb': 2, 'mar': 3, 'apr': 4, 'may': 5, 'jun': 6,
    'jul': 7, 'aug': 8, 'sep': 9, 'oct': 10, 'nov': 11, 'dec': 12,
}


def parse_release_date(s: str) -> date | None:
    """Parse Comscore release-date strings like 'May 1, 2026', 'Jul 3, 2026 (Thu)',
    'Dec 29, 1954 (Wed)', etc.  Returns None for unparseable dates ('Oct (Unset) 2026')."""
    if not s:
        return None
    # Strip trailing day-of-week annotation: 'Nov 26, 2026 (Thu)' -> 'Nov 26, 2026'
    cleaned = re.sub(r'\s*\([^)]*\)\s*$', '', s.strip())
    m = re.match(r'^([A-Za-z]{3})[A-Za-z]*\s+(\d{1,2}),\s*(\d{4})$', cleaned)
    if not m:
        return None
    mon = _MONTHS.get(m.group(1)[:3].lower())
    if not mon:
        return None
    try:
        return date(int(m.group(3)), mon, int(m.group(2)))
    except ValueError:
        return None


def normalize_title(s: str) -> str:
    """Lowercase, strip non-alphanum, for fuzzy title matching."""
    return re.sub(r'[^a-z0-9]', '', (s or '').lower())


def col_letter(idx: int) -> str:
    """0-based col index → A1 letter (0 → A, 25 → Z, 26 → AA)."""
    s = ''
    n = idx + 1
    while n:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s


def parse_money(s: str) -> int | None:
    """'$24,318' or '24,318' → 24318. Returns None if not parseable."""
    if not s:
        return None
    cleaned = re.sub(r'[^\d.-]', '', s)
    if not cleaned:
        return None
    try:
        return int(float(cleaned))
    except ValueError:
        return None


# ──────────────────────────────────────────────────────────────────────────────
# Sheets
# ──────────────────────────────────────────────────────────────────────────────

def open_sheet():
    """Authenticate to Google Sheets — prefer env var (Fly secret), fall back to file."""
    json_blob = os.getenv('GSHEETS_SERVICE_ACCOUNT_JSON', '').strip()
    if json_blob:
        # Fly secret path: dump to temp file, gspread reads from path
        tmp = tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False, encoding='utf-8')
        tmp.write(json_blob)
        tmp.close()
        gc = gspread.service_account(filename=tmp.name)
    elif CREDS_PATH.exists():
        gc = gspread.service_account(filename=str(CREDS_PATH))
    else:
        sys.exit(
            f"ERROR: no Google Sheets credentials found.\n"
            f"Set GSHEETS_SERVICE_ACCOUNT_JSON env var, or place creds at:\n  {CREDS_PATH}"
        )
    return gc.open_by_key(SHEET_ID).worksheet(SHEET_TAB)


def read_title_columns(ws) -> dict[str, dict]:
    """
    Return {normalized_title: {'display': str, 'loc_col_idx': 0-based, 'gross_col_idx': 0-based}}

    Title headers live in row 1 starting at column B (idx 1), at every odd column (B, D, F, ...).
    The cell next to each title in row 2 should be 'Location Count' and the one after 'Gross $'.

    Only titles at or to the right of START_FROM_TITLE are returned — older "done"
    titles are intentionally skipped.
    """
    row1 = ws.row_values(1)  # title headers
    titles = {}

    # Find where START_FROM_TITLE lives so we can filter out earlier columns
    start_norm = normalize_title(START_FROM_TITLE)
    start_col_idx = None
    for col_idx, cell in enumerate(row1):
        if normalize_title(cell or '') == start_norm:
            start_col_idx = col_idx
            break
    if start_col_idx is None:
        log(f"  WARNING: START_FROM_TITLE {START_FROM_TITLE!r} not found in sheet row 1; "
            f"processing all titles")
        start_col_idx = 1  # process from col B onward (skip col A label)

    for col_idx, cell in enumerate(row1):
        if col_idx < start_col_idx:
            continue
        title = (cell or '').strip()
        if not title:
            continue
        norm = normalize_title(title)
        if not norm:
            continue
        titles[norm] = {
            'display': title,
            'loc_col_idx': col_idx,
            'gross_col_idx': col_idx + 1,
        }
    log(f"  -> starting from column of {START_FROM_TITLE!r} (col idx {start_col_idx})")
    return titles


def next_empty_week_row(ws, loc_col_idx: int) -> int:
    """
    Return the WEEK number (1-based) of the next empty row in the title's location-count column.

    Sheet rows 3+ correspond to WEEK 1, 2, 3, ...  We count how many WEEK rows already
    have a value in the location-count cell, then the next-to-fill is that count + 1.
    This is timing-independent: we don't need to know the film's opening date, we just
    fill the next empty row sequentially.
    """
    col_vals = ws.col_values(loc_col_idx + 1)  # 1-based for gspread
    filled_weeks = 0
    # row 3 onwards = WEEK 1, 2, 3, ...
    for row_idx in range(WEEK_BASE_ROW, len(col_vals)):  # idx 2 == row 3 (WEEK 1)
        if (col_vals[row_idx] or '').strip():
            filled_weeks += 1
        else:
            break  # stop at first gap; user fills sequentially
    return filled_weeks + 1


# ──────────────────────────────────────────────────────────────────────────────
# Comscore scraping
# ──────────────────────────────────────────────────────────────────────────────

def _load_title_cache() -> dict:
    """Load the {normalized_title: title_no} cache. Returns {} if missing."""
    try:
        if TITLE_CACHE_PATH.exists():
            return json.loads(TITLE_CACHE_PATH.read_text(encoding='utf-8'))
    except Exception as exc:
        log(f"  WARNING: failed to read title cache: {exc}")
    return {}


def _save_title_cache(cache: dict) -> None:
    try:
        TITLE_CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
        TITLE_CACHE_PATH.write_text(json.dumps(cache, indent=2), encoding='utf-8')
    except Exception as exc:
        log(f"  WARNING: failed to write title cache: {exc}")


# JS extractor for the Film Lookup results page.  Each row has a Title link whose
# href contains title_no=N, plus a Distributor column.  We want the Angel-distributed
# row (when multiple matches exist).
_LOOKUP_RESULTS_JS = """
() => {
    const rows = Array.from(document.querySelectorAll('tbody tr, .boet-tr-body, table tr'));
    const out = [];
    for (const tr of rows) {
        const link = tr.querySelector('a[href*="title_no="]');
        if (!link) continue;
        const m = link.getAttribute('href').match(/title_no=(\\d+)/);
        if (!m) continue;
        const tds = Array.from(tr.querySelectorAll('td'));
        if (!tds.length) continue;
        const rowText = (tr.innerText || tr.textContent || '').toUpperCase();
        const isAngel = /\\bANGEL\\b/.test(rowText);
        out.push({
            title_no: m[1],
            title: link.textContent.trim(),
            release_date: (tds[0] && tds[0].textContent.trim()) || '',
            is_angel: isAngel,
            row_text: rowText.slice(0, 200),
        });
        if (out.length >= 30) break;
    }
    return { rows: out };
}
"""


_WEEK_TABLE_JS = """
() => {
    const pageText = (document.body && document.body.innerText) || '';
    const preparing = pageText.includes('Preparing Data') || pageText.includes('Loading');
    const explicitNoData = pageText.includes('No data is available')
                        || pageText.includes('No results')
                        || pageText.includes('no data');

    const candidateTables = Array.from(document.querySelectorAll('table'));
    const tableInfo = candidateTables.map(t => {
        /* Use only the LAST thead row — Comscore uses a multi-row thead where the
           first row has group-span headers ("Theatre Info", "Weekly Data") and the
           second row has the actual data column headers.  Counting all <th> in both
           rows gives wrong indices that don't match <td> counts in data rows. */
        const theadRows = Array.from(t.querySelectorAll('thead tr'));
        const lastTR = theadRows[theadRows.length - 1];
        const ths = lastTR
            ? Array.from(lastTR.querySelectorAll('th, td')).map(th => th.textContent.trim())
            : [];
        const bodyRows = t.querySelectorAll('tbody tr').length;
        return { thead_last_row_cols: ths.length, thead_sample: ths.slice(0, 8), body_rows: bodyRows };
    });

    /* Find the data table — header is "Date Range Gross" when querying a RANGE,
       or "Week Gross".  Use LAST thead row only.  Pick the LARGEST matching table. */
    let activeTable = null;
    let weekGrossIdx = -1;
    let chosenHeaders = [];
    let bestBodyRows = -1;
    let activeTableIdx = -1;
    candidateTables.forEach((t, ti) => {
        const theadRows = Array.from(t.querySelectorAll('thead tr'));
        const lastTR = theadRows[theadRows.length - 1];
        if (!lastTR) return;
        const headers = Array.from(lastTR.querySelectorAll('th, td'))
            .map(th => th.textContent.trim().toLowerCase());
        let idx = headers.findIndex(h => {
            const n = h.replace(/\\s+/g, '');
            return n === 'daterangegross' || n === 'weekgross';
        });
        if (idx < 0) {
            idx = headers.findIndex(h => h.includes('gross') &&
                (h.includes('range') || h.includes('week') || h.includes('total')));
        }
        if (idx < 0) {
            /* Last resort: any column named just "gross" */
            idx = headers.findIndex(h => h.replace(/\\s+/g, '') === 'gross');
        }
        if (idx < 0) return;
        const bodyRows = t.querySelectorAll('tbody tr, .boet-tr-body').length;
        if (bodyRows > bestBodyRows) {
            activeTable = t;
            weekGrossIdx = idx;
            chosenHeaders = headers;
            bestBodyRows = bodyRows;
            activeTableIdx = ti;
        }
    });

    if (!activeTable) {
        return {
            preparing,
            no_data_explicit: explicitNoData,
            rows: 0,
            week_gross: 0,
            page_total_locs: 0,
            diag: { tables: tableInfo },
        };
    }

    /* CRITICAL: restrict to direct children of tbody — Comscore puts a totals
       row inside <tfoot> that's also marked .boet-tr-body, so without scoping
       we'd include the page total ($24,318) as if it were a 58th theatre,
       roughly doubling the sum. */
    const tbody = activeTable.querySelector('tbody');
    const tRows = tbody
        ? Array.from(tbody.querySelectorAll(':scope > tr, :scope > .boet-tr-body'))
        : [];
    let total = 0;
    let counted = 0;
    const sampleCells = [];
    tRows.forEach((tr, i) => {
        const tds = Array.from(tr.querySelectorAll('td'));
        const targetCell = tds[weekGrossIdx];
        const cellText = targetCell ? targetCell.textContent.trim() : '';
        if (i < 5) {
            sampleCells.push({
                row: i,
                cells_count: tds.length,
                gross_cell_text: cellText,
                all_cell_text: tds.map(td => td.textContent.trim().slice(0, 30)),
            });
        }
        if (targetCell) {
            const raw = cellText.replace(/[^0-9.\\-]/g, '');
            const num = parseFloat(raw);
            if (!isNaN(num)) {
                total += num;
                counted += 1;
            }
        }
    });

    /* Try to read the location count from a page-level summary element,
       e.g. "67 Locations" or "67 Theaters Polled" shown above the table. */
    let pageTotalLocs = 0;
    const locM = pageText.match(/(\\d[\\d,]*)\\s+(?:locations?|theatres?|theaters?)/i);
    if (locM) pageTotalLocs = parseInt(locM[1].replace(/,/g, ''), 10);

    return {
        preparing,
        no_data_explicit: explicitNoData,
        rows: tRows.length,
        page_total_locs: pageTotalLocs,
        week_gross: Math.round(total),
        week_gross_counted: counted,
        chosen_header: (chosenHeaders[weekGrossIdx] || ''),
        diag: {
            tables: tableInfo,
            weekGrossIdx,
            chosenHeaders,
            sampleCells,
            activeTableIdx,
            totalTables: candidateTables.length,
        },
    };
}
"""


def find_title_no_by_search(page, title_name: str) -> str | None:
    """
    Look up a film via Comscore's Legacy Film Lookup form and grab title_no
    from the Angel-distributed row of the results page.

    Manual flow (matches what a human does):
      1. Click "FILM LOOKUP" → goes to /controllers/film_lookup_legacy
      2. Type the title in the "Film Title" input
      3. Click "Run Query"
      4. On the results page, click the title link in the Angel Studios row
         → URL becomes /reports/flash/film_detail?title_no=N

    We short-circuit step 4: extract title_no from the link's href on the
    results page (no need to click through to the detail page).
    """
    page.goto(f"{COMSCORE_BASE}/controllers/film_lookup_legacy",
              wait_until="domcontentloaded", timeout=60_000)

    # Find the Film Title input.  Form field is labelled "Film Title" — try a few
    # likely name attributes, then fall back to label-relative lookup.
    film_title_sel = (
        'input[name="film_title" i], '
        'input[name="title" i], '
        'input[name*="film_title" i], '
        'input[name*="title" i]:not([type="checkbox"]):not([type="hidden"]):not([type="submit"])'
    )
    try:
        page.wait_for_selector(film_title_sel, timeout=15_000)
    except PlaywrightTimeoutError:
        log(f"    WARNING: Film Title input not found on lookup page")
        return None

    inp = page.locator(film_title_sel).first
    try:
        inp.fill('')
    except Exception:
        pass
    inp.fill(title_name)

    # Click "Run Query" button.  Try a few variants.
    run_btn_candidates = [
        'input[type="submit"][value*="Run Query" i]',
        'button:has-text("Run Query")',
        'input[type="button"][value*="Run Query" i]',
    ]
    clicked = False
    for sel in run_btn_candidates:
        try:
            page.locator(sel).first.click(timeout=2000)
            clicked = True
            break
        except Exception:
            continue
    if not clicked:
        # Last resort: press Enter in the input
        inp.press('Enter')

    # Wait for results page to land
    try:
        page.wait_for_url('**/lookup_results*', timeout=30_000)
    except PlaywrightTimeoutError:
        log(f"    WARNING: Run Query didn't navigate to lookup_results")
        return None
    page.wait_for_load_state("domcontentloaded")

    # Poll for results rows to render — Comscore sometimes lags after navigation
    import time as _time
    deadline = _time.time() + 15
    rows = []
    while _time.time() < deadline:
        data = page.evaluate(_LOOKUP_RESULTS_JS)
        rows = data.get('rows') or []
        if rows:
            break
        page.wait_for_timeout(800)

    if not rows:
        log(f"    WARNING: no results found for {title_name!r}")
        return None

    # Prefer the row whose Dist column contains "Angel" (our films)
    angel_rows = [r for r in rows if r.get('is_angel')]
    chosen = (angel_rows[0] if angel_rows else rows[0])
    if angel_rows:
        log(f"    lookup '{title_name}' -> {chosen['title']!r} "
            f"(title_no={chosen['title_no']}, released {chosen.get('release_date', '?')})")
    else:
        log(f"    lookup '{title_name}' -> {chosen['title']!r} "
            f"(title_no={chosen['title_no']}, NO Angel match — using first row, released {chosen.get('release_date', '?')})")
    if len(rows) > 1:
        log(f"    ({len(rows)} total rows, {len(angel_rows)} Angel-distributed)")

    # Return both title_no and release_date so caller can compute target_friday
    return {
        'title_no': chosen['title_no'],
        'release_date': chosen.get('release_date', ''),
    }


def fetch_canada_week(page, title_no: str, friday: date) -> dict:
    """
    Navigate to grosses_by_theatre filtered to Canada + week, return:
      {location_count: int, week_gross: int, no_data: bool, url: str}
    """
    end_thursday = friday + timedelta(days=6)
    # NOTE: the date range is passed as THREE repeated `day_range_rev=` params:
    #   1) =RANGE        (mode)
    #   2) =<start_date> (Friday)
    #   3) =<end_date>   (following Thursday)
    # This is the exact URL Comscore generates when you click "Update Report" in
    # the Filter dialog.  Using `&d=...` instead silently falls back to defaults
    # and returns 0 for every theatre's gross.
    url = (
        f"{COMSCORE_BASE}/reports/flash/grosses_by_theatre"
        f"?title_no={title_no}"
        f"&country_id=CA"
        f"&last_year_range_type=same_days"
        f"&metro_or_city_state_type=metro"
        f"&revenue_day_option=week"
        f"&day_range_rev=RANGE"
        f"&day_range_rev={friday.isoformat()}"
        f"&day_range_rev={end_thursday.isoformat()}"
    )
    log(f"  → fetching {url}")
    page.goto(url, wait_until="domcontentloaded", timeout=60_000)

    # Wait until the COPY button is enabled — Comscore disables it while data
    # is still loading and enables it when the table is fully populated.  This
    # is a far more reliable signal than guessing at row-count stability.
    import time as _time
    _COPY_READY_JS = """
    () => {
        const btns = Array.from(document.querySelectorAll('button, a'));
        const copy = btns.find(b => (b.textContent || '').trim().toUpperCase().includes('COPY'));
        if (!copy) return { found: false, ready: false };
        const disabled = copy.disabled
            || copy.getAttribute('disabled') !== null
            || copy.getAttribute('aria-disabled') === 'true'
            || copy.classList.contains('disabled');
        return { found: true, ready: !disabled };
    }
    """
    deadline = _time.time() + 90
    copy_ready = False
    while _time.time() < deadline:
        try:
            state = page.evaluate(_COPY_READY_JS)
        except Exception:
            state = {'found': False, 'ready': False}
        if state.get('ready'):
            copy_ready = True
            break
        page.wait_for_timeout(1000)

    if copy_ready:
        log("    -> COPY button is enabled — data fully loaded")
        page.wait_for_timeout(1000)
    else:
        log("    -> WARN: timeout waiting for COPY button to enable (90 s); "
            "extracting whatever's rendered")

    # Comscore's table uses virtual scrolling — even after data is "loaded"
    # only the rows in/near the viewport are in DOM, and scrolling unmounts
    # rows that leave the viewport.  Strategy: scroll incrementally from top
    # to bottom, capturing every row's gross at each step into a per-rank
    # dictionary (keyed on the Rank column).  Then sum the unique rows.
    _SCROLL_STEP_JS = """
    (yTarget) => {
        /* Scroll every overflow container that holds a table, plus window */
        const containers = new Set();
        document.querySelectorAll('table').forEach(t => {
            let p = t.parentElement;
            while (p && p !== document.body) {
                const s = getComputedStyle(p);
                if ((s.overflowY === 'auto' || s.overflowY === 'scroll'
                     || s.overflow === 'auto' || s.overflow === 'scroll')
                    && p.scrollHeight > p.clientHeight + 4) {
                    containers.add(p);
                }
                p = p.parentElement;
            }
        });
        let maxScrollH = 0;
        containers.forEach(c => {
            c.scrollTop = yTarget;
            if (c.scrollHeight > maxScrollH) maxScrollH = c.scrollHeight;
        });
        window.scrollTo(0, yTarget);
        return { maxScrollH, containers: containers.size };
    }
    """
    _CAPTURE_VISIBLE_ROWS_JS = """
    () => {
        /* Find the data table by header again (same logic as main extractor) */
        const tables = Array.from(document.querySelectorAll('table'));
        let active = null;
        let idx = -1;
        for (const t of tables) {
            const theadRows = Array.from(t.querySelectorAll('thead tr'));
            const lastTR = theadRows[theadRows.length - 1];
            if (!lastTR) continue;
            const headers = Array.from(lastTR.querySelectorAll('th, td'))
                .map(th => th.textContent.trim().toLowerCase().replace(/\\s+/g, ''));
            let i = headers.findIndex(h => h === 'daterangegross' || h === 'weekgross');
            if (i < 0) continue;
            const bodyRows = t.querySelectorAll('tbody tr').length;
            if (!active || bodyRows > active.querySelectorAll('tbody tr').length) {
                active = t;
                idx = i;
            }
        }
        if (!active) return [];
        const tbody = active.querySelector('tbody');
        if (!tbody) return [];
        const out = [];
        Array.from(tbody.querySelectorAll(':scope > tr, :scope > .boet-tr-body')).forEach(tr => {
            const tds = Array.from(tr.querySelectorAll('td'));
            if (tds.length < idx + 1) return;
            const rank = (tds[0] && tds[0].textContent.trim()) || '';
            const grossText = (tds[idx] && tds[idx].textContent.trim()) || '';
            const num = parseFloat(grossText.replace(/[^0-9.\\-]/g, ''));
            if (!rank || !grossText) return;
            out.push({ rank, gross: isNaN(num) ? 0 : num, grossText });
        });
        return out;
    }
    """
    captured = {}  # rank (str) -> {gross, grossText}
    # First find the scrollable container height
    info = page.evaluate(_SCROLL_STEP_JS, 0)
    max_h = info.get('maxScrollH', 0)
    step_px = 400
    y = 0
    while True:
        # Capture currently-rendered rows
        try:
            visible = page.evaluate(_CAPTURE_VISIBLE_ROWS_JS)
        except Exception:
            visible = []
        for r in visible:
            captured[r['rank']] = r
        if y >= max_h:
            break
        y += step_px
        try:
            info2 = page.evaluate(_SCROLL_STEP_JS, y)
            if info2.get('maxScrollH', 0) > max_h:
                max_h = info2['maxScrollH']
        except Exception:
            pass
        page.wait_for_timeout(350)
    # One final capture at the very bottom
    try:
        page.evaluate(_SCROLL_STEP_JS, 10_000_000)
        page.wait_for_timeout(500)
        for r in page.evaluate(_CAPTURE_VISIBLE_ROWS_JS):
            captured[r['rank']] = r
    except Exception:
        pass

    # Build a synthetic `data` dict that downstream code expects
    total_gross = sum(int(r['gross']) for r in captured.values())
    location_count = len(captured)
    log(f"    -> scroll-capture: {location_count} unique rows, "
        f"total gross ${total_gross:,}")
    data = page.evaluate(_WEEK_TABLE_JS)
    # ALWAYS prefer the scroll-capture result once we have any rows.  It dedupes
    # by Rank and filters out empty/placeholder rows that the post-scroll
    # snapshot still includes in its raw `rows` count.  The buggy old logic
    # ("only if scroll-capture saw MORE rows") let a 21-theatre table report
    # 23 locations because the snapshot tbody still had 2 empty placeholder rows.
    if location_count > 0:
        data['rows'] = location_count
        data['week_gross'] = total_gross

    diag = data.get('diag', {})
    rows = data.get('rows', 0)
    counted = data.get('week_gross_counted', 0)
    gross = data.get('week_gross', 0)
    page_locs = data.get('page_total_locs', 0)
    log(f"    DIAG: table={diag.get('activeTableIdx')}/{diag.get('totalTables')}, "
        f"rows={rows}, page_locs={page_locs}, counted={counted}, gross={gross}, "
        f"idx={diag.get('weekGrossIdx')}, header={data.get('chosen_header')!r}")
    # Always show sample cells so we can verify extraction looks right
    for s in (diag.get('sampleCells') or []):
        log(f"    DIAG row {s.get('row')}: tds={s.get('cells_count')}, "
            f"gross_cell={s.get('gross_cell_text')!r}, "
            f"all={s.get('all_cell_text')}")
    if rows == 0 and not data.get('no_data_explicit'):
        log(f"    DIAG: 0 rows, preparing={data.get('preparing')}")
        for t in (diag.get('tables') or [])[:6]:
            log(f"    DIAG table: {t}")

    # Use page-level location count (e.g. "67 Locations") when available;
    # otherwise fall back to the number of data rows found in the table.
    location_count = page_locs if page_locs > 0 else rows

    return {
        'location_count': location_count,
        'week_gross': gross,
        'no_data': data.get('no_data_explicit', False),
        'chosen_header': data.get('chosen_header', ''),
        'url': url,
    }


# ──────────────────────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────────────────────

def main():
    p = argparse.ArgumentParser()
    p.add_argument('--dry-run', action='store_true', help="Don't write to the sheet")
    p.add_argument('--force', action='store_true', help="Overwrite cells even if already filled")
    p.add_argument('--today', type=str, default=None,
                   help="Override today's date (YYYY-MM-DD) for testing")
    args = p.parse_args()

    today = date.fromisoformat(args.today) if args.today else date.today()
    log(f"O Canada update — today={today} ({today.strftime('%A')})")
    log(f"  Per-film target weeks are computed from each film's opening Friday.")
    if today.weekday() != 4:
        log(f"  (Today is not Friday — that's OK; target weeks are per-film, not based on today.)")
    if args.dry_run:
        log("  *** DRY RUN — no sheet writes will occur ***")

    # 1. Connect to sheet & read title columns
    log("\n[1/3] Reading O Canada sheet structure ...")
    ws = open_sheet()
    title_cols = read_title_columns(ws)
    log(f"  → sheet has {len(title_cols)} title columns")

    # 2. Launch browser + log into Comscore
    log("\n[2/3] Logging into Comscore ...")
    title_cache = _load_title_cache()
    log(f"  -> title cache has {len(title_cache)} entries")

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        ctx = browser.new_context(viewport={'width': 1600, 'height': 900})
        page = ctx.new_page()

        if not login_to_comscore(page):
            log("  ERROR: Comscore login failed")
            browser.close()
            sys.exit(2)

        # Navigate to /home once so the search box is available
        page.goto(f"{COMSCORE_BASE}/home", wait_until="domcontentloaded", timeout=60_000)
        page.wait_for_timeout(1500)

        # 3. Iterate sheet titles → look up title_no + opening_friday → fetch CA data → write
        log("\n[3/3] Updating each title from the sheet ...")
        summary = []
        cache_dirty = False
        for norm, info in title_cols.items():
            display = info['display']

            # Hard skip — titles we don't have rights to or otherwise should not process.
            if norm in SKIP_TITLES:
                log(f"\n  • {display!r}: in SKIP_TITLES, skipping")
                summary.append((display, 'skipped (no Canadian rights)'))
                continue

            # Look up title_no + opening_friday (cache first, then search).
            # Cache values may be old format (str title_no) or new format (dict);
            # treat str as a partial hit that still needs a re-lookup for opening_friday.
            cached = title_cache.get(norm)
            title_no = None
            opening_friday = None
            if isinstance(cached, dict):
                title_no = cached.get('title_no')
                of_str = cached.get('opening_friday')
                if of_str:
                    try:
                        opening_friday = date.fromisoformat(of_str)
                    except ValueError:
                        pass

            if not (title_no and opening_friday):
                log(f"\n  • {display!r}: not fully cached, looking up via Film Lookup ...")
                result = find_title_no_by_search(page, display)
                if not result:
                    log(f"    -> no Comscore match for {display!r}, skipping")
                    summary.append((display, 'no Comscore match'))
                    continue
                title_no = result['title_no']
                release_date = parse_release_date(result.get('release_date', ''))
                # Manual override beats Comscore — for titles whose Comscore
                # date is "(Unset)" or wrong (per the Angel release calendar).
                if norm in RELEASE_DATE_OVERRIDES:
                    override = RELEASE_DATE_OVERRIDES[norm]
                    log(f"    -> using release-date override {override} "
                        f"(Comscore said {result.get('release_date')!r})")
                    release_date = override
                if release_date is None:
                    log(f"    -> couldn't parse release date {result.get('release_date')!r}, skipping")
                    summary.append((display, 'unparseable release date'))
                    continue
                opening_friday = friday_of_week(release_date)
                title_cache[norm] = {
                    'title_no': title_no,
                    'opening_friday': opening_friday.isoformat(),
                    'release_date_raw': result.get('release_date', ''),
                }
                cache_dirty = True
                log(f"    -> cached title_no={title_no}, opening_friday={opening_friday}")
            else:
                log(f"\n  • {display!r}: title_no={title_no}, opening_friday={opening_friday} (cached)")

            # target_week_n = next empty WEEK row in the sheet (sequential fill).
            target_week_n = next_empty_week_row(ws, info['loc_col_idx'])

            # target_friday = opening_friday + (target_week_n - 1) × 7 days
            # This aligns the Comscore data with the sheet row independent of which
            # day we run on. Films opening in the future or weeks not yet complete
            # are filtered below.
            per_film_target_friday = opening_friday + timedelta(days=(target_week_n - 1) * 7)
            week_complete_through = per_film_target_friday + timedelta(days=6)  # Thursday

            log(f"    -> filling sheet WEEK {target_week_n} "
                f"(data for week of {per_film_target_friday}, ending {week_complete_through})")

            if week_complete_through > today:
                log(f"    -> week {per_film_target_friday}..{week_complete_through} "
                    f"is not yet complete (today={today}); skipping")
                summary.append((display, f'WEEK {target_week_n}: not yet complete'))
                continue
            if week_complete_through == today:
                log(f"    NOTE: week ends today — Comscore data may be partial "
                    f"(Thursday not yet fully reported)")

            data = fetch_canada_week(page, title_no, per_film_target_friday)
            loc = data['location_count']
            gross = data['week_gross']
            chosen_header = data.get('chosen_header', '?')
            log(f"    locations={loc}, week_gross=${gross:,} (column matched: {chosen_header!r})")

            if loc == 0:
                log(f"    -> 0 Canadian theatres — title is done in Canada, skipping write")
                summary.append((display, f'WEEK {target_week_n}: 0 locations'))
                continue

            sheet_match = info  # keep variable name for the rest of the function
            cs_title = display
            wk = target_week_n  # so log lines below still make sense
            # fall through to the existing write-cells block below

            # Compute target cells
            sheet_row = WEEK_BASE_ROW + target_week_n  # WEEK 1 → row 3
            loc_cell = f"{col_letter(sheet_match['loc_col_idx'])}{sheet_row}"
            gross_cell = f"{col_letter(sheet_match['gross_col_idx'])}{sheet_row}"

            # Check existing values
            existing = ws.batch_get([loc_cell, gross_cell])
            existing_loc = (existing[0][0][0] if existing[0] and existing[0][0] else '').strip()
            existing_gross = (existing[1][0][0] if existing[1] and existing[1][0] else '').strip()
            already_filled = bool(existing_loc or existing_gross)

            if already_filled and not args.force:
                log(f"    → {loc_cell} / {gross_cell} already filled ({existing_loc!r} / {existing_gross!r}); use --force to overwrite, skipping")
                summary.append((cs_title, f'WEEK {target_week_n}: already filled — skipped'))
                continue

            if args.dry_run:
                log(f"    [dry-run] would write {loc} → {loc_cell}, ${gross:,} → {gross_cell}")
                summary.append((cs_title, f'WEEK {target_week_n}: {loc} locs / ${gross:,} (dry-run)'))
                continue

            ws.batch_update([
                {'range': loc_cell,   'values': [[loc]]},
                {'range': gross_cell, 'values': [[f"${gross:,}.00"]]},
            ])
            log(f"    -> wrote {loc} to {loc_cell} and ${gross:,}.00 to {gross_cell}")
            summary.append((cs_title, f'WEEK {target_week_n}: {loc} locs / ${gross:,}'))

        # Persist the title→title_no cache for next time
        if cache_dirty:
            _save_title_cache(title_cache)
            log(f"\n  -> saved title cache ({len(title_cache)} entries) to {TITLE_CACHE_PATH}")

        browser.close()

    log("\n══════ Summary ══════")
    for title, status in summary:
        log(f"  • {title}: {status}")
    log(f"Total titles processed: {len(summary)}")


if __name__ == '__main__':
    main()
