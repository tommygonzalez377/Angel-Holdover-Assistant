#!/usr/bin/env python3
"""
Daily Grosses Update — Fri/Sat/Sun + WoW deltas
================================================
Pulls Friday/Saturday/Sunday US daily grosses per Angel film from Comscore
Box Office Essentials and writes them into the "Weekend Percentage Drop Box Office"
Google Sheet:
  - Fri-Sat tab: Fri $ + Sat $ per week (Sat/Fri % is a sheet formula)
  - NON SOF tab: Sat $ + Sun $ per week (Sun/Sat % is a sheet formula)

Schedule: Tuesday 9:00 AM America/Phoenix (permanent MST). By Tuesday the
prior weekend's daily grosses are fully reported.

Run modes
---------
- Manual / cron:         python daily_grosses_update.py
- Dry run (no writes):   python daily_grosses_update.py --dry-run
- Force overwrite cells: python daily_grosses_update.py --force
- Override target date:  python daily_grosses_update.py --target-friday YYYY-MM-DD

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

from flash_gross_tool import login_to_comscore, COMSCORE_BASE

SHEET_ID = "1yTnuoNh923ibhQNA1tTrwfltF339oXXvPMDe3CHE1vE"
FRI_SAT_TAB = "Fri - Sat"
NON_SOF_TAB = "NON SOF"
CREDS_PATH = Path(__file__).parent / "creds" / "sheets-service-account.json"
# Share the O Canada title cache — same title_no's, same opening_fridays,
# so a hit in one script warms the other.
TITLE_CACHE_PATH = Path(__file__).parent / "output" / "o_canada_title_cache.json"

MAX_TRACKED_WEEK = 8  # Animal Farm tracks through WK 8 on NON SOF; Fri-Sat has 7 weeks (per-tab row check handles the gap)

# Skip every column to the LEFT of this title in both tabs — those films are
# done, so we don't even attempt cache/Comscore lookups for them.
# Update when the current leftmost-active film exits its tracking window.
START_FROM_TITLE = "Animal Farm"


# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────

def log(msg: str) -> None:
    print(msg, flush=True)


def most_recent_completed_friday(today: date) -> date:
    """Return the most recently completed Friday whose Fri/Sat/Sun is fully
    reported in Comscore. On Tue/Wed/Thu/Fri/Sat/Sun, that's the Friday of the
    just-completed weekend. On Mon, Sunday data may still be partial — return
    the Friday of the weekend BEFORE the most recent one to be safe.

    For this script we expect to run Tuesday at 9 AM MST, so today.weekday()==1.
    """
    wd = today.weekday()
    if wd == 0:  # Monday — Sunday may still be partial
        return today - timedelta(days=10)  # the Friday before last
    # Tue (1), Wed (2), Thu (3) — last Friday is fully settled
    # Fri (4) — last Friday (today's prev) is settled; today is too early
    # Sat (5), Sun (6) — last Friday is settled
    days_back = (wd - 4) % 7
    if days_back == 0:
        # On a Friday, the most recent completed weekend ended last Sunday
        days_back = 7
    return today - timedelta(days=days_back)


def friday_of_week(d: date) -> date:
    """Return the Friday that starts the Fri-Thu film week containing d."""
    return d - timedelta(days=(d.weekday() - 4) % 7)


_MONTHS = {
    'jan': 1, 'feb': 2, 'mar': 3, 'apr': 4, 'may': 5, 'jun': 6,
    'jul': 7, 'aug': 8, 'sep': 9, 'oct': 10, 'nov': 11, 'dec': 12,
}


def parse_release_date(s: str) -> date | None:
    """Parse Comscore release-date strings like 'May 1, 2026', 'Jul 3, 2026 (Thu)'."""
    if not s:
        return None
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
    return re.sub(r'[^a-z0-9]', '', (s or '').lower())


def col_letter(idx: int) -> str:
    """0-based col index → A1 letter (0 → A, 25 → Z, 26 → AA)."""
    s = ''
    n = idx + 1
    while n:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s


# ──────────────────────────────────────────────────────────────────────────────
# Sheets
# ──────────────────────────────────────────────────────────────────────────────

def open_sheet():
    """Authenticate to Google Sheets and return the spreadsheet object."""
    json_blob = os.getenv('GSHEETS_SERVICE_ACCOUNT_JSON', '').strip()
    if json_blob:
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
    return gc.open_by_key(SHEET_ID)


def read_title_columns(ws) -> dict[str, dict]:
    """
    Return {normalized_title: {'display': str, 'col_idx': 0-based}} from row 1.
    Skips empty headers and the trailing "AVERAGE % DROP" column.
    """
    row1 = ws.row_values(1)
    titles = {}
    for col_idx, cell in enumerate(row1):
        title = (cell or '').strip()
        if not title:
            continue
        if 'average' in title.lower() and '%' in title:
            continue
        norm = normalize_title(title)
        if not norm:
            continue
        titles[norm] = {'display': title, 'col_idx': col_idx}
    return titles


_OPENING_RE = re.compile(r'^opening\s+(fri|sat|sun)', re.IGNORECASE)
_WK_RE      = re.compile(r'^wk\s*(\d+)\s+(fri|sat|sun)', re.IGNORECASE)
_BARE_RE    = re.compile(r'^(fri|sat|sun)(day)?$', re.IGNORECASE)


def map_week_rows(col_a_values: list[str]) -> dict[tuple[int, str], int]:
    """
    Parse column A labels into {(week_n, day): row_index_1_based}.
    day ∈ {'fri', 'sat', 'sun'}.

    Handles:
      - "Opening Friday"/"Opening Saturday"/"Opening Sunday" → week 1
      - "WK N Fri"/"WK N Sat"/"WK N Sun" → week N
      - bare "Fri"/"Sat"/"Sun" → same week as the most recent WK label seen above
    """
    out: dict[tuple[int, str], int] = {}
    current_week = None
    for i, val in enumerate(col_a_values):
        v = (val or '').strip()
        if not v:
            continue
        m = _OPENING_RE.match(v)
        if m:
            day = m.group(1).lower()
            out[(1, day)] = i + 1
            current_week = 1
            continue
        m = _WK_RE.match(v)
        if m:
            n = int(m.group(1))
            day = m.group(2).lower()
            out[(n, day)] = i + 1
            current_week = n
            continue
        m = _BARE_RE.match(v)
        if m and current_week is not None:
            day = m.group(1).lower()
            out[(current_week, day)] = i + 1
    return out


# ──────────────────────────────────────────────────────────────────────────────
# Comscore scraping
# ──────────────────────────────────────────────────────────────────────────────

def _load_title_cache() -> dict:
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
        });
        if (out.length >= 30) break;
    }
    return { rows: out };
}
"""


def find_title_no_by_search(page, title_name: str) -> dict | None:
    """Look up title_no + release_date via Comscore's Legacy Film Lookup form."""
    page.goto(f"{COMSCORE_BASE}/controllers/film_lookup_legacy",
              wait_until="domcontentloaded", timeout=60_000)

    film_title_sel = (
        'input[name="film_title" i], '
        'input[name="title" i], '
        'input[name*="film_title" i], '
        'input[name*="title" i]:not([type="checkbox"]):not([type="hidden"]):not([type="submit"])'
    )
    try:
        page.wait_for_selector(film_title_sel, timeout=15_000)
    except PlaywrightTimeoutError:
        log("    WARNING: Film Title input not found on lookup page")
        return None

    inp = page.locator(film_title_sel).first
    try:
        inp.fill('')
    except Exception:
        pass
    inp.fill(title_name)

    for sel in (
        'input[type="submit"][value*="Run Query" i]',
        'button:has-text("Run Query")',
        'input[type="button"][value*="Run Query" i]',
    ):
        try:
            page.locator(sel).first.click(timeout=2000)
            break
        except Exception:
            continue
    else:
        inp.press('Enter')

    try:
        page.wait_for_url('**/lookup_results*', timeout=30_000)
    except PlaywrightTimeoutError:
        log("    WARNING: Run Query didn't navigate to lookup_results")
        return None
    page.wait_for_load_state("domcontentloaded")

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

    angel_rows = [r for r in rows if r.get('is_angel')]
    chosen = (angel_rows[0] if angel_rows else rows[0])
    log(f"    lookup '{title_name}' -> {chosen['title']!r} "
        f"(title_no={chosen['title_no']}, released {chosen.get('release_date', '?')})")
    return {
        'title_no': chosen['title_no'],
        'release_date': chosen.get('release_date', ''),
    }


# Same JS extractors as O Canada — single-day pull just uses start==end in URL.
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

_SCROLL_STEP_JS = """
(yTarget) => {
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
    const tables = Array.from(document.querySelectorAll('table'));
    let active = null;
    let idx = -1;
    for (const t of tables) {
        const theadRows = Array.from(t.querySelectorAll('thead tr'));
        const lastTR = theadRows[theadRows.length - 1];
        if (!lastTR) continue;
        const headers = Array.from(lastTR.querySelectorAll('th, td'))
            .map(th => th.textContent.trim().toLowerCase().replace(/\\s+/g, ''));
        let i = headers.findIndex(h => h === 'daterangegross' || h === 'weekgross' || h === 'gross');
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
        out.push({ rank, gross: isNaN(num) ? 0 : num });
    });
    return out;
}
"""


# Read the tfoot's "Date Range Gross" cell directly — Comscore puts the page
# total there and it renders even with virtual-scrolled tbody. Far more reliable
# than scroll-and-sum across thousands of rows.
_TFOOT_TOTAL_JS = """
() => {
    const tables = Array.from(document.querySelectorAll('table'));
    /* Find the data table with the most body rows and a 'gross' column */
    let best = null;
    let bestRows = -1;
    let bestIdx = -1;
    let bestHeaders = [];
    tables.forEach(t => {
        const theadRows = Array.from(t.querySelectorAll('thead tr'));
        const lastTR = theadRows[theadRows.length - 1];
        if (!lastTR) return;
        const headers = Array.from(lastTR.querySelectorAll('th, td'))
            .map(th => th.textContent.trim());
        const normed = headers.map(h => h.toLowerCase().replace(/\\s+/g, ''));
        let i = normed.findIndex(h => h === 'daterangegross' || h === 'weekgross' || h === 'gross');
        if (i < 0) return;
        const bodyRows = t.querySelectorAll('tbody tr').length;
        if (bodyRows > bestRows) {
            best = t;
            bestRows = bodyRows;
            bestIdx = i;
            bestHeaders = headers;
        }
    });
    if (!best) return { error: 'no table with gross column found' };

    /* Read the tfoot row's gross cell. Comscore tags the totals row as
       .boet-tr-body inside <tfoot>, sometimes as a regular <tr>. */
    const tfoot = best.querySelector('tfoot');
    let tfootGrossText = null;
    let tfootCells = [];
    if (tfoot) {
        const tfRow = tfoot.querySelector('tr, .boet-tr-body');
        if (tfRow) {
            tfootCells = Array.from(tfRow.querySelectorAll('td'))
                .map(td => td.textContent.trim());
            if (tfootCells[bestIdx]) tfootGrossText = tfootCells[bestIdx];
        }
    }

    /* Last data row's rank = theatre count (rows are ranked 1..N) */
    const tbody = best.querySelector('tbody');
    let lastRank = 0;
    let bodyRowCount = 0;
    if (tbody) {
        const trs = Array.from(tbody.querySelectorAll(':scope > tr, :scope > .boet-tr-body'));
        bodyRowCount = trs.length;
        for (let i = trs.length - 1; i >= 0; i--) {
            const tds = trs[i].querySelectorAll('td');
            if (!tds.length) continue;
            const rankText = (tds[0].textContent || '').trim().replace(/,/g, '');
            const n = parseInt(rankText, 10);
            if (!isNaN(n) && n > lastRank) lastRank = n;
        }
    }

    const total = tfootGrossText
        ? parseFloat(tfootGrossText.replace(/[^0-9.\\-]/g, ''))
        : NaN;

    return {
        chosen_header: bestHeaders[bestIdx],
        chosen_idx: bestIdx,
        all_headers: bestHeaders,
        tfoot_gross_text: tfootGrossText,
        tfoot_cells: tfootCells,
        tfoot_gross: isNaN(total) ? null : Math.round(total),
        body_rows: bodyRowCount,
        last_rank: lastRank,
    };
}
"""


# Diagnostic JS — returns full column info + page-level locations + sample rows
# so we can see which column we'd pick and what the data actually looks like.
_DAILY_DIAG_JS = """
() => {
    const pageText = (document.body && document.body.innerText) || '';
    /* Page-level "X locations" / "X theatres polled" indicator */
    let pageTotalLocs = 0;
    const locM = pageText.match(/(\\d[\\d,]*)\\s+(?:locations?|theatres?|theaters?)/i);
    if (locM) pageTotalLocs = parseInt(locM[1].replace(/,/g, ''), 10);

    const tables = Array.from(document.querySelectorAll('table'));
    /* Find the table with the most body rows that has any 'gross' header */
    let best = null;
    let bestRows = -1;
    let bestIdx = -1;
    let bestHeaders = [];
    let bestTableIdx = -1;
    tables.forEach((t, ti) => {
        const theadRows = Array.from(t.querySelectorAll('thead tr'));
        const lastTR = theadRows[theadRows.length - 1];
        if (!lastTR) return;
        const headers = Array.from(lastTR.querySelectorAll('th, td'))
            .map(th => th.textContent.trim());
        const normed = headers.map(h => h.toLowerCase().replace(/\\s+/g, ''));
        let i = normed.findIndex(h => h === 'daterangegross' || h === 'weekgross' || h === 'gross');
        if (i < 0) {
            i = normed.findIndex(h => h.includes('gross'));
        }
        if (i < 0) return;
        const bodyRows = t.querySelectorAll('tbody tr, .boet-tr-body').length;
        if (bodyRows > bestRows) {
            best = t;
            bestRows = bodyRows;
            bestIdx = i;
            bestHeaders = headers;
            bestTableIdx = ti;
        }
    });

    if (!best) {
        return {
            page_total_locs: pageTotalLocs,
            tables_count: tables.length,
            chose: null,
        };
    }

    /* All gross-related columns + what summing each would give for the first
       N currently-rendered rows.  Helps spot if we should be picking a
       different column. */
    const normed = bestHeaders.map(h => h.toLowerCase().replace(/\\s+/g, ''));
    const grossCols = [];
    normed.forEach((h, i) => {
        if (h.includes('gross')) {
            grossCols.push({ idx: i, header: bestHeaders[i] });
        }
    });
    const tbody = best.querySelector('tbody');
    const tRows = tbody
        ? Array.from(tbody.querySelectorAll(':scope > tr, :scope > .boet-tr-body'))
        : [];
    grossCols.forEach(gc => {
        let total = 0;
        tRows.forEach(tr => {
            const tds = Array.from(tr.querySelectorAll('td'));
            const cell = tds[gc.idx];
            if (!cell) return;
            const n = parseFloat(cell.textContent.replace(/[^0-9.\\-]/g, ''));
            if (!isNaN(n)) total += n;
        });
        gc.sum_visible_rows = Math.round(total);
    });

    const sampleCells = [];
    tRows.slice(0, 5).forEach((tr, i) => {
        const tds = Array.from(tr.querySelectorAll('td'));
        sampleCells.push({
            row: i,
            cells: tds.map(td => td.textContent.trim().slice(0, 40)),
        });
    });

    return {
        page_total_locs: pageTotalLocs,
        tables_count: tables.length,
        chose: {
            table_idx: bestTableIdx,
            body_rows: bestRows,
            chosen_idx: bestIdx,
            chosen_header: bestHeaders[bestIdx],
            all_headers: bestHeaders,
            gross_cols: grossCols,
            sample_cells: sampleCells,
        },
    };
}
"""


def fetch_single_day_gross(page, title_no: str, day: date) -> dict:
    """
    Pull a single day's North America grosses_by_theatre total for one title.

    URL matches what the Report Options dialog generates when you set
    "Range: D/D - D/D" (start and end the same):
      - NO country param (defaults to North America = US + CA)
      - revenue_day_option=week
      - day_range_rev=RANGE then ONE day_range_rev=<date> (not two — Comscore
        serializes start==end as a single date param)

    Returns: {'gross': int, 'location_count': int, 'url': str}
    """
    url = (
        f"{COMSCORE_BASE}/reports/flash/grosses_by_theatre"
        f"?title_no={title_no}"
        f"&last_year_range_type=same_days"
        f"&metro_or_city_state_type=metro"
        f"&revenue_day_option=week"
        f"&day_range_rev=RANGE"
        f"&day_range_rev={day.isoformat()}"
    )
    log(f"      → {url}")
    page.goto(url, wait_until="domcontentloaded", timeout=60_000)

    import time as _time
    deadline = _time.time() + 90
    copy_ready = False
    while _time.time() < deadline:
        try:
            state = page.evaluate(_COPY_READY_JS)
        except Exception:
            state = {'ready': False}
        if state.get('ready'):
            copy_ready = True
            page.wait_for_timeout(1500)  # extra settle so tfoot is painted
            break
        page.wait_for_timeout(1000)
    if not copy_ready:
        log("      WARN: 90s timeout waiting for COPY button to enable — total may be missing")

    # Read the tfoot total directly. The tfoot row always renders even when
    # tbody is virtual-scrolled, and Comscore has already summed everything
    # we need into that row's "Date Range Gross" cell.
    try:
        result = page.evaluate(_TFOOT_TOTAL_JS)
    except Exception as exc:
        log(f"      ERROR evaluating tfoot extractor: {exc}")
        return {'gross': 0, 'location_count': 0, 'url': url}

    if result.get('error'):
        log(f"      ERROR: {result['error']}")
        return {'gross': 0, 'location_count': 0, 'url': url}

    log(f"      chose col idx={result.get('chosen_idx')} {result.get('chosen_header')!r} "
        f"(headers: {result.get('all_headers')})")
    log(f"      tfoot cells: {result.get('tfoot_cells')}")
    log(f"      tfoot gross text: {result.get('tfoot_gross_text')!r} -> "
        f"${result.get('tfoot_gross') or 0:,}")
    log(f"      body_rows rendered: {result.get('body_rows')}, last data row rank: {result.get('last_rank')}")

    gross = result.get('tfoot_gross') or 0
    loc_count = result.get('last_rank') or 0
    if gross == 0:
        log("      WARN: tfoot gross is 0 — falling back to scroll-and-sum")
        # Fallback path: scroll + sum (slower, may undercount for large tables)
        captured = {}
        info = page.evaluate(_SCROLL_STEP_JS, 0)
        max_h = info.get('maxScrollH', 0)
        y = 0
        while True:
            try:
                visible = page.evaluate(_CAPTURE_VISIBLE_ROWS_JS)
            except Exception:
                visible = []
            for r in visible:
                captured[r['rank']] = r
            if y >= max_h:
                break
            y += 400
            try:
                info2 = page.evaluate(_SCROLL_STEP_JS, y)
                if info2.get('maxScrollH', 0) > max_h:
                    max_h = info2['maxScrollH']
            except Exception:
                pass
            page.wait_for_timeout(350)
        gross = sum(int(r['gross']) for r in captured.values())
        loc_count = len(captured)
        log(f"      -> fallback scroll-capture: {loc_count} rows, ${gross:,}")

    log(f"      -> {loc_count} theatres, total gross ${gross:,}")
    return {
        'gross': gross,
        'location_count': loc_count,
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
    p.add_argument('--target-friday', type=str, default=None,
                   help="Override the target Friday (YYYY-MM-DD); defaults to most_recent_completed_friday(today)")
    args = p.parse_args()

    today = date.fromisoformat(args.today) if args.today else date.today()
    if args.target_friday:
        target_friday = date.fromisoformat(args.target_friday)
    else:
        target_friday = most_recent_completed_friday(today)
    target_saturday = target_friday + timedelta(days=1)
    target_sunday   = target_friday + timedelta(days=2)

    log(f"Daily Grosses update — today={today} ({today.strftime('%A')})")
    log(f"  Target weekend: {target_friday} (Fri) / {target_saturday} (Sat) / {target_sunday} (Sun)")
    if args.dry_run:
        log("  *** DRY RUN — no sheet writes will occur ***")

    # 1. Open both tabs + read structure
    log("\n[1/3] Reading sheet structure ...")
    sh = open_sheet()
    fs_ws = sh.worksheet(FRI_SAT_TAB)
    ns_ws = sh.worksheet(NON_SOF_TAB)

    fs_titles  = read_title_columns(fs_ws)
    ns_titles  = read_title_columns(ns_ws)
    fs_rowmap  = map_week_rows(fs_ws.col_values(1))
    ns_rowmap  = map_week_rows(ns_ws.col_values(1))

    log(f"  Fri-Sat tab: {len(fs_titles)} title columns, {len(fs_rowmap)} (week, day) row slots")
    log(f"  NON SOF tab: {len(ns_titles)} title columns, {len(ns_rowmap)} (week, day) row slots")

    # Skip every column to the LEFT of START_FROM_TITLE in each tab. Films to
    # the left are done and don't need to be touched. Per-tab because the same
    # film may sit at different column indexes in each tab.
    start_norm = normalize_title(START_FROM_TITLE)

    def trim_to_start(titles: dict) -> dict:
        start_info = titles.get(start_norm)
        if not start_info:
            log(f"  WARNING: START_FROM_TITLE {START_FROM_TITLE!r} not in tab headers; "
                f"processing all titles")
            return titles
        start_idx = start_info['col_idx']
        return {n: info for n, info in titles.items() if info['col_idx'] >= start_idx}

    fs_titles_active = trim_to_start(fs_titles)
    ns_titles_active = trim_to_start(ns_titles)
    log(f"  After START_FROM_TITLE={START_FROM_TITLE!r}: "
        f"Fri-Sat {len(fs_titles_active)} active, NON SOF {len(ns_titles_active)} active")

    # Union of titles across both tabs — process every active film that appears in either.
    all_norms = set(fs_titles_active) | set(ns_titles_active)

    # 2. Comscore login
    log("\n[2/3] Logging into Comscore ...")
    title_cache = _load_title_cache()
    log(f"  -> title cache has {len(title_cache)} entries")
    cache_dirty = False

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        ctx = browser.new_context(viewport={'width': 1600, 'height': 900})
        page = ctx.new_page()

        if not login_to_comscore(page):
            log("  ERROR: Comscore login failed")
            browser.close()
            sys.exit(2)

        page.goto(f"{COMSCORE_BASE}/home", wait_until="domcontentloaded", timeout=60_000)
        page.wait_for_timeout(1500)

        # 3. Iterate films, pull daily data, write cells
        log("\n[3/3] Processing each title ...")
        summary = []
        for norm in sorted(all_norms):
            display = (fs_titles.get(norm) or ns_titles.get(norm))['display']

            # Lookup title_no + opening_friday (cache → Comscore)
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
                log(f"\n  • {display!r}: not fully cached, looking up ...")
                result = find_title_no_by_search(page, display)
                if not result:
                    summary.append((display, 'no Comscore match'))
                    continue
                title_no = result['title_no']
                release_date = parse_release_date(result.get('release_date', ''))
                if release_date is None:
                    log(f"    -> couldn't parse release date {result.get('release_date')!r}; skipping")
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

            # Compute target week N for this film
            days_since_open = (target_friday - opening_friday).days
            if days_since_open < 0:
                log(f"    -> film opens {opening_friday}, not yet open; skipping")
                summary.append((display, f'not yet opened (opens {opening_friday})'))
                continue
            if days_since_open % 7 != 0:
                log(f"    -> WARNING: target_friday {target_friday} is not aligned with opening "
                    f"{opening_friday} (off by {days_since_open % 7} days)")
            week_n = days_since_open // 7 + 1
            if week_n > MAX_TRACKED_WEEK:
                log(f"    -> WK {week_n} > {MAX_TRACKED_WEEK} (past tracking window); skipping")
                summary.append((display, f'WK {week_n}: past tracking window'))
                continue

            log(f"    -> filling WK {week_n} (opened {opening_friday}, target Fri {target_friday})")

            # Resolve target cells (skip if row doesn't exist in a tab)
            fs_info = fs_titles.get(norm)
            ns_info = ns_titles.get(norm)
            fri_row = fs_rowmap.get((week_n, 'fri'))
            sat_row_fs = fs_rowmap.get((week_n, 'sat'))
            sat_row_ns = ns_rowmap.get((week_n, 'sat'))
            sun_row_ns = ns_rowmap.get((week_n, 'sun'))

            writes = []  # list of (worksheet, range_a1, value, label_for_log)

            if fs_info and fri_row and sat_row_fs:
                fri_cell = f"{col_letter(fs_info['col_idx'])}{fri_row}"
                sat_cell_fs = f"{col_letter(fs_info['col_idx'])}{sat_row_fs}"
                writes.append((fs_ws, fri_cell, 'fri', f"Fri-Sat tab Fri ({fri_cell})"))
                writes.append((fs_ws, sat_cell_fs, 'sat', f"Fri-Sat tab Sat ({sat_cell_fs})"))
            elif fs_info:
                log(f"    -> Fri-Sat tab has column but missing WK {week_n} row labels; skipping that tab")

            if ns_info and sat_row_ns and sun_row_ns:
                sat_cell_ns = f"{col_letter(ns_info['col_idx'])}{sat_row_ns}"
                sun_cell_ns = f"{col_letter(ns_info['col_idx'])}{sun_row_ns}"
                writes.append((ns_ws, sat_cell_ns, 'sat', f"NON SOF tab Sat ({sat_cell_ns})"))
                writes.append((ns_ws, sun_cell_ns, 'sun', f"NON SOF tab Sun ({sun_cell_ns})"))
            elif ns_info:
                log(f"    -> NON SOF tab has column but missing WK {week_n} row labels; skipping that tab")

            if not writes:
                log(f"    -> no writable cells for WK {week_n}; skipping film")
                summary.append((display, f'WK {week_n}: no rows in either tab'))
                continue

            # Pull each day's data ONCE — multiple writes share the same number.
            day_grosses: dict[str, int] = {}
            day_target = {'fri': target_friday, 'sat': target_saturday, 'sun': target_sunday}
            needed_days = sorted({w[2] for w in writes}, key=lambda d: ['fri', 'sat', 'sun'].index(d))
            for day in needed_days:
                log(f"    [{day.upper()}] pulling {day_target[day]} ...")
                data = fetch_single_day_gross(page, title_no, day_target[day])
                day_grosses[day] = data['gross']

            log(f"    daily grosses: Fri=${day_grosses.get('fri', 0):,} "
                f"Sat=${day_grosses.get('sat', 0):,} Sun=${day_grosses.get('sun', 0):,}")

            # Pre-read existing cells to honor --force semantics
            existing_by_ws: dict[int, dict[str, str]] = {}
            for ws, rng, _day, _lbl in writes:
                wsid = id(ws)
                existing_by_ws.setdefault(wsid, {'_ws': ws, 'ranges': []})
                existing_by_ws[wsid]['ranges'].append(rng)
            for wsid, bundle in existing_by_ws.items():
                ws = bundle['_ws']
                vals = ws.batch_get(bundle['ranges'])
                bundle['values'] = {
                    rng: ((vals[i][0][0] if vals[i] and vals[i][0] else '').strip())
                    for i, rng in enumerate(bundle['ranges'])
                }

            # Decide which writes to apply
            to_apply_by_ws: dict[int, list[dict]] = {}
            applied_summary = []
            for ws, rng, day, label in writes:
                gross = day_grosses[day]
                existing = existing_by_ws[id(ws)]['values'].get(rng, '')
                if existing and not args.force:
                    log(f"    -> {label} already filled ({existing!r}); use --force to overwrite, skipping")
                    applied_summary.append(f"{label}: skipped (filled)")
                    continue
                if args.dry_run:
                    log(f"    [dry-run] would write ${gross:,} → {label}")
                    applied_summary.append(f"{label}: ${gross:,} (dry-run)")
                    continue
                to_apply_by_ws.setdefault(id(ws), {'_ws': ws, 'updates': []})
                # Write as currency-formatted string + USER_ENTERED so Sheets
                # parses it as a number AND auto-applies currency display format.
                # This way cells render as $X,XXX.00 even when the cell didn't
                # have currency formatting pre-applied. Matches O Canada's pattern.
                to_apply_by_ws[id(ws)]['updates'].append({
                    'range': rng, 'values': [[f"${gross:,}.00"]],
                })
                applied_summary.append(f"{label}: ${gross:,}")

            for bundle in to_apply_by_ws.values():
                bundle['_ws'].batch_update(bundle['updates'], value_input_option='USER_ENTERED')

            summary.append((display, f"WK {week_n}: " + "; ".join(applied_summary)))

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
