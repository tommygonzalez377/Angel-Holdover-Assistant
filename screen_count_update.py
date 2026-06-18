#!/usr/bin/env python3
"""
Screen Count Update — pre-release "# of runs" tracker
=====================================================
For each upcoming Angel film, reads how many opening-week locations are
Agreed + Booked in MICA and writes the count into the pre-release screen-count
Google Sheet ("Updated Chart" tab), one cell per film per week.

This is READ-ONLY against MICA. "Changing the dates" in the manual workflow
means picking Start-Date *filter* values on the Venues table — it NEVER edits
venue records. We only read the "Filtered: N" count.

Manual workflow this automates:
  Sales -> Plans -> click film title -> use the "US, CA, PR" plan ->
  Venues table: Start Date filter = every booked date in the opening-week
  window (Thu before opening .. following Thu) + Status filter = Agreed AND
  Booked -> read "Filtered: N" -> write N to the film's row + week's column.

Update cadence:
  - Release-countdown columns (7wk .. 1wk before release, "at opening") -> Fridays
  - Tickets-on-sale columns (1 wk bf TOS, by TOS) -> Wednesdays
  (TOS columns require per-film TOS dates which are not yet wired; countdown
  columns are fully implemented.)

Sheet: 1eQRg2pcpC2B6fXhBWvwB0NCsT_5m4t3qnFQGSxLUvJM, tab "Updated Chart".
Column map (0-based): A Title · B Release date · C 1wk bf TOS · D by TOS ·
E 7wk · F 6wk · G 5wk · H 4wk · I 3wk · J 2wk · K 1wk bf release · L at opening ·
M # scrns widest · N Screens added Mon · O Notes.

Run modes
---------
  python screen_count_update.py                         # all films, today's column
  python screen_count_update.py --dry-run               # read MICA, preview, no write
  python screen_count_update.py --only "Young Washington"
  python screen_count_update.py --today 2026-05-29      # pretend it's this date
  python screen_count_update.py --mode prod             # MICA prod (default)
  python screen_count_update.py --force                 # overwrite a non-empty cell

Expects env vars:
  MICA_USERNAME / MICA_PASSWORD          — MICA login
  GSHEETS_SERVICE_ACCOUNT_JSON           — service-account JSON (Fly secret)
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
from zoneinfo import ZoneInfo

# The schedule (and the team) think in Pacific. The Fly container clock is UTC, so an
# evening cron fire (6 PM PT) is already "tomorrow" in UTC — `date.today()` would roll a
# day and put the write in the wrong week/column. Always derive "today" in Pacific.
_LOCAL_TZ = ZoneInfo("America/Los_Angeles")

try:
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")
except Exception:
    pass

try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).parent / ".env")
except ImportError:
    pass

import gspread
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout

import booking_plan_update as bp

SHEET_ID  = "1eQRg2pcpC2B6fXhBWvwB0NCsT_5m4t3qnFQGSxLUvJM"
SHEET_TAB = "Updated Chart"
CREDS_PATH = Path(__file__).parent / "creds" / "sheets-service-account.json"
OUTPUT_DIR = Path(__file__).parent / "output"

# 0-based column indices in the "Updated Chart" tab
COL_TITLE        = 0   # A
COL_RELEASE      = 1   # B
COL_TOS_1WK_BF   = 2   # C  (Wednesday cadence)
COL_TOS_BY       = 3   # D  (Wednesday cadence)
COL_7WK          = 4   # E
# F..K = 6wk..1wk before release; L = at opening
COL_AT_OPENING   = 11  # L
# weeks_before_release -> 0-based column index (Friday cadence)
WEEK_COL = {7: 4, 6: 5, 5: 6, 4: 7, 3: 8, 2: 9, 1: 10, 0: COL_AT_OPENING}

# ── Tickets-on-sale (TOS) dates ────────────────────────────────────────────────
# Tickets go on sale 8 weeks before release, on the WEDNESDAY of that week
# (e.g. Young Washington, release 7/3/2026, went on sale Wed 5/6/2026).
# So TOS is normally computed from the release date — see tos_from_release().
# _TOS_DATES_RAW is an OPTIONAL per-film override (lowercased title -> "M/D/YYYY")
# for any film that doesn't follow the 8-weeks-before rule.
_TOS_DATES_RAW: dict[str, str] = {
    # "some film": "6/17/2026",
}
_TOS_WEEKS_BEFORE_RELEASE = 8


def log(msg: str) -> None:
    print(msg, flush=True)


# ──────────────────────────────────────────────────────────────────────────────
# Dates / window
# ──────────────────────────────────────────────────────────────────────────────

def col_letter(idx: int) -> str:
    """0-based col index -> A1 letter (0 -> A, 25 -> Z, 26 -> AA)."""
    s, n = "", idx + 1
    while n:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s


def parse_sheet_date(s: str) -> date | None:
    """Parse a release date cell. Accepts M/D/YYYY, M/D/YY, YYYY-MM-DD, 'July 3, 2026'."""
    if not s:
        return None
    s = s.strip()
    for fmt in ("%m/%d/%Y", "%m/%d/%y", "%Y-%m-%d", "%B %d, %Y", "%b %d, %Y"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    m = re.search(r"(\d{1,2})/(\d{1,2})/(\d{2,4})", s)
    if m:
        mo, dy, yr = int(m.group(1)), int(m.group(2)), int(m.group(3))
        if yr < 100:
            yr += 2000
        try:
            return date(yr, mo, dy)
        except ValueError:
            return None
    return None


def opening_week_window(release: date) -> tuple[date, date]:
    """Opening-week window matched against MICA venue Start Dates.

    MICA stores each venue's Start Date as the *play-week-start Monday*, so the
    window must begin at the Monday of the play-week that contains the release
    Friday (e.g. release 07/03 -> Monday 06/29). Tentpole titles like Young
    Washington put their wide opening-week venues on that Monday. The window ends
    the following Thursday so anything genuinely pushed to a later play-week is
    excluded. Most films sit on the release Friday and fall inside either way.
    """
    monday = release - timedelta(days=release.weekday())  # Mon=0 .. Fri=4 -> -4 days
    following_thursday = release + timedelta(days=6)
    return monday, following_thursday


def weeks_before_release(today: date, release: date) -> int | None:
    """How many whole weeks before release `today` falls — by the Friday of today's week.

    Every day Mon–Sun maps to the SAME week, namely the Friday of that Mon–Sun calendar
    week. e.g. with a 7/3 release: Mon 6/1..Sun 6/7 all -> Fri 6/5 (4 wk); Mon 6/8..Sun
    6/14 all -> Fri 6/12 (3 wk). This matches how the team counts ("4 weeks out" on
    Monday) AND keeps the weekend on the SAME week as the Friday that just passed — so a
    Saturday/Sunday pull refreshes the current week instead of jumping to next week.
    Returns 7..1 for the countdown columns, 0 for the opening week ("at opening"),
    or None if today is outside the 7-weeks-before .. opening window.
    """
    # Friday of the Mon–Sun week containing `today`
    week_friday    = today - timedelta(days=today.weekday()) + timedelta(days=4)
    release_friday = release - timedelta(days=(release.weekday() - 4) % 7)
    delta_weeks = (release_friday - week_friday).days // 7
    if 0 <= delta_weeks <= 7:
        return delta_weeks
    return None


def _tos_overrides() -> dict[str, date]:
    """Parse _TOS_DATES_RAW into {lowercased title: date}, skipping unparseable rows."""
    out = {}
    for title, raw in _TOS_DATES_RAW.items():
        d = parse_sheet_date(raw)
        if d:
            out[title.strip().lower()] = d
    return out


def tos_from_release(release: date) -> date:
    """Tickets-on-sale date = the Wednesday of the week N weeks before release.

    YW (release 7/3/2026) -> Wed 5/6/2026; Brink of War (8/14) -> Wed 6/17.
    """
    nweeks = release - timedelta(days=7 * _TOS_WEEKS_BEFORE_RELEASE)
    return nweeks - timedelta(days=(nweeks.weekday() - 2) % 7)  # back to Wednesday of that week


def tos_target_column(today: date, tos_date: date) -> int | None:
    """Which TOS column (if any) this Wednesday `today` should fill for `tos_date`.

    Tickets go on sale on a Wednesday, so:
      D "by TOS"      = the on-sale Wednesday itself (first Wednesday on/after tos_date)
      C "1 wk bf TOS" = the Wednesday one week before D
    (Computed as a straddle so it's still correct if an override TOS lands mid-week.)
    Returns COL_TOS_BY, COL_TOS_1WK_BF, or None when today isn't a TOS Wednesday.
    """
    d_wed = tos_date + timedelta(days=(2 - tos_date.weekday()) % 7)  # first Wed on/after TOS
    c_wed = d_wed - timedelta(days=7)
    if today == d_wed:
        return COL_TOS_BY
    if today == c_wed:
        return COL_TOS_1WK_BF
    return None


# ──────────────────────────────────────────────────────────────────────────────
# Google Sheets
# ──────────────────────────────────────────────────────────────────────────────

def open_worksheet():
    json_blob = os.getenv("GSHEETS_SERVICE_ACCOUNT_JSON", "").strip()
    if json_blob:
        tmp = tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False, encoding="utf-8")
        tmp.write(json_blob)
        tmp.close()
        gc = gspread.service_account(filename=tmp.name)
    elif CREDS_PATH.exists():
        gc = gspread.service_account(filename=str(CREDS_PATH))
    else:
        sys.exit(
            "ERROR: no Google Sheets credentials found.\n"
            f"Set GSHEETS_SERVICE_ACCOUNT_JSON env var, or place creds at:\n  {CREDS_PATH}"
        )
    try:
        return gc.open_by_key(SHEET_ID).worksheet(SHEET_TAB)
    except (gspread.exceptions.APIError, PermissionError) as exc:
        is_403 = isinstance(exc, PermissionError) or "403" in str(exc)
        if is_403:
            try:
                svc_email = json.loads(json_blob)["client_email"] if json_blob else json.load(open(CREDS_PATH))["client_email"]
            except Exception:
                svc_email = "the service account"
            sys.exit(
                "ERROR: The Screen Count Google Sheet is not shared with the service account.\n"
                f"Fix: open the sheet and share it as Editor with:\n  {svc_email}\n"
                f"  Sheet: https://docs.google.com/spreadsheets/d/{SHEET_ID}/edit\n"
                "Then click Run Screen Count Now again."
            )
        raise


def read_film_rows(ws) -> list[dict]:
    """Return [{'title', 'release', 'row'}] for every data row with a title + release date."""
    values = ws.get_all_values()
    films = []
    for i, row in enumerate(values):
        if i == 0:  # header
            continue
        title = (row[COL_TITLE] if len(row) > COL_TITLE else "").strip()
        if not title:
            continue
        release = parse_sheet_date(row[COL_RELEASE] if len(row) > COL_RELEASE else "")
        films.append({"title": title, "release": release, "row": i + 1})  # 1-based row
    return films


# ──────────────────────────────────────────────────────────────────────────────
# MICA — open plan + filter venues + read Filtered count (read-only)
# ──────────────────────────────────────────────────────────────────────────────

def _set_mica_mode(mode: str) -> None:
    base = bp.MICA_BASE_URLS.get(mode, bp.MICA_BASE_URLS["prod"])
    bp.MICA_PLANS_URL = f"{base}/plans"
    bp.MICA_LOGIN_URL = f"{base}/auth/login"


def _read_filtered_count(page) -> int | None:
    """Read the 'Filtered: N' number shown above the Venues table."""
    try:
        txt = page.evaluate("() => document.body ? document.body.innerText : ''")
    except Exception:
        return None
    m = re.search(r"Filtered:\s*([\d,]+)", txt, re.IGNORECASE)
    if m:
        return int(m.group(1).replace(",", ""))
    return None


# Venue-table column filters live in a second <thead> row, each cell a
# <th class="filter" id="datatable-th-filter-<field>"> wrapping an ng-select.
STATUS_FILTER_TH    = "datatable-th-filter-planVenueStatusDescription"
STARTDATE_FILTER_TH = "datatable-th-filter-startPlayDate"


def _dump_venue_headers(page) -> list[str]:
    try:
        return page.evaluate("""
        () => Array.from(document.querySelectorAll('table thead tr:first-child th'))
                .map(h => h.textContent.trim().replace(/\\s+/g,' '))
                .filter(Boolean)
        """)
    except Exception:
        return []


def _open_filter(page, th_id: str) -> bool:
    """Open the ng-select filter dropdown in the filter-row <th> with the given id."""
    ok = page.evaluate(
        """
        (thId) => {
            const th = document.getElementById(thId);
            if (!th) return false;
            const ng = th.querySelector('ng-select');
            if (!ng) return false;
            const c = ng.querySelector('.ng-select-container');
            if (!c) return false;
            c.dispatchEvent(new MouseEvent('mousedown', { bubbles: true }));
            return true;
        }
        """,
        th_id,
    )
    if ok:
        page.wait_for_timeout(700)
    return bool(ok)


def _ng_option_labels(page) -> list[str]:
    return page.evaluate(
        """() => Array.from(document.querySelectorAll('.ng-dropdown-panel .ng-option'))
                    .map(o => o.textContent.trim()).filter(Boolean)"""
    )


def _click_ng_option(page, label: str) -> bool:
    ok = page.evaluate(
        """
        (label) => {
            const o = Array.from(document.querySelectorAll('.ng-dropdown-panel .ng-option'))
                          .find(e => e.textContent.trim() === label);
            if (o) { o.dispatchEvent(new MouseEvent('click', { bubbles: true })); return true; }
            return false;
        }
        """,
        label,
    )
    if ok:
        page.wait_for_timeout(500)
    return bool(ok)


def _select_status(page, statuses=("Agreed", "Booked")) -> list[str]:
    """Add each status to the Status filter (multi-select chips). Re-opens the
    dropdown per status — selecting one triggers a table reload that can drop the
    panel, so a single open-and-click-twice is unreliable."""
    applied = []
    for status in statuses:
        if not _open_filter(page, STATUS_FILTER_TH):
            log("  WARNING: Status filter ng-select not found")
            break
        page.wait_for_timeout(300)
        if _click_ng_option(page, status):
            applied.append(status)
        else:
            log(f"  WARNING: Status option '{status}' not offered")
        page.keyboard.press("Escape")
        page.wait_for_timeout(1_000)
    log(f"  Status filters applied: {applied}")
    return applied


def _start_date_options(page) -> list[str]:
    """Read the distinct Start Date values offered by the Start Date filter."""
    if not _open_filter(page, STARTDATE_FILTER_TH):
        log("  WARNING: Start Date filter ng-select not found")
        return []
    opts = _ng_option_labels(page)
    page.keyboard.press("Escape")
    page.wait_for_timeout(300)
    return opts


def _select_start_dates(page, labels: list[str]) -> list[str]:
    """Multi-select the given Start Date option labels."""
    if not labels:
        return []
    if not _open_filter(page, STARTDATE_FILTER_TH):
        return []
    selected = []
    for lab in labels:
        if _click_ng_option(page, lab):  # multi-select keeps panel open
            selected.append(lab)
    page.keyboard.press("Escape")
    page.wait_for_timeout(1_200)
    return selected


def pull_film_count(page, ctx, title: str, mode: str, window: tuple[date, date]) -> int | None:
    """Navigate to the film's US/CA/PR plan, apply the opening-week filters, read Filtered N."""
    log(f"\n=== {title} ===")
    bp._navigate_to_plans(page, ctx)
    bp._search_plans_for_title(page, title)
    if not bp._find_and_click_plan(page, title, mode=mode, plan_desc="US, CA, PR"):
        log(f"  ERROR: no plan row found for '{title}'")
        return None
    try:
        page.wait_for_selector("table", timeout=20_000)
    except PlaywrightTimeout:
        log("  ERROR: venue table did not load")
        return None

    # The venue table is large (thousands of rows) and loads asynchronously —
    # wait until the "Filtered: N" summary appears before touching filters.
    for _ in range(40):
        page.wait_for_timeout(1_500)
        if _read_filtered_count(page) is not None:
            break
    bp._dismiss_popups(page)
    bp._screenshot(page, "sc_plan_detail.png")

    lo, hi = window
    log(f"  Venue table headers: {_dump_venue_headers(page)}")
    log(f"  Total venues (unfiltered): {_read_filtered_count(page)}")

    # 1) Status = Agreed + Booked. This plan's date range IS the opening week,
    #    so its Agreed+Booked venues are opening-week by default.
    _select_status(page, ("Agreed", "Booked"))
    page.wait_for_timeout(1_500)
    total = _read_filtered_count(page)
    log(f"  Filtered (Agreed+Booked): {total}")
    if total is None:
        return None

    # 2) Subtract any Agreed+Booked venue with an EXPLICIT start date outside the
    #    opening-week window (e.g. a venue pushed to week 2). Venues sitting on the
    #    plan default date have no explicit value and never appear as an option, so
    #    they correctly stay counted.
    opts = _start_date_options(page)
    in_window  = [o for o in opts if (d := parse_sheet_date(o)) and lo <= d <= hi]
    out_window = [o for o in opts if (d := parse_sheet_date(o)) and not (lo <= d <= hi)]
    log(f"  Start Date options: {opts}")
    log(f"  In-window dates: {in_window} | Out-of-window dates: {out_window}")

    answer = total
    if out_window:
        _select_start_dates(page, out_window)
        x = _read_filtered_count(page)
        log(f"  Agreed+Booked starting out-of-window {out_window}: {x}")
        if x is not None:
            answer = total - x
    bp._screenshot(page, "sc_after_filters.png")

    log(f"  >>> Opening-week Agreed+Booked (window {lo:%m/%d}-{hi:%m/%d}): {answer}")
    return answer


# ──────────────────────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true", help="read MICA + preview, no sheet write")
    ap.add_argument("--force", action="store_true", help="overwrite a non-empty target cell")
    ap.add_argument("--only", default="", help="only process the film whose title contains this")
    ap.add_argument("--today", default="", help="pretend today is YYYY-MM-DD")
    ap.add_argument("--mode", default="prod", choices=["demo", "prod"])
    args = ap.parse_args()

    _set_mica_mode(args.mode)

    today = datetime.now(_LOCAL_TZ).date()   # Pacific, NOT container-UTC (see _LOCAL_TZ note)
    if args.today:
        today = datetime.strptime(args.today, "%Y-%m-%d").date()
    log(f"Screen Count Update — today={today:%Y-%m-%d} ({today:%A}) mode={args.mode} "
        f"{'[DRY RUN]' if args.dry_run else ''}")

    ws = open_worksheet()
    films = read_film_rows(ws)
    if args.only:
        needle = args.only.lower()
        films = [f for f in films if needle in f["title"].lower()]
    log(f"Films to consider: {[f['title'] for f in films]}")

    # Decide target column per film.
    #   Wednesday → TOS columns (C/D) ONLY (no countdown — that's Friday's job, and
    #     filling a countdown column on Wednesday would pre-empt the Friday snapshot).
    #   Any other day (Friday cadence / manual runs) → release-countdown columns (E–L).
    is_wed    = today.weekday() == 2
    overrides = _tos_overrides()
    targets   = []
    for f in films:
        if not f["release"]:
            log(f"  SKIP {f['title']}: no release date in sheet")
            continue

        col = label = None
        if is_wed:
            tos = overrides.get(f["title"].strip().lower()) or tos_from_release(f["release"])
            tcol = tos_target_column(today, tos)
            if tcol is None:
                log(f"  SKIP {f['title']}: Wednesday, but not a TOS week "
                    f"(on sale {tos:%m/%d/%Y})")
                continue
            col   = tcol
            label = "1 wk bf TOS" if tcol == COL_TOS_1WK_BF else "by TOS"
        else:
            wbr = weeks_before_release(today, f["release"])
            if wbr is None:
                log(f"  SKIP {f['title']}: today is outside the 7wk..opening window "
                    f"(release {f['release']:%m/%d/%Y})")
                continue
            col   = WEEK_COL[wbr]
            label = "at opening" if wbr == 0 else f"{wbr} wk before release"

        targets.append({**f, "col": col, "label": label})
        log(f"  TARGET {f['title']}: {label} -> {col_letter(col)}{f['row']}")

    if not targets:
        log("Nothing to do today.")
        return

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=bp._HEADLESS, slow_mo=bp._SLOW_MO, args=bp._BROWSER_ARGS)
        storage = str(bp.AUTH_FILE) if bp.AUTH_FILE.exists() else None
        ctx = browser.new_context(storage_state=storage, viewport={"width": 1600, "height": 1000})
        page = ctx.new_page()
        try:
            for t in targets:
                window = opening_week_window(t["release"])
                log(f"  Opening-week window for {t['title']}: "
                    f"{window[0]:%m/%d/%Y} .. {window[1]:%m/%d/%Y}")
                count = pull_film_count(page, ctx, t["title"], args.mode, window)
                if count is None:
                    log(f"  {t['title']}: could not read a count — skipping write")
                    continue

                cell = f"{col_letter(t['col'])}{t['row']}"
                existing = (ws.acell(cell).value or "").strip()
                # Always write the live number into the CURRENT week's cell, overwriting any
                # early preview (e.g. a Monday run filled it; the Friday snapshot refreshes it).
                # PAST weeks are never re-targeted — weeks_before_release()/tos_target_column()
                # only ever return the current period's column — so finalized cells are never
                # touched. (Pass --force is no longer needed; overwrite is the default.)
                same = existing == str(count)
                if args.dry_run:
                    if not existing:
                        log(f"  DRY RUN: would write {count} -> {cell} ({t['label']})")
                    elif same:
                        log(f"  DRY RUN: {cell} already {existing}; MICA reads {count} — no change ({t['label']})")
                    else:
                        log(f"  DRY RUN: would update {cell}: {existing} -> {count} ({t['label']})")
                    continue
                ws.update_acell(cell, count)
                if existing and not same:
                    log(f"  ✓ updated {cell}: {existing} -> {count} ({t['label']})")
                else:
                    log(f"  ✓ wrote {count} -> {cell} ({t['label']})")
        finally:
            # Keep browser open in local interactive mode for inspection; close on server.
            if bp._SERVER_MODE:
                ctx.close()
                browser.close()

    log("Done.")


if __name__ == "__main__":
    main()
