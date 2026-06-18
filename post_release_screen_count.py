#!/usr/bin/env python3
"""
Post-Release Screen Count — weeks-after-release "# of runs" tracker
===================================================================
Sister to screen_count_update.py (pre-release). For each Angel film currently
1..7 weeks PAST its release, reads how many venues are playing that week from
MICA's Bookings -> Playweeks page and writes the count into the post-release
Google Sheet ("Weeks after release" tab), one cell per film per week.

This is READ-ONLY against MICA — it only sets search filters and reads the
"Filtered: N" count above the playweeks table; it never edits any booking.

Manual workflow this automates (Tommy's walkthrough, 2026-06-02):
  Bookings -> Playweeks (app.mica.co/bookings/review) ->
  Production(s) = the film -> Status = Select All then remove Cancelled + No show
  (leaving Confirmed, Returns in, Partial returns, Invoiced, Expected, No invoice)
  -> Start Date = the week's Friday, End Date = that Friday + 6 days (Thursday)
  -> leave All/Open/Paid/Overpaid on All -> Search -> read "Filtered: N".
  Counts include ALL countries (US + CA + PR).

Week mapping:
  Play-weeks run Friday -> Thursday. Week 1 = the play-week starting on the
  release Friday. On run-Friday F a film's week = (F - releaseFriday)/7 + 1.
  Each Friday only the CURRENT week's column is written, and an already-filled
  cell is never overwritten (snapshots are point-in-time; MICA drifts after).

Sheet: 1eQRg2pcpC2B6fXhBWvwB0NCsT_5m4t3qnFQGSxLUvJM, tab "Weeks after release".
Column map (0-based): A Title · B Release date ·
  C Wk1 # · D Wk2 # · E Wk2 % · F Wk3 # · G Wk3 % · H Wk4 # · I Wk4 % ·
  J Wk5 # · K Wk5 % · L Wk6 # · M Wk6 % · N Wk7 # · O Wk7 %.
The % columns are sheet formulas =(thisN - prevN)/prevN; the script writes only
the # columns and back-fills a missing % formula on new film rows.

Run modes
---------
  python post_release_screen_count.py                    # all films, this week's column
  python post_release_screen_count.py --dry-run          # read MICA, preview, no write
  python post_release_screen_count.py --only "Animal Farm"
  python post_release_screen_count.py --today 2026-06-05 # pretend it's this date
  python post_release_screen_count.py --force            # overwrite a non-empty cell

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
SHEET_TAB = "Weeks after release"
CREDS_PATH = Path(__file__).parent / "creds" / "sheets-service-account.json"

PLAYWEEKS_URL = "https://app.mica.co/bookings/review"

# 0-based columns in the "Weeks after release" tab
COL_TITLE   = 0   # A
COL_RELEASE = 1   # B
# week number -> 0-based # column
WEEK_NUM_COL = {1: 2, 2: 3, 3: 5, 4: 7, 5: 9, 6: 11, 7: 13}   # C D F H J L N
# week number -> 0-based % column (weeks 2..7); % = (thisN - prevN)/prevN
WEEK_PCT_COL = {2: 4, 3: 6, 4: 8, 5: 10, 6: 12, 7: 14}        # E G I K M O
MAX_WEEK = 7

# Playweek statuses to count: everything EXCEPT Cancelled and No show.
COUNT_STATUSES = ["Confirmed", "Returns in", "Partial returns",
                  "Invoiced", "Expected", "No invoice"]

# DOM ids on the Playweeks page (verified 2026-06-02)
NG_PRODUCTION = "search-productions-select"
NG_STATUS     = "search-playweek-status-select"
DATE_START_ID = "periodStartDate"
DATE_END_ID   = "periodEndDate"


def log(msg: str) -> None:
    print(msg, flush=True)


# ──────────────────────────────────────────────────────────────────────────────
# Dates / week mapping
# ──────────────────────────────────────────────────────────────────────────────

def col_letter(idx: int) -> str:
    """0-based col index -> A1 letter (0 -> A, 25 -> Z, 26 -> AA)."""
    s, n = "", idx + 1
    while n:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s


def parse_sheet_date(s: str) -> date | None:
    """Parse a release-date cell. Tolerates a trailing '?' and several formats."""
    if not s:
        return None
    s = s.strip().rstrip("?").strip()
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


def target_friday(today: date) -> date:
    """The Friday whose play-week we snapshot, by day of week (Tommy, 2026-06-02):
      • Tue/Wed/Thu → the UPCOMING Friday (pull early — get a jump on the coming week).
      • Fri        → that day's Friday (the scheduled snapshot).
      • Sat/Sun/Mon → the most recent Friday on/before (the current/just-started week);
                      a Monday manual pull thus targets that week and can backfill it
                      if the Friday run was missed.
    Either way the date range is that Friday → the following Thursday."""
    wd = today.weekday()  # Mon=0 .. Fri=4 .. Sun=6
    if wd in (1, 2, 3):                       # Tue/Wed/Thu → upcoming Friday
        return today + timedelta(days=(4 - wd) % 7)
    return today - timedelta(days=(wd - 4) % 7)  # Fri/Sat/Sun/Mon → most recent Friday


def week1_friday(release: date) -> date:
    """Friday of the opening play-week. For Friday releases this is the release date
    itself. (Thursday-release handling, e.g. Hershey, is deferred — see module docs.)"""
    return release - timedelta(days=(release.weekday() - 4) % 7)


def weeks_after_release(today: date, release: date) -> int | None:
    """Which post-release week the snapshot targets (1 = opening play-week), based on
    the upcoming Friday. None if that week is before release or past week MAX_WEEK."""
    start = target_friday(today)
    delta_weeks = (start - week1_friday(release)).days // 7
    week = delta_weeks + 1
    return week if 1 <= week <= MAX_WEEK else None


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
                svc_email = (json.loads(json_blob)["client_email"] if json_blob
                             else json.load(open(CREDS_PATH))["client_email"])
            except Exception:
                svc_email = "the service account"
            sys.exit(
                "ERROR: The Screen Count Google Sheet is not shared with the service account.\n"
                f"Fix: open the sheet and share it as Editor with:\n  {svc_email}\n"
                f"  Sheet: https://docs.google.com/spreadsheets/d/{SHEET_ID}/edit\n"
            )
        raise


def read_film_rows(ws) -> list[dict]:
    """Return [{'title', 'release', 'row'}] for every data row with a title."""
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
# MICA Playweeks — filter + read "Filtered: N" (read-only)
# ──────────────────────────────────────────────────────────────────────────────

def _norm_title(s: str) -> str:
    """Lowercase, drop a trailing ' (YYYY)', collapse non-alnum for matching."""
    s = re.sub(r"\(\s*\d{4}\s*\)", " ", s).lower()
    return re.sub(r"[^a-z0-9]+", " ", s).strip()


def _open_ng(page, sel_id: str) -> None:
    page.eval_on_selector(
        f'#{sel_id} .ng-select-container',
        "el => el.dispatchEvent(new MouseEvent('mousedown', {bubbles:true}))",
    )
    page.wait_for_timeout(600)


def _ng_options(page) -> list[str]:
    return page.evaluate(
        "() => Array.from(document.querySelectorAll('.ng-dropdown-panel .ng-option'))"
        "        .map(o => o.textContent.trim()).filter(Boolean)"
    )


def _click_option(page, text: str, exact: bool = False) -> str | None:
    return page.evaluate(
        """([text, exact]) => {
            const opts = Array.from(document.querySelectorAll('.ng-dropdown-panel .ng-option'));
            const o = opts.find(e => exact ? e.textContent.trim() === text
                                          : e.textContent.trim().toLowerCase().includes(text.toLowerCase()));
            if (o) { o.dispatchEvent(new MouseEvent('click', {bubbles:true})); return o.textContent.trim(); }
            return null;
        }""",
        [text, exact],
    )


def _clear_ng(page, sel_id: str) -> None:
    page.evaluate(
        """(id) => {
            const w = document.querySelector('#'+id+' .ng-clear-wrapper');
            if (w) w.dispatchEvent(new MouseEvent('click', {bubbles:true}));
        }""",
        sel_id,
    )
    page.wait_for_timeout(400)


def _set_date(page, input_id: str, value: str) -> None:
    page.fill(f'#{input_id}', "")
    page.fill(f'#{input_id}', value)
    page.dispatch_event(f'#{input_id}', 'input')
    page.dispatch_event(f'#{input_id}', 'change')
    page.keyboard.press("Tab")
    page.wait_for_timeout(300)


def _read_filtered(page) -> int | None:
    try:
        txt = page.evaluate("() => document.body ? document.body.innerText : ''")
    except Exception:
        return None
    m = re.search(r"Filtered:\s*([\d,]+)", txt, re.IGNORECASE)
    return int(m.group(1).replace(",", "")) if m else None


def navigate_to_playweeks(page, ctx) -> None:
    """Open the Bookings -> Playweeks page, logging in if needed."""
    log("Navigating to Bookings → Playweeks ...")
    page.goto(PLAYWEEKS_URL, wait_until="domcontentloaded", timeout=40_000)
    page.wait_for_timeout(2_500)
    if bp._is_login_page(page):
        bp._do_login(page, ctx)
        page.goto(PLAYWEEKS_URL, wait_until="domcontentloaded", timeout=40_000)
    page.wait_for_timeout(3_500)
    bp._dismiss_popups(page)
    try:
        page.wait_for_selector(f'#{NG_PRODUCTION}', timeout=15_000)
    except PlaywrightTimeout:
        bp._screenshot(page, "pr_playweeks_load_failed.png")
        sys.exit(f"ERROR: Playweeks page did not load (url={page.url})")


def select_statuses(page) -> list[str]:
    """Select the six counted statuses (all except Cancelled + No show). Once per run."""
    _clear_ng(page, NG_STATUS)
    _open_ng(page, NG_STATUS)
    applied = []
    for s in COUNT_STATUSES:
        r = _click_option(page, s, exact=True)
        if r:
            applied.append(r)
        else:
            log(f"  WARNING: status option '{s}' not offered")
        page.wait_for_timeout(250)
    page.keyboard.press("Escape")
    page.wait_for_timeout(400)
    log(f"  Statuses applied: {applied}")
    return applied


def select_production(page, title: str) -> str | None:
    """Clear then select the production matching `title`. Returns the chosen label."""
    _clear_ng(page, NG_PRODUCTION)
    _open_ng(page, NG_PRODUCTION)
    try:
        page.locator(f'#{NG_PRODUCTION} input').first.type(title, delay=35)
    except Exception:
        pass
    page.wait_for_timeout(1_200)
    opts = _ng_options(page)
    want = _norm_title(title)
    # Prefer an option whose normalized text equals the title; else startswith; else contains.
    chosen = (next((o for o in opts if _norm_title(o) == want), None)
              or next((o for o in opts if _norm_title(o).startswith(want)), None)
              or next((o for o in opts if want in _norm_title(o)), None))
    if not chosen:
        log(f"  ERROR: no production option matches '{title}' (options: {opts[:8]})")
        page.keyboard.press("Escape")
        return None
    _click_option(page, chosen, exact=True)
    page.keyboard.press("Escape")
    page.wait_for_timeout(500)
    return chosen


def pull_week_count(page, start: date, end: date) -> int | None:
    """Set the week dates, Search, return the Filtered venue count."""
    _set_date(page, DATE_START_ID, start.strftime("%m/%d/%Y"))
    _set_date(page, DATE_END_ID, end.strftime("%m/%d/%Y"))
    page.locator('button:has-text("Search")').first.click()
    page.wait_for_timeout(3_800)
    return _read_filtered(page)


# ──────────────────────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true", help="read MICA + preview, no sheet write")
    ap.add_argument("--force", action="store_true", help="overwrite a non-empty target cell")
    ap.add_argument("--only", default="", help="only process films whose title contains this")
    ap.add_argument("--today", default="", help="pretend today is YYYY-MM-DD")
    args = ap.parse_args()

    today = date.today()
    if args.today:
        today = datetime.strptime(args.today, "%Y-%m-%d").date()
    log(f"Post-Release Screen Count — today={today:%Y-%m-%d} ({today:%A}) "
        f"target play-week Fri={target_friday(today):%m/%d/%Y} "
        f"{'[DRY RUN]' if args.dry_run else ''}")

    ws = open_worksheet()
    films = read_film_rows(ws)
    if args.only:
        needle = args.only.lower()
        films = [f for f in films if needle in f["title"].lower()]

    # Decide each film's target week + column.
    targets = []
    for f in films:
        if not f["release"]:
            log(f"  SKIP {f['title']}: no release date in sheet")
            continue
        week = weeks_after_release(today, f["release"])
        if week is None:
            continue  # not currently 1..7 weeks post-release
        col = WEEK_NUM_COL[week]
        start = target_friday(today)
        end = start + timedelta(days=6)
        targets.append({**f, "week": week, "col": col, "start": start, "end": end})
        log(f"  TARGET {f['title']}: week {week} (play-week {start:%m/%d}–{end:%m/%d}) "
            f"-> {col_letter(col)}{f['row']}")

    if not targets:
        log("No films are 1..7 weeks post-release this week. Nothing to do.")
        return

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=bp._HEADLESS, slow_mo=bp._SLOW_MO, args=bp._BROWSER_ARGS)
        storage = str(bp.AUTH_FILE) if bp.AUTH_FILE.exists() else None
        ctx = browser.new_context(storage_state=storage, viewport={"width": 1700, "height": 1000})
        page = ctx.new_page()
        try:
            navigate_to_playweeks(page, ctx)
            select_statuses(page)

            for t in targets:
                log(f"\n=== {t['title']} (week {t['week']}) ===")
                cell = f"{col_letter(t['col'])}{t['row']}"

                # Skip a cell that already has a value (snapshots are point-in-time).
                existing = (ws.acell(cell).value or "").strip()
                if existing and not args.force:
                    log(f"  SKIP: {cell} already has '{existing}' (use --force to overwrite)")
                    continue

                label = select_production(page, t["title"])
                if not label:
                    log(f"  {t['title']}: production not found — skipping")
                    continue
                count = pull_week_count(page, t["start"], t["end"])
                log(f"  {label}: Filtered (week {t['week']}, {t['start']:%m/%d}–{t['end']:%m/%d}) = {count}")
                if count is None:
                    log("  could not read a count — skipping write")
                    continue

                if args.dry_run:
                    log(f"  DRY RUN: would write {count} -> {cell}")
                    continue

                ws.update_acell(cell, count)
                log(f"  ✓ wrote {count} -> {cell}")

                # Back-fill the week-over-week % formula on a new row that lacks it
                # (weeks 2..7; the % is the column immediately right of the # column).
                if t["week"] in WEEK_PCT_COL:
                    pct_cell = f"{col_letter(WEEK_PCT_COL[t['week']])}{t['row']}"
                    prev_cell = f"{col_letter(WEEK_NUM_COL[t['week'] - 1])}{t['row']}"
                    if not (ws.acell(pct_cell).value or "").strip() and \
                       (ws.acell(prev_cell).value or "").strip():
                        formula = f"=({cell}-{prev_cell})/{prev_cell}"
                        # USER_ENTERED so "=..." is stored as a formula, not text.
                        ws.update_acell(pct_cell, formula)
                        # New rows have no number format — match the existing 0.00% style.
                        try:
                            ws.format(pct_cell, {"numberFormat": {"type": "PERCENT", "pattern": "0.00%"}})
                        except Exception as exc:
                            log(f"  (note: could not set % format on {pct_cell}: {exc})")
                        log(f"  ✓ set % formula {formula} -> {pct_cell} (0.00%)")
        finally:
            if bp._SERVER_MODE:
                ctx.close()
                browser.close()

    log("\nDone.")


if __name__ == "__main__":
    main()
