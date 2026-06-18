"""One-off: extend `=-1+(Later/Earlier)` % formulas down Animal Farm's column
on both tabs of the Weekend Percentage Drop Box Office sheet.

- Fri-Sat tab: pair = (Fri = earlier, Sat = later)
- NON SOF tab: pair = (Sat = earlier, Sun = later)

We identify % rows by scanning the AVERAGE % DROP column for `=AVERAGE(...)`,
then for each such row we walk up column A to find the most recent Sat/Sun
(or Fri/Sat) label pair from the SAME week. Then we write
`=-1+(<af><later_row>/<af><earlier_row>)` if Animal Farm's cell is blank.
"""
import re
import sys
from pathlib import Path
import gspread

SHEET_ID = "1yTnuoNh923ibhQNA1tTrwfltF339oXXvPMDe3CHE1vE"
CREDS_PATH = Path(__file__).parent / "creds" / "sheets-service-account.json"
TARGET_TITLE = "Animal Farm"

# Per-tab: (earlier_day, later_day)
TAB_DAYS = {
    "Fri - Sat": ('fri', 'sat'),
    "NON SOF":   ('sat', 'sun'),
}

OPENING_RE = re.compile(r'^opening\s+(fri|sat|sun)', re.IGNORECASE)
WK_RE      = re.compile(r'^wk\s*(\d+)\s+(fri|sat|sun)', re.IGNORECASE)
BARE_RE    = re.compile(r'^(fri|sat|sun)(day)?$', re.IGNORECASE)


def col_letter(idx0):
    s = ''
    n = idx0 + 1
    while n:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s


def build_rowmap(col_a):
    """col A labels -> {(week_n, day): row_1_based}"""
    out = {}
    current_week = None
    for i, val in enumerate(col_a):
        v = (val or '').strip()
        if not v:
            continue
        m = OPENING_RE.match(v)
        if m:
            out[(1, m.group(1).lower())] = i + 1
            current_week = 1
            continue
        m = WK_RE.match(v)
        if m:
            n = int(m.group(1))
            out[(n, m.group(2).lower())] = i + 1
            current_week = n
            continue
        m = BARE_RE.match(v)
        if m and current_week is not None:
            out[(current_week, m.group(1).lower())] = i + 1
    return out


def backfill_tab(ws, tab_name, target_title=TARGET_TITLE):
    earlier_day, later_day = TAB_DAYS[tab_name]
    row1 = ws.row_values(1)
    target_idx = next((i for i, h in enumerate(row1)
                       if (h or '').strip().lower() == target_title.lower()), None)
    if target_idx is None:
        print(f"  '{target_title}' not found")
        return 0
    target_letter = col_letter(target_idx)
    avg_letter    = col_letter(len(row1) - 1)
    print(f"  target col: {target_letter}, average col: {avg_letter}, "
          f"pair: ({earlier_day}, {later_day})")

    col_a = ws.col_values(1)
    rowmap = build_rowmap(col_a)
    weeks = sorted({w for (w, _) in rowmap.keys()})
    print(f"  weeks found in col A: {weeks}")

    # Identify all % rows by scanning the AVERAGE column for =AVERAGE formulas
    avg_range = f"{avg_letter}1:{avg_letter}80"
    avg_vals = ws.get(avg_range, value_render_option='FORMULA')
    pct_rows = [i + 1 for i, r in enumerate(avg_vals)
                if r and isinstance(r[0], str) and r[0].startswith('=AVERAGE')]
    print(f"  pct rows (from =AVERAGE in {avg_letter}): {pct_rows}")

    # Read the entire target column as formulas, to see what's already there
    target_range = f"{target_letter}1:{target_letter}{max(pct_rows) + 2}"
    target_vals = ws.get(target_range, value_render_option='FORMULA')
    flat_target = [(r[0] if r else '') for r in target_vals]
    while len(flat_target) <= max(pct_rows):
        flat_target.append('')

    updates = []
    for pct_row in pct_rows:
        existing = flat_target[pct_row - 1]
        if existing not in ('', None):
            continue
        # Find the week whose later_day row is just above pct_row.
        candidate_week = None
        for wk in weeks:
            later_row = rowmap.get((wk, later_day))
            earlier_row = rowmap.get((wk, earlier_day))
            if later_row is None or earlier_row is None:
                continue
            if later_row < pct_row and (candidate_week is None
                                        or later_row > rowmap[(candidate_week, later_day)]):
                candidate_week = wk
        if candidate_week is None:
            print(f"    row {pct_row}: no Sat/Sun pair above, skipping")
            continue
        earlier_row = rowmap[(candidate_week, earlier_day)]
        later_row   = rowmap[(candidate_week, later_day)]
        formula = f"=-1+({target_letter}{later_row}/{target_letter}{earlier_row})"
        addr = f"{target_letter}{pct_row}"
        updates.append({'range': addr, 'values': [[formula]]})
        print(f"    {addr} <- {formula}  (WK {candidate_week})")

    if updates:
        ws.batch_update(updates, value_input_option='USER_ENTERED')
        print(f"  wrote {len(updates)} formulas")
    else:
        print("  nothing to backfill")
    return len(updates)


def main():
    if not CREDS_PATH.exists():
        sys.exit(f"ERROR: SA creds not found at {CREDS_PATH}")
    gc = gspread.service_account(filename=str(CREDS_PATH))
    sh = gc.open_by_key(SHEET_ID)
    total = 0
    for tab in TAB_DAYS:
        print(f"\n[{tab}]")
        total += backfill_tab(sh.worksheet(tab), tab)
    print(f"\nDone — {total} formulas written")


if __name__ == '__main__':
    main()
