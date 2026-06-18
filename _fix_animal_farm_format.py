"""One-off:
  1. Re-write missing `=-1+(later/earlier)` % formulas in Animal Farm's column.
  2. Apply currency cell formatting to Animal Farm's Fri/Sat/Sun data cells
     so raw integers like 1056 display as $1,056.00.

Targets Sat/Sun (or Fri/Sat) cells from col A labels — pct cells are NOT
formatted as currency.
"""
import re
import sys
from pathlib import Path
import gspread

SHEET_ID = "1yTnuoNh923ibhQNA1tTrwfltF339oXXvPMDe3CHE1vE"
CREDS_PATH = Path(__file__).parent / "creds" / "sheets-service-account.json"
TARGET_TITLE = "Animal Farm"

TAB_DAYS = {
    "Fri - Sat": ('fri', 'sat'),
    "NON SOF":   ('sat', 'sun'),
}

OPENING_RE = re.compile(r'^opening\s+(fri|sat|sun)', re.IGNORECASE)
WK_RE      = re.compile(r'^wk\s*(\d+)\s+(fri|sat|sun)', re.IGNORECASE)
BARE_RE    = re.compile(r'^(fri|sat|sun)(day)?$', re.IGNORECASE)

CURRENCY_FORMAT = {
    "numberFormat": {"type": "CURRENCY", "pattern": '"$"#,##0.00'},
}

# Match the existing % cells in the sheet — light green (#d9ead3) + percent
PCT_FORMAT = {
    "numberFormat": {"type": "PERCENT", "pattern": "0.00%"},
    "backgroundColor": {"red": 0.851, "green": 0.918, "blue": 0.827},
}


def col_letter(idx0):
    s = ''
    n = idx0 + 1
    while n:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s


def build_rowmap(col_a):
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


def fix_tab(ws, tab_name, target_title=TARGET_TITLE):
    earlier_day, later_day = TAB_DAYS[tab_name]
    row1 = ws.row_values(1)
    target_idx = next((i for i, h in enumerate(row1)
                       if (h or '').strip().lower() == target_title.lower()), None)
    if target_idx is None:
        print(f"  '{target_title}' not found")
        return
    target_letter = col_letter(target_idx)
    avg_letter    = col_letter(len(row1) - 1)
    print(f"  target col: {target_letter}, avg col: {avg_letter}, "
          f"pair: ({earlier_day}, {later_day})")

    col_a = ws.col_values(1)
    rowmap = build_rowmap(col_a)

    # ── Step 1: backfill % formulas ────────────────────────────────────────
    avg_range = f"{avg_letter}1:{avg_letter}80"
    avg_vals = ws.get(avg_range, value_render_option='FORMULA')
    pct_rows = [i + 1 for i, r in enumerate(avg_vals)
                if r and isinstance(r[0], str) and r[0].startswith('=AVERAGE')]
    target_range = f"{target_letter}1:{target_letter}{max(pct_rows) + 2}"
    target_vals = ws.get(target_range, value_render_option='FORMULA')
    flat_target = [(r[0] if r else '') for r in target_vals]
    while len(flat_target) <= max(pct_rows):
        flat_target.append('')

    formula_updates = []
    weeks = sorted({w for (w, _) in rowmap.keys()})
    for pct_row in pct_rows:
        existing = flat_target[pct_row - 1]
        if existing not in ('', None):
            continue
        candidate_week = None
        for wk in weeks:
            lr = rowmap.get((wk, later_day))
            er = rowmap.get((wk, earlier_day))
            if lr is None or er is None:
                continue
            if lr < pct_row and (candidate_week is None
                                 or lr > rowmap[(candidate_week, later_day)]):
                candidate_week = wk
        if candidate_week is None:
            continue
        er = rowmap[(candidate_week, earlier_day)]
        lr = rowmap[(candidate_week, later_day)]
        # Skip if the closest day-pair is too far above this % row (>3 rows) —
        # means this % row's corresponding week doesn't actually have day rows yet.
        # (Fri-Sat tab has a stray WK 8 % row at row 33 with no WK 8 Fri/Sat above.)
        if pct_row - lr > 3:
            print(f"    skip {target_letter}{pct_row}: nearest pair (WK {candidate_week}) "
                  f"is row {lr} (gap {pct_row - lr})")
            continue
        formula = f"=-1+({target_letter}{lr}/{target_letter}{er})"
        addr = f"{target_letter}{pct_row}"
        formula_updates.append({'range': addr, 'values': [[formula]]})
        print(f"    formula {addr} <- {formula}  (WK {candidate_week})")

    if formula_updates:
        ws.batch_update(formula_updates, value_input_option='USER_ENTERED')
        print(f"  wrote {len(formula_updates)} % formulas")
    else:
        print("  no missing % formulas")

    # ── Step 2: apply currency format to Sat/Sun (or Fri/Sat) data cells ──
    day_cells = []
    for (wk, day), row in rowmap.items():
        if day in (earlier_day, later_day):
            day_cells.append(f"{target_letter}{row}")
    if day_cells:
        day_cells_sorted = sorted(set(day_cells), key=lambda c: int(c[len(target_letter):]))
        formats = [{"range": cell, "format": CURRENCY_FORMAT} for cell in day_cells_sorted]
        ws.batch_format(formats)
        print(f"  applied CURRENCY format to {len(day_cells_sorted)} data cells")

    # ── Step 3: apply PERCENT + green background to all % rows in target col ──
    pct_cells = [f"{target_letter}{r}" for r in pct_rows]
    if pct_cells:
        pct_formats = [{"range": cell, "format": PCT_FORMAT} for cell in pct_cells]
        ws.batch_format(pct_formats)
        print(f"  applied PERCENT + green format to {len(pct_cells)} % cells: {pct_cells}")


def main():
    if not CREDS_PATH.exists():
        sys.exit(f"ERROR: SA creds not found at {CREDS_PATH}")
    gc = gspread.service_account(filename=str(CREDS_PATH))
    sh = gc.open_by_key(SHEET_ID)
    for tab in TAB_DAYS:
        print(f"\n[{tab}]")
        fix_tab(sh.worksheet(tab), tab)
    print("\nDone")


if __name__ == '__main__':
    main()
