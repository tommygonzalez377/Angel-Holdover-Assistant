"""Diagnostic: dump the formulas/values in % rows of both tabs so we can see
what's actually there in the Animal Farm column vs neighbors."""
from pathlib import Path
import gspread

SHEET_ID = "1yTnuoNh923ibhQNA1tTrwfltF339oXXvPMDe3CHE1vE"
CREDS = Path(__file__).parent / "creds" / "sheets-service-account.json"

gc = gspread.service_account(filename=str(CREDS))
sh = gc.open_by_key(SHEET_ID)


def col_letter(idx0):
    s = ''
    n = idx0 + 1
    while n:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s


for tab in ("Fri - Sat", "NON SOF"):
    print(f"\n========== {tab} ==========")
    ws = sh.worksheet(tab)
    row1 = ws.row_values(1)
    af_idx = next((i for i, h in enumerate(row1)
                   if (h or '').strip().lower() == 'animal farm'), None)
    print(f"Animal Farm at col idx {af_idx} ({col_letter(af_idx) if af_idx is not None else '?'})")
    last_col = col_letter(len(row1) - 1)
    grid = ws.get(f'A1:{last_col}40', value_render_option='FORMULA')
    # Look for rows where any col has a value containing '%' or a formula starting with =
    for ri, row in enumerate(grid):
        has_pct = any(
            isinstance(c, str) and (c.startswith('=') or '%' in c)
            for c in row
        )
        if not has_pct:
            continue
        af_cell = row[af_idx] if af_idx is not None and af_idx < len(row) else '<out-of-range>'
        # Show a sample formula from any other col
        sample = next((c for c in row
                       if isinstance(c, str) and c.startswith('=-1+')), None)
        sample_avg = next((c for c in row
                           if isinstance(c, str) and c.startswith('=AVERAGE')), None)
        print(f"  row {ri+1}: af_cell={af_cell!r}, "
              f"sample_neg1={sample!r}, sample_avg={sample_avg!r}, "
              f"row_len={len(row)}")
