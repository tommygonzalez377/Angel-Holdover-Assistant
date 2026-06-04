# -*- coding: utf-8 -*-
"""
Golden-snapshot harness for the booking-text parsers.

Runs every fixture in tests/fixtures/ through all THREE current parser entry points:
  - Holdover : mica_update.parse_booking_csv(path)            -> list[dict]
  - Booking  : booking_plan_update.parse_open_bookings(text)  -> dict[film, list[dict]]
  - Flash    : flash_gross_tool.load_final_locations(path)     -> list[dict]

Serializes a stable JSON per fixture to tests/snapshots/<name>.json.

Usage:
  python tests/snapshot.py            # (re)generate snapshots from current code
  python tests/snapshot.py --check    # compare current output to saved snapshots; exit 1 on diff

The point: capture today's behavior as a baseline, then after the parser
consolidation refactor, `--check` must show ZERO diff (except explicitly-approved
reconciliations, after which you regenerate the baseline).
"""
import io, os, sys, json, glob, traceback

# Run from the project root regardless of where invoked
_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.dirname(_HERE)
sys.path.insert(0, _ROOT)
os.chdir(_ROOT)

FIX_DIR  = os.path.join(_HERE, "fixtures")
SNAP_DIR = os.path.join(_HERE, "snapshots")


def _safe(fn):
    """Call fn(), capturing any Exception OR SystemExit (flash holdover-grid calls sys.exit)."""
    try:
        return {"ok": fn()}
    except SystemExit as e:
        return {"system_exit": str(e)}
    except Exception as e:
        return {"error": f"{type(e).__name__}: {e}"}


def _norm_rows(rows):
    """Normalize a list[dict] to sorted, stable form."""
    out = []
    for r in rows:
        if not isinstance(r, dict):
            out.append(repr(r)); continue
        out.append({str(k): ("" if v is None else str(v)) for k, v in sorted(r.items())})
    out.sort(key=lambda d: json.dumps(d, sort_keys=True))
    return out


def _run_holdover(path, text):
    import mica_update as M
    res = _safe(lambda: M.parse_booking_csv(__import__("pathlib").Path(path)))
    if "ok" in res:
        res["ok"] = _norm_rows(res["ok"])
    return res


def _run_booking(path, text):
    import booking_plan_update as B
    def go():
        d = B.parse_open_bookings(text)
        return {str(film): _norm_rows(rows) for film, rows in sorted(d.items())}
    return _safe(go)


def _run_flash(path, text):
    import flash_gross_tool as F
    res = _safe(lambda: F.load_final_locations(path))
    if "ok" in res:
        res["ok"] = _norm_rows(res["ok"])
    return res


def snapshot_one(path):
    text = io.open(path, encoding="utf-8").read()
    return {
        "holdover": _run_holdover(path, text),
        "booking":  _run_booking(path, text),
        "flash":    _run_flash(path, text),
    }


def main():
    check = "--check" in sys.argv
    fixtures = sorted(glob.glob(os.path.join(FIX_DIR, "*")))
    if not fixtures:
        print("No fixtures found in tests/fixtures/"); return 0
    os.makedirs(SNAP_DIR, exist_ok=True)
    diffs = 0
    for fx in fixtures:
        name = os.path.basename(fx)
        snap = snapshot_one(fx)
        snap_json = json.dumps(snap, indent=2, ensure_ascii=False, sort_keys=True)
        snap_path = os.path.join(SNAP_DIR, name + ".json")
        if check:
            if not os.path.exists(snap_path):
                print(f"  NEW (no baseline): {name}"); diffs += 1; continue
            old = io.open(snap_path, encoding="utf-8").read()
            if old.strip() != snap_json.strip():
                print(f"  DIFF: {name}"); diffs += 1
            else:
                print(f"  ok:   {name}")
        else:
            io.open(snap_path, "w", encoding="utf-8", newline="\n").write(snap_json + "\n")
            print(f"  wrote {name}.json")
    if check:
        print(f"\n{'FAIL' if diffs else 'PASS'}: {diffs} fixture(s) differ from baseline")
        return 1 if diffs else 0
    print(f"\nWrote {len(fixtures)} snapshots.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
