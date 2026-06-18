# Adding a New Automated "Angel Sheet Updates" Task

**Read this before adding another automated card** (e.g. *Post-Release Screen Count*) to the
**Angel Sheet Updates** tab. It documents the exact pattern, using the existing
**Pre-Release Screen Count** feature as the worked example to copy. There are already
three live cards built on this pattern: **O Canada**, **Daily Grosses**, **Pre-Release Screen Count**.

> **Golden rule:** don't invent a new architecture. Copy an existing feature end-to-end,
> rename the prefix, and change only the data logic. The Pre-Release Screen Count
> (`screen_count_update.py` + the `screencount`/`sc_` wiring in `launcher.py`) is the
> closest template for anything MICA-based.

---

## The shape of every feature (two pieces)

1. **A standalone scraper script** `holdover-tracking/<feature>_update.py`
   - Runnable as a subprocess from `launcher.py`.
   - CLI flags: `--dry-run`, `--force`, `--only`, `--today YYYY-MM-DD`, `--mode demo|prod`.
   - Reads its source (MICA via `booking_plan_update as bp`, or Comscore via `flash_gross_tool`),
     computes a value, writes it to a Google Sheet cell with `gspread`.
   - Prints progress to stdout (the launcher streams it to the browser via SSE).

2. **Eight touch-points in `launcher.py`** that give it a card + button + live log + cron.
   Every touch-point already exists for `screencount` — search the file for that string
   and mirror it with a new prefix.

---

## Part A — the standalone scraper (`screen_count_update.py` as template)

Copy `screen_count_update.py` to `<feature>_update.py` and adjust:

- **Constants** at top: `SHEET_ID`, `SHEET_TAB`, the 0-based column-index constants, and any
  date/column-mapping dicts.
- **`open_worksheet()`** — gspread auth. Uses `GSHEETS_SERVICE_ACCOUNT_JSON` env var (a temp
  file) in production, else local `creds/sheets-service-account.json`. **Catches 403 /
  `PermissionError`** and prints a one-line "share the sheet with `<svc email>`" message
  instead of a stack trace. Keep this.
- **`read_film_rows(ws)`** — pulls the data rows from the sheet (title + release date + row #).
- **The date→column logic** — e.g. `weeks_before_release()`, `tos_from_release()`,
  `tos_target_column()`. This is the part you actually change per feature.
- **`pull_film_count(page, ctx, ...)`** — the MICA scrape. Reuses `bp._navigate_to_plans`,
  `bp._search_plans_for_title`, `bp._find_and_click_plan(plan_desc="US, CA, PR")`,
  `bp._do_login`, `bp._HEADLESS`, `bp.AUTH_FILE`, etc. (single source of truth for MICA auth).
- **`main()`** — parse flags, read sheet, compute a target column+cell per film, launch
  Playwright once, loop films, write.

### Behavioral guarantees baked into `main()` — PRESERVE THESE

These were explicitly requested by Tommy. Do not regress them:

1. **Overwrite the current cell, never a past one.** The scheduled run **overwrites** its
   target cell with the latest live number (e.g. a Monday preview gets refreshed by the
   Friday snapshot). This is safe because the targeting logic only ever returns the
   *current* period's column — so finalized past weeks are never touched. "Don't change the
   numbers" means *don't change past weeks*, NOT "write once." (Pre-Release Screen Count
   learned this the hard way: an early Monday value blocked the Friday refresh under the old
   skip-if-filled rule. Fixed 2026-06-05.)
2. **No back-fill.** The script only ever targets the *current* due cell — it never goes
   back to fill a past/missed column. (This is also what protects past cells from #1.)
3. **No wasted pulls.** Compute the full `targets` list from the sheet + dates **before**
   launching the browser. If `targets` is empty, log `"Nothing to do today."` and return
   **without** logging into MICA.
4. **Dry-run honesty.** In `--dry-run`, still do the live read and report what it *would*
   do — including "would SKIP — cell already has X" when the cell is filled.

---

## Part B — the eight `launcher.py` touch-points

Pick a unique prefix. Pre-Release Screen Count uses **`screencount`** (HTML/JS ids, function
names), **`sc_`** (job-id prefix), and **`/screen-count-*`** (URL paths). For a new feature,
search-and-mirror each of these. Concrete anchors (line numbers drift — search the strings):

| # | What | Where (search for) | Notes |
|---|------|--------------------|-------|
| 1 | **Banner timestamp** | `Deploy` span near line ~517 | Update to the real current clock time on every deploy. |
| 2 | **HTML card** | `Pre-Release Screen Count — # of Runs` (~837) | A `<div>` card inside `#tab-sheets`: title + `…-next-run` label, a `…-dry-run` checkbox, a `…-run-btn`, a `…-reset-btn`, and a `…-progress` log div below it. Copy the whole card block. |
| 3 | **JS functions** | `refreshScreenCountNextRun` (~2066) | Four fns: `refresh…NextRun()`, `…AppendLine()`, `run…Update()` (POST → EventSource → `__SUCCESS__`/`__ERROR__` + `/job-status` polling fallback), `reset…UI()`. Plus two trailing lines that call `refresh…NextRun()` and `setInterval(…, 5*60*1000)`. |
| 4 | **GET: SSE stream** | `/screen-count-stream/` (~2606) | `self._sse_stream(job_id)`. |
| 5 | **GET: next-run** | `/screen-count-next-run` (~2610) | Returns `json.dumps(_<feature>_next_run_info())`. |
| 6 | **POST: trigger** | `/screen-count-update` (~2989) | Reject if `_<feature>_is_running()` (409). Build `user_creds` from `_db.get_credentials`. `job_id = '<pfx>_' + str(int(time.time()*1000))`; `_job_queues[job_id] = queue.Queue()`; spawn `threading.Thread(target=_run_<feature>, args=(job_id, dry_run, user_creds))`. |
| 7 | **Runner + lock** | `_run_screen_count` (~3565 area) | `_<feature>_running_lock`, `_<feature>_running_flag`, `_<feature>_is_running()`, and `_run_<feature>(job_id, dry_run, user_creds)` which `subprocess.Popen`s the script with `env=_build_env(user_creds)`, streams stdout to the queue, emits `__SUCCESS__`/`__ERROR__`, clears the lock in `finally`. |
| 8 | **Scheduler + startup thread** | `_screen_count_scheduler` / `screencount_cron` (~4460) | See Part C. Register the thread next to the other `*_cron` threads in the startup block. |

**`_build_env(user_creds)`** already injects `COMSCORE_USERNAME/PASSWORD` **and**
`MICA_USERNAME/PASSWORD` — no change needed for a MICA or Comscore feature.

---

## Part C — the cron scheduler

Config constants (mirror `_SCREENCOUNT_CRON_*`):

```python
_<FEATURE>_CRON_USER_EMAIL = os.getenv('<FEATURE>_CRON_USER_EMAIL', 'tommy.gonzalez@angel.com')
_<FEATURE>_CRON_TZ         = zoneinfo.ZoneInfo('America/Denver')   # 9 AM local year-round, auto-DST
_<FEATURE>_CRON_TZ_LABEL   = 'MT'
_<FEATURE>_CRON_WEEKDAYS   = (2, 4)   # tuple! Wed=2, Fri=4. Use a 1-tuple for a single day.
_<FEATURE>_CRON_HOUR       = 9
_<FEATURE>_CRON_MINUTE     = 0
```

- **Timezone is always `America/Denver`** (NOT `America/Phoenix` — that drifts to 10 AM during DST).
- **Multi-day cron:** `_screen_count_next_fire()` loops over `_..._WEEKDAYS`, builds a candidate
  per weekday, and returns `min(candidates)`. (O Canada / Daily Grosses fire one day, so they
  use a single `_..._WEEKDAY` int instead — either form is fine; copy whichever matches.)
- **The scheduler checks the right credential** before firing. Screen Count checks
  `cron_user_creds.get('mica_user')`; Comscore features check `comscore_user`. **Match this to
  your data source** or the cron silently skips.
- `_<feature>_next_run_info()` formats the next fire for the UI label (`%A, %b %-d at %-I:%M %p`
  with the `%#` Windows variant) + `' {TZ_LABEL}'`.

### Note on the "Next scheduled" label (a known gotcha)

For a multi-day cron, the label shows the **soonest** fire only. If one of the days does nothing
that week (e.g. the Wednesday TOS run when no film is on-sale that week), the label can look
misleading ("Next: Wednesday" when the real write is Friday). That's cosmetic — leave it unless
asked. If asked, change `_<feature>_next_run_info()` to return both days + what each does.

---

## Part D — Google Sheets auth

- **Service account:** `o-canada-write@angel-booking-assistant.iam.gserviceaccount.com`
  (shared by all sheet-writing features). **The target sheet MUST be shared (Editor) with this
  email** or every read/write 403s. This is the #1 setup failure — confirm it first.
- Local dev creds: `holdover-tracking/creds/sheets-service-account.json` (gitignored).
- Production: Fly secret `GSHEETS_SERVICE_ACCOUNT_JSON` (full JSON contents); the script writes
  it to a temp file and passes it to `gspread.service_account(filename=…)`.
- `gspread` + `google-auth` are already in `requirements.txt`.

---

## Part E — Deploy

1. Update the **banner timestamp** in `launcher.py` (~line 517) to the **real current clock time**
   (run `date` first; don't guess).
2. `python -m py_compile launcher.py <feature>_update.py` to catch syntax errors.
3. Get a scoped token via the `angel_fly_deploy_token` MCP tool (call `angel_fly_deploy_check`
   first; ~30-min expiry).
4. `FLY_ACCESS_TOKEN='<token>' flyctl deploy --app angel-holdover-assistant --remote-only --ha=false`
5. Verify via `angel_fly_logs` — look for your `[<feature>-cron] next fire: …` line at startup.
6. Prod is behind Twingate SSO, so curling endpoints returns a 302 to login — that's expected;
   verify from the logs instead.

---

## Appendix — Pre-Release Screen Count specifics (mirror or contrast for Post-Release)

- **Sheet:** `1eQRg2pcpC2B6fXhBWvwB0NCsT_5m4t3qnFQGSxLUvJM`, tab **"Updated Chart"**.
  Columns (1-based): A Title · B Release date · C "1 wk bf TOS" · D "by TOS" ·
  E–K = 7→1 wk before release · L at opening · M # at widest · N screens added Mon · O Notes.
- **Two cadences in one feature:**
  - **Friday** → release-countdown columns (E–L). `weeks_before_release()` snaps to the
    **UPCOMING** Friday (today if Friday), so Monday counts toward the next Friday milestone
    (6/1 with a 7/3 release = "4 weeks out" → column H).
  - **Wednesday** → tickets-on-sale columns (C/D) **only** (no countdown fallback). Tickets go
    on sale **8 weeks before release, on the Wednesday of that week** (`tos_from_release()`).
    `D` "by TOS" = that on-sale Wednesday; `C` "1 wk bf TOS" = the Wednesday one week before.
- **MICA read:** US/CA/PR plan → Venues table → Status filter = **Agreed + Booked** (apply
  Status before Start Date; reopen the Status dropdown per status) → read **"Filtered: N"**.
  Opening-week window = Monday of the release-week through the following Thursday; subtract any
  Agreed+Booked on out-of-window Start Dates. Filter ids:
  `#datatable-th-filter-planVenueStatusDescription`, `#datatable-th-filter-startPlayDate`.
- **For a Post-Release Screen Count**, the likely deltas are: a different set of columns
  (post-open weeks), a different date→column rule (count *forward* from release instead of
  *toward* it), and possibly a different MICA filter/window — but the same script skeleton,
  the same 8 launcher touch-points (new prefix), and the same no-overwrite/no-backfill/
  no-wasted-pull guarantees. If it writes to the **same sheet**, it can even reuse the same
  service account with no extra sharing.
