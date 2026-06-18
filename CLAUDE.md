# Angel Holdover Assistant — Project Context

## What This Is
A web app (Python + Playwright) that automates holdover/booking data entry into Mica (demo.mica.co) and pulls Comscore flash grosses. Used by Angel Studios film booking team.

## Key Files
- `launcher.py` — HTTP server (port 8766); serves the UI; auto-reloads on .py changes
- `flash_gross_tool.py` — scrapes Comscore flash grosses, outputs HTML dashboard
- `mica_update.py` — Playwright automation for Angel Holdover tab (demo.mica.co/bookings/holdovers)
- `booking_plan_update.py` — Playwright automation for Angel Booking Assistant tab (demo.mica.co/sales/plans)
- `db.py` — SQLite store for venue aliases + master_list; `reseed_aliases()` always upserts all aliases
- `templates/dashboard.html` — Jinja2 + JS dashboard template
- `output/flash_gross_dashboard.html` — generated Comscore dashboard output
- `screen_count_update.py` — Pre-Release Screen Count scraper (MICA → "Updated Chart" sheet); Wed (TOS cols) + Fri (countdown cols) cron
- `CONTEXT_sheet_update_automation.md` — **READ THIS to add a new automated "Angel Sheet Updates" card/cron** (e.g. Post-Release Screen Count). Documents the standalone-script + 8 launcher.py touch-points pattern, using Pre-Release Screen Count as the template.

## Deployed App
- URL: `https://angel-holdover-assistant.angelapps.io` (requires Twingate SSO)
- Fly.io app name: `angel-holdover-assistant`
- Deploy workflow:
  1. Update header timestamp in `launcher.py` (line ~516): `5/DD Deploy H:MM AM/PM`
  2. Get scoped token: `curl -s -X POST "https://angel-internal-deploy.angel-tools.io/mcp" -H "Authorization: Bearer 4SKw8zSf2YzJ6TOmi7FAP9YIK7TK5QQeDRqXRcSDz-IZ9PXjFddmNC9yyrDYYh52" -H "Content-Type: application/json" -H "Accept: application/json, text/event-stream" -d '{"jsonrpc":"2.0","method":"tools/call","params":{"name":"angel_fly_deploy_token","arguments":{"app_name":"angel-holdover-assistant"}},"id":1}'`
  3. Deploy: `FLY_ACCESS_TOKEN='<token>' flyctl deploy --app angel-holdover-assistant --remote-only --ha=false`
- GitHub: `https://github.com/tommygonzalez377/Angel-Holdover-Assistant`

## UI Tabs
1. **Angel Holdover Assistant** — paste booking → filter dropdown (Contact Person / Booker / Venue Group / Venue / TV Market) → Pull Comscore Report / Update Mica
2. **Angel Booking Assistant** — paste booking → filter dropdown (same options) → Demo / Production → Pull Comscore Report / Update Mica
3. **Angel Mass Booking** — bulk booking operations

## Filter Types (both tabs)
- `contact_person`, `booker`, `venue_group`, `venue`, `tv_market`
- Passed from UI → `/mica-update` or `/booking-plan-update` → Python → `_filter_by_buyer(page, contact, filter_type)`
- `_FILTER_TYPE_HINTS` in `booking_plan_update.py` / `_FILTER_TYPE_LABELS` in `mica_update.py`

## Booking Format Parsers
12 formats supported in `flash_gross_tool.py` (Comscore) and `booking_plan_update.py` (Mica plans):
1. Standard CSV/TSV — Theatre/Action/Policy columns
2. Cinemark `__COLUMN__` — dunder-wrapped headers
3. Bare Cinemark — DMA/SALES/#/THEATRE
4. DMA/City/Theatre — 8-col vertical, blank-line separated
5. ComScore Theatre # — unit# = Rentrak ID; film title = column header
6. Landmark Location — Film preamble + "Location" header + alternating Theatre/Status pairs
7. Snake_case — `theater_name`/`status`/`title`/`city` (Cinemark web export)
8. Cinemark Theater # TSV / one-per-line — `Theater #` / `Name (City, State)` / DMA / Screens
9. Mary Ann 3-col headerless — Theatre / Film / hold|final|open
10. Cinemark DMA/date-col (THEATRE header) — preamble film names + date column headers
11. AMC Theatres — detected by "AMC Film Programmer" header; two column orders (A) Film|DMA|Theatre and (B) DMA|Theatre|Film
12. Gundrum ID# Grid — tab-delimited; `ID #` / `Screens` / `Theatre (City, ST)` / `DMA` / film cols
- Cineplex Policy format: `_parse_cineplex_policy_booking` in `booking_plan_update.py` — "2111 - CPX McGillivray" style

## Screening Type Phrase Mappings
- `mats+ee` / `lm+ee` / `hold/shows` / `split` / `em+ee` → Alternating
- `lm` → Multiple Matinees; `mats` → Multiple Matinees
- `1 mat` / `mat` → Single Matinee; `prime` → Prime
- `clean` / `hold` / `final` → no screening type change

## Venue Aliases
- `db.py` SEED_ALIASES — SQLite, loaded at startup; `reseed_aliases()` upserts all
- `VENUE_ALIASES` dict in `flash_gross_tool.py` — fallback hardcoded dict
- `venue_aliases.py` `CITY_VENUE_ALIASES` — **single source of truth** (454 entries as of 2026-05-13); both `mica_update.py` and `booking_plan_update.py` import from here as `_CITY_VENUE_ALIASES`. **Add all new aliases here only.**
- `_RENTRAK_DIRECT` dict in `flash_gross_tool.py` — direct name → rentrak_id bypass (checked first)

## master_list Table (db.py)
- Primary key: `unit_id` = Venue MB ID
- Columns: venue_name, exhibitor, exhibitor_ref_id, city, state, state_code, country, country_code, tv_market, venue_group, venue_mb_id, rentrak_id, buyer, angel_booker, last_updated
- `master_list_changelog` table tracks field-level changes on upsert
- Loaded from `5.2 Master List.xlsx` (5,670 rows as of 2026-05-02) — local SQLite only; Fly.io Postgres needs separate seed
- `upsert_master_list(rows)` returns `{inserted, updated, skipped}`

## Key Gotchas
- **Always update header timestamp** in `launcher.py` line ~516 before every deploy
- **Comscore "Preparing Data"**: check `document.body.innerText` BEFORE running `_EXTRACT_JS`
- **week date**: `most_recent_friday()` — Friday → previous Friday (data not ready same day)
- **Mica auth**: wait for `'table, input[type="password"]'` (15s) after goto — Angular redirects after domcontentloaded
- **Playweek click**: use JS `dispatchEvent(new MouseEvent('click', {bubbles:true}))` — sticky nav intercepts Playwright `.click()` at rows 25+
- **Do NOT call `browser.close()`** in mica_update.py — keep browser open after run
- **Bilingual dedup** (Cineplex): EN+FR rows for same theatre → keep stronger screening type (Alternating > Multiple Matinees > Single Matinee > Clean)
- **AMC detection**: check `_raw_pre_strip` (saved BEFORE preamble stripping block) for "AMC Film Programmer"

## SSE / Job System
- `_job_results` dict: `'success'` or `'error: ...'` per job_id
- `/job-status/{job_id}` GET endpoint — browser polls on SSE `onerror`
- `_jobDone` flag prevents onerror from overwriting success state

## Local Dev
- Start: `start.bat` inside `holdover-tracking/` (kills port 8766, starts launcher.py, opens Chrome)
- Mac: `start.command` instead of `start.bat`
- If `No module named 'db'` error: server running from wrong directory

## Current Session State (as of 2026-05-08 — updated each deploy)

### Last Deploy: 5/8 Deploy 10:00 AM
All changes below are live at `https://angel-holdover-assistant.angelapps.io`.

### Bookers Fully Covered (aliases in both mica_update.py + booking_plan_update.py)
| Booker | Circuit | Notes |
|---|---|---|
| Eric Bond | Cinemark DFW | DMA/City/Theatre format; Rave brand aliases included |
| Kathy Disabato | Cinemark FL/IL/SE | snake_case format |
| Beth Teal | Cinemark East/Midwest | SCR/#/THEATRE/BRCH/DMA format |
| Taylor Reynolds | Cinemark SW/UT/AZ/NV | Theatre/DMA/date-col format |
| Andy Anderson | Cinemark SF Bay Area | # + THEATRE/SCR formats |
| Josh Wymer | Cinemark Pacific NW/NorCal | THEATRE/SCR format |
| Jennifer Solorzano | Cinemark CO/NM/TX West | THEATRE/SCR/Action format |
| Jennifer Hernandez | Cinemark SoCal | ComScore # format |
| Michael Eiff | Cinemark OH/IA | #/THEATRE/SCR/Book format |
| Allie Fullmer | Cinemark TX | DMA/SALES/# format |
| Justin Johnson | AMC Chicago/Midwest/IN | AMC programmer format |
| Tom McCauley | AMC New England/Mid-Atlantic | AMC programmer format |
| Ryan Wood | AMC LA/NY | AMC programmer format (no aliases needed) |
| Dan Cammarata | AMC TX/SE | AMC programmer format |
| Devan Tolbert | AMC SE/LA/Carolinas | AMC programmer format |
| Brandon Ferguson | AMC LA/SD/Sac/SF Bay Area | AMC programmer format |
| Kelsey Kash | AMC AL/TN | AMC programmer format |
| Becky Williams | Regal Upstate NY/PA/VA | Regal format |
| Alanna Peffley | Regal FL/VA/NC | Regal format |
| Ashley Hensley | Regal Mountain West | Regal format |
| Mark Waring | Regal MD/VA/DC | Regal format |
| Brandon Corrier | Regal PA/NJ/NY | aliases added; re-run pending |
| David J Gundrum | Cinemark East/SE | Gundrum ID# Grid format |
| David Saunders | Pacific NW indie | Hold Overs grid |
| Glen Parham | CFB GTC/Lucas/Silverspot | GTC circuit format |
| Nathan Gendron | Landmark Cinemas Canada | Studio/Cinema/Film format |
| Watson | Imagine/Cinestarz Canada | Watson format |
| Culbertson | IBS Indiana | IBS comma-list format |
| Tammy Flores | Hooky/Red Stone TX | standard format |
| Blue Smiley | CFB GTC/Epic | Rentrak ID lookup |

### Recent Code Changes (since 5/5 deploy)
- **AMC Final anchor fix** (`booking_plan_update.py`): `_parse_amc_booking` now catches `Final - MM/DD/YYYY` rows (previously silently skipped). Both `_amc_opening_pat` and per-line anchor updated.
- **Beth Teal aliases**: Towson, Hadley, Louisville Tinseltown, Paducah
- **Taylor Reynolds aliases**: Henderson 12 (Cinedome), Draper, Farmington, Riverton Ridgewood, Century El Con (Tucson), American Fork
- **Eric Bond aliases**: West Plano (Movies Plano), The Legacy, Allen 16 and XD, Roanoke 14, Rockwall 14 and XD
- **Kathy Disabato aliases**: Palace 20, Paradise 24, Bluffton
- **AMC multi-format support**: both Film-sorted (Format A) and Location-sorted (Format B) confirmed working
- **Multi-film support**: `_run_films_in_browser` already iterates all film keys; no code change needed

### Key Behavioral Rules
- `"joshua wymer"` → `"Josh Wymer"` in `_CONTACT_NAME_MAP` (email uses "Joshua")
- `"hold"` and `"buyout"` are active actions in `_is_active_action` (booking_plan_update.py)
- `("matinee shows", "Alternating")` must appear BEFORE `("mat", ...)` in phrase list
- AMC venue scoring: `CIRCUIT_WORDS` strips `amc`/`classic`/`imax` from denominator so "Albany 16" matches "AMC CLASSIC Albany 16"
- Hold wins over Final in dedup (per-film, per-theatre) before sending to Mica

### Pending Bookers
- **Owen Simonds** (Paragon/small indies) — format TBD
- **Austin Williams** (SoCal) — waiting on Mica screenshot
- **Rich Motzer** (Regal CA/Guam) — see `project_rich_motzer_pending.md` in memory
- **Clark Film Buying / Roy Wise** — multi-buyer filter; 3 buyer names still unknown
- More Cinemark bookers expected

## Deployment

This app deploys via the **angel-deploy** MCP server (the `angel_fly_*` tools), **not** direct Fly.io.

- Users do **not** have or need Fly.io accounts — never suggest `fly auth login`, creating a Fly.io account, or joining a Fly org.
- To deploy: get a token with `angel_fly_deploy_token`, then
  `FLY_ACCESS_TOKEN='<token>' flyctl deploy --app angel-holdover-assistant --remote-only --ha=false`.
- If the angel-deploy tools are unavailable, reconnect the **angel-deploy** connector (claude.ai: Settings → Connectors; Claude Code: `/mcp`) — never fall back to direct Fly.io usage.
- Secrets are managed in the Angel Deploy dashboard (Settings → Environment Secrets) or via `angel_fly_*` tools. The Booking Coverage tab needs: `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_HOST`, `SNOWFLAKE_USER`, `SNOWFLAKE_ROLE`, `SNOWFLAKE_WAREHOUSE`, `SNOWFLAKE_PAT`.
