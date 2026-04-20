# Angel Holdover Assistant

A web-based tool for Angel Studios theatrical booking teams. Automates three repetitive weekly workflows:

1. **Flash Gross Report** — pulls Comscore box office flash grosses for all booked locations and generates an HTML dashboard
2. **Mica Holdover Update** — marks holds and finals in [demo.mica.co](https://demo.mica.co) based on a booking sheet
3. **Mica Sales Plan Update** — updates title entries in the Mica sales plan for a given contact

---

## How It Works

### Holdover Tab
1. Drop or paste a booking sheet (CSV, Excel, PDF, or screenshot)
2. Optionally enter a Contact/Booker to auto-update Mica after
3. Click **Pull Comscore Report** — the tool logs into Comscore, scrapes flash grosses for every FINAL/HOLD location, and generates a dashboard
4. Click **Update Mica** to mark holds and finals in Mica automatically

### Booking Tab
1. Paste a booking sheet and fill in Contact and Title
2. Click **Update Sales Plan** — updates the title entry in the Mica sales plan for that contact

### Mass Booking Tab
Processes every buyer/contact found in a booking sheet in one pass. Supports title and circuit filters, plus a dry-run mode to preview matches before making changes.

---

## Local Development

### Prerequisites
- Python 3.11+
- [Playwright](https://playwright.dev/python/) browsers installed

### Setup

```bash
# 1. Clone the repo
git clone <repo-url>
cd holdover-tracking

# 2. Install Python dependencies
pip install -r requirements.txt

# 3. Install Playwright browsers
playwright install chromium

# 4. Copy and fill in environment variables
cp .env.example .env
# Edit .env with your Comscore, Mica, and API credentials

# 5. Start the server
python launcher.py
```

Then open **http://localhost:8080** in your browser.

The server auto-reloads when any `.py` file changes.

### Environment Variables

| Variable | Description |
|---|---|
| `COMSCORE_USERNAME` | Box Office Essentials login |
| `COMSCORE_PASSWORD` | Box Office Essentials password |
| `MICA_USERNAME` | demo.mica.co email |
| `MICA_PASSWORD` | demo.mica.co password |
| `AI_LABS_API_KEY` | Angel AI Labs key (screenshot parsing via Claude vision) |
| `DATABASE_URL` | PostgreSQL URL (leave blank to use local SQLite) |
| `GOOGLE_CLIENT_ID` | Google OAuth client ID (for deployed app login) |
| `GOOGLE_CLIENT_SECRET` | Google OAuth client secret |
| `GOOGLE_ALLOWED_DOMAIN` | Restrict logins to this domain (default: `angel.com`) |
| `SECRET_KEY` | Random secret for session encryption |
| `SERVER_MODE` | Set to `1` when deployed, leave blank locally |

---

## Project Structure

```
holdover-tracking/
├── launcher.py              # Web server + UI (auto-reloads on .py changes)
├── flash_gross_tool.py      # Comscore scraper — booking parser + flash gross dashboard
├── mica_update.py           # Playwright automation for Mica holdover updates
├── booking_plan_update.py   # Playwright automation for Mica sales plan updates
├── db.py                    # SQLite/PostgreSQL venue alias store
├── auth.py                  # Google OAuth login handler
├── templates/
│   └── dashboard.html       # Jinja2 + JS flash gross dashboard template
├── output/                  # Generated dashboards and debug screenshots
├── master_list_cache.csv    # Cached Comscore/Rentrak theatre master list
├── requirements.txt
└── .env.example
```

---

## Deployment (Fly.io)

The app is deployed on Fly.io as an internal tool, accessible only via Twingate SSO.

```bash
# Deploy latest changes
flyctl deploy

# Tail live logs
flyctl logs --app angel-holdover-assistant

# Open the live app
flyctl open --app angel-holdover-assistant
```

Fly.io app name: `angel-holdover-assistant`

---

## Supported Booking Formats

The tool auto-detects the following booking sheet formats on paste or upload:

| Format | Description |
|---|---|
| Standard CSV/TSV | Theatre / Action / Policy columns |
| Cinemark `__COLUMN__` | Dunder-wrapped headers |
| Bare Cinemark | DMA / SALES / # / THEATRE columns |
| DMA/City/Theatre | 8-column vertical with blank-line sections |
| ComScore Theatre # | Unit # = Rentrak ID; film title as column header |
| Landmark Location | Film preamble + Location header |
| Snake_case | `theater_name` / `status` / `title` / `city` (Cinemark web export) |
| Cinemark Theater # TSV | Theater # / Name (City, State) / DMA / Screens / Film columns |
| Mary Ann 3-col | Headerless Theatre / Film / hold\|final\|open |
| Cinemark DMA/date-col | Preamble films + Theatre/DMA/date column headers |
