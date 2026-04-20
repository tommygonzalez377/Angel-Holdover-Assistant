# Angel Holdover Assistant — Team Setup Guide

A tool for the theatrical booking team. Once set up, you launch it by double-clicking an icon — no terminal needed for daily use.

---

## Requirements

- **macOS** (Monterey 12+ recommended) or **Windows 10/11**
- **Python 3.11 or newer** — download free from [python.org/downloads](https://www.python.org/downloads/)
  - **Windows**: During install, check ✅ **"Add Python to PATH"**
- A stable internet connection for the one-time setup (~120 MB download)

---

## One-Time Setup

### Mac

1. **Get the `holdover-tracking` folder** from Tommy or clone the GitHub repo. Save it somewhere convenient (Desktop or Documents).

2. **Open Terminal** — press `⌘ Space`, type `Terminal`, press Enter

3. **Run this once** to allow the setup scripts to execute (paste into Terminal and press Enter):
   ```
   chmod +x ~/Desktop/holdover-tracking/setup.command ~/Desktop/holdover-tracking/start.command
   ```
   *(Adjust the path if you saved it somewhere other than Desktop)*

4. **Double-click `setup.command`** — a Terminal window opens and installs everything:
   - Python packages
   - Playwright Chromium browser (~120 MB, one-time download)

5. Setup complete! Close the Terminal window when it says "Setup complete!"

> **Gatekeeper prompt?** If macOS says "setup.command cannot be opened because it is from an unidentified developer", right-click the file and choose **Open**, then click **Open** in the dialog.

---

### Windows

1. **Get the `holdover-tracking` folder** from Tommy or clone the GitHub repo. Save it on your Desktop.

2. **Double-click `setup.bat`** — a command window opens and installs everything

3. Setup complete! The window will say "Setup complete!" when finished

> **Windows Defender prompt?** If Windows says "Windows protected your PC", click **More info** → **Run anyway**.

---

## Daily Use

### Mac — Option A: Double-click `start.command`
A Terminal window opens, the server starts, and your browser opens automatically at `http://localhost:8766`. Keep the Terminal window open while you use the app. Close it to stop the server.

### Mac — Option B: App icon (for your Dock/Applications)
Move **`Angel Holdover Assistant.app`** to your Applications folder. Double-click it like any other app — browser opens automatically, server runs silently in the background.

> First time: right-click the .app → **Open** → **Open** (bypasses Gatekeeper)

### Windows — Double-click `start.bat`
Opens the server in a minimized window and opens your browser. Close the minimized server window to stop the app.

---

## First Run: Save Your Credentials

The app opens directly — no login required. On first launch, save your credentials so the app can access Comscore and Mica on your behalf:

1. Click **"My Profile"** in the top-right corner of the app
2. Enter your **Comscore** (Box Office Essentials) username & password
3. Enter your **Mica** (demo.mica.co) email & password
4. Click **Save Credentials**

That's it. Your credentials are stored encrypted on your computer only and remembered every time you launch the app. If you see an orange banner at the top, it means credentials haven't been saved yet — click the link in the banner to go straight to the profile page.

---

## Troubleshooting

| Problem | Fix |
|---|---|
| "Python not found" during setup | Install Python 3.11+ from [python.org](https://www.python.org/downloads/). Windows: check "Add to PATH". |
| App won't open on Mac (Gatekeeper) | Right-click the file → Open → Open |
| `setup.bat` blocked by Windows Defender | Click "More info" → "Run anyway" |
| Browser shows "This site can't be reached" | Wait 5–10 more seconds; the server may still be starting. Refresh the page. |
| Orange "credentials not saved" banner | Click the link in the banner and save your Comscore & Mica passwords |
| Something else broken | Ask Tommy or check the terminal window for error messages |

---

## What's Installed (and Where)

Everything stays inside the `holdover-tracking` folder:

```
holdover-tracking/
├── venv/                  ← Python packages (created by setup)
├── .env                   ← Your local config (created by setup, edit with any text editor)
├── holdover.db            ← Local database (venue aliases, your credentials)
└── output/                ← Generated dashboards saved here
```

Nothing is installed system-wide. To uninstall completely, just delete the `holdover-tracking` folder.
