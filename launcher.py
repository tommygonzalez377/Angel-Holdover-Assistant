#!/usr/bin/env python3
"""
Flash Gross Launcher
Drag-and-drop web interface for running the flash gross tool.
Run this file, then drop your booking CSV into the browser window.
"""

import base64
import http.server
import json
import os
import queue
import subprocess
import sys
import threading
import time
import urllib.parse
import webbrowser
from pathlib import Path

try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).parent / '.env')
except ImportError:
    pass

AI_LABS_API_KEY = os.getenv('AI_LABS_API_KEY', '')

# ── Database + Auth init ──────────────────────────────────────────────────────
try:
    import db as _db
    import auth as _auth
    _db.init_db()
    _db.seed_aliases_if_empty()
    _AUTH_ENABLED = bool(os.getenv('GOOGLE_CLIENT_ID'))
    # In local mode, ensure a real DB row exists for the single local user
    if not _AUTH_ENABLED:
        _LOCAL_USER = _db.get_or_create_local_user()
        _LOCAL_USER_ID = _LOCAL_USER['id']
    else:
        _LOCAL_USER_ID = 0
except Exception as _e:
    print(f'[launcher] DB/auth init failed: {_e}')
    _db = None
    _auth = None
    _AUTH_ENABLED = False
    _LOCAL_USER_ID = 0

PORT       = int(os.getenv('PORT', '8766'))
BASE_DIR   = Path(__file__).parent
OUTPUT_DIR = BASE_DIR / "output"
OUTPUT_DIR.mkdir(exist_ok=True)

# Asset paths — configurable via .env, fall back to assets/ folder next to launcher
_ASSETS_DIR = BASE_DIR / "assets"
VIDEO_PATH  = Path(os.getenv('VIDEO_PATH',  str(_ASSETS_DIR / 'bg_video.mp4')))
LOGO_PATH   = Path(os.getenv('LOGO_PATH',   str(_ASSETS_DIR / 'logo.eps')))

def _load_logo_b64() -> str:
    """Convert EPS logo → base64 PNG. Tries Ghostscript (alpha), then Pillow."""
    if not LOGO_PATH.exists():
        return ''
    import tempfile, io
    # Ghostscript: pngalpha gives white paths on transparent background
    for gs_cmd in ('gswin64c', 'gswin32c', 'gs'):
        tmp_path = None
        try:
            with tempfile.NamedTemporaryFile(suffix='.png', delete=False) as tmp:
                tmp_path = tmp.name
            r = subprocess.run(
                [gs_cmd, '-dNOPAUSE', '-dBATCH', '-dSAFER',
                 '-sDEVICE=pngalpha', '-r144',
                 f'-sOutputFile={tmp_path}', str(LOGO_PATH)],
                capture_output=True, timeout=15,
            )
            if r.returncode == 0 and Path(tmp_path).stat().st_size > 0:
                data = Path(tmp_path).read_bytes()
                return base64.standard_b64encode(data).decode()
        except (FileNotFoundError, OSError, subprocess.TimeoutExpired):
            pass
        finally:
            if tmp_path:
                try: Path(tmp_path).unlink()
                except: pass
    # Fallback: Pillow
    try:
        from PIL import Image
        img = Image.open(LOGO_PATH)
        img.load(scale=2)
        buf = io.BytesIO()
        img.save(buf, format='PNG')
        return base64.standard_b64encode(buf.getvalue()).decode()
    except Exception as e:
        print(f'  Logo: could not convert EPS — {e}')
        return ''

_LOGO_B64 = _load_logo_b64()
_LOGO_IMG = (f'<img id="header-logo" src="data:image/png;base64,{_LOGO_B64}" alt="Angel">'
             if _LOGO_B64 else '')
_LOGO_FOOTER_IMG = (f'<img id="footer-logo" src="data:image/png;base64,{_LOGO_B64}" alt="Angel">'
                    if _LOGO_B64 else '')

_job_queues:   dict[str, queue.Queue] = {}
_job_results:  dict[str, str]         = {}   # job_id → 'success' | 'error: ...'
_job_unbooked:        dict[str, list] = {}   # job_id → list of {venue, city, state, screens}
_job_already_booked:  dict[str, list] = {}   # job_id → Agreed in Mica but not on booking sheet
_job_missed:          dict[str, list] = {}   # job_id → on sheet but not found in Mica
_comscore_lock = threading.Lock()             # only one Comscore scrape at a time

# ---------------------------------------------------------------------------
# HTML page
# ---------------------------------------------------------------------------

HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Angel Holdover Assistant</title>
<style>
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
         color: white; min-height: 100vh; background: #0a0a1a; }

  /* ── Background video ────────────────────────────────────────────────── */
  #bg-video {
    position: fixed; top: 0; left: 0;
    width: 100%; height: 100%;
    object-fit: cover; z-index: -2;
  }
  body::before {
    content: ''; position: fixed; top: 0; left: 0;
    width: 100%; height: 100%;
    background: rgba(5, 8, 20, 0.62); z-index: -1;
  }

  /* ── Header ──────────────────────────────────────────────────────────── */
  header {
    background: rgba(5, 8, 20, 0.75); backdrop-filter: blur(10px);
    color: white; padding: 16px 32px;
    display: flex; align-items: center; justify-content: center; gap: 14px;
    box-shadow: 0 1px 0 rgba(255,255,255,0.08);
  }
  header h1 { font-size: 1.35rem; font-weight: 600; letter-spacing: 0.01em; }
  #header-logo { height: 42px; width: auto; display: block; }

  /* ── Tab bar ──────────────────────────────────────────────────────────── */
  #tab-bar {
    display: flex; justify-content: center; gap: 6px;
    padding: 12px 24px 0;
    background: rgba(5,8,20,0.6); backdrop-filter: blur(10px);
    border-bottom: 1px solid rgba(255,255,255,0.08);
  }
  .tab-btn {
    padding: 10px 28px; border: none; border-radius: 8px 8px 0 0;
    background: rgba(255,255,255,0.06); color: rgba(255,255,255,0.5);
    font-size: 0.88rem; font-weight: 700; letter-spacing: 0.04em;
    cursor: pointer; transition: background 0.18s, color 0.18s;
    border-bottom: 2px solid transparent; margin-bottom: -1px;
  }
  .tab-btn:hover { background: rgba(255,255,255,0.1); color: rgba(255,255,255,0.8); }
  .tab-btn.active {
    background: rgba(0,201,212,0.12); color: #00c9d4;
    border-bottom: 2px solid #00c9d4;
  }

  /* ── Tab panels ───────────────────────────────────────────────────────── */
  .tab-panel { display: none; }
  .tab-panel.active { display: block; }

  /* ── Layout ──────────────────────────────────────────────────────────── */
  main {
    max-width: 920px; margin: 48px auto 52px; padding: 0 24px;
    display: flex; gap: 32px; align-items: stretch;
  }

  /* ── Booking assistant tab ────────────────────────────────────────────── */
  #booking-main {
    max-width: 1260px; margin: 48px auto 52px; padding: 0 24px;
    display: flex; gap: 32px; align-items: flex-start;
  }
  #booking-steps-panel { width: 168px; flex-shrink: 0; position: relative; }
  #booking-step-line {
    position: absolute; left: 17px; width: 2px;
    background: rgba(255,255,255,0.15); pointer-events: none;
  }
  #booking-main-col { flex: 1; min-width: 0; }

  /* ── Booking Differences panel ───────────────────────────────────────── */
  #diff-panel {
    width: 320px; flex-shrink: 0;
    background: rgba(255,255,255,0.05); border: 1px solid rgba(255,255,255,0.12);
    border-radius: 14px; padding: 18px 16px; display: none; flex-direction: column; gap: 14px;
    align-self: flex-start;
  }
  #diff-panel.visible { display: flex; }
  #diff-panel-title { font-size: 14px; font-weight: 700; color: #fff; letter-spacing: 0.04em; margin-bottom: 2px; }
  .diff-section { display: flex; flex-direction: column; gap: 6px; }
  .diff-section-hdr {
    font-size: 11px; font-weight: 700; letter-spacing: 0.06em; text-transform: uppercase;
    padding: 5px 8px; border-radius: 6px;
  }
  .diff-section-hdr.hdr-mica   { background: rgba(255,180,0,0.15); color: #ffc850; }
  .diff-section-hdr.hdr-sheet  { background: rgba(255,80,80,0.12);  color: #ff8080; }
  .diff-section-count { font-size: 11px; color: #aaa; padding: 0 2px; }
  .diff-table-wrap { overflow-y: auto; max-height: 260px; }
  .diff-table { width: 100%; border-collapse: collapse; font-size: 11px; }
  .diff-table th { color: #888; text-align: left; padding: 4px 6px; border-bottom: 1px solid rgba(255,255,255,0.08); position: sticky; top: 0; background: rgba(18,18,28,0.97); }
  .diff-table td { color: #ddd; padding: 4px 6px; border-bottom: 1px solid rgba(255,255,255,0.04); vertical-align: top; }
  .diff-table tr:last-child td { border-bottom: none; }
  .diff-table .scr-col { text-align: right; color: #888; }
  .diff-empty { font-size: 11px; color: #555; font-style: italic; padding: 4px 2px; }

  #booking-drop-zone {
    background: rgba(255,255,255,0.07); backdrop-filter: blur(12px);
    border: 2px dashed rgba(255,255,255,0.25);
    border-radius: 18px; padding: 52px 40px;
    text-align: center; cursor: pointer;
    transition: border-color 0.2s, background 0.2s, transform 0.1s;
  }
  #booking-drop-zone:hover,  #mass-drop-zone:hover    { border-color: #00c9d4; background: rgba(0,201,212,0.08); }
  #booking-drop-zone.dragover, #mass-drop-zone.dragover { border-color: #00c9d4; background: rgba(0,201,212,0.12); transform: scale(1.01); }
  #mass-drop-zone { background: rgba(255,255,255,0.07); backdrop-filter: blur(12px);
    border: 2px dashed rgba(255,255,255,0.25); border-radius: 22px; padding: 36px 28px;
    text-align: center; cursor: pointer; transition: border-color 0.2s, background 0.2s, transform 0.1s; }
  #mass-paste-box { background: rgba(255,255,255,0.07); backdrop-filter: blur(12px);
    border-radius: 18px; border: 2px dashed rgba(255,255,255,0.25); overflow: hidden; cursor: text; }
  #mass-paste-label { padding: 22px 28px 0; font-size: 0.78rem; font-weight: 700;
    color: rgba(255,255,255,0.5); text-transform: uppercase; letter-spacing: 0.06em; display: block; }
  #mass-paste-area { width: 100%; min-height: 160px; max-height: 260px;
    padding: 12px 28px 24px; border: none; outline: none; resize: vertical;
    font-size: 0.88rem; font-family: monospace; line-height: 1.6;
    color: white; background: transparent; box-sizing: border-box; }
  #mass-paste-area::placeholder { color: rgba(255,255,255,0.3); }
  #mass-action-btns { display: flex; gap: 12px; margin-top: 16px; }
  #mass-run-btn { flex: 1; padding: 16px; border-radius: 14px; border: none;
    background: linear-gradient(135deg,#00c9d4,#0078ff); color: white;
    font-size: 1rem; font-weight: 700; cursor: pointer; transition: opacity 0.18s; }
  #mass-run-btn:disabled { opacity: 0.5; cursor: not-allowed; }
  #mass-progress { display:none; margin-top:16px; max-height:320px; overflow-y:auto;
    background:rgba(0,0,0,0.35); border-radius:12px; padding:14px 18px;
    font-family:monospace; font-size:0.82rem; color:#e2e8f0; }
  #mass-reset-btn { display:none; margin-top:12px; background:transparent;
    border:1px solid rgba(255,255,255,0.25); border-radius:10px; color:rgba(255,255,255,0.6);
    padding:8px 20px; cursor:pointer; font-size:0.85rem; }

  #booking-paste-box {
    background: rgba(255,255,255,0.07); backdrop-filter: blur(12px);
    border-radius: 18px; border: 2px dashed rgba(255,255,255,0.25); overflow: hidden;
    cursor: text;
  }
  #booking-paste-label {
    padding: 22px 28px 0; font-size: 0.78rem; font-weight: 700;
    color: rgba(255,255,255,0.5); text-transform: uppercase; letter-spacing: 0.06em;
  }
  #booking-paste-area {
    width: 100%; min-height: 160px; max-height: 260px;
    padding: 12px 28px 24px; border: none; outline: none; resize: vertical;
    font-family: 'Courier New', monospace; font-size: 0.82rem;
    line-height: 1.6; color: white; background: transparent; box-sizing: border-box;
  }
  #booking-paste-area::placeholder { color: rgba(255,255,255,0.3); }

  .booking-field {
    display: block; width: 100%; margin-top: 12px;
    padding: 26px 28px; outline: none;
    border: 2px dashed rgba(255,255,255,0.25); border-radius: 18px;
    font-size: 0.95rem; font-family: inherit;
    background: rgba(255,255,255,0.07); backdrop-filter: blur(12px);
    color: white; box-sizing: border-box;
    transition: border-color 0.18s, background 0.18s;
  }
  .booking-field:focus { border-color: #00c9d4; background: rgba(0,201,212,0.08); }
  .booking-field::placeholder { color: rgba(255,255,255,0.35); }

  #booking-action-btns { margin-top: 48px; }
  #booking-run-btn {
    width: 100%; padding: 15px 10px; border: none; border-radius: 8px;
    font-size: 0.88rem; font-weight: 800; letter-spacing: 0.08em;
    text-transform: uppercase; cursor: pointer; transition: filter 0.18s, transform 0.1s;
    background: #00c9d4; color: #05081a;
    box-shadow: 0 4px 20px rgba(0,201,212,0.4);
  }
  #booking-run-btn:hover    { filter: brightness(1.12); transform: translateY(-1px); }
  #booking-run-btn:disabled { background: rgba(0,201,212,0.3); color: rgba(255,255,255,0.4); cursor: default; transform: none; filter: none; box-shadow: none; }

  #booking-progress {
    display: none; margin-top: 16px;
    background: rgba(0,0,0,0.55); backdrop-filter: blur(8px);
    color: #d4d4d4; border-radius: 12px; padding: 16px 20px;
    font-family: 'Courier New', monospace; font-size: 0.8rem;
    line-height: 1.7; max-height: 260px; overflow-y: auto;
    border: 1px solid rgba(255,255,255,0.1);
  }
  #booking-progress .line-ok   { color: #4ade80; }
  #booking-progress .line-warn { color: #facc15; }
  #booking-progress .line-err  { color: #f87171; }
  #booking-reset-btn {
    display: none; margin-top: 12px; width: 100%; padding: 12px;
    background: transparent; color: rgba(255,255,255,0.5);
    border: 1px solid rgba(255,255,255,0.2); border-radius: 8px;
    font-size: 0.88rem; cursor: pointer; transition: background 0.18s;
  }
  #booking-reset-btn:hover { background: rgba(255,255,255,0.08); color: white; }

  /* ── Left steps panel ─────────────────────────────────────────────────── */
  #steps-panel {
    width: 168px; flex-shrink: 0;
    position: relative;
  }
  #step-line {
    position: absolute; left: 17px; width: 2px;
    background: rgba(255,255,255,0.15); pointer-events: none;
  }
  .step {
    display: flex; align-items: center; gap: 12px;
    position: absolute; left: 0; right: 0;
  }
  .step-num {
    width: 36px; height: 36px; flex-shrink: 0;
    background: #00c9d4; color: #05081a; border-radius: 50%;
    display: flex; align-items: center; justify-content: center;
    font-size: 1rem; font-weight: 800;
  }
  .step-label {
    font-size: 0.95rem; font-weight: 600; color: rgba(255,255,255,0.85);
    line-height: 1.4;
  }

  /* ── Right content column ─────────────────────────────────────────────── */
  #main-col { flex: 1; min-width: 0; }

  /* ── Drop zone ────────────────────────────────────────────────────────── */
  #drop-zone {
    background: rgba(255,255,255,0.07);
    backdrop-filter: blur(12px);
    border: 2px dashed rgba(255,255,255,0.25);
    border-radius: 18px; padding: 52px 40px;
    text-align: center; cursor: pointer;
    transition: border-color 0.2s, background 0.2s, transform 0.1s;
  }
  #drop-zone:hover    { border-color: #00c9d4; background: rgba(0,201,212,0.08); }
  #drop-zone.dragover { border-color: #00c9d4; background: rgba(0,201,212,0.12); transform: scale(1.01); }
  #drop-zone.running  { border-color: #f0a500; background: rgba(240,165,0,0.08); cursor: default; }
  #drop-zone.done     { border-color: #22c55e; background: rgba(34,197,94,0.08); cursor: default; }
  #drop-zone.errored  { border-color: #ef4444; background: rgba(239,68,68,0.08); cursor: default; }

  #drop-icon  { font-size: 3.2rem; margin-bottom: 18px; }
  #drop-title { font-size: 1.25rem; font-weight: 700; margin-bottom: 8px; color: white; }
  #drop-sub   { font-size: 0.92rem; color: rgba(255,255,255,0.5); }
  #file-input { display: none; }

  /* ── Progress boxes ───────────────────────────────────────────────────── */
  #progress-box, #mica-progress {
    display: none; margin-top: 16px;
    background: rgba(0,0,0,0.55); backdrop-filter: blur(8px);
    color: #d4d4d4; border-radius: 12px; padding: 16px 20px;
    font-family: 'Courier New', monospace; font-size: 0.8rem;
    line-height: 1.7; max-height: 260px; overflow-y: auto;
    border: 1px solid rgba(255,255,255,0.1);
  }
  #mica-progress { margin-top: 14px; }
  #progress-box .line-ok,  #mica-progress .line-ok   { color: #4ade80; }
  #progress-box .line-warn, #mica-progress .line-warn { color: #facc15; }
  #progress-box .line-err, #mica-progress .line-err   { color: #f87171; }

  /* ── Action buttons ───────────────────────────────────────────────────── */
  #action-btns { display: flex; gap: 10px; margin-top: 48px; }
  #run-paste-btn, #mica-run-btn {
    flex: 1; padding: 15px 10px; border: none; border-radius: 8px;
    font-size: 0.88rem; font-weight: 800; letter-spacing: 0.08em;
    text-transform: uppercase; cursor: pointer; transition: filter 0.18s, transform 0.1s;
    white-space: nowrap;
  }
  #run-paste-btn {
    background: #00c9d4; color: #05081a;
    box-shadow: 0 4px 20px rgba(0,201,212,0.4);
  }
  #run-paste-btn:hover    { filter: brightness(1.12); transform: translateY(-1px); }
  #run-paste-btn:disabled { background: rgba(0,201,212,0.3); color: rgba(255,255,255,0.4); cursor: default; transform: none; filter: none; box-shadow: none; }
  #mica-run-btn {
    background: white; color: #05081a;
    border: none;
    box-shadow: 0 4px 20px rgba(255,255,255,0.2);
  }
  #mica-run-btn:hover    { filter: brightness(0.92); transform: translateY(-1px); }
  #mica-run-btn:disabled { background: rgba(255,255,255,0.2); color: rgba(255,255,255,0.4); cursor: default; transform: none; filter: none; box-shadow: none; }

  /* ── Open / Reset buttons ─────────────────────────────────────────────── */
  #open-btn {
    display: none; margin-top: 22px; width: 100%; padding: 16px;
    background: #00c9d4; color: #05081a; border: none; border-radius: 8px;
    font-size: 0.92rem; font-weight: 800; letter-spacing: 0.08em;
    text-transform: uppercase; cursor: pointer; transition: filter 0.18s;
    box-shadow: 0 4px 20px rgba(0,201,212,0.4);
  }
  #open-btn:hover { filter: brightness(1.12); }
  #open-btn.secondary {
    background: transparent; color: #00c9d4;
    border: 1px solid rgba(0,201,212,0.45);
    box-shadow: none; font-size: 0.85rem; padding: 11px 16px;
    letter-spacing: 0.04em; margin-top: 10px;
  }
  #open-btn.secondary:hover { background: rgba(0,201,212,0.08); filter: none; }
  #reset-btn {
    display: none; margin-top: 12px; width: 100%; padding: 12px;
    background: transparent; color: rgba(255,255,255,0.5);
    border: 1px solid rgba(255,255,255,0.2); border-radius: 8px;
    font-size: 0.88rem; cursor: pointer; transition: background 0.18s;
  }
  #reset-btn:hover { background: rgba(255,255,255,0.08); color: white; }

  .hint { margin-top: 24px; text-align: center; font-size: 0.82rem; color: rgba(255,255,255,0.4); }

  /* ── Or divider ───────────────────────────────────────────────────────── */
  .or-divider {
    display: flex; align-items: center; gap: 12px;
    margin: 20px 0; color: rgba(255,255,255,0.4); font-size: 0.85rem;
  }
  .or-divider::before, .or-divider::after {
    content: ''; flex: 1; height: 1px; background: rgba(255,255,255,0.15);
  }

  /* ── Paste box ────────────────────────────────────────────────────────── */
  #paste-box {
    background: rgba(255,255,255,0.07); backdrop-filter: blur(12px);
    border-radius: 18px; border: 2px dashed rgba(255,255,255,0.25); overflow: hidden;
  }
  #paste-label {
    padding: 22px 28px 0; font-size: 0.78rem; font-weight: 700;
    color: rgba(255,255,255,0.5); text-transform: uppercase; letter-spacing: 0.06em;
  }
  #paste-area {
    width: 100%; min-height: 160px; max-height: 260px;
    padding: 12px 28px 24px; border: none; outline: none; resize: vertical;
    font-family: 'Courier New', monospace; font-size: 0.82rem;
    line-height: 1.6; color: white; background: transparent; box-sizing: border-box;
  }
  #paste-area::placeholder { color: rgba(255,255,255,0.3); }

  /* ── Contact input ────────────────────────────────────────────────────── */
  #mica-contact {
    display: block; width: 100%; margin-top: 12px;
    padding: 26px 28px; outline: none;
    border: 2px dashed rgba(255,255,255,0.25); border-radius: 18px;
    font-size: 0.95rem; font-family: inherit;
    background: rgba(255,255,255,0.07); backdrop-filter: blur(12px);
    color: white; box-sizing: border-box;
    transition: border-color 0.18s, background 0.18s;
  }
  #mica-contact:focus { border-color: #00c9d4; background: rgba(0,201,212,0.08); }
  #mica-contact::placeholder { color: rgba(255,255,255,0.35); }

  /* ── Demo / Production toggle ─────────────────────────────────────────── */
  #mica-mode-toggle, #booking-mode-toggle, #mass-mode-toggle {
    display: flex; gap: 8px; margin-top: 12px;
  }
  .mode-btn {
    flex: 1; padding: 10px 0; border-radius: 12px; border: 2px solid rgba(255,255,255,0.2);
    background: rgba(255,255,255,0.07); color: rgba(255,255,255,0.5);
    font-size: 0.85rem; font-weight: 700; letter-spacing: 0.05em; cursor: pointer;
    transition: all 0.18s;
  }
  .mode-btn:hover { border-color: rgba(255,255,255,0.4); color: rgba(255,255,255,0.8); }
  .mode-btn.active { background: rgba(0,201,212,0.18); border-color: #00c9d4; color: #00c9d4; }
  #mode-prod.active, #bmode-prod.active, #mmode-prod.active { background: rgba(255,160,50,0.18); border-color: #ffa032; color: #ffa032; }

  /* ── Footer ──────────────────────────────────────────────────────────── */
  #page-footer {
    display: flex; justify-content: center; align-items: center;
    padding: 40px 0 36px;
    margin-top: 48px;
  }
  #footer-logo {
    height: 72px;
    opacity: 0.55;
    filter: drop-shadow(0 2px 8px rgba(0,0,0,0.4));
    transition: opacity 0.2s;
  }
  #footer-logo:hover { opacity: 0.85; }
  /* Success toast */
  #success-toast {
    display: none;
    position: fixed; top: 50%; left: 50%; transform: translate(-50%, -50%);
    background: #1a1a2e; border: 2px solid #00e676; border-radius: 16px;
    padding: 40px 60px; text-align: center; z-index: 9999;
    box-shadow: 0 0 60px rgba(0,230,118,0.3);
    animation: toastIn 0.3s ease;
  }
  #success-toast.show { display: block; }
  #success-toast .toast-icon { font-size: 48px; margin-bottom: 12px; }
  #success-toast .toast-title { color: #00e676; font-size: 24px; font-weight: bold; margin-bottom: 8px; }
  #success-toast .toast-sub { color: #aaa; font-size: 14px; margin-bottom: 24px; }
  #success-toast button { background: #00e676; color: #000; border: none; border-radius: 8px; padding: 10px 32px; font-size: 15px; font-weight: bold; cursor: pointer; }
  #toast-overlay { display: none; position: fixed; inset: 0; background: rgba(0,0,0,0.6); z-index: 9998; }
  #toast-overlay.show { display: block; }
  @keyframes toastIn { from { opacity:0; transform: translate(-50%,-50%) scale(0.85); } to { opacity:1; transform: translate(-50%,-50%) scale(1); } }
</style>
</head>
<body>
<div id="toast-overlay"></div>
<div id="success-toast">
  <div class="toast-icon">✅</div>
  <div class="toast-title">Complete!</div>
  <div class="toast-sub" id="toast-sub">Mica sales plan updated successfully.</div>
  <button onclick="closeToast()">OK</button>
</div>
<video id="bg-video" autoplay muted loop playsinline>
  <source src="/bg-video" type="video/mp4">
</video>
<header style="position:relative;">
  __LOGO_IMG__
  <h1>Angel Studios</h1>
  <div id="user-nav" style="margin-left:auto;display:flex;align-items:center;gap:12px;font-size:13px;">
    <span id="user-email" style="color:#aaa;"></span>
    <span style="position:absolute;left:50%;transform:translateX(-50%);color:#aaa;font-size:13px;font-weight:bold;">4/28 10:15AM Update</span>
    <a href="/aliases" style="color:#aaa;text-decoration:none;font-size:12px;">Venue Aliases</a>
    <a id="profile-link" href="/auth/profile" style="color:#00bcd4;text-decoration:none;display:none;">My Profile</a>
    <a id="logout-link" href="/auth/logout" style="color:#888;text-decoration:none;display:none;">Sign Out</a>
  </div>
</header>
<script>
  // Show user nav / credential banner
  fetch('/auth/me').then(r=>r.json()).then(d=>{
    if(d.auth){
      if(!d.local){
        // Server mode: show email and sign-out
        document.getElementById('user-email').textContent = d.email;
        document.getElementById('logout-link').style.display='inline';
      }
      document.getElementById('profile-link').style.display='inline';
      if(!d.has_comscore || !d.has_mica){
        const banner = document.createElement('div');
        banner.style.cssText='background:#e65100;color:#fff;padding:10px 32px;font-size:13px;';
        banner.innerHTML='⚠ Your Comscore or Mica credentials are not saved. <a href="/auth/profile" style="color:#fff;font-weight:bold;">Set them up →</a>';
        document.querySelector('header').insertAdjacentElement('afterend', banner);
      }
    }
  }).catch(()=>{});
</script>

<div id="tab-bar">
  <button class="tab-btn active" onclick="switchTab('holdover')">Angel Holdover Assistant</button>
  <button class="tab-btn"        onclick="switchTab('booking')">Angel Booking Assistant</button>
  <button class="tab-btn"        onclick="switchTab('mass')">Angel Mass Booking</button>
</div>

<div id="tab-holdover" class="tab-panel active">
<main>
  <div id="steps-panel">
    <div id="step-line"></div>
    <div class="step" id="step1">
      <div class="step-num">1</div>
      <div class="step-label">Insert Booking</div>
    </div>
    <div class="step" id="step2">
      <div class="step-num">2</div>
      <div class="step-label">Fill out Contact</div>
    </div>
    <div class="step" id="step3">
      <div class="step-num">3</div>
      <div class="step-label">Pull Comscore Report and/or Update Mica Booking</div>
    </div>
  </div>

  <div id="main-col">
    <div id="drop-zone" onclick="document.getElementById('file-input').click()">
      <div id="drop-icon">📄</div>
      <div id="drop-title">Drop your booking CSV, Excel, PDF, or screenshot here</div>
      <div id="drop-sub">Accepts .csv, .xlsx, .xls, .pdf, .png, .jpg, .jpeg, .webp</div>
      <input type="file" id="file-input" accept=".csv,.xlsx,.xls,.pdf,.png,.jpg,.jpeg,.webp">
    </div>

    <div class="or-divider">or paste booking data / screenshot</div>

    <div id="paste-box">
      <div id="paste-label">Paste here (Ctrl+V) — text or screenshot</div>
      <textarea id="paste-area" placeholder="Ctrl+V to paste a screenshot OR booking text here. For screenshots, Windows OCR will extract FINAL locations automatically."></textarea>
    </div>

    <input id="mica-contact" placeholder="Contact / Booker (auto-filled from booking)">

    <div id="mica-mode-toggle">
      <button class="mode-btn active" id="mode-demo" onclick="setMicaMode('demo')">Demo</button>
      <button class="mode-btn"        id="mode-prod" onclick="setMicaMode('prod')">Production</button>
    </div>

    <div id="action-btns">
      <button id="run-paste-btn" onclick="runPaste()">Pull Comscore Report</button>
      <button id="mica-run-btn" onclick="runMica()">Update Mica ▶</button>
    </div>

    <button id="open-btn"  onclick="window.open('/dashboard','_blank')">Open Dashboard ↗</button>
    <div id="progress-box"></div>
    <div id="mica-progress"></div>
    <button id="reset-btn" onclick="resetUI()">Run Another File</button>
    <p class="hint">Only FINAL booking locations are scraped from Comscore. Mica updates automatically after.</p>
  </div>
</main>
</div><!-- end #tab-holdover -->

<div id="tab-booking" class="tab-panel">
  <main id="booking-main">
    <div id="booking-steps-panel">
      <div id="booking-step-line"></div>
      <div class="step" id="bstep1">
        <div class="step-num">1</div>
        <div class="step-label">Insert Booking</div>
      </div>
      <div class="step" id="bstep2">
        <div class="step-num">2</div>
        <div class="step-label">Fill out Contact &amp; Title</div>
      </div>
      <div class="step" id="bstep3">
        <div class="step-num">3</div>
        <div class="step-label">Update Mica Sales Plan</div>
      </div>
    </div>

    <div id="booking-main-col">
      <div id="booking-drop-zone" onclick="document.getElementById('booking-file-input').click()">
        <div id="booking-drop-icon">📄</div>
        <div id="booking-drop-title">Drop your booking CSV, Excel, PDF, or screenshot here</div>
        <div id="booking-drop-sub">or click to browse</div>
        <input type="file" id="booking-file-input" accept=".csv,.xlsx,.xls,.pdf,.png,.jpg,.jpeg,.webp" style="display:none">
      </div>

      <div class="or-divider" id="booking-or-divider">or paste booking data / screenshot</div>

      <div id="booking-paste-box" onclick="document.getElementById('booking-paste-area').focus()">
        <label for="booking-paste-area" id="booking-paste-label">Paste here (Ctrl+V) — text or screenshot</label>
        <textarea id="booking-paste-area" placeholder="Ctrl+V to paste booking text here."></textarea>
      </div>

      <input id="booking-contact" class="booking-field" placeholder="Contact / Booker">
      <input id="booking-title"   class="booking-field" placeholder="Title (e.g. Solo Mio)">

      <div id="booking-mode-toggle">
        <button class="mode-btn active" id="bmode-demo" onclick="setBookingMode('demo')">Demo</button>
        <button class="mode-btn"        id="bmode-prod" onclick="setBookingMode('prod')">Production</button>
      </div>

      <div id="booking-action-btns">
        <button id="booking-run-btn" onclick="runBookingUpdate()">Update Sales Plan ▶</button>
      </div>

      <div id="booking-progress"></div>
      <button id="booking-reset-btn" onclick="resetBookingUI()">Run Another</button>
      <p class="hint">Updates the title in the Mica sales plan for the specified contact.</p>
    </div>

    <div id="diff-panel">
      <div id="diff-panel-title">Booking Differences</div>

      <!-- Section 1: Agreed in Mica but not on booking sheet -->
      <div class="diff-section">
        <div class="diff-section-hdr hdr-mica">In Mica — Not on Sheet</div>
        <div id="diff-mica-count" class="diff-section-count"></div>
        <div class="diff-table-wrap">
          <table class="diff-table">
            <thead><tr><th>Theatre</th><th>City</th><th>St</th><th class="scr-col">Scr</th></tr></thead>
            <tbody id="diff-mica-tbody"></tbody>
          </table>
        </div>
      </div>

      <!-- Section 2: On booking sheet but not found in Mica -->
      <div class="diff-section">
        <div class="diff-section-hdr hdr-sheet">On Sheet — Not in Mica</div>
        <div id="diff-sheet-count" class="diff-section-count"></div>
        <div class="diff-table-wrap">
          <table class="diff-table">
            <thead><tr><th>Theatre</th><th>Closest Match</th></tr></thead>
            <tbody id="diff-sheet-tbody"></tbody>
          </table>
        </div>
      </div>
    </div>
  </main>
</div><!-- end #tab-booking -->

<div id="tab-mass" class="tab-panel">
  <main id="booking-main">
    <div id="booking-steps-panel">
      <div id="booking-step-line"></div>
      <div class="step" id="mstep1">
        <div class="step-num">1</div>
        <div class="step-label">Insert Booking</div>
      </div>
      <div class="step" id="mstep2">
        <div class="step-num">2</div>
        <div class="step-label">Optional: Filter by Title</div>
      </div>
      <div class="step" id="mstep3">
        <div class="step-num">3</div>
        <div class="step-label">Run Mass Booking in Mica</div>
      </div>
    </div>

    <div id="booking-main-col">
      <div id="mass-drop-zone" onclick="document.getElementById('mass-file-input').click()">
        <div id="mass-drop-icon">📄</div>
        <div id="mass-drop-title">Drop your booking CSV, Excel, PDF, or screenshot here</div>
        <div id="mass-drop-sub">or click to browse</div>
        <input type="file" id="mass-file-input" accept=".csv,.xlsx,.xls,.pdf,.png,.jpg,.jpeg,.webp" style="display:none">
      </div>

      <div class="or-divider" id="mass-or-divider">or paste booking data / screenshot</div>

      <div id="mass-paste-box" onclick="document.getElementById('mass-paste-area').focus()">
        <label for="mass-paste-area" id="mass-paste-label">Paste here (Ctrl+V) — text or screenshot</label>
        <textarea id="mass-paste-area" placeholder="Ctrl+V to paste booking text here."></textarea>
      </div>

      <input id="mass-title" class="booking-field" placeholder="Title filter (optional — leave blank to process all films)">
      <input id="mass-circuit" class="booking-field" placeholder="Circuit filter (optional — e.g. Cineplex)">

      <div id="mass-mode-toggle">
        <button class="mode-btn active" id="mmode-demo" onclick="setMassMode('demo')">Demo</button>
        <button class="mode-btn"        id="mmode-prod" onclick="setMassMode('prod')">Production</button>
      </div>

      <label style="display:flex;align-items:center;gap:8px;margin-top:10px;color:rgba(255,255,255,0.75);font-size:0.88rem;cursor:pointer;">
        <input type="checkbox" id="mass-dry-run" style="width:16px;height:16px;cursor:pointer;">
        Dry Run — preview matches only, no changes made in Mica
      </label>

      <div id="mass-action-btns">
        <button id="mass-run-btn" onclick="runMassBookingUpdate()">Run Mass Booking ▶</button>
      </div>

      <div id="mass-progress"></div>
      <button id="mass-reset-btn" onclick="resetMassUI()">Run Another</button>
      <p class="hint">Automatically processes every buyer/contact found in the booking sheet.</p>
    </div>
  </main>
</div><!-- end #tab-mass -->

<footer id="page-footer">
  __LOGO_FOOTER__
</footer>

<script>
const dropZone    = document.getElementById('drop-zone');
const progressBox = document.getElementById('progress-box');
const openBtn     = document.getElementById('open-btn');
const resetBtn    = document.getElementById('reset-btn');
const fileInput   = document.getElementById('file-input');

let lastBookingText = '';  // stores booking text for auto-chaining Part 2
let _jobDone = false;       // prevents onerror from overwriting a completed job

// Parse Buyer + Film from booking CSV and populate fields
const BUYER_COLS = ['buyer', 'br'];
const FILM_COLS  = ['film', 'attraction', 'title', 'production'];

function extractBookingMetaOnePerLine(text) {
  // One-per-line format: each cell on its own line (email copy-paste from Outlook/Gmail)
  const values = text.split(/\r?\n/).map(l => l.trim()).filter(l => l.length > 0);

  // ── __COLUMN__ format (e.g. __DMA__, __SALES__, __THEATRE__, __ __) ──
  if (values.length > 0 && /^__.*__$/.test(values[0])) {
    const headers = [];
    for (const v of values) {
      if (!/^__.*__$/.test(v)) break;
      const inner = v.replace(/^__|__$/g, '').trim().toUpperCase();
      headers.push(inner || 'BLANK');
    }
    const nCols = headers.length;
    const salesIdx = headers.findIndex(h => ['SALES','BUYER','BR'].includes(h));
    if (salesIdx >= 0 && values.length > nCols + salesIdx) {
      const contact = values[nCols + salesIdx] || '';
      return { contact, film: '' };
    }
    return null;
  }

  // Find "Action" or "Policy" column
  let actionIdx = -1;
  for (let i = 0; i < values.length; i++) {
    const v = values[i].toLowerCase();
    if (v === 'action' || v === 'policy') { actionIdx = i; break; }
  }
  if (actionIdx < 0) return null;
  const KNOWN_COLS = ['buyer','br','unit','theatre','theater','attraction','film','title','type','media','prt'];
  let headerStart = 0;
  for (let i = 0; i <= actionIdx; i++) {
    if (KNOWN_COLS.includes(values[i].toLowerCase())) { headerStart = i; break; }
  }
  const headers  = values.slice(headerStart, actionIdx + 1);
  const nCols    = headers.length;
  const remainder = values.slice(actionIdx + 1);
  if (remainder.length < nCols) return null;
  const firstRow = remainder.slice(0, nCols);
  const buyerIdx = headers.findIndex(h => BUYER_COLS.includes(h.toLowerCase()));
  const filmIdx  = headers.findIndex(h => FILM_COLS.includes(h.toLowerCase()));
  return {
    contact: buyerIdx >= 0 ? firstRow[buyerIdx] : '',
    film:    filmIdx  >= 0 ? firstRow[filmIdx]  : '',
  };
}

function extractBookingMeta(text) {
  const lines = text.split(/\r?\n/).filter(l => l.trim());
  if (lines.length < 2) return null;
  const HEADER_KEYS = ['buyer', 'br', 'attraction', 'film', 'title', 'production', 'theatre', 'theater'];
  // Detect delimiter from the line with the MOST separators among first 5 lines
  let maxTabs = 0, maxCommas = 0;
  for (const line of lines.slice(0, 5)) {
    maxTabs   = Math.max(maxTabs,   (line.match(/\t/g)  || []).length);
    maxCommas = Math.max(maxCommas, (line.match(/,/g)   || []).length);
  }
  // One-per-line format: no tabs or commas found
  if (maxTabs === 0 && maxCommas < 2) return extractBookingMetaOnePerLine(text);

  const delim = maxTabs > maxCommas ? '\t' : ',';
  const parse = line => line.split(delim).map(c => c.trim().replace(/^"|"$/g, ''));
  // Find the header row (first line with 4+ cols and a known header keyword)
  let headerIdx = 0;
  for (let i = 0; i < Math.min(lines.length - 1, 5); i++) {
    const c = parse(lines[i]).map(s => s.toLowerCase());
    if (c.length >= 4 && HEADER_KEYS.some(k => c.includes(k))) { headerIdx = i; break; }
  }
  const cols = parse(lines[headerIdx]).map(c => c.toLowerCase());
  const vals = parse(lines[headerIdx + 1]);
  const buyerIdx = cols.findIndex(c => BUYER_COLS.includes(c));
  const filmIdx  = cols.findIndex(c => FILM_COLS.includes(c));
  return {
    contact: buyerIdx >= 0 ? vals[buyerIdx] : '',
    film:    filmIdx  >= 0 ? vals[filmIdx]  : '',
  };
}

function autoFillMicaFields(text) {
  const meta = extractBookingMeta(text);
  if (!meta) return;
  // Only use contact if it looks like a full name (has a space) — not initials like "LA"
  const contactEl = document.getElementById('mica-contact');
  if (meta.contact && meta.contact.includes(' ') && !contactEl.value.trim()) {
    contactEl.value = meta.contact;
  }
}

// ── Drag & drop events ────────────────────────────────────────────────────
dropZone.addEventListener('dragover',  e => { e.preventDefault(); dropZone.classList.add('dragover'); });
dropZone.addEventListener('dragleave', ()  => dropZone.classList.remove('dragover'));
dropZone.addEventListener('drop', e => {
  e.preventDefault();
  dropZone.classList.remove('dragover');
  const file = e.dataTransfer.files[0];
  if (file) handleFile(file);
});
fileInput.addEventListener('change', () => {
  if (fileInput.files[0]) handleFile(fileInput.files[0]);
});

// ── Booking drop zone events ──────────────────────────────────────────────
const bookingDropZone  = document.getElementById('booking-drop-zone');
const bookingFileInput = document.getElementById('booking-file-input');

bookingDropZone.addEventListener('dragover',  e => { e.preventDefault(); bookingDropZone.classList.add('dragover'); });
bookingDropZone.addEventListener('dragleave', ()  => bookingDropZone.classList.remove('dragover'));
bookingDropZone.addEventListener('drop', e => {
  e.preventDefault();
  bookingDropZone.classList.remove('dragover');
  const file = e.dataTransfer.files[0];
  if (file) handleBookingFile(file);
});
bookingFileInput.addEventListener('change', () => {
  if (bookingFileInput.files[0]) handleBookingFile(bookingFileInput.files[0]);
});

async function handleBookingFile(file) {
  const ext = file.name.toLowerCase().split('.').pop();
  if (!['csv', 'xlsx', 'xls', 'pdf', 'png', 'jpg', 'jpeg', 'webp'].includes(ext)) {
    alert('Please select a .csv, .xlsx, .xls, .pdf, or image file.');
    return;
  }
  document.getElementById('booking-drop-icon').textContent  = '⚙️';
  document.getElementById('booking-drop-title').textContent = 'Reading ' + file.name + '…';
  document.getElementById('booking-drop-sub').textContent   = ['png','jpg','jpeg','webp'].includes(ext) ? 'Reading screenshot with Claude vision…' : '';

  if (ext === 'csv') {
    const text = await file.text();
    document.getElementById('booking-paste-area').value = text;
    document.getElementById('booking-paste-area').dispatchEvent(new Event('input'));
    document.getElementById('booking-drop-icon').textContent  = '✅';
    document.getElementById('booking-drop-title').textContent = file.name + ' loaded';
    document.getElementById('booking-drop-sub').textContent   = 'Booking data is ready below';
    return;
  }

  // Excel / PDF / image: upload to server for extraction
  let result;
  try {
    const res = await fetch('/booking-parse-file', {
      method: 'POST',
      headers: { 'Content-Type': 'application/octet-stream', 'X-Filename': file.name },
      body: await file.arrayBuffer(),
    });
    result = await res.json();
  } catch (err) {
    document.getElementById('booking-drop-icon').textContent  = '❌';
    document.getElementById('booking-drop-title').textContent = 'Failed to read PDF';
    document.getElementById('booking-drop-sub').textContent   = err.message;
    return;
  }

  if (result.error) {
    document.getElementById('booking-drop-icon').textContent  = '❌';
    document.getElementById('booking-drop-title').textContent = 'Failed to read file';
    document.getElementById('booking-drop-sub').textContent   = result.error;
    return;
  }

  document.getElementById('booking-paste-area').value = result.text;
  document.getElementById('booking-paste-area').dispatchEvent(new Event('input'));
  document.getElementById('booking-drop-icon').textContent  = '✅';
  document.getElementById('booking-drop-title').textContent = file.name + ' loaded';
  document.getElementById('booking-drop-sub').textContent   = result.rows + ' booking rows extracted';
}

// ── Clipboard paste (screenshot or text) ─────────────────────────────────
document.getElementById('paste-area').addEventListener('paste', e => {
  const items = e.clipboardData && e.clipboardData.items;
  if (!items) return;

  // Check if clipboard contains an image
  for (const item of items) {
    if (item.type.startsWith('image/')) {
      e.preventDefault(); // Don't paste raw image data into textarea
      const blob = item.getAsFile();
      const file = new File([blob], 'screenshot.png', { type: 'image/png' });
      handleFile(file);
      return;
    }
  }
  // Otherwise it's text — let the default paste happen, auto-fill fires via input event
});

// Auto-fill Mica contact as soon as text is pasted into the textarea
document.getElementById('paste-area').addEventListener('input', () => {
  const text = document.getElementById('paste-area').value.trim();
  if (text) autoFillMicaFields(text);
});

// ── Booking paste area ────────────────────────────────────────────────────
document.getElementById('booking-paste-area').addEventListener('paste', e => {
  const items = Array.from((e.clipboardData && e.clipboardData.items) || []);
  // Excel copies both image + text — only block if it's a pure image with no text
  const hasText = items.some(i => i.type === 'text/plain' || i.type === 'text/html' || i.type === 'text/csv');
  if (hasText) return; // let default paste happen — text will appear in textarea
  const imageItem = items.find(i => i.type.startsWith('image/'));
  if (imageItem) {
    e.preventDefault();
    const blob = imageItem.getAsFile();
    if (blob) handleBookingFile(new File([blob], 'screenshot.png', { type: 'image/png' }));
  }
});

document.getElementById('booking-paste-area').addEventListener('input', () => {
  const text = document.getElementById('booking-paste-area').value.trim();
  if (!text) return;
  // Auto-fill Contact + Title from parsed booking data
  const meta = extractBookingMeta(text);
  if (meta) {
    const contactEl = document.getElementById('booking-contact');
    const titleEl   = document.getElementById('booking-title');
    if (contactEl && !contactEl.value.trim() && meta.contact && meta.contact.includes(' ')) contactEl.value = meta.contact;
    if (titleEl   && !titleEl.value.trim()   && meta.film)    titleEl.value   = meta.film;
  }
});

// Also catch Ctrl+V anywhere on the page (for screenshots pasted outside the textarea)
document.addEventListener('paste', e => {
  // Only handle if neither paste area is focused
  const active = document.activeElement;
  if (active === document.getElementById('paste-area')) return;
  if (active === document.getElementById('booking-paste-area')) return;
  const items = e.clipboardData && e.clipboardData.items;
  if (!items) return;
  for (const item of items) {
    if (item.type.startsWith('image/')) {
      e.preventDefault();
      const blob = item.getAsFile();
      const file = new File([blob], 'screenshot.png', { type: 'image/png' });
      // Route to the correct tab handler
      if (document.getElementById('tab-booking').classList.contains('active')) {
        handleBookingFile(file);
      } else {
        handleFile(file);
      }
      return;
    }
  }
});

// ── Main handler ──────────────────────────────────────────────────────────
async function handleFile(file) {
  const allowed = ['.csv', '.pdf', '.png', '.jpg', '.jpeg', '.webp'];
  if (!allowed.some(ext => file.name.toLowerCase().endsWith(ext))) {
    alert('Please select a .csv, .pdf, or image file (.png, .jpg, .jpeg, .webp).');
    return;
  }

  setRunning(file.name);

  // Read & upload (binary for images/PDFs, text for CSVs)
  const isBinary = /\.(png|jpe?g|webp|pdf)$/i.test(file.name);
  const body = isBinary ? await file.arrayBuffer() : await file.text();
  if (!isBinary) { lastBookingText = body; autoFillMicaFields(body); }  // save + auto-fill
  let job_id;
  try {
    const res = await fetch('/upload', {
      method: 'POST',
      headers: { 'Content-Type': isBinary ? 'application/octet-stream' : 'text/plain', 'X-Filename': file.name },
      body,
    });
    ({ job_id } = await res.json());
  } catch (err) {
    setError('Upload failed: ' + err.message);
    return;
  }

  // Stream output via SSE
  _jobDone = false;
  const src = new EventSource('/stream/' + job_id);
  src.onmessage = e => {
    const line = e.data;
    if (line === '__PING__')             { return; }
    if (line === '__DONE__')             { src.close(); return; }
    if (line === '__SUCCESS__')          { _jobDone = true; src.close(); setDone(); return; }
    if (line.startsWith('__ERROR__'))   { _jobDone = true; src.close(); setError(line.replace('__ERROR__', '').trim()); return; }
    if (line.startsWith('__BOOKING_CSV__:')) {
      try {
        const data = JSON.parse(line.slice('__BOOKING_CSV__:'.length));
        lastBookingText = data.csv;
        autoFillMicaFields(data.csv);
        document.getElementById('paste-area').value = data.csv;
        document.getElementById('drop-sub').textContent = 'Vision complete — logging into Comscore…';
      } catch(e) {}
      return;
    }
    appendLine(line);
  };
  src.onerror = async () => {
    if (_jobDone) return;
    src.close();
    appendLine('--- Connection dropped — polling for result…');
    for (let i = 0; i < 60; i++) {
      await new Promise(r => setTimeout(r, 5000));
      if (_jobDone) return;
      try {
        const r = await fetch('/job-status/' + job_id);
        const d = await r.json();
        if (d.status === 'success') { _jobDone = true; setDone(); return; }
        if (d.status && d.status.startsWith('error:')) { _jobDone = true; setError(d.status.replace('error:', '').trim()); return; }
      } catch(_) {}
    }
    setError('Connection to launcher lost.');
  };
}

// ── UI helpers ────────────────────────────────────────────────────────────
function setRunning(filename) {
  const isImage = /\.(png|jpe?g|webp)$/i.test(filename);
  dropZone.className = 'running';
  document.getElementById('drop-icon').textContent  = '⚙️';
  document.getElementById('drop-title').textContent = 'Processing ' + filename + '…';
  document.getElementById('drop-sub').textContent   = isImage ? 'Reading screenshot with AI vision…' : 'Logging into Comscore and scraping data';
  progressBox.style.display = 'block';
  progressBox.innerHTML = '';
  document.getElementById('run-paste-btn').disabled = true;
}

function setDone() {
  dropZone.className = 'done';
  document.getElementById('drop-icon').textContent  = '✅';
  document.getElementById('drop-title').textContent = 'Done! Dashboard is ready.';
  document.getElementById('drop-sub').textContent   = '';
  openBtn.classList.remove('secondary');
  openBtn.textContent    = 'Open Dashboard ↗';
  openBtn.style.display  = 'block';
  resetBtn.style.display = 'block';
  const rpb = document.getElementById('run-paste-btn');
  rpb.disabled = false; rpb.textContent = 'Pull Comscore Report';
  window.open('/dashboard', '_blank');
}

function setError(msg) {
  dropZone.className = 'errored';
  const rpb = document.getElementById('run-paste-btn');
  rpb.disabled = false; rpb.textContent = 'Pull Comscore Report';
  document.getElementById('drop-icon').textContent  = '❌';
  document.getElementById('drop-title').textContent = 'Something went wrong';
  document.getElementById('drop-sub').textContent   = msg;
  resetBtn.style.display = 'block';
}

function appendLine(text) {
  const div = document.createElement('div');
  if      (/error|failed|not found/i.test(text)) div.className = 'line-err';
  else if (/login successful|dashboard saved|scraping/i.test(text)) div.className = 'line-ok';
  else if (/not in master list/i.test(text)) div.className = 'line-warn';
  div.textContent = text;
  progressBox.appendChild(div);
  progressBox.scrollTop = progressBox.scrollHeight;
}

function showToast(msg) {
  if (msg) document.getElementById('toast-sub').textContent = msg;
  document.getElementById('success-toast').classList.add('show');
  document.getElementById('toast-overlay').classList.add('show');
}
function closeToast() {
  document.getElementById('success-toast').classList.remove('show');
  document.getElementById('toast-overlay').classList.remove('show');
}

async function runPaste() {
  const text = document.getElementById('paste-area').value.trim();
  if (!text) { alert('Please paste some booking data first.'); return; }

  lastBookingText = text;  // save for auto-chain to Part 2
  autoFillMicaFields(text);

  const btn = document.getElementById('run-paste-btn');
  btn.disabled = true;
  btn.textContent = 'Running…';

  setRunning('pasted data');

  let job_id;
  try {
    const res = await fetch('/upload', {
      method: 'POST',
      headers: { 'Content-Type': 'text/plain', 'X-Filename': 'paste.csv' },
      body: text,
    });
    ({ job_id } = await res.json());
  } catch (err) {
    setError('Upload failed: ' + err.message);
    btn.disabled = false;
    btn.textContent = 'Pull Comscore Report';
    return;
  }

  _jobDone = false;
  const src = new EventSource('/stream/' + job_id);
  src.onmessage = e => {
    const line = e.data;
    if (line === '__PING__')           { return; }
    if (line === '__DONE__')           { src.close(); return; }
    if (line === '__SUCCESS__')        { _jobDone = true; src.close(); setDone(); return; }
    if (line.startsWith('__ERROR__')) { _jobDone = true; src.close(); setError(line.replace('__ERROR__', '').trim()); return; }
    appendLine(line);
  };
  src.onerror = async () => {
    if (_jobDone) return;
    src.close();
    appendLine('--- Connection dropped — polling for result…');
    for (let i = 0; i < 60; i++) {
      await new Promise(r => setTimeout(r, 5000));
      if (_jobDone) return;
      try {
        const r = await fetch('/job-status/' + job_id);
        const d = await r.json();
        if (d.status === 'success') { _jobDone = true; setDone(); return; }
        if (d.status && d.status.startsWith('error:')) { _jobDone = true; setError(d.status.replace('error:', '').trim()); return; }
      } catch(_) {}
    }
    setError('Connection to launcher lost.');
  };
}

// ── Demo / Production toggle ──────────────────────────────────────────────
let micaMode = 'demo';
function setMicaMode(mode) {
  micaMode = mode;
  document.getElementById('mode-demo').classList.toggle('active', mode === 'demo');
  document.getElementById('mode-prod').classList.toggle('active', mode === 'prod');
}

// ── Mica update (auto-triggered after Part 1, or manually via button) ────
async function runMica() {
  const contact = document.getElementById('mica-contact').value.trim();
  // Use cached booking text OR whatever is currently in the paste box
  const booking = lastBookingText || document.getElementById('paste-area').value.trim();

  if (!contact) { alert('Please fill in the Contact / Booker field.'); return; }
  if (!booking) { alert('Please paste the booking data in the text box first.'); return; }

  const btn = document.getElementById('mica-run-btn');
  btn.disabled = true;
  btn.textContent = 'Updating Mica...';

  const prog = document.getElementById('mica-progress');
  prog.style.display = 'block';
  prog.innerHTML = '';
  prog.scrollIntoView({ behavior: 'smooth', block: 'start' });

  const resetBtn = () => { btn.disabled = false; btn.textContent = 'Update Mica ▶'; };

  let job_id;
  try {
    const res = await fetch('/mica-update', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ contact, booking, mode: micaMode }),
    });
    const data = await res.json();
    job_id = data.job_id;
    if (!job_id) { micaAppendLine('ERROR: Server returned no job ID — check credentials in Profile.', 'line-err'); resetBtn(); return; }
  } catch (err) {
    micaAppendLine('ERROR: ' + err.message + ' — try refreshing the page.', 'line-err');
    resetBtn(); return;
  }

  const src = new EventSource('/mica-stream/' + job_id);
  src.onmessage = e => {
    const line = e.data;
    if (line === '__PING__')           { return; }
    if (line === '__DONE__')           { src.close(); resetBtn(); showToast('Mica sales plan updated successfully.'); return; }
    if (line === '__SUCCESS__')        { src.close(); resetBtn(); showToast('Mica sales plan updated successfully.'); micaAppendLine('✓ Mica update complete!', 'line-ok'); return; }
    if (line.startsWith('__ERROR__')) { src.close(); resetBtn(); micaAppendLine('ERROR: ' + line.replace('__ERROR__', '').trim(), 'line-err'); return; }
    micaAppendLine(line);
  };
  src.onerror = async () => {
    src.close();
    micaAppendLine('--- Connection dropped — polling for result…', 'line-warn');
    for (let i = 0; i < 60; i++) {
      await new Promise(r => setTimeout(r, 5000));
      try {
        const r = await fetch('/job-status/' + job_id);
        const d = await r.json();
        if (d.status === 'success') { resetBtn(); showToast('Mica sales plan updated successfully.'); micaAppendLine('✓ Mica update complete!', 'line-ok'); return; }
        if (d.status && d.status.startsWith('error:')) { resetBtn(); micaAppendLine('ERROR: ' + d.status.replace('error:','').trim(), 'line-err'); return; }
      } catch(_) {}
    }
    resetBtn(); micaAppendLine('Connection lost.', 'line-err');
  };
}

function micaAppendLine(text, cls) {
  const prog = document.getElementById('mica-progress');
  const div = document.createElement('div');
  if      (cls)                                         div.className = cls;
  else if (/error|failed/i.test(text))                  div.className = 'line-err';
  else if (/warning/i.test(text))                       div.className = 'line-warn';
  else if (/complete|✓|status →|screening/i.test(text)) div.className = 'line-ok';
  div.textContent = text;
  prog.appendChild(div);
  prog.scrollTop = prog.scrollHeight;
}

function switchTab(name) {
  document.querySelectorAll('.tab-btn').forEach((b, i) => {
    b.classList.toggle('active', ['holdover','booking','mass'][i] === name);
  });
  document.querySelectorAll('.tab-panel').forEach(p => p.classList.remove('active'));
  document.getElementById('tab-' + name).classList.add('active');
  if (name === 'holdover') setTimeout(alignSteps, 40);
  if (name === 'booking')  setTimeout(alignBookingSteps, 40);
}

function alignBookingSteps() {
  const panel = document.getElementById('booking-steps-panel');
  if (!panel) return;
  const pRect = panel.getBoundingClientRect();
  const scroll = window.scrollY;
  const half = 18;
  const pairs = [
    [document.getElementById('bstep1'), document.getElementById('booking-or-divider')],
    [document.getElementById('bstep2'), document.getElementById('booking-contact')],
    [document.getElementById('bstep3'), document.getElementById('booking-action-btns')],
  ];
  const midYs = [];
  for (const [step, el] of pairs) {
    if (!step || !el) continue;
    const r = el.getBoundingClientRect();
    const mid = (r.top + scroll) + r.height / 2;
    const top = mid - (pRect.top + scroll) - half;
    step.style.top = Math.max(0, top) + 'px';
    midYs.push(mid - (pRect.top + scroll));
  }
  const line = document.getElementById('booking-step-line');
  if (line && midYs.length >= 2) {
    line.style.top    = midYs[0] + 'px';
    line.style.height = (midYs[midYs.length - 1] - midYs[0]) + 'px';
  }
}

let bookingMode = 'demo';
function setBookingMode(mode) {
  bookingMode = mode;
  document.getElementById('bmode-demo').classList.toggle('active', mode === 'demo');
  document.getElementById('bmode-prod').classList.toggle('active', mode === 'prod');
}

let massMode = 'demo';
function setMassMode(mode) {
  massMode = mode;
  document.getElementById('mmode-demo').classList.toggle('active', mode === 'demo');
  document.getElementById('mmode-prod').classList.toggle('active', mode === 'prod');
}

async function runBookingUpdate() {
  const contact = document.getElementById('booking-contact').value.trim();
  const title   = document.getElementById('booking-title').value.trim();
  if (!contact) { alert('Please fill in the Contact / Booker field.'); return; }
  if (!title)   { alert('Please fill in the Title field.'); return; }

  const btn = document.getElementById('booking-run-btn');
  btn.disabled = true;
  btn.textContent = 'Running…';

  const prog = document.getElementById('booking-progress');
  prog.style.display = 'block';
  prog.innerHTML = '';

  const booking = document.getElementById('booking-paste-area').value.trim();

  let job_id;
  try {
    const res = await fetch('/booking-plan-update', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ contact, title, booking, mode: bookingMode }),
    });
    ({ job_id } = await res.json());
  } catch (err) {
    bookingAppendLine('ERROR: ' + err.message, 'line-err');
    btn.disabled = false;
    btn.textContent = 'Update Sales Plan ▶';
    return;
  }

  let _bpJobDone = false;
  const src = new EventSource('/booking-plan-stream/' + job_id);
  src.onmessage = e => {
    const line = e.data;
    if (line === '__PING__') { return; }
    if (line === '__DONE__') {
      _bpJobDone = true; src.close(); return;
    }
    if (line === '__SUCCESS__') {
      _bpJobDone = true; src.close();
      bookingAppendLine('✓ Booking plan update complete!', 'line-ok');
      btn.disabled = false;
      btn.textContent = 'Update Sales Plan ▶';
      document.getElementById('booking-reset-btn').style.display = 'block';
      // Fetch differences and render unified panel
      fetch('/job-status/' + job_id).then(r => r.json()).then(d => {
        const micaList  = (d.already_booked || []).sort((a,b) => (parseInt(b.screens)||0)-(parseInt(a.screens)||0));
        const sheetList = d.missed || [];
        if (!micaList.length && !sheetList.length) return;
        // Section 1: In Mica, not on sheet
        const micaTbody = document.getElementById('diff-mica-tbody');
        micaTbody.innerHTML = '';
        if (micaList.length) {
          micaList.forEach(v => {
            const tr = document.createElement('tr');
            tr.innerHTML = '<td>' + (v.venue||'') + '</td><td>' + (v.city||'') + '</td><td>' + (v.state||'') + '</td><td class="scr-col">' + (v.screens||'—') + '</td>';
            micaTbody.appendChild(tr);
          });
          document.getElementById('diff-mica-count').textContent = micaList.length + ' venue' + (micaList.length===1?'':'s');
        } else {
          micaTbody.innerHTML = '<tr><td colspan="4" class="diff-empty">None — all Mica bookings are on the sheet</td></tr>';
          document.getElementById('diff-mica-count').textContent = '';
        }
        // Section 2: On sheet, not in Mica
        const sheetTbody = document.getElementById('diff-sheet-tbody');
        sheetTbody.innerHTML = '';
        if (sheetList.length) {
          sheetList.forEach(v => {
            const tr = document.createElement('tr');
            const closest = v.bestMatch ? '<span style="color:#666">' + v.bestMatch + '</span>' : '<span style="color:#555">—</span>';
            tr.innerHTML = '<td>' + (v.venue||v) + '</td><td>' + closest + '</td>';
            sheetTbody.appendChild(tr);
          });
          document.getElementById('diff-sheet-count').textContent = sheetList.length + ' venue' + (sheetList.length===1?'':'s');
        } else {
          sheetTbody.innerHTML = '<tr><td colspan="2" class="diff-empty">None — all sheet entries matched</td></tr>';
          document.getElementById('diff-sheet-count').textContent = '';
        }
        document.getElementById('diff-panel').classList.add('visible');
      }).catch(() => {});
      return;
    }
    if (line.startsWith('__ERROR__')) {
      _bpJobDone = true; src.close();
      bookingAppendLine('ERROR: ' + line.replace('__ERROR__', '').trim(), 'line-err');
      btn.disabled = false;
      btn.textContent = 'Update Sales Plan ▶';
      document.getElementById('booking-reset-btn').style.display = 'block';
      return;
    }
    bookingAppendLine(line);
  };
  src.onerror = async () => {
    if (_bpJobDone) return;
    src.close();
    bookingAppendLine('--- Connection dropped — polling for result…', 'line-warn');
    for (let i = 0; i < 120; i++) {
      await new Promise(r => setTimeout(r, 5000));
      if (_bpJobDone) return;
      try {
        const r = await fetch('/job-status/' + job_id);
        const d = await r.json();
        if (d.status === 'success') {
          _bpJobDone = true;
          bookingAppendLine('✓ Booking plan update complete!', 'line-ok');
          btn.disabled = false; btn.textContent = 'Update Sales Plan ▶';
          document.getElementById('booking-reset-btn').style.display = 'block';
          return;
        }
        if (d.status && d.status.startsWith('error:')) {
          _bpJobDone = true;
          bookingAppendLine('ERROR: ' + d.status.replace('error:','').trim(), 'line-err');
          btn.disabled = false; btn.textContent = 'Update Sales Plan ▶';
          document.getElementById('booking-reset-btn').style.display = 'block';
          return;
        }
      } catch(_) {}
    }
    bookingAppendLine('Connection lost.', 'line-err');
    btn.disabled = false; btn.textContent = 'Update Sales Plan ▶';
  };
}

function bookingAppendLine(text, cls) {
  const prog = document.getElementById('booking-progress');
  const div = document.createElement('div');
  if      (cls)                        div.className = cls;
  else if (/error|failed/i.test(text)) div.className = 'line-err';
  else if (/warning/i.test(text))      div.className = 'line-warn';
  else if (/complete|✓/i.test(text))   div.className = 'line-ok';
  div.textContent = text;
  prog.appendChild(div);
  prog.scrollTop = prog.scrollHeight;
}

function resetBookingUI() {
  document.getElementById('booking-drop-icon').textContent  = '📄';
  document.getElementById('booking-drop-title').textContent = 'Drop your booking CSV, Excel, PDF, or screenshot here';
  document.getElementById('booking-drop-sub').textContent   = 'or click to browse';
  document.getElementById('booking-paste-area').value = '';
  document.getElementById('booking-contact').value = '';
  document.getElementById('booking-title').value = '';
  const prog = document.getElementById('booking-progress');
  prog.style.display = 'none';
  prog.innerHTML = '';
  document.getElementById('booking-reset-btn').style.display = 'none';
  const btn = document.getElementById('booking-run-btn');
  btn.disabled = false;
  btn.textContent = 'Update Sales Plan ▶';
  const diffPanel = document.getElementById('diff-panel');
  if (diffPanel) {
    diffPanel.classList.remove('visible');
    document.getElementById('diff-mica-tbody').innerHTML = '';
    document.getElementById('diff-sheet-tbody').innerHTML = '';
    document.getElementById('diff-mica-count').textContent = '';
    document.getElementById('diff-sheet-count').textContent = '';
  }
}

// ── Mass Booking tab ─────────────────────────────────────────────────────────
let lastMassBookingText = '';

// Drop zone
const massDropZone  = document.getElementById('mass-drop-zone');
const massFileInput = document.getElementById('mass-file-input');

massDropZone.addEventListener('dragover',  e => { e.preventDefault(); massDropZone.classList.add('dragover'); });
massDropZone.addEventListener('dragleave', ()  => massDropZone.classList.remove('dragover'));
massDropZone.addEventListener('drop', e => {
  e.preventDefault();
  massDropZone.classList.remove('dragover');
  const file = e.dataTransfer.files[0];
  if (file) handleMassFile(file);
});
massFileInput.addEventListener('change', () => {
  if (massFileInput.files[0]) handleMassFile(massFileInput.files[0]);
});

async function handleMassFile(file) {
  const ext = file.name.toLowerCase().split('.').pop();
  if (!['csv','xlsx','xls','pdf','png','jpg','jpeg','webp'].includes(ext)) { alert('Please select a .csv, .xlsx, .xls, .pdf, or image file.'); return; }
  document.getElementById('mass-drop-icon').textContent  = '⏳';
  document.getElementById('mass-drop-title').textContent = ['png','jpg','jpeg','webp'].includes(ext) ? 'Reading screenshot with Claude vision…' : 'Loading ' + file.name + '...';
  if (ext === 'csv') {
    const text = await file.text();
    lastMassBookingText = text;
    document.getElementById('mass-paste-area').value = text;
    document.getElementById('mass-drop-icon').textContent  = '✅';
    document.getElementById('mass-drop-title').textContent = file.name + ' loaded';
  } else {
    const fd = new FormData();
    fd.append('file', file);
    const res = await fetch('/booking-parse-file', {
      method: 'POST',
      headers: { 'X-Filename': file.name, 'Content-Length': file.size },
      body: file,
    });
    const result = await res.json();
    if (result.error) { alert('Error: ' + result.error); return; }
    lastMassBookingText = result.text;
    document.getElementById('mass-paste-area').value = result.text;
    document.getElementById('mass-drop-icon').textContent  = '✅';
    document.getElementById('mass-drop-title').textContent = file.name + ' loaded (' + result.rows + ' rows)';
  }
}

document.getElementById('mass-paste-area').addEventListener('input', () => {
  lastMassBookingText = document.getElementById('mass-paste-area').value.trim();
});

async function runMassBookingUpdate() {
  const booking = lastMassBookingText || document.getElementById('mass-paste-area').value.trim();
  const title   = document.getElementById('mass-title').value.trim();
  const circuit = document.getElementById('mass-circuit').value.trim();
  const dryRun  = document.getElementById('mass-dry-run').checked;
  if (!booking) { alert('Please paste or drop a booking sheet first.'); return; }

  const btn  = document.getElementById('mass-run-btn');
  btn.disabled = true;
  btn.textContent = 'Running...';

  const prog = document.getElementById('mass-progress');
  prog.style.display = 'block';
  prog.innerHTML = '';

  let job_id;
  try {
    const res = await fetch('/mass-booking-plan-update', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ title, booking, mode: massMode, circuit, dry_run: dryRun }),
    });
    ({ job_id } = await res.json());
  } catch (err) {
    massAppendLine('ERROR: ' + err.message, 'line-err');
    btn.disabled = false; btn.textContent = 'Run Mass Booking ▶';
    return;
  }

  let _massJobDone = false;
  const src = new EventSource('/booking-plan-stream/' + job_id);
  src.onmessage = e => {
    const line = e.data;
    if (line === '__PING__') { return; }
    if (line === '__DONE__') { _massJobDone = true; src.close(); return; }
    if (line === '__SUCCESS__') {
      _massJobDone = true; src.close();
      const isDry = document.getElementById('mass-dry-run').checked;
      massAppendLine(isDry ? '✓ Dry run complete — no changes made.' : '✓ Mass booking update complete!', 'line-ok');
      document.getElementById('mass-reset-btn').style.display = 'block';
      btn.disabled = false; btn.textContent = 'Run Mass Booking ▶';
      return;
    }
    if (line.startsWith('__ERROR__')) {
      _massJobDone = true; src.close();
      massAppendLine('ERROR: ' + line.replace('__ERROR__','').trim(), 'line-err');
      document.getElementById('mass-reset-btn').style.display = 'block';
      btn.disabled = false; btn.textContent = 'Run Mass Booking ▶';
      return;
    }
    massAppendLine(line);
  };
  src.onerror = async () => {
    if (_massJobDone) return;
    src.close();
    massAppendLine('--- Connection dropped — polling for result…', 'line-warn');
    for (let i = 0; i < 120; i++) {
      await new Promise(r => setTimeout(r, 5000));
      if (_massJobDone) return;
      try {
        const r = await fetch('/job-status/' + job_id);
        const d = await r.json();
        if (d.status === 'success') {
          _massJobDone = true;
          massAppendLine('✓ Mass booking update complete!', 'line-ok');
          document.getElementById('mass-reset-btn').style.display = 'block';
          btn.disabled = false; btn.textContent = 'Run Mass Booking ▶';
          return;
        }
        if (d.status && d.status.startsWith('error:')) {
          _massJobDone = true;
          massAppendLine('ERROR: ' + d.status.replace('error:','').trim(), 'line-err');
          document.getElementById('mass-reset-btn').style.display = 'block';
          btn.disabled = false; btn.textContent = 'Run Mass Booking ▶';
          return;
        }
      } catch(_) {}
    }
    massAppendLine('Connection lost.', 'line-err');
    btn.disabled = false; btn.textContent = 'Run Mass Booking ▶';
  };
}

function massAppendLine(text, cls) {
  const prog = document.getElementById('mass-progress');
  const div  = document.createElement('div');
  if      (cls)                        div.className = cls;
  else if (/error|failed/i.test(text)) div.className = 'line-err';
  else if (/warning/i.test(text))      div.className = 'line-warn';
  else if (/complete|✓/i.test(text))   div.className = 'line-ok';
  div.textContent = text;
  prog.appendChild(div);
  prog.scrollTop = prog.scrollHeight;
}

function resetMassUI() {
  lastMassBookingText = '';
  document.getElementById('mass-drop-icon').textContent  = '📄';
  document.getElementById('mass-drop-title').textContent = 'Drop your booking CSV, Excel, PDF, or screenshot here';
  document.getElementById('mass-drop-sub').textContent   = 'or click to browse';
  document.getElementById('mass-paste-area').value = '';
  document.getElementById('mass-title').value = '';
  document.getElementById('mass-circuit').value = '';
  const prog = document.getElementById('mass-progress');
  prog.style.display = 'none';
  prog.innerHTML = '';
  document.getElementById('mass-reset-btn').style.display = 'none';
  const btn = document.getElementById('mass-run-btn');
  btn.disabled = false;
  btn.textContent = 'Run Mass Booking ▶';
}

function alignSteps() {
  const panel  = document.getElementById('steps-panel');
  const pRect  = panel.getBoundingClientRect();
  const scroll = window.scrollY;
  const half   = 18; // half of 36px circle

  const pairs = [
    [document.getElementById('step1'), document.querySelector('.or-divider')],
    [document.getElementById('step2'), document.getElementById('mica-contact')],
    [document.getElementById('step3'), document.getElementById('action-btns')],
  ];

  const midYs = [];
  for (const [step, el] of pairs) {
    const r = el.getBoundingClientRect();
    const mid = (r.top + scroll) + r.height / 2;
    const top = mid - (pRect.top + scroll) - half;
    step.style.top = Math.max(0, top) + 'px';
    midYs.push(mid - (pRect.top + scroll));
  }

  // Connector line from first to last circle centre
  const line = document.getElementById('step-line');
  line.style.top    = midYs[0] + 'px';
  line.style.height = (midYs[midYs.length - 1] - midYs[0]) + 'px';
}
document.addEventListener('DOMContentLoaded', () => { setTimeout(alignSteps, 80); });
window.addEventListener('resize', () => { alignSteps(); alignBookingSteps(); });

function resetUI() {
  dropZone.className = '';
  document.getElementById('drop-icon').textContent  = '📄';
  document.getElementById('drop-title').textContent = 'Drop your booking CSV, Excel, PDF, or screenshot here';
  document.getElementById('drop-sub').textContent   = 'or click to browse';
  progressBox.style.display = 'none';
  progressBox.innerHTML = '';
  // Keep dashboard button visible as a subtle secondary link
  openBtn.classList.add('secondary');
  openBtn.textContent   = 'View Last Dashboard ↗';
  openBtn.style.display = 'block';
  resetBtn.style.display = 'none';
  fileInput.value = '';
  document.getElementById('paste-area').value = '';
  document.getElementById('mica-contact').value = '';
  document.getElementById('mica-progress').style.display = 'none';
  document.getElementById('mica-progress').innerHTML = '';
  lastBookingText = '';
  const btn = document.getElementById('run-paste-btn');
  btn.disabled = false;
  btn.textContent = 'Pull Comscore Report';
}
</script>
</body>
</html>"""
HTML = HTML.replace('__LOGO_IMG__', _LOGO_IMG)
HTML = HTML.replace('__LOGO_FOOTER__', _LOGO_FOOTER_IMG)


# ---------------------------------------------------------------------------
# HTTP handler
# ---------------------------------------------------------------------------
# Alias manager page
# ---------------------------------------------------------------------------

def _render_alias_manager(aliases: list) -> str:
    rows_html = ''
    for a in aliases:
        city_display = a.get('city') or ''
        rows_html += f"""
        <tr data-id="{a['id']}">
          <td>{a.get('booking_name','')}</td>
          <td>{city_display}</td>
          <td>{a.get('master_name','')}</td>
          <td>{a.get('chain','')}</td>
          <td><button onclick="deleteAlias({a['id']})" style="background:#c62828;color:#fff;border:none;padding:4px 10px;border-radius:4px;cursor:pointer;font-size:12px;">Delete</button></td>
        </tr>"""

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Venue Aliases — Angel Holdover Assistant</title>
<style>
  * {{ box-sizing: border-box; margin: 0; padding: 0; }}
  body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
          background: #111; color: #fff; padding: 32px; }}
  h2 {{ font-size: 20px; margin-bottom: 6px; color: #00bcd4; }}
  .subtitle {{ color: #888; font-size: 13px; margin-bottom: 28px; }}
  .back {{ color: #888; font-size: 13px; text-decoration: none; display: inline-block; margin-bottom: 20px; }}
  .back:hover {{ color: #fff; }}
  .add-form {{ background: #1e1e1e; border: 1px solid #333; border-radius: 8px;
               padding: 20px; margin-bottom: 28px; display: flex; gap: 12px; flex-wrap: wrap; align-items: flex-end; }}
  .field {{ display: flex; flex-direction: column; gap: 4px; }}
  label {{ font-size: 12px; color: #aaa; }}
  input {{ background: #2a2a2a; border: 1px solid #444; border-radius: 6px;
           color: #fff; padding: 8px 10px; font-size: 13px; width: 200px; }}
  input:focus {{ outline: none; border-color: #00bcd4; }}
  .btn {{ background: #00bcd4; color: #000; font-weight: 600; font-size: 13px;
          padding: 8px 18px; border-radius: 6px; border: none; cursor: pointer; }}
  .btn:hover {{ background: #00acc1; }}
  table {{ width: 100%; border-collapse: collapse; font-size: 13px; }}
  th {{ text-align: left; padding: 10px 12px; background: #1a1a1a;
        color: #00bcd4; font-size: 11px; text-transform: uppercase; letter-spacing: 0.05em; }}
  td {{ padding: 9px 12px; border-bottom: 1px solid #222; }}
  tr:hover td {{ background: #1a1a1a; }}
  .count {{ color: #888; font-size: 13px; margin-bottom: 12px; }}
</style>
</head>
<body>
<a class="back" href="/">← Back to tool</a>
<h2>Venue Aliases</h2>
<p class="subtitle">Map booking sheet theatre names to their master list names. City is optional — used when the same name appears in multiple cities.</p>

<div class="add-form">
  <div class="field">
    <label>Booking Name</label>
    <input id="f-booking" placeholder="e.g. Tinseltown USA" />
  </div>
  <div class="field">
    <label>City (optional)</label>
    <input id="f-city" placeholder="e.g. Jacksonville" style="width:140px;" />
  </div>
  <div class="field">
    <label>Master List Name</label>
    <input id="f-master" placeholder="e.g. Cinemark Tinseltown Jacksonville 20 + XD" style="width:300px;" />
  </div>
  <div class="field">
    <label>Chain (optional)</label>
    <input id="f-chain" placeholder="e.g. Cinemark" style="width:120px;" />
  </div>
  <button class="btn" onclick="addAlias()">Add Alias</button>
</div>

<p class="count">{len(aliases)} aliases</p>
<table>
  <thead>
    <tr>
      <th>Booking Name</th>
      <th>City</th>
      <th>Master List Name</th>
      <th>Chain</th>
      <th></th>
    </tr>
  </thead>
  <tbody id="alias-tbody">{rows_html}</tbody>
</table>

<script>
async function addAlias() {{
  const booking = document.getElementById('f-booking').value.trim();
  const master  = document.getElementById('f-master').value.trim();
  if (!booking || !master) {{ alert('Booking name and master name are required.'); return; }}
  const resp = await fetch('/aliases/add', {{
    method: 'POST',
    headers: {{'Content-Type': 'application/json'}},
    body: JSON.stringify({{
      booking_name: booking,
      city:         document.getElementById('f-city').value.trim(),
      master_name:  master,
      chain:        document.getElementById('f-chain').value.trim(),
    }})
  }});
  if (resp.ok) location.reload();
}}

async function deleteAlias(id) {{
  if (!confirm('Delete this alias?')) return;
  const resp = await fetch('/aliases/delete', {{
    method: 'POST',
    headers: {{'Content-Type': 'application/json'}},
    body: JSON.stringify({{id}})
  }});
  if (resp.ok) document.querySelector(`tr[data-id="${{id}}"]`).remove();
}}
</script>
</body>
</html>"""


# ---------------------------------------------------------------------------

class Handler(http.server.BaseHTTPRequestHandler):

    def log_message(self, format, *args):
        pass  # Suppress noisy access logs

    def do_GET(self):
        # ── Auth routes ───────────────────────────────────────────────────
        if self.path == '/auth/login':
            if not _AUTH_ENABLED:
                self.send_response(302); self.send_header('Location', '/'); self.end_headers(); return
            host = self.headers.get('Host', f'localhost:{PORT}')
            login_url = _auth.get_login_url(host)
            self._send_html(_auth.render_login_page(login_url))
            return

        elif self.path.startswith('/auth/callback'):
            host = self.headers.get('Host', f'localhost:{PORT}')
            qs   = urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
            code = qs.get('code', [''])[0]
            if not code:
                self._send_html(_auth.render_login_page(_auth.get_login_url(host), error='Login cancelled.'))
                return
            user = _auth.handle_callback(host, code)
            if not user:
                self._send_html(_auth.render_login_page(_auth.get_login_url(host), error='Login failed — make sure you use your @angel.com account.'))
                return
            cookie = _auth.create_session_cookie(user['id'], user['email'])
            self.send_response(302)
            self.send_header('Set-Cookie', f'session={cookie}; Path=/; HttpOnly; SameSite=Lax; Max-Age={30*86400}')
            self.send_header('Location', '/')
            self.end_headers()
            return

        elif self.path == '/auth/logout':
            self.send_response(302)
            self.send_header('Set-Cookie', 'session=; Path=/; Max-Age=0')
            self.send_header('Location', '/auth/login' if _AUTH_ENABLED else '/')
            self.end_headers()
            return

        elif self.path == '/auth/profile':
            if _AUTH_ENABLED:
                user = _auth.require_auth(self)
                if not user: return
            else:
                user = {'id': _LOCAL_USER_ID, 'email': 'local', 'name': 'Local User'}
            creds = _db.get_credentials(user['id']) if _db else {}
            self._send_html(_auth.render_profile_page(user, creds))
            return

        elif self.path == '/aliases':
            if _AUTH_ENABLED:
                user = _auth.require_auth(self)
                if not user: return
            aliases = _db.get_all_aliases() if _db else []
            self._send_html(_render_alias_manager(aliases))
            return

        elif self.path == '/aliases/data':
            aliases = _db.get_all_aliases() if _db else []
            self._json({'aliases': aliases})
            return

        elif self.path == '/auth/me':
            if not _AUTH_ENABLED:
                # Local mode — always "logged in" as the single local user
                creds = _db.get_credentials(_LOCAL_USER_ID) if _db else {}
                self._json({'auth': True, 'local': True, 'email': '', 'name': '',
                            'has_comscore': bool(creds.get('comscore_user')),
                            'has_mica': bool(creds.get('mica_user'))})
                return
            user = _auth.get_session_user(self)
            if user:
                self._json({'auth': True, 'email': user['email'], 'name': user.get('name',''),
                            'has_comscore': bool(user.get('comscore_user')),
                            'has_mica': bool(user.get('mica_user'))})
            else:
                self._json({'auth': False})
            return

        # ── Main app ──────────────────────────────────────────────────────
        if self.path in ('/', '/index.html'):
            # Redirect to login if auth is enabled and user is not logged in
            if _AUTH_ENABLED and not _auth.get_session_user(self):
                self.send_response(302); self.send_header('Location', '/auth/login'); self.end_headers(); return
            self._send_html(HTML)

        elif self.path.startswith('/stream/'):
            job_id = self.path[len('/stream/'):]
            self._sse_stream(job_id)

        elif self.path.startswith('/mica-stream/'):
            job_id = self.path[len('/mica-stream/'):]
            self._sse_stream(job_id)

        elif self.path.startswith('/booking-plan-stream/'):
            job_id = self.path[len('/booking-plan-stream/'):]
            self._sse_stream(job_id)

        elif self.path.startswith('/job-status/'):
            job_id = self.path[len('/job-status/'):]
            result = _job_results.get(job_id, 'pending')
            body = json.dumps({'status': result, 'unbooked': _job_unbooked.get(job_id, []), 'already_booked': _job_already_booked.get(job_id, []), 'missed': _job_missed.get(job_id, [])}).encode()
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Content-Length', str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        elif self.path == '/admin/bp-pool-size':
            # POST {"size": 1} or {"size": 2} — lets admin scale daemon pool on demand
            length = int(self.headers.get('Content-Length', 0))
            raw    = self.rfile.read(length)
            try:
                payload = json.loads(raw)
                new_size = int(payload.get('size', 1))
            except Exception:
                self.send_response(400); self.end_headers(); return
            new_size = max(1, min(2, new_size))
            global _BP_POOL_SIZE
            with _bp_pool_lock:
                old_size = _BP_POOL_SIZE
                if new_size > old_size:
                    for _si in range(old_size, new_size):
                        _bp_slots.put(_si)
                elif new_size < old_size:
                    # drain the extra slot (if idle)
                    for _si in range(new_size, old_size):
                        try:
                            _bp_slots.get_nowait()
                        except queue.Empty:
                            pass
                _BP_POOL_SIZE = new_size
            body = json.dumps({'pool_size': _BP_POOL_SIZE}).encode()
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Content-Length', str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        elif self.path == '/bg-video':
            self._serve_video(VIDEO_PATH)

        elif self.path == '/dashboard':
            p = OUTPUT_DIR / 'flash_gross_dashboard.html'
            if p.exists():
                data = p.read_bytes()
                self.send_response(200)
                self.send_header('Content-Type', 'text/html; charset=utf-8')
                self.send_header('Content-Length', str(len(data)))
                self.end_headers()
                self.wfile.write(data)
            else:
                self._send_html('<h2 style="font-family:sans-serif;padding:40px">Dashboard not found — run the tool first.</h2>')
        else:
            self.send_response(404)
            self.end_headers()

    def do_POST(self):
        # ── Auth: save profile credentials ────────────────────────────────
        if self.path == '/auth/profile':
            if _AUTH_ENABLED:
                user = _auth.require_auth(self)
                if not user: return
            else:
                user = {'id': _LOCAL_USER_ID, 'email': 'local', 'name': 'Local User'}
            length = int(self.headers.get('Content-Length', 0))
            body   = self.rfile.read(length).decode('utf-8')
            import urllib.parse as _up
            fields = _up.parse_qs(body)
            def _f(k): return fields.get(k, [''])[0]
            if _db:
                _db.save_credentials(
                    user['id'],
                    comscore_user=_f('comscore_user'),
                    comscore_pass=_f('comscore_pass'),
                    mica_user=_f('mica_user'),
                    mica_pass=_f('mica_pass'),
                )
            creds = _db.get_credentials(user['id']) if _db else {}
            self._send_html(_auth.render_profile_page(user, creds, saved=True))
            return

        if self.path == '/aliases/add':
            if _AUTH_ENABLED:
                user = _auth.require_auth(self)
                if not user: return
            length = int(self.headers.get('Content-Length', 0))
            try:
                data = json.loads(self.rfile.read(length))
            except Exception:
                self.send_response(400); self.end_headers(); return
            if _db:
                _db.upsert_alias(
                    data.get('booking_name', ''),
                    data.get('master_name', ''),
                    city=data.get('city', ''),
                    chain=data.get('chain', ''),
                )
            self._json({'ok': True})
            return

        if self.path == '/aliases/delete':
            if _AUTH_ENABLED:
                user = _auth.require_auth(self)
                if not user: return
            length = int(self.headers.get('Content-Length', 0))
            try:
                data = json.loads(self.rfile.read(length))
            except Exception:
                self.send_response(400); self.end_headers(); return
            if _db:
                _db.delete_alias(int(data.get('id', 0)))
            self._json({'ok': True})
            return

        if self.path == '/upload':
            filename = self.headers.get('X-Filename', 'upload.csv')
            length   = int(self.headers.get('Content-Length', 0))
            content  = self.rfile.read(length)

            csv_path = BASE_DIR / filename
            csv_path.write_bytes(content)

            # Get user credentials from DB
            user_creds = {}
            if _db:
                if _AUTH_ENABLED and _auth:
                    user = _auth.get_session_user(self)
                    if user:
                        user_creds = _db.get_credentials(user['id'])
                else:
                    user_creds = _db.get_credentials(_LOCAL_USER_ID)  # local mode: single user

            print(f'[upload] file={filename} creds=comscore:{bool(user_creds.get("comscore_user"))} mica:{bool(user_creds.get("mica_user"))}', flush=True)
            job_id = str(int(time.time() * 1000))
            _job_queues[job_id] = queue.Queue()
            threading.Thread(
                target=_run_tool,
                args=(csv_path, job_id, user_creds),
                daemon=True,
            ).start()

            body = json.dumps({'job_id': job_id}).encode()
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Content-Length', str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        elif self.path == '/mica-update':
            length  = int(self.headers.get('Content-Length', 0))
            raw     = self.rfile.read(length)
            try:
                payload = json.loads(raw)
            except json.JSONDecodeError:
                self.send_response(400); self.end_headers(); return

            contact = payload.get('contact', '').strip()
            booking = payload.get('booking', '').strip()
            mode    = payload.get('mode', 'demo').strip()
            if mode not in ('demo', 'prod'):
                mode = 'demo'

            # Write booking to a temp CSV
            booking_path = BASE_DIR / 'mica_booking.csv'
            booking_path.write_text(booking, encoding='utf-8')

            # Get user credentials from DB
            user_creds = {}
            if _db:
                if _AUTH_ENABLED and _auth:
                    user = _auth.get_session_user(self)
                    if user:
                        user_creds = _db.get_credentials(user['id'])
                else:
                    user_creds = _db.get_credentials(_LOCAL_USER_ID)  # local mode: single user

            print(f'[mica-update] contact={contact!r} creds=mica:{bool(user_creds.get("mica_user"))}', flush=True)
            job_id = 'mica_' + str(int(time.time() * 1000))
            _job_queues[job_id] = queue.Queue()
            threading.Thread(
                target=_run_mica,
                args=(booking_path, contact, job_id, mode, user_creds),
                daemon=True,
            ).start()

            body = json.dumps({'job_id': job_id}).encode()
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Content-Length', str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        elif self.path == '/booking-plan-update':
            length  = int(self.headers.get('Content-Length', 0))
            raw     = self.rfile.read(length)
            try:
                payload = json.loads(raw)
            except json.JSONDecodeError:
                self.send_response(400); self.end_headers(); return

            title   = payload.get('title',   '').strip()
            contact = payload.get('contact', '').strip()
            booking = payload.get('booking', '').strip()
            mode    = payload.get('mode', 'demo').strip()
            if mode not in ('demo', 'prod'):
                mode = 'demo'

            # Write booking text to a temp file for the script
            booking_path = BASE_DIR / 'booking_plan_input.txt'
            booking_path.write_text(booking, encoding='utf-8')

            # Get user credentials from DB
            user_creds = {}
            if _db:
                if _AUTH_ENABLED and _auth:
                    user = _auth.get_session_user(self)
                    if user:
                        user_creds = _db.get_credentials(user['id'])
                else:
                    user_creds = _db.get_credentials(_LOCAL_USER_ID)  # local mode: single user

            job_id = 'bp_' + str(int(time.time() * 1000))
            _job_queues[job_id] = queue.Queue()
            threading.Thread(
                target=_run_booking_plan,
                args=(title, contact, booking_path, job_id, mode, user_creds),
                daemon=True,
            ).start()

            body = json.dumps({'job_id': job_id}).encode()
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Content-Length', str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        elif self.path == '/mass-booking-plan-update':
            length  = int(self.headers.get('Content-Length', 0))
            raw     = self.rfile.read(length)
            try:
                payload = json.loads(raw)
            except json.JSONDecodeError:
                self.send_response(400); self.end_headers(); return

            title    = payload.get('title',   '').strip()
            booking  = payload.get('booking', '').strip()
            mode     = payload.get('mode', 'demo').strip()
            circuit  = payload.get('circuit', '').strip()
            dry_run  = bool(payload.get('dry_run', False))
            if mode not in ('demo', 'prod'):
                mode = 'demo'

            booking_path = BASE_DIR / 'mass_booking_input.txt'
            booking_path.write_text(booking, encoding='utf-8')

            job_id = 'bp_' + str(int(time.time() * 1000))
            _job_queues[job_id] = queue.Queue()
            threading.Thread(
                target=_run_mass_booking_plan,
                args=(title, booking_path, job_id, mode, circuit, dry_run),
                daemon=True,
            ).start()

            body = json.dumps({'job_id': job_id}).encode()
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Content-Length', str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        elif self.path == '/booking-parse-file':
            import tempfile
            filename = self.headers.get('X-Filename', 'upload.pdf')
            length   = int(self.headers.get('Content-Length', 0))
            content  = self.rfile.read(length)
            suffix   = Path(filename).suffix.lower()

            tmp_path = None
            try:
                with tempfile.NamedTemporaryFile(suffix=suffix, delete=False) as tmp:
                    tmp.write(content)
                    tmp_path = Path(tmp.name)

                if suffix == '.pdf':
                    q = queue.Queue()
                    result = _pdf_to_csv(tmp_path, q)
                    if result is None:
                        msgs = []
                        while not q.empty():
                            try: msgs.append(q.get_nowait())
                            except: break
                        err = next((m for m in msgs if m.startswith('ERROR:')), 'Failed to extract PDF')
                        resp = json.dumps({'error': err}).encode()
                    else:
                        _, csv_text = result
                        rows = max(0, len([l for l in csv_text.splitlines() if l.strip()]) - 1)
                        resp = json.dumps({'text': csv_text, 'rows': rows}).encode()
                elif suffix == '.csv':
                    csv_text = content.decode('utf-8', errors='replace')
                    rows = max(0, len([l for l in csv_text.splitlines() if l.strip()]) - 1)
                    resp = json.dumps({'text': csv_text, 'rows': rows}).encode()
                elif suffix in ('.xlsx', '.xls'):
                    csv_text = _excel_to_csv(tmp_path)
                    rows = max(0, len([l for l in csv_text.splitlines() if l.strip()]) - 1)
                    resp = json.dumps({'text': csv_text, 'rows': rows}).encode()
                elif suffix in ('.png', '.jpg', '.jpeg', '.webp'):
                    q = queue.Queue()
                    result = _image_to_csv(tmp_path, q)
                    if result is None:
                        msgs = []
                        while not q.empty():
                            try: msgs.append(q.get_nowait())
                            except: break
                        err = next((m for m in msgs if 'error' in m.lower()), 'Failed to extract screenshot')
                        resp = json.dumps({'error': err}).encode()
                    else:
                        _, csv_text = result
                        rows = max(0, len([l for l in csv_text.splitlines() if l.strip()]) - 1)
                        resp = json.dumps({'text': csv_text, 'rows': rows}).encode()
                else:
                    resp = json.dumps({'error': f'Unsupported file type: {suffix}'}).encode()
            except Exception as ex:
                resp = json.dumps({'error': str(ex)}).encode()
            finally:
                if tmp_path and tmp_path.exists():
                    try: tmp_path.unlink()
                    except: pass

            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Content-Length', str(len(resp)))
            self.end_headers()
            self.wfile.write(resp)

        else:
            self.send_response(404)
            self.end_headers()

    # ── helpers ──────────────────────────────────────────────────────────

    def _json(self, obj: dict):
        data = json.dumps(obj).encode('utf-8')
        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _send_html(self, html: str):
        data = html.encode('utf-8')
        self.send_response(200)
        self.send_header('Content-Type', 'text/html; charset=utf-8')
        self.send_header('Content-Length', str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _serve_video(self, path: Path):
        if not path.exists():
            self.send_response(404); self.end_headers(); return
        file_size = path.stat().st_size
        range_header = self.headers.get('Range')
        if range_header:
            parts = range_header.replace('bytes=', '').split('-')
            start = int(parts[0]) if parts[0] else 0
            end   = int(parts[1]) if len(parts) > 1 and parts[1] else file_size - 1
            end   = min(end, file_size - 1)
            length = end - start + 1
            self.send_response(206)
            self.send_header('Content-Type', 'video/mp4')
            self.send_header('Content-Range', f'bytes {start}-{end}/{file_size}')
            self.send_header('Content-Length', str(length))
            self.send_header('Accept-Ranges', 'bytes')
            self.end_headers()
            with open(path, 'rb') as f:
                f.seek(start)
                self.wfile.write(f.read(length))
        else:
            self.send_response(200)
            self.send_header('Content-Type', 'video/mp4')
            self.send_header('Content-Length', str(file_size))
            self.send_header('Accept-Ranges', 'bytes')
            self.end_headers()
            with open(path, 'rb') as f:
                self.wfile.write(f.read())

    def _sse_stream(self, job_id: str):
        q = _job_queues.get(job_id)
        if not q:
            self.send_response(404)
            self.end_headers()
            return

        self.send_response(200)
        self.send_header('Content-Type',  'text/event-stream')
        self.send_header('Cache-Control', 'no-cache')
        self.send_header('Connection',    'keep-alive')
        self.end_headers()

        try:
            while True:
                try:
                    msg = q.get(timeout=10)
                except queue.Empty:
                    # Send a real data heartbeat — SSE comments may be stripped by proxies
                    self.wfile.write(b'data: __PING__\n\n')
                    self.wfile.flush()
                    continue

                if msg is None:
                    self.wfile.write(b'data: __DONE__\n\n')
                    self.wfile.flush()
                    break

                safe = msg.replace('\n', ' ').replace('\r', '')
                self.wfile.write(f'data: {safe}\n\n'.encode('utf-8'))
                self.wfile.flush()

        except (BrokenPipeError, ConnectionResetError):
            pass


# ---------------------------------------------------------------------------
# Image → CSV via AI Labs (Claude Sonnet vision)
# ---------------------------------------------------------------------------

def _image_to_csv(image_path: Path, q: queue.Queue):
    """
    Use AI Labs / anthropic/claude-4.5-sonnet vision to extract booking data.
    Passes image as a base64 data URI — no separate upload step needed.
    Returns (csv_path, csv_text) on success, or None on failure.
    """
    if not AI_LABS_API_KEY:
        q.put('ERROR: AI_LABS_API_KEY not set in .env — cannot process image')
        return None

    import urllib.request, urllib.error, ssl, time

    # Build an SSL context that works on macOS (Python from python.org ships
    # without system certs; certifi provides its own trusted CA bundle).
    try:
        import certifi as _certifi
        _ssl_ctx = ssl.create_default_context(cafile=_certifi.where())
    except ImportError:
        _ssl_ctx = ssl.create_default_context()

    AI_LABS_BASE = 'https://ai-labs.angel-tools.io/api/v1'
    headers_auth = {
        'Authorization': f'Bearer {AI_LABS_API_KEY}',
        'Content-Type': 'application/json',
    }

    ext = image_path.suffix.lower()
    media_map = {'.png': 'image/png', '.jpg': 'image/jpeg', '.jpeg': 'image/jpeg', '.webp': 'image/webp'}
    media_type = media_map.get(ext, 'image/png')
    img_b64 = base64.standard_b64encode(image_path.read_bytes()).decode()
    data_uri = f'data:{media_type};base64,{img_b64}'

    prompt = (
        "Extract all booking rows from this booking report image.\n\n"
        "Output a TAB-separated file with exactly 4 columns: Theatre\tCity\tAction\tPhrase\n\n"
        "Rules:\n"
        "- Theatre: the venue/theatre name as shown\n"
        "- City: the city the theatre is in (leave blank if not shown)\n"
        "- Action: \"Final\" if the status/change type contains \"Final\"; "
        "\"Hold\" if it contains \"Hold\" or \"Holdover\"; skip all other rows\n"
        "- Phrase: screening type — use \"split\" if \"Split screen\" appears; "
        "\"mats\" if matinee/mats; \"prime\" if prime; leave blank otherwise\n\n"
        "IMPORTANT — some booking sheets list ALL theatres in a circuit with screen-count columns "
        "(labeled 2D, 3D, COMBO, Screens, or similar). In these formats:\n"
        "- A number (e.g. 1, 2, 3) in a screen-count column means that theatre IS playing the film — include it\n"
        "- The action may appear as a single letter: F or f = Final, H or h = Hold (circled or not)\n"
        "- If no letter is present but there is a number, default Action to Hold\n"
        "- Theatres with NO number in any screen-count column are NOT playing — skip them entirely\n\n"
        "Use TAB characters (not commas) to separate columns. "
        "Output ONLY the raw tab-separated lines. First line must be: Theatre\tCity\tAction\tPhrase"
    )

    q.put('Reading screenshot with Claude Sonnet vision ...')

    # ── Submit prediction ──────────────────────────────────────────────────
    try:
        pred_req = urllib.request.Request(
            f'{AI_LABS_BASE}/predictions',
            data=json.dumps({
                'model': 'anthropic/claude-4.5-sonnet',
                'input': {
                    'prompt': prompt,
                    'image': data_uri,
                    'max_tokens': 2000,
                },
            }).encode(),
            headers=headers_auth,
            method='POST',
        )
        with urllib.request.urlopen(pred_req, timeout=30, context=_ssl_ctx) as r:
            pred_data = json.loads(r.read())
    except Exception as e:
        q.put(f'ERROR: AI Labs prediction request failed — {e}')
        return None

    pred_id = pred_data.get('id')
    if not pred_id:
        q.put(f'ERROR: AI Labs prediction missing id — {pred_data}')
        return None

    # ── Poll for result ────────────────────────────────────────────────────
    for _ in range(30):
        time.sleep(2)
        try:
            poll_req = urllib.request.Request(
                f'{AI_LABS_BASE}/predictions/{pred_id}',
                headers=headers_auth,
                method='GET',
            )
            with urllib.request.urlopen(poll_req, timeout=15, context=_ssl_ctx) as r:
                result = json.loads(r.read())
        except Exception as e:
            q.put(f'ERROR: AI Labs poll failed — {e}')
            return None

        status = result.get('status', '')
        if status == 'completed':
            break
        # Non-200 errors surface as HTTP exceptions; any unexpected status → fail
        if status not in ('pending', ''):
            q.put(f'ERROR: AI Labs prediction unexpected status: {status} — {result}')
            return None
    else:
        q.put('ERROR: AI Labs prediction timed out after 60s')
        return None

    output = result.get('output') or ''
    if isinstance(output, list):
        output = ''.join(output)
    csv_text = output.strip()

    # Strip markdown code fences if model wrapped the output
    if csv_text.startswith('```'):
        csv_text = '\n'.join(ln for ln in csv_text.splitlines() if not ln.startswith('```')).strip()

    row_count = max(0, csv_text.count('\n'))
    q.put(f'Extracted {row_count} booking rows from screenshot')

    csv_path = image_path.with_suffix('.csv')
    csv_path.write_text(csv_text, encoding='utf-8')
    q.put(f'Saved as: {csv_path.name}')

    return csv_path, csv_text


# ---------------------------------------------------------------------------
# Excel → CSV
# ---------------------------------------------------------------------------

def _excel_to_csv(xlsx_path: Path) -> str:
    """Convert first sheet of an Excel workbook to CSV text (no index/header added)."""
    import pandas as pd
    df = pd.read_excel(xlsx_path, header=None, dtype=str).fillna('')
    return df.to_csv(index=False, header=False)


# ---------------------------------------------------------------------------
# PDF → CSV via pdfplumber
# ---------------------------------------------------------------------------

def _pdf_render_to_image(pdf_path: Path, q: queue.Queue) -> Path | None:
    """Render the first page of a PDF to a PNG using pymupdf (for scanned PDFs)."""
    try:
        import fitz  # pymupdf
    except ImportError:
        q.put('ERROR: pymupdf not installed — run: pip install pymupdf')
        return None
    try:
        doc  = fitz.open(str(pdf_path))
        page = doc[0]
        pix  = page.get_pixmap(dpi=150)
        img_path = pdf_path.with_suffix('.png')
        pix.save(str(img_path))
        doc.close()
        return img_path
    except Exception as e:
        q.put(f'ERROR: Could not render PDF to image — {e}')
        return None


def _pdf_to_csv(pdf_path: Path, q: queue.Queue):
    """
    Extract booking table from a PDF and return (csv_path, csv_text).
    For digital PDFs: uses pdfplumber text extraction.
    For scanned/image PDFs: falls back to rendering page as image → vision API.
    """
    try:
        import pdfplumber
    except ImportError:
        q.put('ERROR: pdfplumber not installed — run: python -m pip install pdfplumber')
        return None

    q.put('Reading PDF ...')

    rows = []
    try:
        with pdfplumber.open(pdf_path) as pdf:
            for page in pdf.pages:
                tables = page.extract_tables()
                for table in tables:
                    rows.extend(table)
    except Exception as e:
        q.put(f'ERROR: Failed to read PDF — {e}')
        return None

    if not rows:
        q.put('No text found in PDF — looks like a scanned document. Trying image analysis ...')
        img_path = _pdf_render_to_image(pdf_path, q)
        if img_path:
            return _image_to_csv(img_path, q)
        return None

    # Find the header row — look for a row containing "Theatre" (or "Theater") and some action column
    header_idx = None
    header_row = None
    theatre_col = None
    action_col  = None

    for i, row in enumerate(rows):
        cells = [str(c).strip() if c else '' for c in row]
        cells_lower = [c.lower() for c in cells]
        has_theatre = any('theatre' in c or 'theater' in c or 'location' in c for c in cells_lower)
        has_action  = any('change' in c or 'action' in c or 'status' in c or 'type' in c for c in cells_lower)
        if has_theatre and has_action:
            header_idx = i
            header_row = cells
            for j, c in enumerate(cells_lower):
                if 'theatre' in c or 'theater' in c or 'location' in c:
                    theatre_col = j
                if 'change' in c or 'status' in c or 'type' in c:
                    action_col = j
            break

    if header_idx is None or theatre_col is None or action_col is None:
        q.put('WARNING: Could not auto-detect columns — trying fallback (col 0 = Theatre, col 1 = Action)')
        theatre_col = 0
        action_col  = 1
        header_idx  = 0

    # Parse data rows
    csv_lines = ['Theatre,Action,Phrase']
    data_start = header_idx + 1

    for row in rows[data_start:]:
        cells = [str(c).strip() if c else '' for c in row]
        if len(cells) <= max(theatre_col, action_col):
            continue

        theatre    = cells[theatre_col].replace(',', ' ').strip()
        change_raw = cells[action_col].strip()
        change_lwr = change_raw.lower()

        if not theatre or not change_raw:
            continue

        # Determine Action
        if 'final' in change_lwr:
            action = 'Final'
        elif 'holdover' in change_lwr or 'hold' in change_lwr:
            action = 'Hold'
        else:
            continue  # skip rows that are not Final or Hold

        # Determine Phrase
        phrase = ''
        if 'split' in change_lwr:
            phrase = 'split'
        elif 'mat' in change_lwr:
            phrase = 'mats'
        elif 'prime' in change_lwr:
            phrase = 'prime'

        csv_lines.append(f'{theatre},{action},{phrase}')

    if len(csv_lines) <= 1:
        q.put('No Final/Hold rows found via text extraction — trying image analysis ...')
        img_path = _pdf_render_to_image(pdf_path, q)
        if img_path:
            return _image_to_csv(img_path, q)
        return None

    csv_text = '\n'.join(csv_lines)
    row_count = len(csv_lines) - 1
    q.put(f'Extracted {row_count} booking rows from PDF')

    csv_path = pdf_path.with_suffix('.csv')
    csv_path.write_text(csv_text, encoding='utf-8')
    q.put(f'Saved as: {csv_path.name}')

    return csv_path, csv_text


# ---------------------------------------------------------------------------
# Tool runner (background thread)
# ---------------------------------------------------------------------------

def _build_env(user_creds: dict) -> dict:
    """Build subprocess env, overriding credentials with per-user values from DB."""
    env = {**os.environ, 'PYTHONIOENCODING': 'utf-8'}
    if user_creds.get('comscore_user'):
        env['COMSCORE_USERNAME'] = user_creds['comscore_user']
    if user_creds.get('comscore_pass'):
        env['COMSCORE_PASSWORD'] = user_creds['comscore_pass']
    if user_creds.get('mica_user'):
        env['MICA_USERNAME'] = user_creds['mica_user']
    if user_creds.get('mica_pass'):
        env['MICA_PASSWORD'] = user_creds['mica_pass']
    return env


def _run_tool(csv_path: Path, job_id: str, user_creds: dict = {}):
    q = _job_queues[job_id]
    try:
        # PDF upload → extract booking CSV via pdfplumber
        if csv_path.suffix.lower() == '.pdf':
            result = _pdf_to_csv(csv_path, q)
            if result is None:
                return
            csv_path, csv_text = result
            q.put('__BOOKING_CSV__:' + json.dumps({'csv': csv_text}))

        # Excel upload → convert to CSV in-place
        elif csv_path.suffix.lower() in ('.xlsx', '.xls'):
            q.put('Converting Excel to CSV ...')
            csv_text = _excel_to_csv(csv_path)
            csv_path = csv_path.with_suffix('.csv')
            csv_path.write_text(csv_text, encoding='utf-8')
            q.put('__BOOKING_CSV__:' + json.dumps({'csv': csv_text}))

        # Image upload → extract booking CSV via Claude vision
        elif csv_path.suffix.lower() in ('.png', '.jpg', '.jpeg', '.webp'):
            result = _image_to_csv(csv_path, q)
            if result is None:
                return
            csv_path, csv_text = result
            # Send extracted CSV to the frontend so it auto-fills the Mica booking field
            q.put('__BOOKING_CSV__:' + json.dumps({'csv': csv_text}))

        # Only one Comscore scrape at a time — queue others with a message
        if not _comscore_lock.acquire(blocking=False):
            q.put('Another Comscore job is already running — waiting for it to finish...')
            print('[comscore] Waiting for lock...', flush=True)
            _comscore_lock.acquire()
            q.put('Previous job finished — starting now...')

        try:
            proc = subprocess.Popen(
                [sys.executable, str(BASE_DIR / 'flash_gross_tool.py'), str(csv_path)],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                encoding='utf-8',
                errors='replace',
                cwd=str(BASE_DIR),
                env=_build_env(user_creds),
            )
            for line in proc.stdout:
                line = line.rstrip()
                print(f'[comscore] {line}', flush=True)
                q.put(line)
            proc.wait()
        finally:
            _comscore_lock.release()

        if proc.returncode == 0:
            _job_results[job_id] = 'success'
            q.put('__SUCCESS__')
        else:
            _job_results[job_id] = f'error: Tool exited with code {proc.returncode}'
            q.put(f'__ERROR__ Tool exited with code {proc.returncode}')
    except Exception as exc:
        print(f'[comscore] EXCEPTION: {exc}', flush=True)
        _job_results[job_id] = f'error: {exc}'
        q.put(f'__ERROR__ {exc}')
    finally:
        q.put(None)  # Signal stream end


# ---------------------------------------------------------------------------
# Mica runner (background thread)
# ---------------------------------------------------------------------------

def _run_mica(booking_path: Path, contact: str, job_id: str, mode: str = "demo", user_creds: dict = {}):
    q = _job_queues[job_id]
    try:
        proc = subprocess.Popen(
            [
                sys.executable, '-u',
                str(BASE_DIR / 'mica_update.py'),
                str(booking_path),
                '--contact', contact,
                '--mode', mode,
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding='utf-8',
            errors='replace',
            env=_build_env(user_creds),
            cwd=str(BASE_DIR),
        )
        for line in proc.stdout:
            line = line.rstrip()
            print(f'[mica] {line}', flush=True)
            q.put(line)
        proc.wait()
        if proc.returncode == 0:
            _job_results[job_id] = 'success'
            q.put('__SUCCESS__')
        else:
            _job_results[job_id] = f'error: Mica script exited with code {proc.returncode}'
            q.put(f'__ERROR__ Mica script exited with code {proc.returncode}')
    except Exception as exc:
        print(f'[mica] EXCEPTION: {exc}', flush=True)
        _job_results[job_id] = f'error: {exc}'
        q.put(f'__ERROR__ {exc}')
    finally:
        q.put(None)


# ---------------------------------------------------------------------------
# Booking plan daemon — persistent browser reused across jobs
# ---------------------------------------------------------------------------

_BP_POOL_SIZE    = 1      # 1 normally; bump to 2 via /admin/bp-pool-size
_bp_procs:  list = [None, None]              # proc per slot (max 2)
_bp_envs:   list = [None, None]              # env snapshot per slot
_bp_modes:  list = [None, None]              # mode per slot
_bp_slots:  queue.Queue = queue.Queue()      # available slot indices
_bp_pool_lock = threading.Lock()             # guards pool-size changes
for _i in range(_BP_POOL_SIZE):
    _bp_slots.put(_i)


def _start_bp_daemon(slot: int, mode: str, user_creds: dict):
    """Start a booking plan daemon in the given slot and wait for __READY__."""
    env = _build_env(user_creds)
    proc = subprocess.Popen(
        [sys.executable, '-u', str(BASE_DIR / 'booking_plan_update.py'), '--daemon', '--mode', mode],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding='utf-8',
        errors='replace',
        env=env,
        cwd=str(BASE_DIR),
    )
    startup_lines = []
    for line in proc.stdout:
        stripped = line.rstrip()
        if stripped == '__READY__':
            break
        startup_lines.append(stripped)
    _bp_procs[slot] = proc
    _bp_envs[slot]  = env
    _bp_modes[slot] = mode
    return startup_lines


def _ensure_bp_daemon(slot: int, mode: str, user_creds: dict):
    """Ensure daemon in slot is running with correct env+mode. Caller owns the slot."""
    env = _build_env(user_creds)
    if (_bp_procs[slot] is not None
            and _bp_procs[slot].poll() is None
            and _bp_modes[slot] == mode
            and _bp_envs[slot] == env):
        return True
    if _bp_procs[slot] is not None and _bp_procs[slot].poll() is None:
        try:
            _bp_procs[slot].stdin.write('__QUIT__\n')
            _bp_procs[slot].stdin.flush()
        except Exception:
            pass
    _start_bp_daemon(slot, mode, user_creds)
    return True


# ---------------------------------------------------------------------------
# Booking plan runner (background thread)
# ---------------------------------------------------------------------------

def _run_booking_plan(title: str, contact: str, booking_path: Path, job_id: str, mode: str = 'demo', user_creds: dict = {}):
    import json as _json
    q = _job_queues[job_id]
    try:
        booking_text = ''
        if booking_path and Path(booking_path).exists():
            booking_text = Path(booking_path).read_text(encoding='utf-8-sig', errors='replace')

        slot = _bp_slots.get()   # blocks until a daemon slot is free
        try:
            _ensure_bp_daemon(slot, mode, user_creds)
            job_payload = _json.dumps({
                'title': title,
                'contact': contact,
                'booking_text': booking_text,
            })
            _bp_procs[slot].stdin.write(job_payload + '\n')
            _bp_procs[slot].stdin.flush()

            for line in _bp_procs[slot].stdout:
                stripped = line.rstrip()
                if stripped == '__JOB_DONE__':
                    _job_results[job_id] = 'success'
                    q.put('__SUCCESS__')
                    break
                elif stripped.startswith('__JOB_ERROR__'):
                    msg = stripped.replace('__JOB_ERROR__', '').strip()
                    _job_results[job_id] = f'error: {msg}'
                    q.put(f'__ERROR__ {msg}')
                    break
                elif stripped.startswith('__UNBOOKED__:'):
                    try:
                        _job_unbooked[job_id] = _json.loads(stripped[len('__UNBOOKED__:'):])
                    except Exception:
                        pass
                    # don't forward to SSE — UI fetches via /job-status/
                elif stripped.startswith('__ALREADY_BOOKED__:'):
                    try:
                        _job_already_booked[job_id] = _json.loads(stripped[len('__ALREADY_BOOKED__:'):])
                    except Exception:
                        pass
                elif stripped.startswith('__MISSED__:'):
                    try:
                        _job_missed[job_id] = _json.loads(stripped[len('__MISSED__:'):])
                    except Exception:
                        pass
                else:
                    q.put(stripped)

        finally:
            _bp_slots.put(slot)   # return slot to pool

    except Exception as exc:
        _job_results[job_id] = f'error: {exc}'
        q.put(f'__ERROR__ {exc}')
    finally:
        q.put(None)


# ---------------------------------------------------------------------------
# Mass booking plan runner (background thread)
# ---------------------------------------------------------------------------

def _run_mass_booking_plan(title: str, booking_path: Path, job_id: str, mode: str = 'demo', circuit: str = '', dry_run: bool = False):
    q = _job_queues[job_id]
    try:
        cmd = [
            sys.executable, '-u',
            str(BASE_DIR / 'booking_plan_update.py'),
            '--mass',
            '--booking', str(booking_path),
            '--mode', mode,
        ]
        if title:
            cmd += ['--title', title]
        if circuit:
            cmd += ['--circuit', circuit]
        if dry_run:
            cmd += ['--dry-run']
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding='utf-8',
            errors='replace',
            env={**__import__('os').environ, 'PYTHONIOENCODING': 'utf-8'},
            cwd=str(BASE_DIR),
        )
        for line in proc.stdout:
            q.put(line.rstrip())
        proc.wait()
        if proc.returncode == 0:
            _job_results[job_id] = 'success'
            q.put('__SUCCESS__')
        else:
            _job_results[job_id] = f'error: Mass booking script exited with code {proc.returncode}'
            q.put(f'__ERROR__ Mass booking script exited with code {proc.returncode}')
    except Exception as exc:
        _job_results[job_id] = f'error: {exc}'
        q.put(f'__ERROR__ {exc}')
    finally:
        q.put(None)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def _auto_update():
    """Pull latest changes from GitHub if this is a git repo."""
    try:
        import subprocess as _sp
        repo = BASE_DIR.parent
        # Check if inside a git repo
        result = _sp.run(['git', '-C', str(repo), 'rev-parse', '--is-inside-work-tree'],
                         capture_output=True, text=True)
        if result.returncode != 0:
            return  # Not a git repo — skip silently
        # Check for updates
        _sp.run(['git', '-C', str(repo), 'fetch'], capture_output=True, timeout=10)
        status = _sp.run(['git', '-C', str(repo), 'status', '-uno'],
                         capture_output=True, text=True).stdout
        if 'Your branch is up to date' in status:
            print('Auto-update: already up to date.')
            return
        # Pull updates
        pull = _sp.run(['git', '-C', str(repo), 'pull', '--ff-only'],
                       capture_output=True, text=True, timeout=30)
        if pull.returncode == 0:
            print(f'Auto-update: pulled latest changes.\n{pull.stdout.strip()}')
        else:
            print(f'Auto-update: could not pull (local changes present?).\n{pull.stderr.strip()}')
    except Exception as e:
        print(f'Auto-update: skipped ({e})')


def _watch_and_restart():
    """Background thread: restart the process if any .py file in this dir changes."""
    watch_dir = Path(__file__).parent
    py_files = list(watch_dir.glob('*.py'))
    mtimes = {f: f.stat().st_mtime for f in py_files if f.exists()}
    while True:
        time.sleep(1)
        for f in watch_dir.glob('*.py'):
            try:
                mtime = f.stat().st_mtime
            except OSError:
                continue
            if mtimes.get(f) != mtime:
                print(f'\n[reloader] {f.name} changed — restarting ...\n')
                time.sleep(0.3)  # brief pause so the file write finishes
                os.execv(sys.executable, [sys.executable] + sys.argv)


if __name__ == '__main__':
    # Only run the watcher in the main process (not after os.execv re-entry)
    if os.environ.get('_LAUNCHER_RELOADER') != '1':
        os.environ['_LAUNCHER_RELOADER'] = '1'
    watcher = threading.Thread(target=_watch_and_restart, daemon=True)
    watcher.start()

    _auto_update()
    host = '0.0.0.0' if os.getenv('SERVER_MODE') else 'localhost'
    server = http.server.ThreadingHTTPServer((host, PORT), Handler)
    url = f'http://localhost:{PORT}'
    print(f'Flash Gross Launcher -> {url}')
    print('Press Ctrl+C to stop.\n')
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print('\nStopped.')
