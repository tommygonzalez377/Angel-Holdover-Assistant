"""
auth.py — Google OAuth 2.0 + session management for the Angel Holdover Assistant.

Flow:
  1. User visits /auth/login → redirected to Google
  2. Google redirects to /auth/callback?code=...
  3. We exchange code for tokens, fetch user info, upsert user in DB
  4. Set a signed session cookie → user is logged in
  5. Subsequent requests read the cookie to identify the user

Session cookie: "session" — a base64-encoded JSON payload signed with SECRET_KEY.
"""

import os
import json
import hmac
import hashlib
import base64
import time
import urllib.parse
import urllib.request
from db import upsert_user, get_user_by_email

GOOGLE_CLIENT_ID     = os.getenv('GOOGLE_CLIENT_ID', '')
GOOGLE_CLIENT_SECRET = os.getenv('GOOGLE_CLIENT_SECRET', '')
GOOGLE_ALLOWED_DOMAIN= os.getenv('GOOGLE_ALLOWED_DOMAIN', 'angel.com')
SECRET_KEY           = os.getenv('SECRET_KEY', 'dev-secret-change-me')

GOOGLE_AUTH_URL  = 'https://accounts.google.com/o/oauth2/v2/auth'
GOOGLE_TOKEN_URL = 'https://oauth2.googleapis.com/token'
GOOGLE_USERINFO  = 'https://www.googleapis.com/oauth2/v3/userinfo'

SCOPES = 'openid email profile'


def _get_redirect_uri(host: str) -> str:
    scheme = 'https' if os.getenv('SERVER_MODE') else 'http'
    return f'{scheme}://{host}/auth/callback'


# ── Session cookie ────────────────────────────────────────────────────────────

def _sign(payload: str) -> str:
    sig = hmac.new(SECRET_KEY.encode(), payload.encode(), hashlib.sha256).hexdigest()
    return sig


def create_session_cookie(user_id: int, email: str) -> str:
    """Create a signed session cookie value."""
    data = json.dumps({'uid': user_id, 'email': email, 'ts': int(time.time())})
    b64  = base64.urlsafe_b64encode(data.encode()).decode()
    sig  = _sign(b64)
    return f'{b64}.{sig}'


def verify_session_cookie(cookie_value: str) -> dict | None:
    """Verify and decode a session cookie. Returns payload dict or None."""
    if not cookie_value:
        return None
    try:
        b64, sig = cookie_value.rsplit('.', 1)
    except ValueError:
        return None
    if not hmac.compare_digest(_sign(b64), sig):
        return None
    try:
        data = json.loads(base64.urlsafe_b64decode(b64.encode()).decode())
    except Exception:
        return None
    # Sessions expire after 30 days
    if time.time() - data.get('ts', 0) > 30 * 86400:
        return None
    return data


def get_session_user(handler) -> dict | None:
    """
    Extract the current user from the request's session cookie.
    Returns user dict (id, email, name, ...) or None if not logged in.
    """
    cookie_header = handler.headers.get('Cookie', '')
    cookies = {}
    for part in cookie_header.split(';'):
        part = part.strip()
        if '=' in part:
            k, v = part.split('=', 1)
            cookies[k.strip()] = v.strip()

    session_val = cookies.get('session', '')
    payload = verify_session_cookie(session_val)
    if not payload:
        return None

    from db import get_user_by_id
    return get_user_by_id(payload['uid'])


def require_auth(handler) -> dict | None:
    """
    Check auth; if not logged in, send a 302 redirect to /auth/login and return None.
    Usage: user = require_auth(self); if not user: return
    """
    user = get_session_user(handler)
    if not user:
        handler.send_response(302)
        handler.send_header('Location', '/auth/login')
        handler.end_headers()
    return user


# ── OAuth flow ────────────────────────────────────────────────────────────────

def get_login_url(host: str, state: str = '') -> str:
    """Build the Google OAuth authorization URL."""
    params = {
        'client_id':     GOOGLE_CLIENT_ID,
        'redirect_uri':  _get_redirect_uri(host),
        'response_type': 'code',
        'scope':         SCOPES,
        'access_type':   'online',
        'prompt':        'select_account',
    }
    if state:
        params['state'] = state
    return GOOGLE_AUTH_URL + '?' + urllib.parse.urlencode(params)


def handle_callback(host: str, code: str) -> dict | None:
    """
    Exchange authorization code for user info.
    Returns user dict on success, None on failure.
    """
    if not GOOGLE_CLIENT_ID or not GOOGLE_CLIENT_SECRET:
        return None

    redirect_uri = _get_redirect_uri(host)

    # Exchange code for tokens
    token_data = urllib.parse.urlencode({
        'code':          code,
        'client_id':     GOOGLE_CLIENT_ID,
        'client_secret': GOOGLE_CLIENT_SECRET,
        'redirect_uri':  redirect_uri,
        'grant_type':    'authorization_code',
    }).encode()

    try:
        req = urllib.request.Request(GOOGLE_TOKEN_URL, data=token_data, method='POST')
        req.add_header('Content-Type', 'application/x-www-form-urlencoded')
        with urllib.request.urlopen(req, timeout=15) as r:
            tokens = json.loads(r.read())
    except Exception as e:
        print(f'[auth] Token exchange failed: {e}')
        return None

    access_token = tokens.get('access_token')
    if not access_token:
        print(f'[auth] No access token in response: {tokens}')
        return None

    # Fetch user info
    try:
        req = urllib.request.Request(GOOGLE_USERINFO)
        req.add_header('Authorization', f'Bearer {access_token}')
        with urllib.request.urlopen(req, timeout=15) as r:
            info = json.loads(r.read())
    except Exception as e:
        print(f'[auth] User info fetch failed: {e}')
        return None

    email     = info.get('email', '')
    name      = info.get('name', '')
    google_id = info.get('sub', '')

    # Enforce allowed domain
    if GOOGLE_ALLOWED_DOMAIN and not email.endswith(f'@{GOOGLE_ALLOWED_DOMAIN}'):
        print(f'[auth] Rejected login from {email} — not @{GOOGLE_ALLOWED_DOMAIN}')
        return None

    # Upsert user in DB
    user = upsert_user(email=email, name=name, google_id=google_id)
    return user


# ── Login page HTML ───────────────────────────────────────────────────────────

LOGIN_PAGE = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Angel Holdover Assistant — Sign In</title>
<style>
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body {
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
    background: #111;
    color: #fff;
    min-height: 100vh;
    display: flex;
    align-items: center;
    justify-content: center;
  }
  .card {
    background: #1e1e1e;
    border: 1px solid #333;
    border-radius: 12px;
    padding: 48px 40px;
    width: 380px;
    text-align: center;
  }
  .logo { font-size: 28px; font-weight: 700; color: #00bcd4; margin-bottom: 8px; }
  .subtitle { color: #888; font-size: 14px; margin-bottom: 36px; }
  .google-btn {
    display: flex;
    align-items: center;
    justify-content: center;
    gap: 12px;
    background: #fff;
    color: #333;
    font-size: 15px;
    font-weight: 500;
    padding: 12px 24px;
    border-radius: 8px;
    text-decoration: none;
    transition: background 0.15s;
    border: none;
    cursor: pointer;
    width: 100%;
  }
  .google-btn:hover { background: #f0f0f0; }
  .google-icon { width: 20px; height: 20px; }
  .error { color: #ef5350; font-size: 13px; margin-top: 16px; }
</style>
</head>
<body>
<div class="card">
  <div class="logo">Angel Studios</div>
  <div class="subtitle">Holdover Assistant</div>
  <a class="google-btn" href="{login_url}">
    <svg class="google-icon" viewBox="0 0 24 24">
      <path fill="#4285F4" d="M22.56 12.25c0-.78-.07-1.53-.2-2.25H12v4.26h5.92c-.26 1.37-1.04 2.53-2.21 3.31v2.77h3.57c2.08-1.92 3.28-4.74 3.28-8.09z"/>
      <path fill="#34A853" d="M12 23c2.97 0 5.46-.98 7.28-2.66l-3.57-2.77c-.98.66-2.23 1.06-3.71 1.06-2.86 0-5.29-1.93-6.16-4.53H2.18v2.84C3.99 20.53 7.7 23 12 23z"/>
      <path fill="#FBBC05" d="M5.84 14.09c-.22-.66-.35-1.36-.35-2.09s.13-1.43.35-2.09V7.07H2.18C1.43 8.55 1 10.22 1 12s.43 3.45 1.18 4.93l3.66-2.84z"/>
      <path fill="#EA4335" d="M12 5.38c1.62 0 3.06.56 4.21 1.64l3.15-3.15C17.45 2.09 14.97 1 12 1 7.7 1 3.99 3.47 2.18 7.07l3.66 2.84c.87-2.6 3.3-4.53 6.16-4.53z"/>
    </svg>
    Sign in with Google
  </a>
  {error_html}
</div>
</body>
</html>"""


def render_login_page(login_url: str, error: str = '') -> str:
    error_html = f'<p class="error">{error}</p>' if error else ''
    return LOGIN_PAGE.replace('{login_url}', login_url).replace('{error_html}', error_html)


# ── Profile page HTML ─────────────────────────────────────────────────────────

PROFILE_PAGE = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>My Profile — Angel Holdover Assistant</title>
<style>
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body {
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
    background: #111; color: #fff;
    min-height: 100vh;
    display: flex; align-items: center; justify-content: center;
  }
  .card {
    background: #1e1e1e; border: 1px solid #333; border-radius: 12px;
    padding: 40px; width: 460px;
  }
  h2 { font-size: 20px; margin-bottom: 6px; }
  .email { color: #888; font-size: 13px; margin-bottom: 28px; }
  .section { margin-bottom: 24px; }
  .section h3 { font-size: 13px; text-transform: uppercase; letter-spacing: 0.05em;
                color: #00bcd4; margin-bottom: 12px; }
  label { display: block; font-size: 13px; color: #aaa; margin-bottom: 4px; }
  input {
    width: 100%; background: #2a2a2a; border: 1px solid #444; border-radius: 6px;
    color: #fff; padding: 9px 12px; font-size: 14px; margin-bottom: 12px;
  }
  input:focus { outline: none; border-color: #00bcd4; }
  .btn {
    background: #00bcd4; color: #000; font-weight: 600; font-size: 14px;
    padding: 10px 24px; border-radius: 6px; border: none; cursor: pointer;
    margin-right: 10px;
  }
  .btn:hover { background: #00acc1; }
  .btn-secondary {
    background: transparent; color: #888; border: 1px solid #444;
    padding: 10px 20px; border-radius: 6px; font-size: 14px; cursor: pointer;
    text-decoration: none; display: inline-block;
  }
  .success { color: #66bb6a; font-size: 13px; margin-top: 12px; }
  .back { display: inline-block; color: #888; font-size: 13px;
          text-decoration: none; margin-bottom: 24px; }
  .back:hover { color: #fff; }
</style>
</head>
<body>
<div class="card">
  <a class="back" href="/">← Back to tool</a>
  <h2>{name}</h2>
  <div class="email">{email}</div>
  <form method="POST" action="/auth/profile">
    <div class="section">
      <h3>Comscore Credentials</h3>
      <label>Username</label>
      <input type="text" name="comscore_user" value="{comscore_user}" placeholder="Your Comscore username" autocomplete="off">
      <label>Password</label>
      <input type="password" name="comscore_pass" value="{comscore_pass}" placeholder="Your Comscore password" autocomplete="new-password">
    </div>
    <div class="section">
      <h3>Mica Credentials</h3>
      <label>Email</label>
      <input type="email" name="mica_user" value="{mica_user}" placeholder="your@angel.com" autocomplete="off">
      <label>Password</label>
      <input type="password" name="mica_pass" value="{mica_pass}" placeholder="Your Mica password" autocomplete="new-password">
    </div>
    <button class="btn" type="submit">Save Credentials</button>
    {success_html}
  </form>
</div>
</body>
</html>"""


def render_profile_page(user: dict, creds: dict, saved: bool = False) -> str:
    success_html = '<p class="success">✓ Credentials saved</p>' if saved else ''
    return (PROFILE_PAGE
            .replace('{name}',          user.get('name') or user.get('email', ''))
            .replace('{email}',         user.get('email', ''))
            .replace('{comscore_user}', creds.get('comscore_user', ''))
            .replace('{comscore_pass}', creds.get('comscore_pass', ''))
            .replace('{mica_user}',     creds.get('mica_user', ''))
            .replace('{mica_pass}',     creds.get('mica_pass', ''))
            .replace('{success_html}',  success_html))
