"""
db.py — Database layer for the Angel Holdover Assistant.

Uses PostgreSQL when DATABASE_URL is set (production / Fly.io),
falls back to SQLite for local development.

Tables:
  users          — Google OAuth users with encrypted Comscore/Mica credentials
  venue_aliases  — booking name → master list name mappings (replaces hardcoded dicts)
  master_list    — Comscore/Rentrak theatre master list cache (replaces CSV file)
"""

import os
import sqlite3
import hashlib
import hmac
import base64
import json
from pathlib import Path
from datetime import datetime

# ── Connection ────────────────────────────────────────────────────────────────

DATABASE_URL = os.getenv('DATABASE_URL', '')
SECRET_KEY   = os.getenv('SECRET_KEY', 'dev-secret-change-me')
BASE_DIR     = Path(__file__).parent
SQLITE_PATH  = BASE_DIR / 'output' / 'holdover.db'

_IS_POSTGRES = bool(DATABASE_URL)


def _get_conn():
    """Return a database connection (PostgreSQL or SQLite)."""
    if _IS_POSTGRES:
        import psycopg2
        import psycopg2.extras
        conn = psycopg2.connect(DATABASE_URL)
        conn.autocommit = False
        return conn
    else:
        SQLITE_PATH.parent.mkdir(exist_ok=True)
        conn = sqlite3.connect(str(SQLITE_PATH))
        conn.row_factory = sqlite3.Row
        conn.execute('PRAGMA journal_mode=WAL')
        return conn


def _placeholder():
    """SQL placeholder: %s for Postgres, ? for SQLite."""
    return '%s' if _IS_POSTGRES else '?'


def _now():
    return datetime.utcnow().isoformat()


# ── Schema ────────────────────────────────────────────────────────────────────

_SCHEMA_POSTGRES = """
CREATE TABLE IF NOT EXISTS users (
    id              SERIAL PRIMARY KEY,
    email           TEXT UNIQUE NOT NULL,
    name            TEXT,
    google_id       TEXT UNIQUE,
    comscore_user   TEXT,
    comscore_pass   TEXT,
    mica_user       TEXT,
    mica_pass       TEXT,
    created_at      TIMESTAMP DEFAULT NOW(),
    last_login      TIMESTAMP
);

CREATE TABLE IF NOT EXISTS venue_aliases (
    id              SERIAL PRIMARY KEY,
    booking_name    TEXT NOT NULL,
    city            TEXT DEFAULT '',
    master_name     TEXT NOT NULL,
    chain           TEXT DEFAULT '',
    created_by      INTEGER REFERENCES users(id),
    created_at      TIMESTAMP DEFAULT NOW(),
    UNIQUE(booking_name, city)
);

CREATE TABLE IF NOT EXISTS master_list (
    unit_id          TEXT PRIMARY KEY,
    venue_name       TEXT NOT NULL,
    exhibitor        TEXT DEFAULT '',
    exhibitor_ref_id TEXT DEFAULT '',
    city             TEXT DEFAULT '',
    state            TEXT DEFAULT '',
    state_code       TEXT DEFAULT '',
    country          TEXT DEFAULT '',
    country_code     TEXT DEFAULT '',
    tv_market        TEXT DEFAULT '',
    venue_group      TEXT DEFAULT '',
    venue_mb_id      TEXT DEFAULT '',
    rentrak_id       TEXT DEFAULT '',
    buyer            TEXT DEFAULT '',
    angel_booker     TEXT DEFAULT '',
    last_updated     TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS master_list_changelog (
    id           SERIAL PRIMARY KEY,
    venue_mb_id  TEXT NOT NULL,
    venue_name   TEXT DEFAULT '',
    field_name   TEXT NOT NULL,
    old_value    TEXT DEFAULT '',
    new_value    TEXT DEFAULT '',
    changed_at   TIMESTAMP DEFAULT NOW()
);
"""

_SCHEMA_SQLITE = """
CREATE TABLE IF NOT EXISTS users (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    email           TEXT UNIQUE NOT NULL,
    name            TEXT,
    google_id       TEXT UNIQUE,
    comscore_user   TEXT,
    comscore_pass   TEXT,
    mica_user       TEXT,
    mica_pass       TEXT,
    created_at      TEXT DEFAULT (datetime('now')),
    last_login      TEXT
);

CREATE TABLE IF NOT EXISTS venue_aliases (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    booking_name    TEXT NOT NULL,
    city            TEXT DEFAULT '',
    master_name     TEXT NOT NULL,
    chain           TEXT DEFAULT '',
    created_by      INTEGER REFERENCES users(id),
    created_at      TEXT DEFAULT (datetime('now')),
    UNIQUE(booking_name, city)
);

CREATE TABLE IF NOT EXISTS master_list (
    unit_id          TEXT PRIMARY KEY,
    venue_name       TEXT NOT NULL,
    exhibitor        TEXT DEFAULT '',
    exhibitor_ref_id TEXT DEFAULT '',
    city             TEXT DEFAULT '',
    state            TEXT DEFAULT '',
    state_code       TEXT DEFAULT '',
    country          TEXT DEFAULT '',
    country_code     TEXT DEFAULT '',
    tv_market        TEXT DEFAULT '',
    venue_group      TEXT DEFAULT '',
    venue_mb_id      TEXT DEFAULT '',
    rentrak_id       TEXT DEFAULT '',
    buyer            TEXT DEFAULT '',
    angel_booker     TEXT DEFAULT '',
    last_updated     TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS master_list_changelog (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    venue_mb_id TEXT NOT NULL,
    venue_name  TEXT DEFAULT '',
    field_name  TEXT NOT NULL,
    old_value   TEXT DEFAULT '',
    new_value   TEXT DEFAULT '',
    changed_at  TEXT DEFAULT (datetime('now'))
);
"""


def init_db():
    """Create tables if they don't exist. Safe to call on every startup."""
    conn = _get_conn()
    try:
        cur = conn.cursor()
        schema = _SCHEMA_POSTGRES if _IS_POSTGRES else _SCHEMA_SQLITE
        # Execute each statement separately (SQLite doesn't support multi-statement)
        for stmt in schema.strip().split(';'):
            stmt = stmt.strip()
            if stmt:
                cur.execute(stmt)
        conn.commit()
        print(f'[db] {"PostgreSQL" if _IS_POSTGRES else "SQLite"} ready — {DATABASE_URL or SQLITE_PATH}')
    finally:
        conn.close()


# ── Credential encryption ─────────────────────────────────────────────────────
# Simple symmetric encryption using HMAC + XOR with the SECRET_KEY.
# Not military-grade but prevents plaintext passwords in the DB.

def _derive_key(length: int = 32) -> bytes:
    return hashlib.pbkdf2_hmac('sha256', SECRET_KEY.encode(), b'angel-holdover', 100_000, dklen=length)


def encrypt(plaintext: str) -> str:
    """Encrypt a string → base64-encoded ciphertext."""
    if not plaintext:
        return ''
    key = _derive_key(len(plaintext.encode()))
    ct  = bytes(a ^ b for a, b in zip(plaintext.encode(), key))
    return base64.urlsafe_b64encode(ct).decode()


def decrypt(ciphertext: str) -> str:
    """Decrypt base64-encoded ciphertext → plaintext."""
    if not ciphertext:
        return ''
    try:
        ct  = base64.urlsafe_b64decode(ciphertext.encode())
        key = _derive_key(len(ct))
        return bytes(a ^ b for a, b in zip(ct, key)).decode()
    except Exception:
        return ''


# ── Users ─────────────────────────────────────────────────────────────────────

def get_or_create_local_user() -> dict:
    """
    Return (or create) the single local user used in local/desktop mode.
    This ensures save_credentials / get_credentials have a real row to work with.
    """
    return upsert_user(email='local', name='Local User')


def upsert_user(email: str, name: str = '', google_id: str = '') -> dict:
    """Create or update a user on Google login. Returns the user row."""
    p = _placeholder()
    conn = _get_conn()
    try:
        cur = conn.cursor()
        if _IS_POSTGRES:
            cur.execute(f"""
                INSERT INTO users (email, name, google_id, last_login)
                VALUES ({p}, {p}, {p}, NOW())
                ON CONFLICT (email) DO UPDATE
                    SET name={p}, google_id=COALESCE(EXCLUDED.google_id, users.google_id),
                        last_login=NOW()
                RETURNING id, email, name, comscore_user, comscore_pass, mica_user, mica_pass
            """, (email, name, google_id, name))
            row = dict(cur.fetchone())
        else:
            cur.execute(f"""
                INSERT INTO users (email, name, google_id, last_login)
                VALUES ({p}, {p}, {p}, datetime('now'))
                ON CONFLICT(email) DO UPDATE
                    SET name={p},
                        google_id=COALESCE(excluded.google_id, users.google_id),
                        last_login=datetime('now')
            """, (email, name, google_id, name))
            cur.execute(f'SELECT * FROM users WHERE email={p}', (email,))
            row = dict(cur.fetchone())
        conn.commit()
        return row
    finally:
        conn.close()


def get_user_by_email(email: str) -> dict | None:
    p = _placeholder()
    conn = _get_conn()
    try:
        cur = conn.cursor()
        cur.execute(f'SELECT * FROM users WHERE email={p}', (email,))
        row = cur.fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def get_user_by_id(user_id: int) -> dict | None:
    p = _placeholder()
    conn = _get_conn()
    try:
        cur = conn.cursor()
        cur.execute(f'SELECT * FROM users WHERE id={p}', (user_id,))
        row = cur.fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def save_credentials(user_id: int, comscore_user: str = '', comscore_pass: str = '',
                     mica_user: str = '', mica_pass: str = ''):
    """Save encrypted Comscore + Mica credentials for a user."""
    p = _placeholder()
    conn = _get_conn()
    try:
        cur = conn.cursor()
        cur.execute(f"""
            UPDATE users SET
                comscore_user={p}, comscore_pass={p},
                mica_user={p},     mica_pass={p}
            WHERE id={p}
        """, (comscore_user, encrypt(comscore_pass),
              mica_user,     encrypt(mica_pass),
              user_id))
        conn.commit()
    finally:
        conn.close()


def get_credentials(user_id: int) -> dict:
    """Return decrypted credentials for a user."""
    user = get_user_by_id(user_id)
    if not user:
        return {}
    return {
        'comscore_user': user.get('comscore_user') or '',
        'comscore_pass': decrypt(user.get('comscore_pass') or ''),
        'mica_user':     user.get('mica_user') or '',
        'mica_pass':     decrypt(user.get('mica_pass') or ''),
    }


# ── Venue aliases ─────────────────────────────────────────────────────────────

def get_all_aliases() -> list[dict]:
    """Return all venue aliases as a list of dicts."""
    conn = _get_conn()
    try:
        cur = conn.cursor()
        cur.execute('SELECT id, booking_name, city, master_name, chain FROM venue_aliases ORDER BY booking_name')
        return [dict(row) for row in cur.fetchall()]
    finally:
        conn.close()


def get_alias(booking_name: str, city: str = '') -> str | None:
    """Look up master name for a booking name (city-qualified first, then plain)."""
    p = _placeholder()
    conn = _get_conn()
    try:
        cur = conn.cursor()
        name_lower = booking_name.lower().strip()
        city_lower = city.lower().strip()
        # Try city-qualified first
        if city_lower:
            cur.execute(f'SELECT master_name FROM venue_aliases WHERE booking_name={p} AND city={p}',
                        (name_lower, city_lower))
            row = cur.fetchone()
            if row:
                return row[0] if not _IS_POSTGRES else row['master_name']
        # Fall back to plain name
        cur.execute(f'SELECT master_name FROM venue_aliases WHERE booking_name={p} AND city={p}',
                    (name_lower, ''))
        row = cur.fetchone()
        if row:
            return row[0] if not _IS_POSTGRES else row['master_name']
        return None
    finally:
        conn.close()


def upsert_alias(booking_name: str, master_name: str, city: str = '',
                 chain: str = '', created_by: int | None = None):
    """Add or update a venue alias."""
    p = _placeholder()
    conn = _get_conn()
    try:
        cur = conn.cursor()
        name_lower = booking_name.lower().strip()
        city_lower = city.lower().strip()
        if _IS_POSTGRES:
            cur.execute(f"""
                INSERT INTO venue_aliases (booking_name, city, master_name, chain, created_by)
                VALUES ({p},{p},{p},{p},{p})
                ON CONFLICT (booking_name, city) DO UPDATE
                    SET master_name=EXCLUDED.master_name, chain=EXCLUDED.chain
            """, (name_lower, city_lower, master_name.lower().strip(), chain, created_by))
        else:
            cur.execute(f"""
                INSERT INTO venue_aliases (booking_name, city, master_name, chain, created_by)
                VALUES ({p},{p},{p},{p},{p})
                ON CONFLICT(booking_name, city) DO UPDATE
                    SET master_name=excluded.master_name, chain=excluded.chain
            """, (name_lower, city_lower, master_name.lower().strip(), chain, created_by))
        conn.commit()
    finally:
        conn.close()


def delete_alias(alias_id: int):
    p = _placeholder()
    conn = _get_conn()
    try:
        cur = conn.cursor()
        cur.execute(f'DELETE FROM venue_aliases WHERE id={p}', (alias_id,))
        conn.commit()
    finally:
        conn.close()


# ── Master list ───────────────────────────────────────────────────────────────

def get_master_list_age() -> int:
    """Return age of master list in days, or 999 if empty."""
    conn = _get_conn()
    try:
        cur = conn.cursor()
        cur.execute('SELECT MAX(last_updated) FROM master_list')
        row = cur.fetchone()
        val = row[0] if row else None
        if not val:
            return 999
        if isinstance(val, str):
            updated = datetime.fromisoformat(val)
        else:
            updated = val
        return (datetime.utcnow() - updated).days
    finally:
        conn.close()


def upsert_master_list(rows: list[dict]):
    """Bulk upsert master list rows. Uses Venue MB ID as primary key. Logs field-level changes."""
    if not rows:
        return
    p = _placeholder()
    conn = _get_conn()
    try:
        cur = conn.cursor()
        now = _now()
        inserted = updated = skipped = 0
        changelog_rows = []

        for r in rows:
            # unit_id = Venue MB ID (reliable unique key across US/CA/PR)
            unit_id    = str(r.get('unit_id', r.get('Venue MB ID', ''))).strip()
            venue_name = str(r.get('venue_name', r.get('Venue', ''))).strip()
            if not unit_id or not venue_name:
                skipped += 1
                continue

            def _s(key, alt=''):
                v = r.get(key, r.get(alt, ''))
                return '' if (v is None or str(v).strip().lower() in ('nan', 'none', '')) else str(v).strip()

            new_vals = {
                'venue_name':       venue_name,
                'exhibitor':        _s('exhibitor', 'Exhibitor'),
                'exhibitor_ref_id': _s('exhibitor_ref_id', "Exhibitor's Ref ID"),
                'city':             _s('city', 'City'),
                'state':            _s('state', 'State'),
                'state_code':       _s('state_code', 'State Code'),
                'country':          _s('country', 'Country'),
                'country_code':     _s('country_code', 'Country Code'),
                'tv_market':        _s('tv_market', 'TV Market'),
                'venue_group':      _s('venue_group', 'Venue Group'),
                'venue_mb_id':      unit_id,
                'rentrak_id':       _s('rentrak_id', 'Venue Rentrak ID'),
                'buyer':            _s('buyer', 'Buyer'),
                'angel_booker':     _s('angel_booker', 'Booker(s)'),
            }

            # Check for existing row to detect changes
            cur.execute('SELECT * FROM master_list WHERE unit_id = ?', (unit_id,)) if not _IS_POSTGRES else \
                cur.execute('SELECT * FROM master_list WHERE unit_id = %s', (unit_id,))
            existing = cur.fetchone()

            if existing:
                existing_dict = dict(existing)
                changed_fields = [
                    f for f in new_vals
                    if str(existing_dict.get(f, '')).strip() != new_vals[f]
                ]
                if changed_fields:
                    for field in changed_fields:
                        changelog_rows.append((
                            unit_id, venue_name, field,
                            str(existing_dict.get(field, '')), new_vals[field], now
                        ))
                    updated += 1
                else:
                    skipped += 1
                    continue
            else:
                inserted += 1

            args = (
                unit_id,
                new_vals['venue_name'], new_vals['exhibitor'], new_vals['exhibitor_ref_id'],
                new_vals['city'], new_vals['state'], new_vals['state_code'],
                new_vals['country'], new_vals['country_code'], new_vals['tv_market'],
                new_vals['venue_group'], new_vals['venue_mb_id'], new_vals['rentrak_id'],
                new_vals['buyer'], new_vals['angel_booker'], now,
            )
            if _IS_POSTGRES:
                cur.execute(f"""
                    INSERT INTO master_list
                        (unit_id, venue_name, exhibitor, exhibitor_ref_id, city, state, state_code,
                         country, country_code, tv_market, venue_group, venue_mb_id,
                         rentrak_id, buyer, angel_booker, last_updated)
                    VALUES ({p},{p},{p},{p},{p},{p},{p},{p},{p},{p},{p},{p},{p},{p},{p},{p})
                    ON CONFLICT (unit_id) DO UPDATE SET
                        venue_name=EXCLUDED.venue_name, exhibitor=EXCLUDED.exhibitor,
                        exhibitor_ref_id=EXCLUDED.exhibitor_ref_id,
                        city=EXCLUDED.city, state=EXCLUDED.state, state_code=EXCLUDED.state_code,
                        country=EXCLUDED.country, country_code=EXCLUDED.country_code,
                        tv_market=EXCLUDED.tv_market, venue_group=EXCLUDED.venue_group,
                        venue_mb_id=EXCLUDED.venue_mb_id, rentrak_id=EXCLUDED.rentrak_id,
                        buyer=EXCLUDED.buyer, angel_booker=EXCLUDED.angel_booker,
                        last_updated=EXCLUDED.last_updated
                """, args)
            else:
                cur.execute(f"""
                    INSERT INTO master_list
                        (unit_id, venue_name, exhibitor, exhibitor_ref_id, city, state, state_code,
                         country, country_code, tv_market, venue_group, venue_mb_id,
                         rentrak_id, buyer, angel_booker, last_updated)
                    VALUES ({p},{p},{p},{p},{p},{p},{p},{p},{p},{p},{p},{p},{p},{p},{p},{p})
                    ON CONFLICT(unit_id) DO UPDATE SET
                        venue_name=excluded.venue_name, exhibitor=excluded.exhibitor,
                        exhibitor_ref_id=excluded.exhibitor_ref_id,
                        city=excluded.city, state=excluded.state, state_code=excluded.state_code,
                        country=excluded.country, country_code=excluded.country_code,
                        tv_market=excluded.tv_market, venue_group=excluded.venue_group,
                        venue_mb_id=excluded.venue_mb_id, rentrak_id=excluded.rentrak_id,
                        buyer=excluded.buyer, angel_booker=excluded.angel_booker,
                        last_updated=excluded.last_updated
                """, args)

        # Write changelog
        if changelog_rows:
            cur.executemany(
                f'INSERT INTO master_list_changelog (venue_mb_id, venue_name, field_name, old_value, new_value, changed_at) '
                f'VALUES ({p},{p},{p},{p},{p},{p})',
                changelog_rows
            )

        conn.commit()
        print(f'[db] Master list: {inserted} inserted, {updated} updated, {skipped} unchanged/skipped')
        return {'inserted': inserted, 'updated': updated, 'skipped': skipped}
    finally:
        conn.close()


def get_master_list_as_dicts() -> list[dict]:
    """Return full master list as list of dicts (for compatibility with existing code)."""
    conn = _get_conn()
    try:
        cur = conn.cursor()
        cur.execute('SELECT * FROM master_list')
        return [dict(row) for row in cur.fetchall()]
    finally:
        conn.close()


def get_master_list_count() -> int:
    conn = _get_conn()
    try:
        cur = conn.cursor()
        cur.execute('SELECT COUNT(*) FROM master_list')
        return cur.fetchone()[0]
    finally:
        conn.close()


def get_master_list_changelog(limit: int = 100) -> list[dict]:
    """Return recent master list changes, newest first."""
    conn = _get_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            'SELECT * FROM master_list_changelog ORDER BY changed_at DESC LIMIT ?', (limit,)
        ) if not _IS_POSTGRES else cur.execute(
            'SELECT * FROM master_list_changelog ORDER BY changed_at DESC LIMIT %s', (limit,)
        )
        return [dict(row) for row in cur.fetchall()]
    finally:
        conn.close()


def migrate_master_list_schema():
    """Add new columns to master_list if upgrading from old schema. Safe to run multiple times."""
    conn = _get_conn()
    try:
        cur = conn.cursor()
        new_cols = [
            ('exhibitor_ref_id', "TEXT DEFAULT ''"),
            ('state_code',       "TEXT DEFAULT ''"),
            ('country_code',     "TEXT DEFAULT ''"),
            ('tv_market',        "TEXT DEFAULT ''"),
            ('venue_group',      "TEXT DEFAULT ''"),
            ('angel_booker',     "TEXT DEFAULT ''"),
        ]
        for col, typedef in new_cols:
            try:
                cur.execute(f'ALTER TABLE master_list ADD COLUMN {col} {typedef}')
            except Exception:
                pass  # column already exists
        # Create changelog table if missing
        cur.execute("""
            CREATE TABLE IF NOT EXISTS master_list_changelog (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                venue_mb_id TEXT NOT NULL,
                venue_name  TEXT DEFAULT '',
                field_name  TEXT NOT NULL,
                old_value   TEXT DEFAULT '',
                new_value   TEXT DEFAULT '',
                changed_at  TEXT DEFAULT (datetime('now'))
            )
        """)
        conn.commit()
        print('[db] master_list schema migration complete')
    finally:
        conn.close()


# ── Seed aliases from hardcoded dicts ─────────────────────────────────────────

def _do_seed_aliases():
    """Upsert all seed aliases unconditionally. Called by both seed functions."""
    print('[db] Upserting seed venue aliases...')

    # Combined alias list from both flash_gross_tool.py and mica_update.py
    SEED_ALIASES = [
        # Comscore / flash_gross_tool aliases
        ('west chester 18',              '',            'amc west chester township 18',         'AMC'),
        ('landmark 12 surrey',           '',            'landmark guildford 12 (100)',           'Landmark'),
        ('cinemark 22 + imax',           '',            'cinemark lancaster 22',                 'Cinemark'),
        ('cinemark 22 + imax',           'lancaster',   'cinemark lancaster 22',                 'Cinemark'),
        ('cinemark 16 +xd',              'victorville', 'cinemark victorville 16 + xd',          'Cinemark'),
        # Mica / mica_update aliases — Cinemark DFW
        ('cinemark central plano 10',    '',            'cinemark movies plano 10',              'Cinemark'),
        ('cut! by cinemark',             '',            'cinemark cut! 10',                      'Cinemark'),
        ('cinemark 17',                  '',            'cinemark 17 + imax',                    'Cinemark'),
        ('rave ridgmar 13',              '',            'cinemark ridgmar mall 13 + xd',         'Cinemark'),
        ('rave north east mall 18',      '',            'cinemark northeast mall 18 + xd',       'Cinemark'),
        ('cinemark cleburne',            '',            'cinemark cinema cleburne 6',            'Cinemark'),
        ('cinemark 12 and xd',           '',            'cinemark mansfield 12 + xd',            'Cinemark'),
        ('tinseltown grapevine and xd',  '',            'cinemark tinseltown grapevine 17 + xd', 'Cinemark'),
        ('cinemark 17 + imax',           '',            'cinemark tulsa 17',                     'Cinemark'),
        # City-qualified — Cinemark DFW
        ('cinemark 14',                  'cedar hill',  'cinemark cedar hill 14',                'Cinemark'),
        ('movies 14',                    'lancaster',   'cinemark movies lancaster 14',           'Cinemark'),
        ('cinemark 14',                  'denton',      'cinemark denton 14',                    'Cinemark'),
        ('cinemark 12',                  'sherman',     'cinemark sherman 12',                   'Cinemark'),
        ('movies 8',                     'paris',       'cinemark movies paris 8',               'Cinemark'),
        # Small-exhibitor city+state aliases
        ('espanola, nm',                 '',            'dreamcatcher 10',                       ''),
        ('espanola',                     '',            'dreamcatcher 10',                       ''),
        ('independence, mo',             '',            'pharaoh independence 4',                ''),
        ('guymon, ok',                   '',            'northridge guymon 8',                   ''),
        ('florence, sc',                 '',            'julia florence 4',                      ''),
        ('tulsa, ok',                    '',            'eton tulsa 6',                          ''),
        ('kirksville, mo',               '',            'downtown kirksville 8',                 ''),
        ('marion, nc',                   '',            'hometown cinemas marion 2',             ''),
        ('fulton, mo',                   '',            'fulton cinema 8',                       ''),
        ('lumberton, nc',                '',            'hometown lumberton 4',                  ''),
        ('marshall, mo',                 '',            'cinema marshall 3',                     ''),
        ('milford, ia',                  '',            'pioneer milford 1',                     ''),
        ('parsons, ks',                  '',            'the parsons theatre',                   ''),
        ('norton, ks',                   '',            'norton theatre',                        ''),
        # Cinemark national shorthand
        ('tinseltown usa',               'jacksonville', 'cinemark tinseltown jacksonville 20 + xd',  'Cinemark'),
        ('tinseltown usa',               'fayetteville', 'cinemark tinseltown fayetteville 17 + xd',  'Cinemark'),
        ('tinseltown usa',               'north aurora', 'cinemark tinseltown north aurora 17 usa',   'Cinemark'),
        ('cinemark west dundee, il',     '',             'cinemark spring hill mall 8 + xd',         'Cinemark'),
        ('cinemark west dundee',         '',             'cinemark spring hill mall 8 + xd',         'Cinemark'),
        ('movies 8 ladson oakbrook ii',  '',             'cinemark movies summerville 8',             'Cinemark'),
        ('movies 8 ladson oakbrook ii',  'summerville',  'cinemark movies summerville 8',             'Cinemark'),
        ('movies 10',                    'bourbonnais',  'cinemark movies bourbonnais 10',            'Cinemark'),
        ('movies 10',                    '',             'cinemark movies bourbonnais 10',            'Cinemark'),
        ('cinemark louis joliet mall',   '',             'cinemark louis joliet mall 14',             'Cinemark'),
        ('deer park 16',                 '',             'cinemark century deer park 16',             'Cinemark'),
        ('deer park 16',                 'deer park',    'cinemark century deer park 16',             'Cinemark'),
        ('valparaiso commons shopping center', '',       'cinemark at valparaiso 12',                 'Cinemark'),
        ('cinemark seven bridges',       '',             'cinemark 7 bridges woodridge 16 imax',      'Cinemark'),
        ('cinemark seven bridges',       'woodridge',    'cinemark 7 bridges woodridge 16 imax',      'Cinemark'),
        # Cinemark Southeast / Midwest (Kathy Di circuit)
        ('cinemark bluffton',                  'bluffton',      'cinemark bluffton 12',                              'Cinemark'),
        ('cinemark bluffton',                  '',              'cinemark bluffton 12',                              'Cinemark'),
        ('cinemark at myrtle beach',           'myrtle beach',  'cinemark myrtle beach 14',                          'Cinemark'),
        ('cinemark at myrtle beach',           '',              'cinemark myrtle beach 14',                          'Cinemark'),
        ('cinemark boynton beach 14 and xd',   'boynton beach', 'cinemark boynton beach 14 + xd',                   'Cinemark'),
        ('cinemark boynton beach 14 and xd',   '',              'cinemark boynton beach 14 + xd',                   'Cinemark'),
        ('cinemark palace 20',                 'boca raton',    'cinemark palace 20',                               'Cinemark'),
        ('cinemark durbin park',               'st johns',      'cinemark durbin park 16',                          'Cinemark'),
        ('cinemark durbin park',               '',              'cinemark durbin park 16',                          'Cinemark'),
        ('cinemark atlantic north town center','jacksonville',   'cinemark atlantic north town center 16',           'Cinemark'),
        ('cinemark atlantic north town center','',              'cinemark atlantic north town center 16',            'Cinemark'),
        ('cinemark paradise 24',               'davie',         'cinemark paradise 24 + xd',                        'Cinemark'),
        ('cinemark paradise 24',               '',              'cinemark paradise 24 + xd',                        'Cinemark'),
        ('cinemark orlando and xd',            'orlando',       'cinemark festival bay orlando 20 + xd',            'Cinemark'),
        ('cinemark orlando and xd',            '',              'cinemark festival bay orlando 20 + xd',            'Cinemark'),
        ('universal cinemark at citywalk',     'orlando',       'universal cinemark at citywalk 20',                'Cinemark'),
        ('universal cinemark at citywalk',     '',              'universal cinemark at citywalk 20',                'Cinemark'),
        ('cinemark lakeland square mall 12',   'lakeland',      'cinemark lakeland square mall 12',                 'Cinemark'),
        ('cinemark lakeland square mall 12',   '',              'cinemark lakeland square mall 12',                 'Cinemark'),
        # cinemark melrose park resolves fine via direct name lookup (Rentrak 8526)
        # Regal
        ('fairfield stm 16 & imax',      '',            'regal edwards fairfield 16',            'Regal'),
        ('stockton cty ctr stm 16 & imax','',           'regal stockton city centre 16',         'Regal'),
        ('oviedo mall stm 22',           '',            'regal oviedo marketplace 22',           'Regal'),
        ('regal naples 4dx & imax',      '',            'regal hollywood cinema naples 20',      'Regal'),
        ('la habra stm 16',              '',            'regal la habra marketplace 16',         'Regal'),
        # ── Cinemark "Allie Fullmer" circuit (Theater # / Name (City, State) format) ──
        ('cinemark perkins rowe + xd',        'baton rouge',   'cinemark perkins rowe 16 + xd',                    'Cinemark'),
        ('tinseltown usa 15 + xd',            'beaumont',      'cinemark tinseltown usa beaumont 15 + xd',         'Cinemark'),
        ('cinemark 16',                        'gulfport',      'cinemark gulfport 16',                             'Cinemark'),
        ('movies 8',                           'tupelo',        'cinemark movies tupelo 8',                         'Cinemark'),
        ('cinemark monaco + xd',              'huntsville',    'cinemark monaco 16 & xd',                          'Cinemark'),
        ('cinemark tinseltown 17 + xd',       'pearl',         'cinemark tinseltown pearl 17 + xd',                'Cinemark'),
        ('cinemark 14 + xd',                  'lake charles',  'cinemark lake charles 14',                         'Cinemark'),
        ('cinemark movie bistro lake charles', '',             'cinemark movie bistro lake charles 9',              'Cinemark'),
        ('cinemark movie bistro lake charles', 'lake charles', 'cinemark movie bistro lake charles 9',              'Cinemark'),
        ('tinseltown 14',                     'benton',        'cinemark tinseltown usa benton 14',                'Cinemark'),
        ('cinemark towne centre + xd',        'conway',        'cinemark towne centre 12 + xd',                    'Cinemark'),
        ('cinemark towne centre + xd',        '',              'cinemark towne centre 12 + xd',                    'Cinemark'),
        ('colonel glenn 18 + xd',             '',              'cinemark colonel glenn 18 + xd',                   'Cinemark'),
        ('colonel glenn 18 + xd',             'little rock',   'cinemark colonel glenn 18 + xd',                   'Cinemark'),
        ('tinseltown 17',                     'west monroe',   'cinemark tinseltown west monroe 17',               'Cinemark'),
        ('cinemark tinseltown 17 + xd',       'shreveport',    'cinemark tinseltown shreveport 17 + xd',           'Cinemark'),
        ('cinemark 14',                       'texarkana',     'cinemark texarkana 14',                            'Cinemark'),
        ('lufkin movies 12',                  '',              'cinemark lufkin 12',                               'Cinemark'),
        ('lufkin movies 12',                  'lufkin',        'cinemark lufkin 12',                               'Cinemark'),
        ('cinemark harker heights 16',        'harker heights','cinemark harker heights 16',                       'Cinemark'),
        ('cinemark harker heights 16',        '',              'cinemark harker heights 16',                       'Cinemark'),
        ('cinemark temple & xd (12/20)',      'temple',        'cinemark temple 12 + xd',                         'Cinemark'),
        ('cinemark temple & xd',              'temple',        'cinemark temple 12 + xd',                         'Cinemark'),
        ('cinemark waco and xd',              'waco',          'cinemark cottonwood creek market waco 14',         'Cinemark'),
        ('cinemark waco and xd',              '',              'cinemark cottonwood creek market waco 14',         'Cinemark'),
        ('cinemark 14',                       'wichita falls', 'cinemark wichita falls 14',                        'Cinemark'),
        # ── Cinemark Pacific Northwest (THEATRE-header booking) ──────────────────
        ('lincoln square cinema with imax',   'bellevue',      'cinemark lincoln square cinemas imax 16',          'Cinemark'),
        ('lincoln square cinema with imax',   '',              'cinemark lincoln square cinemas imax 16',          'Cinemark'),
        ('lincoln square cinema bistro 6',    'bellevue',      'cinemark reserve lincoln square dine-in 6',        'Cinemark'),
        ('lincoln square cinema bistro 6',    '',              'cinemark reserve lincoln square dine-in 6',        'Cinemark'),
        ('cinemark totem lake + xd',          'kirkland',      'cinemark village at totem lake 8',                 'Cinemark'),
        ('cinemark totem lake + xd',          '',              'cinemark village at totem lake 8',                 'Cinemark'),
        ('century walla walla grand cinema 12','walla walla',  'cinemark walla walla grand cinema12',              'Cinemark'),
        ('century walla walla grand cinema 12','',             'cinemark walla walla grand cinema12',              'Cinemark'),
        # ── Cinemark Taylor Reynolds circuit (grayed-out venues) ─────────────────
        ('las vegas samstown 18',              'las vegas',     "cinemark century 18 sam's town (las vegas)",        'Cinemark'),
        ('las vegas samstown 18',              '',              "cinemark century 18 sam's town (las vegas)",        'Cinemark'),
        ('las vegas santa fe station 16 + xd', 'las vegas',    'cinemark century las vegas santa fe station 16 + xd', 'Cinemark'),
        ('las vegas santa fe station 16 + xd', '',             'cinemark century las vegas santa fe station 16 + xd', 'Cinemark'),
        ('sugarhouse movies 10',               'salt lake city','cinemark sugarhouse salt lake city 10',             'Cinemark'),
        ('sugarhouse movies 10',               '',              'cinemark sugarhouse salt lake city 10',             'Cinemark'),
        ('cinemark layton and xd',             'layton',        'cinemark layton 7 + xd',                           'Cinemark'),
        ('cinemark layton and xd',             '',              'cinemark layton 7 + xd',                           'Cinemark'),
        ('cinemark west valley + xd',          'west valley city', 'cinemark west valley 10 + xd',                  'Cinemark'),
        ('cinemark west valley + xd',          '',              'cinemark west valley 10 + xd',                     'Cinemark'),
        ('tucson park place 20 + xd',          'tucson',        'cinemark century park place 20 + xd',              'Cinemark'),
        ('tucson park place 20 + xd',          '',              'cinemark century park place 20 + xd',              'Cinemark'),
        ('century tucson marketplace and xd',  'tucson',        'cinemark century tucson marketplace  14+ xd',      'Cinemark'),
        ('century tucson marketplace and xd',  '',              'cinemark century tucson marketplace  14+ xd',      'Cinemark'),
        ('imperial valley 14',                 'el centro',     'cinemark century imperial valley mall 14 (elcentro)', 'Cinemark'),
        ('imperial valley 14',                 '',              'cinemark century imperial valley mall 14 (elcentro)', 'Cinemark'),
        # City-qualified "Cinemark 16" (ambiguous without city)
        ('cinemark 16',                        'mesa',          'cinemark mesa 16',                                 'Cinemark'),
        ('cinemark 16',                        'provo',         'cinemark provo 16',                                'Cinemark'),
        # Reno Parklane
        ('reno parklane 16',                   'reno',          'cinemark century park lane 16 (reno)',             'Cinemark'),
        ('reno parklane 16',                   '',              'cinemark century park lane 16 (reno)',             'Cinemark'),
        # CineLux city-only aliases
        ('cinelux - watsonville',              '',              'cinelux green valley watsonville 9',               'CineLux'),
        ('cinelux - scotts valley',            '',              'cinelux scotts valley cinema 10',                  'CineLux'),
        ('cinelux - morgan hill',              '',              'cinelux tennant station morgan hill 11',            'CineLux'),
        # Taylor Reynolds Cinemark aliases
        ('henderson 12',                       '',              'cinemark cinedome 12 (henderson)',                  'Cinemark'),
        ('henderson 12',                       'henderson',     'cinemark cinedome 12 (henderson)',                  'Cinemark'),
        ('cinemark 12',                        'american fork', 'cinemark american fork 12',                        'Cinemark'),
        # Landmark Kinjal Nagada aliases
        ('landmark 8 west kelowna',            '',              'landmark xtreme west kelowna 8 (163)',              'Landmark Cinemas (Canada)'),
        ('landmark 8 nanaimo',                 '',              'landmark nanaimo (avalon 8) (130)',                 'Landmark Cinemas (Canada)'),
        # Brad Bills city/state aliases
        ('new haven, mo',                      '',              'walt theatre new haven 1',                         ''),
        ('new haven',                          'new haven',     'walt theatre new haven 1',                         ''),
        ('lamar, mo',                          '',              'plaza lamar 1',                                    ''),
        ('lamar',                              'lamar',         'plaza lamar 1',                                    ''),
        ('borger, tx',                         '',              'morley borger 5',                                  ''),
        ('borger',                             'borger',        'morley borger 5',                                  ''),
        ('mountain grove, mo',                 '',              'fun city 5 cinemas',                               ''),
        ('mountain grove',                     'mountain grove','fun city 5 cinemas',                               ''),
    ]

    for booking_name, city, master_name, chain in SEED_ALIASES:
        upsert_alias(booking_name, master_name, city=city, chain=chain)

    print(f'[db] Seeded {len(SEED_ALIASES)} venue aliases')


def seed_aliases_if_empty():
    """Legacy function — now delegates to reseed_aliases for simplicity."""
    reseed_aliases()


def reseed_aliases():
    """
    Always upsert all seed aliases (safe to call repeatedly — uses INSERT OR REPLACE).
    Called on every launcher startup so new aliases added to code are
    automatically propagated to existing databases without manual migration.
    """
    _do_seed_aliases()


if __name__ == '__main__':
    init_db()
    seed_aliases_if_empty()
    print(f'Aliases in DB: {len(get_all_aliases())}')
    print(f'Master list rows: {get_master_list_count()}')
