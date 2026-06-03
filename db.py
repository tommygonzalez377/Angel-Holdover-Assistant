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
        # ── Clark Film Buying (Roy Wise circuit, bring-back booking, added 2026-05-12/13) ──
        # Buyers: Codi Kruse, Ken Kruse, Shayla Schuhriemen
        ('centennial theater',                 '',              'centennial sheridan 6',                            ''),
        ('fox 5 theater',                      '',              'fox sterling 5',                                   ''),
        ('picture show 12 - bn',               '',              'picture show at berlin',                           ''),
        ('picture show 6 - sa',                '',              'picture show main street',                         ''),
        ('picture show 6 - bl',                '',              'picture show bloomingdale court',                  ''),
        ('picture show 8 - cs',                '',              'picture show citadel crossing',                    ''),
        ('picture show 10 - ew',               '',              'picture show at east windsor',                     ''),
        ('picture show 11 - fr',               '',              'picture show at southcoast marketplace',           ''),
        ('picture show 7 - gj',                '',              'picture show at grand junction',                   ''),
        ('picture show 5 - ma',                '',              'picture show merchants exchange',                  ''),
        ('picture show 8 - me',                '',              'picture show superstition springs',                ''),
        ('picture show 10 - pr',               '',              'picture show frontier village',                    ''),
        ('studio city 10',                     '',              'studio city east',                                 ''),
        ('capitol stadium 12',                 '',              'studio city capitol cinema 16 + arq',              ''),
        ('cover 4 theater',                    '',              'cover ft morgan 4',                                ''),
        ('grand theater',                      '',              'grand theatre lander 1',                           ''),
        ('acme theater',                       '',              'acme theatre riverton 1',                          ''),
        ('montana theatre',                    '',              'montana theatre miles city 3',                     ''),
        ('movies 3',                           '',              'movies rawlins 3',                                 ''),
        ('ritz theater',                       '',              'ritz theatre thomaston 1',                         ''),
        ('studio city foothills',              '',              'studio city foothills gillette 6',                 ''),
        ('studio city uw plaza',               '',              'studio city uw',                                   ''),
        # ── Synced from venue_aliases.py CITY_VENUE_ALIASES (2026-06-03) ──
        # David Gundrum (Cinemark East/SE — ComScore "Theater #" format)
        ('tinseltown usa', 'rochester', 'Cinemark Tinseltown Cinema Rochester 16', 'Cinemark'),
        ('tinseltown usa', 'erie', 'Cinemark Tinseltown Erie 17', 'Cinemark'),
        ('tinseltown 14', 'oak ridge', 'Cinemark Tinseltown Oak Ridge 14', 'Cinemark'),
        ('tinseltown', 'salisbury', 'Cinemark Tinseltown Salisbury 14', 'Cinemark'),
        ('cinemark 16', 'somerdale', 'Cinemark Movies Somerdale 16 + XD', 'Cinemark'),
        ('cinemark 7', 'asheboro', 'Cinemark Asheboro 7', 'Cinemark'),
        ('movies 10', 'wilmington', 'Cinemark Movies Wilmington 10', 'Cinemark'),
        ('cinemark movies 10', 'ashland', 'Cinemark Town Cinema Ashland 10', 'Cinemark'),
        ('cinemark mccandless crossing', 'pittsburgh', 'Cinemark North Hills 12 + XD', 'Cinemark'),
        ('cinemark center township', 'monaca', 'Cinemark Monaca 12', 'Cinemark'),
        ('cinemark 10 bridgeport', 'bridgeport', 'Cinemark Meadowbrook Mall 10', 'Cinemark'),
        ('the carolina cinemark asheville', 'asheville', 'Cinemark Carolina Asheville 14', 'Cinemark'),
        ('cinemark city center', 'newport news', 'Cinemark City Center 12 + XD', 'Cinemark'),
        ('rave hazlet 12', '', 'Cinemark Hazlet 12', 'Cinemark'),
        ('cinemark christiana newark, de', '', 'Cinemark Christiana 12 + XD', 'Cinemark'),
        ('cinemark hill country galleria 14', 'bee cave', 'Cinemark Hill Country Bee Cave 14', 'Cinemark'),
        ('cinemark mccreless mall 10', 'san antonio', 'Cinemark Mccreless San Antonio 10', 'Cinemark'),
        ('309 cinema 9', '', 'AMC 309 Cinemas North Wales 9', 'AMC'),
        ('aberdeen', '', 'Golden Ticket Cinemas Aberdeen 5', ''),
        ('academy 8', '', 'AMC Academy Greenbelt 8', 'AMC'),
        ('alderwood 16', '', 'AMC Alderwood Lynnwood 16', 'AMC'),
        ('ale house', '', 'Golden Ticket Cinemas Greensboro Ale House 10', ''),
        ('ale house plf', '', 'Golden Ticket Cinemas Greensboro Ale House 10', ''),
        ('aliante stm 16 & imax', '', 'Regal Aliante N Las Vegas 16', 'Regal'),
        ('alliston', '', 'imagine cinemas alliston', ''),
        ('arrowhead town center 14', '', 'AMC Arrowhead 14', 'AMC'),
        ('auburn stm 17', '', 'Auburn Stadium 17', ''),
        ('aventura mall 24', '', 'AMC Aventura 24 & IMAX', 'AMC'),
        ('avenue 16', '', 'AMC Avenue Melbourne 16', 'AMC'),
        ('aviation 12', '', 'AMC Aviation Linden 12', 'AMC'),
        ('aviation mall 9', '', 'Aviation Mall Queensbury 9', ''),
        ('barkley vlg stm 16 imax & rpx', '', 'Regal Barkley Village Bellingham 16 IMAX & RPX', 'Regal'),
        ('battle ground', '', 'Battle Ground Cinema 8', ''),
        ('battlefield 10', '', 'AMC CLASSIC Battlefield Ft Oglethorpe 10', 'AMC'),
        ('bayou 15', '', 'AMC Bayou Pensacola 15 & IMAX', 'AMC'),
        ('bella bottega stm 11', '', 'Regal Bella Bottega Redmond 11', 'Regal'),
        ('belltower stm 20', '', 'Regal Belltower Ft Myers 20', 'Regal'),
        ('benton harbor 14 + dbox', '', 'Celebration! Benton Harbor', ''),
        ('berkshire 8', '', 'AMC Berkshire Wyomissing 8', 'AMC'),
        ('birkdale stm 16 & rpx', '', 'Regal Birkdale Huntersville 16', 'Regal'),
        ('bloomington', '', 'Golden Ticket Bloomington Ale House 10', ''),
        ('bluefield', '', 'Golden Ticket Cinemas Bluefield 8', ''),
        ('boise stm 22 & imax', '', 'Regal Edwards Boise 21 ScreenX, 4DX & IMAX', 'Regal'),
        ('borger', 'tx', 'morley borger 5', ''),
        ('borger tx', '', 'morley borger 5', ''),
        ('boulder station stm 11', '', 'Regal Boulder Station Las Vegas 11', 'Regal'),
        ('brazos 14', '', 'AMC Brazos Stadium Lake Jackson 14', 'AMC'),
        ('bremerton', '', 'SEEfilm Cinema', ''),
        ('bridgeport stm 18 & imax', '', 'Bridgeport Tigard 18', ''),
        ('burlington', '', 'cine starz burlington', ''),
        ('canby', '', 'Canby Cinema 8', ''),
        ('carlsbad 12', '', 'Regal Carlsbad 12', 'Regal'),
        ('cascade stm 16 imax & rpx', '', 'Cascade Vancouver 16', ''),
        ('castleton square 14', '', 'AMC Castleton Indianapolis 14', 'AMC'),
        ('century 16 + imax', 'corpus christi', 'cinemark century corpus christi 16 + xd and imax', 'Cinemark'),
        ('century arden and xd', '', 'cinemark century arden + xd 14', 'Cinemark'),
        ('century arden and xd', 'sacramento', 'cinemark century arden + xd 14', 'Cinemark'),
        ('century at hayward', '', 'cinemark century at hayward 12', 'Cinemark'),
        ('century at hayward', 'hayward', 'cinemark century at hayward 12', 'Cinemark'),
        ('century el con + xd', '', 'cinemark century 20 el con and xd (tucson)', 'Cinemark'),
        ('century el con + xd', 'tucson', 'cinemark century 20 el con and xd (tucson)', 'Cinemark'),
        ('century la quinta + xd', 'la quinta', 'cinemark century la quinta 12 + xd', 'Cinemark'),
        ('century marina + xd', '', 'cinemark century marina + xd 5', 'Cinemark'),
        ('century marina + xd', 'marina', 'cinemark century marina + xd 5', 'Cinemark'),
        ('champlain centre stm 8', '', 'Champlain Plattsburgh 8', ''),
        ('cinema 12', '', 'Malco Olive Branch Cinema 12', 'Malco'),
        ('cinema 12', 'olive branch', 'Malco Olive Branch Cinema 12', 'Malco'),
        ('cinema 16', 'fort smith', 'Malco Fort Smith 16', 'Malco'),
        ('cinema 16', 'ft smith', 'Malco Fort Smith 16', 'Malco'),
        ('cinema 99 stm 11', '', 'Cinema 99 Vancouver', ''),
        ('cinema carousel 16', '', 'Celebration! Cinema Carousel', ''),
        ('cinemark (the legacy)', '', 'cinemark legacy 24 + xd', 'Cinemark'),
        ('cinemark (the legacy)', 'plano', 'cinemark legacy 24 + xd', 'Cinemark'),
        ('cinemark 12', 'greeley', 'cinemark greeley 12', 'Cinemark'),
        ('cinemark 12', 'rosenberg', 'cinemark rosenberg 12', 'Cinemark'),
        ('cinemark 12', 'victoria', 'cinemark victoria 12', 'Cinemark'),
        ('cinemark 12', 'zanesville', 'cinemark colony square mall', 'Cinemark'),
        ('cinemark 12 + xd', 'cypress', 'cinemark cypress 12 + xd', 'Cinemark'),
        ('cinemark 12 + xd', 'pearland', 'cinemark pearland 12 + xd', 'Cinemark'),
        ('cinemark 12 and xd', 'los angeles', 'cinemark 12 howard hughes la and xd', 'Cinemark'),
        ('cinemark 14', 'mansfield', 'cinemark mansfield 14', 'Cinemark'),
        ('cinemark 14', 'round rock', 'cinemark round rock 14', 'Cinemark'),
        ('cinemark 14', 'strongsville', 'cinemark strongsville 14', 'Cinemark'),
        ('cinemark 15 + xd', 'hadley', 'cinemark hadley 15 + xd', 'Cinemark'),
        ('cinemark 16', 'fort collins', 'cinemark fort collins 16', 'Cinemark'),
        ('cinemark 16', 'palmdale', 'cinemark antelope valley mall palmdale 16', 'Cinemark'),
        ('cinemark 16 + xd', 'brownsville', 'cinemark brownsville 16 + xd', 'Cinemark'),
        ('cinemark 16 + xd', 'harlingen', 'cinemark harlingen 16 + xd', 'Cinemark'),
        ('cinemark 17', 'springfield', 'cinemark springfield 17', 'Cinemark'),
        ('cinemark 18 + xd', 'webster', 'cinemark webster 18 + xd', 'Cinemark'),
        ('cinemark 19 + xd', 'katy', 'cinemark katy 19 + xd', 'Cinemark'),
        ('cinemark 24 + xd', 'west jordan', 'cinemark west jordan 24+xd', 'Cinemark'),
        ('cinemark 7', 'eagle pass', 'cinemark eagle pass 7', 'Cinemark'),
        ('cinemark abilene and xd', '', 'cinemark abilene 12', 'Cinemark'),
        ('cinemark abilene and xd', 'abilene', 'cinemark abilene 12', 'Cinemark'),
        ('cinemark allen 16 and xd', '', 'cinemark allen 16 + xd', 'Cinemark'),
        ('cinemark allen 16 and xd', 'allen', 'cinemark allen 16 + xd', 'Cinemark'),
        ('cinemark draper + xd', '', 'cinemark draper 12 + xd', 'Cinemark'),
        ('cinemark draper + xd', 'draper', 'cinemark draper 12 + xd', 'Cinemark'),
        ('cinemark farmington + xd', '', 'cinemark farmington 14+ xd', 'Cinemark'),
        ('cinemark farmington + xd', 'farmington', 'cinemark farmington 14+ xd', 'Cinemark'),
        ('cinemark paducah', '', 'cinemark paducah 12', 'Cinemark'),
        ('cinemark paducah', 'paducah', 'cinemark paducah 12', 'Cinemark'),
        ('cinemark palace 20', '', 'cinemark palace 20 + xd', 'Cinemark'),
        ('cinemark riverton + xd', '', 'cinemark riverton ridgewood 14 +xd', 'Cinemark'),
        ('cinemark riverton + xd', 'riverton', 'cinemark riverton ridgewood 14 +xd', 'Cinemark'),
        ('cinemark roanoke 14', '', 'cinemark roanoke 14 + xd', 'Cinemark'),
        ('cinemark roanoke 14', 'roanoke', 'cinemark roanoke 14 + xd', 'Cinemark'),
        ('cinemark rockwall 14 and xd', '', 'cinemark rockwall 14 + xd', 'Cinemark'),
        ('cinemark rockwall 14 and xd', 'rockwall', 'cinemark rockwall 14 + xd', 'Cinemark'),
        ('cinemark san antonio 16', 'san antonio', 'cinemark movies san antonio 16', 'Cinemark'),
        ('cinemark spanish fork + xd', '', 'cinemark spanish fork 8+xd', 'Cinemark'),
        ('cinemark spanish fork + xd', 'spanish fork', 'cinemark spanish fork 8+xd', 'Cinemark'),
        ('cinemark tinseltown + xd', 'louisville', 'cinemark tinseltown louisville + xd', 'Cinemark'),
        ('cinemark towson + xd', '', 'cinemark towson 15 + xd', 'Cinemark'),
        ('cinemark towson + xd', 'towson', 'cinemark towson 15 + xd', 'Cinemark'),
        ('cinemark west dundee', 'il', 'cinemark spring hill mall 8 + xd', 'Cinemark'),
        ('cinemark west plano', '', 'cinemark movies plano 10', 'Cinemark'),
        ('cinemark west plano', 'plano', 'cinemark movies plano 10', 'Cinemark'),
        ('cinestars', '', 'Hood River Cinemas 5', ''),
        ('city center stm 12', '', 'City Vancouver 12', ''),
        ('clarion', '', 'Golden Ticket Clarion 5', ''),
        ('clarksville stm 16 & rpx', '', 'Regal Clarksville 16', 'Regal'),
        ('cloquet', '', 'Premiere Cloquet 6', ''),
        ('coast', '', 'Coast Fort Bragg 4', ''),
        ('coldwater crossing stm 14', '', 'Regal Coldwater Fort Wayne 14', 'Regal'),
        ('college station + xd', 'college station', 'cinemark college station 18 + xd', 'Cinemark'),
        ('collierville', '', 'Malco Towne Collierville 16', 'Malco'),
        ('collierville', 'memphis', 'Malco Towne Collierville 16', 'Malco'),
        ('colonial 18', '', 'AMC Colonial Lawrenceville 18', 'AMC'),
        ('columbia mall 14', '', 'AMC Columbia Maryland 14', 'AMC'),
        ('coral ridge 10', '', 'AMC Coral Ridge Ft Lauderdale 10', 'AMC'),
        ('corpus 16', '', 'AMC Corpus Christi 16', 'AMC'),
        ('cote des nieges', '', 'cine starz cote-des-neiges 7', ''),
        ('cote-des-nieges', '', 'cine starz cote-des-neiges 7', ''),
        ('crossroads 15 + imax', '', 'Celebration! Crossroads & Imax', ''),
        ('cut! by cinemark cypress', '', 'Cinemark Cut 8!', 'Cinemark'),
        ('cut! by cinemark cypress', 'cypress', 'Cinemark Cut 8!', 'Cinemark'),
        ('danville stadium 12', '', 'GTC Danville 12', ''),
        ('decatur 10', '', 'AMC CLASSIC Decatur 10', 'AMC'),
        ('deerfield twn ctr stm 16 & rpx', '', 'Regal Deerfield Towne Center Mason 16', 'Regal'),
        ('deluxe longueuil', '', 'cinestarz longueuil 14', ''),
        ('deluxe taschereau', '', 'cine starz taschereau 12', ''),
        ('destiny usa stm 19 imax & rpx', '', 'Regal Destiny Mall Cinema Syracuse 17', 'Regal'),
        ('dickinson', '', 'Golden Ticket Dickinson 3', ''),
        ('dothan pavilion 12', '', 'AMC CLASSIC Dothan Pavillion 12', 'AMC'),
        ('dublin', '', 'Golden Ticket Cinemas Dublin 6', ''),
        ('dubois', '', 'Golden Ticket Cinemas DuBois 5', ''),
        ('e. greenbush 8', '', 'East Greenbush 8', ''),
        ('eastchase 9', '', 'AMC Eastchase Ft Worth 9', 'AMC'),
        ('eastridge 15', '', 'AMC Eastridge Mall San Jose 15 & IMAX', 'AMC'),
        ('el dorado stm 14 & imax', '', 'Regal El Dorado Hills 14', 'Regal'),
        ('epic', '', 'Apex Muskogee 6', ''),
        ('epic', 'muskogee', 'Apex Muskogee 6', ''),
        ('escondido stm 16 & imax', '', 'Regal Escondido Stadium 16', 'Regal'),
        ('espanola', 'nm', 'dreamcatcher 10', 'AMC'),
        ('espanola nm', '', 'dreamcatcher 10', 'AMC'),
        ('esplanade 14', '', 'AMC DINE-IN Esplanade 14', 'AMC'),
        ('evans 14', '', 'GTC Evans 14', ''),
        ('everett stm 16 & rpx', '', 'Regal Everett Stadium 16 & RPX', 'Regal'),
        ('factoria 8', '', 'AMC Factoria Bellevue 8', 'AMC'),
        ('fairground 10', '', 'AMC Fairgrounds 10', 'AMC'),
        ('firewheel town center 18', '', 'AMC Firewheel Garland 18', 'AMC'),
        ('flatiron crossing 14', '', 'AMC Flatiron Broomfield 14', 'AMC'),
        ('florence', 'sc', 'julia florence 4', ''),
        ('florence sc', '', 'julia florence 4', ''),
        ('foothills 15', '', 'AMC Foothills Tucson 15', 'AMC'),
        ('forum 30', '', 'AMC Forum Sterling Heights 17', 'AMC'),
        ('fountains 18', '', 'AMC Fountains Stafford 18 & IMAX', 'AMC'),
        ('fox stm 16 & imax', '', 'Regal Fox Ashburn 16', 'Regal'),
        ('franklin square stm 14', '', 'Regal Franklin Square Gastonia 14', 'Regal'),
        ('freehold 14', '', 'AMC Freehold Metroplex 14', 'AMC'),
        ('fulton', 'mo', 'fulton cinema 8', ''),
        ('fulton mo', '', 'fulton cinema 8', ''),
        ('galaxy stm 14', '', 'Regal Galaxy Indianapolis 14', 'Regal'),
        ('galewood 14', '', 'AMC Galewood Crossings 14', 'AMC'),
        ('gateway 7', '', 'GTC Gateway 7', ''),
        ('goshen', '', 'linway plaza goshen 14', ''),
        ('goshen', 'in', 'linway plaza goshen 14', ''),
        ('grand rapids north 17 + imax', '', 'Celebration! Cinema GR North', ''),
        ('grand rapids south 15 + c premium', '', 'Celebration! South', ''),
        ('gratiot 15', '', 'AMC Star Gratiot Clinton Township 15', 'AMC'),
        ('greenville', '', 'Golden Ticket Cinemas Greenville Grande 14', ''),
        ('greenville plf', '', 'Golden Ticket Cinemas Greenville Grande 14', ''),
        ('greenwood stm 14 & rpx', '', 'Regal Greenwood 14', 'Regal'),
        ('guymon', 'ok', 'northridge guymon 8', ''),
        ('guymon ok', '', 'northridge guymon 8', ''),
        ('hacienda stm 20 imax & rpx', '', 'Regal Hacienda Crossings Dublin 20 & IMAX', 'Regal'),
        ('hadley theatre stm 16', '', 'Regal Hadley Cinemas South Plainfield 16', 'Regal'),
        ('hampton 24', '', 'AMC Hampton Towne Centre 24', 'AMC'),
        ('hanes 12', '', 'AMC Hanes Winston Salem 12', 'AMC'),
        ('harrison', '', 'Golden Ticket Cinemas Harrison 8', ''),
        ('hastings', '', 'Golden Ticket Cinemas Hastings 3', ''),
        ('hawthorn 12', '', 'AMC Hawthorn Vernon Hills 12', 'AMC'),
        ('hollywood usa 20', 'pasadena', 'cinemark hollywood pasadena 20', 'Cinemark'),
        ('hooky entertainment + sdx + imax', '', 'Hooky Entertainment + SDX + IMAX Hutto 8', ''),
        ('hooky entertainment + sdx + imax', 'hutto', 'Hooky Entertainment + SDX + IMAX Hutto 8', ''),
        ('houma palace 10', '', 'AMC Houma 10 *TEMP 9*', 'AMC'),
        ('independence', '', 'Independence Cinema 8', ''),
        ('independence', 'mo', 'pharaoh independence 4', ''),
        ('independence 20', '', 'AMC Independence Commons 20', 'AMC'),
        ('independence mo', '', 'pharaoh independence 4', ''),
        ('island 7', '', 'GTC Island 7', ''),
        ('issaquah stm 12 imax & rpx', '', 'Regal Issaquah Highland IMAX & RPX 12', 'Regal'),
        ('jamestown', '', 'Bison 6 Cinema', ''),
        ('jerseyville', '', 'the stadium theater 3', ''),
        ('jerseyville', 'il', 'the stadium theater 3', ''),
        ('john r 15', '', 'AMC John R Theatre 15', 'AMC'),
        ('jonesboro towne', '', 'Malco Jonesboro Towne Cinema', 'Malco'),
        ('jonesboro towne', 'jonesboro', 'Malco Jonesboro Towne Cinema', 'Malco'),
        ('kearney', '', 'Golden Ticket Cinemas Hilltop 4', ''),
        ('kendall vlg stm 16 imax & rpx', '', 'Regal Kendall Village Miami 16', 'Regal'),
        ('kingstowne stm 16 & rpx', '', 'Regal Kingstowne Cinema 16', 'Regal'),
        ('kirksville', 'mo', 'downtown kirksville 8', ''),
        ('kirksville mo', '', 'downtown kirksville 8', ''),
        ('lakeline 9', '', 'AMC Lakeline Mall Cedar Park 9', 'AMC'),
        ('lakeshore windsor', '', 'imagine cinemas lakeshore', ''),
        ('lamar', 'mo', 'plaza lamar 1', ''),
        ('lamar mo', '', 'plaza lamar 1', ''),
        ('landmark 8', '', 'AMC Landmark 8', 'AMC'),
        ('lansing 19 + c premium xl', '', 'Celebration! Lansing & Imax', ''),
        ('lansing mall stm 12 & rpx', '', 'Regal Lansing Mall 12', 'Regal'),
        ('laurel towne centre 12', '', 'Regal Laurel Town Center 12', 'Regal'),
        ('legends 14', '', 'AMC Legends Kansas City 14', 'AMC'),
        ('lenoir', '', 'Golden Ticket Cinemas Twin 2', ''),
        ('levittown 10', '', 'AMC DINE-IN Levittown 10', 'AMC'),
        ('liberty 9', '', 'GTC Liberty 9', ''),
        ('litchfield', '', 'westside litchfield 3', ''),
        ('litchfield', 'il', 'westside litchfield 3', ''),
        ('living room indy', '', 'Living Room Theaters Indianapolis', ''),
        ('living room pdx', '', 'Living Room Theatres Portland', ''),
        ('london', '', 'imagine cinemas london', ''),
        ('loudoun 11', '', 'AMC Loudoun Station Ashburn 11', 'AMC'),
        ('lumberton', 'nc', 'hometown lumberton 4', ''),
        ('lumberton nc', '', 'hometown lumberton 4', ''),
        ('luverne', '', 'Verne Drive-in Luverne 1', ''),
        ('madisonville', '', 'Golden Ticket Cinemas Capitol 8', ''),
        ('main place 6', '', 'cinemark movies 6', 'Cinemark'),
        ('main place 6', 'mcallen', 'cinemark movies 6', 'Cinemark'),
        ('malco cinema 8', '', 'Malco Columbus 8', 'Malco'),
        ('malco cinema 8', 'columbus', 'Malco Columbus 8', 'Malco'),
        ('mall 7', '', 'GTC Mall Cinemas 7', ''),
        ('manor stm 16', '', 'Regal Manor Cinema Lancaster 16', 'Regal'),
        ('marion', 'nc', 'hometown cinemas marion 2', ''),
        ('marion nc', '', 'hometown cinemas marion 2', ''),
        ('marketfair 10', '', 'AMC Marketfair Princeton 10', 'AMC'),
        ('marlton 8', '', 'AMC Marlton Cinemas 8', 'AMC'),
        ('marshall', 'mo', 'cinema marshall 3', ''),
        ('marshall mo', '', 'cinema marshall 3', ''),
        ('martin village stm 16 & imax', '', 'Regal Martin Village Lacey 16', 'Regal'),
        ('marysville 14', '', 'Regal Marysville 14', 'Regal'),
        ('mayfair 18', '', 'AMC Mayfair Mall Wauwatosa 18', 'AMC'),
        ('mercado 20', '', 'AMC Mercado Santa Clara 20 & IMAX', 'AMC'),
        ('meridian', '', 'Golden Ticket Cinemas Meridian 6', ''),
        ('meridian 16', '', 'Regal Meridian 16', 'Regal'),
        ('mesa grand 14', '', 'AMC Mesa Grande 14', 'AMC'),
        ('methuen 20', '', 'AMC Methuen at the Loop 20', 'AMC'),
        ('metreon 16', '', 'AMC Metreon San Francisco 16 & IMAX', 'AMC'),
        ('middlesboro', '', 'Golden Ticket Cinemas Middlesboro 4', ''),
        ('milford', 'ia', 'pioneer milford 1', ''),
        ('milford ia', '', 'pioneer milford 1', ''),
        ('milwaukie', '', 'Milwaukie Portland 2', ''),
        ('minot', '', 'Oak Park Theater 1', ''),
        ('mira mesa stm 18 & imax & rpx', '', 'Regal Mira Mesa San Diego 18', 'Regal'),
        ('mj capital center 12', '', 'AMC Magic Johnson Capital Center 12', 'AMC'),
        ('moultrie stadium 6 cinemas', '', 'GTC Moultrie 6', ''),
        ('mountain cinemas 8', '', 'GTC Mountain Cinemas 8', ''),
        ('mountain grove', '', 'fun city 5 cinemas', ''),
        ('mountain grove', 'mo', 'fun city 5 cinemas', ''),
        ('mountain grove mo', '', 'fun city 5 cinemas', ''),
        ('moviehouse & eatery mckinney 10', '', 'Moviehouse & Eatery Mc Kinney 10 (TX)', ''),
        ('movies 16', 'gahanna', 'cinemark stoneridge plaza movies 16', 'Cinemark'),
        ('movies 8', 'del rio', 'cinemark movies del rio 8', 'Cinemark'),
        ('movies 9', '', 'Malco Cinema Winchester 9', 'Malco'),
        ('movies 9', 'winchester', 'Malco Cinema Winchester 9', 'Malco'),
        ('movies on tv stm 16', '', 'Movies Hillsboro 16', ''),
        ('mt vernon 8', '', 'AMC CLASSIC Mount Vernon 8', 'AMC'),
        ('mt. pleasant 11', '', 'Celebration! Mt. Pleasant', ''),
        ('nappanee theatre', '', 'nappanee theatre 1', ''),
        ('natomas mktplace stm 16 & rpx', '', 'Natomas Marketplace 16', ''),
        ('new river valley stm 14 & rpx', '', 'Regal New River Valley Christiansburg 14', 'Regal'),
        ('newport centre 11', '', 'AMC Newport Centre Jersey City 11', 'AMC'),
        ('nitro stm 12', '', 'Regal Nitro 12', 'Regal'),
        ('north platte', '', 'Golden Ticket Cinemas Platte River 6', ''),
        ('northpark 15', '', 'AMC North Park Dallas 15 & IMAX', 'AMC'),
        ('northrock 14', '', 'AMC Northrock Wichita 14', 'AMC'),
        ('norton', 'ks', 'norton theatre', ''),
        ('norton ks', '', 'norton theatre', ''),
        ('oak grove', '', 'Oak Grove Portland 8', ''),
        ('oceanside stm 16', '', 'Regal Oceanside Stadium 16', 'Regal'),
        ('onamia', '', 'Grand Makwa Cinema Onamia 4', ''),
        ('orange 30', '', 'AMC Block Orange 30 & IMAX', 'AMC'),
        ('orchard 12', '', 'AMC Orchard Town Center Westminster 12', 'AMC'),
        ('ox commons', '', 'Malco Oxford Commons 8', 'Malco'),
        ('ox commons', 'oxford', 'Malco Oxford Commons 8', 'Malco'),
        ('palisades 21', '', 'AMC Palisades Center 21', 'AMC'),
        ('palmetto grande stm 16', '', 'Regal Palmetto Grande Mt Pleasant 16', 'Regal'),
        ('parkway plaza stm 18 & imax', '', 'Regal Parkway Plaza El Cajon 18', 'Regal'),
        ('parkway pointe 15', '', 'AMC Parkway Point Atlanta 15', 'AMC'),
        ('parsons', 'ks', 'the parsons theatre', ''),
        ('parsons ks', '', 'the parsons theatre', ''),
        ('pavilion stm 14 & rpx', '', 'Regal Pavilion Port Orange 14', 'Regal'),
        ('pensacola 18', '', 'AMC CLASSIC Pensacola 18', 'AMC'),
        ('peru 8', '', 'AMC Peru Mall 8', 'AMC'),
        ('plainville 20', '', 'AMC Plainville Cinema 20', 'AMC'),
        ('pooler stadium 14 w/gtx', '', 'GTC Pooler 14', ''),
        ('poulsbo stm 10', '', 'Regal Poulsbo 10', 'Regal'),
        ('prairiefire 17', '', 'AMC DINE-IN Prairie Fire 17', 'AMC'),
        ('promenade mall', '', 'imagine cinemas promenade 6', ''),
        ('quail springs mall 24', '', 'AMC Quail Springs Oklahoma City 24 & IMAX', 'AMC'),
        ('rancho del rey stm 16', '', 'Regal Rancho Del Rey Chula Vista 16', 'Regal'),
        ('rancho san diego stm 15', '', 'Regal Rancho San Diego 15', 'Regal'),
        ('randhurst 12', '', 'AMC Randhurst Mount Prospect 12', 'AMC'),
        ('rapid city', '', 'Golden Ticket Cinemas Rushmore 7', ''),
        ('razorback 16', '', 'Malco Razorback Cinema Grill & IMAX', 'Malco'),
        ('razorback 16', 'fayetteville', 'Malco Razorback Cinema Grill & IMAX', 'Malco'),
        ('red rock stm 16 & imax', '', 'Regal Red Rock Las Vegas 16', 'Regal'),
        ('redstone 14 cinemas', '', 'Red Stone 14 Cinemas', ''),
        ('redstone 14 cinemas w/pdx', '', 'Red Stone 14 Cinemas', ''),
        ('reg king of prussia 4dx & imax', '', 'Regal King Of Prussia 16', 'Regal'),
        ('regal bistro at the falls', '', 'Regal Falls Miami 12', 'Regal'),
        ('regal dania point 4dx rpx & vip', '', 'Regal Dania Pointe 16', 'Regal'),
        ('regal dania point 4dx rpx &vip', '', 'Regal Dania Pointe 16', 'Regal'),
        ('regal mission marketplace rpx', '', 'Regal Mission Marketplace 20', 'Regal'),
        ('regency 20', '', 'AMC Regency Sq Brandon 20 & IMAX', 'AMC'),
        ('regency 24', '', 'AMC Regency Sq Jacksonville 24 & IMAX', 'AMC'),
        ('regency square 8', '', 'Epic Regency Cinema Stuart 8', ''),
        ('rensselaer', '', 'fountain stone theaters rensselaer 5', ''),
        ('rensselaer', 'in', 'fountain stone theaters rensselaer 5', ''),
        ('rhinelander', '', 'Rouman Cinema Rhinelander 6', ''),
        ('rivercenter 11', '', 'AMC Rivercenter San Antonio 9', 'AMC'),
        ('rivertown 13 + c premium', '', 'Celebration! Cinema Rivertown', ''),
        ('riverview 14', '', 'AMC Riverview Gibsonton 14', 'AMC'),
        ('riverwatch', '', 'GTC Riverwatch Cinemas 12', ''),
        ('roseburg', '', 'Roseburg Cinema', ''),
        ('roxy', '', 'Roxy Dickson 8', ''),
        ('roxy', 'dickson', 'Roxy Dickson 8', ''),
        ('royal palm beach stm 18 & rpx', '', 'Regal Royal Palm Plaza Royal Palm Beach 12', 'Regal'),
        ('san marcos stm 18', '', 'Regal San Marcos 18', 'Regal'),
        ('sandhill stm 16 imax & rpx', '', 'Regal Sandhill Columbia 14', 'Regal'),
        ('sandy', '', 'Sandy Cinema 8', ''),
        ('santiam stm 11', '', 'Santiam Salem 11', ''),
        ('saratoga 14', '', 'AMC Saratoga San Jose 14 & IMAX', 'AMC'),
        ('scappoose', '', 'Scappoose Cinema 7', ''),
        ('scottsbluff', '', 'Golden Ticket Cinemas Reel Lux 6 *temp 4*', ''),
        ('shawnee', '', 'Golden Ticket Cinemas Shawnee 6', ''),
        ('shawnee plf', '', 'Golden Ticket Cinemas Shawnee 6', ''),
        ('sierra vista 10', '', 'cinemark sierra vista 10', 'Cinemark'),
        ('sierra vista 10', 'sierra vista', 'cinemark sierra vista 10', 'Cinemark'),
        ('sikes 10', '', 'AMC Sikes Senter 10', 'AMC'),
        ('sioux falls', '', 'West Mall 7 Theatres', ''),
        ('smithfield cinemas 10', '', 'GTC Smithfield 10', ''),
        ('southaven', '', 'Malco Desoto Southaven 16', 'Malco'),
        ('southaven', 'memphis', 'Malco Desoto Southaven 16', 'Malco'),
        ('southcenter 16', '', 'AMC Southcenter Tukwila 16 & IMAX', 'AMC'),
        ('southdale center 16', '', 'AMC Southdale Edina 16', 'AMC'),
        ('southern hill 12', '', 'AMC Southern Hills 12', 'AMC'),
        ('southgate 9', '', 'AMC DINE-IN Southgate 9', 'AMC'),
        ('southlake pavilion 24', '', 'AMC Southlake Morrow 23 & IMAX', 'AMC'),
        ('southlands 16', '', 'AMC Southlands Aurora 16', 'AMC'),
        ('southpark meadows 14', '', 'cinemark southpark mall austin 14', 'Cinemark'),
        ('southpark meadows 14', 'austin', 'cinemark southpark mall austin 14', 'Cinemark'),
        ('southroads 20', '', 'AMC Southroads Tulsa 20', 'AMC'),
        ('spartan stm 16', '', 'Regal Spartan Spartanburg 16', 'Regal'),
        ('spooner', '', 'Palace Spooner 2', ''),
        ('springdale', '', 'Malco Springdale Cinema Grill', 'Malco'),
        ('springdale', 'springdale', 'Malco Springdale Cinema Grill', 'Malco'),
        ('springfield 11', '', 'AMC Springfield 11', 'AMC'),
        ('springfield 12', '', 'AMC CLASSIC Springfield 12 with IMAX', 'AMC'),
        ('springfield 8', '', 'AMC Springfield 8', 'AMC'),
        ('st charles towne center 9', '', 'AMC St. Charles Waldorf 9', 'AMC'),
        ('st laurent', '', 'cine starz st laurent centre', ''),
        ('st. clairsville', '', 'Golden Tickets St. Clairsville 5', ''),
        ('st. laurent', '', 'cine starz st laurent centre', ''),
        ('stage', '', 'Malco Stage Cinema Bartlett', 'Malco'),
        ('stage', 'memphis', 'Malco Stage Cinema Bartlett', 'Malco'),
        ('stark street stm 10', '', 'Stark Gresham 10', ''),
        ('stone hill town center', '', 'Cinemark Stone Hill Town Ctr Pflugerville 9 *TEMP 5*', 'Cinemark'),
        ('stone hill town center', 'pflugerville', 'Cinemark Stone Hill Town Ctr Pflugerville 9 *TEMP 5*', 'Cinemark'),
        ('stonebriar mall 24', '', 'AMC Stonebriar Frisco 24 & IMAX', 'AMC'),
        ('stonecrest stm 22 imax & rpx', '', 'Regal Stonecrest @ Piper Glen Charlotte 22', 'Regal'),
        ('stones river 9', '', 'AMC Stone River 9', 'AMC'),
        ('stonybrook 20', '', 'AMC Stonybrook Louisville 20', 'AMC'),
        ('studio 28', '', 'AMC Studio Olathe 30', 'AMC'),
        ('sundial 12', '', 'AMC Sundial St Petersburg', 'AMC'),
        ('sunset place 24', '', 'AMC Sunset S Miami 24 & IMAX', 'AMC'),
        ('sunset station stm 13 & imax', '', 'Regal Sunset Station Henderson 13', 'Regal'),
        ('surprise 14', '', 'AMC Surprise Pointe 14', 'AMC'),
        ('tallahassee 20', '', 'AMC Tallahassee Mall 20 & IMAX', 'AMC'),
        ('the landing stm 14 & rpx', '', 'Regal The Landing Renton 14 & RPX', 'Regal'),
        ('thornton place stm 14 & imax', '', 'Regal Thornton Place Seattle 14 & IMAX', 'Regal'),
        ('thoroughbred 20', '', 'AMC Thoroughbred Franklin 20 & IMAX', 'AMC'),
        ('tinseltown 17 + xd', 'jacinto city', 'cinemark tinseltown jacinto city 17 + xd', 'Cinemark'),
        ('tinseltown 17 + xd', 'the woodlands', 'cinemark the woodlands 17 + xd', 'Cinemark'),
        ('tinseltown 20 + xd', 'pflugerville', 'cinemark tinseltown pflugerville 20 + xd', 'Cinemark'),
        ('tinseltown 290 16 + xd', 'houston', 'cinemark tinseltown 290 houston 16 + xd', 'Cinemark'),
        ('tinseltown usa + xd', 'north canton', 'cinemark tinseltown n canton 24+ xd', 'Cinemark'),
        ('town square 18', '', 'AMC Town Square Las Vegas 18 & IMAX', 'AMC'),
        ('tulsa', 'ok', 'eton tulsa 6', ''),
        ('tulsa 12', '', 'AMC CLASSIC Tulsa Hills 12', 'AMC'),
        ('tulsa ok', '', 'eton tulsa 6', ''),
        ('tup commons', '', 'Malco Tupelo Commons 10', 'Malco'),
        ('tup commons', 'tupelo', 'Malco Tupelo Commons 10', 'Malco'),
        ('tyler 16', '', 'AMC Tyler Riverside 16 & IMAX', 'AMC'),
        ('university 16 cinemas w/gtx', '', 'GTC University 16', ''),
        ('valdosta stadium 15 w/gtx', '', 'GTC Valdosta 15', ''),
        ('valley city', '', 'Valley Twin Cinema 2', ''),
        ('valley mall stm 16', '', 'Regal Valley Mall Hagerstown 16', 'Regal'),
        ('vestavia 10', '', 'AMC DINE-IN Vestavia Hills 10', 'AMC'),
        ('veterans expressway 24', '', 'AMC Veterans Tampa 24 & IMAX', 'AMC'),
        ('village park cinema stm 17', '', 'Regal Village Park Carmel 17', 'Regal'),
        ('w. des moines jordan creek + xd', '', 'cinemark century 20 jordan creek and xd', 'Cinemark'),
        ('waikoloa 3', '', 'waikoloa village cinema 3', ''),
        ('washington sq 12', '', 'AMC Washington Square 12', 'AMC'),
        ('washington township 14', '', 'Regal Washington Township Sewell 14', 'Regal'),
        ('waynesville', '', 'Smoky Mountain Cinema 3', ''),
        ('west manchester stm 13', '', 'Regal West Manchester York 13', 'Regal'),
        ('westmoreland 15', '', 'AMC Westmoreland Greensburg 15', 'AMC'),
        ('weston 8', '', 'AMC Weston Cinema Sunrise 8', 'AMC'),
        ('westshore plaza 14', '', 'AMC Westshore Tampa 14', 'AMC'),
        ('wilder stm 14', '', 'Regal Wilder 14', 'Regal'),
        ('willmar', '', 'Golden Ticket Cinemas Kandi 6', ''),
        ('willoughby commons stm 16', '', 'Regal Willoughby Commons 16', 'Regal'),
        ('willowbrook 24', '', 'AMC Willowbrook Houston 24', 'AMC'),
        ('winter park village 20 & rpx', '', 'Regal Winter Park Village 16', 'Regal'),
        ('woodlands square 20', '', 'AMC Woodland Sq Oldsmar 20 & IMAX', 'AMC'),
        ('worldgate 9', '', 'AMC Worldgate Herndon 9', 'AMC'),
        ('worthington', '', 'New Grand Theatre', ''),
        ('yorktown 18', '', 'AMC Yorktown Lombard 18', 'AMC'),
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
