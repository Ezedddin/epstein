"""
Populate SQLite (epstein.db) with flights from the epsteinexposed.com export API.

Recommended (all in this script):
  - Set EPSTEINEXPOSED_COOKIE (same cookie string as in your logged-in browser)
  - Run:  python data_to_db.py
  The script downloads from the export URL via GET and writes into the database.

Optional: use an existing JSON file on disk (--from-json) if you do not want to use
a cookie or when importing a previously saved export offline.

Source: GET https://epsteinexposed.com/api/v2/export/flights?format=json
Authentication: browser cookie in the EPSTEINEXPOSED_COOKIE environment variable.
"""

import argparse
import json
import os
import sqlite3
import time
from pathlib import Path

import requests

# Wait for locks (e.g. DB Browser, Jupyter, another script) instead of failing immediately.
SQLITE_BUSY_TIMEOUT_SEC = 30.0
EXPORT_URL = "https://epsteinexposed.com/api/v2/export/flights?format=json"
LOCAL_JSON = "flights.json"
INSERT_BATCH_SIZE = 25

_SCRIPT_DIR = Path(__file__).resolve().parent
_REPO_ROOT = _SCRIPT_DIR.parent


def _default_db_path() -> Path:
    """Prefer an existing epstein.db; otherwise use repo root (next to to_db/)."""
    for candidate in (
        _REPO_ROOT / "epstein.db",
        _SCRIPT_DIR / "epstein.db",
        Path.cwd() / "epstein.db",
    ):
        if candidate.is_file():
            return candidate.resolve()
    return (_REPO_ROOT / "epstein.db").resolve()


def _resolve_flights_json(path_str: str) -> Path:
    """Resolve flights.json whether you run from repo root or from to_db/."""
    p = Path(path_str).expanduser()
    if p.is_file():
        return p.resolve()
    if not p.is_absolute():
        rel = p
        for base in (_REPO_ROOT, _SCRIPT_DIR, Path.cwd()):
            cand = (base / rel).resolve()
            if cand.is_file():
                return cand
    return p.resolve()


DB_PATH = str(_default_db_path())

CREATE_FLIGHTS_SQL = """
CREATE TABLE IF NOT EXISTS flights (
    id TEXT PRIMARY KEY,
    flight_date TEXT,
    origin_airport_id INTEGER,
    destination_airport_id INTEGER,
    aircraft_id INTEGER,
    pilot TEXT,
    passenger_count INTEGER,
    passenger_names_json TEXT,
    departure_weather_id INTEGER,
    arrival_weather_id INTEGER,
    FOREIGN KEY (origin_airport_id) REFERENCES airports(id),
    FOREIGN KEY (destination_airport_id) REFERENCES airports(id),
    FOREIGN KEY (aircraft_id) REFERENCES aircraft(id),
    FOREIGN KEY (departure_weather_id) REFERENCES weather_conditions(id),
    FOREIGN KEY (arrival_weather_id) REFERENCES weather_conditions(id)
);
"""

CREATE_AIRPORTS_SQL = """
CREATE TABLE IF NOT EXISTS airports (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE
);
"""

CREATE_AIRCRAFT_SQL = """
CREATE TABLE IF NOT EXISTS aircraft (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE
);
"""

CREATE_WEATHER_CONDITIONS_SQL = """
CREATE TABLE IF NOT EXISTS weather_conditions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    code TEXT NOT NULL UNIQUE
);
"""

FLIGHT_INDEX_SQL = (
    "CREATE INDEX IF NOT EXISTS idx_flights_flight_date ON flights(flight_date)",
    "CREATE INDEX IF NOT EXISTS idx_flights_origin_airport_id ON flights(origin_airport_id)",
    "CREATE INDEX IF NOT EXISTS idx_flights_destination_airport_id ON flights(destination_airport_id)",
    "CREATE INDEX IF NOT EXISTS idx_flights_aircraft_id ON flights(aircraft_id)",
    "CREATE INDEX IF NOT EXISTS idx_flights_departure_weather_id ON flights(departure_weather_id)",
    "CREATE INDEX IF NOT EXISTS idx_flights_arrival_weather_id ON flights(arrival_weather_id)",
)

INSERT_SQL = """
INSERT OR REPLACE INTO flights (
    id, flight_date,
    origin_airport_id, destination_airport_id, aircraft_id,
    pilot, passenger_count, passenger_names_json,
    departure_weather_id, arrival_weather_id
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
"""


def fetch_export_json():
    cookie = os.environ.get("EPSTEINEXPOSED_COOKIE", "").strip()
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; flight-import/1.0)",
    }
    if cookie:
        headers["Cookie"] = cookie
    try:
        r = requests.get(EXPORT_URL, headers=headers, timeout=300)
    except requests.RequestException as e:
        raise RuntimeError(
            f"Network error during GET {EXPORT_URL}: {e}"
        ) from e

    if not r.ok:
        preview = (r.text or "")[:500].replace("\n", " ")
        raise RuntimeError(
            f"API returned HTTP {r.status_code} (not OK). "
            "Common causes: invalid/expired cookie, or no access to this export. "
            f"Response preview: {preview!r}"
        )

    try:
        return r.json()
    except json.JSONDecodeError as e:
        preview = (r.text or "")[:400].replace("\n", " ")
        raise ValueError(
            f"Response is not JSON (HTTP {r.status_code}). "
            f"Possibly an HTML login page. Preview: {preview!r}"
        ) from e


def load_payload_from_file(path=LOCAL_JSON):
    with open(path, "r", encoding="utf-8") as f:
        raw = f.read()
    if not raw.strip():
        raise ValueError(
            f"JSON file is empty (no data): {path}. "
            "Fetch data via this script with EPSTEINEXPOSED_COOKIE (see module docstring), "
            "or fill this file with a valid export JSON payload."
        )
    try:
        return json.loads(raw)
    except json.JSONDecodeError as e:
        raise ValueError(
            f"Invalid JSON in {path} ({e}). "
            "Verify this is the raw API export (list or object with 'data'), not HTML/empty content."
        ) from e


def parse_flights_payload(payload):
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict) and "data" in payload:
        return payload["data"]
    raise ValueError("Expected JSON as a list or an object with a 'data' key.")


def _clean_text(value):
    if not isinstance(value, str):
        return None
    cleaned = value.strip()
    return cleaned or None


def _to_int(value, default=0):
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def row_from_record(rec):
    names = rec.get("passenger_names")
    passenger_json = (
        json.dumps(names, ensure_ascii=False) if names is not None else None
    )
    return {
        "id": rec["id"],
        "flight_date": rec.get("date"),
        "origin": _clean_text(rec.get("origin")),
        "destination": _clean_text(rec.get("destination")),
        "aircraft": _clean_text(rec.get("aircraft")),
        "pilot": rec.get("pilot") or "",
        "passenger_count": _to_int(rec.get("passenger_count"), default=0),
        "passenger_names_json": passenger_json,
        "departure_weather_id": None,
        "arrival_weather_id": None,
    }


def _get_or_create_lookup_id(conn, table_name, value, cache):
    if not value:
        return None
    if value in cache:
        return cache[value]

    row = conn.execute(
        f"SELECT id FROM {table_name} WHERE name = ?", (value,)
    ).fetchone()
    if row:
        cache[value] = row[0]
        return row[0]

    conn.execute(f"INSERT INTO {table_name} (name) VALUES (?)", (value,))
    row = conn.execute(
        f"SELECT id FROM {table_name} WHERE name = ?", (value,)
    ).fetchone()
    cache[value] = row[0]
    return row[0]


def write_flights_to_db(flights, db_path=DB_PATH):
    max_attempts = 4
    for attempt in range(1, max_attempts + 1):
        conn = sqlite3.connect(db_path, timeout=SQLITE_BUSY_TIMEOUT_SEC)
        try:
            conn.execute("PRAGMA foreign_keys = ON")
            conn.execute(f"PRAGMA busy_timeout = {int(SQLITE_BUSY_TIMEOUT_SEC * 1000)}")
            conn.execute("BEGIN IMMEDIATE")
            conn.execute("DROP TABLE IF EXISTS flights")
            conn.execute("DROP TABLE IF EXISTS airports")
            conn.execute("DROP TABLE IF EXISTS aircraft")
            conn.execute("DROP TABLE IF EXISTS weather_conditions")
            conn.execute(CREATE_AIRPORTS_SQL)
            conn.execute(CREATE_AIRCRAFT_SQL)
            conn.execute(CREATE_WEATHER_CONDITIONS_SQL)
            conn.executemany(
                "INSERT OR IGNORE INTO weather_conditions (code) VALUES (?)",
                [("sunny",), ("rainy",), ("unknown_day",)],
            )
            conn.execute(CREATE_FLIGHTS_SQL)
            for sql in FLIGHT_INDEX_SQL:
                conn.execute(sql)
            airport_cache = {}
            aircraft_cache = {}
            batch = []

            for flight in flights:
                row = row_from_record(flight)
                origin_airport_id = _get_or_create_lookup_id(
                    conn, "airports", row["origin"], airport_cache
                )
                destination_airport_id = _get_or_create_lookup_id(
                    conn, "airports", row["destination"], airport_cache
                )
                aircraft_id = _get_or_create_lookup_id(
                    conn, "aircraft", row["aircraft"], aircraft_cache
                )

                batch.append(
                    (
                        row["id"],
                        row["flight_date"],
                        origin_airport_id,
                        destination_airport_id,
                        aircraft_id,
                        row["pilot"],
                        row["passenger_count"],
                        row["passenger_names_json"],
                        row["departure_weather_id"],
                        row["arrival_weather_id"],
                    )
                )

                if len(batch) >= INSERT_BATCH_SIZE:
                    conn.executemany(INSERT_SQL, batch)
                    batch.clear()

            if batch:
                conn.executemany(INSERT_SQL, batch)
            conn.commit()
            return
        except sqlite3.OperationalError as e:
            msg = str(e).lower()
            if "database is locked" in msg and attempt < max_attempts:
                wait_s = attempt * 2
                print(
                    f"Database is locked (attempt {attempt}/{max_attempts}); "
                    f"waiting {wait_s}s before retry..."
                )
                time.sleep(wait_s)
                continue
            if "database is locked" in msg:
                raise RuntimeError(
                    "Database remains locked. Close DB Browser/SQLite extensions/Jupyter "
                    f"that have {db_path} open and try again."
                ) from e
            raise
        finally:
            conn.close()


def import_flights_json_to_db(json_path=LOCAL_JSON, db_path=DB_PATH):
    """Read flights from a JSON file (API export shape) and write them to SQLite."""
    if not os.path.isfile(json_path):
        raise FileNotFoundError(json_path)
    payload = load_payload_from_file(json_path)
    flights = parse_flights_payload(payload)
    write_flights_to_db(flights, db_path=db_path)
    return len(flights)


def main(argv=None):
    parser = argparse.ArgumentParser(
        description=(
            "Download flight export from epsteinexposed.com into SQLite (epstein.db), "
            "or import from a JSON file."
        ),
        epilog=(
            "Recommended — live download:\n"
            "  python data_to_db.py\n"
            "  (optionally with EPSTEINEXPOSED_COOKIE if your session requires it)\n\n"
            "Option — from file (--from-json ignores the cookie):\n"
            "  python data_to_db.py --from-json [flights.json]"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--from-json",
        nargs="?",
        const=LOCAL_JSON,
        default=None,
        metavar="PATH",
        help=(
            "Load from local JSON file only (default name: flights.json). "
            "No API call; EPSTEINEXPOSED_COOKIE is ignored."
        ),
    )
    args = parser.parse_args(argv)
    db_path = str(_default_db_path())

    if args.from_json is not None:
        json_file = _resolve_flights_json(args.from_json)
        print("Reading from", json_file)
        try:
            n = import_flights_json_to_db(json_path=str(json_file), db_path=db_path)
        except ValueError as e:
            raise SystemExit(str(e)) from None
        print(n, "flights saved to", db_path)
        return

    payload = None
    try:
        payload = fetch_export_json()
        print("Downloaded export from epsteinexposed.com")
    except (RuntimeError, ValueError) as api_error:
        if _resolve_flights_json(LOCAL_JSON).is_file():
            print(f"Live API download failed, falling back to local file: {api_error}")
        else:
            raise SystemExit(str(api_error)) from None

    if payload is None and _resolve_flights_json(LOCAL_JSON).is_file():
        json_file = _resolve_flights_json(LOCAL_JSON)
        print("Reading from", json_file)
        try:
            n = import_flights_json_to_db(json_path=str(json_file), db_path=db_path)
        except ValueError as e:
            raise SystemExit(str(e)) from None
        print(n, "flights saved to", db_path)
        return
    elif payload is None:
        tried = _REPO_ROOT / LOCAL_JSON
        raise SystemExit(
            "Live API download failed and no usable flights.json was found "
            f"(including search at {tried}).\n"
            "  • Try again, or set EPSTEINEXPOSED_COOKIE if the API requires a session.\n"
            "  • Or provide a valid export JSON and use "
            "python to_db/data_to_db.py --from-json PATH/to/file.json"
        )

    flights = parse_flights_payload(payload)
    write_flights_to_db(flights, db_path=db_path)
    print(len(flights), "flights saved to", db_path)


if __name__ == "__main__":
    main()
