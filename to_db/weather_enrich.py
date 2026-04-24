from __future__ import annotations

import argparse
import json
import re
import sqlite3
import time
from pathlib import Path

import requests

_SCRIPT_DIR = Path(__file__).resolve().parent
_REPO_ROOT = _SCRIPT_DIR.parent


def _default_db_path() -> Path:
    """Prefer an existing epstein.db in repo root, then to_db/, then cwd."""
    for candidate in (
        _REPO_ROOT / "epstein.db",
        _SCRIPT_DIR / "epstein.db",
        Path.cwd() / "epstein.db",
    ):
        if candidate.is_file():
            return candidate.resolve()
    return (_REPO_ROOT / "epstein.db").resolve()


DB_PATH = str(_default_db_path())
GEOCODE_CACHE_PATH = Path(__file__).resolve().parent / "weather_geocode_cache.json"
GEOCODE_URL = "https://geocoding-api.open-meteo.com/v1/search"
ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"

REQUEST_PAUSE_SEC = 0.2
ARCHIVE_RATE_LIMIT_WAIT_SEC = 10
ARCHIVE_MAX_RETRIES = 2

WEATHER_SUNNY = "sunny"
WEATHER_RAINY = "rainy"
WEATHER_UNKNOWN_DAY = "unknown_day"
ERROR_NO_ORIGIN = "error_no_origin"
ERROR_NO_DESTINATION = "error_no_destination"
ERROR_GEOCODE = "error_geocode"
ERROR_BAD_DATE = "error_bad_date"
ERROR_ARCHIVE_RATE_LIMIT = "error_archive_rate_limit"
ERROR_ARCHIVE_REQUEST = "error_archive_request"
ERROR_ARCHIVE_RESPONSE = "error_archive_response"
ERROR_DAY_NOT_FOUND = "error_day_not_found"
ERROR_NO_DAILY_FIELDS = "error_no_daily_fields"

ORIGIN_NORMALIZATION_MAP = {
    "MCN Airport": "Middle Georgia Regional Airport",
    "TUS Airport": "Tucson International Airport",
    "TIST Airport": "Cyril E. King Airport",
    "Teterboro Airport, NJ": "Teterboro Airport",
    "PDK Airport": "DeKalb-Peachtree Airport",
    "MDPP Airport": "Gregorio Luperón International Airport",
    "ISM Airport": "Kissimmee Gateway Airport",
    "Albuquerque International Sunport, NM": "Albuquerque International Sunport",
    "TJSJ Airport": "Luis Muñoz Marín International Airport",
    "ISP Airport": "Long Island MacArthur Airport",
    "Palm Beach International, FL": "Palm Beach International Airport",
    "Laurence G. Hanscom Field, Bedford, MA": "Laurence G Hanscom Field",
    "NUQ Airport": "Moffett Federal Airfield",
    "EWR Airport": "Newark Liberty International Airport",
    "BQK Airport": "Brunswick Golden Isles Airport",
    "CNM Airport": "Cavern City Air Terminal",
    "Newark Liberty International, NJ": "Newark Liberty International Airport",
    "MYNN Airport": "Lynden Pindling International Airport",
    "VQQ Airport": "Cecil Airport",
    "LGB Airport": "Long Beach Airport",
    "Washington Dulles International, VA": "Washington Dulles International Airport",
    "LCQ Airport": "Lake City Gateway Airport",
    "SEF Airport": "Sebring Regional Airport",
    "RYY Airport": "Cobb County International Airport McCollum Field",
    "CNO Airport": "Chino Airport",
    "Miami International, FL": "Miami International Airport",
    "John F. Kennedy International, NY": "John F Kennedy International Airport",
    "RSW Airport": "Southwest Florida International Airport",
    "Santa Fe Municipal Airport, NM": "Santa Fe Regional Airport",
    "Cyril E. King Airport, USVI": "Cyril E King Airport",
    "Savannah/Hilton Head International, GA": "Savannah Hilton Head International Airport",
    "John Glenn Columbus International, OH": "John Glenn Columbus International Airport",
    "JAX Airport": "Jacksonville International Airport",
    "Van Nuys Airport, CA": "Van Nuys Airport",
    "Aspen/Pitkin County Airport, CO": "Aspen Pitkin County Airport",
    "MDW Airport": "Chicago Midway International Airport",
    "CYQX Airport": "Gander International Airport",
    "JFK International, NY": "John F. Kennedy International Airport", 
    "Montreal-Trudeau International, Canada": "Montréal Pierre Elliott Trudeau International Airport",
    "ABY Airport": "Southwest Georgia Regional Airport",
    "PBI-OFF Airport": "Palm Beach International Airport",
    "MYEF Airport": "Exuma International Airport",
    "Bangor International, ME": "Bangor International Airport",
    "Westchester County Airport, NY": "Westchester County Airport",
    "MPPV Airport": "Toussaint Louverture International Airport",
    "TLPL Airport": "Hewanorra International Airport",
    "Long Island MacArthur, NY": "Long Island MacArthur Airport",
    "GNV Airport": "Gainesville Regional Airport",
    "Fort Lauderdale-Hollywood International, FL": "Fort Lauderdale Hollywood International Airport",
    "SAF Airport": "Santa Fe Regional Airport",
    "LaGuardia Airport, NY": "LaGuardia Airport",
    "EYW Airport": "Key West International Airport",
    "AVO Airport": "Avon Park Executive Airport",
    "LEE Airport": "Leesburg International Airport",
    "LAL Airport": "Lakeland Linder International Airport",
    "Ronald Reagan Washington National, DC": "Ronald Reagan Washington National Airport",
    "San Diego International, CA": "San Diego International Airport",
    "Phoenix Sky Harbor International, AZ": "Phoenix Sky Harbor International Airport",
    "DAL Airport": "Dallas Love Field",
    "Boston Logan International, MA": "Logan International Airport",
    "APF Airport": "Naples Municipal Airport",
    "BCT Airport": "Boca Raton Airport",
    "STL Airport": "St Louis Lambert International Airport",
    "CYJT Airport": "Stephenville International Airport",
    "Martha's Vineyard Airport, MA": "Marthas Vineyard Airport",
    "Dallas/Fort Worth International, TX": "Dallas Fort Worth International Airport",
    "Columbus Airport, OH": "John Glenn Columbus International Airport",
    "VNC Airport": "Venice Municipal Airport",
    "Princess Juliana International, St. Maarten": "Princess Juliana International Airport",
    "TIX Airport": "Space Coast Regional Airport",
    "LZU Airport": "Gwinnett County Airport Briscoe Field",
    "MBGT Airport": "JAGS McCartney International Airport",
    "MYGF Airport": "Grand Bahama International Airport",
    "MMZH Airport": "Ixtapa Zihuatanejo International Airport",
    "Atlantic City International, NJ": "Atlantic City International Airport",
    "DNN Airport": "Dalton Municipal Airport",
    "FTY Airport": "Fulton County Airport Brown Field",
    "Daytona Beach International, FL": "Daytona Beach International Airport",
    "JZI Airport": "Charleston Executive Airport",
    "MYEM Airport": "Governors Harbour Airport",
    "X31 Airport": "Leeward Air Ranch",
    "NEW Airport": "Lakefront Airport",
    "Denver International, CO": "Denver International",
    "Santa Fe Regional Airport": "Los Alamos",
    "Little St. James Island, USVI": "Little Saint James",
    "London Luton, UK": "Luton"
}


def _normalize_calendar_date(value: str | float | int) -> str:
    s = str(value).strip()
    if not s:
        return ""
    if "T" in s:
        s = s.split("T", 1)[0]
    elif " " in s:
        s = s.split()[0]
    if len(s) >= 10 and s[4] == "-" and s[7] == "-":
        return s[:10]
    return s


def _load_geocode_cache() -> dict[str, list[float] | None]:
    if not GEOCODE_CACHE_PATH.is_file():
        return {}
    try:
        return json.loads(GEOCODE_CACHE_PATH.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return {}


def _save_geocode_cache(cache: dict[str, list[float] | None]) -> None:
    GEOCODE_CACHE_PATH.write_text(json.dumps(cache, indent=2, sort_keys=True), encoding="utf-8")


def _geocode_queries(origin: str) -> list[str]:
    s = origin.strip()
    out: list[str] = []
    m = re.match(r"^([A-Z]{3,4})\b", s)
    if m:
        out.append(m.group(1))
    out.append(s.split(",", 1)[0].strip())
    if "international" in s.lower() and "airport" not in s.lower():
        out.append(f"{s.split(',', 1)[0].strip()} Airport")
    out.append(s)
    dedup: list[str] = []
    seen: set[str] = set()
    for q in out:
        if q and q not in seen:
            seen.add(q)
            dedup.append(q[:120])
    return dedup


def geocode_origin(origin: str, session: requests.Session, cache: dict[str, list[float] | None]) -> tuple[float, float] | None:
    key = origin.strip()
    if key in cache:
        loc = cache[key]
        return tuple(loc) if loc is not None else None
    for q in _geocode_queries(origin):
        r = session.get(GEOCODE_URL, params={"name": q, "count": 1, "language": "en", "format": "json"}, timeout=30)
        r.raise_for_status()
        results = (r.json().get("results") or [])
        time.sleep(REQUEST_PAUSE_SEC)
        if results:
            lat = float(results[0]["latitude"])
            lon = float(results[0]["longitude"])
            cache[key] = [lat, lon]
            return lat, lon
    cache[key] = None
    return None


def classify_daily(precip_mm: float | None, wmo_code: int | None) -> str:
    if precip_mm is None and wmo_code is None:
        return ERROR_NO_DAILY_FIELDS
    precip = float(precip_mm) if precip_mm is not None else 0.0
    if precip >= 1.0:
        return WEATHER_RAINY
    if wmo_code is not None and int(wmo_code) in (set(range(51, 68)) | set(range(71, 78)) | {77, 80, 81, 82, 85, 86, 95, 96, 99}):
        return WEATHER_RAINY
    return WEATHER_SUNNY


def ensure_weather_columns(conn: sqlite3.Connection) -> None:
    cols = {row[1] for row in conn.execute("PRAGMA table_info(flights)").fetchall()}
    if "departure_weather_id" not in cols:
        conn.execute("ALTER TABLE flights ADD COLUMN departure_weather_id INTEGER")
    if "arrival_weather_id" not in cols:
        conn.execute("ALTER TABLE flights ADD COLUMN arrival_weather_id INTEGER")
    conn.commit()


def ensure_flight_weather_indexes(conn: sqlite3.Connection) -> None:
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_flights_departure_weather_id ON flights(departure_weather_id)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_flights_arrival_weather_id ON flights(arrival_weather_id)"
    )
    conn.commit()


def ensure_weather_conditions_table(conn: sqlite3.Connection) -> dict[str, int]:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS weather_conditions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            code TEXT NOT NULL UNIQUE
        )
        """
    )
    conn.executemany(
        "INSERT OR IGNORE INTO weather_conditions (code) VALUES (?)",
        [(WEATHER_SUNNY,), (WEATHER_RAINY,), (WEATHER_UNKNOWN_DAY,)],
    )
    conn.commit()
    rows = conn.execute("SELECT id, code FROM weather_conditions").fetchall()
    return {code: wid for wid, code in rows}


def normalize_airport_dimension(conn: sqlite3.Connection) -> None:
    for old_name, new_name in ORIGIN_NORMALIZATION_MAP.items():
        old_row = conn.execute(
            "SELECT id FROM airports WHERE name = ?", (old_name,)
        ).fetchone()
        if not old_row:
            continue
        old_id = old_row[0]

        new_row = conn.execute(
            "SELECT id FROM airports WHERE name = ?", (new_name,)
        ).fetchone()
        if new_row:
            new_id = new_row[0]
            if new_id != old_id:
                conn.execute(
                    "UPDATE flights SET origin_airport_id = ? WHERE origin_airport_id = ?",
                    (new_id, old_id),
                )
                conn.execute(
                    "UPDATE flights SET destination_airport_id = ? WHERE destination_airport_id = ?",
                    (new_id, old_id),
                )
                conn.execute("DELETE FROM airports WHERE id = ?", (old_id,))
        else:
            conn.execute(
                "UPDATE airports SET name = ? WHERE id = ?",
                (new_name, old_id),
            )
    conn.commit()


def to_weather_condition(status: str) -> str:
    if status == WEATHER_SUNNY:
        return WEATHER_SUNNY
    if status == WEATHER_RAINY:
        return WEATHER_RAINY
    return WEATHER_UNKNOWN_DAY


def fetch_daily_series(lat: float, lon: float, day: str, session: requests.Session) -> tuple[float | None, int | None] | str:
    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": day,
        "end_date": day,
        "daily": "precipitation_sum,weathercode",
        "timezone": "auto",
        "precipitation_unit": "mm",
    }
    for attempt in range(ARCHIVE_MAX_RETRIES + 1):
        try:
            r = session.get(ARCHIVE_URL, params=params, timeout=60)
            if r.status_code == 429:
                if attempt < ARCHIVE_MAX_RETRIES:
                    print(f"Rate limit, wait {ARCHIVE_RATE_LIMIT_WAIT_SEC}s ({attempt + 1}/{ARCHIVE_MAX_RETRIES})")
                    time.sleep(ARCHIVE_RATE_LIMIT_WAIT_SEC)
                    continue
                return ERROR_ARCHIVE_RATE_LIMIT
            r.raise_for_status()
            payload = r.json()
        except requests.RequestException:
            return ERROR_ARCHIVE_REQUEST
        daily = payload.get("daily") or {}
        times = daily.get("time") or []
        precs = daily.get("precipitation_sum") or []
        codes = daily.get("weathercode") or []
        if not times:
            return ERROR_DAY_NOT_FOUND
        return (
            float(precs[0]) if precs and precs[0] is not None else None,
            int(codes[0]) if codes and codes[0] is not None else None,
        )
    return ERROR_ARCHIVE_RESPONSE


def resolve_airport_weather_status(
    airport: str | None,
    day: str,
    session: requests.Session,
    cache: dict[str, list[float] | None],
    missing_airport_error: str,
) -> str:
    if not airport or not str(airport).strip():
        return missing_airport_error

    loc = geocode_origin(str(airport), session, cache)
    if loc is None:
        return ERROR_GEOCODE

    pair_or_error = fetch_daily_series(loc[0], loc[1], day, session)
    if isinstance(pair_or_error, str):
        return pair_or_error
    return classify_daily(pair_or_error[0], pair_or_error[1])


def enrich_db(db_path: Path, limit: int | None = None) -> None:
    conn = sqlite3.connect(db_path, timeout=30.0)
    try:
        conn.execute("PRAGMA foreign_keys = ON")
        has_flights = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='flights'"
        ).fetchone()
        if not has_flights:
            size = db_path.stat().st_size if db_path.exists() else 0
            raise RuntimeError(
                f"Missing flights table in {db_path} (file size: {size} bytes). "
                "Create/populate the database first with: python to_db/data_to_db.py"
            )

        ensure_weather_columns(conn)
        ensure_flight_weather_indexes(conn)
        weather_condition_ids = ensure_weather_conditions_table(conn)
        normalize_airport_dimension(conn)

        rows = conn.execute(
            """
            SELECT
                f.id,
                f.flight_date,
                ao.name AS origin_name,
                ad.name AS destination_name
            FROM flights f
            LEFT JOIN airports ao ON ao.id = f.origin_airport_id
            LEFT JOIN airports ad ON ad.id = f.destination_airport_id
            WHERE f.flight_date IS NOT NULL
              AND TRIM(f.flight_date) != ''
            """
        ).fetchall()
        if limit is not None:
            rows = rows[:limit]

        session = requests.Session()
        session.headers["User-Agent"] = "flight-weather-enrich/1.0"
        cache = _load_geocode_cache()

        updates: list[tuple[int, int, str]] = []
        total = len(rows)
        for idx, (fid, fdate, origin, destination) in enumerate(rows, start=1):
            if idx == 1 or idx % 100 == 0 or idx == total:
                print(f"Progress {idx}/{total}")
            day = _normalize_calendar_date(str(fdate))
            if not re.match(r"^\d{4}-\d{2}-\d{2}$", day):
                dep_status = ERROR_BAD_DATE
                arr_status = ERROR_BAD_DATE
                dep_code = to_weather_condition(dep_status)
                arr_code = to_weather_condition(arr_status)
                updates.append(
                    (
                        weather_condition_ids[dep_code],
                        weather_condition_ids[arr_code],
                        fid,
                    )
                )
                continue

            dep_status = resolve_airport_weather_status(
                airport=origin,
                day=day,
                session=session,
                cache=cache,
                missing_airport_error=ERROR_NO_ORIGIN,
            )
            arr_status = resolve_airport_weather_status(
                airport=destination,
                day=day,
                session=session,
                cache=cache,
                missing_airport_error=ERROR_NO_DESTINATION,
            )

            dep_code = to_weather_condition(dep_status)
            arr_code = to_weather_condition(arr_status)
            updates.append(
                (
                    weather_condition_ids[dep_code],
                    weather_condition_ids[arr_code],
                    fid,
                )
            )
            time.sleep(REQUEST_PAUSE_SEC)

        _save_geocode_cache(cache)
        conn.executemany(
            "UPDATE flights SET departure_weather_id = ?, arrival_weather_id = ? WHERE id = ?",
            updates,
        )
        conn.commit()
        print(
            f"Updated departure_weather_id and arrival_weather_id for {len(updates)} rows in {db_path}"
        )
    finally:
        conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Normalize airport names and enrich departure+arrival weather from Open-Meteo."
    )
    parser.add_argument("--db", default=str(DB_PATH), help=f"SQLite database path (default: {DB_PATH})")
    parser.add_argument("--limit", type=int, default=None, metavar="N", help="Only process first N flights")
    args = parser.parse_args()
    enrich_db(Path(args.db), limit=args.limit)


if __name__ == "__main__":
    main()

