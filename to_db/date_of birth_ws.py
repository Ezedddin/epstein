import sqlite3
import json
import requests
from bs4 import BeautifulSoup
import re
from pathlib import Path

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

conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

has_flights = cursor.execute(
    "SELECT 1 FROM sqlite_master WHERE type='table' AND name='flights'"
).fetchone()
if not has_flights:
    raise RuntimeError(
        f"Missing flights table in {DB_PATH}. "
        "Populate the database first with: python to_db/data_to_db.py"
    )

# -------------------------
# 1. Extract names
# -------------------------
passengers = cursor.execute(
    "SELECT DISTINCT passenger_names_json FROM flights"
).fetchall()

names = set()

for passenger in passengers:
    try:
        parsed = json.loads(passenger[0])
        for name in parsed:
            names.add(name)
    except:
        continue

# -------------------------
# 2. Clean names
# -------------------------
clean_names = set()

for name in names:
    name = name.strip()
    name = name.replace(" ", "_")
    clean_names.add(name)

# -------------------------
# 3. check column exists
# -------------------------
cursor.execute("PRAGMA table_info(flights)")
columns = [col[1] for col in cursor.fetchall()]

if "birth_place" not in columns:
    conn.execute("ALTER TABLE flights ADD COLUMN birth_place TEXT")
    conn.commit()

# -------------------------
# helpers
# -------------------------
headers = {"User-Agent": "Mozilla/5.0"}

US_STATES = [
    "Alabama","Alaska","Arizona","Arkansas","California","Colorado","Connecticut",
    "Delaware","Florida","Georgia","Hawaii","Idaho","Illinois","Indiana","Iowa",
    "Kansas","Kentucky","Louisiana","Maine","Maryland","Massachusetts","Michigan",
    "Minnesota","Mississippi","Missouri","Montana","Nebraska","Nevada","New Hampshire",
    "New Jersey","New Mexico","New York","North Carolina","North Dakota","Ohio",
    "Oklahoma","Oregon","Pennsylvania","Rhode Island","South Carolina","South Dakota",
    "Tennessee","Texas","Utah","Vermont","Virginia","Washington","West Virginia",
    "Wisconsin","Wyoming"
]

def extract_country(text):
    if not text:
        return None

    text = text.strip()

    # split op comma
    parts = [p.strip() for p in text.split(",")]

    if not parts:
        return None

    last = parts[-1]

    # US cases
    if "United States" in last or "US" in last:
        return "United States"

    # US state → United States
    for state in US_STATES:
        if state.lower() in last.lower():
            return "United States"

    # anders: neem laatste stuk (land)
    return last


# -------------------------
# 4. Scrape + update per person
# -------------------------

conn.execute("CREATE TABLE IF NOT EXISTS passengers(name TEXT, country TEXT)")

for name in clean_names:
    url = f"https://en.wikipedia.org/wiki/{name}"

    try:
        res = requests.get(url, headers=headers, timeout=10)
        soup = BeautifulSoup(res.text, "html.parser")
    except:
        continue

    infobox = soup.find("table", class_="infobox")

    birth_data = None

    if infobox:
        for row in infobox.find_all("tr"):
            th = row.find("th")
            td = row.find("td")

            if th and "born" in th.text.lower():
                birth_data = td.get_text(" ", strip=True)
                break

    # fallback birthplace div
    if not birth_data:
        div = soup.find("div", class_="birthplace")
        if div:
            birth_data = div.get_text(" ", strip=True)

    # -------------------------
    # CLEAN → ONLY COUNTRY
    # -------------------------
    country = extract_country(birth_data)

    # -------------------------
    # UPDATE DATABASE
    # -------------------------

    conn.execute("INSERT INTO passengers(name, country) VALUES (?, ?)", (name, country))

    conn.commit()

conn.close()