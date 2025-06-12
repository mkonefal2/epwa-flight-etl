import requests
import json
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv
import os


# === ✈️ API Parameters ===
env_path = Path(__file__).resolve().parents[1] / '.env'
load_dotenv(dotenv_path=env_path)
API_KEY = os.getenv("AVIATIONSTACK_API_KEY")
if not API_KEY:
    raise ValueError("AVIATIONSTACK_API_KEY environment variable is not set.")
BASE_URL = 'http://api.aviationstack.com/v1/flights'
AIRPORT_ICAO = 'EPWA'

# === 📂 File Paths ===
project_root = Path(__file__).resolve().parents[1]
raw_dir = project_root / "data" / "raw"
raw_dir.mkdir(parents=True, exist_ok=True)

today = datetime.utcnow().strftime('%Y-%m-%d')
arr_file = raw_dir / f"flights_arr_{today}.json"
dep_file = raw_dir / f"flights_dep_{today}.json"

# === 🧠 Data Fetching Function ===
def fetch_data(flight_direction):
    params = {
        'access_key': API_KEY,
        ('dep_icao' if flight_direction == 'departure' else 'arr_icao'): AIRPORT_ICAO
    }
    response = requests.get(BASE_URL, params=params)
    response.raise_for_status()
    return response.json()

def main():
    """Fetch today's arrival and departure data and save it as JSON."""
    try:
        arr_data = fetch_data("arrival")
        dep_data = fetch_data("departure")

        with open(arr_file, "w") as f:
            json.dump(arr_data, f)

        with open(dep_file, "w") as f:
            json.dump(dep_data, f)

        print(f"[✔] Saved files: {arr_file.name}, {dep_file.name}")

    except Exception as e:
        print(f"[✖] Error while fetching data: {e}")
        raise


if __name__ == "__main__":
    main()
