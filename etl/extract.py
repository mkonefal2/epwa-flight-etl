import requests
import json
from datetime import datetime
from pathlib import Path

# === ✈️ Parametry API ===
API_KEY = 'e631d933b8b787e286c781b9a6b951cc'
BASE_URL = 'http://api.aviationstack.com/v1/flights'
AIRPORT_ICAO = 'EPWA'

# === 📂 Ścieżki do plików ===
project_root = Path(__file__).resolve().parents[1]
raw_dir = project_root / "data" / "raw"
raw_dir.mkdir(parents=True, exist_ok=True)

today = datetime.utcnow().strftime('%Y-%m-%d')
arr_file = raw_dir / f"flights_arr_{today}.json"
dep_file = raw_dir / f"flights_dep_{today}.json"

# === 🧠 Funkcja pobierająca ===
def fetch_data(flight_direction):
    params = {
        'access_key': API_KEY,
        ('dep_icao' if flight_direction == 'departure' else 'arr_icao'): AIRPORT_ICAO
    }
    response = requests.get(BASE_URL, params=params)
    response.raise_for_status()
    return response.json()

# === 📦 Pobieranie i zapis ===
try:
    arr_data = fetch_data('arrival')
    dep_data = fetch_data('departure')

    with open(arr_file, 'w') as f:
        json.dump(arr_data, f)

    with open(dep_file, 'w') as f:
        json.dump(dep_data, f)

    print(f"[✔] Zapisano pliki: {arr_file.name}, {dep_file.name}")

except Exception as e:
    print(f"[✖] Błąd podczas pobierania danych: {e}")
    raise
