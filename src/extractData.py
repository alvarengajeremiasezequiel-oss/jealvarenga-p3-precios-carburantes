import json
import time
import urllib.request
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()


API_URL = "https://sedeaplicaciones.minetur.gob.es/ServiciosRESTCarburantes/PreciosCarburantes/EstacionesTerrestres/"
RAW_DIR = Path(__file__).resolve().parent.parent / "data" / "raw"


def fetch_data(max_retries=10) -> dict:
    for attempt in range(max_retries):
        try:
            req = urllib.request.Request(API_URL, headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=30) as resp:
                return json.loads(resp.read())
        except (urllib.error.URLError, ConnectionResetError) as e:
            if attempt == max_retries - 1:
                raise
            print(f"  Retry {attempt + 1}/{max_retries} after: {e}")
            time.sleep(2 ** attempt)


def save_raw(data: dict, ingestion_ts: str) -> str:
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    path = RAW_DIR / f"{ingestion_ts.replace(':', '-')}.json"
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    return str(path)


def main():
    ingestion_ts = time.strftime("%Y-%m-%d %H:%M:%S")

    print(f"[{ingestion_ts}] Fetching data from API...")
    data = fetch_data()

    print(f"[{ingestion_ts}] Saving raw JSON...")
    raw_path = save_raw(data, ingestion_ts)
    stations = data.get("ListaEESSPrecio", [])
    print(f"  Raw data saved to: {raw_path}")
    print(f"  Total stations: {len(stations)}")
    print(f"[{ingestion_ts}] Done.")


if __name__ == "__main__":
    main()
