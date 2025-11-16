import subprocess
import time
import logging
from pathlib import Path
import shutil

logging.basicConfig(
    filename="run_all_log.txt",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

BASE_DIR = Path(__file__).resolve().parent
GMAPS_DIR = BASE_DIR / "20251105 GMaps Scraper"
SOCIAL_DIR = BASE_DIR / "20251105 Socials Scraper"

scripts = [
    GMAPS_DIR / "make_queries.py",
    GMAPS_DIR / "search_query.py",
    GMAPS_DIR / "get_place_data.py",
]

def run_script(script_path: Path, retries=2, cwd: Path | None = None):
    for attempt in range(1, retries + 2):
        print(f"\n🚀 Futtatás: {script_path} (próbálkozás {attempt})")
        logging.info(f"Futtatás: {script_path}, próbálkozás {attempt}")
        try:
            subprocess.run(
                ["python3", str(script_path)],
                check=True,
                cwd=str(cwd) if cwd else None,
            )
            print(f"✅ {script_path} sikeresen lefutott.")
            logging.info(f"{script_path} sikeresen lefutott.")
            return True
        except subprocess.CalledProcessError as e:
            print(f"❌ Hiba: {e}")
            logging.error(f"Hiba: {e}")
            if attempt < retries + 1:
                print("🔁 Újrapróbálás 5 mp múlva...")
                time.sleep(5)
            else:
                print("⚠️ Feladom ezt a scriptet.")
                return False

if __name__ == "__main__":
    print("=== RUN ALL START ===")
    logging.info("=== RUN ALL START ===")

    for script in scripts:
        ok = run_script(script, cwd=GMAPS_DIR)
        if not ok:
            print("⛔ Megállok, mert ez a lépés nem futott le.")
            break
        time.sleep(2)
    else:
        gmaps_csv = GMAPS_DIR / "places_data.csv"
        if gmaps_csv.exists():
            target_csv = SOCIAL_DIR / "input.csv"
            shutil.copy2(gmaps_csv, target_csv)
            print(f"📁 Átmásoltam: {gmaps_csv} → {target_csv}")
            logging.info(f"Átmásoltam: {gmaps_csv} → {target_csv}")

            social_script = SOCIAL_DIR / "social_media_scraper.py"
            if social_script.exists():
                run_script(social_script, cwd=SOCIAL_DIR)
            else:
                print("⚠️ Social script nem található, ezt kihagyom.")
        else:
            print("⚠️ Nem találom a places_data.csv-t, nem tudom átadni a Social scrapernek.")
            logging.warning("places_data.csv hiányzik.")

    print("\n🏁 Kész.")
