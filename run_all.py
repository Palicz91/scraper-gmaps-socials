import subprocess
import time
import logging
from pathlib import Path
import shutil
import csv

from telegram_notify import notify, stage_done, stage_failed, pipeline_summary

logging.basicConfig(
    filename="run_all_log.txt",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

BASE_DIR = Path(__file__).resolve().parent
GMAPS_DIR = BASE_DIR / "20251105 GMaps Scraper"
SOCIAL_DIR = BASE_DIR / "20251105 Socials Scraper"

GMAPS_ARTIFACTS = [
    "links.txt",
    "places_data.csv",
    "last_processed.txt",
    "google_maps_queries.txt",
    "scraper_log.txt",
]

SOCIAL_ARTIFACTS = [
    "input.csv",
    "output.csv",
    "output_cleared.csv",
    "scraper.log",
]

scripts = [
    ("Query generation", GMAPS_DIR / "make_queries.py"),
    ("Search query", GMAPS_DIR / "search_query.py"),
    ("Place data scrape", GMAPS_DIR / "get_place_data.py"),
]


def count_csv_rows(filepath: Path) -> int:
    """Count data rows in a CSV file."""
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            return sum(1 for _ in csv.reader(f)) - 1  # minus header
    except Exception:
        return 0


def cleanup_artifacts():
    deleted = 0
    for filename in GMAPS_ARTIFACTS:
        filepath = GMAPS_DIR / filename
        if filepath.exists():
            filepath.unlink()
            print(f"  🗑️  {filepath}")
            deleted += 1
    for filename in SOCIAL_ARTIFACTS:
        filepath = SOCIAL_DIR / filename
        if filepath.exists():
            filepath.unlink()
            print(f"  🗑️  {filepath}")
            deleted += 1
    if deleted == 0:
        print("  ℹ️  Nem volt törölhető fájl.")
    else:
        print(f"  ✅ {deleted} fájl törölve.")
    logging.info(f"Cleanup: {deleted} fájl törölve.")


def run_script(name: str, script_path: Path, retries=2, cwd: Path | None = None):
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
            stage_done(name)
            return True
        except subprocess.CalledProcessError as e:
            print(f"❌ Hiba: {e}")
            logging.error(f"Hiba: {e}")
            if attempt < retries + 1:
                print("🔁 Újrapróbálás 5 mp múlva...")
                time.sleep(5)
            else:
                print("⚠️ Feladom ezt a scriptet.")
                stage_failed(name, str(e))
                return False


def run_postprocess(input_csv: Path):
    postprocess_script = SOCIAL_DIR / "postprocess_places.py"
    if not postprocess_script.exists():
        print("⚠️ Postprocess script nem található.")
        logging.warning("postprocess_places.py hiányzik.")
        return

    print(f"\n🚀 Postprocess futtatása: {postprocess_script} {input_csv}")
    logging.info(f"Postprocess futtatása: {postprocess_script} {input_csv}")
    try:
        subprocess.run(
            ["python3", str(postprocess_script), str(input_csv)],
            check=True,
            cwd=str(SOCIAL_DIR),
        )
        print("✅ Postprocess sikeresen lefutott.")
        logging.info("Postprocess sikeresen lefutott.")
        stage_done("Postprocess")
    except subprocess.CalledProcessError as e:
        print(f"❌ Postprocess hiba: {e}")
        logging.error(f"Postprocess hiba: {e}")
        stage_failed("Postprocess", str(e))


if __name__ == "__main__":
    pipeline_start = time.time()
    
    print("=== RUN ALL START ===")
    logging.info("=== RUN ALL START ===")
    notify("🟢 <b>Pipeline indult</b>")

    # Cleanup
    print("\n🧹 Cleanup...")
    cleanup_artifacts()

    total_places = 0
    social_found = 0

    for name, script in scripts:
        ok = run_script(name, script, cwd=GMAPS_DIR)
        if not ok:
            print("⛔ Megállok, mert ez a lépés nem futott le.")
            notify(f"⛔ <b>Pipeline leállt</b> — {name} sikertelen")
            break
        time.sleep(2)
    else:
        gmaps_csv = GMAPS_DIR / "places_data.csv"
        if gmaps_csv.exists():
            total_places = count_csv_rows(gmaps_csv)
            target_csv = SOCIAL_DIR / "input.csv"
            shutil.copy2(gmaps_csv, target_csv)
            print(f"📁 Átmásoltam: {gmaps_csv} → {target_csv}")
            logging.info(f"Átmásoltam: {gmaps_csv} → {target_csv}")
            stage_done("GMaps → Social copy", f"📍 {total_places} hely")

            social_script = SOCIAL_DIR / "social_media_scraper.py"
            if social_script.exists():
                ok_social = run_script("Social media scrape", social_script, cwd=SOCIAL_DIR)
                if ok_social:
                    output_csv = SOCIAL_DIR / "output.csv"
                    if output_csv.exists():
                        social_found = count_csv_rows(output_csv)
                        run_postprocess(output_csv)
                    else:
                        print("⚠️ Nem találom az output.csv-t.")
                        logging.warning("output.csv hiányzik.")
                        stage_failed("Social output", "output.csv hiányzik")
            else:
                print("⚠️ Social script nem található.")
        else:
            print("⚠️ Nem találom a places_data.csv-t.")
            logging.warning("places_data.csv hiányzik.")
            stage_failed("GMaps output", "places_data.csv hiányzik")

    duration = time.time() - pipeline_start
    pipeline_summary(total_places, social_found, duration)
    print("\n🏁 Kész.")
