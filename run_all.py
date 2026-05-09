from dotenv import load_dotenv
from pathlib import Path as _P
load_dotenv(_P(__file__).resolve().parent / ".env")

import subprocess
import sys
import time
import logging
import json
import os
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
RUN_CONFIG_FILE = BASE_DIR / "run_config.json"

GMAPS_ARTIFACTS = [
    "links.txt",
    "links.txt.tmp",
    "places_data.csv",
    "last_processed.txt",
    "google_maps_queries.txt",
    "processed_queries.txt",
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


def load_run_config() -> dict:
    """Load runtime config written by the Telegram bot."""
    try:
        if RUN_CONFIG_FILE.exists():
            config = json.loads(RUN_CONFIG_FILE.read_text(encoding="utf-8"))
            logging.info(f"Loaded run config: {config}")
            return config
    except Exception as e:
        logging.warning(f"Could not read run_config.json: {e}")
    return {}


def sync_to_master_table(csv_path: Path):
    """Upsert scraped places into the scraped_places master table."""
    from supabase import create_client as _sc
    sb = _sc(os.environ.get("SUPABASE_URL", ""), os.environ.get("SUPABASE_SERVICE_ROLE_KEY", ""))

    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    if not rows:
        return

    scraper_name = Path(__file__).resolve().parent.name  # "scraper2" or "Scraper"
    now = __import__("datetime").datetime.now(__import__("datetime").timezone.utc).isoformat()

    def safe_int(v):
        try: return int(v) if v not in (None, '') else None
        except: return None

    def safe_float(v):
        try: return float(v) if v not in (None, '') else None
        except: return None

    batch = []
    for r in rows:
        url = r.get("url", "").strip()
        if not url:
            continue
        batch.append({
            "google_maps_url": url,
            "name": r.get("name", ""),
            "category": r.get("category", ""),
            "website": r.get("website", ""),
            "phone": r.get("phone", ""),
            "address": r.get("address", ""),
            "located_in": r.get("located_in", ""),
            "plus_code": r.get("plus_code", ""),
            "lat": safe_float(r.get("lat")),
            "lng": safe_float(r.get("lng")),
            "rating": safe_float(r.get("rating")),
            "review_count": safe_int(r.get("reviews")),
            "reviews_loaded": safe_int(r.get("reviews_loaded")),
            "reviews_answered": safe_int(r.get("reviews_answered")),
            "reviews_unanswered": safe_int(r.get("reviews_unanswered")),
            "reviews_unanswered_pct": safe_float(r.get("reviews_unanswered_pct")),
            "negative_total": safe_int(r.get("negative_total")),
            "negative_unanswered": safe_int(r.get("negative_unanswered")),
            "negative_unanswered_pct": safe_float(r.get("negative_unanswered_pct")),
            "est_unanswered": safe_int(r.get("est_unanswered")),
            "est_negative_unanswered": safe_int(r.get("est_negative_unanswered")),
            "stars_5": safe_int(r.get("stars_5")),
            "stars_4": safe_int(r.get("stars_4")),
            "stars_3": safe_int(r.get("stars_3")),
            "stars_2": safe_int(r.get("stars_2")),
            "stars_1": safe_int(r.get("stars_1")),
            "est_stars_5": safe_int(r.get("est_stars_5")),
            "est_stars_4": safe_int(r.get("est_stars_4")),
            "est_stars_3": safe_int(r.get("est_stars_3")),
            "est_stars_2": safe_int(r.get("est_stars_2")),
            "est_stars_1": safe_int(r.get("est_stars_1")),
            "scraped_by": scraper_name,
            "last_scraped_at": now,
        })

    # Upsert in batches of 100
    upserted = 0
    for i in range(0, len(batch), 100):
        sb.table("scraped_places").upsert(
            batch[i:i+100],
            on_conflict="google_maps_url",
            ignore_duplicates=False
        ).execute()
        upserted += len(batch[i:i+100])

    print(f"📊 Master table: {upserted} places synced to scraped_places")
    logging.info(f"Master table sync: {upserted} places upserted")


def count_csv_rows(filepath: Path) -> int:
    """Count data rows in a CSV file."""
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            return sum(1 for _ in csv.reader(f)) - 1  # minus header
    except Exception:
        return 0


def backup_before_cleanup():
    """Create backup of places_data.csv before cleanup. Keep max 3 backups."""
    from datetime import datetime
    places_csv = GMAPS_DIR / "places_data.csv"
    if places_csv.exists() and places_csv.stat().st_size > 100:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_path = GMAPS_DIR / f"places_data_backup_{ts}.csv"
        shutil.copy2(places_csv, backup_path)
        print(f"  💾 Backup: {backup_path.name}")
        logging.info(f"Backup created: {backup_path}")
        # Keep max 3 backups
        backups = sorted(GMAPS_DIR.glob("places_data_backup_*.csv"), key=lambda f: f.stat().st_mtime)
        while len(backups) > 3:
            old = backups.pop(0)
            old.unlink()
            print(f"  🗑️  Old backup removed: {old.name}")


def cleanup_artifacts(force=False):
    if not force:
        print("  ℹ️  Cleanup skipped (resume mode). Use --clean to force.")
        logging.info("Cleanup skipped (resume mode).")
        return
    backup_before_cleanup()
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
    # Also clean backup files
    for f in GMAPS_DIR.glob("links_backup_*.txt"):
        f.unlink()
        deleted += 1
    if deleted == 0:
        print("  ℹ️  Nothing to clean up.")
    else:
        print(f"  ✅ {deleted} files deleted.")
    logging.info(f"Cleanup: {deleted} files deleted.")


def detect_resume_state():
    """Detect which pipeline stage to resume from based on existing artifacts."""
    links_file = GMAPS_DIR / "links.txt"
    places_csv = GMAPS_DIR / "places_data.csv"
    queries_file = GMAPS_DIR / "google_maps_queries.txt"
    processed_q = GMAPS_DIR / "processed_queries.txt"

    # Stage 2: place_data — if we have links AND places_data already exists
    if places_csv.exists() and places_csv.stat().st_size > 100:
        rows = count_csv_rows(places_csv)
        link_count = sum(1 for _ in open(links_file)) if links_file.exists() else 0
        print(f"  📊 Resume: places_data.csv has {rows} rows, links.txt has {link_count} links")
        logging.info(f"Resume: stage=place_data, rows={rows}, links={link_count}")
        return 2  # skip to get_place_data

    # Stage 1: search_query — if we have links.txt (search in progress or done)
    if links_file.exists() and links_file.stat().st_size > 0:
        link_count = sum(1 for _ in open(links_file))
        done_queries = sum(1 for _ in open(processed_q)) if processed_q.exists() else 0
        total_queries = sum(1 for _ in open(queries_file)) if queries_file.exists() else 0

        if total_queries > 0 and done_queries >= total_queries:
            print(f"  📊 Resume: search complete ({link_count} links, {done_queries}/{total_queries} queries)")
            logging.info(f"Resume: stage=place_data (search done), links={link_count}")
            return 2  # search done, skip to place_data
        else:
            print(f"  📊 Resume: search in progress ({link_count} links, {done_queries}/{total_queries} queries)")
            logging.info(f"Resume: stage=search_query, links={link_count}, queries={done_queries}/{total_queries}")
            return 1  # resume search_query

    # Stage 1: if queries file exists but no links yet
    if queries_file.exists():
        print(f"  📊 Resume: queries generated, starting search")
        logging.info("Resume: stage=search_query (queries exist)")
        return 1

    # Stage 0: fresh start
    print("  📊 Fresh start (no artifacts found)")
    logging.info("Resume: stage=fresh start")
    return 0


def run_script(name: str, script_path: Path, retries=2, cwd: Path | None = None, extra_env: dict | None = None):
    import os
    env = os.environ.copy()
    if extra_env:
        env.update(extra_env)

    for attempt in range(1, retries + 2):
        print(f"\n🚀 Running: {script_path} (attempt {attempt})")
        logging.info(f"Running: {script_path}, attempt {attempt}")
        try:
            subprocess.run(
                [sys.executable, str(script_path)],
                check=True,
                cwd=str(cwd) if cwd else None,
                env=env,
            )
            print(f"✅ {script_path} completed successfully.")
            logging.info(f"{script_path} completed successfully.")
            stage_done(name)
            return True
        except subprocess.CalledProcessError as e:
            print(f"❌ Error: {e}")
            logging.error(f"Error: {e}")
            if attempt < retries + 1:
                print("🔁 Retrying in 5s...")
                time.sleep(5)
            else:
                print("⚠️ Giving up on this script.")
                stage_failed(name, str(e))
                return False


def run_postprocess(input_csv: Path):
    postprocess_script = SOCIAL_DIR / "postprocess_places.py"
    if not postprocess_script.exists():
        print("⚠️ Postprocess script not found.")
        logging.warning("postprocess_places.py missing.")
        return

    print(f"\n🚀 Running postprocess: {postprocess_script} {input_csv}")
    logging.info(f"Running postprocess: {postprocess_script} {input_csv}")
    try:
        subprocess.run(
            [sys.executable, str(postprocess_script), str(input_csv)],
            check=True,
            cwd=str(SOCIAL_DIR),
        )
        print("✅ Postprocess completed.")
        logging.info("Postprocess completed.")
        stage_done("Postprocess")
    except subprocess.CalledProcessError as e:
        print(f"❌ Postprocess error: {e}")
        logging.error(f"Postprocess error: {e}")
        stage_failed("Postprocess", str(e))


if __name__ == "__main__":
    pipeline_start = time.time()

    # Load config from Telegram bot
    run_config = load_run_config()
    scrape_reviews = run_config.get("scrape_reviews", True)
    do_email_validation = run_config.get("email_validation", True)

    mode_parts = []
    if scrape_reviews:
        mode_parts.append("reviews")
    if do_email_validation:
        mode_parts.append("email validation")
    mode_label = ", ".join(mode_parts) if mode_parts else "scrape only"
    print(f"=== PIPELINE START — {mode_label} ===")
    logging.info(f"=== PIPELINE START — {mode_label} ===")
    notify(f"🟢 <b>Pipeline started</b> — {mode_label}")

    # Cleanup only with --clean flag
    force_clean = "--clean" in sys.argv
    print("\n🧹 Checking artifacts...")
    cleanup_artifacts(force=force_clean)

    # Detect resume state
    start_from = detect_resume_state()
    if start_from > 0:
        notify(f"🔄 <b>Resuming</b> from stage {start_from}: <code>{scripts[start_from][0]}</code>")

    total_places = 0
    social_found = 0
    pipeline_success = False

    # Pass review config to get_place_data.py via env var
    scrape_env = {"SCRAPE_REVIEWS": "true" if scrape_reviews else "false"}

    for name, script in scripts[start_from:]:
        ok = run_script(name, script, cwd=GMAPS_DIR, extra_env=scrape_env)
        if not ok:
            print(f"⛔ Stopping — {name} failed.")
            notify(f"⛔ <b>Pipeline stopped</b> — {name} failed")
            break
        time.sleep(2)
    else:
        gmaps_csv = GMAPS_DIR / "places_data.csv"
        if gmaps_csv.exists():
            total_places = count_csv_rows(gmaps_csv)
            target_csv = SOCIAL_DIR / "input.csv"
            shutil.copy2(gmaps_csv, target_csv)
            print(f"📁 Copied: {gmaps_csv} → {target_csv}")
            logging.info(f"Copied: {gmaps_csv} → {target_csv}")
            stage_done("GMaps → Social copy", f"📍 {total_places} places")
            pipeline_success = True

            # Sync to scraped_places master table (upsert by google_maps_url)
            try:
                sync_to_master_table(gmaps_csv)
            except Exception as e:
                logging.error(f"Master table sync error: {e}")
                print(f"⚠️ Master table sync failed: {e}")

            social_script = SOCIAL_DIR / "social_media_scraper.py"
            if social_script.exists():
                ok_social = run_script("Social media scrape", social_script, cwd=SOCIAL_DIR)
                if ok_social:
                    output_csv = SOCIAL_DIR / "output.csv"
                    if output_csv.exists():
                        social_found = count_csv_rows(output_csv)
                        run_postprocess(output_csv)
                    else:
                        print("⚠️ output.csv not found.")
                        logging.warning("output.csv missing.")
                        stage_failed("Social output", "output.csv missing")
                        pipeline_success = False
                else:
                    pipeline_success = False
            else:
                print("⚠️ Social script not found.")
        else:
            print("⚠️ places_data.csv not found.")
            logging.warning("places_data.csv missing.")
            stage_failed("GMaps output", "places_data.csv missing")

    duration = time.time() - pipeline_start
    pipeline_summary(total_places, social_found, duration)

    # ─── Email Find & Verify ───
    email_output = None
    cleared_csv = SOCIAL_DIR / "output_cleared.csv"
    if not do_email_validation:
        print("\n⏭️ Email validation skipped (config).")
        logging.info("Email validation skipped by config.")
    elif cleared_csv.exists() and cleared_csv.stat().st_size > 0:
        print("\n📧 Running email pipeline...")
        logging.info("Running email pipeline...")
        notify("📧 <b>Email pipeline started</b>")
        try:
            from email_pipeline import process_csv
            email_result = process_csv(
                str(cleared_csv),
                str(SOCIAL_DIR / "output_emails.csv"),
            )
            if email_result:
                email_output = Path(email_result["output_path"])
                stats = email_result
                msg = (
                    f"📧 <b>Email pipeline done</b>\n"
                    f"With email: {stats['with_email']}/{stats['total']}\n"
                    f"Found: {stats['find_stats']['found']} | Catch-all: {stats['find_stats']['catch_all']}\n"
                    f"Verified OK: {stats['verify_stats'].get('safe',0) + stats['verify_stats'].get('risky',0)}"
                )
                notify(msg)
                stage_done("Email pipeline")
            else:
                stage_failed("Email pipeline", "No result returned")
        except Exception as e:
            logging.error(f"Email pipeline error: {e}")
            stage_failed("Email pipeline", str(e))
    else:
        print("⚠️ No output_cleared.csv found, skipping email pipeline.")
        logging.warning("output_cleared.csv missing, skipping email pipeline.")

    # Send output files via Telegram
    from telegram_notify import send_file
    output_files = [
        GMAPS_DIR / "places_data.csv",
        SOCIAL_DIR / "output.csv",
        SOCIAL_DIR / "output_cleared.csv",
    ]
    if email_output and email_output.exists():
        output_files.append(email_output)

    for f in output_files:
        if f.exists() and f.stat().st_size > 0:
            send_file(str(f), f"📎 {f.name}")

    # Clean up run config (but NOT artifacts — those are needed for resume)
    if RUN_CONFIG_FILE.exists():
        RUN_CONFIG_FILE.unlink()

    # Clean up artifacts only after SUCCESSFUL full pipeline completion
    if pipeline_success:
        print("\n🧹 Pipeline complete — cleaning up artifacts for next run...")
        cleanup_artifacts(force=True)
    else:
        print("\n⚠️ Pipeline failed — keeping artifacts for resume on next run.")

    print("\n🏁 Done.")
