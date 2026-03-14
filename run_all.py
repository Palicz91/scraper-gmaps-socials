import subprocess
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
        print("  ℹ️  Nothing to clean up.")
    else:
        print(f"  ✅ {deleted} files deleted.")
    logging.info(f"Cleanup: {deleted} files deleted.")


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
                ["python3", str(script_path)],
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
            ["python3", str(postprocess_script), str(input_csv)],
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

    mode_label = "with review analysis" if scrape_reviews else "fast mode (no reviews)"
    print(f"=== PIPELINE START — {mode_label} ===")
    logging.info(f"=== PIPELINE START — {mode_label} ===")
    notify(f"🟢 <b>Pipeline started</b> — {mode_label}")

    # Cleanup
    print("\n🧹 Cleaning up...")
    cleanup_artifacts()

    total_places = 0
    social_found = 0

    # Pass review config to get_place_data.py via env var
    scrape_env = {"SCRAPE_REVIEWS": "true" if scrape_reviews else "false"}

    for name, script in scripts:
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
    if cleared_csv.exists() and cleared_csv.stat().st_size > 0:
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

    # ─── Cold Email Pipeline (Classify → Generate → Smartlead) ───
    cold_email_csv = None
    email_source = email_output if email_output and email_output.exists() else cleared_csv
    if email_source and email_source.exists() and email_source.stat().st_size > 0:
        print("\n🎯 Running cold email pipeline...")
        logging.info("Cold email pipeline started")
        notify("🎯 <b>Cold email pipeline started</b>")

        try:
            # Phase 3: Classify
            from cold_email_classifier import classify_csv
            classified_path = str(BASE_DIR / "cold_email_classified.csv")
            classify_result = classify_csv(str(email_source), classified_path)

            if classify_result.get("classified", 0) > 0:
                msg = (
                    f"📊 <b>Classified</b>: {classify_result['classified']}/{classify_result['total']}\n"
                    f"A(burning)={classify_result['buckets']['A']} "
                    f"B(eroding)={classify_result['buckets']['B']} "
                    f"D(sleeping)={classify_result['buckets']['D']}\n"
                    f"Skipped: {classify_result['skipped']}"
                )
                notify(msg)
                stage_done("Classify leads")

                # Phase 4: Generate AI emails
                gemini_key = os.environ.get("GEMINI_API_KEY", "")
                if gemini_key:
                    from cold_email_generator import generate_emails_csv
                    generated_path = str(BASE_DIR / "cold_email_generated.csv")
                    gen_result = generate_emails_csv(classified_path, generated_path)

                    if gen_result.get("generated", 0) > 0:
                        cold_email_csv = Path(generated_path)
                        msg = (
                            f"📧 <b>AI emails generated</b>: {gen_result['generated']}/{gen_result['total']}\n"
                            f"Failed: {gen_result['failed']}\n"
                            f"A={gen_result['by_bucket'].get('A',0)} "
                            f"B={gen_result['by_bucket'].get('B',0)} "
                            f"D={gen_result['by_bucket'].get('D',0)}"
                        )
                        notify(msg)
                        stage_done("Generate AI emails")

                        # Phase 5: Push to Smartlead
                        sl_key = os.environ.get("SMARTLEAD_API_KEY", "")
                        if sl_key:
                            from cold_email_smartlead import push_csv_to_smartlead
                            push_result = push_csv_to_smartlead(generated_path)
                            msg = (
                                f"📤 <b>Smartlead push</b>: {push_result.get('total_added', 0)} added\n"
                                f"Failed: {push_result.get('total_failed', 0)}"
                            )
                            notify(msg)
                            stage_done("Smartlead push")
                        else:
                            print("⚠️ SMARTLEAD_API_KEY not set, skipping push.")
                            logging.warning("SMARTLEAD_API_KEY not set")
                    else:
                        stage_failed("Generate AI emails", f"0 generated out of {gen_result.get('total', 0)}")
                else:
                    print("⚠️ GEMINI_API_KEY not set, skipping AI email generation.")
                    logging.warning("GEMINI_API_KEY not set")
            else:
                print("⚠️ No leads classified, skipping email generation.")
                logging.info("No leads classified (all skipped)")
                notify(f"⚠️ No leads classified out of {classify_result.get('total', 0)} (all skipped)")

        except Exception as e:
            logging.error(f"Cold email pipeline error: {e}", exc_info=True)
            stage_failed("Cold email pipeline", str(e))

    # Add generated CSV to output files
    if cold_email_csv and cold_email_csv.exists():
        output_files.append(cold_email_csv)

    for f in output_files:
        if f.exists() and f.stat().st_size > 0:
            send_file(str(f), f"📎 {f.name}")

    # Clean up run config
    if RUN_CONFIG_FILE.exists():
        RUN_CONFIG_FILE.unlink()

    print("\n🏁 Done.")
