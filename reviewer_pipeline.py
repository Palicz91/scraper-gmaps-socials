"""
Reviewer Pipeline — Queue-based Worker
Runs on Hetzner VPS via cron (hourly).
1. Fetches pending items from reviewer_scrape_queue
2. For each: opens the restaurant's Maps page, finds reviewer's contrib link
3. Scrapes contrib page for all reviews
4. Writes results to reviewer_profiles + reviewer_reviews
5. Links back to reviews table via photo_hash → google_reviewer_id

Non-blocking: failures don't affect the core review manager flow.
"""

import os
import sys
import json
import re
import time
import random
import logging
import unicodedata
import fcntl

LOCK_FILE = "/tmp/reviewer_pipeline.lock"

def acquire_lock():
    """Prevent duplicate instances via flock."""
    lock_fd = open(LOCK_FILE, "w")
    try:
        fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        lock_fd.write(str(os.getpid()))
        lock_fd.flush()
        return lock_fd
    except BlockingIOError:
        print("Another reviewer pipeline instance is running. Exiting.")
        sys.exit(0)
from datetime import datetime, timezone
from pathlib import Path

log = logging.getLogger("reviewer_pipeline")

BATCH_LIMIT = 20  # max reviewers per cron run


def _get_supabase():
    from supabase import create_client
    url = os.environ.get("SUPABASE_URL", "")
    key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY", "")
    if not url or not key:
        raise ValueError("SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY required")
    return create_client(url, key)


def _send_telegram(text):
    import urllib.request
    bot_token = os.environ.get("TELEGRAM_BOT_TOKEN", "")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID", "")
    if not bot_token or not chat_id:
        return
    try:
        data = json.dumps({"chat_id": chat_id, "text": text}).encode()
        req = urllib.request.Request(
            f"https://api.telegram.org/bot{bot_token}/sendMessage",
            data=data,
            headers={"Content-Type": "application/json"},
        )
        urllib.request.urlopen(req, timeout=10)
    except Exception as e:
        log.error(f"Telegram send failed: {e}")


def extract_photo_hash(photo_url):
    """Extract ACg8oc... hash from Google profile photo URL."""
    if not photo_url:
        return None
    m = re.search(r'ACg8oc[A-Za-z0-9_-]+', photo_url)
    return m.group(0) if m else None


def _normalize_name(name):
    """Normalize a name for fuzzy comparison: lowercase, strip accents, collapse whitespace."""
    if not name:
        return ""
    # NFKD decomposition strips accents
    name = unicodedata.normalize("NFKD", name)
    name = "".join(c for c in name if not unicodedata.combining(c))
    name = name.lower().strip()
    name = re.sub(r'\s+', ' ', name)
    return name


def _fuzzy_name_match(name_a, name_b):
    """Check if two names match after normalization. Also handles partial matches."""
    a = _normalize_name(name_a)
    b = _normalize_name(name_b)
    if not a or not b:
        return False
    if a == b:
        return True
    # One name contains the other (handles "John D." vs "John D")
    if a in b or b in a:
        return True
    # Match if all words of the shorter name appear in the longer
    short, long_ = (a, b) if len(a) <= len(b) else (b, a)
    short_words = short.split()
    long_words = long_.split()
    if len(short_words) >= 2 and all(w in long_words for w in short_words):
        return True
    return False


def run_queue_worker():
    log.info("=== Reviewer Queue Worker START ===")

    try:
        supabase = _get_supabase()
    except Exception as e:
        log.error(f"Supabase connection failed: {e}")
        return

    # Fetch pending queue items
    result = supabase.from_("reviewer_scrape_queue") \
        .select("*") \
        .eq("status", "pending") \
        .order("created_at") \
        .limit(BATCH_LIMIT) \
        .execute()

    queue_items = result.data or []
    if not queue_items:
        log.info("No pending items in queue")
        return

    log.info(f"Processing {len(queue_items)} queue items")

    from reviewer_id_extractor import extract_ids_from_place, create_driver
    from contrib_scraper import scrape_contributor

    stats = {"processed": 0, "profiled": 0, "failed": 0, "skipped": 0}

    driver = create_driver()

    for item in queue_items:
        item_id = item["id"]
        photo_hash = item["photo_hash"]
        reviewer_name = item.get("reviewer_name", "")
        connection_id = item.get("connection_id")

        try:
            # Mark as processing
            supabase.from_("reviewer_scrape_queue") \
                .update({"status": "processing"}) \
                .eq("id", item_id) \
                .execute()

            # Get the business's Google Maps URL from the connection
            conn_result = supabase.from_("google_connections") \
                .select("location_name, maps_uri") \
                .eq("id", connection_id) \
                .single() \
                .execute()

            if not conn_result.data:
                log.warning(f"Connection {connection_id} not found, skipping")
                supabase.from_("reviewer_scrape_queue") \
                    .update({"status": "skipped", "error_message": "connection not found", "processed_at": datetime.now(timezone.utc).isoformat()}) \
                    .eq("id", item_id).execute()
                stats["skipped"] += 1
                continue

            maps_url = conn_result.data.get("maps_uri", "")
            place_name = conn_result.data.get("location_name", "")

            if not maps_url and not place_name:
                supabase.from_("reviewer_scrape_queue") \
                    .update({"status": "skipped", "error_message": "no maps URL or place name", "processed_at": datetime.now(timezone.utc).isoformat()}) \
                    .eq("id", item_id).execute()
                stats["skipped"] += 1
                continue

            # Step 1: Extract reviewer IDs from the place's review page
            log.info(f"Extracting reviewer IDs from {place_name or maps_url[:50]}")
            reviewers = extract_ids_from_place(
                maps_url or f"https://www.google.com/maps/search/{place_name.replace(' ', '+')}",
                max_scrolls=30,
                driver=driver,
                place_name=place_name,
            )

            # Step 2: Match reviewer by name (fuzzy) or photo_hash
            google_user_id = None
            match_method = None

            # Try name match first
            for r in reviewers:
                if _fuzzy_name_match(r.get("reviewer_name", ""), reviewer_name):
                    google_user_id = r["google_user_id"]
                    match_method = "name"
                    break

            # Fallback: match by photo_hash
            if not google_user_id and photo_hash:
                for r in reviewers:
                    if r.get("photo_hash") and r["photo_hash"] == photo_hash:
                        google_user_id = r["google_user_id"]
                        match_method = "photo_hash"
                        log.info(f"Photo hash match for '{reviewer_name}' → {google_user_id}")
                        break

            if not google_user_id:
                log.warning(f"Could not match reviewer '{reviewer_name}' in {place_name} (tried name + photo_hash among {len(reviewers)} reviewers)")
                supabase.from_("reviewer_scrape_queue") \
                    .update({"status": "failed", "error_message": f"no match among {len(reviewers)} reviewers (name+photo)", "processed_at": datetime.now(timezone.utc).isoformat()}) \
                    .eq("id", item_id).execute()
                stats["failed"] += 1
                time.sleep(random.uniform(3, 6))
                continue

            log.info(f"Matched {reviewer_name} → contrib ID {google_user_id} (via {match_method})")

            # Step 3: Scrape contributor profile
            contrib_result = scrape_contributor(google_user_id, driver=driver, max_scrolls=50)

            if not contrib_result:
                supabase.from_("reviewer_scrape_queue") \
                    .update({"status": "failed", "error_message": "contrib scrape returned null", "google_user_id": google_user_id, "processed_at": datetime.now(timezone.utc).isoformat()}) \
                    .eq("id", item_id).execute()
                stats["failed"] += 1
                time.sleep(random.uniform(3, 6))
                continue

            profile = contrib_result["profile"]
            reviews = contrib_result["reviews"]

            # Step 4: Upsert reviewer_profiles
            supabase.from_("reviewer_profiles").upsert({
                "google_user_id": google_user_id,
                "photo_hash": photo_hash,
                "display_name": profile.get("display_name", reviewer_name),
                "photo_url": profile.get("photo_url", ""),
                "total_review_count": profile.get("total_review_count", 0),
                "local_guide_level": profile.get("local_guide_level"),
                "scraped_at": datetime.now(timezone.utc).isoformat(),
                "updated_at": datetime.now(timezone.utc).isoformat(),
            }, on_conflict="google_user_id").execute()

            # Get reviewer_profile_id
            profile_result = supabase.from_("reviewer_profiles") \
                .select("id") \
                .eq("google_user_id", google_user_id) \
                .single().execute()
            profile_id = profile_result.data["id"] if profile_result.data else None

            # Step 5: Insert reviewer_reviews
            if profile_id and reviews:
                review_rows = [{
                    "reviewer_profile_id": profile_id,
                    "place_name": rev.get("place_name", ""),
                    "place_address": rev.get("place_address", ""),
                    "stars": rev.get("stars", 0),
                    "review_text": rev.get("review_text", ""),
                    "reviewed_at_text": rev.get("reviewed_at", ""),
                    "scraped_at": datetime.now(timezone.utc).isoformat(),
                } for rev in reviews]

                for i in range(0, len(review_rows), 50):
                    supabase.from_("reviewer_reviews").upsert(
                        review_rows[i:i+50],
                        on_conflict="reviewer_profile_id,place_name,reviewed_at_text"
                    ).execute()

            # Step 6: Link back to reviews table
            supabase.from_("reviews") \
                .update({
                    "google_reviewer_id": google_user_id,
                    "reviewer_profile_id": profile_id,
                }) \
                .eq("connection_id", connection_id) \
                .eq("reviewer_name", reviewer_name) \
                .execute()

            # Step 7: Mark queue item as completed
            supabase.from_("reviewer_scrape_queue") \
                .update({
                    "status": "completed",
                    "google_user_id": google_user_id,
                    "processed_at": datetime.now(timezone.utc).isoformat(),
                }) \
                .eq("id", item_id).execute()

            stats["profiled"] += 1
            stats["processed"] += 1
            log.info(f"✓ {reviewer_name}: {google_user_id} → {len(reviews)} reviews scraped")

            time.sleep(random.uniform(5, 10))

        except Exception as e:
            log.error(f"Error processing queue item {item_id}: {e}")
            try:
                supabase.from_("reviewer_scrape_queue") \
                    .update({"status": "failed", "error_message": str(e)[:500], "processed_at": datetime.now(timezone.utc).isoformat()}) \
                    .eq("id", item_id).execute()
            except:
                pass
            stats["failed"] += 1
            continue

    driver.quit()

    summary = (
        f"Reviewer pipeline kész\n"
        f"Processed: {stats['processed']}/{len(queue_items)}\n"
        f"Profiled: {stats['profiled']}\n"
        f"Failed: {stats['failed']}\n"
        f"Skipped: {stats['skipped']}"
    )
    log.info(summary)
    if stats["profiled"] > 0 or stats["failed"] > 0:
        _send_telegram(f"🔍 {summary}")

    log.info("=== Reviewer Queue Worker END ===")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

    lock_fd = acquire_lock()

    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        pass

    run_queue_worker()
