"""
Reviewer Pipeline — Parallel Queue Worker
Runs on Hetzner VPS via cron (every 30 min, staggered for 2 workers).
1. Claims pending items via FOR UPDATE SKIP LOCKED (no collisions)
2. For each: opens the restaurant's Maps page, finds reviewer's contrib link
3. Scrapes contrib page for all reviews
4. Writes results to reviewer_profiles + reviewer_reviews
5. Links back to reviews table via photo_hash → google_reviewer_id

Non-blocking: failures don't affect the core review manager flow.
Supports multiple parallel workers via --worker-id flag.
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
import signal
import argparse

from datetime import datetime, timezone
from pathlib import Path

log = logging.getLogger("reviewer_pipeline")

BATCH_LIMIT = 50
CACHE_DAYS = 180  # 6 months — don't re-scrape within this window
MAX_RETRIES = 2  # 2026-04-07: lowered from 3 — data showed only 1% marginal success on the 3rd attempt
MAX_RUNTIME_MINUTES = 60  # hard limit per worker run (lock prevents overlap)
PROCESSING_TIMEOUT_MINUTES = 30  # recover stuck "processing" items
MIN_DELAY = 8  # seconds between requests (rate limit safety)
MAX_DELAY = 15


def acquire_lock(worker_id):
    """Prevent duplicate instances of the same worker via flock."""
    lock_file = f"/tmp/reviewer_pipeline_w{worker_id}.lock"
    lock_fd = open(lock_file, "w")
    try:
        fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        lock_fd.write(str(os.getpid()))
        lock_fd.flush()
        return lock_fd
    except BlockingIOError:
        print(f"Worker {worker_id} already running. Exiting.")
        sys.exit(0)


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
    if not name:
        return ""
    name = unicodedata.normalize("NFKD", name)
    name = "".join(c for c in name if not unicodedata.combining(c))
    name = name.lower().strip()
    name = re.sub(r'\s+', ' ', name)
    return name


def _fuzzy_name_match(name_a, name_b):
    a = _normalize_name(name_a)
    b = _normalize_name(name_b)
    if not a or not b:
        return False
    if a == b:
        return True
    if a in b or b in a:
        return True
    short, long_ = (a, b) if len(a) <= len(b) else (b, a)
    short_words = short.split()
    long_words = long_.split()
    if len(short_words) >= 2 and all(w in long_words for w in short_words):
        return True
    return False


def _is_mostly_non_latin(name):
    """Return True if more than half of the non-space characters are outside
    the Latin script (Burmese, Thai, Arabic, CJK, etc.). Used to fail-fast on
    names that the scraped pool usually can't match."""
    if not name:
        return False
    chars = [c for c in name if not c.isspace() and c.isalpha()]
    if not chars:
        return False
    non_latin = 0
    for c in chars:
        try:
            n = unicodedata.name(c, "")
        except ValueError:
            continue
        if n and "LATIN" not in n:
            non_latin += 1
    return non_latin / len(chars) > 0.5


def claim_queue_items(supabase, worker_id, limit):
    """
    Claim pending items atomically via RPC (FOR UPDATE SKIP LOCKED).

    Connection selection is now ROUND-ROBIN (random pick among connections
    that have pending items), not biased toward the connection with the most
    pending items. The old top_conn logic caused head-of-line blocking:
    connections with many hard-to-match reviewers (e.g. Burmese names at
    YKKO MBK with a 2.7% match rate) dominated every run and starved the
    easy-to-match connections (e.g. Holey).
    """
    # Pick a random connection that has pending items.
    # We sample up to 500 pending rows to cover all active connections.
    try:
        sample = supabase.from_("reviewer_scrape_queue") \
            .select("connection_id") \
            .eq("status", "pending") \
            .limit(500) \
            .execute()
        if sample.data:
            distinct_conns = list({r["connection_id"] for r in sample.data if r.get("connection_id")})
            chosen_conn = random.choice(distinct_conns) if distinct_conns else None
        else:
            chosen_conn = None
    except Exception:
        chosen_conn = None

    log.info(f"W{worker_id}: claim picking connection {chosen_conn} (round-robin)")

    # Claim items for the chosen connection, oldest-created first.
    query = supabase.from_("reviewer_scrape_queue") \
        .select("*") \
        .eq("status", "pending") \
        .order("created_at") \
        .limit(limit)
    if chosen_conn:
        query = query.eq("connection_id", chosen_conn)
    result = query.execute()

    items = result.data or []
    claimed = []
    for item in items:
        # Try to claim (optimistic — another worker might grab it too)
        update_result = supabase.from_("reviewer_scrape_queue") \
            .update({"status": "processing", "processed_at": datetime.now(timezone.utc).isoformat()}) \
            .eq("id", item["id"]) \
            .eq("status", "pending") \
            .execute()
        # If status was still pending, we claimed it
        if update_result.data:
            claimed.append(item)

    return claimed


def run_queue_worker(worker_id=1):
    start_time = time.time()
    log.info(f"=== Reviewer Worker {worker_id} START ===")

    try:
        supabase = _get_supabase()
    except Exception as e:
        log.error(f"Supabase connection failed: {e}")
        return

    # Recover stuck processing items (older than 30 min)
    try:
        from datetime import timedelta
        cutoff_iso = (datetime.now(timezone.utc) - timedelta(minutes=PROCESSING_TIMEOUT_MINUTES)).isoformat()
        recovered = supabase.from_("reviewer_scrape_queue") \
            .update({"status": "pending", "error_message": f"recovered by worker {worker_id}"}) \
            .eq("status", "processing") \
            .lt("processed_at", cutoff_iso) \
            .execute()
        if recovered.data:
            log.info(f"Recovered {len(recovered.data)} stuck processing items")
    except Exception as e:
        log.warning(f"Recovery check failed: {e}")

    # Claim items
    queue_items = claim_queue_items(supabase, worker_id, BATCH_LIMIT)
    if not queue_items:
        log.info(f"Worker {worker_id}: no pending items")
        return

    log.info(f"Worker {worker_id}: claimed {len(queue_items)} items")

    from reviewer_id_extractor import extract_ids_from_place, create_driver
    from contrib_scraper import scrape_contributor

    stats = {"processed": 0, "profiled": 0, "failed": 0, "skipped": 0, "linked": 0}
    shutdown = False

    def handle_signal(sig, frame):
        nonlocal shutdown
        log.info(f"Worker {worker_id}: signal {sig}, finishing current item...")
        shutdown = True

    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)

    driver = create_driver()

    # Sort items by connection_id so same-location items are grouped together
    queue_items.sort(key=lambda x: x.get("connection_id", ""))

    # Location-level cache: {connection_id: {"reviewers": [...], "maps_url": ..., "place_name": ..., "fetched_at": time}}
    location_cache = {}
    LOCATION_CACHE_TTL = 1800  # 30 minutes

    for item in queue_items:
        # Check runtime limit
        elapsed = (time.time() - start_time) / 60
        if elapsed >= MAX_RUNTIME_MINUTES or shutdown:
            log.info(f"Worker {worker_id}: {'shutdown signal' if shutdown else f'time limit ({MAX_RUNTIME_MINUTES}m)'}, stopping")
            # Release unclaimed items back to pending
            for remaining in queue_items[queue_items.index(item):]:
                supabase.from_("reviewer_scrape_queue") \
                    .update({"status": "pending"}) \
                    .eq("id", remaining["id"]) \
                    .eq("status", "processing") \
                    .execute()
            break

        item_id = item["id"]
        photo_hash = item["photo_hash"]
        reviewer_name = item.get("reviewer_name", "")
        connection_id = item.get("connection_id")

        try:
            # Check location cache first
            cached = location_cache.get(connection_id)
            if cached and (time.time() - cached["fetched_at"]) < LOCATION_CACHE_TTL:
                reviewers = cached["reviewers"]
                maps_url = cached["maps_url"]
                place_name = cached["place_name"]
                log.info(f"W{worker_id}: Using cached IDs for {place_name} ({len(reviewers)} reviewers)")
            else:
                # Fetch connection info
                conn_result = supabase.from_("google_connections") \
                    .select("location_name, maps_uri") \
                    .eq("id", connection_id) \
                    .single() \
                    .execute()

                if not conn_result.data:
                    log.warning(f"W{worker_id}: Connection {connection_id} not found")
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

                # Step 1: Extract reviewer IDs — rotate sort order for better coverage
                retry_count = item.get("retry_count") or 0
                sort_orders = ["newest", "highest", "lowest"]
                sort_order = sort_orders[retry_count % len(sort_orders)]

                log.info(f"W{worker_id}: Extracting IDs from {place_name or maps_url[:50]} (sort: {sort_order})")
                new_reviewers = extract_ids_from_place(
                    maps_url or f"https://www.google.com/maps/search/{place_name.replace(' ', '+')}",
                    max_scrolls=30,
                    driver=driver,
                    place_name=place_name,
                    sort_order=sort_order,
                )

                # Merge with existing cache (accumulate reviewer IDs across sort orders)
                existing = location_cache.get(connection_id, {}).get("reviewers", [])
                existing_ids = {r["google_user_id"] for r in existing}
                merged = list(existing)
                for r in new_reviewers:
                    if r["google_user_id"] not in existing_ids:
                        merged.append(r)
                        existing_ids.add(r["google_user_id"])

                reviewers = merged
                location_cache[connection_id] = {
                    "reviewers": merged,
                    "maps_url": maps_url,
                    "place_name": place_name,
                    "fetched_at": time.time(),
                }
                log.info(f"W{worker_id}: {len(new_reviewers)} new + {len(existing)} cached = {len(merged)} total IDs for {place_name}")

            # Step 2: Match reviewer by name (fuzzy) or photo_hash
            google_user_id = None
            match_method = None

            for r in reviewers:
                if _fuzzy_name_match(r.get("reviewer_name", ""), reviewer_name):
                    google_user_id = r["google_user_id"]
                    match_method = "name"
                    break

            if not google_user_id and photo_hash:
                for r in reviewers:
                    if r.get("photo_hash") and r["photo_hash"] == photo_hash:
                        google_user_id = r["google_user_id"]
                        match_method = "photo_hash"
                        break

            if not google_user_id:
                retry_count = (item.get("retry_count") or 0) + 1
                # Invalidate location cache so next retry gets a fresh scroll
                if connection_id in location_cache:
                    del location_cache[connection_id]

                # Fail-fast for mostly-non-Latin names: if the name is dominated
                # by non-Latin script (Burmese, Thai, Arabic, CJK, etc.) and the
                # scraped reviewer pool for this connection doesn't surface it
                # on the first try, the chance of a later retry matching it is
                # very low. Cap non-Latin retries at 1 to stop clogging the queue.
                effective_max = 1 if _is_mostly_non_latin(reviewer_name) else MAX_RETRIES

                if retry_count >= effective_max:
                    supabase.from_("reviewer_scrape_queue") \
                        .update({"status": "permanently_failed", "error_message": f"no match after {retry_count} attempts among {len(reviewers)} reviewers (cap={effective_max})", "retry_count": retry_count, "processed_at": datetime.now(timezone.utc).isoformat()}) \
                        .eq("id", item_id).execute()
                else:
                    supabase.from_("reviewer_scrape_queue") \
                        .update({"status": "pending", "error_message": f"retry {retry_count}/{effective_max}: no match among {len(reviewers)} reviewers", "retry_count": retry_count, "processed_at": datetime.now(timezone.utc).isoformat()}) \
                        .eq("id", item_id).execute()
                stats["failed"] += 1
                time.sleep(random.uniform(3, 6))
                continue

            log.info(f"W{worker_id}: Matched {reviewer_name} → {google_user_id} (via {match_method})")

            # Step 2.5: Cross-connection cache check (use limit(1) instead of maybe_single to avoid 406)
            existing_profile_data = None
            try:
                ep_result = supabase.from_("reviewer_profiles") \
                    .select("id, scraped_at") \
                    .eq("google_user_id", google_user_id) \
                    .limit(1) \
                    .execute()
                if ep_result.data and len(ep_result.data) > 0:
                    existing_profile_data = ep_result.data[0]
            except Exception as e:
                log.warning(f"Profile cache check failed: {e}")

            if existing_profile_data and existing_profile_data.get("scraped_at"):
                scraped_dt = datetime.fromisoformat(existing_profile_data["scraped_at"].replace("Z", "+00:00"))
                age_days = (datetime.now(timezone.utc) - scraped_dt).days
                if age_days < CACHE_DAYS:
                    profile_id = existing_profile_data["id"]
                    log.info(f"W{worker_id}: Cache hit: {reviewer_name} scraped {age_days}d ago")

                    supabase.from_("reviews") \
                        .update({"google_reviewer_id": google_user_id, "reviewer_profile_id": profile_id}) \
                        .eq("connection_id", connection_id) \
                        .eq("reviewer_name", reviewer_name) \
                        .execute()

                    supabase.from_("reviewer_scrape_queue") \
                        .update({"status": "linked", "google_user_id": google_user_id, "processed_at": datetime.now(timezone.utc).isoformat()}) \
                        .eq("id", item_id).execute()

                    stats["linked"] += 1
                    stats["processed"] += 1
                    time.sleep(random.uniform(1, 3))
                    continue

            # Step 3: Scrape contributor profile
            contrib_result = scrape_contributor(google_user_id, driver=driver, max_scrolls=30)

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

            # Step 6: Link back
            supabase.from_("reviews") \
                .update({"google_reviewer_id": google_user_id, "reviewer_profile_id": profile_id}) \
                .eq("connection_id", connection_id) \
                .eq("reviewer_name", reviewer_name) \
                .execute()

            # Step 7: Mark completed
            supabase.from_("reviewer_scrape_queue") \
                .update({"status": "completed", "google_user_id": google_user_id, "processed_at": datetime.now(timezone.utc).isoformat()}) \
                .eq("id", item_id).execute()

            stats["profiled"] += 1
            stats["processed"] += 1
            log.info(f"W{worker_id}: ✓ {reviewer_name}: {google_user_id} → {len(reviews)} reviews")

            time.sleep(random.uniform(MIN_DELAY, MAX_DELAY))

        except Exception as e:
            log.error(f"W{worker_id}: Error on {item_id}: {e}")
            try:
                supabase.from_("reviewer_scrape_queue") \
                    .update({"status": "failed", "error_message": str(e)[:500], "processed_at": datetime.now(timezone.utc).isoformat()}) \
                    .eq("id", item_id).execute()
            except:
                pass
            stats["failed"] += 1
            continue

    try:
        driver.quit()
    except:
        pass

    elapsed_min = round((time.time() - start_time) / 60, 1)
    total_attempted = stats['processed'] + stats['failed']
    match_rate = round(stats['processed'] / total_attempted * 100) if total_attempted > 0 else 0
    summary = (
        f"Reviewer W{worker_id} kész ({elapsed_min}m)\n"
        f"Processed: {stats['processed']}/{len(queue_items)}\n"
        f"Profiled: {stats['profiled']}\n"
        f"Linked (cache): {stats['linked']}\n"
        f"Failed: {stats['failed']}\n"
        f"Skipped: {stats['skipped']}\n"
        f"Match rate: {match_rate}%"
    )
    log.info(summary)
    # Only send Telegram on errors/low match rate — routine stats in daily health check
    if total_attempted >= 5 and match_rate < 30:
        _send_telegram(f"⚠️ Reviewer W{worker_id}: low match rate ({match_rate}%) — {stats['failed']}/{total_attempted} failed")

    log.info(f"=== Reviewer Worker {worker_id} END ===")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--worker-id", type=int, default=1, help="Worker ID (1 or 2)")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format=f"%(asctime)s - W{args.worker_id} - %(levelname)s - %(message)s"
    )

    lock_fd = acquire_lock(args.worker_id)

    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        pass

    run_queue_worker(worker_id=args.worker_id)
