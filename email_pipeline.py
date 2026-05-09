"""
Email Find & Verify Pipeline
Runs after the scraper — finds missing emails, verifies all.
Uses Reacher API directly on localhost for speed.
"""

import csv
import json
import time
import logging
import requests
from pathlib import Path
import os
from dotenv import load_dotenv

ENV_PATH = Path(__file__).resolve().parent / ".env"
load_dotenv(ENV_PATH)
from concurrent.futures import ThreadPoolExecutor, as_completed

REACHER_URL = os.environ.get("REACHER_URL", "http://localhost:9999/v0/check_email")
REACHER_SECRET = os.environ.get("REACHER_SECRET", "")

CONCURRENCY_FIND = 3
CONCURRENCY_VERIFY = 5

# Language-specific prefix lists — ordered by likelihood (most common first = faster find)
PREFIXES_EN = [
    "info", "hello", "contact", "reservations", "booking", "reserve",
    "eat", "dine", "events", "catering", "host",
    "manager", "admin", "office", "reception", "mail",
]

PREFIXES_HU = [
    "info", "hello", "foglalas", "asztalfoglalas", "etterem", "rendeles",
    "kapcsolat", "vendeglato", "recepcion", "szalloda", "panzio",
    "contact", "booking", "admin", "mail",
]

PREFIXES_DE = [
    "info", "kontakt", "reservierung", "buchung", "restaurant", "gastro",
    "hallo", "willkommen", "office", "empfang", "tisch",
    "contact", "hello", "admin", "mail", "booking",
]

# Country → prefix list mapping. Fallback: EN.
COUNTRY_PREFIXES = {
    "Hungary": PREFIXES_HU,
    "Magyarország": PREFIXES_HU,
    "Austria": PREFIXES_DE,
    "Germany": PREFIXES_DE,
    "Switzerland": PREFIXES_DE,
    "Liechtenstein": PREFIXES_DE,
    "Luxembourg": PREFIXES_DE,
}

def get_prefixes_for_country(country: str) -> list:
    """Get the appropriate email prefix list for a country. Fallback: EN."""
    if not country:
        return PREFIXES_EN
    return COUNTRY_PREFIXES.get(country.strip(), PREFIXES_EN)

SKIP_DOMAINS = {
    "facebook.com", "google.com", "instagram.com", "twitter.com", "tiktok.com",
    "youtube.com", "linkedin.com", "tripadvisor.com", "yelp.com", "line.me",
    "grab.com", "foodpanda.com", "wongnai.com", "agoda.com", "booking.com",
    "wolt.com", "amrest.eu", "accor.com", "szte.hu",
}

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
log = logging.getLogger("email_pipeline")


def check_email(email: str) -> dict:
    """Call Reacher API to check a single email."""
    try:
        r = requests.post(
            REACHER_URL,
            json={"to_email": email},
            headers={
                "Content-Type": "application/json",
                "x-reacher-secret": REACHER_SECRET,
            },
            timeout=120,
        )
        r.raise_for_status()
        return r.json()
    except Exception as e:
        return {"is_reachable": "error", "error": str(e)}


def extract_domain(url: str) -> str | None:
    """Extract clean domain from URL, skip social/aggregator sites."""
    if not url:
        return None
    d = url.strip().lower()
    d = d.replace("https://", "").replace("http://", "")
    d = d.replace("www.", "")
    d = d.split("/")[0].split(":")[0].split("?")[0]
    if not d or "." not in d:
        return None
    for skip in SKIP_DOMAINS:
        if d == skip or d.endswith("." + skip):
            return None
    return d


def clean_email(raw: str) -> str | None:
    """Clean scraped email field."""
    if not raw:
        return None
    e = raw.strip().split(",")[0].strip().split("?")[0].strip()
    if "@" not in e:
        return None
    return e.lower()


def find_email_for_domain(domain: str, country: str = "") -> dict:
    """Try to find a working email for a domain using country-specific prefixes."""
    # MX + catch-all check
    mx_result = check_email(f"test-mx-probe-99999@{domain}")

    if not mx_result.get("mx", {}).get("accepts_mail"):
        return {"found_email": None, "status": "no_mx", "tried": 1}

    if mx_result.get("smtp", {}).get("is_deliverable") or mx_result.get("smtp", {}).get("is_catch_all"):
        return {"found_email": f"info@{domain}", "status": "catch_all", "tried": 1}

    # Try country-specific prefixes
    prefixes = get_prefixes_for_country(country)
    for i, prefix in enumerate(prefixes):
        email = f"{prefix}@{domain}"
        result = check_email(email)
        reachable = result.get("is_reachable", "unknown")
        deliverable = result.get("smtp", {}).get("is_deliverable", False)

        if reachable == "safe" or (reachable == "risky" and deliverable):
            return {"found_email": email, "status": "found", "tried": i + 2}

    return {"found_email": None, "status": "not_found", "tried": len(prefixes) + 1}


def process_csv(input_path: str, output_path: str = None):
    """Main pipeline: read CSV, find emails, verify all, write results."""
    input_file = Path(input_path)
    if not input_file.exists():
        log.error(f"Input file not found: {input_path}")
        return

    if output_path is None:
        output_path = str(input_file.parent / (input_file.stem + "_emails.csv"))

    # Read CSV
    with open(input_file, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames
        rows = list(reader)

    log.info(f"Loaded {len(rows)} rows from {input_path}")

    # Check required columns
    has_email_col = "scraped_email" in fieldnames
    has_website_col = "website" in fieldnames

    if not has_email_col and not has_website_col:
        log.error("CSV needs at least 'website' or 'scraped_email' column")
        return

    # ─── PHASE 1: Find emails ───
    log.info("=== PHASE 1: Find emails ===")
    find_stats = {"scraped": 0, "found": 0, "catch_all": 0, "not_found": 0, "no_mx": 0, "skip": 0, "error": 0}

    for row in rows:
        scraped = clean_email(row.get("scraped_email", "")) if has_email_col else None
        if scraped:
            row["_final_email"] = scraped
            row["_email_source"] = "scraped"
            row["_find_status"] = "skip"
            find_stats["scraped"] += 1
        else:
            row["_final_email"] = None
            row["_email_source"] = "none"
            row["_find_status"] = "pending"

    # Find emails for rows without scraped email
    has_country_col = "country" in fieldnames
    needs_find = []
    for i, row in enumerate(rows):
        if row["_final_email"] is None and has_website_col:
            domain = extract_domain(row.get("website", ""))
            if domain:
                country = row.get("country", "") if has_country_col else ""
                needs_find.append((i, domain, country))
            else:
                row["_find_status"] = "skip_domain"
                find_stats["skip"] += 1

    log.info(f"  {find_stats['scraped']} scraped, {len(needs_find)} domains to search, {find_stats['skip']} skipped")

    if needs_find:
        with ThreadPoolExecutor(max_workers=CONCURRENCY_FIND) as pool:
            futures = {pool.submit(find_email_for_domain, domain, country): idx for idx, domain, country in needs_find}
            done_count = 0
            for future in as_completed(futures):
                idx = futures[future]
                try:
                    result = future.result()
                except Exception as e:
                    result = {"found_email": None, "status": "error", "tried": 0}

                row = rows[idx]
                row["_find_status"] = result["status"]
                if result["found_email"]:
                    row["_final_email"] = result["found_email"]
                    row["_email_source"] = "found"

                find_stats[result["status"]] = find_stats.get(result["status"], 0) + 1
                done_count += 1
                if done_count % 10 == 0:
                    log.info(f"  Find progress: {done_count}/{len(needs_find)}")

    log.info(f"  Find results: {json.dumps(find_stats)}")

    # ─── PHASE 2: Verify emails ───
    log.info("=== PHASE 2: Verify emails ===")
    verify_stats = {"safe": 0, "risky": 0, "invalid": 0, "unknown": 0, "error": 0}

    needs_verify = [(i, row["_final_email"]) for i, row in enumerate(rows) if row["_final_email"]]
    log.info(f"  {len(needs_verify)} emails to verify")

    if needs_verify:
        with ThreadPoolExecutor(max_workers=CONCURRENCY_VERIFY) as pool:
            futures = {pool.submit(check_email, email): idx for idx, email in needs_verify}
            done_count = 0
            for future in as_completed(futures):
                idx = futures[future]
                try:
                    result = future.result()
                except Exception as e:
                    result = {"is_reachable": "error"}

                row = rows[idx]
                status = result.get("is_reachable", "error")
                row["_verify_status"] = status
                row["_smtp_deliverable"] = result.get("smtp", {}).get("is_deliverable", "")
                row["_is_catch_all"] = result.get("smtp", {}).get("is_catch_all", "")
                row["_is_role_account"] = result.get("misc", {}).get("is_role_account", "")

                verify_stats[status] = verify_stats.get(status, 0) + 1
                done_count += 1
                if done_count % 10 == 0:
                    log.info(f"  Verify progress: {done_count}/{len(needs_verify)}")

    # Fill verify status for rows without email
    for row in rows:
        if "_verify_status" not in row:
            row["_verify_status"] = ""
            row["_smtp_deliverable"] = ""
            row["_is_catch_all"] = ""
            row["_is_role_account"] = ""

    log.info(f"  Verify results: {json.dumps(verify_stats)}")

    # ─── Write output CSV ───
    extra_cols = ["final_email", "email_source", "find_status", "verify_status", "smtp_deliverable", "is_catch_all", "is_role_account"]
    out_fieldnames = fieldnames + extra_cols

    with open(output_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=out_fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            row["final_email"] = row.pop("_final_email", "")
            row["email_source"] = row.pop("_email_source", "")
            row["find_status"] = row.pop("_find_status", "")
            row["verify_status"] = row.pop("_verify_status", "")
            row["smtp_deliverable"] = row.pop("_smtp_deliverable", "")
            row["is_catch_all"] = row.pop("_is_catch_all", "")
            row["is_role_account"] = row.pop("_is_role_account", "")
            writer.writerow(row)

    log.info(f"Output written to {output_path}")

    # ─── Write cleaned CSV (safe + risky only) ───
    cleaned_path = str(Path(output_path).parent / (Path(output_path).stem + "_cleaned.csv"))
    cleaned_count = 0
    with open(cleaned_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=out_fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            if not row.get("final_email"):
                continue
            if row.get("verify_status") in ("invalid", "error", "unknown", ""):
                continue
            writer.writerow(row)
            cleaned_count += 1

    log.info(f"Cleaned output written to {cleaned_path} ({cleaned_count} rows)")

    # Summary
    total = len(rows)
    with_email = sum(1 for r in rows if r.get("final_email"))
    verified_ok = verify_stats.get("safe", 0) + verify_stats.get("risky", 0)

    summary = (
        f"📧 Email Pipeline Complete\n"
        f"  Total: {total}\n"
        f"  With email: {with_email} ({round(with_email/total*100)}%)\n"
        f"  Scraped: {find_stats['scraped']} | Found: {find_stats['found']} | Catch-all: {find_stats['catch_all']}\n"
        f"  Verified OK: {verified_ok} | Invalid: {verify_stats['invalid']} | Unknown: {verify_stats['unknown']}\n"
        f"  Cleaned CSV: {cleaned_count} rows"
    )
    log.info(summary)
    print(summary)

    return {
        "output_path": output_path,
        "cleaned_path": cleaned_path,
        "total": total,
        "with_email": with_email,
        "cleaned_count": cleaned_count,
        "find_stats": find_stats,
        "verify_stats": verify_stats,
    }


if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print("Usage: python email_pipeline.py <input.csv> [output.csv]")
        sys.exit(1)
    input_csv = sys.argv[1]
    output_csv = sys.argv[2] if len(sys.argv) > 2 else None
    process_csv(input_csv, output_csv)
