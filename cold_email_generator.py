"""
Cold Email Generator - Phase 4
Reads classified CSV, calls Gemini 3 Flash per lead, outputs CSV with AI-generated emails.
"""

import csv
import json
import time
import logging
import os
import requests
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

from cold_email_prompts import get_system_prompt, get_user_prompt, get_opener_for_index

log = logging.getLogger("email_gen")

# ─── Config ─────────────────────────────────────────────────

GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY", "")
GEMINI_MODEL = os.environ.get("GEMINI_MODEL", "gemini-2.5-flash")
GEMINI_URL = f"https://generativelanguage.googleapis.com/v1beta/models/{GEMINI_MODEL}:generateContent"

MAX_RETRIES = 3
RETRY_DELAY = 5
CONCURRENCY = 3  # parallel API calls
RATE_LIMIT_DELAY = 1.0  # seconds between calls (free tier: 15 RPM)
TEMPERATURE = 0.9
MAX_OUTPUT_TOKENS = 4096


# ─── Gemini API call ────────────────────────────────────────

def call_gemini(system_prompt: str, user_prompt: str) -> dict | None:
    """
    Call Gemini API, parse JSON response.
    Returns dict with seq1_subject, seq1_body, seq2_subject, seq2_body or None.
    """
    if not GEMINI_API_KEY:
        log.error("GEMINI_API_KEY not set")
        return None

    payload = {
        "contents": [
            {"role": "user", "parts": [{"text": user_prompt}]}
        ],
        "systemInstruction": {
            "parts": [{"text": system_prompt}]
        },
        "generationConfig": {
            "temperature": TEMPERATURE,
            "maxOutputTokens": MAX_OUTPUT_TOKENS,
            "responseMimeType": "application/json",
        },
    }

    for attempt in range(MAX_RETRIES):
        try:
            r = requests.post(
                f"{GEMINI_URL}?key={GEMINI_API_KEY}",
                json=payload,
                headers={"Content-Type": "application/json"},
                timeout=60,
            )

            if r.status_code == 429:
                wait = RETRY_DELAY * (attempt + 2)
                log.warning(f"Rate limited, waiting {wait}s")
                time.sleep(wait)
                continue

            if r.status_code != 200:
                log.error(f"Gemini API error {r.status_code}: {r.text[:300]}")
                if attempt < MAX_RETRIES - 1:
                    time.sleep(RETRY_DELAY)
                continue

            data = r.json()

            # Extract text from response
            text = ""
            try:
                text = data["candidates"][0]["content"]["parts"][0]["text"]
            except (KeyError, IndexError):
                log.error(f"Unexpected response structure: {json.dumps(data)[:300]}")
                continue

            # Parse JSON
            text = text.strip()
            # Strip markdown code fences if present
            if text.startswith("```"):
                text = text.split("\n", 1)[-1]
            if text.endswith("```"):
                text = text.rsplit("```", 1)[0]
            text = text.strip()

            result = json.loads(text)

            # Validate required fields
            required = ["seq1_subject", "seq1_body", "seq2_subject", "seq2_body"]
            if all(k in result and result[k] for k in required):
                return result
            else:
                log.warning(f"Missing fields in response: {list(result.keys())}")
                continue

        except json.JSONDecodeError as e:
            log.error(f"JSON parse error: {e}. Raw: {text[:200]}")
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_DELAY)
        except requests.exceptions.Timeout:
            log.error("Gemini API timeout")
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_DELAY)
        except Exception as e:
            log.error(f"Unexpected error: {e}")
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_DELAY)

    return None


# ─── Generate for single lead ───────────────────────────────

def generate_for_lead(lead: dict, lead_index: int) -> dict | None:
    """
    Generate seq1 + seq2 emails for one classified lead.
    Returns lead dict enriched with AI fields, or None on failure.
    """
    bucket = lead.get("bucket", "B")
    opener = get_opener_for_index(lead_index)

    system_prompt = get_system_prompt(bucket, opener)
    user_prompt = get_user_prompt(lead)

    result = call_gemini(system_prompt, user_prompt)
    if not result:
        log.error(f"Failed to generate for {lead.get('place_name', '?')}")
        return None

    # Word count validation
    seq1_words = len(result["seq1_body"].split())
    seq2_words = len(result["seq2_body"].split())

    if seq1_words > 80:
        log.warning(f"seq1 too long ({seq1_words}w) for {lead.get('place_name')}")
    if seq2_words > 60:
        log.warning(f"seq2 too long ({seq2_words}w) for {lead.get('place_name')}")

    return {
        **lead,
        "opener_style": opener,
        "ai_seq1_subject": result["seq1_subject"],
        "ai_seq1_body": result["seq1_body"],
        "ai_seq2_subject": result["seq2_subject"],
        "ai_seq2_body": result["seq2_body"],
    }


# ─── Batch process CSV ──────────────────────────────────────

def generate_emails_csv(input_path: str, output_path: str = None) -> dict:
    """
    Read classified CSV, generate AI emails for each lead, write output.
    Returns stats dict.
    """
    in_path = Path(input_path)
    if not in_path.exists():
        log.error(f"Input not found: {input_path}")
        return {"total": 0, "generated": 0, "failed": 0}

    if output_path is None:
        output_path = str(in_path.parent / (in_path.stem + "_emails_generated.csv"))

    # Read all leads
    with open(in_path, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        leads = list(reader)

    log.info(f"Generating emails for {len(leads)} leads")
    print(f"📧 Generating AI emails for {len(leads)} leads...")

    stats = {"total": len(leads), "generated": 0, "failed": 0, "by_bucket": {"A": 0, "B": 0, "D": 0}}
    results = []

    # Sequential with rate limiting (Gemini free tier: 15 RPM)
    for i, lead in enumerate(leads):
        place = lead.get("place_name", "?")
        bucket = lead.get("bucket", "?")

        result = generate_for_lead(lead, i)
        if result:
            results.append(result)
            stats["generated"] += 1
            stats["by_bucket"][bucket] = stats["by_bucket"].get(bucket, 0) + 1
            print(f"  ✓ [{i+1}/{len(leads)}] {place} (bucket {bucket})")
        else:
            stats["failed"] += 1
            print(f"  ✗ [{i+1}/{len(leads)}] {place} FAILED")

        # Rate limiting
        if i < len(leads) - 1:
            time.sleep(RATE_LIMIT_DELAY)

        # Progress log every 20
        if (i + 1) % 20 == 0:
            log.info(f"Progress: {i+1}/{len(leads)} | Generated: {stats['generated']} | Failed: {stats['failed']}")

    # Write output CSV
    if results:
        fieldnames = list(results[0].keys())
        with open(output_path, "w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(results)

    log.info(
        f"Email generation done: {stats['generated']}/{stats['total']} | "
        f"Failed: {stats['failed']} | Buckets: {stats['by_bucket']}"
    )
    print(
        f"📧 Done: {stats['generated']}/{stats['total']} generated | "
        f"Failed: {stats['failed']} | By bucket: A={stats['by_bucket'].get('A',0)} B={stats['by_bucket'].get('B',0)} D={stats['by_bucket'].get('D',0)}"
    )

    return {**stats, "output_path": output_path}


if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    if len(sys.argv) < 2:
        print("Usage: python cold_email_generator.py <classified.csv>")
        print("Env: GEMINI_API_KEY=your_key")
        sys.exit(1)
    result = generate_emails_csv(sys.argv[1])
    print(f"\nResult: {result}")
