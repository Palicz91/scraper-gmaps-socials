"""
Cold Email Smartlead Push - Phase 5
Reads generated CSV, pushes leads to Smartlead campaigns (bucket = campaign).
"""

import csv
import json
import time
import logging
import os
import requests
from pathlib import Path

from cold_email_prompts import SEQ3_SUBJECT, SEQ3_BODY

log = logging.getLogger("smartlead")

# ─── Config ─────────────────────────────────────────────────

SMARTLEAD_API_KEY = os.environ.get("SMARTLEAD_API_KEY", "")
SMARTLEAD_BASE = "https://server.smartlead.ai/api/v1"

# Campaign IDs per bucket - set these after creating campaigns in Smartlead
CAMPAIGN_IDS = {
    "A": int(os.environ.get("SMARTLEAD_CAMPAIGN_A", "0")),
    "B": int(os.environ.get("SMARTLEAD_CAMPAIGN_B", "0")),
    "D": int(os.environ.get("SMARTLEAD_CAMPAIGN_D", "0")),
}

BATCH_SIZE = 100  # leads per API call (max 400)
RATE_LIMIT_DELAY = 1.5  # seconds between API calls


# ─── Smartlead API helpers ──────────────────────────────────

def _sl_post(path: str, data: dict) -> dict | None:
    """POST to Smartlead API."""
    url = f"{SMARTLEAD_BASE}{path}?api_key={SMARTLEAD_API_KEY}"
    try:
        r = requests.post(url, json=data, headers={"Content-Type": "application/json"}, timeout=30)
        if r.status_code == 200:
            return r.json()
        else:
            log.error(f"Smartlead API {r.status_code}: {r.text[:300]}")
            return None
    except Exception as e:
        log.error(f"Smartlead API error: {e}")
        return None


def _sl_get(path: str) -> dict | None:
    """GET from Smartlead API."""
    url = f"{SMARTLEAD_BASE}{path}?api_key={SMARTLEAD_API_KEY}"
    try:
        r = requests.get(url, timeout=30)
        if r.status_code == 200:
            return r.json()
        else:
            log.error(f"Smartlead API {r.status_code}: {r.text[:300]}")
            return None
    except Exception as e:
        log.error(f"Smartlead API error: {e}")
        return None


# ─── Campaign setup ─────────────────────────────────────────

def setup_campaign_sequences(campaign_id: int) -> bool:
    """
    Set up the 3-step sequence for a campaign.
    Seq1 & Seq2 use custom fields, Seq3 is fixed breakup.
    Campaign must be PAUSED to update sequences.
    """
    if not campaign_id:
        return False

    sequences = [
        {
            "id": None,
            "seq_number": 1,
            "subject": "{{ai_seq1_subject}}",
            "email_body": "{{ai_seq1_body}}",
            "seq_delay_details": {"delay_in_days": 0},
        },
        {
            "id": None,
            "seq_number": 2,
            "subject": "{{ai_seq2_subject}}",
            "email_body": "{{ai_seq2_body}}",
            "seq_delay_details": {"delay_in_days": 3},
        },
        {
            "id": None,
            "seq_number": 3,
            "subject": SEQ3_SUBJECT,
            "email_body": SEQ3_BODY,
            "seq_delay_details": {"delay_in_days": 4},
        },
    ]

    result = _sl_post(f"/campaigns/{campaign_id}/sequences", {"sequences": sequences})
    if result:
        log.info(f"Sequences set for campaign {campaign_id}")
        return True
    else:
        log.error(f"Failed to set sequences for campaign {campaign_id}")
        return False


# ─── Push leads ─────────────────────────────────────────────

def push_leads_to_campaign(campaign_id: int, leads: list[dict]) -> dict:
    """
    Push a batch of leads to a Smartlead campaign.
    Returns stats dict.
    """
    stats = {"added": 0, "skipped": 0, "failed": 0, "batches": 0}

    if not campaign_id or not SMARTLEAD_API_KEY:
        log.error("Missing campaign_id or API key")
        return stats

    # Build lead list in Smartlead format
    lead_list = []
    for lead in leads:
        sl_lead = {
            "email": lead["email"],
            "company_name": lead.get("place_name", ""),
            "website": lead.get("website", ""),
            "phone_number": lead.get("phone", ""),
            "location": f"{lead.get('city', '')}, {lead.get('country', '')}".strip(", "),
            "custom_fields": {
                "ai_seq1_subject": lead.get("ai_seq1_subject", ""),
                "ai_seq1_body": lead.get("ai_seq1_body", ""),
                "ai_seq2_subject": lead.get("ai_seq2_subject", ""),
                "ai_seq2_body": lead.get("ai_seq2_body", ""),
                "bucket": lead.get("bucket", ""),
                "segment": lead.get("bucket_name", ""),
                "rating": str(lead.get("rating", "")),
                "total_reviews": str(lead.get("total_reviews", "")),
                "unanswered": str(lead.get("est_unanswered", "")),
                "neg_unanswered": str(lead.get("neg_unanswered", "")),
                "place_name": lead.get("place_name", ""),
                "category": lead.get("category", ""),
            },
        }
        lead_list.append(sl_lead)

    # Send in batches
    for i in range(0, len(lead_list), BATCH_SIZE):
        batch = lead_list[i:i + BATCH_SIZE]
        stats["batches"] += 1

        payload = {
            "lead_list": batch,
            "settings": {
                "ignore_duplicate_leads_in_other_campaign": False,
                "ignore_global_block_list": False,
                "ignore_community_bounce_list": False,
                "return_lead_ids": True,
            },
        }

        result = _sl_post(f"/campaigns/{campaign_id}/leads", payload)
        if result and result.get("success"):
            added = result.get("added_count", 0)
            skipped = result.get("skipped_count", 0)
            stats["added"] += added
            stats["skipped"] += skipped
            log.info(f"Batch {stats['batches']}: {added} added, {skipped} skipped")
        else:
            stats["failed"] += len(batch)
            log.error(f"Batch {stats['batches']} failed for campaign {campaign_id}")

        if i + BATCH_SIZE < len(lead_list):
            time.sleep(RATE_LIMIT_DELAY)

    return stats


# ─── Main push flow ─────────────────────────────────────────

def push_csv_to_smartlead(input_path: str) -> dict:
    """
    Read generated CSV, group by bucket, push to respective Smartlead campaigns.
    Returns combined stats.
    """
    in_path = Path(input_path)
    if not in_path.exists():
        log.error(f"Input not found: {input_path}")
        return {}

    if not SMARTLEAD_API_KEY:
        log.error("SMARTLEAD_API_KEY not set")
        return {}

    # Check campaign IDs
    for bucket, cid in CAMPAIGN_IDS.items():
        if not cid:
            log.warning(f"No campaign ID for bucket {bucket} (SMARTLEAD_CAMPAIGN_{bucket})")

    # Read and group by bucket
    with open(in_path, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    buckets = {}
    for row in rows:
        b = row.get("bucket", "")
        if b not in buckets:
            buckets[b] = []
        buckets[b].append(row)

    # Push per bucket
    all_stats = {}
    total_added = 0
    total_failed = 0

    for bucket, leads in buckets.items():
        campaign_id = CAMPAIGN_IDS.get(bucket)
        if not campaign_id:
            log.warning(f"Skipping bucket {bucket}: no campaign ID configured")
            continue

        print(f"  📤 Pushing {len(leads)} leads to campaign {campaign_id} (bucket {bucket})...")
        stats = push_leads_to_campaign(campaign_id, leads)
        all_stats[bucket] = stats
        total_added += stats["added"]
        total_failed += stats["failed"]
        print(f"  ✓ Bucket {bucket}: {stats['added']} added, {stats['skipped']} skipped, {stats['failed']} failed")

    print(
        f"📤 Smartlead push done: {total_added} added, {total_failed} failed "
        f"across {len(buckets)} buckets"
    )
    log.info(f"Smartlead push complete: {json.dumps(all_stats)}")

    return {"total_leads": len(rows), "total_added": total_added, "total_failed": total_failed, "by_bucket": all_stats}


if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    if len(sys.argv) < 2:
        print("Usage: python cold_email_smartlead.py <generated.csv>")
        print("Env: SMARTLEAD_API_KEY, SMARTLEAD_CAMPAIGN_A, SMARTLEAD_CAMPAIGN_B, SMARTLEAD_CAMPAIGN_D")
        sys.exit(1)

    if "--setup-sequences" in sys.argv:
        for bucket, cid in CAMPAIGN_IDS.items():
            if cid:
                print(f"Setting up sequences for campaign {cid} (bucket {bucket})...")
                setup_campaign_sequences(cid)
    else:
        result = push_csv_to_smartlead(sys.argv[1])
        print(f"\nResult: {json.dumps(result, indent=2)}")
