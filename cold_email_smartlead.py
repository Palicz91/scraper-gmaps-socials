"""
Cold Email Smartlead Push - Phase 5
Single campaign, bucket differentiation via custom fields.
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
CAMPAIGN_ID = int(os.environ.get("SMARTLEAD_CAMPAIGN_ID", "0"))

BATCH_SIZE = 100  # leads per API call (max 400)
RATE_LIMIT_DELAY = 1.5  # seconds between API calls


# ─── Smartlead API helpers ──────────────────────────────────

def _sl_post(path: str, data: dict) -> dict | None:
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


# ─── Campaign setup ─────────────────────────────────────────

def setup_campaign_sequences(campaign_id: int = None) -> bool:
    """
    Set up the 3-step sequence. Campaign must be PAUSED.
    """
    cid = campaign_id or CAMPAIGN_ID
    if not cid:
        log.error("No campaign ID")
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

    result = _sl_post(f"/campaigns/{cid}/sequences", {"sequences": sequences})
    if result:
        log.info(f"Sequences set for campaign {cid}")
        print(f"✅ Sequences configured for campaign {cid}")
        return True
    else:
        log.error(f"Failed to set sequences for campaign {cid}")
        return False


# ─── Push leads ─────────────────────────────────────────────

def push_csv_to_smartlead(input_path: str) -> dict:
    """
    Read generated CSV, push all leads to single Smartlead campaign.
    Bucket tracked via custom field for filtering in analytics.
    """
    in_path = Path(input_path)
    if not in_path.exists():
        log.error(f"Input not found: {input_path}")
        return {"total": 0, "added": 0, "skipped": 0, "failed": 0}

    if not SMARTLEAD_API_KEY or not CAMPAIGN_ID:
        log.error("SMARTLEAD_API_KEY or SMARTLEAD_CAMPAIGN_ID not set")
        return {"total": 0, "added": 0, "skipped": 0, "failed": 0}

    # Read leads
    with open(in_path, "r", encoding="utf-8-sig") as f:
        leads = list(csv.DictReader(f))

    log.info(f"Pushing {len(leads)} leads to campaign {CAMPAIGN_ID}")
    print(f"📤 Pushing {len(leads)} leads to Smartlead campaign {CAMPAIGN_ID}...")

    # Build Smartlead lead list
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
    stats = {"total": len(lead_list), "added": 0, "skipped": 0, "failed": 0, "batches": 0}

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

        result = _sl_post(f"/campaigns/{CAMPAIGN_ID}/leads", payload)
        if result and result.get("success"):
            added = result.get("added_count", 0)
            skipped = result.get("skipped_count", 0)
            stats["added"] += added
            stats["skipped"] += skipped
            log.info(f"Batch {stats['batches']}: {added} added, {skipped} skipped")
        else:
            stats["failed"] += len(batch)
            log.error(f"Batch {stats['batches']} failed")

        if i + BATCH_SIZE < len(lead_list):
            time.sleep(RATE_LIMIT_DELAY)

    # Bucket breakdown for logging
    bucket_counts = {}
    for lead in leads:
        b = lead.get("bucket", "?")
        bucket_counts[b] = bucket_counts.get(b, 0) + 1

    print(
        f"📤 Done: {stats['added']} added, {stats['skipped']} skipped, {stats['failed']} failed\n"
        f"   Buckets: {' '.join(f'{k}={v}' for k, v in sorted(bucket_counts.items()))}"
    )
    log.info(f"Smartlead push complete: {json.dumps(stats)}")

    return stats


if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

    if len(sys.argv) < 2:
        print("Usage:")
        print("  python cold_email_smartlead.py <generated.csv>        # push leads")
        print("  python cold_email_smartlead.py --setup-sequences      # configure sequences")
        print("Env: SMARTLEAD_API_KEY, SMARTLEAD_CAMPAIGN_ID")
        sys.exit(1)

    if sys.argv[1] == "--setup-sequences":
        setup_campaign_sequences()
    else:
        result = push_csv_to_smartlead(sys.argv[1])
        print(f"\nResult: {json.dumps(result, indent=2)}")
