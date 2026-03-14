"""
Cold Email Classifier - Phase 3
Strict waterfall: first match wins, no overlaps.
Rule-based, no AI needed.
"""

import csv
import logging
from pathlib import Path

log = logging.getLogger("classifier")

# ─── Waterfall rules ───────────────────────────────────────

def classify_lead(row: dict) -> dict | None:
    """
    Classify a single lead row into a bucket.
    Returns dict with bucket + context, or None if SKIP.
    """
    rating = _float(row.get("rating"))
    total_reviews = _int(row.get("reviews"))
    est_unanswered = _int(row.get("est_unanswered"))
    unanswered_pct = _float(row.get("reviews_unanswered_pct"))
    neg_unanswered = _int(row.get("est_negative_unanswered"))
    neg_unanswered_pct = _float(row.get("negative_unanswered_pct"))

    est_s5 = _int(row.get("est_stars_5"))
    est_s4 = _int(row.get("est_stars_4"))
    est_s3 = _int(row.get("est_stars_3"))
    est_s2 = _int(row.get("est_stars_2"))
    est_s1 = _int(row.get("est_stars_1"))

    email = (row.get("final_email") or row.get("scraped_email") or "").strip()

    # ─── SKIP layer 1: no email ───
    if not email or "@" not in email:
        return None

    # ─── SKIP layer 2: not enough data ───
    if total_reviews < 50:
        return None

    # ─── SKIP layer 3: they're handling it ───
    if unanswered_pct < 25:
        return None

    # ─── SKIP layer 4: unanswered are mostly positive (5-star) ───
    if est_unanswered > 0 and est_s5 > 0:
        # Rough check: if 5-star ratio in loaded sample is > 70% of unanswered
        loaded = _int(row.get("reviews_loaded"))
        loaded_unanswered = _int(row.get("reviews_unanswered"))
        stars_5_raw = _int(row.get("stars_5"))
        if loaded_unanswered > 0 and stars_5_raw / loaded_unanswered > 0.7:
            if neg_unanswered < 3:
                return None

    # ─── Extract location context ───
    address = row.get("address", "")
    parts = [p.strip() for p in address.split(",") if p.strip()]
    street = parts[0] if len(parts) >= 1 else ""
    city = parts[-2] if len(parts) >= 3 else (parts[-1] if len(parts) >= 2 else "")
    country = row.get("country", "")

    base = {
        "email": email,
        "place_name": (row.get("simple_name") or row.get("name", "")).strip(),
        "category": row.get("category", "restaurant"),
        "street": street,
        "city": city,
        "country": country,
        "rating": rating,
        "total_reviews": total_reviews,
        "est_unanswered": est_unanswered,
        "unanswered_pct": unanswered_pct,
        "neg_unanswered": neg_unanswered,
        "neg_unanswered_pct": neg_unanswered_pct,
        "est_stars_5": est_s5,
        "est_stars_4": est_s4,
        "est_stars_3": est_s3,
        "est_stars_2": est_s2,
        "est_stars_1": est_s1,
        "website": row.get("website", ""),
        "phone": row.get("phone", ""),
    }

    # ─── Bucket A: "Burning" ───
    # Low rating + many unanswered negatives
    if rating < 4.0 and neg_unanswered >= 10:
        return {
            **base,
            "bucket": "A",
            "bucket_name": "burning",
            "pain": f"{neg_unanswered} negative reviews without a reply",
        }

    # ─── Bucket D: "Sleeping elite" ───
    # High rating but ignoring reviews
    if rating >= 4.5 and est_unanswered > 30:
        return {
            **base,
            "bucket": "D",
            "bucket_name": "sleeping",
            "pain": f"{rating} rating but ~{est_unanswered} reviews ignored",
        }

    # ─── Bucket B: "Eroding" ───
    # Catch-all for significant unanswered %
    if unanswered_pct > 50:
        return {
            **base,
            "bucket": "B",
            "bucket_name": "eroding",
            "pain": f"{unanswered_pct}% of reviews unanswered",
        }

    # ─── Remaining: SKIP (pain not sharp enough) ───
    return None


# ─── Batch process ──────────────────────────────────────────

def classify_csv(input_path: str, output_path: str = None) -> dict:
    """
    Read CSV, classify each row, write classified leads to output CSV.
    Returns stats dict.
    """
    in_path = Path(input_path)
    if not in_path.exists():
        log.error(f"Input not found: {input_path}")
        return {"total": 0, "classified": 0, "skipped": 0, "buckets": {}}

    if output_path is None:
        output_path = str(in_path.parent / (in_path.stem + "_classified.csv"))

    stats = {"total": 0, "classified": 0, "skipped": 0, "buckets": {"A": 0, "B": 0, "D": 0}}
    classified_rows = []

    with open(in_path, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            stats["total"] += 1
            result = classify_lead(row)
            if result is None:
                stats["skipped"] += 1
                continue
            stats["classified"] += 1
            stats["buckets"][result["bucket"]] += 1
            classified_rows.append(result)

    # Write output
    if classified_rows:
        fieldnames = list(classified_rows[0].keys())
        with open(output_path, "w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(classified_rows)

    log.info(
        f"Classified {stats['classified']}/{stats['total']} leads. "
        f"Buckets: A={stats['buckets']['A']}, B={stats['buckets']['B']}, D={stats['buckets']['D']}. "
        f"Skipped: {stats['skipped']}"
    )
    print(
        f"📊 Classified: {stats['classified']}/{stats['total']} | "
        f"A(burning)={stats['buckets']['A']} B(eroding)={stats['buckets']['B']} D(sleeping)={stats['buckets']['D']} | "
        f"Skipped: {stats['skipped']}"
    )

    return {**stats, "output_path": output_path}


# ─── Helpers ────────────────────────────────────────────────

def _int(val) -> int:
    try:
        return int(float(val or 0))
    except (ValueError, TypeError):
        return 0

def _float(val) -> float:
    try:
        return float(val or 0)
    except (ValueError, TypeError):
        return 0.0


if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    if len(sys.argv) < 2:
        print("Usage: python cold_email_classifier.py <input.csv>")
        sys.exit(1)
    result = classify_csv(sys.argv[1])
    print(f"\nResult: {result}")
