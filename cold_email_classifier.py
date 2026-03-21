"""
Cold Email Classifier - LRPI Model B Scoring
Replaces the simple if/else bucket system with the weighted 7-component
LRPI-Lite model. Computes cluster benchmarks from the batch, scores each
lead 0-100, assigns bands, and routes to email buckets.

Formula: LRPI_Lite = C01*0.25 + C15*0.20 + C04*0.15 + C07*0.12
                    + C05*0.10 + C11*0.10 + C08*0.08

Bands (from Excel data):
  Critical:  < 25    → bucket A (burning)
  Poor:      25-45   → bucket A (burning)
  Average:   46-65   → bucket B (eroding) if est_neg_unanswered >= 5
  Good:      66-80   → SKIP
  Excellent: 81+     → SKIP
"""

import csv
import math
import logging
from pathlib import Path
from collections import defaultdict

log = logging.getLogger("classifier")


# ─── LRPI Model B weights ──────────────────────────────────

WEIGHTS = {
    "C01": 0.25,  # Response rate
    "C15": 0.20,  # BSS-lite (inverted)
    "C04": 0.15,  # Est unanswered neg (scored)
    "C07": 0.12,  # Positive concentration
    "C05": 0.10,  # Rating vs cluster avg
    "C11": 0.10,  # Volume percentile
    "C08": 0.08,  # Neg ratio (inverted)
}

BAND_CUTOFFS = [
    (25, "Critical"),
    (45, "Poor"),
    (65, "Average"),
    (80, "Good"),
    (999, "Excellent"),
]

# Band → email bucket mapping
BAND_TO_BUCKET = {
    "Critical": ("A", "burning"),
    "Poor": ("A", "burning"),
    "Average": ("B", "eroding"),  # only if est_neg >= 5
    "Good": None,       # skip
    "Excellent": None,  # skip
}


# ─── Category → parent cluster lookup ──────────────────────

CATEGORY_TO_CLUSTER = {
    "Restaurant": "Restaurant",
    "Italian restaurant": "Restaurant",
    "Sushi restaurant": "Restaurant",
    "Mexican restaurant": "Restaurant",
    "Chinese restaurant": "Restaurant",
    "Thai restaurant": "Restaurant",
    "Indian restaurant": "Restaurant",
    "Pizza restaurant": "Restaurant",
    "Seafood restaurant": "Restaurant",
    "Steakhouse": "Restaurant",
    "Fine dining restaurant": "Restaurant",
    "Fast food restaurant": "Fast food and takeaway",
    "Buffet restaurant": "Restaurant",
    "Brunch restaurant": "Restaurant",
    "Vegan restaurant": "Restaurant",
    "Vegetarian restaurant": "Restaurant",
    "BBQ restaurant": "Restaurant",
    "Mediterranean restaurant": "Restaurant",
    "Ramen restaurant": "Restaurant",
    "Hamburger restaurant": "Restaurant",
    "Korean restaurant": "Restaurant",
    "Vietnamese restaurant": "Restaurant",
    "Japanese restaurant": "Restaurant",
    "French restaurant": "Restaurant",
    "Greek restaurant": "Restaurant",
    "Spanish restaurant": "Restaurant",
    "Peruvian restaurant": "Restaurant",
    "Brazilian restaurant": "Restaurant",
    "Ethiopian restaurant": "Restaurant",
    "Lebanese restaurant": "Restaurant",
    "Turkish restaurant": "Restaurant",
    "Afghan restaurant": "Restaurant",
    "Cuban restaurant": "Restaurant",
    "Gastropub": "Restaurant",
    "Soul food restaurant": "Restaurant",
    "Tapas restaurant": "Restaurant",
    "Diner": "Restaurant",
    "Family restaurant": "Restaurant",
    "Fish and chips restaurant": "Restaurant",
    "Fondue restaurant": "Restaurant",
    "Food court": "Restaurant",
    "Cafe": "Cafe and coffee shop",
    "Coffee shop": "Cafe and coffee shop",
    "Espresso bar": "Cafe and coffee shop",
    "Tea house": "Cafe and coffee shop",
    "Bubble tea shop": "Cafe and coffee shop",
    "Juice bar": "Cafe and coffee shop",
    "Smoothie shop": "Cafe and coffee shop",
    "Internet cafe": "Cafe and coffee shop",
    "Cat cafe": "Cafe and coffee shop",
    "Dessert shop": "Cafe and coffee shop",
    "Ice cream shop": "Cafe and coffee shop",
    "Frozen yogurt shop": "Cafe and coffee shop",
    "Bakery": "Cafe and coffee shop",
    "Patisserie": "Cafe and coffee shop",
    "Donut shop": "Cafe and coffee shop",
    "Cupcake shop": "Cafe and coffee shop",
    "Bar": "Bar and nightlife",
    "Wine bar": "Bar and nightlife",
    "Cocktail bar": "Bar and nightlife",
    "Sports bar": "Bar and nightlife",
    "Pub": "Bar and nightlife",
    "Beer garden": "Bar and nightlife",
    "Brewery": "Bar and nightlife",
    "Nightclub": "Bar and nightlife",
    "Karaoke bar": "Bar and nightlife",
    "Hookah bar": "Bar and nightlife",
    "Lounge": "Bar and nightlife",
    "Comedy club": "Bar and nightlife",
    "Live music venue": "Bar and nightlife",
    "Winery": "Bar and nightlife",
    "Pizza delivery": "Fast food and takeaway",
    "Sandwich shop": "Fast food and takeaway",
    "Hot dog stand": "Fast food and takeaway",
    "Food truck": "Fast food and takeaway",
    "Kebab shop": "Fast food and takeaway",
    "Fried chicken restaurant": "Fast food and takeaway",
    "Takeout restaurant": "Fast food and takeaway",
    "Dentist": "Dentist",
    "Dental clinic": "Dentist",
    "Cosmetic dentist": "Dentist",
    "Pediatric dentist": "Dentist",
    "Orthodontist": "Dentist",
    "Oral surgeon": "Dentist",
    "Endodontist": "Dentist",
    "Periodontist": "Dentist",
    "Emergency dental service": "Dentist",
    "Teeth whitening service": "Dentist",
    "Dental implants provider": "Dentist",
    "Doctor": "Doctor and medical practice",
    "General practitioner": "Doctor and medical practice",
    "Family practice physician": "Doctor and medical practice",
    "Urgent care center": "Doctor and medical practice",
    "Walk-in clinic": "Doctor and medical practice",
    "Medical clinic": "Doctor and medical practice",
    "Medical center": "Doctor and medical practice",
    "Health clinic": "Doctor and medical practice",
    "Cardiologist": "Medical specialist",
    "Dermatologist": "Medical specialist",
    "Ophthalmologist": "Medical specialist",
    "Orthopedic surgeon": "Medical specialist",
    "Plastic surgeon": "Medical specialist",
    "Chiropractor": "Medical specialist",
    "Physiotherapist": "Medical specialist",
    "Physical therapy clinic": "Medical specialist",
    "Acupuncturist": "Medical specialist",
    "Optometrist": "Medical specialist",
    "Podiatrist": "Medical specialist",
    "Psychologist": "Mental health and therapy",
    "Psychiatrist": "Mental health and therapy",
    "Counselor": "Mental health and therapy",
    "Therapist": "Mental health and therapy",
    "Marriage counselor": "Mental health and therapy",
    "Auto repair shop": "Auto repair and service",
    "Car repair": "Auto repair and service",
    "Mechanic": "Auto repair and service",
    "Auto body shop": "Auto repair and service",
    "Brake shop": "Auto repair and service",
    "Tire shop": "Auto repair and service",
    "Oil change service": "Auto repair and service",
    "Transmission shop": "Auto repair and service",
    "Auto glass shop": "Auto repair and service",
    "Muffler shop": "Auto repair and service",
    "Car dealer": "Car dealer",
    "Used car dealer": "Car dealer",
    "Motorcycle dealer": "Car dealer",
    "Truck dealer": "Car dealer",
    "RV dealer": "Car dealer",
    "Car wash": "Car wash and detailing",
    "Auto detailing service": "Car wash and detailing",
    "Hair salon": "Hair salon and barber",
    "Barber shop": "Hair salon and barber",
    "Beauty salon": "Hair salon and barber",
    "Spa": "Spa and wellness",
    "Day spa": "Spa and wellness",
    "Massage therapist": "Spa and wellness",
    "Nail salon": "Spa and wellness",
    "Waxing salon": "Spa and wellness",
    "Tanning salon": "Spa and wellness",
    "Gym": "Fitness and gym",
    "Fitness center": "Fitness and gym",
    "CrossFit gym": "Fitness and gym",
    "Yoga studio": "Fitness and gym",
    "Pilates studio": "Fitness and gym",
    "Personal trainer": "Fitness and gym",
    "Martial arts school": "Fitness and gym",
    "Hotel": "Hotel and lodging",
    "Motel": "Hotel and lodging",
    "Hostel": "Hotel and lodging",
    "Resort": "Hotel and lodging",
    "Boutique hotel": "Hotel and lodging",
    "Bed and breakfast": "Hotel and lodging",
    "Inn": "Hotel and lodging",
    "Lodge": "Hotel and lodging",
    "Vacation rental": "Vacation and short-term rental",
    "Campground": "Vacation and short-term rental",
    "RV park": "Vacation and short-term rental",
    "Plumber": "Home repair and trades",
    "Electrician": "Home repair and trades",
    "HVAC contractor": "Home repair and trades",
    "Roofer": "Home repair and trades",
    "Painter": "Home repair and trades",
    "Handyman": "Home repair and trades",
    "Locksmith": "Home repair and trades",
    "Carpenter": "Home repair and trades",
    "Pest control service": "Home repair and trades",
    "Garage door supplier": "Home repair and trades",
    "Fence contractor": "Home repair and trades",
    "House cleaning service": "Cleaning service",
    "Commercial cleaning service": "Cleaning service",
    "Carpet cleaning service": "Cleaning service",
    "Window cleaning service": "Cleaning service",
    "Landscaper": "Landscaping and outdoor",
    "Lawn care service": "Landscaping and outdoor",
    "Tree service": "Landscaping and outdoor",
    "Moving company": "Moving and storage",
    "Storage facility": "Moving and storage",
    "Lawyer": "Legal services",
    "Law firm": "Legal services",
    "Attorney": "Legal services",
    "Personal injury attorney": "Legal services",
    "Family lawyer": "Legal services",
    "Criminal lawyer": "Legal services",
    "Immigration lawyer": "Legal services",
    "Accountant": "Financial services",
    "Tax preparer": "Financial services",
    "Financial planner": "Financial services",
    "Insurance agent": "Financial services",
    "Mortgage broker": "Financial services",
    "Real estate agent": "Real estate",
    "Real estate agency": "Real estate",
    "Property management company": "Real estate",
    "Clothing store": "Retail store",
    "Shoe store": "Retail store",
    "Jewelry store": "Retail store",
    "Electronics store": "Retail store",
    "Furniture store": "Retail store",
    "Pet store": "Retail store",
    "Hardware store": "Retail store",
    "Grocery store": "Retail store",
    "Supermarket": "Retail store",
    "Convenience store": "Retail store",
    "Florist": "Retail store",
    "Liquor store": "Retail store",
    "Book store": "Retail store",
    "School": "Education and tutoring",
    "Preschool": "Education and tutoring",
    "Daycare": "Education and tutoring",
    "Tutoring service": "Education and tutoring",
    "Language school": "Education and tutoring",
    "Driving school": "Education and tutoring",
    "Veterinarian": "Pet services",
    "Animal hospital": "Pet services",
    "Pet groomer": "Pet services",
    "Dog trainer": "Pet services",
    "Movie theater": "Entertainment and recreation",
    "Bowling alley": "Entertainment and recreation",
    "Escape room": "Entertainment and recreation",
    "Amusement park": "Entertainment and recreation",
    "Museum": "Entertainment and recreation",
    "Zoo": "Entertainment and recreation",
    "Dry cleaner": "Personal services",
    "Laundromat": "Personal services",
    "Tailor": "Personal services",
    "Phone repair": "Personal services",
    "Computer repair": "Personal services",
    "Photographer": "Personal services",
    "Tattoo shop": "Personal services",
    "Taxi service": "Transportation and logistics",
    "Towing service": "Transportation and logistics",
    "Car rental": "Transportation and logistics",
}


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

def _clamp(val, lo=0, hi=100):
    return max(lo, min(hi, val))

def _get_cluster(category: str) -> str:
    return CATEGORY_TO_CLUSTER.get(category, "Restaurant")

def _assign_band(score: float) -> str:
    for cutoff, band in BAND_CUTOFFS:
        if score < cutoff:
            return band
    return "Excellent"


# ─── LRPI Model B components ───────────────────────────────

def calc_c01_response_rate(answered: int, total: int) -> float:
    """C01: Response rate (0-100). Higher = better."""
    if total <= 0:
        return 0
    return _clamp((answered / total) * 100)


def calc_c04_est_neg_unanswered(stars_1: int, stars_2: int, unanswered: int, total: int) -> tuple[float, int]:
    """C04: Est unanswered negatives scored (0-100). Also returns raw count."""
    neg_total = stars_1 + stars_2
    if total <= 0 or neg_total <= 0:
        return 100, 0
    non_response_rate = unanswered / total if total > 0 else 0
    est_neg_unanswered = round(neg_total * non_response_rate)
    scored = _clamp(100 - min(est_neg_unanswered / 20, 1) * 100)
    return scored, est_neg_unanswered


def calc_c05_rating_vs_cluster(rating: float, cluster_avg_rating: float) -> float:
    """C05: Rating vs cluster average (0-100). 50 = at average."""
    if cluster_avg_rating <= 0:
        return 50
    delta = rating - cluster_avg_rating
    return _clamp(50 + (delta / 2) * 50)


def calc_c07_positive_concentration(stars_4: int, stars_5: int, total: int) -> float:
    """C07: Positive concentration (0-100). % of reviews that are 4-5 star."""
    if total <= 0:
        return 0
    return _clamp((stars_4 + stars_5) / total * 100)


def calc_c08_neg_ratio_inverted(stars_1: int, stars_2: int, total: int) -> float:
    """C08: Negative ratio inverted (0-100). 4x multiplier. Lower neg = higher score."""
    if total <= 0:
        return 100
    neg_ratio = (stars_1 + stars_2) / total
    return _clamp(100 - min(neg_ratio * 4, 1) * 100)


def calc_c11_volume_percentile(total: int, all_cluster_totals: list[int]) -> float:
    """C11: Volume percentile within cluster (0-100)."""
    if not all_cluster_totals or total <= 0:
        return 0
    below = sum(1 for t in all_cluster_totals if t < total)
    return _clamp(below / max(len(all_cluster_totals), 1) * 100)


def calc_c15_bss_lite_inverted(unanswered: int, total: int, est_neg_unanswered: int, neg_total: int) -> float:
    """C15: BSS-lite inverted (0-100). Higher = less backlog severity."""
    vol_penalty = min(unanswered / 50, 1) * 100
    vol_severity = (unanswered / total * 100) if total > 0 else 0
    neg_exposure = (est_neg_unanswered / max(neg_total, 1) * 100) if neg_total > 0 else 0
    bss = vol_penalty * 0.40 + vol_severity * 0.35 + neg_exposure * 0.25
    return _clamp(100 - bss)


# ─── Cluster benchmark computation ─────────────────────────

def compute_cluster_benchmarks(rows: list[dict]) -> dict:
    """
    First pass: compute per-cluster averages from the batch.
    Returns {cluster_name: {avg_rating, avg_response_rate, review_volumes: [...]}}
    """
    clusters = defaultdict(lambda: {
        "ratings": [], "response_rates": [], "review_volumes": [],
    })

    for row in rows:
        total = _int(row.get("reviews"))
        if total < 10:
            continue
        category = row.get("category", "")
        cluster = _get_cluster(category)
        rating = _float(row.get("rating"))
        answered = _int(row.get("reviews_answered") or row.get("est_unanswered", 0))

        # If we have reviews_answered, use it. Otherwise estimate from unanswered_pct
        unanswered_pct = _float(row.get("reviews_unanswered_pct"))
        if answered == 0 and unanswered_pct > 0:
            answered = round(total * (1 - unanswered_pct / 100))
        response_rate = (answered / total * 100) if total > 0 else 0

        if rating > 0:
            clusters[cluster]["ratings"].append(rating)
        clusters[cluster]["response_rates"].append(response_rate)
        clusters[cluster]["review_volumes"].append(total)

    # Compute averages
    benchmarks = {}
    for cluster, data in clusters.items():
        benchmarks[cluster] = {
            "avg_rating": sum(data["ratings"]) / len(data["ratings"]) if data["ratings"] else 4.0,
            "avg_response_rate": sum(data["response_rates"]) / len(data["response_rates"]) if data["response_rates"] else 25.0,
            "review_volumes": sorted(data["review_volumes"]),
            "count": len(data["review_volumes"]),
        }

    return benchmarks


# ─── Score a single lead ───────────────────────────────────

def score_lead(row: dict, benchmarks: dict) -> dict | None:
    """
    Compute LRPI-Lite score for one lead.
    Returns enriched dict with score, band, components, or None if SKIP.
    """
    email = (row.get("final_email") or row.get("scraped_email") or "").strip()
    total = _int(row.get("reviews"))
    rating = _float(row.get("rating"))
    category = row.get("category", "")

    # ─── Hard skips ───
    if not email or "@" not in email:
        return None
    if total < 50:
        return None

    # ─── Parse fields ───
    answered = _int(row.get("reviews_answered"))
    unanswered = _int(row.get("reviews_unanswered") or row.get("est_unanswered"))
    unanswered_pct = _float(row.get("reviews_unanswered_pct"))

    # Star counts (use est_ if raw not available)
    s1 = _int(row.get("est_stars_1") or row.get("stars_1"))
    s2 = _int(row.get("est_stars_2") or row.get("stars_2"))
    s3 = _int(row.get("est_stars_3") or row.get("stars_3"))
    s4 = _int(row.get("est_stars_4") or row.get("stars_4"))
    s5 = _int(row.get("est_stars_5") or row.get("stars_5"))

    # If no answered count, estimate from pct
    if answered == 0 and unanswered_pct > 0 and total > 0:
        answered = round(total * (1 - unanswered_pct / 100))
        unanswered = total - answered

    # If star data missing, estimate from rating
    star_total = s1 + s2 + s3 + s4 + s5
    if star_total == 0 and total > 0 and rating > 0:
        # Rough estimate based on rating
        if rating >= 4.5:
            s5, s4, s3, s2, s1 = [round(total * p) for p in [0.65, 0.20, 0.08, 0.04, 0.03]]
        elif rating >= 4.0:
            s5, s4, s3, s2, s1 = [round(total * p) for p in [0.50, 0.25, 0.12, 0.07, 0.06]]
        elif rating >= 3.5:
            s5, s4, s3, s2, s1 = [round(total * p) for p in [0.35, 0.20, 0.15, 0.15, 0.15]]
        else:
            s5, s4, s3, s2, s1 = [round(total * p) for p in [0.20, 0.15, 0.15, 0.20, 0.30]]

    neg_total = s1 + s2

    # ─── Cluster context ───
    cluster = _get_cluster(category)
    bench = benchmarks.get(cluster, benchmarks.get("Restaurant", {
        "avg_rating": 4.0, "avg_response_rate": 25.0, "review_volumes": [100], "count": 1,
    }))

    # ─── Compute components ───
    c01 = calc_c01_response_rate(answered, total)
    c04_scored, est_neg_unanswered = calc_c04_est_neg_unanswered(s1, s2, unanswered, total)
    c05 = calc_c05_rating_vs_cluster(rating, bench["avg_rating"])
    c07 = calc_c07_positive_concentration(s4, s5, total)
    c08 = calc_c08_neg_ratio_inverted(s1, s2, total)
    c11 = calc_c11_volume_percentile(total, bench["review_volumes"])
    c15 = calc_c15_bss_lite_inverted(unanswered, total, est_neg_unanswered, neg_total)

    # ─── Weighted score ───
    raw_score = (
        c01 * WEIGHTS["C01"]
        + c15 * WEIGHTS["C15"]
        + c04_scored * WEIGHTS["C04"]
        + c07 * WEIGHTS["C07"]
        + c05 * WEIGHTS["C05"]
        + c11 * WEIGHTS["C11"]
        + c08 * WEIGHTS["C08"]
    )

    # ─── Confidence factor ───
    confidence = min(total / 50, 1.0)
    adj_score = round(raw_score * confidence + 50 * (1 - confidence))

    # ─── Band ───
    band = _assign_band(adj_score)

    # ─── Route to bucket or skip ───

    # Bucket D override: sleeping elite (high rating, ignoring reviews)
    # Checked BEFORE band routing because these places score Poor/Average
    # due to bad C01/C15, but the email angle is different from burning/eroding
    if rating >= 4.5 and c01 < 30 and unanswered > 30:
        bucket, bucket_name = "D", "sleeping"
        band_override = True
    else:
        band_override = False
        routing = BAND_TO_BUCKET.get(band)

        if routing is None:
            return None  # Good/Excellent → skip

        bucket, bucket_name = routing

        # Average band: only email if real pain exists
        if band == "Average" and est_neg_unanswered < 5 and unanswered_pct < 60:
            return None

    # Skip if unanswered < 25% (they're handling it)
    if unanswered_pct > 0 and unanswered_pct < 25:
        return None

    # ─── Pain point text ───
    if bucket == "D":
        pain = f"{rating} rating but ~{unanswered} reviews ignored"
    elif band == "Critical":
        if est_neg_unanswered >= 10:
            pain = f"{est_neg_unanswered} negative reviews without a reply"
        else:
            pain = f"{unanswered_pct:.0f}% of reviews unanswered with a {rating} rating"
    elif band == "Poor":
        if est_neg_unanswered >= 10:
            pain = f"{est_neg_unanswered} negative reviews without a reply"
        else:
            pain = f"{unanswered_pct:.0f}% of reviews unanswered"
    else:  # Average → B
        if est_neg_unanswered >= 5:
            pain = f"~{est_neg_unanswered} unanswered complaints among {total} reviews"
        else:
            pain = f"{unanswered_pct:.0f}% of reviews unanswered"

    # ─── Location context ───
    address = row.get("address", "")
    parts = [p.strip() for p in address.split(",") if p.strip()]
    # Try to find city from country column first, fall back to address parsing
    city_from_row = row.get("city", "")
    if city_from_row:
        city = city_from_row
        street = parts[1] if len(parts) >= 2 else (parts[0] if parts else "")
    else:
        street = parts[0] if len(parts) >= 1 else ""
        city = parts[-2] if len(parts) >= 3 else (parts[-1] if len(parts) >= 2 else "")
    country = row.get("country", "")

    return {
        "email": email,
        "place_name": (row.get("simple_name") or row.get("name", "")).strip(),
        "category": category,
        "cluster": cluster,
        "street": street,
        "city": city,
        "country": country,
        "rating": rating,
        "total_reviews": total,
        "est_unanswered": unanswered,
        "unanswered_pct": round(unanswered_pct, 1),
        "neg_unanswered": est_neg_unanswered,
        "neg_unanswered_pct": round(neg_total / total * 100, 1) if total > 0 else 0,
        "est_stars_5": s5,
        "est_stars_4": s4,
        "est_stars_3": s3,
        "est_stars_2": s2,
        "est_stars_1": s1,
        "website": row.get("website", ""),
        "phone": row.get("phone", ""),
        # LRPI scores
        "lrpi_score": adj_score,
        "lrpi_band": band,
        "lrpi_c01_response_rate": round(c01, 1),
        "lrpi_c04_neg_unanswered": round(c04_scored, 1),
        "lrpi_c05_rating_vs_cluster": round(c05, 1),
        "lrpi_c07_positive_pct": round(c07, 1),
        "lrpi_c08_neg_ratio_inv": round(c08, 1),
        "lrpi_c11_volume_pctl": round(c11, 1),
        "lrpi_c15_bss_inv": round(c15, 1),
        # Cluster context
        "cluster_avg_rating": round(bench["avg_rating"], 2),
        "cluster_avg_response_rate": round(bench["avg_response_rate"], 1),
        "cluster_count": bench["count"],
        # Bucket routing
        "bucket": bucket,
        "bucket_name": bucket_name,
        "pain": pain,
    }


# ─── Batch process ──────────────────────────────────────────

def classify_csv(input_path: str, output_path: str = None) -> dict:
    """
    Read CSV, compute cluster benchmarks, score each lead with LRPI Model B,
    write classified leads to output CSV. Returns stats dict.
    """
    in_path = Path(input_path)
    if not in_path.exists():
        log.error(f"Input not found: {input_path}")
        return {"total": 0, "classified": 0, "skipped": 0, "buckets": {}}

    if output_path is None:
        output_path = str(in_path.parent / (in_path.stem + "_classified.csv"))

    # Read all rows
    with open(in_path, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    log.info(f"Loaded {len(rows)} leads, computing cluster benchmarks...")

    # Pass 1: cluster benchmarks
    benchmarks = compute_cluster_benchmarks(rows)
    log.info(f"Computed benchmarks for {len(benchmarks)} clusters")

    # Pass 2: score each lead
    stats = {
        "total": len(rows),
        "classified": 0,
        "skipped": 0,
        "buckets": {"A": 0, "B": 0, "D": 0},
        "bands": {"Critical": 0, "Poor": 0, "Average": 0, "Good": 0, "Excellent": 0},
    }
    classified_rows = []

    for row in rows:
        result = score_lead(row, benchmarks)
        if result is None:
            stats["skipped"] += 1
            continue
        stats["classified"] += 1
        stats["buckets"][result["bucket"]] += 1
        stats["bands"][result["lrpi_band"]] += 1
        classified_rows.append(result)

    # Write output
    if classified_rows:
        fieldnames = list(classified_rows[0].keys())
        with open(output_path, "w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(classified_rows)

    log.info(
        f"LRPI classified {stats['classified']}/{stats['total']} leads. "
        f"Bands: {stats['bands']}. Buckets: {stats['buckets']}. Skipped: {stats['skipped']}"
    )
    print(
        f"📊 LRPI Model B: {stats['classified']}/{stats['total']} classified\n"
        f"   Bands: Critical={stats['bands']['Critical']} Poor={stats['bands']['Poor']} "
        f"Average={stats['bands']['Average']} Good={stats['bands']['Good']} Excellent={stats['bands']['Excellent']}\n"
        f"   Buckets: A(burning)={stats['buckets']['A']} B(eroding)={stats['buckets']['B']} D(sleeping)={stats['buckets']['D']}\n"
        f"   Skipped: {stats['skipped']} (no email + low data + low pain + managed)"
    )

    return {**stats, "output_path": output_path, "benchmarks": {
        k: {kk: vv for kk, vv in v.items() if kk != "review_volumes"}
        for k, v in benchmarks.items()
    }}


if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    if len(sys.argv) < 2:
        print("Usage: python cold_email_classifier.py <input.csv>")
        sys.exit(1)
    result = classify_csv(sys.argv[1])
    print(f"\nBenchmarks:")
    for cluster, bench in result.get("benchmarks", {}).items():
        print(f"  {cluster}: avg_rating={bench['avg_rating']:.2f} avg_response={bench['avg_response_rate']:.1f}% n={bench['count']}")
