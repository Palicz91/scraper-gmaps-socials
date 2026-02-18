"""
Single-place review audit scraper.
Uses the proven get_place_data.py logic that already works on this VPS.
"""

import sys
import os
import logging

# Add the GMaps scraper directory to path so we can import from it
GMAPS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "20251105 GMaps Scraper")
sys.path.insert(0, GMAPS_DIR)

from get_place_data import create_driver, get_place_data

logger = logging.getLogger(__name__)


def run_single_place_audit(maps_url: str, place_id: str, place_name: str = "", place_address: str = "") -> dict | None:
    """
    Run audit on a single Google Maps place.
    Uses the same scraper logic as get_place_data.py which is proven to work.

    Returns dict with reviews_loaded, answered, unanswered, unanswered_pct
    or None on failure.
    """
    driver = None
    try:
        driver = create_driver()

        # Build a search URL similar to what search_query.py generates
        # This format is proven to work on this VPS
        if place_name and place_address:
            search_term = f"{place_name} {place_address}"
        elif place_name:
            search_term = place_name
        else:
            search_term = f"place_id:{place_id}"

        search_url = f"https://www.google.com/maps/search/{search_term.replace(' ', '+')}?hl=en"
        logger.info(f"Audit URL: {search_url}")

        # Use the existing get_place_data function with review scraping enabled
        place_data = get_place_data(
            driver,
            search_url,
            max_retries=2,
            scrape_reviews=True,
            max_review_scrolls=50,
            min_reviews_for_analysis=0,  # Always analyze, even small places
        )

        if not place_data or place_data == "BROWSER_CRASHED":
            logger.error(f"Scraper failed for {place_name}")
            return None

        # Map the output to our expected format
        reviews_loaded = int(place_data.get('reviews_loaded', 0) or 0)
        answered = int(place_data.get('reviews_answered', 0) or 0)
        unanswered = int(place_data.get('reviews_unanswered', 0) or 0)
        unanswered_pct = float(place_data.get('reviews_unanswered_pct', 0) or 0)

        result = {
            'reviews_loaded': reviews_loaded,
            'answered': answered,
            'unanswered': unanswered,
            'unanswered_pct': unanswered_pct,
            'total_reviews_on_page': int(place_data.get('reviews', 0) or 0),
            'place_name': place_data.get('name', place_name),
            'rating': place_data.get('rating', ''),
        }

        logger.info(
            f"Audit result for {result['place_name']}: "
            f"{reviews_loaded} loaded, {unanswered} unanswered ({unanswered_pct}%)"
        )
        return result

    except Exception as e:
        logger.error(f"Audit failed: {e}", exc_info=True)
        return None

    finally:
        if driver:
            try:
                driver.quit()
            except:
                pass


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

    # Usage: python audit_scraper.py <place_id> [place_name] [place_address]
    test_place_id = sys.argv[1] if len(sys.argv) > 1 else "ChIJM1KEgCTcQUcRnr7f9tjnbmo"
    test_name = sys.argv[2] if len(sys.argv) > 2 else "Déryné"
    test_address = sys.argv[3] if len(sys.argv) > 3 else "Budapest Krisztina tér"

    print(f"Testing audit for: {test_name} ({test_place_id})")
    result = run_single_place_audit("", test_place_id, test_name, test_address)
    if result:
        print(f"\nResult:")
        for k, v in result.items():
            print(f"  {k}: {v}")
    else:
        print("Audit failed.")
