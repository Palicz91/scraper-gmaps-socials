"""
Google Maps Contributor Profile Scraper
Scrapes google.com/maps/contrib/{userId}/reviews to get all reviews by a reviewer.

Input: List of Google user IDs
Output: Profile info + all reviews per reviewer
"""

import time
import re
import random
import logging
import json
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options

from datetime import datetime, timedelta

log = logging.getLogger("contrib_scraper")


def parse_relative_date(relative_text, reference_date=None):
    """
    Convert relative date strings to:
    - date_category: "this_week" / "week" / "month" / "year" / "older"
    - date_estimated: approximate ISO date
    Handles: English, German, Hungarian.
    Returns (date_category, date_estimated) tuple.
    """
    if not relative_text:
        return ("", "")

    ref = reference_date or datetime.utcnow()
    text = relative_text.lower().strip()

    day_words = ['day', 'tag', 'nap']
    week_words = ['week', 'woche', 'hét']
    month_words = ['month', 'monat', 'hónap']
    year_words = ['year', 'jahr', 'év']

    m = re.search(r'(\d+)', text)
    num = int(m.group(1)) if m else 1

    days = 0
    category = ""
    if any(w in text for w in day_words):
        days = num
        category = "this_week" if num <= 7 else "week"
    elif any(w in text for w in week_words):
        days = num * 7
        category = "this_week" if num == 1 else "week"
    elif any(w in text for w in month_words):
        days = num * 30
        category = "month"
    elif any(w in text for w in year_words):
        days = num * 365
        category = "year" if num == 1 else "older"
    else:
        return ("", "")

    estimated = ref - timedelta(days=days)
    return (category, estimated.strftime("%Y-%m-%d"))

USER_AGENTS = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
]

MIN_DELAY = 5
MAX_DELAY = 12
DRIVER_RESTART_EVERY = 50
BATCH_PAUSE = 60
BATCH_SIZE = 100


def create_driver():
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--window-size=1280,1000")
    options.add_argument("--lang=en")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--incognito")
    options.add_argument("--disable-extensions")

    ua = random.choice(USER_AGENTS)
    options.add_argument(f"user-agent={ua}")

    driver = webdriver.Chrome(options=options)
    driver.set_page_load_timeout(30)

    driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
        "source": """
            Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
            window.navigator.chrome = {runtime: {}};
            Object.defineProperty(navigator, 'languages', {get: () => ['en-US', 'en']});
            Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4, 5]});
        """
    })

    return driver


def accept_google_consent(driver, timeout=3):
    try:
        if "consent.google" not in driver.current_url and "Before you continue" not in driver.page_source:
            return False
        consent_buttons = [
            "//button[contains(., 'Accept all')]",
            "//button[contains(., 'Reject all')]",
            "//button[contains(., 'Accept')]",
            "//button[contains(., 'I agree')]",
            "//form//button[1]",
        ]
        for xpath in consent_buttons:
            try:
                button = WebDriverWait(driver, timeout).until(
                    EC.element_to_be_clickable((By.XPATH, xpath))
                )
                button.click()
                time.sleep(2)
                return True
            except:
                continue
        return False
    except:
        return False


def scroll_contrib_reviews(driver, max_scrolls=200, scroll_pause=2.0):
    """
    Scroll through all reviews on a contributor's profile page.
    Uses the side panel scrollable container (same as place review panel).
    """
    # Find scrollable container — same as place reviews
    scrollable_selectors = [
        'div.m6QErb.DxyBCb.kA9KIf.dS8AEf',
        'div.m6QErb.DxyBCb.kA9KIf',
        'div.m6QErb.DxyBCb',
        'div.m6QErb',
    ]

    scrollable = None
    for sel in scrollable_selectors:
        els = driver.find_elements(By.CSS_SELECTOR, sel)
        if els:
            scrollable = els[0]
            break

    last_count = 0
    stale = 0

    for i in range(max_scrolls):
        # Scroll the panel container, with body fallback
        if scrollable:
            driver.execute_script(
                "arguments[0].scrollTop = arguments[0].scrollHeight", scrollable
            )
        else:
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight)")
        time.sleep(scroll_pause)

        # Count review cards — use jftiEf (top-level cards, no duplicates)
        count = len(driver.find_elements(By.CSS_SELECTOR, 'div.jftiEf'))

        if count == last_count:
            stale += 1
            if stale >= 5:  # 5 stale scrolls before giving up (lazy load can be slow)
                break
        else:
            stale = 0
            last_count = count

        if i % 10 == 0:
            log.info(f"  Scrolling contrib page... {count} reviews loaded")

    return last_count


def extract_profile_info(driver):
    """Extract profile header info from contrib page."""
    profile = {
        "display_name": "",
        "photo_url": "",
        "total_review_count": 0,
        "photo_count": 0,
        "local_guide_level": None,
    }

    # Display name — contrib page uses button.fontHeadlineLarge or div.PMkhac header
    for sel in ['button.geAzIe', 'button.fontHeadlineLarge', 'div.PMkhac', 'div.BZZkgb', 'h1']:
        try:
            el = driver.find_element(By.CSS_SELECTOR, sel)
            # PMkhac/BZZkgb contain name + Local Guide on separate lines — take first line
            text = el.text.strip().split("\n")[0].strip()
            if text and len(text) < 100 and text not in ("Ergebnisse", "Results"):
                profile["display_name"] = text
                break
        except:
            continue

    # Profile photo — aria-label based (multi-language: "Profile photo", "Profilbild", etc.)
    for sel in ['img[aria-label*="rofil"]', 'img[src*="googleusercontent.com/a"]', 'img[alt*="Photo"]']:
        try:
            img = driver.find_element(By.CSS_SELECTOR, sel)
            src = img.get_attribute("src") or ""
            if src and "googleusercontent.com" in src:
                profile["photo_url"] = src
                break
        except:
            continue

    # Review count, ratings count, photo count from page text
    # Stats line format: "417 Rezensionen · 11 Bewertungen" or "123 reviews · 45 ratings"
    try:
        page_text = driver.find_element(By.TAG_NAME, "body").text
        # Review count (multi-language: reviews/Rezensionen/értékelések/avis)
        m = re.search(r'(\d[\d.,]*)\s+(?:review|rezension|értékelés|avis)', page_text, re.IGNORECASE)
        if m:
            profile["total_review_count"] = int(re.sub(r'[.,]', '', m.group(1)))
        # Ratings count (Bewertungen/ratings — star-only reviews without text)
        m = re.search(r'(\d[\d.,]*)\s+(?:rating|bewertung)', page_text, re.IGNORECASE)
        if m:
            profile["ratings_count"] = int(re.sub(r'[.,]', '', m.group(1)))
        # Photo count — try clicking Fotos tab to get the count
        for tab in driver.find_elements(By.CSS_SELECTOR, 'button[role="tab"]'):
            tab_text = tab.text.strip().lower()
            if any(w in tab_text for w in ['foto', 'photo', 'kép']):
                tab.click()
                time.sleep(2)
                # Count photo elements or look for count text
                photo_text = driver.find_element(By.TAG_NAME, "body").text
                m = re.search(r'(\d[\d.,]*)\s+(?:photo|foto|kép)', photo_text, re.IGNORECASE)
                if m:
                    profile["photo_count"] = int(re.sub(r'[.,]', '', m.group(1)))
                else:
                    # Count photo grid items
                    photos = driver.find_elements(By.CSS_SELECTOR, 'div[role="img"], img[src*="googleusercontent"]')
                    if photos:
                        profile["photo_count"] = len(photos)
                # Switch back to reviews tab
                for rtab in driver.find_elements(By.CSS_SELECTOR, 'button[role="tab"]'):
                    rt = rtab.text.strip().lower()
                    if any(w in rt for w in ['review', 'rezension', 'értékelés', 'avis']):
                        rtab.click()
                        time.sleep(1)
                        break
                break
        # LG points
        m = re.search(r'([\d.,]+)\s+(?:Punkte|points|pont)', page_text, re.IGNORECASE)
        if m:
            profile["lg_points"] = int(re.sub(r'[.,]', '', m.group(1)))
    except:
        pass

    # Local Guide level — parse exact level number
    try:
        # Try dedicated selector first
        for sel in ['button.a4wekd', 'span.FNyx3']:
            try:
                text = driver.find_element(By.CSS_SELECTOR, sel).text.strip()
                m = re.search(r'Level\s*(\d+)', text, re.IGNORECASE)
                if m:
                    profile["local_guide_level"] = int(m.group(1))
                    break
            except:
                continue

        # Fallback: search page source
        if profile["local_guide_level"] is None:
            page_source = driver.page_source
            m = re.search(r'Local Guide[^<]*Level\s*(\d+)', page_source, re.IGNORECASE)
            if m:
                profile["local_guide_level"] = int(m.group(1))
            elif "Local Guide" in page_source:
                profile["local_guide_level"] = 0  # badge present but no level visible
    except:
        pass

    return profile


def extract_contrib_reviews(driver):
    """Extract all review data from a contributor's review page."""
    reviews = []

    # Find review containers — use most specific selector first
    # div.jftiEf are the top-level review cards on contrib pages
    # div[data-review-id] may be nested inside, causing duplicates
    review_elements = []
    for sel in ['div.jftiEf', 'div[data-review-id]']:
        review_elements = driver.find_elements(By.CSS_SELECTOR, sel)
        if review_elements:
            break

    for el in review_elements:
        try:
            review = {
                "place_name": "",
                "place_address": "",
                "stars": 0,
                "review_text": "",
                "reviewed_at": "",
            }

            # Place name — the business being reviewed
            for sel in ['div.d4r55', 'span.d4r55', 'a.d4r55', 'div[class*="fontTitle"]']:
                try:
                    review["place_name"] = el.find_element(By.CSS_SELECTOR, sel).text.strip()
                    if review["place_name"]:
                        break
                except:
                    continue

            # If no place name found, try link text
            if not review["place_name"]:
                for a in el.find_elements(By.CSS_SELECTOR, 'a[href*="/maps/place/"]'):
                    text = a.text.strip()
                    if text:
                        review["place_name"] = text
                        break

            # Star rating (multi-language: "X stars", "X Sterne", "X csillag", etc.)
            try:
                star_el = el.find_element(By.CSS_SELECTOR, 'span[role="img"]')
                star_label = star_el.get_attribute("aria-label") or ""
                m = re.search(r'(\d+)', star_label)
                if m:
                    review["stars"] = int(m.group(1))
            except:
                pass

            # Review text
            for sel in ['span.wiI7pd', 'div.MyEned span', 'span[class*="review-full-text"]']:
                try:
                    review["review_text"] = el.find_element(By.CSS_SELECTOR, sel).text.strip()
                    if review["review_text"]:
                        break
                except:
                    continue

            # Date
            for sel in ['span.rsqaWe', 'span[class*="publishDate"]']:
                try:
                    review["reviewed_at"] = el.find_element(By.CSS_SELECTOR, sel).text.strip()
                    if review["reviewed_at"]:
                        break
                except:
                    continue

            # Place address (if available)
            for sel in ['span.RfnDt', 'div[class*="address"]']:
                try:
                    review["place_address"] = el.find_element(By.CSS_SELECTOR, sel).text.strip()
                    if review["place_address"]:
                        break
                except:
                    continue

            # Convert relative date to category + estimated date
            date_cat, date_est = parse_relative_date(review["reviewed_at"])
            review["date_category"] = date_cat
            review["review_date_estimated"] = date_est

            if review["place_name"] or review["stars"]:
                reviews.append(review)

        except Exception as e:
            log.debug(f"Error extracting contrib review: {e}")
            continue

    return reviews


def scrape_contributor(google_user_id, driver=None, max_scrolls=100):
    """
    Scrape a single contributor profile.
    Returns: {profile: {...}, reviews: [...]}
    """
    own_driver = driver is None
    if own_driver:
        driver = create_driver()

    url = f"https://www.google.com/maps/contrib/{google_user_id}/reviews"
    scrape_timestamp = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

    try:
        log.info(f"Scraping contributor {google_user_id}")
        driver.get(url)
        accept_google_consent(driver)

        # Wait for page to render
        time.sleep(random.uniform(3, 5))

        # Check if profile exists (redirect to maps home = not found)
        if "/contrib/" not in driver.current_url:
            log.warning(f"Contributor {google_user_id} not found (redirected)")
            return None

        # Extract profile info
        profile = extract_profile_info(driver)
        profile["google_user_id"] = google_user_id
        profile["scraped_at"] = scrape_timestamp

        # Scroll through all reviews
        loaded = scroll_contrib_reviews(driver, max_scrolls=max_scrolls)
        log.info(f"Loaded {loaded} reviews for {profile['display_name'] or google_user_id}")

        # Extract review data
        reviews = extract_contrib_reviews(driver)
        log.info(f"Extracted {len(reviews)} reviews")

        profile["scraped_reviews_count"] = len(reviews)

        return {
            "profile": profile,
            "reviews": reviews,
        }

    except Exception as e:
        log.error(f"Error scraping contributor {google_user_id}: {e}")
        return None

    finally:
        if own_driver:
            driver.quit()


def scrape_contributors(user_ids, on_result=None):
    """
    Batch scrape multiple contributors.
    on_result callback: called with (google_user_id, result_dict) after each scrape.
    """
    driver = create_driver()
    results = []

    for i, uid in enumerate(user_ids):
        # Driver restart for fresh session
        if i > 0 and i % DRIVER_RESTART_EVERY == 0:
            log.info(f"Restarting browser (every {DRIVER_RESTART_EVERY})...")
            driver.quit()
            driver = create_driver()

        # Batch pause
        if i > 0 and i % BATCH_SIZE == 0:
            log.info(f"Batch pause {BATCH_PAUSE}s...")
            time.sleep(BATCH_PAUSE)

        result = scrape_contributor(uid, driver=driver)

        if result:
            results.append(result)
            if on_result:
                on_result(uid, result)
            log.info(
                f"[{i+1}/{len(user_ids)}] {result['profile'].get('display_name', uid)}: "
                f"{len(result['reviews'])} reviews"
            )
        else:
            log.warning(f"[{i+1}/{len(user_ids)}] {uid}: FAILED or not found")

        # Rate limiting
        if i < len(user_ids) - 1:
            time.sleep(random.uniform(MIN_DELAY, MAX_DELAY))

    driver.quit()
    return results


if __name__ == "__main__":
    import sys

    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

    if len(sys.argv) < 2:
        print("Usage: python contrib_scraper.py <google_user_id> [<id2> ...]")
        sys.exit(1)

    user_ids = sys.argv[1:]
    results = scrape_contributors(user_ids)

    for r in results:
        print(f"\n{'='*60}")
        print(f"Profile: {json.dumps(r['profile'], ensure_ascii=False, indent=2)}")
        print(f"Reviews ({len(r['reviews'])}):")
        for rev in r["reviews"]:
            print(f"  {rev['stars']}★ {rev['place_name']}: {rev['review_text'][:80]}")
