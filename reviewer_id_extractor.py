"""
Reviewer ID Extractor
Opens a Google Maps place URL, scrolls through reviews,
and extracts Google contributor user IDs from reviewer profile links.

Input: Google Maps place URL (or place_id)
Output: List of {google_user_id, reviewer_name, star_rating, review_text_snippet}
"""

import time
import re
import random
import logging
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options

log = logging.getLogger("reviewer_id_extractor")

USER_AGENTS = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
]


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


def sort_reviews_by_newest(driver):
    """Click the sort dropdown and select 'Newest' to prioritize recent reviewers."""
    sort_button_selectors = [
        "//button[contains(@aria-label, 'Sort')]",
        "//button[contains(@aria-label, 'sort')]",
        "//button[contains(@aria-label, 'Sortieren')]",
        "//button[contains(@data-value, 'Sort')]",
        "//button[.//span[contains(text(), 'Most relevant')]]",
        "//button[.//span[contains(text(), 'Relevanteste')]]",
        "//button[.//span[contains(text(), 'Newest')]]",
    ]
    for xpath in sort_button_selectors:
        try:
            btn = WebDriverWait(driver, 3).until(
                EC.element_to_be_clickable((By.XPATH, xpath))
            )
            btn.click()
            time.sleep(2)
            break
        except:
            continue
    else:
        log.info("Sort button not found, proceeding with default order")
        return False

    # Click "Newest" in the dropdown menu
    newest_selectors = [
        "//div[@role='menuitemradio' and contains(., 'Newest')]",
        "//div[@role='menuitemradio' and contains(., 'Neueste')]",
        "//li[contains(., 'Newest')]",
        "//div[@data-index='1']",  # Newest is typically the second option
    ]
    for xpath in newest_selectors:
        try:
            item = WebDriverWait(driver, 3).until(
                EC.element_to_be_clickable((By.XPATH, xpath))
            )
            item.click()
            time.sleep(3)
            log.info("Sorted reviews by Newest")
            return True
        except:
            continue

    log.info("Could not select Newest sort option")
    return False


def scroll_review_panel(driver, max_scrolls=50, scroll_pause=1.5):
    """Scroll through reviews in the Maps place review panel."""
    scrollable_selectors = [
        'div.m6QErb.DxyBCb.kA9KIf.dS8AEf.XiKgde',
        'div.m6QErb.DxyBCb.kA9KIf.dS8AEf',
        'div.m6QErb.DxyBCb.kA9KIf',
        'div[role="feed"]',
        'div.m6QErb',
    ]

    scrollable = None
    for sel in scrollable_selectors:
        try:
            elements = driver.find_elements(By.CSS_SELECTOR, sel)
            for el in elements:
                # Pick the one that is actually scrollable (has overflow)
                h = driver.execute_script("return arguments[0].scrollHeight", el)
                if h > 200:
                    scrollable = el
                    break
            if scrollable:
                break
        except:
            continue

    # Fallback: find any scrollable container with reviews inside
    if not scrollable:
        try:
            scrollable = driver.execute_script("""
                var feeds = document.querySelectorAll('div[role="feed"], div.m6QErb');
                for (var i = 0; i < feeds.length; i++) {
                    if (feeds[i].scrollHeight > feeds[i].clientHeight) return feeds[i];
                }
                return null;
            """)
        except:
            pass

    if not scrollable:
        log.warning("Could not find scrollable review panel")
        return 0

    last_count = 0
    stale = 0

    for i in range(max_scrolls):
        driver.execute_script(
            "arguments[0].scrollTop = arguments[0].scrollHeight", scrollable
        )
        time.sleep(scroll_pause)

        reviews = driver.find_elements(By.CSS_SELECTOR, 'div[data-review-id]')
        count = len(reviews)

        if count == last_count:
            stale += 1
            if stale >= 3:
                break
        else:
            stale = 0
            last_count = count

    return last_count


def open_reviews_tab(driver):
    """Click the Reviews tab on a Maps place page. Handles multi-language UI."""
    tab_selectors = [
        "//button[contains(@aria-label, 'Reviews')]",
        "//button[contains(@aria-label, 'review')]",
        "//button[contains(@aria-label, 'Rezension')]",   # German
        "//button[contains(@aria-label, 'Bewertung')]",   # German alt
        "//button[contains(@aria-label, 'értékelés')]",   # Hungarian
        "//button[contains(@aria-label, 'avis')]",        # French
        "//button[contains(., 'Reviews')]",
        "//button[contains(., 'Rezensionen')]",
        "//button[@data-tab-id='reviews']",
    ]
    for xpath in tab_selectors:
        try:
            tab = WebDriverWait(driver, 5).until(
                EC.element_to_be_clickable((By.XPATH, xpath))
            )
            tab.click()
            # Wait for reviews to actually load
            time.sleep(8)
            return True
        except:
            continue
    return False


def extract_reviewer_ids(driver):
    """
    Extract Google user IDs from the currently loaded review panel.
    Returns list of dicts: {google_user_id, reviewer_name, star_rating, review_text_snippet}
    """
    results = []
    review_elements = driver.find_elements(By.CSS_SELECTOR, 'div[data-review-id]')

    for el in review_elements:
        try:
            # Extract contributor user ID from profile link
            google_user_id = None

            # Try button[data-href] first (Google often uses this)
            for btn in el.find_elements(By.CSS_SELECTOR, 'button[data-href*="/maps/contrib/"]'):
                href = btn.get_attribute("data-href") or ""
                m = re.search(r'/maps/contrib/(\d+)', href)
                if m:
                    google_user_id = m.group(1)
                    break

            # Fallback: <a href>
            if not google_user_id:
                for a in el.find_elements(By.CSS_SELECTOR, 'a[href*="/maps/contrib/"]'):
                    href = a.get_attribute("href") or ""
                    m = re.search(r'/maps/contrib/(\d+)', href)
                    if m:
                        google_user_id = m.group(1)
                        break

            if not google_user_id:
                continue

            # Reviewer name
            reviewer_name = ""
            for sel in ['div.d4r55', 'span.d4r55', 'button.WEBjve']:
                try:
                    reviewer_name = el.find_element(By.CSS_SELECTOR, sel).text.strip()
                    if reviewer_name:
                        break
                except:
                    continue

            # Star rating (multi-language: "X stars", "X Sterne", "X csillag")
            star_rating = 0
            try:
                star_el = el.find_element(By.CSS_SELECTOR, 'span[role="img"]')
                star_label = star_el.get_attribute("aria-label") or ""
                m = re.search(r'(\d+)', star_label)
                if m:
                    star_rating = int(m.group(1))
            except:
                pass

            # Reviewer photo hash (ACg8oc... from profile photo URL)
            photo_hash = ""
            try:
                img_el = el.find_element(By.CSS_SELECTOR, 'img.NBa7we')
                img_src = img_el.get_attribute("src") or ""
                ph_m = re.search(r'ACg8oc[A-Za-z0-9_-]+', img_src)
                if ph_m:
                    photo_hash = ph_m.group(0)
            except:
                pass

            # Review text snippet
            review_text = ""
            for sel in ['span.wiI7pd', 'div.MyEned span']:
                try:
                    review_text = el.find_element(By.CSS_SELECTOR, sel).text.strip()[:200]
                    if review_text:
                        break
                except:
                    continue

            results.append({
                "google_user_id": google_user_id,
                "reviewer_name": reviewer_name,
                "star_rating": star_rating,
                "review_text_snippet": review_text,
                "photo_hash": photo_hash,
            })

        except Exception as e:
            log.debug(f"Error extracting reviewer from element: {e}")
            continue

    # Deduplicate by user_id
    seen = set()
    deduped = []
    for r in results:
        if r["google_user_id"] not in seen:
            seen.add(r["google_user_id"])
            deduped.append(r)

    return deduped


def extract_ids_from_place(place_url, max_scrolls=50, driver=None, place_name=None):
    """
    Main entry point: open a Maps place URL, scroll reviews, extract user IDs.
    If direct URL fails to load reviews, falls back to search by place_name.
    Returns list of reviewer dicts.
    """
    own_driver = driver is None
    if own_driver:
        driver = create_driver()

    try:
        log.info(f"Loading place: {place_url[:80]}")
        driver.get(place_url)
        time.sleep(5)
        accept_google_consent(driver)
        time.sleep(3)

        # Check if we're on a search results page (need to click first result)
        if "/maps/search/" in driver.current_url or "/maps/place/" not in driver.current_url:
            result_links = driver.find_elements(By.CSS_SELECTOR, 'a[href*="/maps/place/"]')
            if result_links:
                log.info("On search results, clicking first result...")
                result_links[0].click()
                time.sleep(5)

        # Check if the place loaded (h1 with actual text)
        h1_loaded = False
        for h1 in driver.find_elements(By.CSS_SELECTOR, "h1"):
            if h1.text.strip() and h1.text.strip() not in ("Ergebnisse", "Results"):
                h1_loaded = True
                break

        # If direct URL didn't load the place, try search by name
        if not h1_loaded and place_name:
            log.info(f"Direct URL didn't load, searching for: {place_name}")
            search_url = f"https://www.google.com/maps/search/{place_name.replace(' ', '+')}"
            driver.get(search_url)
            time.sleep(6)
            accept_google_consent(driver)
            time.sleep(2)

            result_links = driver.find_elements(By.CSS_SELECTOR, 'a[href*="/maps/place/"]')
            if result_links:
                result_links[0].click()
                time.sleep(5)
            else:
                log.warning(f"No search results for {place_name}")
                return []

        # Open reviews tab
        if not open_reviews_tab(driver):
            log.warning("Could not open Reviews tab")
            return []

        # Sort by newest to prioritize recent reviewers
        sort_reviews_by_newest(driver)

        # Scroll through reviews
        loaded = scroll_review_panel(driver, max_scrolls=max_scrolls)
        log.info(f"Loaded {loaded} reviews, extracting user IDs...")

        # Extract user IDs
        reviewers = extract_reviewer_ids(driver)
        log.info(f"Extracted {len(reviewers)} unique reviewer IDs")

        return reviewers

    except Exception as e:
        log.error(f"Error scraping place: {e}")
        return []

    finally:
        if own_driver:
            driver.quit()


if __name__ == "__main__":
    import sys
    import json

    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

    if len(sys.argv) < 2:
        print("Usage: python reviewer_id_extractor.py <google_maps_place_url>")
        sys.exit(1)

    url = sys.argv[1]
    results = extract_ids_from_place(url, max_scrolls=30)
    print(f"\nFound {len(results)} reviewers:")
    for r in results:
        print(json.dumps(r, ensure_ascii=False))
