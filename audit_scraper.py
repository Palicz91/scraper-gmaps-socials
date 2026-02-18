"""
Single-place review audit scraper.
Opens one Google Maps place, counts answered vs unanswered reviews.
"""

import time
import re
import logging
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException, WebDriverException

logger = logging.getLogger(__name__)


def create_driver():
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--window-size=1280,1000")
    options.add_argument("--lang=en-US")
    options.add_argument("--ignore-certificate-errors")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--disable-infobars")
    options.add_argument("--disable-webgl")
    options.add_argument("--disable-software-rasterizer")
    options.add_argument("--disable-features=VizDisplayCompositor")
    options.add_argument("--incognito")
    options.add_argument("--disable-extensions")
    options.add_argument("--disable-plugins")
    options.add_argument(
        "user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
    options.add_experimental_option("prefs", {
        "intl.accept_languages": "en-US,en",
    })
    driver = webdriver.Chrome(options=options)
    driver.set_page_load_timeout(30)
    return driver


def accept_google_consent(driver, timeout=5):
    try:
        page_src = driver.page_source
        if "consent.google" not in driver.current_url \
           and "Before you continue" not in page_src \
           and "Bevor du fortfährst" not in page_src:
            return False
        buttons = [
            "//button[contains(., 'Accept all')]",
            "//button[contains(., 'Reject all')]",
            "//button[contains(., 'Accept')]",
            "//button[contains(., 'I agree')]",
            "//button[contains(., 'Alle akzeptieren')]",
            "//button[contains(., 'Alle ablehnen')]",
            "//button[@aria-label='Accept all']",
            "//form//button[1]",
        ]
        for xpath in buttons:
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


def open_reviews_tab(driver):
    """Open the reviews tab - works with any language."""
    for tab_index in ["1", "2", "3"]:
        try:
            tab = driver.find_element(
                By.CSS_SELECTOR, f'button.hh2c6[data-tab-index="{tab_index}"]'
            )
            label = tab.get_attribute("aria-label") or ""
            review_keywords = ["review", "rezension", "avis", "reseñ", "recens", "értékelés"]
            if any(kw in label.lower() for kw in review_keywords):
                tab.click()
                time.sleep(2)
                logger.info(f"Opened reviews tab: {label}")
                return True
        except:
            continue

    try:
        buttons = driver.find_elements(By.CSS_SELECTOR, "button[aria-label]")
        for btn in buttons:
            label = btn.get_attribute("aria-label") or ""
            review_keywords = ["review", "rezension", "avis", "reseñ", "recens", "értékelés"]
            if any(kw in label.lower() for kw in review_keywords):
                btn.click()
                time.sleep(2)
                logger.info(f"Opened reviews via button: {label}")
                return True
    except:
        pass

    logger.warning("Could not find reviews tab")
    return False


def scroll_reviews(driver, max_scrolls=50, scroll_pause=1.5):
    scrollable_selectors = [
        'div.m6QErb.DxyBCb.kA9KIf.dS8AEf',
        'div.m6QErb.DxyBCb.kA9KIf',
        'div.m6QErb',
    ]
    scrollable = None
    for sel in scrollable_selectors:
        try:
            elements = driver.find_elements(By.CSS_SELECTOR, sel)
            for el in elements:
                height = el.get_attribute("scrollHeight")
                if height and int(height) > 200:
                    scrollable = el
                    break
            if scrollable:
                break
        except:
            continue

    if not scrollable:
        logger.warning("Could not find scrollable review panel")
        return

    last_review_count = 0
    stale_count = 0

    for scroll_num in range(max_scrolls):
        driver.execute_script(
            "arguments[0].scrollTop = arguments[0].scrollHeight",
            scrollable
        )
        time.sleep(scroll_pause)

        reviews = find_review_elements(driver)
        current_count = len(reviews)

        if current_count == last_review_count:
            stale_count += 1
            if stale_count >= 3:
                logger.info(f"Finished scrolling. Total reviews loaded: {current_count}")
                return
        else:
            stale_count = 0
            last_review_count = current_count

        if scroll_num % 10 == 0:
            logger.info(f"Scrolling... {current_count} reviews loaded")

    logger.info(f"Max scrolls reached. Reviews loaded: {last_review_count}")


def find_review_elements(driver):
    """Try multiple selectors to find review containers."""
    selectors = [
        "div[data-review-id]",
        "div[data-reviewid]",
        "div.jftiEf",
        "div.GHT2ce",
        "div.WMbnJf",
    ]
    for sel in selectors:
        elements = driver.find_elements(By.CSS_SELECTOR, sel)
        if elements:
            return elements
    return []


def count_unanswered_reviews(driver):
    result = {
        'reviews_loaded': 0,
        'answered': 0,
        'unanswered': 0,
        'unanswered_pct': 0,
    }

    try:
        review_elements = find_review_elements(driver)
        total = len(review_elements)
        result['reviews_loaded'] = total

        if total == 0:
            return result

        answered = 0
        for review_el in review_elements:
            try:
                owner_responses = review_el.find_elements(By.CSS_SELECTOR, 'div.CDe7pd')
                if not owner_responses:
                    owner_responses = review_el.find_elements(
                        By.XPATH, './/span[contains(text(), "Response from")]'
                    )
                if not owner_responses:
                    owner_responses = review_el.find_elements(
                        By.XPATH, './/span[contains(text(), "Antwort von")]'
                    )
                if not owner_responses:
                    owner_responses = review_el.find_elements(By.CSS_SELECTOR, 'div.d6SCIc')
                if owner_responses:
                    answered += 1
            except:
                continue

        unanswered = total - answered
        result['answered'] = answered
        result['unanswered'] = unanswered
        result['unanswered_pct'] = round((unanswered / total) * 100, 1) if total > 0 else 0

    except Exception as e:
        logger.error(f"Error counting reviews: {e}")

    return result


def run_single_place_audit(maps_url: str, place_id: str, max_retries: int = 2) -> dict | None:
    """
    Run audit on a single Google Maps place.
    Returns dict with reviews_loaded, answered, unanswered, unanswered_pct
    or None on failure.
    """
    search_url = f"https://www.google.com/maps/search/?api=1&query=Google&query_place_id={place_id}&hl=en"

    for attempt in range(max_retries):
        driver = None
        try:
            driver = create_driver()
            logger.info(f"Attempt {attempt + 1}: Loading {search_url}")

            driver.get(search_url)
            time.sleep(3)
            accept_google_consent(driver)
            time.sleep(3)

            if "consent.google" in driver.current_url:
                driver.get(search_url)
                time.sleep(5)

            WebDriverWait(driver, 15).until(
                lambda d: len(d.find_elements(By.CSS_SELECTOR, "h1")) > 0
            )
            time.sleep(3)

            h1_elements = driver.find_elements(By.CSS_SELECTOR, "h1")
            place_name = h1_elements[0].text if h1_elements else "Unknown"
            logger.info(f"Place loaded: {place_name}")

            total_reviews = 0
            try:
                from scrapy import Selector
                selector = Selector(text=driver.page_source)
                reviews_text = selector.xpath(
                    '//span[contains(@aria-label, "review")]/@aria-label'
                ).get()
                if reviews_text:
                    match = re.search(r'([\d,]+)', reviews_text)
                    if match:
                        total_reviews = int(match.group(1).replace(',', ''))
            except:
                pass

            logger.info(f"Total reviews on page: {total_reviews}")

            if total_reviews > 500:
                max_scrolls = 30
            elif total_reviews > 200:
                max_scrolls = 50
            else:
                max_scrolls = 80

            if open_reviews_tab(driver):
                time.sleep(3)
                scroll_reviews(driver, max_scrolls=max_scrolls)
                result = count_unanswered_reviews(driver)
                result['total_reviews_on_page'] = total_reviews
                result['place_name'] = place_name

                logger.info(
                    f"Audit result: {result['reviews_loaded']} loaded, "
                    f"{result['unanswered']} unanswered ({result['unanswered_pct']}%)"
                )
                return result
            else:
                logger.warning("Could not open reviews tab")

        except Exception as e:
            logger.error(f"Attempt {attempt + 1} failed: {e}", exc_info=True)
            if attempt < max_retries - 1:
                time.sleep(3)

        finally:
            if driver:
                try:
                    driver.quit()
                except:
                    pass

    logger.error(f"All attempts failed for place_id: {place_id}")
    return None


if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    test_place_id = sys.argv[1] if len(sys.argv) > 1 else "ChIJM1KEgCTcQUcRnr7f9tjnbmo"
    print(f"Testing audit for place_id: {test_place_id}")
    result = run_single_place_audit("", test_place_id)
    if result:
        print(f"\nResult:")
        for k, v in result.items():
            print(f"  {k}: {v}")
    else:
        print("Audit failed.")
