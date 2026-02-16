import time
import csv
import re
import logging
import os
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from scrapy import Selector

# egyszerű fájl alapú logolás
logging.basicConfig(
    filename="scraper_log.txt",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)


def create_driver():
    from selenium.webdriver.chrome.options import Options

    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--window-size=1280,1000")
    options.add_argument("--lang=en")
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

    driver = webdriver.Chrome(options=options)
    driver.set_page_load_timeout(30)
    return driver


def read_links_from_file(filename="links.txt"):
    """Read links from file and remove duplicates."""
    try:
        with open(filename, 'r', encoding='utf-8') as file:
            links = [line.strip() for line in file.readlines() if line.strip()]
        unique_links = list(dict.fromkeys(links))
        print(f"Loaded {len(links)} links, {len(unique_links)} unique links")
        return unique_links
    except FileNotFoundError:
        print(f"Error: File '{filename}' not found.")
        return []


def extract_coordinates_from_url(url):
    """Extract latitude and longitude from Google Maps URL."""
    coord_match = re.search(r'!3d([^!]+)!4d([^!]+)', url)
    if coord_match:
        try:
            lat = float(coord_match.group(1))
            lng = float(coord_match.group(2))
            return lat, lng
        except ValueError:
            pass
    return None, None


def accept_google_consent(driver, timeout=3):
    """Handle Google's GDPR consent popup if it appears."""
    try:
        if "consent.google" not in driver.current_url and "Before you continue" not in driver.page_source:
            return False

        print("  🔍 Consent popup detected, attempting to handle...")

        consent_buttons = [
            "//button[contains(., 'Accept all')]",
            "//button[contains(., 'Reject all')]",
            "//button[contains(., 'Accept')]",
            "//button[contains(., 'I agree')]",
            "//button[contains(., 'Elfogadom')]",
            "//button[contains(., 'Összes elfogadása')]",
            "//button[@aria-label='Accept all']",
            "//form//button[1]",
        ]

        for xpath in consent_buttons:
            try:
                button = WebDriverWait(driver, timeout).until(
                    EC.element_to_be_clickable((By.XPATH, xpath))
                )
                button.click()
                print("  ✓ Consent popup accepted")
                logging.info("Google consent popup accepted")
                time.sleep(2)
                return True
            except:
                continue

        print("  ⚠️  Consent popup detected but no button found")
        return False
    except Exception as e:
        logging.debug(f"No consent popup or failed to handle: {e}")
        return False


def scroll_reviews(driver, max_scrolls=50, scroll_pause=1.5):
    """
    Scroll through all reviews in the review panel.
    Returns when no new reviews are loading.
    """
    # A reviews panel scrollable container
    scrollable_selectors = [
        'div.m6QErb.DxyBCb.kA9KIf.dS8AEf',  # Reviews panel scrollable div
        'div.m6QErb.DxyBCb.kA9KIf',
        'div.m6QErb',
    ]

    scrollable = None
    for sel in scrollable_selectors:
        try:
            scrollable = driver.find_element(By.CSS_SELECTOR, sel)
            break
        except:
            continue

    if not scrollable:
        print("  ⚠️  Could not find scrollable review panel")
        return

    last_review_count = 0
    stale_count = 0

    for scroll_num in range(max_scrolls):
        # Scroll down
        driver.execute_script(
            "arguments[0].scrollTop = arguments[0].scrollHeight",
            scrollable
        )
        time.sleep(scroll_pause)

        # Count current reviews
        reviews = driver.find_elements(
            By.CSS_SELECTOR, 'div[data-review-id]'
        )
        current_count = len(reviews)

        if current_count == last_review_count:
            stale_count += 1
            if stale_count >= 3:
                # No new reviews loaded after 3 scrolls
                print(f"  📜 Finished scrolling. Total reviews loaded: {current_count}")
                return
        else:
            stale_count = 0
            last_review_count = current_count

        if scroll_num % 10 == 0:
            print(f"  📜 Scrolling... {current_count} reviews loaded so far")

    print(f"  📜 Max scrolls reached. Reviews loaded: {last_review_count}")


def count_unanswered_reviews(driver, max_reviews_to_check=None):
    """
    Count total reviews and unanswered reviews on the current page.
    Assumes we're already on the reviews tab.
    
    Args:
        max_reviews_to_check: If set, limits how many reviews to scroll through.
                              None = scroll all (can be slow for 500+ review places).
                              Set to e.g. 100 for faster but approximate results.
    
    Returns:
        dict with total_reviews, answered, unanswered, unanswered_pct
    """
    result = {
        'total_reviews_loaded': 0,
        'answered': 0,
        'unanswered': 0,
        'unanswered_pct': 0,
    }

    try:
        # Find all review containers
        review_elements = driver.find_elements(By.CSS_SELECTOR, 'div[data-review-id]')
        total = len(review_elements)
        result['total_reviews_loaded'] = total

        if total == 0:
            return result

        answered = 0
        for review_el in review_elements:
            try:
                # Check if there's an owner response within this review
                # Google Maps shows "Response from the owner" in a specific div
                owner_responses = review_el.find_elements(
                    By.CSS_SELECTOR, 'div.CDe7pd'  # Owner response container
                )
                if not owner_responses:
                    # Alternative selector
                    owner_responses = review_el.find_elements(
                        By.XPATH, './/span[contains(text(), "Response from")]'
                    )
                if not owner_responses:
                    # Yet another pattern
                    owner_responses = review_el.find_elements(
                        By.XPATH, './/div[contains(@class, "owner-response")]'
                    )
                
                if owner_responses:
                    answered += 1
            except:
                continue

        unanswered = total - answered
        result['answered'] = answered
        result['unanswered'] = unanswered
        result['unanswered_pct'] = round((unanswered / total) * 100, 1) if total > 0 else 0

    except Exception as e:
        print(f"  ⚠️  Error counting reviews: {e}")
        logging.error(f"Error counting reviews: {e}")

    return result


def open_reviews_tab(driver):
    """
    Click on the Reviews tab to open the reviews panel.
    Returns True if successful.
    """
    try:
        # Method 1: Click the reviews tab button
        tab_selectors = [
            "button[aria-label*='Reviews']",
            "button[aria-label*='review']",
            "button.hh2c6[data-tab-index='1']",  # Usually the 2nd tab
        ]
        
        for sel in tab_selectors:
            try:
                tab = driver.find_element(By.CSS_SELECTOR, sel)
                tab.click()
                time.sleep(2)
                return True
            except:
                continue

        # Method 2: Click the review count text (e.g. "1,234 reviews")
        try:
            review_link = driver.find_element(
                By.XPATH, '//button[contains(@aria-label, "review")]'
            )
            review_link.click()
            time.sleep(2)
            return True
        except:
            pass

        # Method 3: Click "More reviews" or similar
        try:
            more_reviews = driver.find_element(
                By.XPATH, '//span[contains(text(), "review")]/..'
            )
            more_reviews.click()
            time.sleep(2)
            return True
        except:
            pass

        print("  ⚠️  Could not find Reviews tab")
        return False

    except Exception as e:
        print(f"  ⚠️  Error opening reviews tab: {e}")
        return False


def sort_reviews_newest(driver):
    """
    Sort reviews by newest first (optional, helps see recent unanswered ones).
    """
    try:
        # Click sort button
        sort_button = driver.find_element(
            By.CSS_SELECTOR, 'button[aria-label="Sort reviews"]'
        )
        sort_button.click()
        time.sleep(1)

        # Click "Newest"
        newest_option = driver.find_element(
            By.XPATH, '//div[@role="menuitemradio" and @data-index="1"]'
        )
        newest_option.click()
        time.sleep(2)
        return True
    except:
        return False


def get_place_data(driver, url, max_retries=3, scrape_reviews=True, max_review_scrolls=50, min_reviews_for_analysis=100):
    """
    Extract all place data from Google Maps page, including unanswered review count.
    
    Args:
        scrape_reviews: If True, also counts unanswered reviews (slower but more data).
        max_review_scrolls: How many times to scroll the review panel.
        min_reviews_for_analysis: Skip review analysis for places below this threshold.
    """
    print(f"Processing: {url}")
    logging.info(f"Processing: {url}")

    for attempt in range(max_retries):
        try:
            driver.get(url)

            accept_google_consent(driver, timeout=3)

            WebDriverWait(driver, 15).until(
                lambda d: "maps" in d.current_url
                    and len(d.find_elements(By.CSS_SELECTOR, "h1")) > 0
                    and "Before you continue" not in d.page_source
            )

            time.sleep(3)

            item = dict()
            page_source = driver.page_source
            selector = Selector(text=page_source)

            lat, lng = extract_coordinates_from_url(url)

            item['name'] = selector.css('h1 ::text').extract_first('')
            item['url'] = url
            item['category'] = selector.css('button.DkEaL  ::text').extract_first('')
            item['website'] = selector.css('a[data-tooltip="Open website"] ::attr(href)').extract_first('')
            item['phone'] = selector.css('button[data-tooltip="Copy phone number"] ::attr(aria-label)').extract_first('')
            item['lat'] = lat
            item['lng'] = lng

            # Rating
            try:
                rating_elem = driver.find_element(By.CSS_SELECTOR, 'div[role="img"][aria-label*="star"]')
                aria = rating_elem.get_attribute('aria-label')
                m = re.search(r'([\d.,]+)', aria)
                item['rating'] = m.group(1).replace(',', '.') if m else ''
            except:
                item['rating'] = ''

            # Total review count (from the main page, before scrolling)
            reviews_text = selector.xpath('//span[contains(@aria-label, "review")]/@aria-label').get()
            if reviews_text:
                reviews_match = re.search(r'([\d,]+)', reviews_text)
                item['reviews'] = reviews_match.group(1).replace(',', '') if reviews_match else ''
            else:
                item['reviews'] = ''

            try:
                item['address'] = selector.css('button[data-item-id="address"] ::text').extract()[-1]
            except:
                item['address'] = ''
            try:
                item['located_in'] = selector.css('button[data-item-id="locatedin"] ::text').extract()[-1]
            except:
                item['located_in'] = ''
            item['plus_code'] = selector.css('button[data-tooltip="Copy plus code"] ::attr(aria-label)').extract_first('')

            # ============================
            # REVIEW ANALYSIS (new feature)
            # ============================
            item['reviews_loaded'] = ''
            item['reviews_answered'] = ''
            item['reviews_unanswered'] = ''
            item['reviews_unanswered_pct'] = ''

            if scrape_reviews and item.get('reviews') and int(item.get('reviews', '0') or '0') > 0:
                total_reviews = int(item['reviews'])
                
                # Skip review analysis for small places (not worth the outreach)
                if total_reviews < min_reviews_for_analysis:
                    print(f"  ⏭️  Skipping review analysis ({total_reviews} < {min_reviews_for_analysis} threshold)")
                else:
                    print(f"  📊 Analyzing reviews ({total_reviews} total)...")

                    # Skip review scrolling for places with 1000+ reviews (too slow)
                    # You can adjust this threshold
                    if total_reviews > 500:
                        print(f"  ⚡ {total_reviews} reviews is a lot, limiting scroll to ~200")
                        effective_scrolls = 30  # ~200 reviews
                    else:
                        effective_scrolls = max_review_scrolls

                    # Open reviews tab
                    if open_reviews_tab(driver):
                        time.sleep(2)
                        
                        # Optional: sort by newest to see recent unanswered ones
                        # sort_reviews_newest(driver)
                        
                        # Scroll to load reviews
                        scroll_reviews(driver, max_scrolls=effective_scrolls)

                        # Count answered vs unanswered
                        review_stats = count_unanswered_reviews(driver)
                        item['reviews_loaded'] = review_stats['total_reviews_loaded']
                        item['reviews_answered'] = review_stats['answered']
                        item['reviews_unanswered'] = review_stats['unanswered']
                        item['reviews_unanswered_pct'] = review_stats['unanswered_pct']

                        print(f"  📊 Reviews: {review_stats['total_reviews_loaded']} loaded, "
                              f"{review_stats['unanswered']} unanswered ({review_stats['unanswered_pct']}%)")
                    else:
                        print("  ⚠️  Could not open reviews tab, skipping review analysis")

            print(f"  ✓ Extracted data for: {item['name']}")
            logging.info(f"Successfully extracted data for: {item['name']}")
            return item

        except Exception as e:
            error_msg = str(e).lower()

            if "crashed" in error_msg or "session" in error_msg or "invalid session" in error_msg:
                print(f"  💀 Browser crashed! Signaling restart...")
                logging.error(f"Browser crashed on {url}: {e}")
                return "BROWSER_CRASHED"

            print(f"  ✗ Attempt {attempt + 1}/{max_retries} failed: {e}")
            logging.error(f"Error processing {url} (attempt {attempt + 1}): {e}")

            if attempt < max_retries - 1:
                print(f"  Retrying in 2 seconds...")
                time.sleep(2)

    print(f"  Failed to process {url} after {max_retries} attempts")
    logging.error(f"All {max_retries} attempts failed for {url}")
    return None


def save_single_record_to_csv(record, filename="places_data.csv"):
    """Save a single place record to CSV file (append mode)."""
    fieldnames = [
        'name', 'url', 'category', 'website', 'phone', 'lat', 'lng',
        'reviews', 'rating', 'address', 'located_in', 'plus_code',
        # New review analysis fields
        'reviews_loaded', 'reviews_answered', 'reviews_unanswered', 'reviews_unanswered_pct'
    ]

    try:
        file_exists = os.path.exists(filename)

        with open(filename, 'a', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            if not file_exists:
                writer.writeheader()
            writer.writerow(record)
        print(f"  ✓ Record saved to {filename}")
    except Exception as e:
        print(f"  ✗ Error saving record to CSV: {e}")


def get_last_processed_index():
    try:
        with open("last_processed.txt", "r") as f:
            return int(f.read().strip())
    except Exception:
        return 0


def save_last_processed_index(index):
    try:
        with open("last_processed.txt", "w") as f:
            f.write(str(index))
    except Exception as e:
        logging.error(f"Failed to save progress index: {e}")


def main():
    """Main function to process all links and extract data."""
    print("Starting Google Maps scraper with review analysis...")
    print("=" * 60)

    # =============================================
    # CONFIG - adjust these for speed vs completeness
    # =============================================
    SCRAPE_REVIEWS = True        # Set False to skip review analysis (faster)
    MAX_REVIEW_SCROLLS = 50      # More scrolls = more reviews loaded, but slower
    MIN_REVIEWS_FOR_ANALYSIS = 100  # Skip review analysis for places with fewer reviews
    OUTPUT_FILE = "places_data.csv"
    # =============================================

    links = read_links_from_file()
    if not links:
        print("No links found. Exiting.")
        return

    driver = None
    processed_count = 0

    start_index = get_last_processed_index()
    if start_index > 0:
        print(f"Resuming from link {start_index + 1}/{len(links)}")

    try:
        driver = create_driver()
        print("Browser started successfully")
    except Exception as e:
        print(f"Failed to create browser at start: {e}")
        return

    for i, link in enumerate(links[start_index:], start=start_index + 1):
        print(f"\n--- Processing link {i}/{len(links)} ---")

        # Preventive restart every 50 links
        if i % 50 == 0:
            try:
                driver.quit()
            except:
                pass
            time.sleep(1)
            driver = create_driver()
            print("🔁 Preventive browser restart")

        place_data = get_place_data(
            driver, link,
            scrape_reviews=SCRAPE_REVIEWS,
            max_review_scrolls=MAX_REVIEW_SCROLLS,
            min_reviews_for_analysis=MIN_REVIEWS_FOR_ANALYSIS
        )

        # Crash handling
        if place_data == "BROWSER_CRASHED":
            try:
                driver.quit()
            except:
                pass
            time.sleep(2)
            driver = create_driver()
            print("🔄 Browser restarted after crash")

            place_data = get_place_data(
                driver, link,
                scrape_reviews=SCRAPE_REVIEWS,
                max_review_scrolls=MAX_REVIEW_SCROLLS,
                min_reviews_for_analysis=MIN_REVIEWS_FOR_ANALYSIS
            )

            if place_data == "BROWSER_CRASHED":
                print(f"  ⚠️  Double crash on {link}, skipping")
                logging.error(f"Double crash on {link}, skipping")
                try:
                    driver.quit()
                except:
                    pass
                time.sleep(2)
                driver = create_driver()
                continue

        if place_data and place_data != "BROWSER_CRASHED":
            save_single_record_to_csv(place_data, OUTPUT_FILE)
            processed_count += 1
            print(f"Progress: {processed_count} places processed successfully")
        else:
            logging.error(f"Failed to extract data for {link}")

        if i % 5 == 0:
            save_last_processed_index(i)
            logging.info(f"Progress saved at link {i}/{len(links)}")

    try:
        driver.quit()
    except:
        pass

    save_last_processed_index(len(links))
    logging.info("All links processed, progress reset.")

    print(f"\nProcess completed. Total places processed: {processed_count}")
    print(f"Output file: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
