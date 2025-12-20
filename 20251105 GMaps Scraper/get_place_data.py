import time
import csv
import re
import logging
import os
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
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

    # ✅ FIX: Clean profile minden indításnál (memória-szivárgás megelőzése)
    options.add_argument("--incognito")
    
    # ✅ NEW: Memória optimalizálás
    options.add_argument("--disable-extensions")
    options.add_argument("--disable-plugins")
    options.add_argument("--single-process")  # Kevesebb memória

    # user-agent a blokkolás elkerülésére
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
        
        # Remove duplicates while preserving order
        unique_links = list(dict.fromkeys(links))
        print(f"Loaded {len(links)} links, {len(unique_links)} unique links")
        return unique_links
    except FileNotFoundError:
        print(f"Error: File '{filename}' not found.")
        return []


def extract_place_id_from_url(url):
    """Extract place ID from Google Maps URL."""
    # Look for place ID in the URL
    place_id_match = re.search(r'!19s([^/?]+)', url)
    if place_id_match:
        return place_id_match.group(1)
    
    # Alternative: extract from data parameter
    data_match = re.search(r'/data=([^/?]+)', url)
    if data_match:
        return data_match.group(1)
    
    return None


def extract_coordinates_from_url(url):
    """Extract latitude and longitude from Google Maps URL."""
    # Look for coordinates in the URL
    coord_match = re.search(r'!3d([^!]+)!4d([^!]+)', url)
    if coord_match:
        try:
            lat = float(coord_match.group(1))
            lng = float(coord_match.group(2))
            return lat, lng
        except ValueError:
            pass
    
    return None, None


def get_place_data(driver, url, max_retries=3):
    """Extract all place data from Google Maps page - with crash detection."""
    print(f"Processing: {url}")
    logging.info(f"Processing: {url}")
    
    for attempt in range(max_retries):
        try:
            driver.get(url)
            
            # WebDriverWait BELÜL marad a try-ban, nem return None!
            WebDriverWait(driver, 15).until(
                lambda d: "maps" in d.current_url and len(d.find_elements(By.CSS_SELECTOR, "h1")) > 0
            )
            
            time.sleep(3)  # Wait for page to load
            
            item = dict()
            # Get page source and create Scrapy Selector
            page_source = driver.page_source
            selector = Selector(text=page_source)
            
            # Extract coordinates from URL
            lat, lng = extract_coordinates_from_url(url)
            
            item['name'] = selector.css('h1 ::text').extract_first('')
            item['url'] = url
            item['category'] = selector.css('button.DkEaL  ::text').extract_first('')
            item['website'] = selector.css('a[data-tooltip="Open website"] ::attr(href)').extract_first('')
            item['phone'] = selector.css('button[data-tooltip="Copy phone number"] ::attr(aria-label)').extract_first('')
            item['lat'] = lat
            item['lng'] = lng
            
            # Rating (csillag) - többféle selectorral próbálkozunk
            rating_text = (
                selector.css('.F7nice::text').get()
                or selector.css('.MW4etd::text').get()
                or selector.css('span[aria-label*="star"]::attr(aria-label)').get()
                or selector.css('div[role="img"][aria-label*="star"]::attr(aria-label)').get()
            )

            if rating_text:
                m = re.search(r'([\d.]+)', rating_text)
                item['rating'] = m.group(1) if m else ''
            else:
                item['rating'] = ''

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
            
            print(f"  ✓ Extracted data for: {item['name']}")
            logging.info(f"Successfully extracted data for: {item['name']}")
            return item
            
        except Exception as e:
            error_msg = str(e).lower()
            
            # 🔴 Browser crash detection
            if "crashed" in error_msg or "session" in error_msg or "invalid session" in error_msg:
                print(f"  💀 Browser crashed! Signaling restart...")
                logging.error(f"Browser crashed on {url}: {e}")
                return "BROWSER_CRASHED"  # Signal to main() for restart
            
            # Regular error -> retry
            print(f"  ✗ Attempt {attempt + 1}/{max_retries} failed: {e}")
            logging.error(f"Error processing {url} (attempt {attempt + 1}): {e}")
            
            if attempt < max_retries - 1:
                print(f"  Retrying in 2 seconds...")
                time.sleep(2)
    
    # Csak ha mind a 3 próba sikertelen
    print(f"  Failed to process {url} after {max_retries} attempts")
    logging.error(f"All {max_retries} attempts failed for {url}")
    return None


def save_to_csv(data_list, filename="places_data.csv"):
    """Save place data to CSV file."""
    if not data_list:
        print("No data to save.")
        return
    
    fieldnames = [
        'name', 'url', 'category', 'website', 'phone', 'lat', 'lng', 'reviews',
        'rating', 'address', 'located_in', 'plus_code'
    ]
    
    try:
        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(data_list)
        print(f"✓ Data saved to {filename}")
    except Exception as e:
        print(f"✗ Error saving to CSV: {e}")


def save_single_record_to_csv(record, filename="places_data.csv"):
    """Save a single place record to CSV file (append mode)."""
    fieldnames = [
        'name', 'url', 'category', 'website', 'phone', 'lat', 'lng', 'reviews',
        'rating', 'address', 'located_in', 'plus_code'
    ]
    
    try:
        file_exists = os.path.exists(filename)
        
        with open(filename, 'a', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            
            # Write header only if file is new
            if not file_exists:
                writer.writeheader()
            
            writer.writerow(record)
        print(f"✓ Record saved to {filename}")
    except Exception as e:
        print(f"✗ Error saving record to CSV: {e}")


def get_last_processed_index():
    """Read last processed index from file (0-based)."""
    try:
        with open("last_processed.txt", "r") as f:
            return int(f.read().strip())
    except Exception:
        return 0

def save_last_processed_index(index):
    """Save last processed index to file."""
    try:
        with open("last_processed.txt", "w") as f:
            f.write(str(index))
    except Exception as e:
        logging.error(f"Failed to save progress index: {e}")


def main():
    """Main function to process all links and extract data."""
    print("Starting Google Maps place data extraction using Scrapy Selector...")

    links = read_links_from_file()
    if not links:
        print("No links found. Exiting.")
        return

    driver = None
    processed_count = 0

    start_index = get_last_processed_index()
    if start_index > 0:
        print(f"Resuming from link {start_index + 1}/{len(links)}")

    # indíts egyszer drivert
    try:
        driver = create_driver()
        print("Browser started successfully")
    except Exception as e:
        print(f"Failed to create browser at start: {e}")
        return

    for i, link in enumerate(links[start_index:], start=start_index + 1):
        print(f"\n--- Processing link {i}/{len(links)} ---")

        # ✅ UPDATED: Gyakoribb preventív restart (100 → 50)
        if i % 50 == 0:
            try:
                driver.quit()
            except:
                pass
            time.sleep(1)  # Adj időt a rendszernek
            driver = create_driver()
            print("🔁 Preventive browser restart")

        place_data = get_place_data(driver, link)
        
        # ✅ UPDATED: Dupla crash kezelés
        if place_data == "BROWSER_CRASHED":
            try:
                driver.quit()
            except:
                pass
            time.sleep(2)  # Adj időt a rendszernek
            driver = create_driver()
            print("🔄 Browser restarted after crash")
            logging.info("Browser restarted after crash")
            
            # Retry this same link with fresh browser
            place_data = get_place_data(driver, link)
            
            # Ha még mindig crash → skip this link, ne akadjon be
            if place_data == "BROWSER_CRASHED":
                print(f"  ⚠️  Double crash on {link}, skipping to next link")
                logging.error(f"Double crash on {link}, skipping")
                try:
                    driver.quit()
                except:
                    pass
                time.sleep(2)
                driver = create_driver()
                continue  # Következő linkre
        
        if place_data and place_data != "BROWSER_CRASHED":
            save_single_record_to_csv(place_data, "places_data.csv")
            processed_count += 1
            print(f"Progress: {processed_count} places processed successfully")
        else:
            logging.error(f"Failed to extract data for {link}")

        # minden 5 link után elmentjük a haladást
        if i % 5 == 0:
            save_last_processed_index(i)
            logging.info(f"Progress saved at link {i}/{len(links)}")

    try:
        driver.quit()
    except:
        pass

    # végső állapot mentése
    save_last_processed_index(len(links))
    logging.info("All links processed, progress reset.")

    print(f"\nProcess completed. Total places processed: {processed_count}")


if __name__ == "__main__":
    main()
