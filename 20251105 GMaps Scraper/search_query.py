import time
import traceback
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, WebDriverException
from selenium.webdriver.chrome.options import Options


def create_driver():
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--disable-webgl")
    options.add_argument("--disable-software-rasterizer")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--disable-features=VizDisplayCompositor")
    options.add_argument("--lang=en-US")
    # User agent - ez a kulcs!
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36")
    
    driver = webdriver.Chrome(options=options)
    driver.set_page_load_timeout(30)
    return driver


def handle_consent(driver):
    """Handle Google consent page if it appears."""
    try:
        time.sleep(2)
        if "consent.google" in driver.current_url:
            print("  [INFO] Consent page detected, trying to accept...")
            # Try different button selectors
            buttons = [
                "button[aria-label*='Accept']",
                "button[aria-label*='Hyväksy']",  # Finnish
                "button[aria-label*='Akzeptieren']",  # German
                "form[action*='consent'] button",
                "button[jsname='higCR']",
                "button[jsname='b3VHJd']"
            ]
            for selector in buttons:
                try:
                    btn = driver.find_element(By.CSS_SELECTOR, selector)
                    btn.click()
                    print(f"  [INFO] Clicked consent button: {selector}")
                    time.sleep(2)
                    return True
                except:
                    continue
            # Fallback: try all buttons with "Accept" text
            try:
                all_buttons = driver.find_elements(By.TAG_NAME, "button")
                for btn in all_buttons:
                    if btn.is_displayed():
                        btn.click()
                        time.sleep(2)
                        if "consent.google" not in driver.current_url:
                            print("  [INFO] Consent handled via button click")
                            return True
            except:
                pass
    except Exception as e:
        print(f"  [WARN] Consent handling error: {e}")
    return False


def get_queries():
    with open('google_maps_queries.txt', 'r') as file:
        queries = [line.strip() for line in file.readlines() if line.strip()]
    return queries


def scroll_and_extract_links(driver, query):
    """
    Scroll through all Google Maps search results and extract location links.
    """
    print(f"Searching for: {query}")
    
    query_encoded = query.replace(' ', '+')

    # Oldal betöltése + első várakozás is try-ban
    try:
        driver.get(f"https://www.google.com/maps/search/{query_encoded}?hl=en")
        handle_consent(driver)  # <-- ÚJ: Handle consent if needed
        WebDriverWait(driver, 30).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "div[role='feed']"))
        )
    except TimeoutException:
        print(f"  [TIMEOUT] Initial load timeout for query: {query}")
        return []
    except WebDriverException as e:
        msg = str(e)
        print(f"  [WEBDRIVER ERROR] Initial load error for query: {query} -> {msg}")
        traceback.print_exc()

        # 🔥 Ha konkrétan tab crashed / session deleted / invalid session id, dobjuk tovább,
        # hogy a külső try újraindítsa a drivert
        if ("tab crashed" in msg.lower() 
            or "session deleted" in msg.lower()
            or "invalid session id" in msg.lower()):
            raise

        # Egyéb WebDriver hibáknál csak üres listával visszatérünk
        return []
    except Exception as e:
        print(f"  [ERROR] Initial load error for query: {query} -> {e}")
        traceback.print_exc()
        return []
    
    links = set()
    last_height = 0
    scroll_attempts = 0
    max_scroll_attempts = 2
    
    try:
        results_container = driver.find_element(By.CSS_SELECTOR, "div[role='feed']")
        
        while scroll_attempts < max_scroll_attempts:
            # Scroll down
            driver.execute_script(
                "arguments[0].scrollTop = arguments[0].scrollHeight",
                results_container
            )
            time.sleep(1.5)
            
            current_height = driver.execute_script(
                "return arguments[0].scrollHeight",
                results_container
            )
            
            # Linkek kigyűjtése
            try:
                business_links = driver.find_elements(
                    By.CSS_SELECTOR,
                    "a[href*='/maps/place/']"
                )
                for link in business_links:
                    href = link.get_attribute('href')
                    if href and '/maps/place/' in href:
                        links.add(href)
                print(f"  Found {len(links)} unique links so far...")
            except Exception as e:
                print(f"  Error extracting links: {e}")
                traceback.print_exc()
            
            # Scroll vége ellenőrzés
            if current_height == last_height:
                scroll_attempts += 1
                print(f"  No new content loaded, attempt {scroll_attempts}/{max_scroll_attempts}")
            else:
                scroll_attempts = 0
                last_height = current_height
            
            # Load more gomb, ha van
            try:
                load_more_button = driver.find_element(
                    By.CSS_SELECTOR,
                    "button[aria-label*='Load more']"
                )
                if load_more_button.is_displayed():
                    load_more_button.click()
                    time.sleep(3)
                    print("  Clicked 'Load more' button")
            except NoSuchElementException:
                pass
        
        print(f"  Finished scrolling. Total unique links found: {len(links)}")
        return list(links)
        
    except TimeoutException:
        print(f"  Timeout while scrolling for query: {query}")
        return []
    except WebDriverException as e:
        print(f"  [WEBDRIVER ERROR] During scrolling: {e}")
        traceback.print_exc()
        raise
    except Exception as e:
        print(f"  Error during scrolling for query {query}: {e}")
        traceback.print_exc()
        return []


def search_query(driver, query):
    """
    Search for a query and extract all location links.
    """
    links = scroll_and_extract_links(driver, query)
    return links


def save_links_to_file(all_links, filename="links.txt"):
    """
    Save all extracted links to a file.
    """
    try:
        with open(filename, 'w', encoding='utf-8') as file:
            for link in all_links:
                file.write(f"{link}\n")
        print(f"Saved {len(all_links)} links to {filename}")
    except Exception as e:
        print(f"Error saving links to file: {e}")


if __name__ == '__main__':
    driver = create_driver()
    queries = get_queries()
    all_links = set()  # Use set to avoid duplicates across queries
    
    try:
        for i, query in enumerate(queries, 1):
            print(f"\n--- Processing query {i}/{len(queries)} ---")

            # 🔁 Minden 100. query után indítsunk friss Chrome-ot,
            # hogy ne fújódjon fel egyetlen session
            if i % 100 == 1 and i != 1:
                print(f"--- Restarting driver at query {i} to avoid Chrome bloat ---")
                try:
                    driver.quit()
                except Exception:
                    pass
                driver = create_driver()

            try:
                links = search_query(driver, query)
                print(f"  Query returned {len(links)} links")
                all_links.update(links)
            except Exception as e:
                print(f"[ERROR] Query {i}/{len(queries)} FAILED: {query}")
                print(f"        Exception: {e}")
                traceback.print_exc()
                # opcionális: driver újraindítás, ha nagyon megkergül
                try:
                    driver.quit()
                except Exception:
                    pass
                driver = create_driver()
                # ugrunk a következő queryre
                continue
            
            # Save progress after each query
            save_links_to_file(list(all_links), "links.txt")
            
            # Small delay between queries
            time.sleep(2)
            
    except KeyboardInterrupt:
        print("\nProcess interrupted by user. Saving current progress...")
    finally:
        # Final save
        save_links_to_file(list(all_links), "links.txt")
        try:
            driver.quit()
        except Exception:
            pass
        print(f"\nProcess completed. Total unique links collected: {len(all_links)}")
