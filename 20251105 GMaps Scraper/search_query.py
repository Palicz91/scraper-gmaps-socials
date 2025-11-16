import time
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import re
from selenium.webdriver.chrome.options import Options


def create_driver():
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--window-size=1280,1000")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--disable-blink-features=AutomationControlled")
    driver = webdriver.Chrome(options=options)
    return driver


def get_queries():
    with open('google_maps_queries.txt', 'r') as file:
        queries = [line.strip() for line in file.readlines() if line.strip()]
    return queries


def scroll_and_extract_links(driver, query):
    """
    Scroll through all Google Maps search results and extract location links.
    """
    print(f"Searching for: {query}")
    
    # Navigate to Google Maps search
    query_encoded = query.replace(' ', '+')
    driver.get(f"https://www.google.com/maps/search/{query_encoded}?hl=en")
    time.sleep(5)
    
    links = set()  # Use set to avoid duplicates
    last_height = 0
    scroll_attempts = 0
    max_scroll_attempts = 2  # Prevent infinite scrolling
    
    try:
        # Wait for the results panel to load
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "div[role='feed']"))
        )
        
        while scroll_attempts < max_scroll_attempts:
            # Find the scrollable results container
            results_container = driver.find_element(By.CSS_SELECTOR, "div[role='feed']")
            
            # Scroll to the bottom of the results
            driver.execute_script("arguments[0].scrollTop = arguments[0].scrollHeight", results_container)
            time.sleep(3)  # Wait for new results to load
            
            # Get current scroll height
            current_height = driver.execute_script("return arguments[0].scrollHeight", results_container)
            
            # Extract links from current view
            try:
                # Look for business listing links
                business_links = driver.find_elements(By.CSS_SELECTOR, "a[href*='/maps/place/']")
                
                for link in business_links:
                    href = link.get_attribute('href')
                    if href and '/maps/place/' in href:
                        links.add(href)
                
                print(f"  Found {len(links)} unique links so far...")
                
            except Exception as e:
                print(f"  Error extracting links: {e}")
            
            # Check if we've reached the bottom
            if current_height == last_height:
                scroll_attempts += 1
                print(f"  No new content loaded, attempt {scroll_attempts}/{max_scroll_attempts}")
            else:
                scroll_attempts = 0  # Reset counter if we found new content
                last_height = current_height
            
            # Also try to click "Load more" button if it exists
            try:
                load_more_button = driver.find_element(By.CSS_SELECTOR, "button[aria-label*='Load more']")
                if load_more_button.is_displayed():
                    load_more_button.click()
                    time.sleep(3)
                    print("  Clicked 'Load more' button")
            except NoSuchElementException:
                pass  # No load more button found
        
        print(f"  Finished scrolling. Total unique links found: {len(links)}")
        return list(links)
        
    except TimeoutException:
        print(f"  Timeout waiting for results to load")
        return []
    except Exception as e:
        print(f"  Error during scrolling: {e}")
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
            links = search_query(driver, query)
            all_links.update(links)
            
            # Save progress after each query
            save_links_to_file(list(all_links), "links.txt")
            
            # Small delay between queries
            time.sleep(2)
            
    except KeyboardInterrupt:
        print("\nProcess interrupted by user. Saving current progress...")
    except Exception as e:
        print(f"Error during execution: {e}")
    finally:
        # Final save
        save_links_to_file(list(all_links), "links.txt")
        driver.quit()
        print(f"\nProcess completed. Total unique links collected: {len(all_links)}")
