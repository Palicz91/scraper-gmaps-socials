#!/usr/bin/env python3
"""
Social Media Scraper for Business Websites

This script reads a CSV file with business information and scrapes social media
information from their websites using Playwright browser automation.
"""

import csv
import re
import time
import logging
import os
import signal
import sys
from urllib.parse import urljoin, urlparse
from typing import Dict, List, Optional, Tuple
import asyncio
from playwright.async_api import async_playwright, Browser, Page, TimeoutError as PlaywrightTimeoutError
import pandas as pd
import chardet

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('scraper.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# problémás domainek, amiket instant skipelünk
BAD_DOMAIN_PATTERNS = [
    "gulfcar.com",
    "autocarni.com",
]

class SocialMediaScraper:
    def __init__(self, headless: bool = True, timeout: int = 30000, max_scrape_time: int = 60):
        """
        Initialize the social media scraper.
        
        Args:
            headless: Whether to run browser in headless mode
            timeout: Page load timeout in milliseconds
            max_scrape_time: Maximum time in seconds to spend scraping each website
        """
        self.headless = headless
        self.timeout = timeout
        self.max_scrape_time = max_scrape_time
        self.browser = None
        self.playwright = None
        
        # Blacklisted domains that are known to be problematic
        self.BAD_DOMAIN_PATTERNS = [
            "gulfcar.com"
        ]
        
        # Common contact page paths
        self.COMMON_CONTACT_PATHS = [
            "",              # főoldal
            "contact",
            "kontakt",
            "contact-us",
            "about",
            "impressum",
            "kontak",
            "get-in-touch"
        ]
        
        # Email patterns for professional email detection
        self.email_patterns = [
            r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
        ]
        
        
        # Social media URL patterns
        self.social_patterns = {
            'facebook': [
                r'https?://(?:www\.)?facebook\.com/[A-Za-z0-9._-]+/?',
                r'https?://(?:www\.)?fb\.com/[A-Za-z0-9._-]+/?',
                r'https?://(?:www\.)?m\.facebook\.com/[A-Za-z0-9._-]+/?'
            ],
            'instagram': [
                r'https?://(?:www\.)?instagram\.com/[A-Za-z0-9._-]+/?',
                r'https?://(?:www\.)?instagr\.am/[A-Za-z0-9._-]+/?'
            ],
            'linkedin': [
                r'https?://(?:www\.)?linkedin\.com/(?:in|company)/[A-Za-z0-9._-]+/?',
                r'https?://(?:www\.)?linkedin\.com/company/[A-Za-z0-9._-]+/?'
            ],
            'twitter': [
                r'https?://(?:www\.)?twitter\.com/[A-Za-z0-9._-]+/?',
                r'https?://(?:www\.)?x\.com/[A-Za-z0-9._-]+/?',
                r'https?://(?:www\.)?t\.co/[A-Za-z0-9._-]+/?'
            ],
            'tiktok': [
                r'https?://(?:www\.)?tiktok\.com/@[A-Za-z0-9._-]+/?',
                r'https?://(?:www\.)?tiktok\.com/[A-Za-z0-9._-]+/?',
                r'https?://vm\.tiktok\.com/[A-Za-z0-9._-]+/?'
            ]
        }
        
        # Professional email indicators (higher score = more professional)
        self.professional_indicators = {
            'info@': 10,
            'contact@': 9,
            'hello@': 8,
            'support@': 7,
            'sales@': 6,
            'admin@': 5,
            'office@': 4,
            'business@': 3,
            'general@': 2,
            'noreply@': 1,
            'no-reply@': 1
        }
        
        # Unprofessional email indicators (lower score)
        self.unprofessional_indicators = [
            'test@', 'temp@', 'example@', 'sample@', 'dummy@'
        ]

    def get_best_email(self, emails: List[str]) -> str:
        """Score emails and return the most professional one."""
        def score(email):
            score = 0
            for key, val in self.professional_indicators.items():
                if key in email:
                    score += val
            if any(bad in email for bad in self.unprofessional_indicators):
                score -= 10
            if any(domain in email for domain in ['gmail.com', 'yahoo.com', 'hotmail.com']):
                score -= 5
            return score
        
        if not emails:
            return ''
        return sorted(emails, key=score, reverse=True)[0]

    async def start_browser(self):
        """Start the Playwright browser."""
        try:
            self.playwright = await async_playwright().start()
            self.browser = await self.playwright.chromium.launch(
                headless=self.headless,
                args=['--no-sandbox', '--disable-dev-shm-usage']
            )
            logger.info("Browser started successfully")
        except Exception as e:
            logger.error(f"Failed to start browser: {e}")
            raise

    async def close_browser(self):
        """Close the browser."""
        if self.browser:
            await self.browser.close()
        if self.playwright:
            await self.playwright.stop()
        logger.info("Browser closed")

    def extract_emails(self, text: str) -> str:
        """Extract ALL emails from text and return them comma-separated."""
        emails = []
        for pattern in self.email_patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            emails.extend(matches)

        # obfuszkált formák
        emails.extend(self._normalize_obfuscated(text))

        # Remove duplicates and convert to lowercase
        emails = list(set([email.lower() for email in emails]))
        
        # Return all emails comma-separated
        return ', '.join(emails)

    def _normalize_obfuscated(self, text: str) -> List[str]:
        """Extract obfuscated emails with [at] and [dot] patterns."""
        candidates = []
        patterns = [
            (r'\s?\[at\]\s?', '@'),
            (r'\s?\(at\)\s?', '@'),
            (r'\s? at \s?', '@'),
            (r'\s?\[dot\]\s?', '.'),
            (r'\s?\(dot\)\s?', '.'),
            (r'\s? dot \s?', '.')
        ]
        for m in re.findall(r'[A-Za-z0-9._%+-]+\s?(?:\[at\]|\(at\)|at)\s?[A-Za-z0-9.-]+\s?(?:\[dot\]|\(dot\)|dot)\s?[A-Za-z]{2,}', text, re.IGNORECASE):
            clean = m
            for pat, repl in patterns:
                clean = re.sub(pat, repl, clean, flags=re.IGNORECASE)
            candidates.append(clean.lower())
        return candidates

    def extract_phone_numbers(self, text: str) -> Tuple[str, str]:
        """Extract phone numbers from text using keyword-based search and Hungarian preferences."""
        candidates = set()

        # 1) kulcsszó közeli blokkokat keressük
        KEYWORDS = ["phone", "telefon", "tel", "kapcsolat", "call", "contact", "mobil", "hívás"]
        blocks = []
        lower = text.lower()
        
        for kw in KEYWORDS:
            idx = lower.find(kw)
            if idx != -1:
                start = max(0, idx - 80)
                end = min(len(text), idx + 120)
                blocks.append(text[start:end])

        # ha nincs kulcsszavas blokk, akkor legfeljebb a legelső 2000 karaktert nézzük
        if not blocks:
            blocks = [text[:2000]]

        # Improved phone regex pattern
        phone_regex = re.compile(r'\+?\d[\d\s().-]{6,20}')
        
        for block in blocks:
            for match in phone_regex.findall(block):
                # Clean the number
                num = re.sub(r'[^\d+]', '', match)
                # Valid phone number length (7-15 digits)
                if 7 <= len(num) <= 15:
                    candidates.add(self.normalize_hu(num))

        # magyar preferencia scoring
        def score(num: str) -> int:
            s = 0
            if num.startswith('+36'):
                s += 3
            elif num.startswith('06'):
                s += 2
            if len(num) >= 9:
                s += 1
            # Prefer numbers that look like mobile (Hungarian mobile numbers)
            if num.startswith('+3620') or num.startswith('+3630') or num.startswith('+3670'):
                s += 2
            elif num.startswith('0620') or num.startswith('0630') or num.startswith('0670'):
                s += 2
            return s

        # Sort by score and limit results
        ordered = sorted(candidates, key=score, reverse=True)
        phones = ordered[:3]  # limitáljuk a legjobb 3-ra

        return ', '.join(phones), ''

    def normalize_hu(self, num: str) -> str:
        """Normalize Hungarian phone numbers to +36 format."""
        if num.startswith('06'):
            return '+36' + num[2:]
        return num

    async def fetch_page_content(self, page: Page, url: str) -> str:
        """Fetch page content with extra timeout and error handling."""
        try:
            async def _goto():
                await page.goto(url, timeout=self.timeout, wait_until="domcontentloaded")
                await asyncio.sleep(0.5)
                return await page.content()

            # extra safeguard: ha a goto+content is túl sokáig tart, dobjuk
            return await asyncio.wait_for(_goto(), timeout=float(self.max_scrape_time))
        except (PlaywrightTimeoutError, asyncio.TimeoutError) as e:
            logger.warning(f"Timeout loading {url}: {e}")
            return ""
        except Exception as e:
            logger.warning(f"Error loading {url}: {e}")
            return ""

    def extract_social_links(self, content: str, base_url: str) -> Dict[str, str]:
        """Extract social media links from page content."""
        social_links = {}
        for platform, patterns in self.social_patterns.items():
            for pattern in patterns:
                matches = re.findall(pattern, content, re.IGNORECASE)
                if matches:
                    link = matches[0]
                    if not link.startswith('http'):
                        link = 'https://' + link
                    social_links[platform] = link
                    break
        return social_links

    async def check_meta_tags(self, page: Page, result: Dict[str, str], base_url: str):
        """Check meta tags for social media links."""
        try:
            # Check for Open Graph meta tags
            try:
                og_tags = await asyncio.wait_for(
                    page.query_selector_all('meta[property^="og:"]'),
                    timeout=5.0
                )
                for tag in og_tags:
                    try:
                        property_name = await asyncio.wait_for(
                            tag.get_attribute('property'),
                            timeout=1.0
                        )
                        content = await asyncio.wait_for(
                            tag.get_attribute('content'),
                            timeout=1.0
                        )
                        
                        if property_name and content:
                            if 'facebook' in property_name.lower() and not result['facebook']:
                                if 'facebook.com' in content:
                                    result['facebook'] = content
                            elif 'instagram' in property_name.lower() and not result['instagram']:
                                if 'instagram.com' in content:
                                    result['instagram'] = content
                            elif 'linkedin' in property_name.lower() and not result['linkedin']:
                                if 'linkedin.com' in content:
                                    result['linkedin'] = content
                            elif 'twitter' in property_name.lower() and not result['twitter']:
                                if 'twitter.com' in content or 'x.com' in content:
                                    result['twitter'] = content
                            elif 'tiktok' in property_name.lower() and not result['tiktok']:
                                if 'tiktok.com' in content:
                                    result['tiktok'] = content
                    except asyncio.TimeoutError:
                        continue
                    except Exception as e:
                        logger.debug(f"Error processing meta tag: {e}")
                        continue
            except asyncio.TimeoutError:
                logger.debug("Timeout querying Open Graph meta tags")
            except Exception as e:
                logger.debug(f"Error querying Open Graph meta tags: {e}")
            
        except Exception as e:
            logger.warning(f"Error checking meta tags: {e}")

    def detect_encoding(self, file_path: str) -> str:
        """Detect the encoding of a file."""
        try:
            with open(file_path, 'rb') as f:
                raw_data = f.read()
                result = chardet.detect(raw_data)
                encoding = result['encoding']
                confidence = result['confidence']
                logger.info(f"Detected encoding: {encoding} (confidence: {confidence:.2f})")

                # --- EZT ADD HOZZÁ ---
                if encoding is None or confidence < 0.7:
                    encoding = 'utf-8-sig'
                elif encoding.lower() in ['iso-8859-2', 'windows-1250']:
                    encoding = 'cp1250'
                # ----------------------

                return encoding
        except Exception as e:
            logger.warning(f"Could not detect encoding: {e}")
            return 'utf-8'

    async def process_csv(self, input_file: str, output_file: str):
        """
        Process the input CSV file with retry logic and better error handling.
        
        Args:
            input_file: Path to input CSV file
            output_file: Path to output CSV file
        """
        context = None
        try:
            # Start one context for all websites (időnként reseteljük)
            context = await self.browser.new_context()
            
            # Detect file encoding first
            detected_encoding = self.detect_encoding(input_file)
            
            # Read input CSV with proper encoding handling
            encodings_to_try = ['utf-8-sig', 'utf-8', detected_encoding, 'cp1250', 'latin-1', 'cp1252', 'iso-8859-1', 'utf-16']
            df = None
            
            for encoding in encodings_to_try:
                try:
                    df = pd.read_csv(input_file, encoding=encoding)
                    logger.info(f"Successfully loaded CSV with {encoding} encoding")
                    break
                except (UnicodeDecodeError, UnicodeError):
                    logger.warning(f"Failed to read with {encoding} encoding, trying next...")
                    continue
            
            if df is None:
                raise ValueError("Could not read CSV file with any of the attempted encodings")
            
            logger.info(f"Loaded {len(df)} rows from {input_file}")
            
            # Check if website column exists
            if 'website' not in df.columns:
                logger.error("'website' column not found in input CSV")
                return
            
            # Add new columns for social media data including raw email
            social_columns = [
                'scraped_email',
                'scraped_email_raw',
                'scraped_phone',
                'scraped_whatsapp',
                'scraped_facebook',
                'scraped_instagram',
                'scraped_linkedin',
                'scraped_twitter',
                'scraped_tiktok'
            ]
            
            for col in social_columns:
                if col not in df.columns:
                    df[col] = ''
            
            # Create initial output file with headers
            df.to_csv(output_file, index=False, encoding='utf-8-sig', sep=',')
            logger.info(f"Created initial output file: {output_file}")
            
            # Process each row with retry logic
            for index, row in df.iterrows():
                website = row['website']
                if pd.isna(website) or str(website).strip() == '':
                    logger.info(f"Row {index + 1}: No website URL, skipping")
                    continue

                website = str(website).strip()

                # --- BLACKLIST CHECK ITT ---
                parsed = urlparse(website if website.startswith(("http://", "https://")) else "https://" + website)
                domain = parsed.netloc.lower()
                if any(bad in domain for bad in BAD_DOMAIN_PATTERNS):
                    logger.warning(f"Row {index + 1}: Blacklisted domain {domain}, skipping")
                    continue
                # ---------------------------

                logger.info(f"Processing row {index + 1}/{len(df)}: {website}")

                # per-URL új page
                try:
                    page = await context.new_page()
                except Exception as e:
                    logger.error(f"Could not open new page for {website}: {e}")
                    # próbáljunk teljes context resetet
                    try:
                        await context.close()
                    except Exception:
                        pass
                    context = await self.browser.new_context()
                    page = await context.new_page()

                # Retry logic for each website
                social_data = {}
                try:
                    for attempt in range(2):  # 2 attempts
                        try:
                            social_data = await asyncio.wait_for(
                                self.scrape_website(website, page),
                                timeout=float(self.max_scrape_time)
                            )
                            break
                        except asyncio.TimeoutError as e:
                            logger.warning(f"Attempt {attempt + 1} TIMEOUT for {website}: {e}")
                            if attempt == 1:
                                raise
                        except Exception as e:
                            logger.warning(f"Attempt {attempt + 1} failed for {website}: {e}")
                            if attempt == 1:
                                raise
                except Exception as e:
                    logger.error(f"All attempts failed for {website}: {e}")
                    social_data = {
                        'email': '', 'email_raw': '', 'phone': '', 'whatsapp': '', 'facebook': '',
                        'instagram': '', 'linkedin': '', 'twitter': '', 'tiktok': ''
                    }
                finally:
                    # per-URL page bezárása, hogy ne maradjon „beragadt" állapot
                    try:
                        await page.close()
                    except Exception:
                        pass
                
                # Update the dataframe
                df.at[index, 'scraped_email'] = social_data.get('email', '')
                df.at[index, 'scraped_email_raw'] = social_data.get('email_raw', '')
                df.at[index, 'scraped_phone'] = social_data.get('phone', '')
                df.at[index, 'scraped_whatsapp'] = social_data.get('whatsapp', '')
                df.at[index, 'scraped_facebook'] = social_data.get('facebook', '')
                df.at[index, 'scraped_instagram'] = social_data.get('instagram', '')
                df.at[index, 'scraped_linkedin'] = social_data.get('linkedin', '')
                df.at[index, 'scraped_twitter'] = social_data.get('twitter', '')
                df.at[index, 'scraped_tiktok'] = social_data.get('tiktok', '')
                
                # X soronként context reset (pl. 200-nál)
                if (index + 1) % 200 == 0:
                    logger.info(f"Resetting browser context at row {index + 1}")
                    try:
                        await context.close()
                    except Exception:
                        pass
                    context = await self.browser.new_context()
                
                # Mentsd csak minden 25. sor után (vagy az utolsó sor végén)
                if (index + 1) % 25 == 0 or (index + 1) == len(df):
                    try:
                        loop = asyncio.get_event_loop()
                        await asyncio.wait_for(
                            loop.run_in_executor(
                                None,
                                lambda: df.to_csv(output_file, index=False, encoding='utf-8-sig', sep=',')
                            ),
                            timeout=10.0
                        )
                        logger.info(f"Progress saved at row {index + 1}")
                    except asyncio.TimeoutError:
                        logger.error(f"Timeout saving file at row {index + 1}")
                    except Exception as e:
                        logger.error(f"Error saving file at row {index + 1}: {e}")
                
                logger.info(f"Completed row {index + 1}")
                
                # Add a small delay between requests
                await asyncio.sleep(0.2)
            
            logger.info(f"Final results saved to {output_file}")
            
        except Exception as e:
            logger.error(f"Error processing CSV: {e}")
            raise
        finally:
            # Ensure context is closed even if an error occurred
            if context:
                try:
                    await context.close()
                    logger.info("Browser context closed")
                except Exception:
                    pass

    async def scrape_website(self, url: str, page: Page) -> Dict[str, str]:
        """
        Scrape a website for social media information by checking multiple pages.
        
        Args:
            url: Website URL to scrape
            page: Playwright page object to use for scraping
            
        Returns:
            Dictionary with scraped social media information
        """
        result = {
            'email': '',
            'email_raw': '',
            'phone': '',
            'whatsapp': '',
            'facebook': '',
            'instagram': '',
            'linkedin': '',
            'twitter': '',
            'tiktok': ''
        }
        
        if not url or url.strip() == '':
            return result

        # Ensure URL has protocol
        url = url.strip()
        if not url.startswith(('http://', 'https://')):
            url = 'https://' + url

        # domain + blacklist check itt is (extra védelem)
        parsed_for_domain = urlparse(url)
        domain = parsed_for_domain.netloc.lower()
        if any(bad in domain for bad in BAD_DOMAIN_PATTERNS):
            logger.warning(f"Skipping blacklisted domain in scrape_website: {domain}")
            return result

        await page.set_viewport_size({"width": 1920, "height": 1080})
        
        try:
            logger.info(f"Scraping: {url}")
            
            parsed = urlparse(url)
            base = f"{parsed.scheme}://{parsed.netloc}"

            start_time = time.time()  # ← FALIÓRA

            all_emails = set()
            all_phones = set()
            all_whatsapp = set()
            social_links_final = {}

            # 0) Check the ORIGINAL URL first
            pages_to_check = [url]

            # 1) Then add typical contact pages on the domain
            for path in self.COMMON_CONTACT_PATHS:
                full_url = base if path == "" else urljoin(base + "/", path)
                if full_url not in pages_to_check:
                    pages_to_check.append(full_url)

            # Check all pages
            for full_url in pages_to_check:
                # faliórás guard: ha túl sok idő ment el, leállunk
                if time.time() - start_time > self.max_scrape_time:
                    logger.warning(f"Max scrape time exceeded for {url}, stopping page checks.")
                    break

                logger.info(f"Checking page: {full_url}")
                
                content = await self.fetch_page_content(page, full_url)
                if not content:
                    continue

                # 1) Extract emails using regex
                emails_in_page = self.extract_emails(content)
                if emails_in_page:
                    for e in emails_in_page.split(","):
                        e = e.strip()
                        if e:
                            all_emails.add(e)

                # 2) Extract mailto links from DOM
                try:
                    mailtos = await page.eval_on_selector_all(
                        "a[href^='mailto:']",
                        "elements => elements.map(el => el.getAttribute('href'))"
                    )
                    for m in mailtos or []:
                        m = m.replace("mailto:", "").strip()
                        if m:
                            all_emails.add(m)
                except Exception:
                    pass

                # 3) Extract tel links from DOM (cleanest source)
                try:
                    tels = await page.eval_on_selector_all(
                        "a[href^='tel:']",
                        "elements => elements.map(el => el.getAttribute('href'))"
                    )
                    for t in tels or []:
                        num = re.sub(r'[^\d+]', '', t.replace('tel:', ''))
                        if 7 <= len(num) <= 15:
                            all_phones.add(self.normalize_hu(num))
                except Exception:
                    pass

                # 4) Extract phone numbers from text
                phones, whats = self.extract_phone_numbers(content)
                if phones:
                    for p in phones.split(","):
                        p = p.strip()
                        if p:
                            all_phones.add(p)
                if whats:
                    for w in whats.split(","):
                        w = w.strip()
                        if w:
                            all_whatsapp.add(w)

                # 5) Extract social media links
                social_links = self.extract_social_links(content, full_url)
                for platform, link in social_links.items():
                    # Only add if we don't have one yet (don't overwrite)
                    if platform not in social_links_final:
                        social_links_final[platform] = link
                        logger.info(f"Found {platform}: {link}")
                
                # Update result for early exit check
                if all_emails:
                    result['email'] = self.get_best_email(list(all_emails))
                if all_phones:
                    result['phone'] = ', '.join(sorted(list(all_phones), key=lambda x: (x.startswith('+36'), x.startswith('06'), len(x)), reverse=True)[:3])
                for platform, link in social_links_final.items():
                    result[platform] = link
                
                # Early exit: ha már van elég adat, ne menj tovább
                if result['email'] and result['phone'] and (
                    result['facebook'] or result['instagram'] or result['linkedin']
                ):
                    logger.info("Sufficient data found, stopping further page checks.")
                    break
            
            # Compile final results
            if all_emails:
                result['email'] = self.get_best_email(list(all_emails))
                result['email_raw'] = ', '.join(sorted(all_emails))
                logger.info(f"Found best email: {result['email']}")
                logger.info(f"All emails: {result['email_raw']}")
            if all_phones:
                # Sort phones by Hungarian preferences
                ordered = sorted(list(all_phones), key=lambda x: (x.startswith('+36'), x.startswith('06'), len(x)), reverse=True)
                result['phone'] = ', '.join(ordered[:3])
                logger.info(f"Found phone numbers: {result['phone']}")
            if all_whatsapp:
                result['whatsapp'] = ', '.join(list(all_whatsapp))
                logger.info(f"Found WhatsApp numbers: {result['whatsapp']}")
            
            for platform, link in social_links_final.items():
                result[platform] = link
                logger.info(f"Found {platform}: {link}")
            
            # Also check for social media links in page source and meta tags from the last page
            try:
                await asyncio.wait_for(
                    self.check_meta_tags(page, result, url),
                    timeout=10.0
                )
            except asyncio.TimeoutError:
                logger.warning(f"Timeout checking meta tags for {url}")
            except Exception as e:
                logger.warning(f"Error checking meta tags for {url}: {e}")
            
        except PlaywrightTimeoutError:
            logger.warning(f"Timeout while scraping {url}")
        except asyncio.TimeoutError:
            logger.warning(f"Overall timeout while scraping {url}")
        except Exception as e:
            logger.error(f"Error scraping {url}: {e}")
        
        return result

# Graceful shutdown handling
def handle_exit(sig, frame):
    logger.info("Graceful shutdown initiated...")
    sys.exit(0)

signal.signal(signal.SIGINT, handle_exit)
signal.signal(signal.SIGTERM, handle_exit)

async def main():
    """Main function to run the scraper."""
    scraper = SocialMediaScraper(
        headless=True,
        timeout=15000,      # 15s page goto timeout
        max_scrape_time=25  # max 25s / website
    )
    
    try:
        await scraper.start_browser()
        await scraper.process_csv('input.csv', 'output.csv')
    except Exception as e:
        logger.error(f"Scraper failed: {e}")
    finally:
        await scraper.close_browser()

if __name__ == "__main__":
    asyncio.run(main())
