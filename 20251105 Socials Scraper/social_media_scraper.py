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
import gc
import threading
import subprocess
from urllib.parse import urljoin, urlparse
from typing import Dict, List, Optional, Tuple
import asyncio
from playwright.async_api import async_playwright, Browser, Page, TimeoutError as PlaywrightTimeoutError
import pandas as pd
import chardet
from pathlib import Path
import psutil

# Add timeout exception and handler BEFORE logging config
class TimeoutException(Exception):
    """Raised when SIGALRM fires."""
    pass

def timeout_handler(signum, frame):
    """Handle SIGALRM signal."""
    raise TimeoutException("Hard timeout via SIGALRM")

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
    "saulautosales.com",
    "tinyurl.com",
    "bit.ly",
    "t.co",
    "goo.gl",
    "ow.ly",
    "rebrand.ly",
    "shorturl.at",
    "buff.ly",
    "is.gd",
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
        self.watchdog_triggered = False
        
        # Blacklisted domains that are known to be problematic
        self.BAD_DOMAIN_PATTERNS = [
            "gulfcar.com",
            "tinyurl.com",
            "bit.ly",
            "t.co",
            "goo.gl",
            "ow.ly",
            "rebrand.ly",
            "shorturl.at",
            "buff.ly",
            "is.gd",
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
        
        # Chatbot signatures for detection
        self.chatbot_signatures = [
            'tidio', 'tidiochat',
            'intercom', 'intercom-container',
            'drift', 'drift-widget',
            'crisp', 'crisp-client',
            'hubspot-messages', 'hs-messages',
            'zendesk', 'zopim',
            'livechat', 'livechatinc',
            'tawk', 'tawk.to',
            'freshchat', 'freshdesk',
            'olark',
            'chatra',
            'botpress',
            'voiceflow',
            'landbot',
            'manychat',
            'chatbot.com',
            'kommunicate',
            'userlike',
            'smartsupp',
            'jivochat', 'jivosite'
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

    def detect_chatbot(self, content: str) -> Tuple[bool, str]:
        """Check if page has a chatbot widget."""
        content_lower = content.lower()
        for sig in self.chatbot_signatures:
            if sig in content_lower:
                return True, sig
        return False, ''

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
                args=[
                    '--no-sandbox',
                    '--disable-dev-shm-usage',
                    '--disable-gpu',
                    '--disable-extensions',
                    '--disable-background-networking',
                    '--disable-default-apps',
                    '--disable-sync',
                    '--disable-translate',
                    '--mute-audio',
                    '--no-first-run',
                    '--safebrowsing-disable-auto-update',
                    '--js-flags=--max-old-space-size=512',  # V8 heap limit
                    '--renderer-process-limit=2',  # max 2 renderer process
                ]
            )
            logger.info("Browser started successfully with memory optimizations")
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

    def is_valid_email(self, email: str) -> bool:
        """Validate email format and filter out suspicious patterns."""
        email = email.strip().lower()
        
        # Basic regex check
        if not re.match(r'^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$', email):
            return False
        
        # Check for URL parameters or query strings
        if any(x in email for x in ['?', '&', '/', '=', ' ']):
            return False
        
        # Check for common invalid patterns
        invalid_patterns = [
            'example.com',
            'test.com',
            'domain.com',
            'email.com',
            'yoursite.com',
            'company.com',
            'yourdomain',
        ]
        
        if any(pattern in email for pattern in invalid_patterns):
            return False
        
        # Check for overly long local or domain parts
        parts = email.split('@')
        if len(parts) != 2:
            return False
        
        local, domain = parts
        if len(local) > 64 or len(domain) > 255:
            return False
        
        return True

    def extract_emails(self, text: str) -> str:
        """Extract ALL valid emails from text and return them comma-separated."""
        emails = []
        for pattern in self.email_patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            emails.extend(matches)

        # obfuszkált formák
        emails.extend(self._normalize_obfuscated(text))

        # Remove duplicates and validate
        valid_emails = []
        seen = set()
        for email in emails:
            email_lower = email.lower()
            if email_lower not in seen and self.is_valid_email(email_lower):
                valid_emails.append(email_lower)
                seen.add(email_lower)
        
        # Return all valid emails comma-separated
        return ', '.join(valid_emails)

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
            # Progress file kezelés
            progress_file = Path("scraper_progress.txt")
            start_index = 0
            if progress_file.exists():
                try:
                    start_index = int(progress_file.read_text().strip())
                    logger.info(f"Resuming from row {start_index}")
                except Exception as e:
                    logger.warning(f"Could not read progress file: {e}")
                    start_index = 0
            
            # Ha folytatunk, az output.csv-ból olvassunk (ott vannak az eddigi eredmények)
            if start_index > 0 and Path(output_file).exists():
                file_to_read = output_file
                logger.info(f"Resuming: reading from {output_file}")
            else:
                file_to_read = input_file
                logger.info(f"Fresh start: reading from {input_file}")
            
            # Start one context for all websites
            context = await self.browser.new_context()
            
            # Globális gyorsítás: képek, videók, fontok, css, stb tiltása
            async def route_handler(route):
                rtype = route.request.resource_type
                if rtype in ("image", "media", "font", "stylesheet"):
                    await route.abort()
                else:
                    await route.continue_()

            await context.route("**/*", route_handler)
            
            # Detect file encoding first
            detected_encoding = self.detect_encoding(file_to_read)
            
            # Read input CSV with proper encoding handling
            encodings_to_try = ['utf-8-sig', 'utf-8', detected_encoding, 'cp1250', 'latin-1', 'cp1252', 'iso-8859-1', 'utf-16']
            df = None
            
            for encoding in encodings_to_try:
                try:
                    # Sniff delimiter from first 2048 bytes
                    with open(file_to_read, 'r', encoding=encoding) as f:
                        sample = f.read(2048)
                    
                    if sample.count(';') > sample.count(','):
                        sep = ';'
                    else:
                        sep = ','
                    
                    df = pd.read_csv(file_to_read, encoding=encoding, dtype=str, low_memory=True, sep=sep)
                    logger.info(f"Successfully loaded CSV with {encoding} encoding and '{sep}' separator")
                    break
                except (UnicodeDecodeError, UnicodeError):
                    logger.warning(f"Failed to read with {encoding} encoding, trying next...")
                    continue
            
            if df is None:
                raise ValueError("Could not read CSV file with any of the attempted encodings")
            
            logger.info(f"Loaded {len(df)} rows from {file_to_read}")
            
            # Check if website column exists
            if 'website' not in df.columns:
                logger.error("'website' column not found in input CSV")
                return
            
            # Add new columns for social media data including chatbot detection
            social_columns = [
                'scraped_email',
                'scraped_email_raw',
                'scraped_phone',
                'scraped_whatsapp',
                'scraped_facebook',
                'scraped_instagram',
                'scraped_linkedin',
                'scraped_twitter',
                'scraped_tiktok',
                'has_chatbot',
                'chatbot_type'
            ]
            
            for col in social_columns:
                if col not in df.columns:
                    df[col] = ''
            
            # Create initial output file with headers ONLY if fresh start
            if start_index == 0:
                df.to_csv(output_file, index=False, encoding='utf-8-sig', sep=',')
                logger.info(f"Created initial output file: {output_file}")
            else:
                logger.info(f"Resuming - using existing output file: {output_file}")
            
            # Set up signal handler for hard timeout
            signal.signal(signal.SIGALRM, timeout_handler)
            
            # Process each row with retry logic
            for index, row in df.iterrows():
                # Skip already processed rows
                if index < start_index:
                    continue
                
                # Memory check on EVERY row
                context = await self.check_memory_and_restart(context, route_handler, index)
                
                website = row['website']
                if pd.isna(website) or str(website).strip() == '':
                    logger.info(f"Row {index + 1}: No website URL, skipping")
                    # Save progress even for skipped rows
                    progress_file.write_text(str(index + 1))
                    continue

                website = str(website).strip()

                # --- BLACKLIST CHECK ITT ---
                parsed = urlparse(website if website.startswith(("http://", "https://")) else "https://" + website)
                domain = parsed.netloc.lower()
                if any(bad in domain for bad in self.BAD_DOMAIN_PATTERNS):
                    logger.warning(f"Row {index + 1}: Blacklisted domain {domain}, skipping")
                    # Save progress for blacklisted domains too
                    progress_file.write_text(str(index + 1))
                    continue
                # ---------------------------

                logger.info(f"Processing row {index + 1}/{len(df)}: {website}")
                
                page = None
                social_data = {}
                
                try:
                    page = await context.new_page()
                    page.set_default_navigation_timeout(15000)
                    page.set_default_timeout(5000)
                    
                    # Use hard timeout-protected scraping
                    social_data = await self.scrape_with_hard_timeout(website, page, timeout_sec=40)
                    
                    # Check if timeout occurred
                    if social_data is None:
                        logger.warning(f"Hard timeout for {website} - forcing cleanup")
                        social_data = {
                            'email': '', 'email_raw': '', 'phone': '', 'whatsapp': '', 'facebook': '',
                            'instagram': '', 'linkedin': '', 'twitter': '', 'tiktok': '',
                            'has_chatbot': 'NO', 'chatbot_type': ''
                        }
                        # Force page close
                        if page:
                            try:
                                await page.close()
                            except Exception:
                                pass
                        # Force context reset
                        try:
                            await context.close()
                        except Exception:
                            pass
                        gc.collect()
                        context = await self.browser.new_context()
                        await context.route("**/*", route_handler)
                    
                except Exception as e:
                    logger.error(f"Failed for {website}: {e}")
                    social_data = {
                        'email': '', 'email_raw': '', 'phone': '', 'whatsapp': '', 'facebook': '',
                        'instagram': '', 'linkedin': '', 'twitter': '', 'tiktok': '',
                        'has_chatbot': 'NO', 'chatbot_type': ''
                    }
                    
                finally:
                    # Close page if still open
                    if page:
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
                df.at[index, 'has_chatbot'] = social_data.get('has_chatbot', 'NO')
                df.at[index, 'chatbot_type'] = social_data.get('chatbot_type', '')
                
                # Context reset every 20 rows (reduced from 50)
                if (index + 1) % 20 == 0:
                    logger.info(f"Resetting browser context at row {index + 1} - forcing GC")
                    try:
                        await context.close()
                    except Exception:
                        pass
                    gc.collect()
                    await asyncio.sleep(0.5)
                    try:
                        context = await self.browser.new_context()
                        await context.route("**/*", route_handler)
                    except Exception:
                        logger.warning(f"Browser dead at row {index + 1}, doing full restart")
                        context = await self._full_restart(route_handler)
                
                # Full browser restart every 200 rows (reduced from 500)
                if (index + 1) % 200 == 0:
                    logger.info(f"Full browser restart at row {index + 1}")
                    context = await self._full_restart(route_handler)
                
                # Save every 25 rows
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
                
                # Save progress after each row
                progress_file.write_text(str(index + 1))
                
                # Add a small delay between requests
                await asyncio.sleep(0.2)
            
            # Reset signal handler
            signal.alarm(0)
            
            # Reset progress file after completion
            if progress_file.exists():
                progress_file.unlink()
                logger.info("Progress file deleted - scraping completed")
            
            logger.info(f"Final results saved to {output_file}")
            
        except Exception as e:
            logger.error(f"Error processing CSV: {e}")
            raise
        finally:
            # Cancel any pending alarm
            signal.alarm(0)
            
            # Ensure context is closed even if an error occurred
            if context:
                try:
                    await context.close()
                    logger.info("Browser context closed")
                except Exception:
                    pass

    async def process_row_with_watchdog(self, coro, timeout_sec: int = 45):
        """
        Wrapper that uses a watchdog thread to enforce hard timeout.
        
        Args:
            coro: Coroutine to execute
            timeout_sec: Maximum time to wait before killing the operation
            
        Returns:
            Result of the coroutine or None if timeout
        """
        done = asyncio.Event()
        self.watchdog_triggered = False
        
        def watchdog():
            if not done.wait(timeout_sec):
                logger.warning(f"Watchdog fired after {timeout_sec}s - forcing cleanup")
                self.watchdog_triggered = True
        
        t = threading.Thread(target=watchdog, daemon=True)
        t.start()
        
        try:
            result = await asyncio.wait_for(coro, timeout=float(timeout_sec - 1))
            return result
        except asyncio.TimeoutError:
            logger.warning(f"Asyncio timeout after {timeout_sec}s")
            return None
        finally:
            done.set()

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
            'tiktok': '',
            'has_chatbot': 'NO',
            'chatbot_type': ''
        }
        
        if not url or url.strip() == '':
            return result

        # 🔥 Hard limit egy webhelyre, hogy semmilyen bug ne tudjon 2 napig pörögni
        row_start_time = time.time()
        HARD_LIMIT = self.max_scrape_time + 10  # pl. max_scrape_time=25 mellett 35 sec / domain

        # URL normalizálás - http → https (sok http site redirectel végtelenségig)
        url = url.strip()
        if url.startswith('http://'):
            url = 'https://' + url[7:]
        elif not url.startswith('https://'):
            url = 'https://' + url

        # domain + blacklist check itt is (extra védelem)
        parsed_for_domain = urlparse(url)
        domain = parsed_for_domain.netloc.lower()
        if any(bad in domain for bad in self.BAD_DOMAIN_PATTERNS):
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
            chatbot_detected = False
            chatbot_type_found = ''

            # 0) Check the ORIGINAL URL first
            pages_to_check = [url]

            # 1) Then add typical contact pages on the domain
            for path in self.COMMON_CONTACT_PATHS:
                full_url = base if path == "" else urljoin(base + "/", path)
                if full_url not in pages_to_check:
                    pages_to_check.append(full_url)

            # Check all pages
            for full_url in pages_to_check:
                # Mid-scrape memory check
                try:
                    proc = psutil.Process()
                    mid_mem = proc.memory_info().rss + sum(c.memory_info().rss for c in proc.children(recursive=True))
                    if mid_mem > 1500 * 1024 * 1024:  # 1.5GB
                        logger.warning(f"Memory spike during scrape ({mid_mem // 1024 // 1024} MB), aborting this website")
                        break
                except Exception:
                    pass

                # hard wall erre a website-ra is
                if time.time() - row_start_time > HARD_LIMIT:
                    logger.warning(f"Hard time limit exceeded for {url}, aborting scrape_website early.")
                    break

                # faliórás guard: ha túl sok idő ment el, leállunk
                if time.time() - start_time > self.max_scrape_time:
                    logger.warning(f"Max scrape time exceeded for {url}, stopping page checks.")
                    break

                logger.info(f"Checking page: {full_url}")

                content = await self.fetch_page_content(page, full_url)
                if not content:
                    continue

                # 🔥 Nagyon nagy oldalak levágása, hogy a regexek ne pörgessék végtelenségig a CPU-t
                MAX_CONTENT_LEN = 150_000  # kb. 500 KB szöveg bőven elég, hogy megtaláljuk a kontaktot és a social linkeket
                if len(content) > MAX_CONTENT_LEN:
                    logger.info(f"Content for {full_url} too large ({len(content)} chars), truncating to {MAX_CONTENT_LEN}.")
                    content = content[:MAX_CONTENT_LEN]

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
                social_links = self.extract_social_links(content, base)
                if social_links:
                    social_links_final.update(social_links)

                # NEW: Detect chatbot
                if not chatbot_detected:
                    has_chatbot, chatbot_type = self.detect_chatbot(content)
                    if has_chatbot:
                        chatbot_detected = True
                        chatbot_type_found = chatbot_type
                        logger.info(f"Found chatbot: {chatbot_type}")

                # Check if we have enough data to stop further checks
                if (
                    all_emails
                    and all_phones
                    and (social_links_final.get('facebook') or social_links_final.get('instagram') or social_links_final.get('linkedin'))
                    and chatbot_detected
                ):
                    logger.info("Sufficient data found (including chatbot), stopping further page checks.")
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
            
            # Add chatbot results
            if chatbot_detected:
                result['has_chatbot'] = 'YES'
                result['chatbot_type'] = chatbot_type_found
            
        except PlaywrightTimeoutError:
            logger.warning(f"Timeout while scraping {url}")
        except asyncio.TimeoutError:
            logger.warning(f"Overall timeout while scraping {url}")
        except Exception as e:
            logger.error(f"Error scraping {url}: {e}")
        
        return result

    async def scrape_with_hard_timeout(self, url, page, timeout_sec=40):
        """Scrape with hard asyncio timeout and context recovery."""
        try:
            return await asyncio.wait_for(
                self.scrape_website(url, page),
                timeout=float(timeout_sec)
            )
        except asyncio.TimeoutError:
            logger.warning(f"Hard timeout after {timeout_sec}s for {url}")
            return None

    async def check_memory_and_restart(self, context, route_handler, index):
        """Restart browser if memory usage is too high."""
        try:
            process = psutil.Process()
            rss_mb = process.memory_info().rss / 1024 / 1024
            children_rss = sum(c.memory_info().rss for c in process.children(recursive=True)) / 1024 / 1024
            total_mb = rss_mb + children_rss
            
            if total_mb > 2000:  # 2GB
                logger.warning(f"Memory too high ({total_mb:.0f} MB) at row {index}, forcing full restart")
                
                # 1. Close gracefully
                try:
                    await context.close()
                except Exception:
                    pass
                try:
                    await self.browser.close()
                except Exception:
                    pass
                try:
                    await self.playwright.stop()
                except Exception:
                    pass
                
                # 2. KILL ALL chromium processes hard
                try:
                    subprocess.run(['pkill', '-9', '-f', 'chromium'], timeout=5)
                except Exception:
                    pass
                
                # 3. Wait and force GC
                gc.collect()
                await asyncio.sleep(5)
                
                # 4. Verify memory actually dropped
                new_rss = psutil.Process().memory_info().rss / 1024 / 1024
                logger.info(f"Memory after cleanup: {new_rss:.0f} MB")
                
                # 5. Restart playwright from scratch
                self.playwright = await async_playwright().start()
                self.browser = await self.playwright.chromium.launch(
                    headless=self.headless,
                    args=[
                        '--no-sandbox', '--disable-dev-shm-usage', '--disable-gpu',
                        '--disable-extensions', '--disable-background-networking',
                        '--js-flags=--max-old-space-size=256',
                        '--renderer-process-limit=1', '--single-process',
                    ]
                )
                context = await self.browser.new_context()
                await context.route("**/*", route_handler)
                
                logger.info(f"Full restart complete at row {index}")
                return context
            
            # Only log memory every 50 rows to reduce noise
            if (index + 1) % 50 == 0:
                logger.info(f"Memory usage at row {index}: {total_mb:.0f} MB (Python: {rss_mb:.0f}, Chromium: {children_rss:.0f})")
        except Exception as e:
            logger.warning(f"Error checking memory: {e}")
        
        return context

    async def _full_restart(self, route_handler):
        """Full playwright + browser restart with hard cleanup."""
        try:
            await self.browser.close()
        except Exception:
            pass
        try:
            await self.playwright.stop()
        except Exception:
            pass
        try:
            subprocess.run(['pkill', '-9', '-f', 'chromium'], timeout=5)
        except Exception:
            pass
        gc.collect()
        await asyncio.sleep(3)
        
        self.playwright = await async_playwright().start()
        self.browser = await self.playwright.chromium.launch(
            headless=self.headless,
            args=[
                '--no-sandbox', '--disable-dev-shm-usage', '--disable-gpu',
                '--disable-extensions', '--disable-background-networking',
                '--js-flags=--max-old-space-size=256',
                '--renderer-process-limit=1', '--single-process',
            ]
        )
        context = await self.browser.new_context()
        await context.route("**/*", route_handler)
        logger.info("Full restart complete")
        return context

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
        timeout=10000,      # 10s page goto timeout
        max_scrape_time=20  # max 20s / website
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
