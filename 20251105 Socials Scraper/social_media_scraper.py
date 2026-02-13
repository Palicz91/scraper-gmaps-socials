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
import subprocess
from urllib.parse import urljoin, urlparse
from typing import Dict, List, Tuple
import asyncio
from playwright.async_api import async_playwright, Browser, Page, TimeoutError as PlaywrightTimeoutError
import pandas as pd
import chardet
from pathlib import Path
import psutil

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

BROWSER_ARGS = [
    '--no-sandbox',
    '--disable-dev-shm-usage',
    '--disable-gpu',
    '--disable-extensions',
    '--disable-background-networking',
    '--js-flags=--max-old-space-size=512',
    '--renderer-process-limit=2',
]

BAD_DOMAIN_PATTERNS = [
    "gulfcar.com", "autocarni.com", "saulautosales.com",
    "tinyurl.com", "bit.ly", "t.co", "goo.gl",
    "ow.ly", "rebrand.ly", "shorturl.at", "buff.ly", "is.gd",
]


class SocialMediaScraper:
    def __init__(self, headless: bool = True, timeout: int = 10000, max_scrape_time: int = 20):
        self.headless = headless
        self.timeout = timeout
        self.max_scrape_time = max_scrape_time
        self.browser = None
        self.playwright = None

        self.COMMON_CONTACT_PATHS = [
            "", "contact", "kontakt", "contact-us", "about", "impressum", "kontak", "get-in-touch"
        ]

        self.email_patterns = [r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b']

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
            ],
            'twitter': [
                r'https?://(?:www\.)?twitter\.com/[A-Za-z0-9._-]+/?',
                r'https?://(?:www\.)?x\.com/[A-Za-z0-9._-]+/?',
            ],
            'tiktok': [
                r'https?://(?:www\.)?tiktok\.com/@[A-Za-z0-9._-]+/?',
                r'https?://vm\.tiktok\.com/[A-Za-z0-9._-]+/?'
            ]
        }

        self.professional_indicators = {
            'info@': 10, 'contact@': 9, 'hello@': 8, 'support@': 7,
            'sales@': 6, 'admin@': 5, 'office@': 4, 'business@': 3,
            'general@': 2, 'noreply@': 1, 'no-reply@': 1
        }
        self.unprofessional_indicators = ['test@', 'temp@', 'example@', 'sample@', 'dummy@']

    # ── Browser lifecycle ──────────────────────────────────────────────

    async def start_browser(self):
        self.playwright = await async_playwright().start()
        self.browser = await self.playwright.chromium.launch(
            headless=self.headless, args=BROWSER_ARGS
        )
        logger.info("Browser started")

    async def close_browser(self):
        if self.browser:
            try: await self.browser.close()
            except Exception: pass
        if self.playwright:
            try: await self.playwright.stop()
            except Exception: pass
        logger.info("Browser closed")

    async def _full_restart(self, route_handler):
        """Kill everything and restart from scratch."""
        try: await self.browser.close()
        except Exception: pass
        try: await self.playwright.stop()
        except Exception: pass
        try: subprocess.run(['pkill', '-9', '-f', 'chromium'], timeout=5)
        except Exception: pass
        gc.collect()
        await asyncio.sleep(3)
        self.playwright = await async_playwright().start()
        self.browser = await self.playwright.chromium.launch(
            headless=self.headless, args=BROWSER_ARGS
        )
        ctx = await self.browser.new_context()
        await ctx.route("**/*", route_handler)
        logger.info("Full restart complete")
        return ctx

    async def check_memory_and_restart(self, context, route_handler, index):
        """Restart browser if memory > 2 GB."""
        try:
            proc = psutil.Process()
            rss = proc.memory_info().rss / 1024 / 1024
            children = sum(c.memory_info().rss for c in proc.children(recursive=True)) / 1024 / 1024
            total = rss + children

            if total > 2000:
                logger.warning(f"Memory {total:.0f} MB at row {index}, restarting")
                try: await context.close()
                except Exception: pass
                try: await self.browser.close()
                except Exception: pass
                try: await self.playwright.stop()
                except Exception: pass
                try: subprocess.run(['pkill', '-9', '-f', 'chromium'], timeout=5)
                except Exception: pass
                gc.collect()
                await asyncio.sleep(5)
                new_rss = psutil.Process().memory_info().rss / 1024 / 1024
                logger.info(f"Memory after cleanup: {new_rss:.0f} MB")
                self.playwright = await async_playwright().start()
                self.browser = await self.playwright.chromium.launch(
                    headless=self.headless, args=BROWSER_ARGS
                )
                context = await self.browser.new_context()
                await context.route("**/*", route_handler)
                return context

            if (index + 1) % 50 == 0:
                logger.info(f"Memory at row {index}: {total:.0f} MB")
        except Exception as e:
            logger.warning(f"Memory check error: {e}")
        return context

    # ── Extraction helpers ─────────────────────────────────────────────

    def is_valid_email(self, email: str) -> bool:
        email = email.strip().lower()
        if not re.match(r'^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$', email):
            return False
        if any(x in email for x in ['?', '&', '/', '=', ' ']):
            return False
        bad = ['example.com', 'test.com', 'domain.com', 'email.com', 'yoursite.com', 'company.com', 'yourdomain']
        if any(b in email for b in bad):
            return False
        parts = email.split('@')
        if len(parts) != 2: return False
        if len(parts[0]) > 64 or len(parts[1]) > 255: return False
        return True

    def extract_emails(self, text: str) -> str:
        emails = re.findall(self.email_patterns[0], text, re.IGNORECASE)
        emails.extend(self._normalize_obfuscated(text))
        seen, valid = set(), []
        for e in emails:
            el = e.lower()
            if el not in seen and self.is_valid_email(el):
                valid.append(el); seen.add(el)
        return ', '.join(valid)

    def _normalize_obfuscated(self, text: str) -> List[str]:
        candidates = []
        pats = [(r'\s?\[at\]\s?', '@'), (r'\s?\(at\)\s?', '@'), (r'\s? at \s?', '@'),
                (r'\s?\[dot\]\s?', '.'), (r'\s?\(dot\)\s?', '.'), (r'\s? dot \s?', '.')]
        for m in re.findall(r'[A-Za-z0-9._%+-]+\s?(?:\[at\]|\(at\)|at)\s?[A-Za-z0-9.-]+\s?(?:\[dot\]|\(dot\)|dot)\s?[A-Za-z]{2,}', text, re.IGNORECASE):
            clean = m
            for p, r in pats:
                clean = re.sub(p, r, clean, flags=re.IGNORECASE)
            candidates.append(clean.lower())
        return candidates

    def get_best_email(self, emails: List[str]) -> str:
        def score(e):
            s = sum(v for k, v in self.professional_indicators.items() if k in e)
            if any(b in e for b in self.unprofessional_indicators): s -= 10
            if any(d in e for d in ['gmail.com', 'yahoo.com', 'hotmail.com']): s -= 5
            return s
        return sorted(emails, key=score, reverse=True)[0] if emails else ''

    def extract_phone_numbers(self, text: str) -> Tuple[str, str]:
        candidates = set()
        kws = ["phone", "telefon", "tel", "kapcsolat", "call", "contact", "mobil"]
        blocks, lower = [], text.lower()
        for kw in kws:
            idx = lower.find(kw)
            if idx != -1:
                blocks.append(text[max(0, idx-80):min(len(text), idx+120)])
        if not blocks: blocks = [text[:2000]]
        for block in blocks:
            for m in re.findall(r'\+?\d[\d\s().-]{6,20}', block):
                num = re.sub(r'[^\d+]', '', m)
                if 7 <= len(num) <= 15:
                    candidates.add(self.normalize_hu(num))

        def score(n):
            s = 0
            if n.startswith('+36'): s += 3
            elif n.startswith('06'): s += 2
            if len(n) >= 9: s += 1
            return s

        ordered = sorted(candidates, key=score, reverse=True)[:3]
        return ', '.join(ordered), ''

    def normalize_hu(self, num: str) -> str:
        return '+36' + num[2:] if num.startswith('06') else num

    def extract_social_links(self, content: str, base_url: str) -> Dict[str, str]:
        links = {}
        for platform, patterns in self.social_patterns.items():
            for pat in patterns:
                matches = re.findall(pat, content, re.IGNORECASE)
                if matches:
                    link = matches[0]
                    if not link.startswith('http'): link = 'https://' + link
                    links[platform] = link
                    break
        return links

    async def fetch_page_content(self, page: Page, url: str) -> str:
        try:
            async def _goto():
                await page.goto(url, timeout=self.timeout, wait_until="domcontentloaded")
                await asyncio.sleep(0.5)
                return await page.content()
            return await asyncio.wait_for(_goto(), timeout=float(self.max_scrape_time))
        except Exception as e:
            logger.warning(f"Error loading {url}: {e}")
            return ""

    async def check_meta_tags(self, page: Page, result: Dict[str, str], base_url: str):
        try:
            og_tags = await asyncio.wait_for(
                page.query_selector_all('meta[property^="og:"]'), timeout=5.0
            )
            for tag in og_tags:
                try:
                    prop = await asyncio.wait_for(tag.get_attribute('property'), timeout=1.0)
                    content = await asyncio.wait_for(tag.get_attribute('content'), timeout=1.0)
                    if not prop or not content: continue
                    pl = prop.lower()
                    for platform, domain in [('facebook','facebook.com'),('instagram','instagram.com'),
                                             ('linkedin','linkedin.com'),('twitter','twitter.com'),
                                             ('twitter','x.com'),('tiktok','tiktok.com')]:
                        if platform in pl and not result.get(platform) and domain in content:
                            result[platform] = content
                except Exception: continue
        except Exception: pass

    # ── Core scraping ──────────────────────────────────────────────────

    async def scrape_website(self, url: str, page: Page) -> Dict[str, str]:
        result = {
            'email': '', 'email_raw': '', 'phone': '', 'whatsapp': '',
            'facebook': '', 'instagram': '', 'linkedin': '', 'twitter': '', 'tiktok': ''
        }
        if not url or not url.strip(): return result

        row_start = time.time()
        HARD_LIMIT = self.max_scrape_time + 10

        url = url.strip()
        if url.startswith('http://'): url = 'https://' + url[7:]
        elif not url.startswith('https://'): url = 'https://' + url

        parsed = urlparse(url)
        domain = parsed.netloc.lower()
        if any(bad in domain for bad in BAD_DOMAIN_PATTERNS):
            return result

        await page.set_viewport_size({"width": 1920, "height": 1080})

        try:
            base = f"{parsed.scheme}://{parsed.netloc}"
            start = time.time()

            all_emails, all_phones, all_whatsapp = set(), set(), set()
            social_final = {}

            pages_to_check = [url]
            for path in self.COMMON_CONTACT_PATHS:
                full = base if path == "" else urljoin(base + "/", path)
                if full not in pages_to_check:
                    pages_to_check.append(full)

            for full_url in pages_to_check:
                # Mid-scrape memory check
                try:
                    proc = psutil.Process()
                    mid_mem = proc.memory_info().rss + sum(
                        c.memory_info().rss for c in proc.children(recursive=True))
                    if mid_mem > 1500 * 1024 * 1024:
                        logger.warning(f"Memory spike ({mid_mem // 1024 // 1024} MB), aborting website")
                        break
                except Exception: pass

                if time.time() - row_start > HARD_LIMIT: break
                if time.time() - start > self.max_scrape_time: break

                content = await self.fetch_page_content(page, full_url)
                if not content: continue

                if len(content) > 150_000:
                    content = content[:150_000]

                # Emails from HTML
                found = self.extract_emails(content)
                if found:
                    for e in found.split(","):
                        e = e.strip()
                        if e: all_emails.add(e)

                # Mailto links
                try:
                    mailtos = await page.eval_on_selector_all(
                        "a[href^='mailto:']",
                        "elements => elements.map(el => el.getAttribute('href'))")
                    for m in mailtos or []:
                        m = m.replace("mailto:", "").strip()
                        if m: all_emails.add(m)
                except Exception: pass

                # Tel links
                try:
                    tels = await page.eval_on_selector_all(
                        "a[href^='tel:']",
                        "elements => elements.map(el => el.getAttribute('href'))")
                    for t in tels or []:
                        num = re.sub(r'[^\d+]', '', t.replace('tel:', ''))
                        if 7 <= len(num) <= 15:
                            all_phones.add(self.normalize_hu(num))
                except Exception: pass

                # Phones from text
                phones, whats = self.extract_phone_numbers(content)
                for p in phones.split(","):
                    p = p.strip()
                    if p: all_phones.add(p)

                # Social links
                social = self.extract_social_links(content, base)
                if social: social_final.update(social)

                # Early exit if we have enough
                if all_emails and all_phones and any(
                    social_final.get(p) for p in ['facebook', 'instagram', 'linkedin']):
                    break

            # Compile results
            if all_emails:
                result['email'] = self.get_best_email(list(all_emails))
                result['email_raw'] = ', '.join(sorted(all_emails))
            if all_phones:
                ordered = sorted(list(all_phones),
                    key=lambda x: (x.startswith('+36'), x.startswith('06'), len(x)), reverse=True)
                result['phone'] = ', '.join(ordered[:3])
            for platform, link in social_final.items():
                result[platform] = link

            # Meta tags from last page
            try:
                await asyncio.wait_for(self.check_meta_tags(page, result, url), timeout=10.0)
            except Exception: pass

        except Exception as e:
            logger.error(f"Error scraping {url}: {e}")

        return result

    async def scrape_with_hard_timeout(self, url, page, timeout_sec=40):
        try:
            return await asyncio.wait_for(
                self.scrape_website(url, page), timeout=float(timeout_sec))
        except asyncio.TimeoutError:
            logger.warning(f"Hard timeout {timeout_sec}s for {url}")
            return None

    # ── CSV processing ─────────────────────────────────────────────────

    def detect_encoding(self, path: str) -> str:
        try:
            with open(path, 'rb') as f:
                r = chardet.detect(f.read())
            enc, conf = r['encoding'], r['confidence']
            if enc is None or conf < 0.7: return 'utf-8-sig'
            if enc.lower() in ['iso-8859-2', 'windows-1250']: return 'cp1250'
            return enc
        except Exception:
            return 'utf-8'

    async def process_csv(self, input_file: str, output_file: str):
        context = None
        try:
            progress_file = Path("scraper_progress.txt")
            start_index = 0
            if progress_file.exists():
                try:
                    start_index = int(progress_file.read_text().strip())
                    logger.info(f"Resuming from row {start_index}")
                except Exception:
                    start_index = 0

            if start_index > 0 and Path(output_file).exists():
                file_to_read = output_file
            else:
                file_to_read = input_file

            context = await self.browser.new_context()

            async def route_handler(route):
                if route.request.resource_type in ("image", "media", "font", "stylesheet"):
                    await route.abort()
                else:
                    await route.continue_()
            await context.route("**/*", route_handler)

            # Read CSV
            detected = self.detect_encoding(file_to_read)
            df = None
            for enc in ['utf-8-sig', 'utf-8', detected, 'cp1250', 'latin-1']:
                try:
                    with open(file_to_read, 'r', encoding=enc) as f:
                        sample = f.read(2048)
                    sep = ';' if sample.count(';') > sample.count(',') else ','
                    df = pd.read_csv(file_to_read, encoding=enc, dtype=str, low_memory=True, sep=sep)
                    break
                except (UnicodeDecodeError, UnicodeError):
                    continue
            if df is None:
                raise ValueError("Could not read CSV")

            logger.info(f"Loaded {len(df)} rows")
            if 'website' not in df.columns:
                logger.error("No 'website' column"); return

            for col in ['scraped_email', 'scraped_email_raw', 'scraped_phone', 'scraped_whatsapp',
                        'scraped_facebook', 'scraped_instagram', 'scraped_linkedin',
                        'scraped_twitter', 'scraped_tiktok']:
                if col not in df.columns: df[col] = ''

            if start_index == 0:
                df.to_csv(output_file, index=False, encoding='utf-8-sig', sep=',')

            empty_result = {
                'email': '', 'email_raw': '', 'phone': '', 'whatsapp': '',
                'facebook': '', 'instagram': '', 'linkedin': '', 'twitter': '', 'tiktok': ''
            }

            for index, row in df.iterrows():
                if index < start_index: continue

                # Skip rows that already have data
                ex_email = str(df.at[index, 'scraped_email']) if pd.notna(df.at[index, 'scraped_email']) else ''
                ex_fb = str(df.at[index, 'scraped_facebook']) if pd.notna(df.at[index, 'scraped_facebook']) else ''
                ex_phone = str(df.at[index, 'scraped_phone']) if pd.notna(df.at[index, 'scraped_phone']) else ''
                if ex_email or ex_fb or ex_phone:
                    progress_file.write_text(str(index + 1))
                    continue

                context = await self.check_memory_and_restart(context, route_handler, index)

                website = row['website']
                if pd.isna(website) or str(website).strip() == '':
                    progress_file.write_text(str(index + 1))
                    continue

                website = str(website).strip()
                parsed = urlparse(website if website.startswith(("http://", "https://")) else "https://" + website)
                if any(bad in parsed.netloc.lower() for bad in BAD_DOMAIN_PATTERNS):
                    progress_file.write_text(str(index + 1))
                    continue

                logger.info(f"Processing row {index + 1}/{len(df)}: {website}")

                page, social_data = None, dict(empty_result)
                try:
                    page = await context.new_page()
                    page.set_default_navigation_timeout(15000)
                    page.set_default_timeout(5000)
                    result = await self.scrape_with_hard_timeout(website, page, timeout_sec=40)
                    if result is None:
                        logger.warning(f"Timeout for {website}")
                        if page:
                            try: await page.close()
                            except Exception: pass
                        try: await context.close()
                        except Exception: pass
                        gc.collect()
                        try:
                            context = await self.browser.new_context()
                            await context.route("**/*", route_handler)
                        except Exception:
                            context = await self._full_restart(route_handler)
                    else:
                        social_data = result
                except Exception as e:
                    logger.error(f"Failed for {website}: {e}")
                finally:
                    if page:
                        try: await page.close()
                        except Exception: pass

                df.at[index, 'scraped_email'] = social_data.get('email', '')
                df.at[index, 'scraped_email_raw'] = social_data.get('email_raw', '')
                df.at[index, 'scraped_phone'] = social_data.get('phone', '')
                df.at[index, 'scraped_whatsapp'] = social_data.get('whatsapp', '')
                df.at[index, 'scraped_facebook'] = social_data.get('facebook', '')
                df.at[index, 'scraped_instagram'] = social_data.get('instagram', '')
                df.at[index, 'scraped_linkedin'] = social_data.get('linkedin', '')
                df.at[index, 'scraped_twitter'] = social_data.get('twitter', '')
                df.at[index, 'scraped_tiktok'] = social_data.get('tiktok', '')

                # Context reset every 20 rows
                if (index + 1) % 20 == 0:
                    try: await context.close()
                    except Exception: pass
                    gc.collect()
                    await asyncio.sleep(0.5)
                    try:
                        context = await self.browser.new_context()
                        await context.route("**/*", route_handler)
                    except Exception:
                        context = await self._full_restart(route_handler)

                # Full restart every 200 rows
                if (index + 1) % 200 == 0:
                    context = await self._full_restart(route_handler)

                # Save every 25 rows
                if (index + 1) % 25 == 0 or (index + 1) == len(df):
                    try:
                        loop = asyncio.get_event_loop()
                        await asyncio.wait_for(
                            loop.run_in_executor(None,
                                lambda: df.to_csv(output_file, index=False, encoding='utf-8-sig', sep=',')),
                            timeout=10.0)
                        logger.info(f"Saved at row {index + 1}")
                    except Exception as e:
                        logger.error(f"Save error at row {index + 1}: {e}")

                logger.info(f"Completed row {index + 1}")
                progress_file.write_text(str(index + 1))
                await asyncio.sleep(0.2)

            # Final save
            df.to_csv(output_file, index=False, encoding='utf-8-sig', sep=',')
            if progress_file.exists():
                progress_file.unlink()
            logger.info("Scraping completed")

        except Exception as e:
            logger.error(f"CSV processing error: {e}")
            raise
        finally:
            if context:
                try: await context.close()
                except Exception: pass


# Graceful shutdown
def handle_exit(sig, frame):
    logger.info("Shutdown...")
    sys.exit(0)

signal.signal(signal.SIGINT, handle_exit)
signal.signal(signal.SIGTERM, handle_exit)


async def main():
    scraper = SocialMediaScraper(headless=True, timeout=10000, max_scrape_time=20)
    try:
        await scraper.start_browser()
        await scraper.process_csv('input.csv', 'output.csv')
    except Exception as e:
        logger.error(f"Scraper failed: {e}")
    finally:
        await scraper.close_browser()

if __name__ == "__main__":
    asyncio.run(main())
