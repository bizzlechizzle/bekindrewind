#!/usr/bin/env python3

import json
import sqlite3
import asyncio
import random
from playwright.async_api import async_playwright
from pathlib import Path

# User agents for rotation
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
]

def load_config():
    """Load user preferences from 2jznoshit.json"""
    with open("2jznoshit.json", "r") as f:
        return json.load(f)

def setup_logging(config):
    """Setup logging if enabled in config"""
    if not config.get("tokyo_drift", {}).get("logs", False):
        return None
    
    import logging
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('tokyo_drift.log'),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)

def get_pending_items(db_path="danger2manifold.db"):
    """Get items that need processing (no_monica is NULL)"""
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    
    # Get unique season/series combinations where no_monica is NULL
    cur.execute("""
        SELECT DISTINCT it_sea_no, it_series, y_monica, 
               MIN(it_checksum) as sample_checksum
        FROM monica 
        WHERE no_monica IS NULL 
        GROUP BY it_sea_no, it_series, y_monica
    """)
    
    items = cur.fetchall()
    conn.close()
    return items

def update_html_for_season(db_path, it_sea_no, it_series, html_content):
    """Update all rows for a season with the HTML content"""
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    
    cur.execute("""
        UPDATE monica 
        SET no_monica = ? 
        WHERE it_sea_no = ? AND it_series = ? AND no_monica IS NULL
    """, (html_content, it_sea_no, it_series))
    
    rows_updated = cur.rowcount
    conn.commit()
    conn.close()
    
    return rows_updated

async def scrape_page(url, logger=None):
    """Scrape a single page with playwright, clicking Details tab if present"""
    async with async_playwright() as p:
        # Enable JavaScript since we need to click elements
        browser = await p.chromium.launch(
            headless=True,
            args=[
                '--no-sandbox',
                '--disable-blink-features=AutomationControlled',
                '--disable-extensions',
                '--disable-plugins',
                '--disable-images',  # Speed optimization
            ]
        )
        
        context = await browser.new_context(
            user_agent=random.choice(USER_AGENTS),
            viewport={'width': 1920, 'height': 1080},
            extra_http_headers={
                'Accept-Language': 'en-US,en;q=0.9',
                'Accept-Encoding': 'gzip, deflate, br',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
            }
        )
        
        page = await context.new_page()
        
        try:
            if logger:
                logger.debug(f"Navigating to {url}")
            
            await page.goto(url, timeout=60000, wait_until='domcontentloaded')
            
            # Human-like delay
            await asyncio.sleep(random.uniform(2.0, 4.0))
            
            # Try to click the Details tab if it exists
            try:
                # Multiple selectors to try for the Details tab
                details_selectors = [
                    'button[data-testid="btf-details-tab"]',
                    'button[data-automation-id="btf-details-tab"]',
                    'button#tab-selector-details',
                    'button[name="btf-content-selector"][aria-controls="tab-content-details"]',
                ]
                
                details_clicked = False
                for selector in details_selectors:
                    try:
                        # Check if element exists and is visible
                        element_count = await page.locator(selector).count()
                        if element_count > 0:
                            if logger:
                                logger.debug(f"Found Details tab with selector: {selector}")
                            
                            # Wait for element to be clickable and scroll into view
                            await page.wait_for_selector(selector, state='visible', timeout=5000)
                            await page.locator(selector).scroll_into_view_if_needed()
                            
                            # Try multiple click methods
                            try:
                                # Method 1: Regular click
                                await page.click(selector, timeout=5000)
                                if logger:
                                    logger.debug("Clicked Details tab with regular click")
                            except:
                                try:
                                    # Method 2: Force click
                                    await page.click(selector, force=True, timeout=5000)
                                    if logger:
                                        logger.debug("Clicked Details tab with force click")
                                except:
                                    # Method 3: JavaScript click
                                    await page.evaluate(f'document.querySelector("{selector}").click()')
                                    if logger:
                                        logger.debug("Clicked Details tab with JavaScript")
                            
                            details_clicked = True
                            
                            # Wait for content to load and check if tab is now active
                            await asyncio.sleep(random.uniform(3.0, 5.0))
                            
                            # Check if tab is now selected
                            is_selected = await page.get_attribute(selector, 'aria-selected')
                            if logger:
                                logger.debug(f"Details tab aria-selected: {is_selected}")
                            
                            # Wait for details content to appear
                            try:
                                await page.wait_for_selector('#tab-content-details, [id*="details"], [class*="details"]', timeout=15000)
                                if logger:
                                    logger.debug("Details content appeared")
                            except:
                                if logger:
                                    logger.debug("Details content did not appear within timeout")
                                # Wait a bit more anyway
                                await asyncio.sleep(3)
                            
                            # Additional wait for any AJAX to complete
                            await asyncio.sleep(random.uniform(2.0, 4.0))
                            
                            break
                    except Exception as e:
                        if logger:
                            logger.debug(f"Selector {selector} failed: {e}")
                        continue
                
                if not details_clicked:
                    if logger:
                        logger.debug("No Details tab found or clickable")
            
            except Exception as e:
                if logger:
                    logger.debug(f"Failed to click Details tab: {e}")
            
            # Scroll to trigger lazy loading after potentially loading details
            prev_height = 0
            scroll_attempts = 0
            max_scrolls = 12  # Increased for more content
            
            while scroll_attempts < max_scrolls:
                await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                await asyncio.sleep(random.uniform(1.5, 2.5))
                
                curr_height = await page.evaluate("document.body.scrollHeight")
                if curr_height == prev_height:
                    break
                    
                prev_height = curr_height
                scroll_attempts += 1
            
            # Final wait for any remaining AJAX requests
            await asyncio.sleep(3)
            
            # Get full HTML
            html_content = await page.content()
            
            # Sanity check - be more lenient since details might add significant content
            if len(html_content) < 5000:
                raise Exception(f"HTML too short ({len(html_content)} chars) - likely missing content")
            
            if logger:
                logger.debug(f"Successfully scraped {len(html_content)} chars from {url}")
            
            return html_content
            
        except Exception as e:
            if logger:
                logger.error(f"Failed to scrape {url}: {e}")
            raise
        finally:
            await browser.close()

async def process_items(items, db_path, logger=None):
    """Process all pending items"""
    total_items = len(items)
    
    for i, (it_sea_no, it_series, y_monica, sample_checksum) in enumerate(items, 1):
        try:
            if logger:
                logger.info(f"Processing {i}/{total_items}: {it_series} - {it_sea_no}")
            
            html_content = await scrape_page(y_monica, logger)
            rows_updated = update_html_for_season(db_path, it_sea_no, it_series, html_content)
            
            if logger:
                logger.info(f"Updated {rows_updated} rows for {it_series} - {it_sea_no}")
            
            # Respectful delay between requests
            if i < total_items:
                delay = random.uniform(4, 8)  # Slightly longer delays
                if logger:
                    logger.debug(f"Waiting {delay:.1f}s before next request")
                await asyncio.sleep(delay)
                
        except Exception as e:
            if logger:
                logger.error(f"Failed to process {it_series} - {it_sea_no}: {e}")
            continue

async def main():
    """Main execution function"""
    # Step 1: Load config
    config = load_config()
    logger = setup_logging(config)
    
    if logger:
        logger.info("Starting tokyo_drift.py")
    
    # Step 2: Get pending items
    db_path = "danger2manifold.db"
    items = get_pending_items(db_path)
    
    if not items:
        if logger:
            logger.info("No items to process")
        print("No items to process")
        return
    
    if logger:
        logger.info(f"Found {len(items)} unique items to process")
    
    # Step 3 & 4: Process items
    await process_items(items, db_path, logger)
    
    if logger:
        logger.info("Processing complete")

if __name__ == "__main__":
    asyncio.run(main())