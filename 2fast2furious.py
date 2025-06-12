#!/usr/bin/env python3

import json
import sqlite3
import asyncio
import random
import re
import logging
from playwright.async_api import async_playwright
from urllib.parse import urlparse, quote_plus
import time

# Rotating user agents (current versions)
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36 Edg/125.0.0.0',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.5 Safari/605.1.15'
]

def load_config():
    with open('2jznoshit.json', 'r') as f:
        return json.load(f)

def setup_logging(config):
    if config.get('2fast2furious', {}).get('logs', False):
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(message)s',
            handlers=[
                logging.FileHandler('2fast2furious.log'),
                logging.StreamHandler()
            ]
        )
        return logging.getLogger(__name__)
    return None

def log(logger, message):
    if logger:
        logger.info(message)
    else:
        print(f"[LOG] {message}")

def get_unprocessed_records():
    with sqlite3.connect('danger2manifold.db') as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT it_checksum, it_series, it_sea_no, it_src, it_src_link FROM monica WHERE no_monica IS NULL")
        return cursor.fetchall()

def score_url(url, series, season, src_link, position, logger=None):
    if not url or not series:
        return 0
    
    score = 0
    url_lower = url.lower()
    
    # +1 for top result (position 0)
    if position == 0:
        score += 1
        if logger: log(logger, f"Position bonus: +1 (total: {score})")
    
    # +1 for exact series name match - ALL words must be present
    if series:
        series_words = re.findall(r'\w+', series.lower())
        url_normalized = re.sub(r'[^\w]', ' ', url_lower)
        
        if series_words and all(word in url_normalized for word in series_words):
            score += 1
            if logger: log(logger, f"Series match: +1 (total: {score})")
    
    # +1 for exact season number match
    if season:
        season_match = re.search(r'(\d+)', str(season))
        if season_match:
            season_num = season_match.group(1)
            
            patterns = [
                rf'season[-\s_]*{season_num}(?:\D|$)',
                rf's{season_num}(?:\D|$)', 
                rf'/{season_num}(?:/|$)',
                rf'-{season_num}(?:/|$)'
            ]
            
            if any(re.search(pattern, url_lower) for pattern in patterns):
                score += 1
                if logger: log(logger, f"Season match: +1 (total: {score})")
    
    # +2 for source domain match
    if src_link:
        try:
            url_domain = urlparse(url).netloc.lower().replace('www.', '')
            
            if src_link.startswith(('http://', 'https://')):
                src_domain = urlparse(src_link).netloc.lower().replace('www.', '')
            else:
                src_domain = src_link.lower()
                if not '.' in src_domain:
                    src_domain = f"{src_domain}.com"
            
            if url_domain and src_domain and url_domain == src_domain:
                score += 2
                if logger: log(logger, f"Domain match: +2 (total: {score})")
                
        except Exception as e:
            if logger: log(logger, f"Domain match error: {e}")
    
    return score

async def try_google_search(page, query, logger):
    """Try Google search with multiple fallback strategies"""
    try:
        # Random delay to avoid detection
        await asyncio.sleep(random.uniform(2, 4))
        
        search_url = f"https://www.google.com/search?q={quote_plus(query)}&num=10"
        await page.goto(search_url, wait_until='domcontentloaded', timeout=20000)
        
        # Try multiple selectors for Google results
        selectors = ['h3', '[data-ved] h3', 'h3[class]', '.LC20lb', '.yuRUbf h3']
        
        for selector in selectors:
            try:
                await page.wait_for_selector(selector, timeout=3000)
                break
            except:
                continue
        else:
            # Check if we hit CAPTCHA or blocked
            page_content = await page.content()
            if 'captcha' in page_content.lower() or 'unusual traffic' in page_content.lower():
                log(logger, "Google CAPTCHA detected")
                return None
            log(logger, "No Google results found")
            return None
        
        # Extract URLs with bulletproof logic
        urls = await page.evaluate("""
            () => {
                const results = [];
                
                // Try multiple selectors for links
                const selectors = [
                    'h3 a[href^="/url?q="]',
                    'a[href^="/url?q="]', 
                    'h3 a[href^="http"]',
                    'a[href^="http"]:not([href*="google"]):not([href*="youtube.com/results"])'
                ];
                
                for (const selector of selectors) {
                    const links = document.querySelectorAll(selector);
                    for (const link of links) {
                        let href = link.href;
                        
                        // Decode Google redirect URLs
                        if (href.includes('/url?q=')) {
                            const url = new URL(href);
                            href = decodeURIComponent(url.searchParams.get('q') || '');
                        }
                        
                        if (href && 
                            href.startsWith('http') &&
                            !href.includes('google.com') && 
                            !href.includes('youtube.com/results') &&
                            !href.includes('webcache.googleusercontent.com') &&
                            !href.includes('translate.google.com')) {
                            results.push(href);
                        }
                    }
                    
                    if (results.length >= 3) break;
                }
                
                return [...new Set(results)].slice(0, 3);
            }
        """)
        
        return urls if urls and len(urls) > 0 else None
        
    except Exception as e:
        log(logger, f"Google search failed: {e}")
        return None

async def try_duckduckgo_search(page, query, logger):
    """Fallback to DuckDuckGo search"""
    try:
        await asyncio.sleep(random.uniform(1, 2))
        
        search_url = f"https://duckduckgo.com/?q={quote_plus(query)}"
        await page.goto(search_url, wait_until='domcontentloaded', timeout=15000)
        
        # Wait for DuckDuckGo results
        try:
            await page.wait_for_selector('[data-testid="result"]', timeout=8000)
        except:
            log(logger, "No DuckDuckGo results found")
            return None
        
        urls = await page.evaluate("""
            () => {
                const results = [];
                const links = document.querySelectorAll('[data-testid="result"] a[href^="http"]');
                
                for (const link of links) {
                    const href = link.href;
                    if (href && 
                        href.startsWith('http') &&
                        !href.includes('duckduckgo.com')) {
                        results.push(href);
                    }
                }
                
                return [...new Set(results)].slice(0, 3);
            }
        """)
        
        return urls if urls and len(urls) > 0 else None
        
    except Exception as e:
        log(logger, f"DuckDuckGo search failed: {e}")
        return None

async def try_bing_search(page, query, logger):
    """Final fallback to Bing search"""
    try:
        await asyncio.sleep(random.uniform(1, 2))
        
        search_url = f"https://www.bing.com/search?q={quote_plus(query)}"
        await page.goto(search_url, wait_until='domcontentloaded', timeout=15000)
        
        try:
            await page.wait_for_selector('.b_algo h2 a', timeout=8000)
        except:
            log(logger, "No Bing results found")
            return None
        
        urls = await page.evaluate("""
            () => {
                const results = [];
                const links = document.querySelectorAll('.b_algo h2 a[href^="http"]');
                
                for (const link of links) {
                    const href = link.href;
                    if (href && 
                        href.startsWith('http') &&
                        !href.includes('bing.com')) {
                        results.push(href);
                    }
                }
                
                return [...new Set(results)].slice(0, 3);
            }
        """)
        
        return urls if urls and len(urls) > 0 else None
        
    except Exception as e:
        log(logger, f"Bing search failed: {e}")
        return None

async def search_urls(series, season, src, logger):
    """Bulletproof search with multiple engines"""
    query = f'"{series}" "{season}" "{src}"'
    log(logger, f"Searching: {query}")
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=[
                '--no-sandbox', 
                '--disable-blink-features=AutomationControlled',
                '--disable-web-security',
                '--disable-features=VizDisplayCompositor',
                '--disable-dev-shm-usage'
            ]
        )
        
        context = await browser.new_context(
            user_agent=random.choice(USER_AGENTS),
            viewport={'width': 1366 + random.randint(-100, 100), 'height': 768 + random.randint(-50, 50)},
            extra_http_headers={
                'Accept-Language': 'en-US,en;q=0.9',
                'Accept-Encoding': 'gzip, deflate, br',
                'DNT': '1',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1'
            }
        )
        
        page = await context.new_page()
        
        # Set random mouse movements to appear human
        await page.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
            Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4, 5]});
            Object.defineProperty(navigator, 'languages', {get: () => ['en-US', 'en']});
        """)
        
        try:
            # Try search engines in order: Google -> DuckDuckGo -> Bing
            urls = await try_google_search(page, query, logger)
            if urls:
                log(logger, f"Google: Found {len(urls)} URLs")
                return urls
            
            log(logger, "Google failed, trying DuckDuckGo...")
            urls = await try_duckduckgo_search(page, query, logger)
            if urls:
                log(logger, f"DuckDuckGo: Found {len(urls)} URLs")
                return urls
            
            log(logger, "DuckDuckGo failed, trying Bing...")
            urls = await try_bing_search(page, query, logger)
            if urls:
                log(logger, f"Bing: Found {len(urls)} URLs")
                return urls
            
            log(logger, "All search engines failed")
            return []
            
        finally:
            await browser.close()

def update_database(checksum, url, series, season):
    with sqlite3.connect('danger2manifold.db') as conn:
        cursor = conn.cursor()
        
        # Update current record
        cursor.execute("UPDATE monica SET y_monica = ? WHERE it_checksum = ?", (url, checksum))
        
        # Update all matching series/season records that don't have y_monica
        cursor.execute("""
            UPDATE monica SET y_monica = ? 
            WHERE it_series = ? AND it_sea_no = ? AND (y_monica IS NULL OR y_monica = '')
        """, (url, series, season))
        
        return cursor.rowcount

def get_manual_url(series, season):
    """Get manual URL from user - ALWAYS CALLED when no 4+ score"""
    print(f"\nNo 4+ point results for: {series} - {season}")
    url = input("Enter URL (or press Enter to skip): ").strip()
    return url if url and url.startswith(('http://', 'https://')) else None

async def process_record(record, logger):
    checksum, series, season, src, src_link = record
    
    if not series or not season or not src:
        log(logger, f"Skipping incomplete record: {checksum}")
        return
    
    log(logger, f"Processing: {series} - {season}")
    
    try:
        urls = await search_urls(series, season, src, logger)
        
        # BULLETPROOF: Always handle case where no URLs found
        if not urls:
            log(logger, "No URLs found - requesting manual input")
            manual_url = get_manual_url(series, season)
            if manual_url:
                count = update_database(checksum, manual_url, series, season)
                log(logger, f"Manual: {manual_url} - Updated {count} records")
            else:
                log(logger, "Skipped")
            return
        
        log(logger, f"Found {len(urls)} URLs")
        
        # Score each URL
        scored_urls = []
        for i, url in enumerate(urls):
            score = score_url(url, series, season, src_link, i, logger)
            scored_urls.append((url, score))
            log(logger, f"URL: {url} | Score: {score}")
        
        if not scored_urls:
            log(logger, "No valid URLs to score - requesting manual input")
            manual_url = get_manual_url(series, season)
            if manual_url:
                count = update_database(checksum, manual_url, series, season)
                log(logger, f"Manual: {manual_url} - Updated {count} records")
            else:
                log(logger, "Skipped")
            return
        
        # Get best URL
        best_url, best_score = max(scored_urls, key=lambda x: x[1])
        
        if best_score >= 4:
            count = update_database(checksum, best_url, series, season)
            log(logger, f"Auto-selected: {best_url} (score: {best_score}) - Updated {count} records")
        else:
            # ALWAYS ask for manual input when score < 4
            manual_url = get_manual_url(series, season)
            if manual_url:
                count = update_database(checksum, manual_url, series, season)
                log(logger, f"Manual: {manual_url} - Updated {count} records")
            else:
                log(logger, "Skipped")
                
    except Exception as e:
        log(logger, f"Error processing {series}: {e}")
        # BULLETPROOF: Even on errors, offer manual input
        manual_url = get_manual_url(series, season)
        if manual_url:
            count = update_database(checksum, manual_url, series, season)
            log(logger, f"Manual (after error): {manual_url} - Updated {count} records")

async def main():
    print("2FAST2FURIOUS Starting...")
    
    try:
        config = load_config()
        logger = setup_logging(config)
        records = get_unprocessed_records()
        
        if not records:
            print("No records to process")
            return
        
        # Deduplicate by series/season
        seen = set()
        unique_records = []
        for record in records:
            key = (record[1], record[2])  # series, season
            if key not in seen:
                seen.add(key)
                unique_records.append(record)
        
        print(f"Processing {len(unique_records)} unique series/seasons")
        
        for i, record in enumerate(unique_records):
            await process_record(record, logger)
            
            if i < len(unique_records) - 1:
                await asyncio.sleep(random.uniform(3, 6))
        
        print("Complete")
        
    except FileNotFoundError:
        print("2jznoshit.json not found")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    asyncio.run(main())