#!/usr/bin/env python3
"""
Online Source Metadata Import - ULTRATHINK KISS VERSION
Bulletproof URL import and metadata scraper.
"""

import argparse
import asyncio
import json
import random
import re
import sqlite3
import sys
import time
from pathlib import Path

try:
    from playwright.async_api import async_playwright
except ImportError:
    print("Error: playwright library not found. Install with: pip install playwright")
    sys.exit(1)


# Anti-blocking user agents - rotate between realistic browsers
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:121.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
]

# Viewport sizes - realistic resolutions
VIEWPORTS = [
    {"width": 1920, "height": 1080},
    {"width": 1366, "height": 768},
    {"width": 1536, "height": 864},
    {"width": 1440, "height": 900},
    {"width": 1280, "height": 720},
    {"width": 2560, "height": 1440}
]

def get_random_delay():
    """Human-like random delays."""
    return random.uniform(1.5, 4.2)

def get_random_user_agent():
    """Get random user agent."""
    return random.choice(USER_AGENTS)

def get_random_viewport():
    """Get random viewport size."""
    return random.choice(VIEWPORTS)

async def setup_stealth_browser(page):
    """Setup browser with anti-blocking measures."""
    # Random user agent
    await page.set_user_agent(get_random_user_agent())

    # Random viewport
    viewport = get_random_viewport()
    await page.set_viewport_size(viewport["width"], viewport["height"])

    # Random timezone
    timezones = ["America/New_York", "America/Chicago", "America/Denver", "America/Los_Angeles", "UTC"]
    await page.emulate_timezone(random.choice(timezones))

    # Random language and headers
    languages = ["en-US", "en-GB", "en-CA"]
    await page.set_extra_http_headers({
        "Accept-Language": random.choice(languages),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Encoding": "gzip, deflate, br",
        "Cache-Control": "max-age=0",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Upgrade-Insecure-Requests": "1"
    })

    # Hide webdriver traces
    await page.add_init_script("""
        Object.defineProperty(navigator, 'webdriver', {
            get: () => undefined,
        });

        // Remove Playwright detection
        delete window.playwright;
        delete window.__playwright;
    """)

def load_user_config():
    """Load log location from user.json."""
    try:
        with open("../user.json", 'r') as f:
            config = json.load(f)
        return config.get('default', {}).get('loglocation', '/tmp')
    except Exception as e:
        print(f"Error reading user.json: {e}")
        sys.exit(1)


def extract_urls_from_log(log_path, verbose=False):
    """Extract Amazon URLs from StreamFab.log."""
    log_file = Path(log_path) / "StreamFab.log"
    
    if not log_file.exists():
        print(f"Error: StreamFab.log not found at {log_file}")
        sys.exit(1)
    
    urls = []
    url_pattern = r'https://www\.amazon\.com/gp/video/detail/([A-Z0-9]+)/'
    
    try:
        with open(log_file, 'r', encoding='utf-8', errors='ignore') as f:
            for line in f:
                matches = re.findall(url_pattern, line)
                for match in matches:
                    full_url = f"https://www.amazon.com/gp/video/detail/{match}/"
                    if full_url not in urls:
                        urls.append(full_url)
    except Exception as e:
        print(f"Error reading log file: {e}")
        sys.exit(1)
    
    urls.reverse()  # Most recent first
    
    if verbose:
        print(f"Found {len(urls)} Amazon URLs in log")
    
    return urls


def get_import_data(verbose=False):
    """Get Amazon content from import table."""
    try:
        conn = sqlite3.connect("../tapedeck.db")
        cursor = conn.cursor()
        
        cursor.execute("PRAGMA table_info(import)")
        columns = [row[1] for row in cursor.fetchall()]
        
        if 'series' in columns:
            cursor.execute("""
                SELECT checksum, series, season, episode, title, filesource
                FROM import 
                WHERE filesource = 'Amazon'
                ORDER BY series, season, episode
            """)
            is_tv = True
        else:
            cursor.execute("""
                SELECT checksum, movie, filesource
                FROM import
                WHERE filesource = 'Amazon'
            """)
            is_tv = False
        
        data = cursor.fetchall()
        conn.close()
        
        if verbose:
            content_type = "TV episodes" if is_tv else "movies"
            print(f"Found {len(data)} Amazon {content_type} in database")
        
        return data, is_tv
        
    except Exception as e:
        print(f"Database error: {e}")
        sys.exit(1)


def calculate_url_limit(import_data, is_tv):
    """Calculate how many URLs to check based on content."""
    if not is_tv:
        return len(import_data) * 2  # 2x movies
    
    # Count unique seasons
    seasons = set()
    for row in import_data:
        series, season = row[1], row[2]
        seasons.add((series, season))
    
    return len(seasons) * 2  # 2x seasons


async def scrape_basic_info(url, verbose=False):
    """Quick scrape for series/season matching."""
    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()

            # Apply anti-blocking measures
            await setup_stealth_browser(page)

            await page.goto(url, timeout=30000)
            await asyncio.sleep(get_random_delay())
            
            html = await page.content()
            await browser.close()
            
            metadata = {}
            
            # Series/movie title
            title_match = re.search(r'data-automation-id="title"[^>]*>([^<]+)<', html)
            if title_match:
                title = title_match.group(1).strip()
                metadata['series'] = title
                metadata['movie'] = title
            
            # Season number
            season_match = re.search(r'>Season (\d+)<', html)
            if season_match:
                metadata['season'] = int(season_match.group(1))
            
            return metadata
            
    except Exception:
        return None


async def scrape_full_metadata(url, verbose=False):
    """Full scrape with exact training specifications."""
    if verbose:
        print(f"Scraping: {url}")

    # Rate limiting - random delay between requests
    await asyncio.sleep(get_random_delay())

    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()

            # Apply anti-blocking measures
            await setup_stealth_browser(page)

            await page.goto(url, timeout=60000)

            # Scroll to bottom to trigger lazy loading - EXACT training spec
            prev_height = 0
            while True:
                await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                await asyncio.sleep(get_random_delay())  # simulate human scroll pause
                curr_height = await page.evaluate("document.body.scrollHeight")
                if curr_height == prev_height:
                    break
                prev_height = curr_height
            
            # Wait a bit more for any final AJAX
            await asyncio.sleep(get_random_delay())
            
            # Dump full rendered HTML
            html = await page.content()
            await browser.close()
            
            # KISS sanity check
            if len(html) < 100_000:
                raise Exception("HTML too short — probably missing content")
            
            return parse_amazon_html(html, verbose)
            
    except Exception as e:
        if verbose:
            print(f"Error scraping {url}: {e}")
        return None


def parse_amazon_html(html, verbose=False):
    """Parse using EXACT training selectors - KISS approach."""
    metadata = {}
    
    # Series title - EXACT training selector
    title_match = re.search(r'<h1 class="p-jAFk Qo\+b2C" data-automation-id="title"[^>]*>([^<]+)</h1>', html)
    if title_match:
        metadata['series'] = title_match.group(1).strip()
        metadata['movie'] = title_match.group(1).strip()
    
    # Season - EXACT training selector
    season_match = re.search(r'<div class="dv-node-dp-seasons"><span class="enCoYt"><span class="_36qUej">Season (\d+)</span></span></div>', html)
    if season_match:
        metadata['season'] = int(season_match.group(1))
    
    # Season description - EXACT training selector
    desc_match = re.search(r'<span class="_1H6ABQ" style="--expanded-max-height:unset">([^<]+)</span>', html)
    if desc_match:
        metadata['dseason'] = desc_match.group(1).strip()
    
    # Year - EXACT training selector
    year_match = re.search(r'<span role="img" aria-label="Released (\d{4})" data-automation-id="release-year-badge"[^>]*>(\d{4})</span>', html)
    if year_match:
        metadata['year'] = int(year_match.group(1))
    
    # Network - EXACT training selector
    network_match = re.search(r'<dl class="-Zstym" data-testid="metadata-row"><dt class="_5HWLFr"><h3><span class="_36qUej">Studio</span></h3></dt><dd class="_3k277F">([^<]+)</dd></dl>', html)
    if network_match:
        metadata['network'] = network_match.group(1).strip()
    
    # Genre - EXACT training selector
    genre_match = re.search(r'<div class="I0iH2G" data-testid="genresMetadata"><span data-testid="genre-texts" class="_3F76dX _23dw7w"><a[^>]*class="_1NNx6V">([^<]+)</a></span></div>', html)
    if genre_match:
        metadata['genre'] = genre_match.group(1).strip()
    
    # Rating - EXACT training selector (TV-G, TV-14, TV-MA, etc.)
    rating_match = re.search(r'<span[^>]*data-testid="rating-badge"[^>]*>(TV-[A-Z0-9]+|[A-Z]|PG-?\d*)</span>', html)
    if rating_match:
        metadata['rating'] = rating_match.group(1).strip()
    
    # Cast - EXACT training selector
    cast_match = re.search(r'<dl class="-Zstym" data-testid="metadata-row"><dt class="_5HWLFr"><h3><span class="_36qUej">Cast</span></h3></dt><dd class="_3k277F">(.+?)</dd></dl>', html, re.DOTALL)
    if cast_match:
        cast_html = cast_match.group(1)
        cast_names = re.findall(r'<a[^>]*class="_1NNx6V">([^<]+)</a>', cast_html)
        if cast_names:
            metadata['cast'] = ', '.join(cast_names[:7])
    
    # Series image - EXACT training selector
    img_match = re.search(r'<img[^>]*class="Ah1hNY"[^>]*src="([^"]+)"[^>]*data-testid="base-image"[^>]*loading="eager"', html)
    if img_match:
        metadata['iseries'] = img_match.group(1)
        metadata['imovie'] = img_match.group(1)
    
    # Parse episodes using EXACT training patterns
    episodes = []
    episode_blocks = re.findall(r'<div data-automation-id="ep-title-episode-\d+" class="dCocJw">.*?(?=<div data-automation-id="ep-title-episode-\d+"|$)', html, re.DOTALL)
    
    for block in episode_blocks:
        episode = {}
        
        # Episode number and title - EXACT training pattern
        ep_match = re.search(r'<span class="_36qUej izvPPq"><span>S\d+ E(\d+)</span><span class="Z7ThIH"> - </span><span class="P1uAb6">([^<]+)</span></span>', block)
        if ep_match:
            episode['episode'] = int(ep_match.group(1))
            episode['title'] = ep_match.group(2).strip()
        
        # Episode description - EXACT training pattern
        desc_match = re.search(r'<div class="_1\+KXv2 ci7S35"><div class="p-jAFk _1zr6Jb" data-automation-id="synopsis-[^"]+">.*?<div dir="auto">([^<]+)</div>', block, re.DOTALL)
        if desc_match:
            episode['depisode'] = desc_match.group(1).strip()
        
        # Air date - EXACT training pattern
        air_match = re.search(r'<div class="_1wFEYz ci7S35" data-testid="episode-metadata"><div class="riRKnh"><div data-testid="episode-release-date">([^<]+)</div>', block)
        if air_match:
            episode['airdate'] = air_match.group(1).strip()
        
        # Episode image - EXACT training pattern
        img_match = re.search(r'<img alt="" class="FHb5CR Ah1hNY"[^>]*src="([^"]+)"[^>]*data-testid="base-image"', block)
        if img_match:
            episode['iepisode'] = img_match.group(1)
        
        if episode and 'episode' in episode:
            episodes.append(episode)
    
    metadata['episodes'] = episodes
    
    if verbose and episodes:
        print(f"  Parsed {len(episodes)} episodes")
    
    return metadata


async def match_urls_to_content(urls, import_data, is_tv, verbose=False):
    """Match URLs to content with retry logic."""
    url_limit = calculate_url_limit(import_data, is_tv)
    max_urls = min(len(urls), url_limit)
    
    matches = []
    
    if verbose:
        print(f"Checking {max_urls} URLs (limit: {url_limit})")
    
    if is_tv:
        # Group by series/season
        content_groups = {}
        for row in import_data:
            checksum, series, season, episode, title, filesource = row
            key = (series.lower(), season)
            if key not in content_groups:
                content_groups[key] = []
            content_groups[key].append((checksum, episode, title))
        
        # Try to match URLs
        for i, url in enumerate(urls[:max_urls]):
            if verbose:
                print(f"Checking: {url}")

            # Rate limiting - add delay between URL checks
            if i > 0:
                await asyncio.sleep(get_random_delay())

            metadata = await scrape_basic_info(url, verbose)
            if not metadata:
                continue
                
            scraped_series = metadata.get('series', '').lower()
            scraped_season = metadata.get('season')
            
            # Find matching content group
            for (db_series, db_season), episodes in list(content_groups.items()):
                if scraped_series and db_series:
                    if scraped_series in db_series or db_series in scraped_series:
                        if scraped_season == db_season:
                            if verbose:
                                print(f"  ✅ Matched: {db_series} Season {db_season}")
                            
                            for checksum, episode, title in episodes:
                                matches.append({
                                    'url': url,
                                    'checksum': checksum,
                                    'series': db_series,
                                    'season': db_season,
                                    'episode': episode,
                                    'title': title
                                })
                            
                            del content_groups[(db_series, db_season)]
                            break
        
        # If no matches and we have unused URLs, double the limit and try once more
        if not matches and len(urls) > max_urls:
            if verbose:
                print("No matches found, doubling URL limit for final attempt")
            
            double_limit = min(len(urls), url_limit * 2)
            for i, url in enumerate(urls[max_urls:double_limit]):
                if verbose:
                    print(f"Retry: {url}")

                # Rate limiting for retry attempts
                if i > 0:
                    await asyncio.sleep(get_random_delay())

                metadata = await scrape_basic_info(url, verbose)
                if not metadata:
                    continue
                    
                scraped_series = metadata.get('series', '').lower()
                scraped_season = metadata.get('season')
                
                for (db_series, db_season), episodes in list(content_groups.items()):
                    if scraped_series and db_series:
                        if scraped_series in db_series or db_series in scraped_series:
                            if scraped_season == db_season:
                                if verbose:
                                    print(f"  ✅ Retry matched: {db_series} Season {db_season}")
                                
                                for checksum, episode, title in episodes:
                                    matches.append({
                                        'url': url,
                                        'checksum': checksum,
                                        'series': db_series,
                                        'season': db_season,
                                        'episode': episode,
                                        'title': title
                                    })
                                
                                del content_groups[(db_series, db_season)]
                                break
    
    else:
        # Movie matching
        for i, url in enumerate(urls[:max_urls]):
            # Rate limiting between movie checks
            if i > 0:
                await asyncio.sleep(get_random_delay())

            metadata = await scrape_basic_info(url, verbose)
            if not metadata:
                continue
                
            scraped_title = metadata.get('movie', '').lower()
            
            for checksum, movie, filesource in import_data:
                if scraped_title and movie:
                    if scraped_title in movie.lower() or movie.lower() in scraped_title:
                        matches.append({
                            'url': url,
                            'checksum': checksum,
                            'movie': movie
                        })
                        break
    
    if verbose:
        print(f"✅ Created {len(matches)} matches")
    
    return matches


async def update_database(matches, verbose=False):
    """Update online table and write URLs to import table."""
    try:
        conn = sqlite3.connect("../tapedeck.db")
        cursor = conn.cursor()
        
        # Group by URL for efficient scraping
        url_groups = {}
        for match in matches:
            url = match['url']
            if url not in url_groups:
                url_groups[url] = []
            url_groups[url].append(match)
        
        # Process each URL once
        for url, url_matches in url_groups.items():
            if verbose:
                print(f"Processing: {url} ({len(url_matches)} items)")
            
            metadata = await scrape_full_metadata(url, verbose)
            if not metadata:
                if verbose:
                    print(f"  Skipping - scrape failed")
                continue
            
            # Build episode lookup
            scraped_episodes = {ep['episode']: ep for ep in metadata.get('episodes', []) if 'episode' in ep}
            
            # Update each item
            for match in url_matches:
                if 'series' in match:
                    # TV show
                    episode_num = match['episode']
                    episode_data = scraped_episodes.get(episode_num)
                    
                    cursor.execute("""
                        UPDATE online SET
                            series = ?, season = ?, episode = ?, title = ?,
                            dseason = ?, depisode = ?, year = ?, airdate = ?,
                            network = ?, genre = ?, rating = ?, cast = ?,
                            iseries = ?, iepisode = ?
                        WHERE checksum = ?
                    """, (
                        match['series'],  # Use import data
                        match['season'],  # Use import data
                        match['episode'], # Use import data
                        episode_data.get('title') if episode_data else match.get('title'),
                        metadata.get('dseason'),
                        episode_data.get('depisode') if episode_data else None,
                        metadata.get('year'),
                        episode_data.get('airdate') if episode_data else None,
                        metadata.get('network'),
                        metadata.get('genre'),
                        metadata.get('rating'),
                        metadata.get('cast'),
                        metadata.get('iseries'),
                        episode_data.get('iepisode') if episode_data else None,
                        match['checksum']
                    ))
                else:
                    # Movie
                    cursor.execute("""
                        UPDATE online SET
                            movie = ?, year = ?, genre = ?, rating = ?, cast = ?, imovie = ?
                        WHERE checksum = ?
                    """, (
                        match['movie'],  # Use import data
                        metadata.get('year'),
                        metadata.get('genre'),
                        metadata.get('rating'),
                        metadata.get('cast'),
                        metadata.get('imovie'),
                        match['checksum']
                    ))
                
                # Write URL to import table
                cursor.execute("UPDATE import SET url = ? WHERE checksum = ?", (url, match['checksum']))
        
        conn.commit()
        conn.close()
        return len(matches)
        
    except Exception as e:
        print(f"Database error: {e}")
        return 0


async def main():
    """Main function."""
    parser = argparse.ArgumentParser(description="Import online metadata")
    parser.add_argument("-v", "--verbose", action="store_true", help="Verbose mode")
    args = parser.parse_args()
    
    # Load configuration
    log_location = load_user_config()
    if args.verbose:
        print(f"Log location: {log_location}")
    
    # Extract URLs
    urls = extract_urls_from_log(log_location, args.verbose)
    if not urls:
        print("No Amazon URLs found in log")
        return
    
    # Get import data
    import_data, is_tv = get_import_data(args.verbose)
    if not import_data:
        print("No Amazon content found in database")
        return
    
    # Match URLs to content
    matches = await match_urls_to_content(urls, import_data, is_tv, args.verbose)
    if not matches:
        print("No matches found between URLs and database content")
        return
    
    # Update database
    updated_count = await update_database(matches, args.verbose)
    
    print(f"Successfully processed {updated_count} items")


if __name__ == "__main__":
    asyncio.run(main())