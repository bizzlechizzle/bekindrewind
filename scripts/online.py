#!/usr/bin/env python3

import argparse
import asyncio
import json
import re
import sqlite3
from pathlib import Path

def check_playwright():
    try:
        from playwright.async_api import async_playwright
        return async_playwright
    except ImportError:
        print("Error: playwright library not found. Install with: pip install playwright")
        exit(1)

def get_config():
    config_path = Path(__file__).parent.parent / "user.json"
    with open(config_path, 'r') as f:
        config = json.load(f)
    return config.get('default', {}).get('loglocation', '/tmp')

def get_urls(log_path):
    log_file = Path(log_path) / "StreamFab.log"
    if not log_file.exists():
        print(f"StreamFab.log not found: {log_file}")
        return []

    urls = []
    with open(log_file, 'r', encoding='utf-8', errors='ignore') as f:
        for line in f:
            matches = re.findall(r'https://www\.amazon\.com/gp/video/detail/([A-Z0-9]+)/', line)
            for match in matches:
                url = f"https://www.amazon.com/gp/video/detail/{match}/"
                if url not in urls:
                    urls.append(url)
    return urls[::-1]

def get_content():
    db_path = Path(__file__).parent.parent / "tapedeck.db"
    conn = sqlite3.connect(str(db_path))
    cursor = conn.cursor()

    cursor.execute("PRAGMA table_info(import)")
    cols = {row[1] for row in cursor.fetchall()}

    if 'series' in cols:
        cursor.execute("SELECT checksum, series, season, episode FROM import WHERE dlsource = 'Amazon' ORDER BY series, season, episode")
        data = cursor.fetchall()
        conn.close()
        return data, True
    elif 'movie' in cols:
        cursor.execute("SELECT checksum, movie FROM import WHERE dlsource = 'Amazon'")
        data = cursor.fetchall()
        conn.close()
        return data, False
    else:
        cursor.execute("SELECT checksum FROM import LIMIT 0")
        data = cursor.fetchall()
        conn.close()
        return data, False

async def scrape_page(url):
    async_playwright = check_playwright()
    browsers = ['chromium', 'firefox', 'webkit']

    for browser_name in browsers:
        try:
            async with async_playwright() as p:
                browser = getattr(p, browser_name)
                browser_instance = await browser.launch()
                page = await browser_instance.new_page()

                await page.goto(url, timeout=60000)

                # Exact scrolling logic from instructions (lines 273-281)
                prev_height = 0
                while True:
                    await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                    await asyncio.sleep(2)  # simulate human scroll pause
                    curr_height = await page.evaluate("document.body.scrollHeight")
                    if curr_height == prev_height:
                        break
                    prev_height = curr_height

                # Wait for final AJAX
                await asyncio.sleep(2)

                # Click more buttons per instructions
                try:
                    more_buttons = await page.query_selector_all('button:has-text("more"), button:has-text("More"), [data-testid*="more"]')
                    for btn in more_buttons:
                        await btn.click()
                        await asyncio.sleep(1)
                except:
                    pass

                # Dump full HTML
                html = await page.content()
                await browser_instance.close()

                # KISS sanity check - MINIMUM 100k per instructions (line 290-292)
                if len(html) < 100000:
                    continue  # Try next browser

                return parse_html(html)

        except Exception:
            continue

    return None

def parse_html(html):
    data = {}

    # Title - exact selector from instructions (line 209)
    if m := re.search(r'data-automation-id="title"[^>]*>([^<]+)<', html):
        data['title'] = m.group(1).strip()

    # Season - exact pattern from instructions (line 212)
    if m := re.search(r'<span class="_36qUej">Season (\d+)</span>', html):
        data['season'] = int(m.group(1))

    # Season description - exact selector from instructions (line 219)
    if m := re.search(r'<span class="_1H6ABQ"[^>]*>([^<]+)</span>', html):
        data['dseason'] = m.group(1).strip()

    # Year - exact pattern from instructions (line 223)
    if m := re.search(r'data-automation-id="release-year-badge"[^>]*>(\d{4})</span>', html):
        data['year'] = int(m.group(1))

    # Studio/Network - exact pattern from instructions (line 247)
    if m := re.search(r'<dt[^>]*><h3><span[^>]*>Studio</span></h3></dt><dd[^>]*>([^<]+)</dd>', html):
        data['network'] = m.group(1).strip()

    # Genre - exact pattern from instructions (line 251)
    if m := re.search(r'data-testid="genresMetadata"[^>]*>.*?<a[^>]*>([^<]+)</a>', html, re.DOTALL):
        data['genre'] = m.group(1).strip()

    # Rating - exact pattern from instructions (line 255)
    if m := re.search(r'data-testid="rating-badge"[^>]*>([^<]+)</span>', html):
        data['rating'] = m.group(1).strip()

    # Cast - exact pattern from instructions (line 259)
    if m := re.search(r'<dt[^>]*><h3><span[^>]*>Cast</span></h3></dt><dd[^>]*>(.+?)</dd>', html, re.DOTALL):
        cast_links = re.findall(r'<a[^>]*>([^<]+)</a>', m.group(1))
        if cast_links:
            data['cast'] = ', '.join(cast_links[:5])

    # Series image - exact pattern from instructions (line 267)
    if m := re.search(r'<img[^>]*src="([^"]+)"[^>]*data-testid="base-image"', html):
        data['iseries'] = m.group(1)
        data['imovie'] = m.group(1)

    # Episodes - exact pattern from instructions (line 216, 227, 235, 239)
    episodes = []
    ep_pattern = r'data-automation-id="ep-title-episode-\d+".*?(?=data-automation-id="ep-title-episode-\d+"|$)'
    ep_blocks = re.findall(ep_pattern, html, re.DOTALL)

    for block in ep_blocks:
        ep = {}

        # Episode number and title from instructions (line 227, 231)
        ep_title_pattern = r'<span[^>]*>S\d+ E(\d+)</span><span[^>]*> - </span><span[^>]*>([^<]+)</span>'
        if m := re.search(ep_title_pattern, block):
            ep['episode'] = int(m.group(1))
            ep['title'] = m.group(2).strip()

        # Episode description from instructions (line 235)
        synopsis_pattern = r'data-automation-id="synopsis-[^"]*".*?<div dir="auto">([^<]+)</div>'
        if m := re.search(synopsis_pattern, block, re.DOTALL):
            ep['depisode'] = m.group(1).strip()

        # Air date from instructions (line 239)
        if m := re.search(r'data-testid="episode-release-date">([^<]+)<', block):
            ep['airdate'] = m.group(1).strip()

        # Rating from instructions (line 255) - same metadata block as airdate
        if m := re.search(r'data-testid="rating-badge"[^>]*>([^<]+)</span>', block):
            ep['rating'] = m.group(1).strip()

        # Episode image from instructions (line 263)
        if m := re.search(r'<img[^>]*src="([^"]+)"[^>]*data-testid="base-image"', block):
            ep['iepisode'] = m.group(1)

        if 'episode' in ep:
            episodes.append(ep)

    data['episodes'] = episodes
    return data

def validate_episodes(content_data, scraped_data):
    if not scraped_data.get('episodes'):
        return True

    db_episodes = {}
    for checksum, series, season, episode in content_data:
        key = (series, season)
        if key not in db_episodes:
            db_episodes[key] = set()
        db_episodes[key].add(episode)

    scraped_episodes = {ep['episode'] for ep in scraped_data['episodes']}

    for (series, season), db_eps in db_episodes.items():
        if db_eps != scraped_episodes:
            missing = db_eps - scraped_episodes
            extra = scraped_episodes - db_eps

            with open('missingepisodes.txt', 'w') as f:
                f.write(f"Series: {series}\n")
                f.write(f"Season: {season}\n")
                f.write(f"Local episodes: {sorted(db_eps)}\n")
                f.write(f"Missing episodes: {sorted(missing) if missing else 'None'}\n")
                f.write(f"Extra episodes: {sorted(extra) if extra else 'None'}\n")
                f.write(f"URL: {scraped_data.get('url', 'Unknown')}\n")

            print(f"Episode mismatch for {series} Season {season}")
            print(f"Missing episodes: {sorted(missing) if missing else 'None'}")
            return False

    return True

def update_database(matches, scraped_data):
    db_path = Path(__file__).parent.parent / "tapedeck.db"
    conn = sqlite3.connect(str(db_path))
    cursor = conn.cursor()

    cursor.execute("PRAGMA table_info(online)")
    cols = {row[1] for row in cursor.fetchall()}

    for match in matches:
        if 'series' in match:
            ep_data = next((ep for ep in scraped_data.get('episodes', [])
                           if ep.get('episode') == match['episode']), {})

            updates = []
            values = []

            if 'dseason' in cols and scraped_data.get('dseason'):
                updates.append("dseason = ?")
                values.append(scraped_data['dseason'])
            if 'year' in cols and scraped_data.get('year'):
                updates.append("year = ?")
                values.append(scraped_data['year'])
            if 'depisode' in cols and ep_data.get('depisode'):
                updates.append("depisode = ?")
                values.append(ep_data['depisode'])
            if 'airdate' in cols and ep_data.get('airdate'):
                updates.append("airdate = ?")
                values.append(ep_data['airdate'])
            if 'network' in cols and scraped_data.get('network'):
                updates.append("network = ?")
                values.append(scraped_data['network'])
            if 'genre' in cols and scraped_data.get('genre'):
                updates.append("genre = ?")
                values.append(scraped_data['genre'])
            if 'rating' in cols and (scraped_data.get('rating') or ep_data.get('rating')):
                rating = scraped_data.get('rating') or ep_data.get('rating')
                updates.append("rating = ?")
                values.append(rating)
            if 'cast' in cols and scraped_data.get('cast'):
                updates.append("cast = ?")
                values.append(scraped_data['cast'])
            if 'iseries' in cols and scraped_data.get('iseries'):
                updates.append("iseries = ?")
                values.append(scraped_data['iseries'])
            if 'iepisode' in cols and ep_data.get('iepisode'):
                updates.append("iepisode = ?")
                values.append(ep_data['iepisode'])

            if updates:
                values.append(match['checksum'])
                cursor.execute(f"UPDATE online SET {', '.join(updates)} WHERE checksum = ?", values)
        else:
            updates = []
            values = []

            if 'year' in cols and scraped_data.get('year'):
                updates.append("year = ?")
                values.append(scraped_data['year'])
            if 'genre' in cols and scraped_data.get('genre'):
                updates.append("genre = ?")
                values.append(scraped_data['genre'])
            if 'rating' in cols and scraped_data.get('rating'):
                updates.append("rating = ?")
                values.append(scraped_data['rating'])
            if 'cast' in cols and scraped_data.get('cast'):
                updates.append("cast = ?")
                values.append(scraped_data['cast'])

            if updates:
                values.append(match['checksum'])
                cursor.execute(f"UPDATE online SET {', '.join(updates)} WHERE checksum = ?", values)

        cursor.execute("UPDATE import SET url = ? WHERE checksum = ?", (scraped_data['url'], match['checksum']))

    conn.commit()
    conn.close()

async def main():
    parser = argparse.ArgumentParser(description="Online metadata import")
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()

    log_location = get_config()
    urls = get_urls(log_location)
    content_data, is_tv = get_content()

    if not urls:
        print("No URLs found in log")
        return
    if not content_data:
        print("No Amazon content in database")
        return

    if args.verbose:
        print(f"Found {len(urls)} URLs, {len(content_data)} content items")

    # Calculate URL limit per instructions
    if is_tv:
        limit = len(set((row[1], row[2]) for row in content_data)) * 2
    else:
        limit = len(content_data) * 2

    urls_to_try = urls[:limit] + (urls[limit:limit*2] if limit < len(urls) else [])

    for url in urls_to_try:
        if args.verbose:
            print(f"Trying: {url}")

        scraped = await scrape_page(url)
        if not scraped:
            continue

        scraped['url'] = url
        matches = []

        if is_tv:
            scraped_title = scraped.get('title', '').lower()
            scraped_season = scraped.get('season')

            for checksum, series, season, episode in content_data:
                if scraped_title and series and scraped_season == season:
                    if scraped_title in series.lower() or series.lower() in scraped_title:
                        matches.append({'checksum': checksum, 'series': series, 'season': season, 'episode': episode})

            if matches:
                if not validate_episodes(content_data, scraped):
                    print("Process stopped due to missing episodes")
                    return

                update_database(matches, scraped)
                print(f"Updated {len(matches)} items from {url}")
                return
        else:
            scraped_title = scraped.get('title', '').lower()
            for checksum, movie in content_data:
                if scraped_title and movie:
                    if scraped_title in movie.lower() or movie.lower() in scraped_title:
                        matches.append({'checksum': checksum, 'movie': movie})

            if matches:
                update_database(matches, scraped)
                print(f"Updated {len(matches)} items from {url}")
                return

    print("No matches found")

if __name__ == "__main__":
    asyncio.run(main())