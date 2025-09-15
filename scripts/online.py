#!/usr/bin/env python3

import argparse
import asyncio
import json
import re
import sqlite3
from pathlib import Path
from difflib import SequenceMatcher

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
        # Include episode title for bulletproof matching
        title_col = 'title' if 'title' in cols else 'episode_title' if 'episode_title' in cols else 'name' if 'name' in cols else None
        if title_col:
            cursor.execute(f"SELECT checksum, series, season, episode, {title_col} FROM import WHERE dlsource = 'Amazon' ORDER BY series, season, CAST(episode as INTEGER)")
        else:
            cursor.execute("SELECT checksum, series, season, episode, '' as title FROM import WHERE dlsource = 'Amazon' ORDER BY series, season, CAST(episode as INTEGER)")
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

def similarity_score(a, b):
    if not a or not b:
        return 0.0
    return SequenceMatcher(None, a.lower().strip(), b.lower().strip()).ratio()

def normalize_title(title):
    if not title:
        return ""
    return title.lower().replace(' ', '').replace('-', '').replace('_', '')

def titles_match(title1, title2, threshold=0.8):
    if not title1 or not title2:
        return False

    # Direct similarity with stricter threshold to prevent false hits
    similarity = similarity_score(title1, title2)
    if similarity >= threshold:
        return True

    # Normalized similarity
    norm_similarity = similarity_score(normalize_title(title1), normalize_title(title2))
    if norm_similarity >= threshold:
        return True

    # Contains check ONLY for very close matches to prevent false hits
    t1_lower = title1.lower().strip()
    t2_lower = title2.lower().strip()

    # Stricter contains logic - both strings must be substantial and one must be subset
    if len(t1_lower) > 4 and len(t2_lower) > 4:
        if t1_lower in t2_lower or t2_lower in t1_lower:
            # Additional validation - ensure substantial overlap
            overlap_ratio = len(t1_lower) / len(t2_lower) if len(t2_lower) > len(t1_lower) else len(t2_lower) / len(t1_lower)
            return overlap_ratio >= 0.7

    return False

def extract_episode_number(text):
    if not text:
        return None
    patterns = [
        r'S\d+\s*E(\d+)',  # S1 E5, S01E05
        r'Episode\s*(\d+)',  # Episode 5
        r'Ep\s*(\d+)',  # Ep 5
        r'^(\d+)\.',  # 5. Title
        r'E(\d+)\s*-',  # E5 - Title
        r'^\s*(\d+)\s*$',  # Just a number
    ]

    for pattern in patterns:
        match = re.search(pattern, str(text), re.IGNORECASE)
        if match:
            return int(match.group(1))

    # Handle "E1" format from database
    if str(text).startswith('E'):
        num_match = re.search(r'E(\d+)', str(text))
        if num_match:
            return int(num_match.group(1))

    return None

def clean_episode_title(title):
    if not title:
        return ''

    # Remove common episode prefixes
    patterns = [
        r'^S\d+\s*E\d+\s*[-–—]\s*',  # S1 E5 - Title
        r'^Episode\s*\d+\s*[-–—]\s*',  # Episode 5 - Title
        r'^Ep\s*\d+\s*[-–—]\s*',  # Ep 5 - Title
        r'^\d+\.\s*',  # 5. Title
        r'^E\d+\s*[-–—]\s*',  # E5 - Title
    ]

    cleaned = title.strip()
    for pattern in patterns:
        cleaned = re.sub(pattern, '', cleaned, flags=re.IGNORECASE).strip()

    return cleaned

async def scrape_page(url, database_items=None, verbose=False):
    async_playwright = check_playwright()
    browsers = ['chromium', 'firefox', 'webkit']
    user_agents = [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
        'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36'
    ]

    for browser_name in browsers:
        for user_agent in user_agents:
            try:
                if verbose:
                    print(f"    Trying {browser_name} with {user_agent[:50]}...")
                async with async_playwright() as p:
                    browser = getattr(p, browser_name)
                    browser_instance = await browser.launch()
                    context = await browser_instance.new_context(user_agent=user_agent)
                    page = await context.new_page()

                    await page.goto(url, timeout=60000)

                    prev_height = 0
                    while True:
                        await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                        await asyncio.sleep(2)
                        curr_height = await page.evaluate("document.body.scrollHeight")
                        if curr_height == prev_height:
                            break
                        prev_height = curr_height

                    await asyncio.sleep(2)

                    try:
                        more_buttons = await page.query_selector_all('button:has-text("more"), button:has-text("More"), [data-testid*="more"]')
                        for btn in more_buttons:
                            await btn.click()
                            await asyncio.sleep(1)
                    except:
                        pass

                    html = await page.content()
                    await browser_instance.close()

                    if verbose:
                        print(f"    HTML length: {len(html)}")

                    if len(html) < 100000:
                        if verbose:
                            print(f"    HTML too short, trying next browser/agent")
                        continue

                    result = parse_html(html, database_items, verbose)
                    if verbose:
                        print(f"    Parse result: {result is not None}")
                        if result:
                            print(f"    Title: {result.get('title')}")
                            print(f"    Season: {result.get('season')}")
                    return result

            except Exception as e:
                if verbose:
                    print(f"    Error with {browser_name}: {e}")
                continue

    return None

def extract_pattern(html, patterns):
    for pattern in patterns:
        if m := re.search(pattern, html, re.IGNORECASE | re.DOTALL):
            return m.group(1).strip()
    return None

class BulletproofEpisodeParser:
    def __init__(self, html_content, database_items, verbose=False):
        self.html = html_content
        self.database_items = database_items  # (checksum, series, season, episode, title)
        self.verbose = verbose
        self.expected_episodes = self._build_expected_episodes()

    def _build_expected_episodes(self):
        episodes = []
        for item in self.database_items:
            if len(item) >= 5:
                checksum, series, season, episode, title = item[:5]
            else:
                checksum, series, season, episode = item[:4]
                title = ''

            ep_number = extract_episode_number(episode)
            if ep_number is None:
                ep_number = 1

            episodes.append({
                'checksum': checksum,
                'series': series,
                'season': season,
                'episode_id': episode,
                'episode_number': ep_number,
                'database_title': title or '',
                'title': '',
                'description': '',
                'air_date': '',
                'rating': '',
                'image': ''
            })

        episodes.sort(key=lambda x: x['episode_number'])
        return episodes

    def _extract_all_episodes(self):
        scraped_episodes = []

        # Strategy 1: Episode containers with automation IDs
        ep_pattern = r'data-automation-id="ep-title-episode-\d+".*?(?=data-automation-id="ep-title-episode-\d+"|$)'
        ep_blocks = re.findall(ep_pattern, self.html, re.DOTALL)

        for block in ep_blocks:
            ep = self._parse_episode_block(block)
            if ep:
                scraped_episodes.append(ep)

        # Strategy 2: Direct episode title extraction
        title_patterns = [
            r'<span[^>]*>S\d+ E(\d+)</span><span[^>]*> - </span><span[^>]*>([^<]+)</span>',
            r'<h3[^>]*class="[^"]*izvPPq[^"]*"[^>]*>.*?<span[^>]*>S\d+ E(\d+)</span>.*?<span[^>]*>([^<]+)</span>',
            r'Episode (\d+)[^<]*<[^>]*>([^<]+)'
        ]

        for pattern in title_patterns:
            matches = re.finditer(pattern, self.html, re.IGNORECASE | re.DOTALL)
            for match in matches:
                ep_num = int(match.group(1))
                title = clean_episode_title(match.group(2))
                if title and len(title) > 3:
                    ep = {
                        'episode_number': ep_num,
                        'title': title,
                        'description': '',
                        'air_date': '',
                        'rating': '',
                        'image': ''
                    }
                    # Avoid duplicates
                    if not any(existing['episode_number'] == ep_num for existing in scraped_episodes):
                        scraped_episodes.append(ep)

        # Strategy 3: Text-based extraction
        text_episodes = self._extract_from_text()
        for ep in text_episodes:
            if not any(existing['episode_number'] == ep['episode_number'] for existing in scraped_episodes):
                scraped_episodes.append(ep)

        if self.verbose:
            print(f"  Extracted {len(scraped_episodes)} episodes using multiple strategies")

        return scraped_episodes

    def _parse_episode_block(self, block):
        ep = {'episode_number': None, 'title': '', 'description': '', 'air_date': '', 'rating': '', 'image': ''}

        # Episode number and title
        ep_title_pattern = r'<span[^>]*>S\d+ E(\d+)</span><span[^>]*> - </span><span[^>]*>([^<]+)</span>'
        if m := re.search(ep_title_pattern, block):
            ep['episode_number'] = int(m.group(1))
            ep['title'] = clean_episode_title(m.group(2))

        # Description
        synopsis_pattern = r'data-automation-id="synopsis-[^"]*".*?<div dir="auto">([^<]+)</div>'
        if m := re.search(synopsis_pattern, block, re.DOTALL):
            ep['description'] = m.group(1).strip()

        # Air date
        if m := re.search(r'data-testid="episode-release-date">([^<]+)<', block):
            ep['air_date'] = m.group(1).strip()

        # Rating
        if m := re.search(r'data-testid="rating-badge"[^>]*>([^<]+)</span>', block):
            ep['rating'] = m.group(1).strip()

        # Image
        if m := re.search(r'<img[^>]*src="([^"]+)"[^>]*data-testid="base-image"', block):
            ep['image'] = m.group(1)

        return ep if ep['episode_number'] or ep['title'] else None

    def _extract_from_text(self):
        episodes = []
        pattern = r'S\d+\s*E(\d+)\s*[-–—]\s*([^S\n]+?)(?=S\d+\s*E\d+|$)'
        matches = re.finditer(pattern, self.html, re.IGNORECASE | re.MULTILINE)

        for match in matches:
            ep_number = int(match.group(1))
            title = clean_episode_title(match.group(2).strip())

            if len(title) > 3:
                episodes.append({
                    'episode_number': ep_number,
                    'title': title,
                    'description': '',
                    'air_date': '',
                    'rating': '',
                    'image': ''
                })

        return episodes

    def _match_episodes_to_database(self, scraped_episodes):
        matched_episodes = []
        used_scraped = set()

        for expected in self.expected_episodes:
            best_match = None
            best_score = 0
            best_index = -1

            for i, scraped in enumerate(scraped_episodes):
                if i in used_scraped:
                    continue

                score = 0

                # Exact episode number match
                if scraped.get('episode_number') == expected['episode_number']:
                    score += 50
                # Close episode number
                elif scraped.get('episode_number') and abs(scraped['episode_number'] - expected['episode_number']) <= 1:
                    score += 25

                # Title similarity with database title
                if scraped.get('title') and expected['database_title']:
                    similarity = similarity_score(scraped['title'], expected['database_title'])
                    if similarity > 0.8:
                        score += 30
                    elif similarity > 0.6:
                        score += 15
                    elif similarity > 0.4:
                        score += 5

                # Bonus for having a reasonable title
                if scraped.get('title') and len(scraped['title']) > 3:
                    score += 10

                if score > best_score:
                    best_score = score
                    best_match = scraped
                    best_index = i

            # Use best match if reasonable score
            if best_match and best_score >= 20:
                episode_data = expected.copy()

                # Use fuzzy matching for title
                if best_match.get('title') and expected['database_title']:
                    similarity = similarity_score(best_match['title'], expected['database_title'])
                    if similarity > 0.8:
                        final_title = expected['database_title']  # Use database title
                    else:
                        final_title = best_match['title']  # Use scraped title
                else:
                    final_title = best_match.get('title') or expected['database_title']

                episode_data.update({
                    'title': final_title,
                    'description': best_match.get('description', ''),
                    'air_date': best_match.get('air_date', ''),
                    'rating': best_match.get('rating', ''),
                    'image': best_match.get('image', '')
                })
                used_scraped.add(best_index)
            else:
                # No good match, use database title
                episode_data = expected.copy()
                episode_data['title'] = expected['database_title']
                if self.verbose:
                    print(f"  No match for episode {expected['episode_number']}, using database title")

            matched_episodes.append(episode_data)

        return matched_episodes

    def parse_episodes(self):
        if self.verbose:
            print(f"  Parsing {len(self.expected_episodes)} expected episodes")

        scraped_episodes = self._extract_all_episodes()
        matched_episodes = self._match_episodes_to_database(scraped_episodes)

        if self.verbose:
            successful = len([ep for ep in matched_episodes if ep.get('title')])
            print(f"  Matched {successful}/{len(matched_episodes)} episodes")

        return matched_episodes

def extract_episodes(html, database_items, verbose=False):
    parser = BulletproofEpisodeParser(html, database_items, verbose)
    return parser.parse_episodes()

def parse_html(html, database_items=None, verbose=False):
    data = {}

    data['title'] = extract_pattern(html, [
        r'<h1[^>]*data-automation-id="title"[^>]*>([^<]+)</h1>',
        r'data-automation-id="title"[^>]*>([^<]+)<'
    ])

    season_match = extract_pattern(html, [
        r'<span class="_36qUej">Season (\d+)</span>',
        r'>Season\s+(\d+)<'
    ])
    if season_match:
        data['season'] = int(season_match)

    # Extract season description from specific season description area
    data['dseason'] = extract_pattern(html, [
        r'<span class="_1H6ABQ"[^>]*style="[^"]*expanded-max-height[^"]*">([^<]+)</span>',
        r'<span class="_1H6ABQ"[^>]*>([^<]+)</span>'
    ])

    # Extract SERIES description from meta description or main description areas
    # This should be different from season description
    series_desc = extract_pattern(html, [
        r'<meta name="description" content="([^"]+)"',
        r'<div[^>]*data-testid="[^"]*synopsis[^"]*"[^>]*>.*?<div[^>]*>([^<]+)</div>',
        r'<div[^>]*class="[^"]*synopsis[^"]*"[^>]*>([^<]+)</div>'
    ])

    if data.get('season'):
        # For TV shows, try to get proper series description separate from season description
        if series_desc and len(series_desc) > 50:
            # Only use as series description if it's different from season description
            if not data.get('dseason') or series_desc != data['dseason']:
                data['dseries'] = series_desc
            else:
                # If descriptions are the same, this is likely season description, not series
                # Try alternative patterns for series description
                alt_series_desc = extract_pattern(html, [
                    r'<meta property="og:description" content="([^"]+)"',
                    r'<div[^>]*class="[^"]*show-description[^"]*"[^>]*>([^<]+)</div>',
                    r'<p[^>]*class="[^"]*series-summary[^"]*"[^>]*>([^<]+)</p>'
                ])
                if alt_series_desc and alt_series_desc != data['dseason']:
                    data['dseries'] = alt_series_desc
        # If no distinct series description found, leave dseries empty rather than duplicate dseason
    else:
        # For movies
        if series_desc and len(series_desc) > 50:
            data['dmovie'] = series_desc

    year_match = extract_pattern(html, [
        r'data-automation-id="release-year-badge"[^>]*>(\d{4})</span>',
        r'aria-label="Released (\d{4})"'
    ])
    if year_match:
        data['year'] = int(year_match)
        if not data.get('season'):
            data['release'] = year_match

    studio = extract_pattern(html, [
        r'<dt[^>]*><h3><span[^>]*>Studio</span></h3></dt><dd[^>]*>([^<]+)</dd>'
    ])
    if studio:
        if data.get('season'):
            data['network'] = studio
        else:
            data['studio'] = studio

    # Extract genre with priority for multiple genres, avoid basic "Unscripted" when possible
    genre_patterns = [
        r'data-testid="genresMetadata"[^>]*>.*?<a[^>]*>([^<]+)</a>.*?<a[^>]*>([^<]+)</a>',  # Multiple genres
        r'data-testid="genresMetadata"[^>]*>.*?<a[^>]*>([^<]+)</a>',  # Single genre
        r'<div class="I0iH2G"[^>]*>.*?<a[^>]*>([^<]+)</a>.*?<a[^>]*>([^<]+)</a>',  # Alternative multiple
        r'<div class="I0iH2G"[^>]*>.*?<a[^>]*>([^<]+)</a>'  # Alternative single
    ]

    genre_text = None
    for pattern in genre_patterns:
        match = re.search(pattern, html, re.IGNORECASE | re.DOTALL)
        if match:
            if len(match.groups()) > 1:
                # Multiple genres found
                genres = [g.strip() for g in match.groups() if g and g.strip()]
                genre_text = ', '.join(genres)
            else:
                genre_text = match.group(1).strip()
            break

    # Apply genre quality filter - avoid low-quality generic terms when possible
    if genre_text:
        low_quality_genres = ['unscripted', 'reality tv', 'lifestyle']
        # Only skip if we have a single low-quality genre
        genre_words = [g.strip().lower() for g in genre_text.split(',')]
        if len(genre_words) == 1 and genre_words[0] in low_quality_genres:
            # Try to find better genre alternatives in other parts of the page
            alt_genre = extract_pattern(html, [
                r'<span[^>]*class="[^"]*genre[^"]*"[^>]*>([^<]+)</span>',
                r'<div[^>]*data-testid="[^"]*category[^"]*"[^>]*>([^<]+)</div>',
                r'>Category[^<]*</[^>]*>\s*<[^>]*>([^<]+)<'
            ])
            if alt_genre and alt_genre.lower() not in low_quality_genres:
                data['genre'] = alt_genre
            elif verbose:
                print(f"    Using low-quality genre as fallback: {genre_text}")
                data['genre'] = genre_text  # Use as fallback if no better option
        else:
            data['genre'] = genre_text

    data['rating'] = extract_pattern(html, [
        r'data-testid="rating-badge"[^>]*>([^<]+)</span>',
        r'data-automation-id="rating-badge"[^>]*>([^<]+)</span>'
    ])

    cast_match = extract_pattern(html, [
        r'<dt[^>]*><h3><span[^>]*>Cast</span></h3></dt><dd[^>]*>(.+?)</dd>'
    ])
    if cast_match:
        cast_links = re.findall(r'<a[^>]*>([^<]+)</a>', cast_match)
        if cast_links:
            data['cast'] = ', '.join(cast_links[:5])

    image_url = extract_pattern(html, [
        r'<img[^>]*src="([^"]+)"[^>]*data-testid="base-image"'
    ])
    if image_url:
        if data.get('season'):
            data['iseries'] = image_url
            data['iseason'] = image_url
        else:
            data['imovie'] = image_url

    if database_items:
        data['episodes'] = extract_episodes(html, database_items, verbose)
    else:
        data['episodes'] = []

    # Debug output for description validation
    if verbose and data.get('season'):
        print(f"    Series description: {data.get('dseries', 'None')[:100]}...")
        print(f"    Season description: {data.get('dseason', 'None')[:100]}...")
        if data.get('dseries') == data.get('dseason'):
            print(f"    WARNING: Series and season descriptions are identical!")
            # Clear dseries to avoid duplicate data
            data['dseries'] = None

    return data

def validate_episodes(content_data, scraped_data, is_tv):
    if not is_tv or not scraped_data.get('episodes'):
        return True

    db_episodes = {}
    for row in content_data:
        if len(row) >= 4:
            checksum, series, season, episode = row[:4]
            key = (series, season)
            if key not in db_episodes:
                db_episodes[key] = set()
            # Extract episode number for comparison
            ep_num = extract_episode_number(episode)
            if ep_num:
                db_episodes[key].add(ep_num)

    scraped_episodes = {ep['episode_number'] for ep in scraped_data['episodes'] if ep.get('episode_number')}

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

def update_tv_data(cursor, match, scraped_data, ep_data, cols):
    updates, values = [], []

    series_fields = {'dseries': 'dseries', 'network': 'network', 'genre': 'genre',
                    'rating': 'rating', 'iseries': 'iseries'}
    season_fields = {'dseason': 'dseason', 'cast': 'cast', 'iseason': 'iseason'}
    episode_fields = {'description': 'depisode', 'air_date': 'airdate', 'image': 'iepisode'}

    for field, col in series_fields.items():
        if col in cols and scraped_data.get(field):
            updates.append(f"{col} = ?")
            values.append(scraped_data[field])

    for field, col in season_fields.items():
        if col in cols and scraped_data.get(field):
            updates.append(f"{col} = ?")
            values.append(scraped_data[field])

    for field, col in episode_fields.items():
        if col in cols and ep_data.get(field):
            updates.append(f"{col} = ?")
            values.append(ep_data[field])

    if updates:
        values.append(match['checksum'])
        cursor.execute(f"UPDATE online SET {', '.join(updates)} WHERE checksum = ?", values)

def update_movie_data(cursor, match, scraped_data, cols):
    updates, values = [], []

    movie_fields = {'dmovie': 'dmovie', 'release': 'release', 'studio': 'studio',
                   'genre': 'genre', 'rating': 'rating', 'cast': 'cast', 'imovie': 'imovie'}

    for field, col in movie_fields.items():
        if col in cols and scraped_data.get(field):
            updates.append(f"{col} = ?")
            values.append(scraped_data[field])

    if updates:
        values.append(match['checksum'])
        cursor.execute(f"UPDATE online SET {', '.join(updates)} WHERE checksum = ?", values)

def update_database(matches, scraped_data):
    db_path = Path(__file__).parent.parent / "tapedeck.db"
    conn = sqlite3.connect(str(db_path))
    cursor = conn.cursor()

    cursor.execute("PRAGMA table_info(online)")
    cols = {row[1] for row in cursor.fetchall()}

    for match in matches:
        if 'series' in match:
            # Find episode data by matching episode number or database episode ID
            ep_data = None
            match_episode = match.get('episode', '')
            match_ep_num = extract_episode_number(match_episode)

            for ep in scraped_data.get('episodes', []):
                if ep.get('checksum') == match['checksum']:
                    ep_data = ep
                    break
                elif match_ep_num and ep.get('episode_number') == match_ep_num:
                    ep_data = ep
                    break

            if not ep_data:
                ep_data = {}

            update_tv_data(cursor, match, scraped_data, ep_data, cols)
        else:
            update_movie_data(cursor, match, scraped_data, cols)

        cursor.execute("UPDATE import SET url = ? WHERE checksum = ?",
                      (scraped_data['url'], match['checksum']))

    conn.commit()
    conn.close()

def find_tv_matches(content_data, scraped_title, scraped_season):
    matches = []
    for row in content_data:
        if len(row) >= 4:
            checksum, series, season, episode = row[:4]
            episode_title = row[4] if len(row) >= 5 else ''
            if scraped_season == season and titles_match(scraped_title, series):
                matches.append({
                    'checksum': checksum,
                    'series': series,
                    'season': season,
                    'episode': episode,
                    'episode_title': episode_title
                })
    return matches

def find_movie_matches(content_data, scraped_title):
    matches = []
    for row in content_data:
        if len(row) >= 2:
            checksum, movie = row[:2]
            if titles_match(scraped_title, movie):
                matches.append({'checksum': checksum, 'movie': movie})
    return matches

async def process_url(url, content_data, is_tv, verbose):
    if verbose:
        print(f"Trying: {url}")

    # Get the relevant database items for this season/series
    if is_tv and content_data:
        # Get unique series/season combinations
        season_items = {}
        for row in content_data:
            if len(row) >= 3:
                series, season = row[1], row[2]
                key = (series, season)
                if key not in season_items:
                    season_items[key] = []
                season_items[key].append(row)

        # Try each series/season combination
        for (series, season), items in season_items.items():
            scraped = await scrape_page(url, items, verbose)
            if not scraped:
                continue

            scraped['url'] = url
            scraped_title = scraped.get('title', '')
            scraped_season = scraped.get('season')

            if verbose:
                print(f"  Scraped title: '{scraped_title}', season: {scraped_season}")

            if scraped_title and scraped_season:
                if scraped_season == season and titles_match(scraped_title, series):
                    matches = find_tv_matches(content_data, scraped_title, scraped_season)
                    if matches:
                        if verbose:
                            print(f"  Found {len(matches)} matches for {series} Season {season}")
                        return matches, scraped
    else:
        # Movie handling
        scraped = await scrape_page(url, content_data, verbose)
        if not scraped:
            if verbose:
                print(f"  Failed to scrape {url}")
            return None

        scraped['url'] = url
        scraped_title = scraped.get('title', '')

        if verbose:
            print(f"  Scraped title: '{scraped_title}'")

        if scraped_title:
            matches = find_movie_matches(content_data, scraped_title)
            if matches:
                if verbose:
                    print(f"  Found {len(matches)} matches")
                return matches, scraped

    if verbose:
        print(f"  No matches found for {url}")
    return None

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

    if is_tv:
        needed = set((row[1], row[2]) for row in content_data if len(row) >= 3)
        limit = len(needed) * 2
    else:
        needed = set(row[1] for row in content_data if len(row) >= 2)
        limit = len(needed) * 2

    found_matches = False

    for url_batch in [urls[:limit], urls[limit:limit*2] if len(urls) > limit else []]:
        if not url_batch or (found_matches and url_batch == urls[limit:limit*2]):
            continue

        if url_batch == urls[limit:limit*2] and args.verbose:
            print("Doubling URL limit for final attempt")

        for url in url_batch:
            result = await process_url(url, content_data, is_tv, args.verbose)
            if not result:
                continue

            matches, scraped = result

            if not validate_episodes(content_data, scraped, is_tv):
                print("Process stopped due to missing episodes")
                return

            update_database(matches, scraped)
            print(f"Updated {len(matches)} items from {url}")
            found_matches = True

            if is_tv:
                season_key = (matches[0]['series'], matches[0]['season'])
                needed.discard(season_key)
            else:
                needed.discard(matches[0]['movie'])

            if not needed:
                return

    if not found_matches:
        print("No matches found")

if __name__ == "__main__":
    asyncio.run(main())