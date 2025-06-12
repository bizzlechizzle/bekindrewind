#!/usr/bin/env python3

import sqlite3
import json
import logging
import re
from typing import Dict, List, Optional
from bs4 import BeautifulSoup, Comment
from lxml import html

CONFIG_FILE = '2jznoshit.json'
DB_FILE = 'danger2manifold.db'
LOG_FILE = 'fastandfurious.log'

def setup_logging():
    try:
        with open(CONFIG_FILE, 'r') as f:
            log_enabled = json.load(f).get('fastandfurious', {}).get('logs', False)
    except:
        log_enabled = False
    
    if log_enabled:
        logging.basicConfig(filename=LOG_FILE, level=logging.DEBUG, 
                          format='%(asctime)s - %(message)s', filemode='a')
    else:
        logging.disable(logging.CRITICAL)

def clean_html_content(html_content: str) -> str:
    """Step 1-3: BeautifulSoup sanitize, RE minify for lxml"""
    if not html_content:
        return ""
    
    # BeautifulSoup sanitization
    soup = BeautifulSoup(html_content, 'lxml')
    
    # Remove scripts, styles, comments
    for tag in soup(['script', 'style', 'meta', 'link']):
        tag.decompose()
    
    for comment in soup.find_all(string=lambda text: isinstance(text, Comment)):
        comment.extract()
    
    cleaned = str(soup)
    
    # RE whitespace minification 
    cleaned = re.sub(r'\s+', ' ', cleaned)
    cleaned = re.sub(r'>\s+<', '><', cleaned)
    cleaned = re.sub(r'\s+>', '>', cleaned)
    cleaned = re.sub(r'<\s+', '<', cleaned)
    
    return cleaned.strip()

def extract_series_data(doc) -> Dict:
    """Extract series-level data (same for all episodes in season)"""
    data = {}
    
    # pw_series
    title_elem = doc.xpath('//h1[@data-automation-id="title"]')
    data['pw_series'] = title_elem[0].text_content().strip() if title_elem else ""
    
    # pw_sea_desc  
    desc_elem = doc.xpath('//span[@class="_1H6ABQ"]')
    data['pw_sea_desc'] = desc_elem[0].text_content().strip() if desc_elem else ""
    
    # pw_sea_yr
    year_elem = doc.xpath('//span[@data-automation-id="release-year-badge"]')
    if year_elem:
        year_text = year_elem[0].text_content()
        year_match = re.search(r'(20\d{2})', year_text)
        data['pw_sea_yr'] = year_match.group(1) if year_match else ""
    else:
        data['pw_sea_yr'] = ""
    
    # pw_studio - FIXED XPATH
    studio_elem = doc.xpath('//dl[@data-testid="metadata-row"]//dt[contains(.//text(), "Studio")]/following-sibling::dd')
    data['pw_studio'] = studio_elem[0].text_content().strip() if studio_elem else ""
    
    # pw_genre
    genre_elem = doc.xpath('//div[@data-testid="genresMetadata"]//a')
    data['pw_genre'] = genre_elem[0].text_content().strip() if genre_elem else ""
    
    # pw_cast (top 5) - FIXED XPATH
    cast_elems = doc.xpath('//dl[@data-testid="metadata-row"]//dt[contains(.//text(), "Cast")]/following-sibling::dd//a')
    cast_list = [elem.text_content().strip() for elem in cast_elems[:5]]
    data['pw_cast'] = ', '.join(cast_list)
    
    # pw_series_img
    series_img_elem = doc.xpath('//div[@data-automation-id="hero-background"]//img/@src')
    data['pw_series_img'] = series_img_elem[0] if series_img_elem else ""
    
    # pw_ep_avl
    avl_elem = doc.xpath('//span[contains(@aria-label, "episodes")]')
    if avl_elem:
        avl_text = avl_elem[0].get('aria-label', '')
        avl_match = re.search(r'(\d+)\s+episodes?', avl_text)
        data['pw_ep_avl'] = avl_match.group(1) if avl_match else ""
    else:
        data['pw_ep_avl'] = ""
    
    return data

def parse_episode_number(ep_text: str) -> Optional[int]:
    """Parse episode number from database format like 'Episode 1' -> 1"""
    if not ep_text:
        return None
    
    # Handle "Episode X" format
    match = re.search(r'Episode\s+(\d+)', ep_text, re.IGNORECASE)
    if match:
        return int(match.group(1))
    
    # Handle direct number format
    if ep_text.isdigit():
        return int(ep_text)
    
    return None

def find_matching_episode_data(doc, target_ep_num: int, season_num: str) -> Dict:
    """Find episode data by matching episode numbers from HTML"""
    data = {}
    
    # Get all episode containers
    ep_containers = doc.xpath('//li[@data-testid="episode-list-item"]')
    
    for container in ep_containers:
        # Extract episode info from this container's title
        ep_title_elem = container.xpath('.//h3//span[@class="_36qUej izvPPq"]//span[1]')
        if not ep_title_elem:
            continue
            
        ep_text = ep_title_elem[0].text_content().strip()
        
        # Parse season and episode from various formats (S01E01, S1 E1, etc.)
        patterns = [
            r'S(\d+)\s*E(\d+)',  # S01E01, S1E1
            r'S(\d+)\s+E(\d+)',  # S1 E1
            r'Season\s+(\d+)\s+Episode\s+(\d+)',  # Season 1 Episode 1
        ]
        
        parsed_season = None
        parsed_episode = None
        
        for pattern in patterns:
            match = re.search(pattern, ep_text, re.IGNORECASE)
            if match:
                parsed_season = int(match.group(1))
                parsed_episode = int(match.group(2))
                break
        
        if parsed_episode is None:
            continue
        
        # Check if this matches our target episode
        if parsed_episode == target_ep_num:
            # This is our episode, extract all data from THIS container
            data['pw_sea_no'] = str(parsed_season) if parsed_season else season_num
            data['pw_ep_no'] = str(parsed_episode)
            
            # pw_ep_title - from THIS container
            title_elem = container.xpath('.//span[@class="P1uAb6"]')
            data['pw_ep_title'] = title_elem[0].text_content().strip() if title_elem else ""
            
            # pw_ep_desc - from THIS container
            desc_elem = container.xpath('.//div[@dir="auto"]')
            data['pw_ep_desc'] = desc_elem[0].text_content().strip() if desc_elem else ""
            
            # pw_air - from THIS container
            air_elem = container.xpath('.//div[@data-testid="episode-release-date"]')
            data['pw_air'] = air_elem[0].text_content().strip() if air_elem else ""
            
            # pw_ep_dur - from THIS container
            dur_elem = container.xpath('.//div[@data-testid="episode-runtime"]')
            data['pw_ep_dur'] = dur_elem[0].text_content().strip() if dur_elem else ""
            
            # pw_nw_rat - from THIS container
            rating_elem = container.xpath('.//span[@data-testid="rating-badge"]')
            if rating_elem:
                rating_text = rating_elem[0].text_content().strip()
                data['pw_nw_rat'] = rating_text.split('(')[0].strip()
            else:
                data['pw_nw_rat'] = ""
            
            # pw_ep_img - from THIS container (the episode thumbnail)
            img_elem = container.xpath('.//img[@data-testid="base-image"]/@src')
            data['pw_ep_img'] = img_elem[0] if img_elem else ""
            
            return data
    
    # If no exact match found, return empty data with episode numbers
    logging.warning(f"No matching episode found in HTML for episode {target_ep_num}")
    return {
        'pw_sea_no': season_num,
        'pw_ep_no': str(target_ep_num),
        'pw_ep_title': "",
        'pw_ep_desc': "",
        'pw_air': "",
        'pw_ep_dur': "",
        'pw_nw_rat': "",
        'pw_ep_img': ""
    }

def process_season(conn, season: str):
    """Process all episodes in a season"""
    cursor = conn.cursor()
    
    # Get all episodes for this season
    cursor.execute("""
        SELECT it_checksum, it_series, it_sea_no, it_ep_no, no_monica 
        FROM monica 
        WHERE it_sea_no = ? AND no_monica IS NOT NULL
        ORDER BY CAST(REPLACE(it_ep_no, 'Episode ', '') AS INTEGER)
    """, (season,))
    
    episodes = cursor.fetchall()
    if not episodes:
        return
    
    logging.info(f"Processing {len(episodes)} episodes for season {season}")
    
    # Parse first episode for series data
    first_checksum, first_series, first_sea_no, first_ep_no, first_html = episodes[0]
    
    if not first_html:
        logging.error(f"No HTML content for first episode {first_checksum}")
        return
    
    # Clean and parse first episode
    cleaned_html = clean_html_content(first_html)
    
    try:
        doc = html.fromstring(cleaned_html)
    except Exception as e:
        logging.error(f"Failed to parse HTML for {first_checksum}: {e}")
        return
    
    # Extract series-level data from first episode
    series_data = extract_series_data(doc)
    
    # Count total episodes in DB for this season
    cursor.execute("""
        SELECT COUNT(*) FROM monica WHERE it_sea_no = ?
    """, (season,))
    total_episodes = cursor.fetchone()[0]
    
    # Calculate pw_ep_dif
    expected_episodes = int(series_data.get('pw_ep_avl', '0') or '0')
    pw_ep_dif = str(total_episodes - expected_episodes) if expected_episodes > 0 else ""
    
    # Process each episode
    batch_updates = []
    verification_warnings = []
    
    for idx, (it_checksum, it_series, it_sea_no, it_ep_no, no_monica) in enumerate(episodes):
        # Parse the episode number from database format
        target_ep_num = parse_episode_number(it_ep_no)
        if target_ep_num is None:
            logging.error(f"Could not parse episode number from '{it_ep_no}' for {it_checksum}")
            continue
        
        # For first episode, use already parsed doc and cleaned HTML
        if idx == 0:
            current_doc = doc
            current_cleaned = cleaned_html
        else:
            # Clean and parse subsequent episodes
            current_cleaned = clean_html_content(no_monica)
            try:
                current_doc = html.fromstring(current_cleaned)
            except Exception as e:
                logging.error(f"Failed to parse HTML for {it_checksum}: {e}")
                continue
        
        # Find matching episode data using the target episode number
        episode_data = find_matching_episode_data(current_doc, target_ep_num, it_sea_no)
        
        # VERIFICATION: Check if extracted episode numbers match database
        extracted_season = episode_data.get('pw_sea_no', '')
        extracted_episode = episode_data.get('pw_ep_no', '')
        
        if extracted_episode:
            if extracted_episode != str(target_ep_num):
                warning_msg = f"Episode number mismatch for {it_checksum}: DB={it_ep_no}({target_ep_num}), HTML={extracted_episode}"
                logging.warning(warning_msg)
                verification_warnings.append(warning_msg)
        else:
            logging.warning(f"Could not extract episode numbers from HTML for {it_checksum}")
            verification_warnings.append(f"No episode numbers extracted for {it_checksum}")
        
        # Combine series and episode data
        combined_data = {**series_data, **episode_data}
        combined_data['pw_ep_dif'] = pw_ep_dif
        
        # Create JSON for bye_monica
        json_str = json.dumps(combined_data, separators=(',', ':'))
        
        batch_updates.append((current_cleaned, json_str, it_checksum))
        logging.debug(f"Prepared update for {it_checksum} - Episode {it_ep_no} (target: {target_ep_num})")
    
    # Report verification warnings
    if verification_warnings:
        logging.warning(f"Season {season} had {len(verification_warnings)} verification warnings:")
        for warning in verification_warnings:
            logging.warning(f"  - {warning}")
    
    # Batch update database
    cursor.executemany("""
        UPDATE monica 
        SET no_monica = ?, bye_monica = ?
        WHERE it_checksum = ?
    """, batch_updates)
    
    logging.info(f"Updated {len(batch_updates)} episodes for season {season}")

def main():
    setup_logging()
    
    conn = sqlite3.connect(DB_FILE)
    conn.execute("PRAGMA journal_mode=WAL")
    
    try:
        cursor = conn.cursor()
        
        # Get all seasons
        cursor.execute("SELECT DISTINCT it_sea_no FROM monica ORDER BY it_sea_no")
        seasons = [row[0] for row in cursor.fetchall()]
        
        logging.info(f"Processing {len(seasons)} seasons")
        
        for season in seasons:
            process_season(conn, season)
        
        conn.commit()
        logging.info("All seasons processed successfully")
        
    except Exception as e:
        conn.rollback()
        logging.error(f"Processing failed: {e}")
        raise
    finally:
        conn.close()

if __name__ == "__main__":
    main()