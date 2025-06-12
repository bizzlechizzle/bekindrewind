#!/usr/bin/env python3

import sqlite3
import json
import logging
import requests
import time
from collections import defaultdict
from pathlib import Path

class DomsCharger:
    def __init__(self):
        self.preferences = self._load_preferences()
        self._setup_logging()
        self.headers = None
        self.cache = {}
        
    def _load_preferences(self):
        """Load user preferences with error handling."""
        try:
            with open('2jznoshit.json', 'r') as f:
                return json.load(f)
        except (FileNotFoundError, json.JSONDecodeError) as e:
            print(f"Error loading preferences: {e}")
            exit(1)
    
    def _setup_logging(self):
        """Setup logging based on preferences."""
        if self.preferences.get('doms_charger', {}).get('logs', False):
            logging.basicConfig(
                filename='doms_charger.log',
                level=logging.DEBUG,
                format='%(asctime)s - %(levelname)s - %(message)s',
                filemode='a'
            )
        else:
            logging.disable(logging.CRITICAL)
    
    def _get_auth_token(self):
        """Get TheTVDB auth token with caching."""
        if self.headers:
            return True
            
        api_key = self.preferences.get('api_keys', {}).get('theTVDB')
        if not api_key:
            logging.error("TheTVDB API key not found")
            return False
            
        try:
            response = requests.post(
                'https://api4.thetvdb.com/v4/login',
                json={'apikey': api_key},
                timeout=10
            )
            response.raise_for_status()
            token = response.json()['data']['token']
            self.headers = {'Authorization': f'Bearer {token}'}
            logging.info("Successfully authenticated with TheTVDB")
            return True
        except requests.RequestException as e:
            logging.error(f"Authentication failed: {e}")
            return False
    
    def _api_call(self, url, retries=3):
        """Make API call with retry logic and rate limiting."""
        for attempt in range(retries):
            try:
                time.sleep(0.1)  # Rate limiting
                response = requests.get(url, headers=self.headers, timeout=10)
                
                if response.status_code == 429:  # Rate limited
                    wait_time = 2 ** attempt
                    logging.warning(f"Rate limited, waiting {wait_time}s")
                    time.sleep(wait_time)
                    continue
                    
                if response.status_code == 200:
                    return response.json()
                elif response.status_code == 404:
                    return None
                else:
                    response.raise_for_status()
                    
            except requests.RequestException as e:
                logging.warning(f"API call failed (attempt {attempt + 1}): {e}")
                if attempt == retries - 1:
                    return None
        return None
    
    def _search_series(self, series_name):
        """Search for series by name with caching."""
        cache_key = f"series_{series_name.lower()}"
        if cache_key in self.cache:
            return self.cache[cache_key]
            
        url = f"https://api4.thetvdb.com/v4/search?query={requests.utils.quote(series_name)}&type=series"
        result = self._api_call(url)
        
        if result and result.get('data'):
            series_data = result['data'][0]
            self.cache[cache_key] = series_data
            logging.debug(f"Found series: {series_name} -> ID: {series_data.get('tvdb_id')}")
            return series_data
        
        logging.warning(f"Series not found: {series_name}")
        self.cache[cache_key] = None
        return None
    
    def _get_series_details(self, series_id):
        """Get detailed series information."""
        cache_key = f"series_details_{series_id}"
        if cache_key in self.cache:
            return self.cache[cache_key]
            
        url = f"https://api4.thetvdb.com/v4/series/{series_id}/extended"
        result = self._api_call(url)
        
        if result and result.get('data'):
            self.cache[cache_key] = result['data']
            return result['data']
        
        self.cache[cache_key] = None
        return None
    
    def _get_season_episodes(self, series_id, season_num):
        """Get all episodes for a season."""
        cache_key = f"episodes_{series_id}_{season_num}"
        if cache_key in self.cache:
            return self.cache[cache_key]
            
        url = f"https://api4.thetvdb.com/v4/series/{series_id}/episodes/default?season={season_num}"
        result = self._api_call(url)
        
        episodes = []
        if result and result.get('data', {}).get('episodes'):
            episodes = result['data']['episodes']
            
        self.cache[cache_key] = episodes
        logging.debug(f"Found {len(episodes)} episodes for series {series_id} season {season_num}")
        return episodes
    
    def _extract_season_number(self, season_str):
        """Extract numeric season number from string."""
        if not season_str:
            return None
        # Handle formats like "season 1", "season 01", "s1", etc.
        import re
        match = re.search(r'(\d+)', str(season_str))
        return int(match.group(1)) if match else None
    
    def _extract_episode_number(self, episode_str):
        """Extract numeric episode number from string."""
        if not episode_str:
            return None
        import re
        match = re.search(r'(\d+)', str(episode_str))
        return int(match.group(1)) if match else None
    
    def _load_video_files(self):
        """Load video files from database."""
        try:
            with sqlite3.connect('danger2manifold.db') as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    SELECT it_checksum, it_series, it_sea_no, it_ep_no 
                    FROM doms_charger 
                    WHERE it_checksum IS NOT NULL AND it_series IS NOT NULL
                ''')
                
                files = []
                for row in cursor.fetchall():
                    files.append({
                        'checksum': row[0],
                        'series': row[1],
                        'season': row[2],
                        'episode': row[3]
                    })
                
                logging.info(f"Loaded {len(files)} video files from database")
                return files
                
        except sqlite3.Error as e:
            logging.error(f"Database error: {e}")
            return []
    
    def _update_database(self, checksum, data):
        """Update database row with API data."""
        try:
            with sqlite3.connect('danger2manifold.db') as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    UPDATE doms_charger SET 
                        tvdb_id = ?, tvdb_series = ?, tvdb_series_desc = ?,
                        tvdb_sea_no = ?, tvdb_sea_desc = ?, tvdb_sea_yr = ?,
                        tvdb_ep_no = ?, tvdb_ep_title = ?, tvdb_ep_desc = ?,
                        tvdb_air = ?, tvdb_ep_dur = ?, tvdb_studio = ?,
                        tvdb_genre = ?, tvdb_src_link = ?, tvdb_sea_avl = ?,
                        tvdb_ep_avl = ?
                    WHERE it_checksum = ?
                ''', (
                    data.get('tvdb_id'), data.get('tvdb_series'), data.get('tvdb_series_desc'),
                    data.get('tvdb_sea_no'), data.get('tvdb_sea_desc'), data.get('tvdb_sea_yr'),
                    data.get('tvdb_ep_no'), data.get('tvdb_ep_title'), data.get('tvdb_ep_desc'),
                    data.get('tvdb_air'), data.get('tvdb_ep_dur'), data.get('tvdb_studio'),
                    data.get('tvdb_genre'), data.get('tvdb_src_link'), data.get('tvdb_sea_avl'),
                    data.get('tvdb_ep_avl'), checksum
                ))
                
                if cursor.rowcount > 0:
                    logging.debug(f"Updated database for checksum: {checksum}")
                else:
                    logging.warning(f"No rows updated for checksum: {checksum}")
                    
        except sqlite3.Error as e:
            logging.error(f"Database update error for {checksum}: {e}")
    
    def process_files(self):
        """Main processing function."""
        if not self._get_auth_token():
            print("Failed to authenticate with TheTVDB API")
            return
            
        video_files = self._load_video_files()
        if not video_files:
            print("No video files found in database")
            return
        
        # Group files by series for efficient processing
        series_groups = defaultdict(list)
        for file in video_files:
            series_groups[file['series']].append(file)
        
        processed_count = 0
        
        for series_name, files in series_groups.items():
            logging.info(f"Processing series: {series_name} ({len(files)} files)")
            
            # Search for series
            series_search = self._search_series(series_name)
            if not series_search:
                # Set all files in this series to null values
                for file in files:
                    self._update_database(file['checksum'], {})
                continue
            
            series_id = series_search.get('tvdb_id')
            if not series_id:
                continue
                
            # Get detailed series info
            series_details = self._get_series_details(series_id)
            
            # Group files by season
            season_groups = defaultdict(list)
            for file in files:
                season_groups[file['season']].append(file)
            
            for season_str, season_files in season_groups.items():
                season_num = self._extract_season_number(season_str)
                if season_num is None:
                    continue
                
                # Get episodes for this season
                episodes = self._get_season_episodes(series_id, season_num)
                
                # Calculate season year from first aired episode
                season_year = None
                if episodes:
                    aired_dates = [ep.get('aired') for ep in episodes if ep.get('aired')]
                    if aired_dates:
                        season_year = min(aired_dates)[:4]  # Extract year
                
                # Process each file in this season
                for file in season_files:
                    episode_num = self._extract_episode_number(file['episode'])
                    
                    # Find matching episode
                    episode_data = None
                    if episodes and episode_num is not None:
                        episode_data = next(
                            (ep for ep in episodes if ep.get('number') == episode_num), 
                            None
                        )
                    
                    # Build update data
                    update_data = {
                        'tvdb_id': str(series_id),
                        'tvdb_series': series_search.get('name'),
                        'tvdb_series_desc': series_details.get('overview') if series_details else None,
                        'tvdb_sea_no': season_str,
                        'tvdb_sea_desc': None,  # Season descriptions not available in v4 API
                        'tvdb_sea_yr': season_year,
                        'tvdb_ep_no': file['episode'],
                        'tvdb_ep_title': episode_data.get('name') if episode_data else None,
                        'tvdb_ep_desc': episode_data.get('overview') if episode_data else None,
                        'tvdb_air': episode_data.get('aired') if episode_data else None,
                        'tvdb_ep_dur': episode_data.get('runtime') if episode_data else None,
                        'tvdb_studio': None,  # Network info not consistently available
                        'tvdb_genre': ','.join([g.get('name', '') for g in series_details.get('genres', [])]) if series_details else None,
                        'tvdb_src_link': f"https://thetvdb.com/series/{series_search.get('slug')}" if series_search.get('slug') else None,
                        'tvdb_sea_avl': len(series_details.get('seasons', [])) if series_details else None,
                        'tvdb_ep_avl': len(episodes) if episodes else None
                    }
                    
                    self._update_database(file['checksum'], update_data)
                    processed_count += 1
        
        logging.info(f"Processing complete. Updated {processed_count} files.")
        print(f"Processing complete. Updated {processed_count} files.")

def main():
    charger = DomsCharger()
    charger.process_files()

if __name__ == "__main__":
    main()