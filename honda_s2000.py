#!/usr/bin/env python3

import sqlite3
import json
import logging
import urllib.request
import urllib.parse
import urllib.error
import time
import os
import re
from typing import Dict, List, Optional, Any

class TVMazeProcessor:
    """Honda S2000 TVMaze API processor - follows the exact 3-step pattern"""
    
    def __init__(self, config_file: str = '2jznoshit.json'):
        self.config_file = config_file
        self.db_path = 'danger2manifold.db'
        self.rate_limit_delay = 0.5
        self.last_request_time = 0
        
        # STEP 1: Open user preferences file 2jznoshit.json
        self.config = self._load_config()
        self._setup_logging()
        
    def _load_config(self) -> Dict[str, Any]:
        """STEP 1: Load configuration from 2jznoshit.json"""
        try:
            print("STEP 1: Loading user preferences from 2jznoshit.json...")
            with open(self.config_file, 'r', encoding='utf-8') as f:
                config = json.load(f)
                print("✓ Configuration loaded successfully")
                return config
        except FileNotFoundError:
            print(f"ERROR: Configuration file {self.config_file} not found")
            raise
        except json.JSONDecodeError as e:
            print(f"ERROR: Invalid JSON in {self.config_file}: {e}")
            raise
    
    def _setup_logging(self):
        """Check logs and json output preferences"""
        honda_config = self.config.get('honda_S2000', {})
        logs_enabled = honda_config.get('logs', False)
        json_output = honda_config.get('json', False)
        
        print(f"STEP 1: Checking preferences - Logs: {logs_enabled}, JSON: {json_output}")
        
        if logs_enabled:
            log_level = logging.DEBUG if json_output else logging.INFO
            logging.basicConfig(
                filename='honda_S2000.log',
                level=log_level,
                format='%(asctime)s - %(levelname)s - %(message)s',
                filemode='a'
            )
            logging.info("Honda S2000 TVMaze processor started")
        else:
            logging.disable(logging.CRITICAL)
    
    def get_unprocessed_records(self):
        """Get records from honda_s2000 table that need TVMaze data"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                # Get records where TVMaze data is missing (tvm_id is NULL)
                cursor.execute("""
                    SELECT it_checksum, it_series, it_sea_no, it_ep_no 
                    FROM honda_s2000 
                    WHERE tvm_id IS NULL 
                    AND it_checksum IS NOT NULL 
                    AND it_series IS NOT NULL
                    AND it_sea_no IS NOT NULL
                    AND it_ep_no IS NOT NULL
                """)
                return cursor.fetchall()
        except sqlite3.Error as e:
            logging.error(f"Database error: {e}")
            return []
    
    def _rate_limit(self):
        """Rate limiting for TVMaze API"""
        current_time = time.time()
        time_since_last = current_time - self.last_request_time
        if time_since_last < self.rate_limit_delay:
            time.sleep(self.rate_limit_delay - time_since_last)
        self.last_request_time = time.time()
    
    def _make_request(self, url: str) -> Optional[Dict]:
        """Make HTTP request using urllib"""
        self._rate_limit()
        try:
            logging.debug(f"Making request to: {url}")
            with urllib.request.urlopen(url, timeout=10) as response:
                data = response.read().decode('utf-8')
                return json.loads(data)
        except urllib.error.HTTPError as e:
            if e.code == 404:
                logging.warning(f"Not found: {url}")
                return None
            else:
                logging.error(f"HTTP {e.code}: {url}")
                return None
        except Exception as e:
            logging.error(f"Request failed for {url}: {e}")
            return None
    
    def search_show(self, series_name: str) -> Optional[int]:
        """Search for show and return TVMaze ID"""
        if not series_name:
            return None
        
        query = urllib.parse.quote(series_name.strip())
        url = f"http://api.tvmaze.com/search/shows?q={query}"
        
        results = self._make_request(url)
        if results and len(results) > 0:
            show_id = results[0].get('show', {}).get('id')
            if show_id:
                logging.info(f"Found show '{series_name}' with ID {show_id}")
                return show_id
        
        logging.warning(f"No show found for '{series_name}'")
        return None
    
    def get_show_details(self, show_id: int) -> Optional[Dict]:
        """Get show details with embedded episodes and cast"""
        url = f"http://api.tvmaze.com/shows/{show_id}?embed[]=episodes&embed[]=cast"
        return self._make_request(url)
    
    def get_show_seasons(self, show_id: int) -> Optional[List[Dict]]:
        """Get season information"""
        url = f"http://api.tvmaze.com/shows/{show_id}/seasons"
        return self._make_request(url)
    
    def _safe_get(self, data: Dict, *keys, default=None):
        """Safely get nested values"""
        try:
            result = data
            for key in keys:
                if isinstance(result, dict) and key in result:
                    result = result[key]
                else:
                    return default
            return result if result is not None else default
        except:
            return default
    
    def _clean_html(self, text: str) -> Optional[str]:
        """Remove HTML tags"""
        if not text:
            return None
        try:
            clean = re.sub(r'<[^>]+>', '', text).strip()
            return clean if clean else None
        except:
            return None
    
    def _extract_year(self, date_str: str) -> Optional[str]:
        """Extract year from date"""
        if not date_str:
            return None
        try:
            return date_str.split('-')[0]
        except:
            return None
    
    def _format_cast(self, cast_data: List[Dict]) -> Optional[str]:
        """Get top 5 cast members"""
        if not cast_data:
            return None
        try:
            names = []
            for member in cast_data[:5]:
                name = self._safe_get(member, 'person', 'name')
                if name:
                    names.append(name)
            return ', '.join(names) if names else None
        except:
            return None
    
    def _format_genres(self, genres: List[str]) -> Optional[str]:
        """Format genres"""
        if not genres:
            return None
        try:
            return ', '.join(genres) if isinstance(genres, list) else None
        except:
            return None
    
    def _find_episode(self, episodes: List[Dict], season: int, episode: int) -> Optional[Dict]:
        """Find specific episode"""
        if not episodes:
            return None
        for ep in episodes:
            if ep.get('season') == season and ep.get('number') == episode:
                return ep
        return None
    
    def _find_season(self, seasons: List[Dict], season: int) -> Optional[Dict]:
        """Find specific season"""
        if not seasons:
            return None
        for s in seasons:
            if s.get('number') == season:
                return s
        return None
    
    def _calculate_missing_episodes(self, episodes: List[Dict], seasons: List[Dict]) -> str:
        """Calculate if episodes are missing"""
        try:
            if not episodes or not seasons:
                return "none"
            
            for season in seasons:
                season_num = season.get('number')
                expected = season.get('episodeOrder', 0)
                if season_num and expected:
                    actual = len([ep for ep in episodes if ep.get('season') == season_num])
                    if actual < expected:
                        return "some"
            return "none"
        except:
            return "none"
    
    def process_tvmaze_data(self, checksum: str, series: str, season_str: str, episode_str: str) -> Dict:
        """Process TVMaze data for a single record"""
        logging.info(f"Processing: {series} S{season_str}E{episode_str}")
        
        # Initialize all fields with null values
        result = {
            'tvm_id': None, 'tvm_series': None, 'tvm_series_desc': None,
            'tvm_sea_no': None, 'tvm_sea_desc': None, 'tvm_sea_yr': None,
            'tvm_ep_no': None, 'tvm_ep_title': None, 'tvm_ep_desc': None,
            'tvm_air': None, 'tvm_ep_dur': None, 'tvm_studio': None,
            'tvm_genre': None, 'tvm_nw_rat': None, 'tvm_cast': None,
            'tvm_src_link': None, 'tvm_ep_img': None, 'tvm_sea_img': None,
            'tvm_series_img': None, 'tvm_sea_avl': None, 'tvm_ep_avl': None,
            'tvm_mis_ep': 'none'
        }
        
        try:
            # Search for show
            show_id = self.search_show(series)
            if not show_id:
                return result
            
            # Get show details
            show_data = self.get_show_details(show_id)
            if not show_data:
                return result
            
            # Get seasons
            seasons_data = self.get_show_seasons(show_id)
            
            # Parse season/episode numbers
            try:
                season_num = int(re.search(r'\d+', season_str).group()) if re.search(r'\d+', season_str) else None
                episode_num = int(re.search(r'\d+', episode_str).group()) if re.search(r'\d+', episode_str) else None
            except:
                season_num = None
                episode_num = None
            
            # Extract show data
            result.update({
                'tvm_id': str(show_data.get('id')),
                'tvm_series': show_data.get('name'),
                'tvm_series_desc': self._clean_html(show_data.get('summary')),
                'tvm_studio': (self._safe_get(show_data, 'network', 'name') or 
                              self._safe_get(show_data, 'webChannel', 'name')),
                'tvm_genre': self._format_genres(show_data.get('genres')),
                'tvm_nw_rat': None,  # Not available in TVMaze
                'tvm_cast': self._format_cast(self._safe_get(show_data, '_embedded', 'cast')),
                'tvm_src_link': show_data.get('url'),
                'tvm_series_img': self._safe_get(show_data, 'image', 'medium'),
            })
            
            # Extract episode data
            episodes = self._safe_get(show_data, '_embedded', 'episodes', default=[])
            if season_num and episode_num:
                episode_data = self._find_episode(episodes, season_num, episode_num)
                if episode_data:
                    result.update({
                        'tvm_ep_no': str(episode_data.get('number')),
                        'tvm_ep_title': episode_data.get('name'),
                        'tvm_ep_desc': self._clean_html(episode_data.get('summary')),
                        'tvm_air': episode_data.get('airdate'),
                        'tvm_ep_dur': str(episode_data.get('runtime')) if episode_data.get('runtime') else None,
                        'tvm_ep_img': self._safe_get(episode_data, 'image', 'medium'),
                    })
            
            # Extract season data
            if seasons_data and season_num:
                season_data = self._find_season(seasons_data, season_num)
                if season_data:
                    result.update({
                        'tvm_sea_no': str(season_data.get('number')),
                        'tvm_sea_desc': self._clean_html(season_data.get('summary')),
                        'tvm_sea_yr': self._extract_year(season_data.get('premiereDate')),
                        'tvm_sea_img': self._safe_get(season_data, 'image', 'medium'),
                        'tvm_ep_avl': str(season_data.get('episodeOrder')) if season_data.get('episodeOrder') else None,
                    })
            
            # Calculate totals
            if seasons_data:
                result['tvm_sea_avl'] = str(len(seasons_data))
            
            if episodes and seasons_data:
                result['tvm_mis_ep'] = self._calculate_missing_episodes(episodes, seasons_data)
            
        except Exception as e:
            logging.error(f"Error processing {checksum}: {e}")
        
        return result
    
    def update_database_record(self, checksum: str, tvmaze_data: Dict) -> bool:
        """Update honda_s2000 table record with TVMaze data"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                update_query = """
                UPDATE honda_s2000 SET
                    tvm_id = ?, tvm_series = ?, tvm_series_desc = ?,
                    tvm_sea_no = ?, tvm_sea_desc = ?, tvm_sea_yr = ?,
                    tvm_ep_no = ?, tvm_ep_title = ?, tvm_ep_desc = ?,
                    tvm_air = ?, tvm_ep_dur = ?, tvm_studio = ?,
                    tvm_genre = ?, tvm_nw_rat = ?, tvm_cast = ?,
                    tvm_src_link = ?, tvm_ep_img = ?, tvm_sea_img = ?,
                    tvm_series_img = ?, tvm_sea_avl = ?, tvm_ep_avl = ?,
                    tvm_mis_ep = ?
                WHERE it_checksum = ?
                """
                
                values = (
                    tvmaze_data['tvm_id'], tvmaze_data['tvm_series'], tvmaze_data['tvm_series_desc'],
                    tvmaze_data['tvm_sea_no'], tvmaze_data['tvm_sea_desc'], tvmaze_data['tvm_sea_yr'],
                    tvmaze_data['tvm_ep_no'], tvmaze_data['tvm_ep_title'], tvmaze_data['tvm_ep_desc'],
                    tvmaze_data['tvm_air'], tvmaze_data['tvm_ep_dur'], tvmaze_data['tvm_studio'],
                    tvmaze_data['tvm_genre'], tvmaze_data['tvm_nw_rat'], tvmaze_data['tvm_cast'],
                    tvmaze_data['tvm_src_link'], tvmaze_data['tvm_ep_img'], tvmaze_data['tvm_sea_img'],
                    tvmaze_data['tvm_series_img'], tvmaze_data['tvm_sea_avl'], tvmaze_data['tvm_ep_avl'],
                    tvmaze_data['tvm_mis_ep'], checksum
                )
                
                cursor.execute(update_query, values)
                conn.commit()
                
                if cursor.rowcount > 0:
                    logging.info(f"Updated record for checksum {checksum}")
                    return True
                else:
                    logging.warning(f"No record found for checksum {checksum}")
                    return False
                    
        except sqlite3.Error as e:
            logging.error(f"Database error updating {checksum}: {e}")
            return False
        except Exception as e:
            logging.error(f"Error updating {checksum}: {e}")
            return False
    
    def run(self):
        """Main execution - follow the exact 3-step pattern"""
        try:
            # STEP 2: Open danger2manifold.db and honda_s2000 table
            print("STEP 2: Opening danger2manifold.db and honda_s2000 table...")
            
            if not os.path.exists(self.db_path):
                print(f"ERROR: Database {self.db_path} not found")
                return False
            
            # Get records that need TVMaze data (where tvm_id IS NULL)
            records = self.get_unprocessed_records()
            
            if not records:
                print("No records need TVMaze data processing")
                return True
            
            print(f"✓ Found {len(records)} records to process")
            
            # STEP 3: Call TVMaze API for each record
            print("STEP 3: Calling TVMaze API for each record...")
            
            success_count = 0
            failure_count = 0
            
            for i, (checksum, series, season, episode) in enumerate(records, 1):
                print(f"Processing {i}/{len(records)}: {series} S{season}E{episode}")
                
                try:
                    # Get TVMaze data
                    tvmaze_data = self.process_tvmaze_data(checksum, series, season, episode)
                    
                    # Update database
                    if self.update_database_record(checksum, tvmaze_data):
                        success_count += 1
                        print(f"  ✓ Updated successfully")
                    else:
                        failure_count += 1
                        print(f"  ✗ Update failed")
                        
                except KeyboardInterrupt:
                    print("\nInterrupted by user")
                    break
                except Exception as e:
                    logging.error(f"Error processing record {checksum}: {e}")
                    failure_count += 1
                    print(f"  ✗ Error: {e}")
            
            print(f"\nProcessing complete:")
            print(f"  Success: {success_count}")
            print(f"  Failures: {failure_count}")
            
            logging.info(f"Processing complete - Success: {success_count}, Failures: {failure_count}")
            return failure_count == 0
            
        except Exception as e:
            print(f"FATAL ERROR: {e}")
            logging.error(f"Fatal error: {e}")
            return False


def main():
    """Main entry point"""
    try:
        processor = TVMazeProcessor()
        success = processor.run()
        exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\nInterrupted")
        exit(1)
    except Exception as e:
        print(f"FATAL: {e}")
        exit(1)


if __name__ == "__main__":
    main()