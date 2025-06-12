#!/usr/bin/env python3

import sqlite3
import json
import logging
import requests
import time
import re
from typing import Optional, Dict, Any, Tuple
from urllib.parse import quote_plus
import sys
import os

class HectorOMDBProcessor:
    """
    OMDB API processor for populating the hector table with series/episode metadata.
    Handles API rate limiting, caching, and comprehensive error handling.
    """
    
    def __init__(self):
        self.config = self._load_config()
        self.api_key = self._get_api_key()
        self.db_path = "danger2manifold.db"
        self.base_url = "http://www.omdbapi.com/"
        self.session = requests.Session()
        self.session.timeout = 30
        
        # Cache to avoid redundant API calls
        self.series_cache = {}
        self.season_cache = {}
        
        # Rate limiting
        self.last_request_time = 0
        self.min_request_interval = 0.1  # 100ms between requests
        
        self._setup_logging()
    
    def _load_config(self) -> Dict[str, Any]:
        """Load configuration from 2jznoshit.json with error handling."""
        try:
            with open('2jznoshit.json', 'r') as f:
                return json.load(f)
        except FileNotFoundError:
            print("ERROR: 2jznoshit.json not found")
            sys.exit(1)
        except json.JSONDecodeError as e:
            print(f"ERROR: Invalid JSON in 2jznoshit.json: {e}")
            sys.exit(1)
    
    def _get_api_key(self) -> str:
        """Extract API key from config with validation."""
        try:
            api_key = self.config['api_keys']['omdb']
            if not api_key or api_key.strip() == "":
                raise ValueError("Empty API key")
            return api_key.strip()
        except KeyError:
            print("ERROR: OMDB API key not found in config")
            sys.exit(1)
        except ValueError as e:
            print(f"ERROR: Invalid API key: {e}")
            sys.exit(1)
    
    def _setup_logging(self):
        """Configure logging based on user preferences."""
        hector_config = self.config.get('hector', {})
        log_enabled = hector_config.get('logs', False)
        
        if log_enabled:
            logging.basicConfig(
                filename='hector.log',
                level=logging.DEBUG,
                format='%(asctime)s - %(levelname)s - %(message)s',
                filemode='a'
            )
            logging.info("=== Hector OMDB Processor Started ===")
        else:
            logging.disable(logging.CRITICAL)
    
    def _rate_limit(self):
        """Implement rate limiting for API requests."""
        current_time = time.time()
        time_since_last = current_time - self.last_request_time
        
        if time_since_last < self.min_request_interval:
            sleep_time = self.min_request_interval - time_since_last
            time.sleep(sleep_time)
        
        self.last_request_time = time.time()
    
    def _make_api_request(self, params: Dict[str, str]) -> Optional[Dict[str, Any]]:
        """
        Make a rate-limited API request with comprehensive error handling.
        
        Args:
            params: Dictionary of API parameters
            
        Returns:
            API response as dictionary or None if failed
        """
        self._rate_limit()
        
        # Add API key to parameters
        params['apikey'] = self.api_key
        
        try:
            logging.debug(f"Making OMDB API request: {params}")
            response = self.session.get(self.base_url, params=params)
            response.raise_for_status()
            
            data = response.json()
            
            # Check for OMDB-specific errors
            if data.get('Response') == 'False':
                error_msg = data.get('Error', 'Unknown OMDB error')
                logging.warning(f"OMDB API error: {error_msg} for params: {params}")
                return None
            
            logging.debug(f"Successful API response: {data.get('Title', 'Unknown')}")
            return data
            
        except requests.exceptions.RequestException as e:
            logging.error(f"Network error during API request: {e}")
            return None
        except json.JSONDecodeError as e:
            logging.error(f"Invalid JSON response from OMDB: {e}")
            return None
        except Exception as e:
            logging.error(f"Unexpected error during API request: {e}")
            return None
    
    def _search_series(self, series_name: str) -> Optional[str]:
        """
        Search for series by name and return IMDb ID.
        
        Args:
            series_name: Name of the series to search for
            
        Returns:
            IMDb ID or None if not found
        """
        # Check cache first
        if series_name in self.series_cache:
            logging.debug(f"Using cached IMDb ID for '{series_name}'")
            return self.series_cache[series_name]
        
        params = {
            's': series_name,
            'type': 'series'
        }
        
        data = self._make_api_request(params)
        if not data or 'Search' not in data:
            logging.warning(f"No search results found for series: {series_name}")
            self.series_cache[series_name] = None
            return None
        
        # Look for exact match first, then close match
        search_results = data['Search']
        
        # Exact match
        for result in search_results:
            if result.get('Title', '').lower() == series_name.lower():
                imdb_id = result.get('imdbID')
                logging.info(f"Found exact match for '{series_name}': {imdb_id}")
                self.series_cache[series_name] = imdb_id
                return imdb_id
        
        # Close match (first result)
        if search_results:
            imdb_id = search_results[0].get('imdbID')
            title = search_results[0].get('Title', 'Unknown')
            logging.info(f"Using closest match for '{series_name}': {title} ({imdb_id})")
            self.series_cache[series_name] = imdb_id
            return imdb_id
        
        logging.warning(f"No suitable match found for series: {series_name}")
        self.series_cache[series_name] = None
        return None
    
    def _get_series_data(self, imdb_id: str) -> Dict[str, Any]:
        """
        Fetch detailed series information from OMDB.
        
        Args:
            imdb_id: IMDb ID of the series
            
        Returns:
            Dictionary with series data
        """
        params = {'i': imdb_id}
        data = self._make_api_request(params)
        
        if not data:
            logging.warning(f"Failed to fetch series data for IMDb ID: {imdb_id}")
            return {}
        
        # Extract and clean data
        series_info = {
            'imdb_series': self._clean_field(data.get('Title')),
            'imdb_series_desc': self._clean_field(data.get('Plot')),
            'imdb_studio': self._clean_field(data.get('Production')),
            'imdb_genre': self._clean_field(data.get('Genre')),
            'imdb_nw_rat': self._clean_field(data.get('Rated')),
            'imdb_cast': self._extract_top_actors(data.get('Actors')),
            'imdb_sea_avl': self._clean_field(data.get('totalSeasons')),
            'imdb_src_link': f"https://www.imdb.com/title/{imdb_id}"
        }
        
        logging.debug(f"Extracted series data for {imdb_id}: {series_info['imdb_series']}")
        return series_info
    
    def _get_season_data(self, imdb_id: str, season_no: str) -> Dict[str, Any]:
        """
        Fetch season information from OMDB.
        
        Args:
            imdb_id: IMDb ID of the series
            season_no: Season number (e.g., "01" or "season 1")
            
        Returns:
            Dictionary with season data
        """
        cache_key = f"{imdb_id}_{season_no}"
        if cache_key in self.season_cache:
            logging.debug(f"Using cached season data for {cache_key}")
            return self.season_cache[cache_key]
        
        # Extract numeric part from season string
        season_int = self._extract_number(season_no)
        if season_int is None:
            logging.warning(f"Could not extract season number from: {season_no}")
            season_info = {
                'imdb_ep_avl': None,
                'imdb_sea_yr': None
            }
            self.season_cache[cache_key] = season_info
            return season_info
        
        params = {
            'i': imdb_id,
            'Season': str(season_int)
        }
        
        data = self._make_api_request(params)
        if not data or 'Episodes' not in data:
            logging.warning(f"Failed to fetch season data for {imdb_id}, season {season_no}")
            season_info = {
                'imdb_ep_avl': None,
                'imdb_sea_yr': None
            }
        else:
            episodes = data['Episodes']
            season_info = {
                'imdb_ep_avl': str(len(episodes)),  # Convert to string for database consistency
                'imdb_sea_yr': self._extract_year_from_first_episode(episodes)
            }
        
        self.season_cache[cache_key] = season_info
        logging.debug(f"Extracted season data for {cache_key}: {season_info}")
        return season_info
    
    def _get_episode_data(self, imdb_id: str, season_no: str, episode_no: str) -> Dict[str, Any]:
        """
        Fetch specific episode information from OMDB.
        
        Args:
            imdb_id: IMDb ID of the series
            season_no: Season number (e.g., "01" or "season 1")
            episode_no: Episode number (e.g., "01" or "episode 1")
            
        Returns:
            Dictionary with episode data
        """
        # Extract numeric parts from season and episode strings
        season_int = self._extract_number(season_no)
        episode_int = self._extract_number(episode_no)
        
        if season_int is None or episode_int is None:
            logging.warning(f"Could not extract numbers from season '{season_no}' or episode '{episode_no}'")
            return {
                'imdb_sea_no': season_no,
                'imdb_ep_no': episode_no,
                'imdb_ep_title': None,
                'imdb_ep_desc': None,
                'imdb_air': None
            }
        
        params = {
            'i': imdb_id,
            'Season': str(season_int),
            'Episode': str(episode_int)
        }
        
        data = self._make_api_request(params)
        if not data:
            logging.warning(f"Failed to fetch episode data for {imdb_id} S{season_no}E{episode_no}")
            return {
                'imdb_sea_no': season_no,
                'imdb_ep_no': episode_no,
                'imdb_ep_title': None,
                'imdb_ep_desc': None,
                'imdb_air': None
            }
        
        episode_info = {
            'imdb_sea_no': season_no,
            'imdb_ep_no': episode_no,
            'imdb_ep_title': self._clean_field(data.get('Title')),
            'imdb_ep_desc': self._clean_field(data.get('Plot')),
            'imdb_air': self._clean_field(data.get('Released'))
        }
        
        logging.debug(f"Extracted episode data: {episode_info['imdb_ep_title']}")
        return episode_info
    
    def _clean_field(self, value: Any) -> Optional[str]:
        """Clean and validate field values from OMDB."""
        if not value or value == "N/A" or str(value).strip() == "":
            return None
        return str(value).strip()
    
    def _extract_top_actors(self, actors_str: Optional[str]) -> Optional[str]:
        """Extract top 5 actors from the actors string."""
        if not actors_str or actors_str == "N/A":
            return None
        
        actors = [actor.strip() for actor in actors_str.split(',')]
        top_actors = actors[:5]
        return ', '.join(top_actors)
    
    def _extract_number(self, text: str) -> Optional[int]:
        """
        Extract numeric value from strings like 'season 1', 'episode 10', '01', etc.
        
        Args:
            text: String that may contain a number
            
        Returns:
            Extracted integer or None if no number found
        """
        if not text:
            return None
        
        # First try to convert directly (for cases like "01", "1")
        try:
            return int(text)
        except ValueError:
            pass
        
        # Extract digits from text (for cases like "season 1", "episode 10")
        numbers = re.findall(r'\d+', str(text))
        if numbers:
            try:
                return int(numbers[0])  # Take the first number found
            except ValueError:
                pass
        
        logging.warning(f"Could not extract number from: {text}")
        return None
    
    def _extract_year_from_first_episode(self, episodes: list) -> Optional[str]:
        """Extract year from the first episode's release date."""
        if not episodes:
            return None
        
        first_episode = episodes[0]
        released = first_episode.get('Released')
        
        if not released or released == "N/A":
            return None
        
        # Extract year from date string (e.g., "2011-04-17" -> "2011")
        year_match = re.search(r'\d{4}', released)
        return year_match.group() if year_match else None
    
    def _get_database_connection(self) -> sqlite3.Connection:
        """Get database connection with error handling."""
        try:
            if not os.path.exists(self.db_path):
                logging.error(f"Database file not found: {self.db_path}")
                raise FileNotFoundError(f"Database file not found: {self.db_path}")
            
            conn = sqlite3.connect(self.db_path)
            return conn
            
        except sqlite3.Error as e:
            logging.error(f"Database connection failed: {e}")
            raise
    
    def _fetch_pending_records(self) -> list:
        """Fetch records from hector table that need OMDB data."""
        try:
            with self._get_database_connection() as conn:
                cursor = conn.execute("""
                    SELECT it_checksum, it_series, it_sea_no, it_ep_no
                    FROM hector
                    WHERE imdb_id IS NULL OR imdb_id = ''
                    ORDER BY it_series, it_sea_no, it_ep_no
                """)
                records = cursor.fetchall()
                logging.info(f"Found {len(records)} records to process")
                return records
                
        except sqlite3.Error as e:
            logging.error(f"Failed to fetch pending records: {e}")
            raise
    
    def _update_record(self, checksum: str, omdb_data: Dict[str, Any]):
        """Update a record in the hector table with OMDB data."""
        try:
            with self._get_database_connection() as conn:
                # Prepare update query with all OMDB fields
                update_query = """
                    UPDATE hector SET
                        imdb_id = ?,
                        imdb_series = ?,
                        imdb_series_desc = ?,
                        imdb_sea_no = ?,
                        imdb_sea_desc = NULL,
                        imdb_sea_yr = ?,
                        imdb_ep_no = ?,
                        imdb_ep_title = ?,
                        imdb_ep_desc = ?,
                        imdb_air = ?,
                        imdb_ep_dur = NULL,
                        imdb_studio = ?,
                        imdb_genre = ?,
                        imdb_nw_rat = ?,
                        imdb_cast = ?,
                        imdb_src_link = ?,
                        imdb_sea_avl = ?,
                        imdb_ep_avl = ?
                    WHERE it_checksum = ?
                """
                
                values = (
                    omdb_data.get('imdb_id'),
                    omdb_data.get('imdb_series'),
                    omdb_data.get('imdb_series_desc'),
                    omdb_data.get('imdb_sea_no'),
                    omdb_data.get('imdb_sea_yr'),
                    omdb_data.get('imdb_ep_no'),
                    omdb_data.get('imdb_ep_title'),
                    omdb_data.get('imdb_ep_desc'),
                    omdb_data.get('imdb_air'),
                    omdb_data.get('imdb_studio'),
                    omdb_data.get('imdb_genre'),
                    omdb_data.get('imdb_nw_rat'),
                    omdb_data.get('imdb_cast'),
                    omdb_data.get('imdb_src_link'),
                    omdb_data.get('imdb_sea_avl'),
                    omdb_data.get('imdb_ep_avl'),
                    checksum
                )
                
                cursor = conn.execute(update_query, values)
                if cursor.rowcount == 0:
                    logging.warning(f"No record found with checksum: {checksum}")
                else:
                    logging.debug(f"Updated record {checksum} with OMDB data")
                conn.commit()
                
        except sqlite3.Error as e:
            logging.error(f"Failed to update record {checksum}: {e}")
            raise
    
    def process_records(self):
        """Main processing method to fetch and update all pending records."""
        try:
            records = self._fetch_pending_records()
            
            if not records:
                print("No records to process")
                logging.info("No pending records found")
                return
            
            total_records = len(records)
            processed = 0
            errors = 0
            skipped = 0
            
            print(f"Processing {total_records} records...")
            
            for record in records:
                try:
                    checksum = record[0]  # it_checksum
                    series_name = record[1]  # it_series
                    season_no = record[2]  # it_sea_no
                    episode_no = record[3]  # it_ep_no
                    
                    logging.info(f"Processing: {series_name} S{season_no}E{episode_no}")
                    print(f"Processing: {series_name} S{season_no}E{episode_no} ({processed + skipped + 1}/{total_records})")
                    
                    # Step 1: Search for series IMDb ID
                    imdb_id = self._search_series(series_name)
                    if not imdb_id:
                        logging.warning(f"No IMDb ID found for series: {series_name}")
                        # Still update record to mark as processed (with NULL imdb_id)
                        omdb_data = {
                            'imdb_id': None,
                            'imdb_series': None,
                            'imdb_series_desc': None,
                            'imdb_sea_no': season_no,
                            'imdb_sea_yr': None,
                            'imdb_ep_no': episode_no,
                            'imdb_ep_title': None,
                            'imdb_ep_desc': None,
                            'imdb_air': None,
                            'imdb_studio': None,
                            'imdb_genre': None,
                            'imdb_nw_rat': None,
                            'imdb_cast': None,
                            'imdb_src_link': None,
                            'imdb_sea_avl': None,
                            'imdb_ep_avl': None
                        }
                        self._update_record(checksum, omdb_data)
                        skipped += 1
                        continue
                    
                    # Step 2: Fetch series data
                    series_data = self._get_series_data(imdb_id)
                    
                    # Step 3: Fetch season data
                    season_data = self._get_season_data(imdb_id, season_no)
                    
                    # Step 4: Fetch episode data
                    episode_data = self._get_episode_data(imdb_id, season_no, episode_no)
                    
                    # Combine all data
                    omdb_data = {
                        'imdb_id': imdb_id,
                        **series_data,
                        **season_data,
                        **episode_data
                    }
                    
                    # Step 5: Update database record
                    self._update_record(checksum, omdb_data)
                    
                    processed += 1
                    logging.info(f"Successfully processed: {series_name} S{season_no}E{episode_no}")
                    
                except Exception as e:
                    errors += 1
                    logging.error(f"Error processing record {record[0] if len(record) > 0 else 'unknown'}: {e}")
                    print(f"Error processing record: {e}")
                    # Continue processing other records instead of stopping
                    continue
            
            # Final summary
            print(f"\nProcessing complete:")
            print(f"  Total records: {total_records}")
            print(f"  Successfully processed: {processed}")
            print(f"  Skipped (no IMDb match): {skipped}")
            print(f"  Errors: {errors}")
            
            logging.info(f"Processing complete: {processed}/{total_records} successful, {skipped} skipped, {errors} errors")
            
        except Exception as e:
            logging.error(f"Fatal error during processing: {e}")
            print(f"Fatal error: {e}")
            sys.exit(1)

def main():
    """Main entry point."""
    try:
        processor = HectorOMDBProcessor()
        processor.process_records()
        print("Hector OMDB processing completed successfully")
        
    except KeyboardInterrupt:
        print("\nProcessing interrupted by user")
        logging.info("Processing interrupted by user")
        sys.exit(130)
    except Exception as e:
        print(f"Fatal error: {e}")
        logging.error(f"Fatal error in main: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()