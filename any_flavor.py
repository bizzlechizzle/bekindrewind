#!/usr/bin/env python3

import sqlite3
import json
import logging
import os
import requests
import time
import sys
import hashlib
from typing import Optional, Dict, Any, List, Tuple
from pathlib import Path
import re

class TMDBProcessor:
    def __init__(self):
        """Initialize with strict validation and error handling"""
        self.config = None
        self.logger = None
        self.api_key = None
        self.base_url = "https://api.themoviedb.org/3"
        self.session = None
        self.rate_limit_delay = 0.26  # Just over 4 requests per second to be extra safe
        self.max_retries = 3
        self.request_count = 0
        self.start_time = time.time()
        
        # Initialize everything with proper error handling
        if not self._initialize():
            sys.exit(1)
    
    def _initialize(self) -> bool:
        """Initialize all components with bulletproof error handling"""
        try:
            # Step 1: Load and validate config
            if not self._load_and_validate_config():
                return False
            
            # Setup logging AFTER config is loaded
            if not self._setup_logging():
                return False
            
            # Validate API key
            if not self._validate_api_key():
                return False
            
            # Setup session
            self._setup_session()
            
            self.logger.info("TMDBProcessor initialized successfully")
            return True
            
        except Exception as e:
            print(f"FATAL: Initialization failed: {e}")
            return False
    
    def _load_and_validate_config(self) -> bool:
        """Load and validate 2jznoshit.json with bulletproof checks"""
        config_file = Path('2jznoshit.json')
        
        # Check if file exists
        if not config_file.exists():
            print("ERROR: 2jznoshit.json not found in current directory")
            return False
        
        # Check if file is readable
        if not os.access(config_file, os.R_OK):
            print("ERROR: 2jznoshit.json is not readable")
            return False
        
        try:
            with open(config_file, 'r', encoding='utf-8') as f:
                self.config = json.load(f)
        except json.JSONDecodeError as e:
            print(f"ERROR: Invalid JSON in 2jznoshit.json: {e}")
            return False
        except Exception as e:
            print(f"ERROR: Failed to read 2jznoshit.json: {e}")
            return False
        
        # Validate config structure
        if not isinstance(self.config, dict):
            print("ERROR: 2jznoshit.json must contain a JSON object")
            return False
        
        # Validate any_flavor section exists
        if 'any_flavor' not in self.config:
            print("ERROR: 'any_flavor' section missing from 2jznoshit.json")
            return False
        
        # Validate api_keys section
        if 'api_keys' not in self.config:
            print("ERROR: 'api_keys' section missing from 2jznoshit.json")
            return False
        
        if 'tmdb' not in self.config['api_keys']:
            print("ERROR: 'tmdb' key missing from api_keys section")
            return False
        
        self.api_key = self.config['api_keys']['tmdb']
        
        # Validate API key format (basic check)
        if not self.api_key or not isinstance(self.api_key, str) or len(self.api_key) < 10:
            print("ERROR: Invalid TMDB API key format")
            return False
        
        return True
    
    def _setup_logging(self) -> bool:
        """Setup logging based on config with bulletproof error handling"""
        try:
            any_flavor_config = self.config.get('any_flavor', {})
            log_enabled = any_flavor_config.get('logs', False)
            
            if log_enabled:
                # Ensure we can write to log file
                log_file = Path('any_flavor.log')
                try:
                    # Test write access
                    with open(log_file, 'a', encoding='utf-8') as f:
                        f.write(f"# Log test at {time.asctime()}\n")
                except Exception as e:
                    print(f"ERROR: Cannot write to any_flavor.log: {e}")
                    return False
                
                logging.basicConfig(
                    filename='any_flavor.log',
                    level=logging.DEBUG,  # More verbose for bulletproofing
                    format='%(asctime)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s',
                    filemode='a',
                    encoding='utf-8'
                )
            else:
                # Still setup logging to console for errors
                logging.basicConfig(
                    level=logging.ERROR,
                    format='%(asctime)s - %(levelname)s - %(message)s'
                )
            
            self.logger = logging.getLogger(__name__)
            self.logger.info("="*50)
            self.logger.info("Starting new any_flavor.py session")
            self.logger.info(f"Logging enabled: {log_enabled}")
            self.logger.info(f"JSON output enabled: {any_flavor_config.get('json', False)}")
            
            return True
            
        except Exception as e:
            print(f"ERROR: Failed to setup logging: {e}")
            return False
    
    def _validate_api_key(self) -> bool:
        """Validate TMDB API key by making a test request"""
        try:
            test_url = f"{self.base_url}/configuration"
            response = requests.get(test_url, params={'api_key': self.api_key}, timeout=10)
            
            if response.status_code == 401:
                print("ERROR: Invalid TMDB API key")
                return False
            elif response.status_code == 200:
                self.logger.info("TMDB API key validated successfully")
                return True
            else:
                print(f"ERROR: TMDB API test failed with status {response.status_code}")
                return False
                
        except Exception as e:
            print(f"ERROR: Failed to validate API key: {e}")
            return False
    
    def _setup_session(self):
        """Setup requests session with proper headers and timeouts"""
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'any_flavor.py/1.0 (TMDB API Client)',
            'Accept': 'application/json',
            'Content-Type': 'application/json'
        })
        
        # Setup adapters with retry strategy
        from requests.adapters import HTTPAdapter
        from urllib3.util.retry import Retry
        
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
    
    def _validate_database_access(self) -> bool:
        """Validate database exists and is accessible"""
        db_file = Path('danger2manifold.db')
        
        if not db_file.exists():
            self.logger.error("danger2manifold.db not found")
            return False
        
        if not os.access(db_file, os.R_OK | os.W_OK):
            self.logger.error("danger2manifold.db is not readable/writable")
            return False
        
        try:
            # Test connection and table existence
            with sqlite3.connect(db_file) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='any_flavor'")
                if not cursor.fetchone():
                    self.logger.error("any_flavor table not found in database")
                    return False
                
                # Check table structure
                cursor.execute("PRAGMA table_info(any_flavor)")
                columns = {row[1] for row in cursor.fetchall()}
                
                required_columns = {
                    'it_checksum', 'it_series', 'it_sea_no', 'it_ep_no', 'tmdb_id',
                    'tmdb_series', 'tmdb_series_desc', 'tmdb_sea_desc', 'tmdb_sea_yr',
                    'tmdb_ep_title', 'tmdb_ep_desc', 'tmdb_air', 'tmdb_ep_dur',
                    'tmdb_studio', 'tmdb_genre', 'tmdb_nw_rat', 'tmdb_cast',
                    'tmdb_src_link', 'tmdb_ep_img', 'tmdb_sea_img', 'tmdb_series_img',
                    'tmdb_sea_avl', 'tmdb_ep_avl'
                }
                
                missing_columns = required_columns - columns
                if missing_columns:
                    self.logger.error(f"Missing columns in any_flavor table: {missing_columns}")
                    return False
                
            self.logger.info("Database validation successful")
            return True
            
        except sqlite3.Error as e:
            self.logger.error(f"Database validation failed: {e}")
            return False
    
    def connect_database(self) -> Optional[sqlite3.Connection]:
        """Connect to database with bulletproof error handling"""
        if not self._validate_database_access():
            return None
        
        try:
            conn = sqlite3.connect("danger2manifold.db", timeout=30.0)
            conn.row_factory = sqlite3.Row
            
            # Enable WAL mode for better concurrency
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            conn.execute("PRAGMA cache_size=10000")
            conn.execute("PRAGMA temp_store=MEMORY")
            
            return conn
            
        except sqlite3.Error as e:
            self.logger.error(f"Database connection failed: {e}")
            return None
    
    def get_pending_records(self, conn: sqlite3.Connection) -> List[Dict[str, Any]]:
        """Get records needing TMDB data with bulletproof validation"""
        try:
            cursor = conn.cursor()
            
            # First, check if table has any data at all
            cursor.execute("SELECT COUNT(*) FROM any_flavor")
            total_count = cursor.fetchone()[0]
            
            if total_count == 0:
                self.logger.warning("any_flavor table is empty")
                return []
            
            # Get records that need processing
            query = """
                SELECT it_checksum, it_series, it_sea_no, it_ep_no, tmdb_id
                FROM any_flavor 
                WHERE (tmdb_id IS NULL OR tmdb_series IS NULL)
                AND it_checksum IS NOT NULL 
                AND it_series IS NOT NULL
                AND TRIM(it_series) != ''
                ORDER BY it_series, 
                    CAST(SUBSTR(it_sea_no, INSTR(it_sea_no, ' ') + 1) AS INTEGER),
                    CAST(SUBSTR(it_ep_no, INSTR(it_ep_no, ' ') + 1) AS INTEGER)
            """
            
            cursor.execute(query)
            rows = cursor.fetchall()
            
            if not rows:
                self.logger.info("No pending records found (all records may already be processed)")
                return []
            
            # Validate each record
            valid_records = []
            for row in rows:
                record = {
                    'it_checksum': row['it_checksum'],
                    'it_series': row['it_series'],
                    'it_sea_no': row['it_sea_no'],
                    'it_ep_no': row['it_ep_no'],
                    'tmdb_id': row['tmdb_id']
                }
                
                # Validate checksum format (SHA256)
                if not re.match(r'^[a-f0-9]{64}$', record['it_checksum']):
                    self.logger.warning(f"Invalid checksum format: {record['it_checksum']}")
                    continue
                
                # Validate series name
                if not record['it_series'] or len(record['it_series'].strip()) == 0:
                    self.logger.warning(f"Empty series name for checksum: {record['it_checksum']}")
                    continue
                
                valid_records.append(record)
            
            self.logger.info(f"Found {len(valid_records)} valid records to process out of {len(rows)} total")
            return valid_records
            
        except sqlite3.Error as e:
            self.logger.error(f"Failed to get pending records: {e}")
            return []
        except Exception as e:
            self.logger.error(f"Unexpected error getting pending records: {e}")
            return []
    
    def extract_season_episode_numbers(self, season_str: str, episode_str: str) -> Tuple[Optional[int], Optional[int]]:
        """Extract numeric season and episode numbers with bulletproof parsing"""
        season_num = None
        episode_num = None
        
        try:
            # Extract season number with multiple patterns
            if season_str:
                season_patterns = [
                    r'season\s+(\d+)',  # "season 01"
                    r's(\d+)',          # "s01"
                    r'(\d+)',           # just number
                ]
                
                for pattern in season_patterns:
                    match = re.search(pattern, season_str.lower())
                    if match:
                        season_num = int(match.group(1))
                        break
            
            # Extract episode number with multiple patterns
            if episode_str:
                episode_patterns = [
                    r'episode\s+(\d+)',  # "episode 01"
                    r'ep\s+(\d+)',       # "ep 01"
                    r'e(\d+)',           # "e01"
                    r'(\d+)',            # just number
                ]
                
                for pattern in episode_patterns:
                    match = re.search(pattern, episode_str.lower())
                    if match:
                        episode_num = int(match.group(1))
                        break
            
            # Validate ranges
            if season_num is not None and (season_num < 1 or season_num > 200):
                self.logger.warning(f"Season number out of range: {season_num}")
                season_num = None
            
            if episode_num is not None and (episode_num < 1 or episode_num > 1000):
                self.logger.warning(f"Episode number out of range: {episode_num}")
                episode_num = None
            
            return season_num, episode_num
            
        except Exception as e:
            self.logger.error(f"Error parsing season/episode: {e}")
            return None, None
    
    def make_api_request(self, endpoint: str, params: Dict[str, Any] = None) -> Optional[Dict[str, Any]]:
        """Make TMDB API request with bulletproof error handling and rate limiting"""
        if params is None:
            params = {}
        
        params['api_key'] = self.api_key
        url = f"{self.base_url}/{endpoint}"
        
        # Rate limiting with request counting
        self.request_count += 1
        elapsed = time.time() - self.start_time
        
        # If we're making too many requests per second, slow down
        if self.request_count > 1:
            expected_time = self.request_count * self.rate_limit_delay
            if elapsed < expected_time:
                sleep_time = expected_time - elapsed
                time.sleep(sleep_time)
        
        for attempt in range(self.max_retries):
            try:
                self.logger.debug(f"API Request {self.request_count}: {endpoint} (attempt {attempt + 1})")
                
                response = self.session.get(url, params=params, timeout=15)
                
                # Log response details
                self.logger.debug(f"Response: {response.status_code} for {endpoint}")
                
                if response.status_code == 200:
                    try:
                        data = response.json()
                        self.logger.debug(f"Successful API call to {endpoint}")
                        return data
                    except json.JSONDecodeError as e:
                        self.logger.error(f"Invalid JSON response from {endpoint}: {e}")
                        return None
                        
                elif response.status_code == 404:
                    self.logger.warning(f"Not found (404): {endpoint}")
                    return None
                    
                elif response.status_code == 401:
                    self.logger.error(f"Unauthorized (401): Check API key for {endpoint}")
                    return None
                    
                elif response.status_code == 429:
                    # Rate limited
                    retry_after = int(response.headers.get('Retry-After', 10))
                    self.logger.warning(f"Rate limited (429), waiting {retry_after} seconds")
                    time.sleep(retry_after)
                    continue
                    
                elif response.status_code >= 500:
                    # Server error, retry
                    wait_time = (2 ** attempt) + 1
                    self.logger.warning(f"Server error ({response.status_code}), retrying in {wait_time}s")
                    time.sleep(wait_time)
                    continue
                    
                else:
                    self.logger.error(f"API error {response.status_code}: {endpoint}")
                    if attempt == self.max_retries - 1:
                        return None
                    
            except requests.Timeout:
                self.logger.warning(f"Timeout for {endpoint} (attempt {attempt + 1})")
                if attempt < self.max_retries - 1:
                    time.sleep(2 ** attempt)
            except requests.ConnectionError as e:
                self.logger.warning(f"Connection error for {endpoint}: {e}")
                if attempt < self.max_retries - 1:
                    time.sleep(2 ** attempt)
            except Exception as e:
                self.logger.error(f"Unexpected error for {endpoint}: {e}")
                return None
        
        self.logger.error(f"All retry attempts failed for {endpoint}")
        return None
    
    def search_tv_series(self, series_name: str) -> Optional[Dict[str, Any]]:
        """Search for TV series with improved matching"""
        if not series_name or len(series_name.strip()) == 0:
            return None
        
        # Clean series name for better matching
        clean_name = series_name.strip()
        
        # Remove common prefixes/suffixes that might interfere
        clean_name = re.sub(r'\s*\(.*?\)\s*$', '', clean_name)  # Remove parentheses at end
        clean_name = re.sub(r'\s*(US|UK|AU)\s*$', '', clean_name, flags=re.IGNORECASE)  # Remove country codes
        
        params = {
            'query': clean_name,
            'include_adult': 'false',
            'first_air_date_year': ''  # Don't restrict by year initially
        }
        
        data = self.make_api_request('search/tv', params)
        
        if data and data.get('results'):
            results = data['results']
            
            # Try exact match first
            for result in results:
                if result.get('name', '').lower() == clean_name.lower():
                    self.logger.info(f"Exact match found for '{series_name}': {result.get('name')}")
                    return result
            
            # Return first result if no exact match
            self.logger.info(f"Using first result for '{series_name}': {results[0].get('name')}")
            return results[0]
        
        # Try alternative search with original name
        if clean_name != series_name:
            params['query'] = series_name
            data = self.make_api_request('search/tv', params)
            
            if data and data.get('results'):
                self.logger.info(f"Found result with original name '{series_name}'")
                return data['results'][0]
        
        self.logger.warning(f"No TMDB results found for: {series_name}")
        return None
    
    def get_tv_series_details(self, tmdb_id: int) -> Optional[Dict[str, Any]]:
        """Get TV series details with comprehensive data"""
        if not isinstance(tmdb_id, int) or tmdb_id <= 0:
            self.logger.error(f"Invalid TMDB ID: {tmdb_id}")
            return None
        
        endpoint = f"tv/{tmdb_id}"
        params = {
            'append_to_response': 'credits,content_ratings,external_ids,keywords'
        }
        
        return self.make_api_request(endpoint, params)
    
    def get_season_details(self, tmdb_id: int, season_number: int) -> Optional[Dict[str, Any]]:
        """Get season details with validation"""
        if not isinstance(tmdb_id, int) or tmdb_id <= 0:
            return None
        
        if not isinstance(season_number, int) or season_number < 0:
            return None
        
        endpoint = f"tv/{tmdb_id}/season/{season_number}"
        return self.make_api_request(endpoint)
    
    def get_episode_details(self, tmdb_id: int, season_number: int, episode_number: int) -> Optional[Dict[str, Any]]:
        """Get episode details with validation"""
        if not isinstance(tmdb_id, int) or tmdb_id <= 0:
            return None
        
        if not isinstance(season_number, int) or season_number < 0:
            return None
        
        if not isinstance(episode_number, int) or episode_number < 1:
            return None
        
        endpoint = f"tv/{tmdb_id}/season/{season_number}/episode/{episode_number}"
        params = {
            'append_to_response': 'credits,images'
        }
        
        return self.make_api_request(endpoint, params)
    
    def extract_content_rating(self, content_ratings_data: Dict[str, Any]) -> Optional[str]:
        """Extract content rating with proper validation"""
        try:
            if not content_ratings_data or 'results' not in content_ratings_data:
                return None
            
            ratings = content_ratings_data['results']
            if not isinstance(ratings, list):
                return None
            
            # Priority order for ratings
            priority_countries = ['US', 'GB', 'CA', 'AU']
            
            for country in priority_countries:
                for rating in ratings:
                    if (isinstance(rating, dict) and 
                        rating.get('iso_3166_1') == country and 
                        rating.get('rating')):
                        return rating['rating']
            
            # If no priority country found, return first available
            for rating in ratings:
                if isinstance(rating, dict) and rating.get('rating'):
                    return rating['rating']
            
            return None
            
        except Exception as e:
            self.logger.error(f"Error extracting content rating: {e}")
            return None
    
    def extract_cast(self, credits: Dict[str, Any], limit: int = 5) -> Optional[str]:
        """Extract cast with bulletproof validation"""
        try:
            if not credits or not isinstance(credits, dict):
                return None
            
            cast_list = credits.get('cast', [])
            if not isinstance(cast_list, list):
                return None
            
            cast_names = []
            for i, actor in enumerate(cast_list[:limit]):
                if isinstance(actor, dict) and actor.get('name'):
                    name = str(actor['name']).strip()
                    if name and len(name) > 0:
                        cast_names.append(name)
            
            return ', '.join(cast_names) if cast_names else None
            
        except Exception as e:
            self.logger.error(f"Error extracting cast: {e}")
            return None
    
    def extract_genres(self, genres: List[Dict[str, str]]) -> Optional[str]:
        """Extract genres with validation"""
        try:
            if not genres or not isinstance(genres, list):
                return None
            
            genre_names = []
            for genre in genres:
                if isinstance(genre, dict) and genre.get('name'):
                    name = str(genre['name']).strip()
                    if name and len(name) > 0:
                        genre_names.append(name)
            
            return ', '.join(genre_names) if genre_names else None
            
        except Exception as e:
            self.logger.error(f"Error extracting genres: {e}")
            return None
    
    def extract_production_company(self, production_companies: List[Dict[str, Any]]) -> Optional[str]:
        """Extract production company with validation"""
        try:
            if not production_companies or not isinstance(production_companies, list):
                return None
            
            for company in production_companies:
                if isinstance(company, dict) and company.get('name'):
                    name = str(company['name']).strip()
                    if name and len(name) > 0:
                        return name
            
            return None
            
        except Exception as e:
            self.logger.error(f"Error extracting production company: {e}")
            return None
    
    def safe_str_conversion(self, value: Any) -> Optional[str]:
        """Safely convert value to string"""
        if value is None:
            return None
        
        if isinstance(value, str):
            return value.strip() if value.strip() else None
        
        try:
            str_val = str(value).strip()
            return str_val if str_val else None
        except:
            return None
    
    def process_record(self, record: Dict[str, Any], conn: sqlite3.Connection) -> bool:
        """Process single record with bulletproof error handling"""
        checksum = record['it_checksum']
        
        try:
            series_name = record['it_series']
            season_str = record['it_sea_no']
            episode_str = record['it_ep_no']
            existing_tmdb_id = record['tmdb_id']
            
            self.logger.info(f"Processing: {series_name} - {season_str} - {episode_str} (checksum: {checksum[:8]}...)")
            
            # Extract numeric season and episode numbers
            season_num, episode_num = self.extract_season_episode_numbers(season_str, episode_str)
            self.logger.debug(f"Parsed season: {season_num}, episode: {episode_num}")
            
            # Get TMDB ID
            tmdb_id = existing_tmdb_id
            if not tmdb_id:
                search_result = self.search_tv_series(series_name)
                if not search_result:
                    self.logger.warning(f"No TMDB results for: {series_name}")
                    return False
                
                tmdb_id = search_result.get('id')
                if not tmdb_id:
                    self.logger.warning(f"No TMDB ID in search result for: {series_name}")
                    return False
            
            # Validate tmdb_id
            try:
                tmdb_id = int(tmdb_id)
            except (ValueError, TypeError):
                self.logger.error(f"Invalid TMDB ID: {tmdb_id}")
                return False
            
            # Get series details
            series_data = self.get_tv_series_details(tmdb_id)
            if not series_data:
                self.logger.warning(f"Failed to get series details for TMDB ID: {tmdb_id}")
                return False
            
            # Get season details
            season_data = None
            if season_num:
                season_data = self.get_season_details(tmdb_id, season_num)
                if season_data:
                    self.logger.debug(f"Retrieved season {season_num} data")
                else:
                    self.logger.warning(f"No season {season_num} data for TMDB ID: {tmdb_id}")
            
            # Get episode details
            episode_data = None
            if season_num and episode_num:
                episode_data = self.get_episode_details(tmdb_id, season_num, episode_num)
                if episode_data:
                    self.logger.debug(f"Retrieved episode {season_num}x{episode_num} data")
                else:
                    self.logger.warning(f"No episode {season_num}x{episode_num} data for TMDB ID: {tmdb_id}")
            
            # Build update data with safe extraction
            update_data = {
                'tmdb_id': self.safe_str_conversion(tmdb_id),
                'tmdb_series': self.safe_str_conversion(series_data.get('name')),
                'tmdb_series_desc': self.safe_str_conversion(series_data.get('overview')),
                'tmdb_sea_no': self.safe_str_conversion(season_str),
                'tmdb_sea_desc': self.safe_str_conversion(season_data.get('overview') if season_data else None),
                'tmdb_sea_yr': None,
                'tmdb_ep_no': self.safe_str_conversion(episode_str),
                'tmdb_ep_title': self.safe_str_conversion(episode_data.get('name') if episode_data else None),
                'tmdb_ep_desc': self.safe_str_conversion(episode_data.get('overview') if episode_data else None),
                'tmdb_air': self.safe_str_conversion(episode_data.get('air_date') if episode_data else None),
                'tmdb_ep_dur': None,
                'tmdb_studio': self.extract_production_company(series_data.get('production_companies', [])),
                'tmdb_genre': self.extract_genres(series_data.get('genres', [])),
                'tmdb_nw_rat': self.extract_content_rating(series_data.get('content_ratings', {})),
                'tmdb_cast': None,
                'tmdb_src_link': f"https://www.themoviedb.org/tv/{tmdb_id}",
                'tmdb_ep_img': self.safe_str_conversion(episode_data.get('still_path') if episode_data else None),
                'tmdb_sea_img': self.safe_str_conversion(season_data.get('poster_path') if season_data else None),
                'tmdb_series_img': self.safe_str_conversion(series_data.get('poster_path') or series_data.get('backdrop_path')),
                'tmdb_sea_avl': self.safe_str_conversion(series_data.get('number_of_seasons')),
                'tmdb_ep_avl': self.safe_str_conversion(season_data.get('episode_count') if season_data else None)
            }
            
            # Extract season year safely
            if season_data and season_data.get('air_date'):
                try:
                    air_date = season_data['air_date']
                    if isinstance(air_date, str) and len(air_date) >= 4:
                        year = air_date.split('-')[0]
                        if year.isdigit() and len(year) == 4:
                            update_data['tmdb_sea_yr'] = year
                except Exception as e:
                    self.logger.debug(f"Could not extract season year: {e}")
            
            # Extract episode duration safely
            if episode_data and episode_data.get('runtime'):
                try:
                    runtime = episode_data['runtime']
                    if isinstance(runtime, (int, float)) and runtime > 0:
                        update_data['tmdb_ep_dur'] = str(int(runtime))
                except Exception as e:
                    self.logger.debug(f"Could not extract episode duration: {e}")
            
            # Extract cast safely
            cast_source = None
            if episode_data and episode_data.get('credits'):
                cast_source = episode_data['credits']
            elif series_data and series_data.get('credits'):
                cast_source = series_data['credits']
            
            if cast_source:
                update_data['tmdb_cast'] = self.extract_cast(cast_source)
            
            # Update database with transaction
            try:
                cursor = conn.cursor()
                
                # Check if record still exists (prevent race conditions)
                cursor.execute("SELECT it_checksum FROM any_flavor WHERE it_checksum = ?", (checksum,))
                if not cursor.fetchone():
                    self.logger.warning(f"Record with checksum {checksum} no longer exists")
                    return False
                
                # Build update query dynamically (only update non-null values)
                set_clauses = []
                values = []
                
                for key, value in update_data.items():
                    if value is not None:  # Only update fields we have data for
                        set_clauses.append(f"{key} = ?")
                        values.append(value)
                
                if not set_clauses:
                    self.logger.warning(f"No data to update for checksum {checksum}")
                    return False
                
                values.append(checksum)  # For WHERE clause
                
                update_query = f"""
                    UPDATE any_flavor 
                    SET {', '.join(set_clauses)}
                    WHERE it_checksum = ?
                """
                
                cursor.execute(update_query, values)
                
                if cursor.rowcount == 0:
                    self.logger.warning(f"No rows updated for checksum {checksum}")
                    return False
                
                conn.commit()
                
                self.logger.info(f"Successfully updated {len(set_clauses)} fields for {series_name}")
                return True
                
            except sqlite3.Error as e:
                self.logger.error(f"Database error updating {checksum}: {e}")
                conn.rollback()
                return False
            
        except Exception as e:
            self.logger.error(f"Error processing record {checksum}: {e}")
            return False
    
    def create_progress_summary(self, total: int, processed: int, successful: int, failed: int) -> str:
        """Create detailed progress summary"""
        success_rate = (successful / processed * 100) if processed > 0 else 0
        remaining = total - processed
        
        summary = f"""
=== PROCESSING SUMMARY ===
Total records found: {total}
Processed: {processed}
Successful: {successful}
Failed: {failed}
Success rate: {success_rate:.1f}%
Remaining: {remaining}
API requests made: {self.request_count}
        """.strip()
        
        return summary
    
    def run(self) -> bool:
        """Main execution with comprehensive error handling"""
        self.logger.info("Starting TMDB data collection for any_flavor table")
        
        try:
            # Connect to database
            conn = self.connect_database()
            if not conn:
                print("ERROR: Failed to connect to database")
                return False
            
            # Get pending records
            records = self.get_pending_records(conn)
            
            if not records:
                message = "No pending records found in any_flavor table"
                print(message)
                self.logger.info(message)
                return True
            
            total_records = len(records)
            print(f"Found {total_records} records to process")
            self.logger.info(f"Processing {total_records} records")
            
            # Process records
            successful = 0
            failed = 0
            results = []
            
            # Progress tracking
            last_progress_report = 0
            progress_interval = max(1, total_records // 20)  # Report every 5%
            
            for i, record in enumerate(records, 1):
                series_name = record['it_series']
                checksum_short = record['it_checksum'][:8]
                
                # Progress reporting
                if i - last_progress_report >= progress_interval or i == total_records:
                    progress = (i / total_records) * 100
                    print(f"Progress: {progress:.1f}% ({i}/{total_records}) - Processing: {series_name}")
                    last_progress_report = i
                
                self.logger.debug(f"Starting record {i}/{total_records}: {series_name}")
                
                # Process the record
                if self.process_record(record, conn):
                    successful += 1
                    status = 'success'
                    self.logger.debug(f"Successfully processed {checksum_short}")
                else:
                    failed += 1
                    status = 'failed'
                    self.logger.debug(f"Failed to process {checksum_short}")
                
                # Collect results for JSON output
                if self.config.get('any_flavor', {}).get('json', False):
                    results.append({
                        'checksum': record['it_checksum'],
                        'series': series_name,
                        'season': record['it_sea_no'],
                        'episode': record['it_ep_no'],
                        'status': status,
                        'timestamp': time.strftime('%Y-%m-%d %H:%M:%S')
                    })
                
                # Safety check - if too many consecutive failures, pause
                if failed > 0 and i > 10:
                    recent_failure_rate = failed / i
                    if recent_failure_rate > 0.8:  # More than 80% failure rate
                        self.logger.warning(f"High failure rate detected ({recent_failure_rate:.1%}), continuing with caution")
            
            # Generate final summary
            summary = self.create_progress_summary(total_records, i, successful, failed)
            print(summary)
            self.logger.info(summary.replace('\n', ' | '))
            
            # Output JSON results if requested
            json_enabled = self.config.get('any_flavor', {}).get('json', False)
            if json_enabled and results:
                try:
                    json_file = Path('any_flavor_results.json')
                    with open(json_file, 'w', encoding='utf-8') as f:
                        json.dump({
                            'summary': {
                                'total_records': total_records,
                                'successful': successful,
                                'failed': failed,
                                'success_rate': f"{(successful/total_records*100):.1f}%" if total_records > 0 else "0%",
                                'api_requests': self.request_count,
                                'processing_time': f"{time.time() - self.start_time:.1f}s"
                            },
                            'results': results
                        }, f, indent=2, ensure_ascii=False)
                    
                    print(f"Results saved to {json_file}")
                    self.logger.info(f"JSON results saved to {json_file}")
                    
                except Exception as e:
                    self.logger.error(f"Failed to save JSON results: {e}")
            
            # Determine success
            success_threshold = 0.7  # At least 70% success rate
            overall_success = (successful / total_records) >= success_threshold if total_records > 0 else False
            
            if overall_success:
                self.logger.info("Processing completed successfully")
                return True
            else:
                self.logger.warning(f"Processing completed with low success rate: {successful}/{total_records}")
                return False
            
        except KeyboardInterrupt:
            print("\n\nProcessing interrupted by user")
            self.logger.warning("Processing interrupted by user")
            return False
            
        except Exception as e:
            error_msg = f"Fatal error during processing: {e}"
            print(f"ERROR: {error_msg}")
            self.logger.error(error_msg)
            return False
            
        finally:
            if 'conn' in locals() and conn:
                try:
                    conn.close()
                    self.logger.debug("Database connection closed")
                except Exception as e:
                    self.logger.error(f"Error closing database: {e}")
            
            # Final cleanup
            if hasattr(self, 'session') and self.session:
                self.session.close()

def main():
    """Main entry point with comprehensive error handling"""
    try:
        print("=== any_flavor.py - TMDB API Data Processor ===")
        print("Initializing...")
        
        processor = TMDBProcessor()
        
        print("Starting processing...")
        success = processor.run()
        
        if success:
            print("\n✅ Processing completed successfully!")
            sys.exit(0)
        else:
            print("\n❌ Processing completed with errors")
            sys.exit(1)
            
    except KeyboardInterrupt:
        print("\n\n⏹️  Processing interrupted by user")
        sys.exit(130)  # Standard exit code for Ctrl+C
        
    except Exception as e:
        print(f"\n💥 Fatal error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()