#!/usr/bin/env python3
"""
oil_change.py - Media file organization and copying script
Bulletproof version with comprehensive error handling and validation
"""

import json
import sqlite3
import os
import sys
import shutil
import subprocess
import logging
import threading
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Tuple, Optional, Set
import hashlib
import time
import platform
import re
from contextlib import contextmanager

class MediaOrganizer:
    def __init__(self):
        self.config = None
        self.db_file = 'danger2manifold.db'
        self.config_file = '2jznoshit.json'
        self.logger = self._setup_basic_logger()
        self.processed_files: Set[str] = set()
        self.db_lock = threading.Lock()
        self.processed_lock = threading.Lock()
        
    def _setup_basic_logger(self):
        """Setup a basic logger that's always available"""
        logger = logging.getLogger(__name__)
        if not logger.handlers:
            handler = logging.StreamHandler(sys.stdout)
            formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            logger.addHandler(handler)
            logger.setLevel(logging.INFO)
        return logger
        
    def setup_logging(self, enable_logs: bool) -> None:
        """Configure logging based on user preferences"""
        # Clear existing handlers
        for handler in self.logger.handlers[:]:
            self.logger.removeHandler(handler)
            
        if enable_logs:
            # Setup detailed logging to file and console
            log_file = f'oil_change_{int(time.time())}.log'
            file_handler = logging.FileHandler(log_file)
            console_handler = logging.StreamHandler(sys.stdout)
            
            formatter = logging.Formatter('%(asctime)s - %(threadName)s - %(levelname)s - %(message)s')
            file_handler.setFormatter(formatter)
            console_handler.setFormatter(formatter)
            
            self.logger.addHandler(file_handler)
            self.logger.addHandler(console_handler)
            self.logger.setLevel(logging.DEBUG)
            
            self.logger.info(f"Detailed logging enabled - log file: {log_file}")
        else:
            # Minimal console logging
            handler = logging.StreamHandler(sys.stdout)
            formatter = logging.Formatter('%(levelname)s: %(message)s')
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
            self.logger.setLevel(logging.INFO)
        
        # Log system info for debugging
        if enable_logs:
            self.logger.debug(f"System: {platform.system()} {platform.release()}")
            self.logger.debug(f"Python: {sys.version}")
            self.logger.debug(f"Current working directory: {os.getcwd()}")
    
    def load_config(self) -> Dict:
        """Load and validate user preferences from 2jznoshit.json"""
        if not os.path.exists(self.config_file):
            self.logger.error(f"Configuration file not found: {self.config_file}")
            sys.exit(1)
            
        try:
            with open(self.config_file, 'r', encoding='utf-8') as f:
                config = json.load(f)
            
            # Validate required config sections
            required_sections = ['oil_change', 'user_input']
            for section in required_sections:
                if section not in config:
                    self.logger.error(f"Missing required section '{section}' in config")
                    sys.exit(1)
            
            if 'default' not in config['user_input']:
                self.logger.error("Missing 'user_input.default' section in config")
                sys.exit(1)
                
            default_config = config['user_input']['default']
            if 'def_loc' not in default_config or not default_config['def_loc']:
                self.logger.error("Missing or empty 'def_loc' in user_input.default")
                sys.exit(1)
            
            # Validate def_loc path
            def_loc = default_config['def_loc']
            if not os.path.isabs(def_loc):
                self.logger.error(f"def_loc must be an absolute path: {def_loc}")
                sys.exit(1)
            
            self.config = config
            self.logger.info(f"Configuration loaded successfully from {self.config_file}")
            self.logger.debug(f"def_loc: {def_loc}")
            self.logger.debug(f"logs enabled: {config.get('oil_change', {}).get('logs', False)}")
            
            return config
            
        except json.JSONDecodeError as e:
            self.logger.error(f"Invalid JSON in config file: {e}")
            sys.exit(1)
        except Exception as e:
            self.logger.error(f"Failed to load config: {e}")
            sys.exit(1)
    
    @contextmanager
    def get_db_connection(self):
        """Thread-safe database connection context manager"""
        if not os.path.exists(self.db_file):
            self.logger.error(f"Database file not found: {self.db_file}")
            raise FileNotFoundError(f"Database not found: {self.db_file}")
            
        conn = None
        try:
            with self.db_lock:
                conn = sqlite3.connect(self.db_file, timeout=30.0)
                conn.row_factory = sqlite3.Row
                
                # Verify table and columns exist
                cursor = conn.cursor()
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='import_tuner'")
                if not cursor.fetchone():
                    raise ValueError("Table 'import_tuner' not found in database")
                
                # Check required columns
                cursor.execute("PRAGMA table_info(import_tuner)")
                columns = [row[1] for row in cursor.fetchall()]
                required_columns = ['it_checksum', 'it_torrent', 'it_sea_no', 'it_ep_no', 
                                  'file_location', 'it_series', 'it_ep_title', 'it_def_loc']
                
                missing_columns = [col for col in required_columns if col not in columns]
                if missing_columns:
                    raise ValueError(f"Missing required columns: {missing_columns}")
                
                yield conn
                
        except sqlite3.Error as e:
            self.logger.error(f"Database error: {e}")
            raise
        finally:
            if conn:
                conn.close()
    
    def get_import_data(self) -> List[Dict]:
        """Fetch and validate all records from import_tuner table"""
        try:
            with self.get_db_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT it_checksum, it_torrent, it_sea_no, it_ep_no, 
                           file_location, it_series, it_ep_title, it_def_loc
                    FROM import_tuner
                    WHERE file_location IS NOT NULL 
                    AND it_checksum IS NOT NULL
                    AND it_series IS NOT NULL
                    AND TRIM(file_location) != ''
                    AND TRIM(it_checksum) != ''
                    AND TRIM(it_series) != ''
                """)
                
                records = []
                for row in cursor.fetchall():
                    # Convert to dict for easier handling
                    record = dict(row)
                    
                    # Validate and clean data
                    if not self._validate_record(record):
                        continue
                        
                    records.append(record)
                
                self.logger.info(f"Found {len(records)} valid records in import_tuner table")
                return records
                
        except Exception as e:
            self.logger.error(f"Failed to fetch import data: {e}")
            return []
    
    def _validate_record(self, record: Dict) -> bool:
        """Validate a single record has all required data"""
        required_fields = ['it_checksum', 'file_location', 'it_series']
        
        for field in required_fields:
            if not record.get(field) or not str(record[field]).strip():
                self.logger.warning(f"Invalid record - missing {field}: {record.get('it_checksum', 'unknown')}")
                return False
        
        # Validate torrent type
        torrent_type = str(record.get('it_torrent', 'season')).lower().strip()
        valid_types = ['series', 'season', 'episode', 'all']
        if torrent_type not in valid_types:
            self.logger.warning(f"Invalid torrent type '{torrent_type}', defaulting to 'season'")
            record['it_torrent'] = 'season'
        
        # Validate checksum format (should be 64 char hex)
        checksum = record['it_checksum'].strip()
        if not re.match(r'^[a-fA-F0-9]{64}$', checksum):
            self.logger.warning(f"Invalid checksum format: {checksum}")
            return False
        
        return True
    
    def normalize_folder_name(self, name: str) -> str:
        """Safely normalize folder names with proper error handling"""
        if not name:
            return "Unknown"
        
        # Clean the input
        name = str(name).strip()
        if not name:
            return "Unknown"
        
        # Remove/replace problematic characters for filesystem
        # Keep alphanumeric, spaces, hyphens, periods, and parentheses
        cleaned = re.sub(r'[<>:"/\\|?*]', '', name)
        cleaned = re.sub(r'\s+', ' ', cleaned).strip()
        
        if not cleaned:
            return "Unknown"
        
        # Handle season/episode normalization
        # Convert "season 1" -> "Season 01", "episode 9" -> "Episode 09"
        season_match = re.match(r'^season\s+(\d+)$', cleaned.lower())
        if season_match:
            season_num = int(season_match.group(1))
            return f"Season {season_num:02d}"
        
        episode_match = re.match(r'^episode\s+(\d+)$', cleaned.lower())
        if episode_match:
            episode_num = int(episode_match.group(1))
            return f"Episode {episode_num:02d}"
        
        # Title case for series names, but preserve certain words
        words = cleaned.split()
        result_words = []
        
        for word in words:
            # Preserve common abbreviations and Roman numerals
            if word.upper() in ['TV', 'DVD', 'HD', 'US', 'UK', 'II', 'III', 'IV', 'VI', 'VII', 'VIII', 'IX', 'XI']:
                result_words.append(word.upper())
            elif word.lower() in ['a', 'an', 'the', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for', 'of', 'with']:
                # Keep articles/prepositions lowercase unless they're the first word
                if not result_words:  # First word
                    result_words.append(word.capitalize())
                else:
                    result_words.append(word.lower())
            else:
                result_words.append(word.capitalize())
        
        result = ' '.join(result_words)
        
        # Final cleanup - remove any double spaces and trim
        result = re.sub(r'\s+', ' ', result).strip()
        
        return result or "Unknown"
    
    def create_folder_structure(self, record: Dict, def_loc: str) -> str:
        """Create appropriate folder structure based on torrent type"""
        try:
            torrent_type = str(record['it_torrent']).lower().strip()
            series = self.normalize_folder_name(record['it_series'])
            season = self.normalize_folder_name(record.get('it_sea_no', ''))
            episode = self.normalize_folder_name(record.get('it_ep_no', ''))
            
            base_path = Path(def_loc)
            
            self.logger.debug(f"Creating structure - Series: '{series}', Season: '{season}', Episode: '{episode}', Type: '{torrent_type}'")
            
            if torrent_type in ['series', 'all']:
                # Series/Season/Files structure
                if not season or season == "Unknown":
                    season = "Season 01"
                season_folder = f"{series} - {season}"
                target_path = base_path / series / season_folder
                
            elif torrent_type == 'season':
                # Season/Files structure (your Per Season Layout)
                if not season or season == "Unknown":
                    season = "Season 01"
                season_folder = f"{series} - {season}"
                target_path = base_path / season_folder
                
            elif torrent_type == 'episode':
                # Episode/Files structure (your Per Episode Layout)
                if not season or season == "Unknown":
                    season = "Season 01"
                if not episode or episode == "Unknown":
                    episode = "Episode 01"
                episode_folder = f"{series} - {season} - {episode}"
                target_path = base_path / episode_folder
                
            else:
                # Default to season structure
                self.logger.warning(f"Unknown torrent type: {torrent_type}, using season structure")
                if not season or season == "Unknown":
                    season = "Season 01"
                season_folder = f"{series} - {season}"
                target_path = base_path / season_folder
            
            # Ensure path length doesn't exceed filesystem limits
            if len(str(target_path)) > 250:  # Conservative limit
                self.logger.warning(f"Path too long, truncating: {target_path}")
                # Truncate series name if needed
                series_short = series[:50] + "..." if len(series) > 50 else series
                if torrent_type == 'episode':
                    episode_folder = f"{series_short} - {season} - {episode}"
                    target_path = base_path / episode_folder
                else:
                    season_folder = f"{series_short} - {season}"
                    target_path = base_path / season_folder
            
            # Create directory structure with proper error handling
            try:
                target_path.mkdir(parents=True, exist_ok=True)
                self.logger.debug(f"Created directory: {target_path}")
            except OSError as e:
                if e.errno == 36:  # File name too long
                    self.logger.error(f"Filename too long: {target_path}")
                    raise ValueError(f"Path too long: {target_path}")
                else:
                    raise
            
            return str(target_path)
            
        except Exception as e:
            self.logger.error(f"Failed to create folder structure for {record.get('it_checksum', 'unknown')}: {e}")
            raise
    
    def check_disk_space(self, file_path: str, destination: str) -> bool:
        """Check if there's enough disk space for the file"""
        try:
            file_size = os.path.getsize(file_path)
            dest_stat = shutil.disk_usage(destination)
            available_space = dest_stat.free
            
            # Require 10% more space than file size as buffer
            required_space = int(file_size * 1.1)
            
            if available_space < required_space:
                self.logger.error(f"Insufficient disk space. Need: {required_space:,} bytes, Available: {available_space:,} bytes")
                return False
            
            self.logger.debug(f"Disk space check passed. File: {file_size:,} bytes, Available: {available_space:,} bytes")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to check disk space: {e}")
            return False
    
    def verify_checksum(self, file_path: str, expected_checksum: str) -> bool:
        """Verify file integrity using SHA256 checksum with progress reporting"""
        if not expected_checksum or not os.path.exists(file_path):
            return False
        
        try:
            sha256_hash = hashlib.sha256()
            file_size = os.path.getsize(file_path)
            
            self.logger.debug(f"Verifying checksum for {os.path.basename(file_path)} ({file_size:,} bytes)")
            
            bytes_read = 0
            last_progress = 0
            
            with open(file_path, "rb") as f:
                while True:
                    chunk = f.read(8192)
                    if not chunk:
                        break
                    sha256_hash.update(chunk)
                    bytes_read += len(chunk)
                    
                    # Report progress for large files (>100MB)
                    if file_size > 100 * 1024 * 1024:
                        progress = (bytes_read / file_size) * 100
                        if progress - last_progress >= 25:  # Report every 25%
                            self.logger.debug(f"Checksum verification: {progress:.0f}%")
                            last_progress = progress
            
            calculated = sha256_hash.hexdigest()
            match = calculated.lower() == expected_checksum.lower()
            
            if match:
                self.logger.debug(f"Checksum verified successfully: {os.path.basename(file_path)}")
            else:
                self.logger.error(f"Checksum mismatch for {file_path}")
                self.logger.error(f"Expected: {expected_checksum}")
                self.logger.error(f"Got: {calculated}")
            
            return match
            
        except Exception as e:
            self.logger.error(f"Checksum verification failed for {file_path}: {e}")
            return False
    
    def is_rsync_available(self) -> bool:
        """Check if rsync is available and get its path"""
        try:
            # Try common rsync locations
            rsync_paths = ['rsync']
            if platform.system() == 'Darwin':  # macOS
                rsync_paths.extend(['/usr/bin/rsync', '/opt/homebrew/bin/rsync'])
            elif platform.system() == 'Linux':
                rsync_paths.extend(['/usr/bin/rsync', '/bin/rsync'])
            
            for rsync_path in rsync_paths:
                try:
                    result = subprocess.run([rsync_path, '--version'], 
                                          capture_output=True, text=True, timeout=10)
                    if result.returncode == 0:
                        self.logger.debug(f"Found rsync at: {rsync_path}")
                        return True
                except (subprocess.TimeoutExpired, FileNotFoundError, OSError):
                    continue
            
            self.logger.debug("rsync not found in standard locations")
            return False
            
        except Exception as e:
            self.logger.debug(f"Error checking for rsync: {e}")
            return False
    
    def copy_file_rsync(self, source: str, destination: str) -> bool:
        """Copy file using rsync with comprehensive error handling"""
        if not self.is_rsync_available():
            self.logger.debug("rsync not available, using fallback method")
            return self.copy_file_fallback(source, destination)
        
        try:
            # Ensure destination directory exists
            dest_dir = os.path.dirname(destination)
            os.makedirs(dest_dir, exist_ok=True)
            
            # Build rsync command with optimal flags
            cmd = [
                'rsync',
                '-avh',           # archive, verbose, human-readable
                '--progress',     # show progress
                '--partial',      # keep partial files
                '--inplace',      # update destination file in-place
                '--sparse',       # handle sparse files efficiently
                '--compress',     # compress during transfer (helps with network copies)
                source,
                destination
            ]
            
            self.logger.debug(f"Running rsync: {' '.join(cmd)}")
            
            # Run rsync with proper timeout and error handling
            start_time = time.time()
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=7200  # 2 hour timeout for very large files
            )
            
            elapsed_time = time.time() - start_time
            
            if result.returncode == 0:
                file_size = os.path.getsize(destination)
                speed_mbps = (file_size / (1024 * 1024)) / elapsed_time if elapsed_time > 0 else 0
                self.logger.info(f"rsync completed: {os.path.basename(source)} ({file_size:,} bytes) in {elapsed_time:.1f}s ({speed_mbps:.1f} MB/s)")
                return True
            else:
                self.logger.warning(f"rsync failed (exit code {result.returncode})")
                if result.stderr:
                    self.logger.warning(f"rsync stderr: {result.stderr.strip()}")
                
                # Try fallback method if rsync failed
                self.logger.info("Attempting fallback copy method")
                return self.copy_file_fallback(source, destination)
                
        except subprocess.TimeoutExpired:
            self.logger.error(f"rsync timeout after 2 hours for {source}")
            return False
        except Exception as e:
            self.logger.warning(f"rsync error for {source}: {e}")
            return self.copy_file_fallback(source, destination)
    
    def copy_file_fallback(self, source: str, destination: str) -> bool:
        """Fallback file copy using Python with progress tracking and verification"""
        try:
            # Ensure destination directory exists
            dest_dir = os.path.dirname(destination)
            os.makedirs(dest_dir, exist_ok=True)
            
            file_size = os.path.getsize(source)
            self.logger.debug(f"Using fallback copy for {os.path.basename(source)} ({file_size:,} bytes)")
            
            start_time = time.time()
            
            # Use larger chunks for better performance with large files
            chunk_size = 1024 * 1024  # 1MB chunks
            copied = 0
            last_progress = 0
            
            with open(source, 'rb') as src, open(destination, 'wb') as dst:
                while True:
                    chunk = src.read(chunk_size)
                    if not chunk:
                        break
                    
                    dst.write(chunk)
                    copied += len(chunk)
                    
                    # Report progress for large files
                    if file_size > 50 * 1024 * 1024:  # Files > 50MB
                        progress = (copied / file_size) * 100
                        if progress - last_progress >= 10:  # Report every 10%
                            elapsed = time.time() - start_time
                            speed_mbps = (copied / (1024 * 1024)) / elapsed if elapsed > 0 else 0
                            self.logger.debug(f"Copy progress: {progress:.0f}% ({speed_mbps:.1f} MB/s)")
                            last_progress = progress
            
            # Copy file attributes (timestamps, permissions)
            try:
                shutil.copystat(source, destination)
            except OSError as e:
                self.logger.debug(f"Could not copy file attributes: {e}")
            
            elapsed_time = time.time() - start_time
            speed_mbps = (file_size / (1024 * 1024)) / elapsed_time if elapsed_time > 0 else 0
            self.logger.info(f"Fallback copy completed: {os.path.basename(source)} in {elapsed_time:.1f}s ({speed_mbps:.1f} MB/s)")
            
            return True
            
        except Exception as e:
            self.logger.error(f"Fallback copy failed for {source}: {e}")
            # Clean up partial file
            try:
                if os.path.exists(destination):
                    os.remove(destination)
            except OSError:
                pass
            return False
    
    def update_database(self, checksum: str, new_location: str) -> bool:
        """Update it_def_loc in database with proper error handling"""
        try:
            with self.get_db_connection() as conn:
                cursor = conn.cursor()
                
                # Use a transaction for safety
                cursor.execute("BEGIN TRANSACTION")
                
                try:
                    cursor.execute(
                        "UPDATE import_tuner SET it_def_loc = ? WHERE it_checksum = ?",
                        (new_location, checksum)
                    )
                    
                    if cursor.rowcount == 0:
                        self.logger.warning(f"No rows updated for checksum: {checksum}")
                        cursor.execute("ROLLBACK")
                        return False
                    
                    cursor.execute("COMMIT")
                    self.logger.debug(f"Database updated: {checksum} -> {new_location}")
                    return True
                    
                except sqlite3.Error as e:
                    cursor.execute("ROLLBACK")
                    self.logger.error(f"Database update failed, rolled back: {e}")
                    return False
                    
        except Exception as e:
            self.logger.error(f"Database update error for {checksum}: {e}")
            return False
    
    def process_file(self, record: Dict, def_loc: str) -> Tuple[bool, str]:
        """Process a single file record with comprehensive error handling"""
        checksum = record.get('it_checksum', 'unknown')
        
        try:
            source_path = record['file_location']
            
            # Thread-safe check for already processed files
            with self.processed_lock:
                if checksum in self.processed_files:
                    return True, f"Already processed: {checksum}"
                # Add to processed set immediately to prevent duplicates
                self.processed_files.add(checksum)
            
            # Verify source file exists and is accessible
            if not os.path.exists(source_path):
                return False, f"Source file not found: {source_path}"
            
            if not os.access(source_path, os.R_OK):
                return False, f"Source file not readable: {source_path}"
            
            # Get file info
            try:
                file_size = os.path.getsize(source_path)
                if file_size == 0:
                    return False, f"Source file is empty: {source_path}"
            except OSError as e:
                return False, f"Cannot access source file: {source_path} - {e}"
            
            self.logger.debug(f"Processing: {os.path.basename(source_path)} ({file_size:,} bytes)")
            
            # Check disk space before proceeding
            if not self.check_disk_space(source_path, def_loc):
                return False, f"Insufficient disk space for: {source_path}"
            
            # Verify source file integrity
            if not self.verify_checksum(source_path, checksum):
                return False, f"Source checksum verification failed: {source_path}"
            
            # Create target directory structure
            try:
                target_dir = self.create_folder_structure(record, def_loc)
            except Exception as e:
                return False, f"Failed to create folder structure: {e}"
            
            # Generate target file path
            filename = os.path.basename(source_path)
            target_path = os.path.join(target_dir, filename)
            
            # Handle existing target file
            if os.path.exists(target_path):
                if self.verify_checksum(target_path, checksum):
                    # File already exists and is valid
                    if self.update_database(checksum, target_path):
                        return True, f"Target already exists and is valid: {os.path.basename(target_path)}"
                    else:
                        return False, f"Database update failed for existing file: {checksum}"
                else:
                    # Remove corrupted existing file
                    self.logger.warning(f"Removing corrupted existing file: {target_path}")
                    try:
                        os.remove(target_path)
                    except OSError as e:
                        return False, f"Cannot remove corrupted file: {target_path} - {e}"
            
            # Copy the file
            self.logger.info(f"Copying: {os.path.basename(source_path)} -> {os.path.basename(target_dir)}")
            
            copy_success = self.copy_file_rsync(source_path, target_path)
            
            if not copy_success:
                return False, f"Copy operation failed: {source_path}"
            
            # Verify copied file integrity
            if not self.verify_checksum(target_path, checksum):
                self.logger.error(f"Target file corrupted after copy, removing: {target_path}")
                try:
                    os.remove(target_path)
                except OSError:
                    pass
                return False, f"Target file verification failed: {target_path}"
            
            # Update database
            if not self.update_database(checksum, target_path):
                return False, f"Database update failed: {checksum}"
            
            return True, f"Successfully processed: {os.path.basename(source_path)}"
            
        except Exception as e:
            # Remove from processed set if we failed
            with self.processed_lock:
                self.processed_files.discard(checksum)
            
            self.logger.error(f"Unexpected error processing {checksum}: {e}")
            return False, f"Processing failed with error: {e}"
    
    def run(self) -> None:
        """Main execution function with comprehensive error handling"""
        start_time = time.time()
        
        try:
            self.logger.info("=== Starting oil_change process ===")
            
            # Step 1: Load configuration
            self.logger.info("Step 1: Loading configuration...")
            config = self.load_config()
            
            # Setup logging based on config
            log_enabled = config.get('oil_change', {}).get('logs', False)
            self.setup_logging(log_enabled)
            
            self.logger.info(f"Logging enabled: {log_enabled}")
            
            # Get and validate def_loc
            def_loc = config['user_input']['default']['def_loc']
            self.logger.info(f"Target location: {def_loc}")
            
            # Ensure def_loc exists and is writable
            if not os.path.exists(def_loc):
                self.logger.info(f"Creating target directory: {def_loc}")
                try:
                    os.makedirs(def_loc, exist_ok=True)
                except OSError as e:
                    self.logger.error(f"Failed to create target directory: {e}")
                    sys.exit(1)
            
            if not os.access(def_loc, os.W_OK):
                self.logger.error(f"Target directory is not writable: {def_loc}")
                sys.exit(1)
            
            # Check available disk space
            try:
                disk_usage = shutil.disk_usage(def_loc)
                available_gb = disk_usage.free / (1024**3)
                self.logger.info(f"Available disk space: {available_gb:.1f} GB")
                
                if available_gb < 1.0:  # Less than 1GB available
                    self.logger.warning("Low disk space available!")
            except Exception as e:
                self.logger.warning(f"Could not check disk space: {e}")
            
            # Step 2: Get import data from database
            self.logger.info("Step 2: Loading import data from database...")
            records = self.get_import_data()
            
            if not records:
                self.logger.info("No records found to process")
                return
            
            self.logger.info(f"Found {len(records)} records to process")
            
            # Analyze torrent types
            torrent_types = {}
            for record in records:
                t_type = record.get('it_torrent', 'unknown')
                torrent_types[t_type] = torrent_types.get(t_type, 0) + 1
            
            self.logger.info("Torrent type distribution:")
            for t_type, count in torrent_types.items():
                self.logger.info(f"  {t_type}: {count} files")
            
            # Step 3-4: Process files with optimized threading
            self.logger.info("Step 3-4: Processing files...")
            
            # Determine optimal number of workers
            # For file I/O operations, too many threads can hurt performance
            cpu_count = os.cpu_count() or 1
            max_workers = min(cpu_count, 3)  # Conservative limit for file operations
            
            self.logger.info(f"Using {max_workers} worker threads")
            
            successful = 0
            failed = 0
            total_bytes_processed = 0
            
            # Process files with thread pool
            with ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="FileProcessor") as executor:
                # Submit all tasks
                future_to_record = {
                    executor.submit(self.process_file, record, def_loc): record 
                    for record in records
                }
                
                self.logger.info(f"Submitted {len(future_to_record)} processing tasks")
                
                # Process completed tasks
                for i, future in enumerate(as_completed(future_to_record), 1):
                    record = future_to_record[future]
                    checksum = record.get('it_checksum', 'unknown')
                    
                    try:
                        success, message = future.result()
                        
                        if success:
                            successful += 1
                            # Try to get file size for statistics
                            try:
                                file_path = record.get('file_location', '')
                                if file_path and os.path.exists(file_path):
                                    total_bytes_processed += os.path.getsize(file_path)
                            except:
                                pass  # Don't fail on statistics
                            
                            self.logger.info(f"✓ [{i}/{len(records)}] {message}")
                        else:
                            failed += 1
                            self.logger.error(f"✗ [{i}/{len(records)}] {message}")
                            
                    except Exception as e:
                        failed += 1
                        self.logger.error(f"✗ [{i}/{len(records)}] Unexpected error for {checksum}: {e}")
                
                # Wait for all tasks to complete
                self.logger.debug("Waiting for all tasks to complete...")
            
            # Final summary
            elapsed_time = time.time() - start_time
            total_gb_processed = total_bytes_processed / (1024**3)
            
            self.logger.info("=== Process completed ===")
            self.logger.info(f"Total time: {elapsed_time:.1f} seconds ({elapsed_time/60:.1f} minutes)")
            self.logger.info(f"Results: {successful} successful, {failed} failed")
            self.logger.info(f"Data processed: {total_gb_processed:.2f} GB")
            
            if total_bytes_processed > 0 and elapsed_time > 0:
                avg_speed_mbps = (total_bytes_processed / (1024*1024)) / elapsed_time
                self.logger.info(f"Average speed: {avg_speed_mbps:.1f} MB/s")
            
            # Report any issues
            if failed > 0:
                self.logger.warning(f"{failed} files failed to process")
                if log_enabled:
                    self.logger.warning("Check log file for detailed error information")
                else:
                    self.logger.warning("Enable logging in config for detailed error information")
            
            # Success/failure exit codes
            if failed > 0:
                sys.exit(1)  # Partial failure
            else:
                self.logger.info("All files processed successfully!")
                sys.exit(0)  # Complete success
                
        except KeyboardInterrupt:
            self.logger.info("Process interrupted by user (Ctrl+C)")
            sys.exit(130)  # Standard exit code for SIGINT
            
        except Exception as e:
            self.logger.error(f"Fatal error in main process: {e}")
            if hasattr(self, 'logger') and self.config and self.config.get('oil_change', {}).get('logs', False):
                import traceback
                self.logger.error("Full traceback:")
                self.logger.error(traceback.format_exc())
            raise

def main():
    """Entry point with comprehensive error handling"""
    organizer = None
    
    try:
        # Create organizer instance
        organizer = MediaOrganizer()
        
        # Validate required files exist before starting
        required_files = ['2jznoshit.json', 'danger2manifold.db']
        missing_files = [f for f in required_files if not os.path.exists(f)]
        
        if missing_files:
            print(f"ERROR: Required files missing: {', '.join(missing_files)}")
            print("Please ensure these files are in the current directory:")
            print("  - 2jznoshit.json (configuration file)")
            print("  - danger2manifold.db (database file)")
            sys.exit(1)
        
        # Run the main process
        organizer.run()
        
    except KeyboardInterrupt:
        if organizer and hasattr(organizer, 'logger'):
            organizer.logger.info("Process interrupted by user")
        else:
            print("\nProcess interrupted by user")
        sys.exit(130)
        
    except SystemExit:
        # Re-raise SystemExit to preserve exit codes
        raise
        
    except Exception as e:
        if organizer and hasattr(organizer, 'logger'):
            organizer.logger.error(f"Fatal error: {e}")
        else:
            print(f"FATAL ERROR: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()