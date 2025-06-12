#!/usr/bin/env python3
"""
oil_change - Media file organization and relocation utility
Follows exact specifications for cleaning, restructuring, and moving media files.
"""

import json
import sqlite3
import os
import shutil
import hashlib
import logging
from pathlib import Path
from typing import Dict, List, Tuple, Optional, Set
from concurrent.futures import ThreadPoolExecutor, as_completed
import sys
import re


class MediaOrganizer:
    VALID_TORRENT_TYPES = {'series', 'season', 'episode', 'all'}
    VIDEO_EXTENSIONS = {'.mkv', '.mp4', '.avi', '.mov', '.wmv', '.flv', '.webm', '.m4v', '.ts', '.m2ts'}
    SUBTITLE_EXTENSIONS = {'.srt', '.ass', '.ssa', '.vtt', '.sub', '.idx', '.sup'}
    
    def __init__(self):
        self.config = {}
        self.db_conn = None
        # Initialize basic logger immediately to prevent NoneType errors
        logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
        self.logger = logging.getLogger(__name__)
        
    def setup_logging(self, enable_logs: bool) -> None:
        """Configure logging based on user preferences - STEP 1 requirement."""
        # Clear any existing handlers to avoid duplication
        for handler in logging.root.handlers[:]:
            logging.root.removeHandler(handler)
        
        log_level = logging.DEBUG if enable_logs else logging.WARNING
        
        handlers = [logging.StreamHandler(sys.stdout)]
        if enable_logs:
            try:
                handlers.append(logging.FileHandler('oil_change.log', mode='w'))
            except (OSError, IOError) as e:
                print(f"WARNING: Could not create log file: {e}")
            
        logging.basicConfig(
            level=log_level,
            format='%(asctime)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s',
            handlers=handlers,
            force=True
        )
        
        # Reinitialize logger with new config
        self.logger = logging.getLogger(__name__)
        self.logger.debug("Logging reconfigured with full debug and verbose tracking")

    def load_config(self) -> Dict:
        """STEP 1: Load user preferences from 2jznoshit.json."""
        config_path = Path('2jznoshit.json')
        if not config_path.exists():
            print("FATAL: 2jznoshit.json not found in current directory")
            sys.exit(1)
            
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                config = json.load(f)
                
            # Validate required structure
            if 'oil_change' not in config:
                print("FATAL: 'oil_change' section missing from config")
                sys.exit(1)
                
            if 'user_input' not in config or 'default' not in config['user_input']:
                print("FATAL: 'user_input.default' section missing from config")
                sys.exit(1)
                
            if 'def_loc' not in config['user_input']['default']:
                print("FATAL: 'def_loc' not found in user_input.default")
                sys.exit(1)
                
            if self.logger:
                self.logger.debug(f"Config loaded successfully: oil_change.logs={config['oil_change'].get('logs', False)}")
            return config
            
        except json.JSONDecodeError as e:
            print(f"FATAL: Invalid JSON in 2jznoshit.json: {e}")
            sys.exit(1)
        except Exception as e:
            print(f"FATAL: Error reading config file: {e}")
            sys.exit(1)

    def connect_db(self) -> sqlite3.Connection:
        """STEP 2: Connect to danger2manifold.db database."""
        db_path = Path('danger2manifold.db')
        if not db_path.exists():
            if self.logger:
                self.logger.error("FATAL: danger2manifold.db not found in current directory")
            else:
                print("FATAL: danger2manifold.db not found in current directory")
            sys.exit(1)
            
        try:
            conn = sqlite3.connect(str(db_path), timeout=30.0)
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            
            # Verify import_tuner table exists
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='import_tuner'")
            if not cursor.fetchone():
                if self.logger:
                    self.logger.error("FATAL: import_tuner table not found in database")
                else:
                    print("FATAL: import_tuner table not found in database")
                sys.exit(1)
                
            if self.logger:
                self.logger.debug("Database connection established to danger2manifold.db")
            return conn
            
        except sqlite3.Error as e:
            if self.logger:
                self.logger.error(f"FATAL: Database connection failed: {e}")
            else:
                print(f"FATAL: Database connection failed: {e}")
            sys.exit(1)

    def get_import_tuner_data(self) -> List[Dict]:
        """STEP 2: Fetch all records from import_tuner table only."""
        try:
            cursor = self.db_conn.cursor()
            cursor.execute("""
                SELECT file_location, file_name, it_checksum, it_def_loc, it_ep_avl, 
                       it_ep_no, it_ep_title, it_sea_no, it_series, it_special, 
                       it_src, it_src_link, it_subtitles, it_torrent
                FROM import_tuner
                ORDER BY it_series, it_sea_no, it_ep_no
            """)
            rows = cursor.fetchall()
            
            if not rows:
                self.logger.warning("No records found in import_tuner table")
                return []
            
            data = []
            for row in rows:
                record = dict(row)
                
                # Validate critical fields
                if not record.get('it_checksum'):
                    self.logger.error(f"SKIP: Missing checksum for {record.get('file_name', 'unknown')}")
                    continue
                    
                if not record.get('file_location'):
                    self.logger.error(f"SKIP: Missing file_location for {record.get('it_checksum', 'unknown')}")
                    continue
                    
                if not record.get('it_torrent'):
                    self.logger.warning(f"Missing it_torrent for {record['it_checksum']}, using 'season' default")
                    record['it_torrent'] = 'season'
                    
                # Validate torrent type
                if record['it_torrent'] not in self.VALID_TORRENT_TYPES:
                    self.logger.error(f"INVALID: it_torrent '{record['it_torrent']}' not in {self.VALID_TORRENT_TYPES}")
                    continue
                    
                data.append(record)
            
            self.logger.debug(f"Retrieved {len(data)} valid records from import_tuner table")
            return data
            
        except sqlite3.Error as e:
            self.logger.error(f"FATAL: Database query failed: {e}")
            sys.exit(1)

    def clean_folders(self, folders_to_clean: Set[str]) -> None:
        """STEP 3: Clean folders - remove non-video/subtitle files, keep only media files."""
        self.logger.debug(f"Cleaning {len(folders_to_clean)} folders")
        
        for folder_path in folders_to_clean:
            path = Path(folder_path)
            if not path.exists():
                self.logger.warning(f"Folder does not exist, skipping: {folder_path}")
                continue
                
            self.logger.debug(f"Cleaning folder: {folder_path}")
            files_removed = 0
            dirs_removed = 0
            
            # Walk from bottom up to handle nested directories
            for root, dirs, files in os.walk(path, topdown=False):
                root_path = Path(root)
                
                # Remove non-media files
                for file in files:
                    file_path = root_path / file
                    file_ext = file_path.suffix.lower()
                    
                    if (file_ext not in self.VIDEO_EXTENSIONS and 
                        file_ext not in self.SUBTITLE_EXTENSIONS):
                        try:
                            file_path.unlink()
                            files_removed += 1
                            self.logger.debug(f"Removed non-media file: {file_path}")
                        except OSError as e:
                            self.logger.warning(f"Failed to remove file {file_path}: {e}")
                
                # Remove empty directories (but not the root folder we're cleaning)
                if root_path != path:
                    try:
                        if not any(root_path.iterdir()):
                            root_path.rmdir()
                            dirs_removed += 1
                            self.logger.debug(f"Removed empty directory: {root_path}")
                    except OSError:
                        pass  # Directory not empty or permission issue
            
            self.logger.debug(f"Cleaned {folder_path}: {files_removed} files, {dirs_removed} directories removed")

    def normalize_name(self, name: str) -> str:
        """Normalize names with proper capitalization and no duplication."""
        if not name:
            return ""
            
        # Clean up the name
        name = str(name).strip()
        name = re.sub(r'\s+', ' ', name)  # Multiple spaces to single space
        
        # Title case
        name = name.title()
        
        # Fix common issues
        name = re.sub(r'\bS(\d+)\b', r'Season \1', name)  # S01 -> Season 01
        name = re.sub(r'\bE(\d+)\b', r'Episode \1', name)  # E01 -> Episode 01
        
        # Remove duplicate words (case insensitive)
        words = name.split()
        normalized_words = []
        prev_word_lower = None
        
        for word in words:
            word_lower = word.lower()
            if word_lower != prev_word_lower:
                normalized_words.append(word)
            prev_word_lower = word_lower
        
        result = ' '.join(normalized_words)
        
        # Ensure no double "Episode" or "Season"
        result = re.sub(r'\b(Episode|Season)\s+\1\b', r'\1', result, flags=re.IGNORECASE)
        
        return result

    def create_folder_structure(self, records: List[Dict], def_loc: str) -> Dict[str, str]:
        """STEP 4: Create folder structure in def_loc based on it_torrent preferences."""
        def_path = Path(def_loc)
        
        # Create base def_loc if it doesn't exist
        try:
            def_path.mkdir(parents=True, exist_ok=True)
            self.logger.debug(f"Ensured def_loc exists: {def_loc}")
        except OSError as e:
            self.logger.error(f"FATAL: Cannot create def_loc {def_loc}: {e}")
            sys.exit(1)
        
        folder_map = {}  # checksum -> destination_folder_path
        
        for record in records:
            checksum = record['it_checksum']
            series = self.normalize_name(record.get('it_series', ''))
            season = self.normalize_name(record.get('it_sea_no', ''))
            episode = self.normalize_name(record.get('it_ep_no', ''))
            torrent_type = record['it_torrent']
            
            if not series:
                self.logger.error(f"SKIP: Missing series name for checksum {checksum}")
                continue
            
            try:
                if torrent_type == 'series':
                    # Default Folder Layout: Series/Season/
                    series_path = def_path / series
                    season_path = series_path / season if season else series_path
                    season_path.mkdir(parents=True, exist_ok=True)
                    folder_map[checksum] = str(season_path)
                    
                elif torrent_type == 'all':
                    # Same as series - recreate same folder structure
                    series_path = def_path / series
                    season_path = series_path / season if season else series_path
                    season_path.mkdir(parents=True, exist_ok=True)
                    folder_map[checksum] = str(season_path)
                    
                elif torrent_type == 'season':
                    # Per Season Layout: "Series - Season XX"
                    if season:
                        season_folder = f"{series} - {season}"
                    else:
                        season_folder = series
                    season_path = def_path / season_folder
                    season_path.mkdir(parents=True, exist_ok=True)
                    folder_map[checksum] = str(season_path)
                    
                elif torrent_type == 'episode':
                    # Per Episode Layout: "Series - Season XX - Episode XX"
                    if season and episode:
                        episode_folder = f"{series} - {season} - {episode}"
                    elif season:
                        episode_folder = f"{series} - {season}"
                    else:
                        episode_folder = series
                    episode_path = def_path / episode_folder
                    episode_path.mkdir(parents=True, exist_ok=True)
                    folder_map[checksum] = str(episode_path)
                    
                self.logger.debug(f"Created folder for {checksum}: {folder_map[checksum]}")
                
            except OSError as e:
                self.logger.error(f"Failed to create folder structure for {checksum}: {e}")
                continue
        
        self.logger.debug(f"Created folder structure for {len(folder_map)} items")
        return folder_map

    def verify_checksum(self, file_path: str, expected_checksum: str) -> bool:
        """Verify file integrity using SHA256 checksum."""
        if not expected_checksum:
            return False
            
        try:
            sha256_hash = hashlib.sha256()
            with open(file_path, 'rb') as f:
                while chunk := f.read(65536):
                    sha256_hash.update(chunk)
            
            calculated = sha256_hash.hexdigest()
            match = calculated == expected_checksum
            
            if not match:
                self.logger.error(f"Checksum mismatch for {file_path}")
                self.logger.error(f"Expected: {expected_checksum}")
                self.logger.error(f"Calculated: {calculated}")
            
            return match
            
        except (OSError, IOError) as e:
            self.logger.error(f"Checksum verification failed for {file_path}: {e}")
            return False

    def copy_file_with_verification(self, src: str, dst: str, checksum: str) -> bool:
        """Copy file and verify integrity."""
        src_path = Path(src)
        dst_path = Path(dst)
        
        if not src_path.exists():
            self.logger.error(f"Source file not found: {src}")
            return False
        
        # Verify source file first
        if not self.verify_checksum(src, checksum):
            self.logger.error(f"Source file checksum invalid: {src}")
            return False
        
        try:
            # Ensure destination directory exists
            dst_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Copy with metadata preservation
            shutil.copy2(src, dst)
            self.logger.debug(f"Copied file: {src} -> {dst}")
            
            # Verify destination file
            if self.verify_checksum(dst, checksum):
                self.logger.debug(f"Copy verified successfully: {dst}")
                return True
            else:
                # Remove corrupted copy
                dst_path.unlink(missing_ok=True)
                self.logger.error(f"Copy verification failed, removed corrupted file: {dst}")
                return False
                
        except (OSError, IOError, shutil.Error) as e:
            self.logger.error(f"Copy operation failed: {src} -> {dst}: {e}")
            dst_path.unlink(missing_ok=True)  # Clean up partial copy
            return False

    def find_subtitle_files(self, video_path: str) -> List[str]:
        """Find associated subtitle files for a video file."""
        video_path_obj = Path(video_path)
        video_stem = video_path_obj.stem
        video_dir = video_path_obj.parent
        
        subtitle_files = []
        
        if video_dir.exists():
            for file_path in video_dir.iterdir():
                if (file_path.is_file() and 
                    file_path.suffix.lower() in self.SUBTITLE_EXTENSIONS and
                    file_path.stem.startswith(video_stem)):
                    subtitle_files.append(str(file_path))
        
        return subtitle_files

    def process_file(self, record: Dict, folder_map: Dict[str, str]) -> Tuple[str, Optional[str]]:
        """STEP 5: Process a single file record with subtitle handling."""
        checksum = record['it_checksum']
        src_path = record['file_location']
        file_name = record['file_name']
        
        if checksum not in folder_map:
            self.logger.error(f"No destination folder mapping for checksum: {checksum}")
            return checksum, None
        
        dest_folder = folder_map[checksum]
        dest_file_path = Path(dest_folder) / file_name
        
        # Skip if already exists and verified
        if dest_file_path.exists() and self.verify_checksum(str(dest_file_path), checksum):
            self.logger.debug(f"File already exists and verified: {dest_file_path}")
            return checksum, str(dest_file_path)
        
        # Copy main video file
        if not self.copy_file_with_verification(src_path, str(dest_file_path), checksum):
            return checksum, None
        
        # Handle subtitle files
        subtitle_files = self.find_subtitle_files(src_path)
        for subtitle_src in subtitle_files:
            subtitle_name = Path(subtitle_src).name
            subtitle_dest = Path(dest_folder) / subtitle_name
            
            try:
                if not subtitle_dest.exists():
                    shutil.copy2(subtitle_src, str(subtitle_dest))
                    self.logger.debug(f"Copied subtitle: {subtitle_src} -> {subtitle_dest}")
            except (OSError, IOError, shutil.Error) as e:
                self.logger.warning(f"Failed to copy subtitle {subtitle_src}: {e}")
        
        return checksum, str(dest_file_path)

    def update_database(self, updates: Dict[str, Optional[str]]) -> None:
        """STEP 5: Update it_def_loc in database with full file paths."""
        if not updates:
            self.logger.warning("No updates to apply to database")
            return
        
        successful_updates = [(path, checksum) for checksum, path in updates.items() if path is not None]
        
        if not successful_updates:
            self.logger.warning("No successful file operations to update in database")
            return
        
        try:
            cursor = self.db_conn.cursor()
            
            # Update in batch for efficiency
            cursor.executemany(
                "UPDATE import_tuner SET it_def_loc = ? WHERE it_checksum = ?",
                successful_updates
            )
            
            self.db_conn.commit()
            self.logger.info(f"Updated database it_def_loc for {len(successful_updates)} files")
            
            # Log failures
            failed_count = len(updates) - len(successful_updates)
            if failed_count > 0:
                self.logger.warning(f"Failed to process {failed_count} files")
                
        except sqlite3.Error as e:
            self.logger.error(f"Database update failed: {e}")
            self.db_conn.rollback()
            raise

    def run(self) -> None:
        """Main execution function following exact step sequence."""
        try:
            # STEP 1: Load configuration and setup logging
            self.config = self.load_config()
            self.setup_logging(self.config['oil_change'].get('logs', False))
            
            # STEP 2: Connect to database and get data
            self.db_conn = self.connect_db()
            records = self.get_import_tuner_data()
            
            if not records:
                self.logger.info("No valid records found in import_tuner table")
                return
            
            # Get def_loc from config
            def_loc = self.config['user_input']['default']['def_loc']
            
            # STEP 3: Clean folders - get unique source folders and destination folder
            folders_to_clean = set()
            
            # Add source folders
            for record in records:
                source_folder = str(Path(record['file_location']).parent)
                folders_to_clean.add(source_folder)
            
            # Add destination folder
            folders_to_clean.add(def_loc)
            
            self.clean_folders(folders_to_clean)
            
            # STEP 4: Create folder structure based on torrent preferences
            folder_map = self.create_folder_structure(records, def_loc)
            
            if not folder_map:
                self.logger.error("No valid folder mappings created")
                return
            
            # STEP 5: Process files with optimal concurrency
            max_workers = min(max(1, os.cpu_count() // 2), len(records), 8)  # Conservative threading
            updates = {}
            
            self.logger.info(f"Processing {len(records)} files with {max_workers} workers")
            
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                future_to_checksum = {
                    executor.submit(self.process_file, record, folder_map): record['it_checksum']
                    for record in records if record['it_checksum'] in folder_map
                }
                
                for future in as_completed(future_to_checksum):
                    try:
                        checksum, new_location = future.result()
                        updates[checksum] = new_location
                    except Exception as e:
                        checksum = future_to_checksum[future]
                        self.logger.error(f"Processing failed for {checksum}: {e}")
                        updates[checksum] = None
            
            # Update database with new it_def_loc values
            self.update_database(updates)
            
            # Final report
            success_count = sum(1 for loc in updates.values() if loc is not None)
            total_count = len(records)
            self.logger.info(f"oil_change complete: {success_count}/{total_count} files processed successfully")
            
            if success_count < total_count:
                self.logger.warning(f"{total_count - success_count} files failed processing")
                
        except Exception as e:
            self.logger.error(f"FATAL: Unexpected error during execution: {e}")
            raise
        finally:
            if self.db_conn:
                self.db_conn.close()
                self.logger.debug("Database connection closed")


if __name__ == "__main__":
    try:
        organizer = MediaOrganizer()
        organizer.run()
    except KeyboardInterrupt:
        print("\nOperation cancelled by user")
        sys.exit(130)
    except Exception as e:
        print(f"FATAL ERROR: {e}")
        sys.exit(1)