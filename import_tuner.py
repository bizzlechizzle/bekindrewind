#!/usr/bin/env python3
"""
TV Series Video File Scanner - Production Ready
Scans video files, generates checksums, parses metadata, and populates database.
"""

import hashlib
import json
import logging
import re
import sqlite3
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

# Configuration
VIDEO_EXTENSIONS = frozenset(['.mkv', '.mp4', '.avi', '.mov', '.wmv', '.flv', '.webm', '.m4v'])
SUBTITLE_EXTENSIONS = frozenset(['.srt', '.ass', '.ssa', '.sub', '.vtt', '.sbv', '.scc', '.ttml', '.dfxp'])
SKIP_DIRS = frozenset(['.DS_Store', 'Thumbs.db', '.AppleDouble', '.LSOverride', 
                       'desktop.ini', '.Spotlight-V100', '.Trashes', '.fseventsd'])
SPECIAL_KEYWORDS = frozenset(['sneak peek', 'teaser', 'trailer', 'extended edition', 'behind the scenes',
                              'making of', 'deleted scenes', 'blooper', 'bloopers', 'gag reel',
                              "director's cut", 'directors cut', 'uncut', 'bonus', 'extra', 'preview',
                              'directors', 'sneak peak'])

# Pre-compile regex
FILENAME_PATTERN = re.compile(r'^(.+?)_S(\d+)E(\d+)_(.+)$')

# Global config cache
_CONFIG_CACHE = None


def load_config():
    """Load and cache configuration."""
    global _CONFIG_CACHE
    if _CONFIG_CACHE is None:
        try:
            with open('2jznoshit.json', 'r') as f:
                _CONFIG_CACHE = json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            _CONFIG_CACHE = {}
    return _CONFIG_CACHE


def setup_logging() -> bool:
    """Setup logging based on 2jznoshit.json preferences."""
    config = load_config()
    log_enabled = config.get('import_tuner', {}).get('logs', True)
    
    if log_enabled:
        logging.basicConfig(
            filename='import_tuner.log',
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            filemode='a'
        )
    else:
        logging.disable(logging.CRITICAL)
    
    return log_enabled


def load_user_preferences() -> Dict[str, str]:
    """Load user preferences for source, link, and torrent options."""
    config = load_config()
    defaults = config.get('user_input', {}).get('default', {})
    return {
        'it_src': defaults.get('it_src', ''),
        'it_src_link': defaults.get('it_src_link', ''),
        'it_torrent': defaults.get('it_torrent', ''),
        'timeout': defaults.get('timeout', '5 seconds')
    }


def check_library_match(series_name: str) -> Optional[Dict[str, str]]:
    """Check if series exists in 2jznoshit library."""
    config = load_config()
    library = config.get('library', {})
    series_lower = series_name.lower().replace(' ', '').replace('_', '')
    
    for service, info in library.items():
        if service.lower().replace(' ', '').replace('_', '') == series_lower:
            return info
    return None


def calculate_sha256(file_path: Path) -> str:
    """Calculate SHA256 checksum with optimized settings."""
    sha256_hash = hashlib.sha256()
    try:
        # Optimized for SSD: 8MB chunks, 8MB buffer
        with file_path.open('rb', buffering=8388608) as f:
            while chunk := f.read(8388608):
                sha256_hash.update(chunk)
        return sha256_hash.hexdigest()
    except (OSError, PermissionError) as e:
        logging.warning(f"Cannot read file {file_path}: {e}")
        return ""


def find_subtitle_files(video_path: Path) -> str:
    """Check if subtitles exist for video file. Returns 'yes' or empty string."""
    video_stem = video_path.stem
    try:
        parent_files = list(video_path.parent.iterdir())
        for item in parent_files:
            if (item.is_file() and 
                item.suffix.lower() in SUBTITLE_EXTENSIONS and
                item.stem.startswith(video_stem)):
                return 'yes'
    except (PermissionError, OSError):
        pass
    return ''


def parse_filename(filename: str) -> Optional[Dict[str, str]]:
    """Parse TV show filename following the pattern: Series_SxxExx_Special_Title."""
    stem = Path(filename).stem
    
    match = FILENAME_PATTERN.match(stem)
    if not match:
        logging.warning(f"Filename does not match expected pattern: {filename}")
        return None
    
    series, season_num, episode_num, remainder = match.groups()
    series = series.replace('_', ' ')
    
    # Check for special keywords at the start of remainder
    remainder_lower = remainder.lower()
    special = ""
    title = remainder
    
    for keyword in SPECIAL_KEYWORDS:
        keyword_underscore = keyword.replace(' ', '_')
        if remainder_lower.startswith(keyword.lower()):
            if len(remainder) > len(keyword_underscore) and remainder[len(keyword_underscore)] == '_':
                special = remainder[:len(keyword_underscore)].replace('_', ' ')
                title = remainder[len(keyword_underscore) + 1:]
            else:
                special = keyword
                title = remainder[len(keyword_underscore):]
            break
    
    title = title.replace('_', ' ').strip()
    if not title:
        title = f"Episode {int(episode_num)}"
    
    return {
        'series': series,
        'season': f"{int(season_num):02d}",
        'episode': f"{int(episode_num):02d}",
        'title': title,
        'special': special
    }


def find_video_files(root_path: Path) -> List[Path]:
    """Recursively find all video files, skipping system directories."""
    video_files = []
    
    try:
        for item in root_path.rglob('*'):
            if (item.is_file() and 
                item.suffix.lower() in VIDEO_EXTENSIONS and
                not any(part.startswith('.') or part in SKIP_DIRS 
                       for part in item.parts)):
                video_files.append(item)
    except (PermissionError, OSError) as e:
        logging.error(f"Error scanning directory {root_path}: {e}")
    
    return video_files


def get_database_tables() -> Set[str]:
    """Get all table names from database that have matching columns."""
    try:
        with sqlite3.connect('danger2manifold.db') as conn:
            cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = {row[0] for row in cursor.fetchall()}
            tables.discard('import_tuner')
            return tables
    except sqlite3.Error as e:
        logging.error(f"Failed to get database tables: {e}")
        return set()


def process_single_file(args: Tuple[Path, Dict[str, str]]) -> Optional[Dict[str, str]]:
    """Process a single video file and return database row data."""
    file_path, user_prefs = args
    
    parsed = parse_filename(file_path.name)
    if not parsed:
        return None
    
    checksum = calculate_sha256(file_path)
    if not checksum:
        return None
    
    # Check for library match
    library_match = check_library_match(parsed['series'])
    
    if library_match:
        it_src = library_match.get('it_src', user_prefs['it_src'])
        it_src_link = library_match.get('it_src_link', user_prefs['it_src_link'])
    else:
        it_src = user_prefs['it_src']
        it_src_link = user_prefs['it_src_link']
    
    return {
        'it_checksum': checksum,
        'file_name': file_path.name,
        'file_location': str(file_path.resolve()),
        'it_series': parsed['series'],
        'it_sea_no': f"season {int(parsed['season'])}",
        'it_ep_no': f"episode {int(parsed['episode'])}",
        'it_ep_title': parsed['title'],
        'it_special': parsed['special'],
        'it_subtitles': find_subtitle_files(file_path),
        'it_src': it_src,
        'it_src_link': it_src_link,
        'it_torrent': user_prefs['it_torrent']
    }


def get_user_input(series_name: str, user_prefs: Dict[str, str], log_enabled: bool) -> Dict[str, str]:
    """Get user input for source, link, and torrent options with timeout."""
    import signal
    
    def timeout_handler(signum, frame):
        raise TimeoutError()
    
    timeout_str = user_prefs.get('timeout', '5 seconds')
    try:
        timeout = int(timeout_str.split()[0])
    except (ValueError, IndexError):
        timeout = 5
    
    print(f"\nProcessing series: {series_name}")
    print(f"Use defaults? (y/n) - Auto-selecting 'y' in {timeout} seconds...")
    
    try:
        signal.signal(signal.SIGALRM, timeout_handler)
        signal.alarm(timeout)
        response = input('').strip().lower()
        signal.alarm(0)
    except (TimeoutError, KeyboardInterrupt, EOFError):
        response = 'y'
        print('y')
    
    if response != 'n':
        return user_prefs
    
    # Get custom input
    try:
        src = input("Enter source: ").strip() or user_prefs['it_src']
        src_link = input("Enter source link: ").strip() or user_prefs['it_src_link']
        torrent = input("Enter torrent option (series/seasons/episode/all): ").strip() or user_prefs['it_torrent']
        
        return {
            'it_src': src,
            'it_src_link': src_link,
            'it_torrent': torrent,
            'timeout': user_prefs['timeout']
        }
    except (EOFError, KeyboardInterrupt):
        return user_prefs


def save_to_database(data: List[Dict[str, str]]) -> None:
    """Save processed data to database and populate cross-tables."""
    if not data:
        return
    
    with sqlite3.connect('danger2manifold.db') as conn:
        conn.execute('PRAGMA journal_mode=WAL')
        conn.execute('PRAGMA synchronous=NORMAL')
        conn.execute('PRAGMA cache_size=10000')
        
        # Create table if not exists
        conn.execute('''
            CREATE TABLE IF NOT EXISTS import_tuner (
                it_checksum TEXT PRIMARY KEY,
                file_name TEXT,
                file_location TEXT,
                it_series TEXT,
                it_sea_no TEXT,
                it_ep_no TEXT,
                it_ep_title TEXT,
                it_special TEXT,
                it_subtitles TEXT,
                it_src TEXT,
                it_src_link TEXT,
                it_torrent TEXT,
                it_ep_avl INTEGER DEFAULT 0
            )
        ''')
        
        # Batch insert to import_tuner
        conn.executemany('''
            INSERT OR REPLACE INTO import_tuner 
            (it_checksum, file_name, file_location, it_series, it_sea_no, it_ep_no, 
             it_ep_title, it_special, it_subtitles, it_src, it_src_link, it_torrent, it_ep_avl)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', [
            (entry['it_checksum'], entry['file_name'], entry['file_location'],
             entry['it_series'], entry['it_sea_no'], entry['it_ep_no'],
             entry['it_ep_title'], entry['it_special'], entry['it_subtitles'],
             entry['it_src'], entry['it_src_link'], entry['it_torrent'], 0)
            for entry in data
        ])
        
        # Get all tables for cross-population
        cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [row[0] for row in cursor.fetchall() if row[0] != 'import_tuner']
        
        # Batch process all tables
        for table in tables:
            try:
                cursor = conn.execute(f"PRAGMA table_info(`{table}`)")
                columns = [row[1] for row in cursor.fetchall()]
                
                # Map columns efficiently
                column_mapping = [col for col in columns if col in 
                                 ['it_checksum', 'it_series', 'it_sea_no', 'it_ep_no', 'it_src', 'it_src_link']]
                
                if column_mapping:
                    placeholders = ', '.join(['?'] * len(column_mapping))
                    columns_str = ', '.join([f'`{col}`' for col in column_mapping])
                    
                    batch_data = [
                        tuple(entry[col] for col in column_mapping)
                        for entry in data
                    ]
                    
                    conn.executemany(f'''
                        INSERT OR IGNORE INTO `{table}` ({columns_str})
                        VALUES ({placeholders})
                    ''', batch_data)
                    
            except sqlite3.Error as e:
                logging.warning(f"Failed to update table {table}: {e}")
        
        logging.info(f"Saved {len(data)} records to database and updated cross-tables")


def save_to_json(data: List[Dict[str, str]]) -> None:
    """Save processed data to JSON file if enabled."""
    config = load_config()
    json_enabled = config.get('import_tuner', {}).get('json', True)
    
    if json_enabled and data:
        try:
            with open('import_tuner.json', 'w') as f:
                json.dump(data, f, indent=2)
            logging.info(f"Saved {len(data)} records to JSON")
        except OSError as e:
            logging.error(f"JSON save error: {e}")


def main():
    """Main function."""
    log_enabled = setup_logging()
    
    # Get directory
    if len(sys.argv) >= 2:
        target_path = sys.argv[1]
    else:
        try:
            target_path = input("Enter directory path: ").strip()
            if not target_path:
                if log_enabled:
                    print("No directory provided")
                logging.error("No directory provided")
                return
        except (EOFError, KeyboardInterrupt):
            if log_enabled:
                print("\nOperation cancelled")
            logging.info("User cancelled directory input")
            return
    
    # Validate directory
    target_path = target_path.replace('\\ ', ' ').strip('\'"')
    
    try:
        directory = Path(target_path).expanduser().resolve()
        if not directory.exists():
            if log_enabled:
                print(f"Error: Directory does not exist: {target_path}")
            logging.error(f"Directory does not exist: {target_path}")
            return
        
        if not directory.is_dir():
            if log_enabled:
                print(f"Error: Path is not a directory: {target_path}")
            logging.error(f"Path is not a directory: {target_path}")
            return
            
    except (OSError, RuntimeError) as e:
        if log_enabled:
            print(f"Error: Invalid directory path: {target_path}")
        logging.error(f"Invalid directory path: {target_path} - {e}")
        return
    
    # Find video files
    video_files = find_video_files(directory)
    if not video_files:
        if log_enabled:
            print(f"No video files found in: {target_path}")
        logging.info(f"No video files found in: {target_path}")
        return
    
    logging.info(f"Found {len(video_files)} video files")
    
    # Load preferences and get user input
    user_prefs = load_user_preferences()
    first_file_parsed = parse_filename(video_files[0].name)
    if first_file_parsed:
        user_prefs = get_user_input(first_file_parsed['series'], user_prefs, log_enabled)
    
    # Process files with optimized threading
    processed_data = []
    failed_count = 0
    
    import os
    # Optimal thread count for I/O bound operations
    max_workers = min(os.cpu_count() * 2, 12)
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        file_args = [(file_path, user_prefs) for file_path in video_files]
        future_to_file = {executor.submit(process_single_file, args): args[0] 
                         for args in file_args}
        
        for future in as_completed(future_to_file):
            file_path = future_to_file[future]
            try:
                result = future.result()
                if result:
                    processed_data.append(result)
                else:
                    failed_count += 1
                    logging.warning(f"Failed to process: {file_path.name}")
            except Exception as e:
                failed_count += 1
                logging.error(f"Exception processing {file_path.name}: {e}")
    
    # Save results
    if processed_data:
        save_to_database(processed_data)
        save_to_json(processed_data)
        
        if log_enabled:
            print(f"Successfully processed {len(processed_data)} files")
            if failed_count:
                print(f"Failed to process {failed_count} files")
        
        logging.info(f"Processing complete: {len(processed_data)} success, {failed_count} failed")
    else:
        if log_enabled:
            print("No files were successfully processed")
        logging.warning("No files were successfully processed")


if __name__ == '__main__':
    main()