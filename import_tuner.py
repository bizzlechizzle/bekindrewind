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
from typing import Dict, List, Optional, Tuple

# Configuration
VIDEO_EXTENSIONS = frozenset(['.mkv', '.mp4', '.avi', '.mov', '.wmv', '.flv', '.webm', '.m4v'])
SUBTITLE_EXTENSIONS = frozenset(['.srt', '.ass', '.ssa', '.sub', '.vtt', '.sbv', '.scc', '.ttml', '.dfxp'])
SKIP_DIRS = frozenset(['.DS_Store', 'Thumbs.db', '.AppleDouble', '.LSOverride', 
                       'desktop.ini', '.Spotlight-V100', '.Trashes', '.fseventsd'])
SPECIAL_KEYWORDS = frozenset(['sneak peek', 'teaser', 'trailer', 'extended edition', 'behind the scenes',
                              'making of', 'deleted scenes', 'blooper', 'bloopers', 'gag reel',
                              "director's cut", 'directors cut', 'uncut', 'bonus', 'extra', 'preview',
                              'directors', 'sneak peak'])

# Pre-compile regex - matches Series_SxxExx_Remainder
FILENAME_PATTERN = re.compile(r'^(.+?)_S(\d+)E(\d+)_(.*)$')

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
        for item in video_path.parent.iterdir():
            if (item.is_file() and 
                item.suffix.lower() in SUBTITLE_EXTENSIONS and
                item.stem.startswith(video_stem)):
                return 'yes'
    except (PermissionError, OSError):
        pass
    return ''


def parse_filename(filename: str) -> Optional[Dict[str, str]]:
    """Parse TV show filename following pattern: Series_SxxExx_Special_Title."""
    stem = Path(filename).stem
    
    match = FILENAME_PATTERN.match(stem)
    if not match:
        logging.warning(f"Filename does not match pattern Series_SxxExx_Remainder: {filename}")
        return None
    
    series, season_num, episode_num, remainder = match.groups()
    series = series.replace('_', ' ')
    
    # Parse special and title from remainder
    special = ""
    title = remainder
    
    if remainder:
        remainder_lower = remainder.lower()
        
        for keyword in SPECIAL_KEYWORDS:
            keyword_underscore = keyword.replace(' ', '_')
            if remainder_lower.startswith(keyword.lower()):
                # Check if there's content after the keyword
                if len(remainder) > len(keyword_underscore) and remainder[len(keyword_underscore)] == '_':
                    special = remainder[:len(keyword_underscore)].replace('_', ' ')
                    title = remainder[len(keyword_underscore) + 1:]
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


def process_single_file(args: Tuple[Path, Dict[str, str]]) -> Optional[Dict[str, str]]:
    """Process a single video file and return database row data."""
    file_path, user_prefs = args
    
    parsed = parse_filename(file_path.name)
    if not parsed:
        logging.warning(f"Failed to parse filename: {file_path.name}")
        return None
    
    checksum = calculate_sha256(file_path)
    if not checksum:
        logging.warning(f"Failed to calculate checksum: {file_path.name}")
        return None
    
    # Check for library match
    library_match = check_library_match(parsed['series'])
    
    if library_match:
        it_src = library_match.get('it_src', user_prefs['it_src'])
        it_src_link = library_match.get('it_src_link', user_prefs['it_src_link'])
        logging.info(f"Library match found for series: {parsed['series']}")
    else:
        it_src = user_prefs['it_src']
        it_src_link = user_prefs['it_src_link']
    
    result = {
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
    
    logging.info(f"Successfully processed: {file_path.name}")
    return result


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


def save_to_database(data: List[Dict[str, str]], log_enabled: bool) -> None:
    """Save processed data to database and populate ALL tables with matching it_ columns."""
    if not data:
        return
    
    with sqlite3.connect('danger2manifold.db') as conn:
        conn.execute('PRAGMA journal_mode=WAL')
        conn.execute('PRAGMA synchronous=NORMAL')
        conn.execute('PRAGMA cache_size=10000')
        
        # Create import_tuner table if not exists
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
                it_ep_avl INTEGER DEFAULT 0,
                it_def_loc TEXT
            )
        ''')
        
        # Get existing checksums
        existing_checksums = set(row[0] for row in conn.execute('SELECT it_checksum FROM import_tuner'))
        
        # Filter duplicates
        new_data = []
        duplicate_count = 0
        
        for entry in data:
            if entry['it_checksum'] in existing_checksums:
                duplicate_count += 1
                logging.warning(f"Duplicate checksum found: {entry['file_name']}")
            else:
                new_data.append(entry)
        
        # Insert new records into import_tuner
        if new_data:
            try:
                conn.executemany('''
                    INSERT INTO import_tuner 
                    (it_checksum, file_name, file_location, it_series, it_sea_no, it_ep_no, 
                     it_ep_title, it_special, it_subtitles, it_src, it_src_link, it_torrent, it_ep_avl, it_def_loc)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', [
                    (entry['it_checksum'], entry['file_name'], entry['file_location'],
                     entry['it_series'], entry['it_sea_no'], entry['it_ep_no'],
                     entry['it_ep_title'], entry['it_special'], entry['it_subtitles'],
                     entry['it_src'], entry['it_src_link'], entry['it_torrent'], 0, entry['file_location'])
                    for entry in new_data
                ])
                logging.info(f"Inserted {len(new_data)} records into import_tuner")
            except sqlite3.Error as e:
                logging.error(f"Failed to insert into import_tuner: {e}")
                return
            
            # Get all other tables in database
            tables = [row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'") 
                     if row[0] != 'import_tuner']
            
            # For each table, find ALL it_ columns and fill them
            for table in tables:
                try:
                    columns_info = conn.execute(f"PRAGMA table_info(`{table}`)").fetchall()
                    table_columns = [col[1] for col in columns_info]  # col[1] is column name
                    
                    # Find ALL columns that start with "it_" 
                    it_columns = [col for col in table_columns if col.startswith('it_')]
                    
                    if not it_columns:
                        logging.info(f"Table '{table}' has no 'it_' columns")
                        continue
                    
                    # Special handling for oil_change table column mappings
                    if table == 'oil_change':
                        column_mappings = []
                        if 'it_checksum' in table_columns:
                            column_mappings.append(('it_checksum', 'it_checksum'))
                        if 'file_loc' in table_columns:
                            column_mappings.append(('file_location', 'file_loc'))
                        if 'it_torrent' in table_columns:
                            column_mappings.append(('it_torrent', 'it_torrent'))
                    else:
                        # For all other tables, match it_ columns directly
                        column_mappings = [(col, col) for col in it_columns if col in 
                                         ['it_checksum', 'it_series', 'it_sea_no', 'it_ep_no', 'it_ep_title', 
                                          'it_special', 'it_subtitles', 'it_src', 'it_src_link', 'it_torrent']]
                    
                    if column_mappings:
                        # Build INSERT query
                        db_columns = [db_col for _, db_col in column_mappings]
                        placeholders = ', '.join(['?'] * len(db_columns))
                        columns_str = ', '.join([f'`{col}`' for col in db_columns])
                        
                        # Prepare data rows
                        rows = []
                        for entry in new_data:
                            row = []
                            for data_col, _ in column_mappings:
                                value = entry.get(data_col, '')
                                row.append(value)
                            rows.append(tuple(row))
                        
                        # Insert data
                        conn.executemany(f'''
                            INSERT OR REPLACE INTO `{table}` ({columns_str})
                            VALUES ({placeholders})
                        ''', rows)
                        
                        logging.info(f"Updated table '{table}' with {len(column_mappings)} it_ columns: {db_columns}")
                    else:
                        logging.info(f"Table '{table}' has it_ columns but none match our data: {it_columns}")
                        
                except sqlite3.Error as e:
                    logging.error(f"Failed to update table '{table}': {e}")
                except Exception as e:
                    logging.error(f"Unexpected error updating table '{table}': {e}")
        
        # Report results
        if log_enabled and duplicate_count > 0:
            print(f"Skipped {duplicate_count} duplicate files")
        
        logging.info(f"Saved {len(new_data)} new records, skipped {duplicate_count} duplicates")


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


def clean_drag_drop_path(path_str: str) -> str:
    """Clean up messy drag and drop paths."""
    if not path_str:
        return path_str
    
    cleaned = path_str.strip().strip('\'"')
    
    # Test if path exists as-is first
    try:
        if Path(cleaned).expanduser().exists():
            return cleaned
    except (OSError, RuntimeError):
        pass
    
    # Try unescaping common shell escapes
    unescaped = cleaned.replace('\\ ', ' ').replace("\\'", "'").replace('\\"', '"').replace('\\!', '!').replace('\\(', '(').replace('\\)', ')').replace('\\[', '[').replace('\\]', ']').replace('\\&', '&').replace('\\\\', '\\')
    
    try:
        if Path(unescaped).expanduser().exists():
            return unescaped
    except (OSError, RuntimeError):
        pass
    
    return cleaned


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
    
    # Clean up drag and drop path
    target_path = clean_drag_drop_path(target_path)
    
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
        save_to_database(processed_data, log_enabled)
        save_to_json(processed_data)
        
        if log_enabled:
            print(f"Successfully processed {len(processed_data)} files")
            if failed_count:
                print(f"Failed to process {failed_count} files")
        
        logging.info(f"Processing complete: {len(processed_data)} success, {failed_count} failed")
    else:
        if log_enabled:
            print(f"No files were successfully processed. Found {len(video_files)} video files, {failed_count} failed processing")
        logging.warning(f"No files were successfully processed. Found {len(video_files)} video files, {failed_count} failed processing")
        
        # Debug: Log first few filenames to check parsing
        if video_files and log_enabled:
            print("Sample filenames found:")
            for i, vf in enumerate(video_files[:3]):
                print(f"  {i+1}. {vf.name}")
                parsed = parse_filename(vf.name)
                print(f"     Parsed: {parsed}")
                if not parsed:
                    print(f"     ERROR: Filename doesn't match pattern Series_SxxExx_Title")


if __name__ == '__main__':
    main()