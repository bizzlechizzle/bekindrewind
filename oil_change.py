#!/usr/bin/env python3

import sqlite3
import json
import logging
import os
import shutil
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
import re

def setup_logging():
    """Setup logging based on config"""
    try:
        with open('2jznoshit.json', 'r') as f:
            log_enabled = json.load(f).get('oil_change', {}).get('logs', False)
    except (FileNotFoundError, json.JSONDecodeError):
        log_enabled = False
    
    if log_enabled:
        logging.basicConfig(
            filename='oil_change.log',
            level=logging.DEBUG,
            format='%(asctime)s - %(levelname)s - %(message)s',
            filemode='a'
        )
    else:
        logging.disable(logging.CRITICAL)

def get_config():
    """Load configuration"""
    try:
        with open('2jznoshit.json', 'r') as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {'user_input': {'default': {'def_loc': '/tmp'}}}

def get_db_data():
    """Get all import_tuner rows"""
    if not os.path.exists('danger2manifold.db'):
        logging.error("Database file not found")
        return []
    
    try:
        conn = sqlite3.connect('danger2manifold.db')
        conn.row_factory = sqlite3.Row
        cursor = conn.execute("SELECT * FROM import_tuner")
        data = cursor.fetchall()
        conn.close()
        return data
    except sqlite3.Error as e:
        logging.error(f"Database error: {e}")
        return []

def get_media_files(file_path):
    """Get media files - handle both files and directories"""
    video_exts = {'.mp4', '.mkv', '.avi', '.mov', '.wmv', '.flv', '.webm', '.m4v', '.ts', '.m2ts'}
    subtitle_exts = {'.srt', '.ass', '.ssa', '.sub', '.idx', '.vtt', '.sup'}
    
    path = Path(file_path)
    if not path.exists():
        return []
    
    media_files = []
    
    try:
        if path.is_file():
            # Single file - check if it's a video file and add it
            if path.suffix.lower() in video_exts:
                media_files.append(path)
                # Look for subtitle files with same name in same directory
                for sub_file in path.parent.glob(f"{path.stem}.*"):
                    if sub_file.suffix.lower() in subtitle_exts:
                        media_files.append(sub_file)
        elif path.is_dir():
            # Directory - find all media files and clean non-media
            for file in path.rglob('*'):
                if file.is_file():
                    if file.suffix.lower() in video_exts or file.suffix.lower() in subtitle_exts:
                        media_files.append(file)
                    else:
                        try:
                            file.unlink()
                        except OSError:
                            pass
            
            # Remove empty directories
            for dir_path in sorted(path.rglob('*'), key=lambda x: len(x.parts), reverse=True):
                if dir_path.is_dir():
                    try:
                        dir_path.rmdir()
                    except OSError:
                        pass
    except OSError as e:
        logging.warning(f"Error processing path {file_path}: {e}")
    
    return media_files

def normalize_name(name):
    """Normalize names - title case, no duplicates, no invalid chars"""
    if not name:
        return ""
    
    # Remove duplicate words (case insensitive)
    words = []
    seen = set()
    for word in name.split():
        word_lower = word.lower()
        if word_lower not in seen:
            seen.add(word_lower)
            words.append(word.title())
    
    result = ' '.join(words)
    # Remove invalid filesystem characters
    invalid_chars = '<>:"/\\|?*'
    for char in invalid_chars:
        result = result.replace(char, '')
    
    return result.strip()

def get_folder_structure(row, torrent_pref):
    """Get target folder structure based on torrent preference"""
    series = normalize_name(row['it_series'])
    
    # Extract numbers from season/episode strings
    season_match = re.search(r'(\d+)', str(row['it_sea_no'] or '1'))
    episode_match = re.search(r'(\d+)', str(row['it_ep_no'] or '1'))
    
    season_num = int(season_match.group(1)) if season_match else 1
    episode_num = int(episode_match.group(1)) if episode_match else 1
    
    season = f"Season {season_num:02d}"
    episode = f"Episode {episode_num:02d}"
    
    if torrent_pref in ['series', 'all']:
        return series, f"{series}/{season}"
    elif torrent_pref == 'season':
        return f"{series} - {season}", f"{series} - {season}"
    elif torrent_pref == 'episode':
        return f"{series} - {season} - {episode}", f"{series} - {season} - {episode}"
    else:
        return series, f"{series}/{season}"

def copy_files(media_files, target_path):
    """Copy media files to target location"""
    if not media_files:
        return []
    
    target = Path(target_path)
    try:
        target.mkdir(parents=True, exist_ok=True)
    except OSError as e:
        logging.error(f"Failed to create target directory {target}: {e}")
        return []
    
    copied = []
    for file in media_files:
        try:
            dest = target / file.name
            shutil.copy2(file, dest)
            copied.append(dest)
        except OSError as e:
            logging.error(f"Copy failed {file} -> {dest}: {e}")
    
    return copied

def process_row(args):
    """Process single row"""
    row, def_loc = args
    
    try:
        checksum = row['it_checksum']
        source_path = row['file_location']
        torrent_pref = row['it_torrent']
        
        # Check if source path exists
        if not source_path or not Path(source_path).exists():
            logging.warning(f"Source path missing or not found: {source_path}")
            return None
        
        # Get media files (handles both files and directories)
        media_files = get_media_files(source_path)
        if not media_files:
            logging.warning(f"No media files found at: {source_path}")
            return None
        
        # Get folder structure
        folder_name, relative_path = get_folder_structure(row, torrent_pref)
        
        # Copy to target location
        target_path = Path(def_loc) / relative_path
        copied_files = copy_files(media_files, target_path)
        
        if not copied_files:
            logging.error(f"Failed to copy files for {checksum}")
            return None
        
        logging.info(f"Successfully processed {checksum}: {len(copied_files)} files copied")
        return (checksum, str(target_path), torrent_pref, folder_name)
        
    except Exception as e:
        logging.error(f"Exception processing {row['it_checksum']}: {e}")
        return None

def record_oil_change(results):
    """Record results to oil_change table"""
    if not results:
        return
    
    try:
        conn = sqlite3.connect('danger2manifold.db')
        
        # Delete existing entries
        checksums = [r[0] for r in results]
        placeholders = ','.join('?' * len(checksums))
        conn.execute(f"DELETE FROM oil_change WHERE it_checksum IN ({placeholders})", checksums)
        
        # Insert new entries
        conn.executemany(
            "INSERT INTO oil_change (it_checksum, file_loc, it_torrent, oc_name) VALUES (?, ?, ?, ?)",
            results
        )
        conn.commit()
        conn.close()
        logging.info(f"Recorded {len(results)} entries to oil_change table")
    except sqlite3.Error as e:
        logging.error(f"Database error recording results: {e}")

def main():
    """Main function"""
    setup_logging()
    logging.info("Starting oil_change process")
    
    # Load config
    config = get_config()
    def_loc = config.get('user_input', {}).get('default', {}).get('def_loc', '/tmp')
    
    # Get data
    import_data = get_db_data()
    if not import_data:
        print("No data found in import_tuner table")
        logging.info("No data found in import_tuner table")
        return
    
    print(f"Processing {len(import_data)} entries...")
    
    # Process with optimal thread count
    max_workers = min(os.cpu_count() or 1, 4)  # Reduce to avoid I/O bottleneck
    results = []
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        args_list = [(row, def_loc) for row in import_data]
        future_to_index = {executor.submit(process_row, args): i for i, args in enumerate(args_list)}
        
        completed = 0
        for future in future_to_index:
            try:
                result = future.result(timeout=30)  # 30 second timeout per file
                i = future_to_index[future]
                completed += 1
                
                if result:
                    results.append(result)
                    print(f"Success {completed}/{len(import_data)}: {result[0][:8]}...")
                else:
                    print(f"Failed {completed}/{len(import_data)}: {import_data[i]['it_checksum'][:8]}...")
            except Exception as e:
                i = future_to_index[future]
                completed += 1
                print(f"Error {completed}/{len(import_data)}: {import_data[i]['it_checksum'][:8]}... - {e}")
    
    # Record results
    record_oil_change(results)
    
    print(f"Complete: {len(results)}/{len(import_data)} successful")
    logging.info(f"Process completed: {len(results)}/{len(import_data)} successful")

if __name__ == "__main__":
    main()