#!/usr/bin/env python3

import json
import sqlite3
import os
import shutil
import logging
from pathlib import Path
import sys
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

VIDEO_EXTS = frozenset(['.mkv', '.mp4', '.avi', '.mov', '.wmv', '.flv', '.webm', '.m4v'])
SUBTITLE_EXTS = frozenset(['.srt', '.ass', '.ssa', '.vtt', '.sub', '.idx', '.sup'])
MEDIA_EXTS = VIDEO_EXTS | SUBTITLE_EXTS

_log_lock = threading.Lock()

def setup_logging():
    try:
        with open('2jznoshit.json', 'r') as f:
            log_enabled = json.load(f).get('oil_change', {}).get('logs', False)
    except:
        log_enabled = False
    
    if log_enabled:
        logging.basicConfig(
            level=logging.DEBUG,
            format='%(asctime)s - %(threadName)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('oil_change.log'),
                logging.StreamHandler()
            ]
        )
    else:
        logging.basicConfig(level=logging.CRITICAL)

def load_config():
    try:
        with open('2jznoshit.json', 'r') as f:
            config = json.load(f)
        def_loc = config.get('user_input', {}).get('default', {}).get('def_loc', './processed')
        return Path(def_loc).resolve()
    except Exception as e:
        logging.error(f"Config load failed: {e}")
        sys.exit(1)

def get_import_tuner_data():
    try:
        with sqlite3.connect('danger2manifold.db') as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM import_tuner")
            return cursor.fetchall()
    except sqlite3.Error as e:
        logging.error(f"Database error: {e}")
        sys.exit(1)

def extract_number(value):
    if not value:
        return 1
    match = re.search(r'\d+', str(value))
    return int(match.group()) if match else 1

def normalize_name(text):
    if not text:
        return ""
    
    words = re.split(r'[\s\-_]+', str(text).strip())
    normalized_words = []
    seen = set()
    
    for word in words:
        word_clean = word.strip().title()
        if word_clean and word_clean.lower() not in seen:
            normalized_words.append(word_clean)
            seen.add(word_clean.lower())
    
    return ' '.join(normalized_words)

def find_media_files(src_path):
    """Find all media files in source path - handles both files and directories"""
    media_files = []
    
    if not src_path.exists():
        return media_files
    
    if src_path.is_file():
        # Single file case
        if src_path.suffix.lower() in MEDIA_EXTS:
            media_files.append(src_path)
    else:
        # Directory case - walk recursively
        for root, dirs, files in os.walk(src_path):
            for file in files:
                file_path = Path(root) / file
                if file_path.suffix.lower() in MEDIA_EXTS:
                    media_files.append(file_path)
    
    return media_files

def clean_folder_recursive(folder_path):
    """Remove non-media files and empty directories recursively"""
    if not folder_path.exists() or folder_path.is_file():
        return
    
    files_to_remove = []
    for root, dirs, files in os.walk(folder_path):
        for file in files:
            file_path = Path(root) / file
            if file_path.suffix.lower() not in MEDIA_EXTS:
                files_to_remove.append(file_path)
    
    for file_path in files_to_remove:
        try:
            file_path.unlink()
        except OSError:
            pass
    
    for root, dirs, files in os.walk(folder_path, topdown=False):
        for dir_name in dirs:
            dir_path = Path(root) / dir_name
            try:
                if not any(dir_path.iterdir()):
                    dir_path.rmdir()
            except OSError:
                pass

def copy_media_files(src_path, dst_path, torrent_pref):
    """Copy media files based on torrent preference"""
    dst_path.mkdir(parents=True, exist_ok=True)
    
    if src_path.is_file():
        # Single file - copy directly
        dst_file = dst_path / src_path.name
        try:
            if not (dst_file.exists() and src_path.stat().st_size == dst_file.stat().st_size):
                shutil.copy2(src_path, dst_file)
        except (OSError, IOError) as e:
            logging.error(f"Failed to copy {src_path} to {dst_file}: {e}")
    else:
        # Directory - copy based on preference
        if torrent_pref in ['series', 'all']:
            # Keep original structure
            if dst_path.exists():
                shutil.rmtree(dst_path)
            shutil.copytree(src_path, dst_path, dirs_exist_ok=True)
        else:
            # Copy media files preserving relative structure
            for root, dirs, files in os.walk(src_path):
                for file in files:
                    src_file = Path(root) / file
                    if src_file.suffix.lower() in MEDIA_EXTS:
                        rel_path = src_file.relative_to(src_path)
                        dst_file = dst_path / rel_path
                        dst_file.parent.mkdir(parents=True, exist_ok=True)
                        
                        try:
                            if not (dst_file.exists() and src_file.stat().st_size == dst_file.stat().st_size):
                                shutil.copy2(src_file, dst_file)
                        except (OSError, IOError) as e:
                            logging.error(f"Failed to copy {src_file} to {dst_file}: {e}")

def record_location(checksum, file_loc, torrent_type):
    """Record new location in oil_change table"""
    try:
        with sqlite3.connect('danger2manifold.db', timeout=30.0) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "INSERT OR REPLACE INTO oil_change (it_checksum, file_loc, it_torrent) VALUES (?, ?, ?)",
                (checksum, str(file_loc), torrent_type)
            )
            conn.commit()
    except sqlite3.Error as e:
        with _log_lock:
            logging.error(f"Failed to record location for {checksum}: {e}")

def process_single_row(row, def_loc):
    """Process single import_tuner row"""
    try:
        checksum = row[0]
        file_name = row[1]
        file_location = row[2]
        series = row[3]
        sea_no = row[4]
        ep_no = row[5]
        ep_title = row[6]
        special = row[7]
        subtitles = row[8]
        src = row[9]
        src_link = row[10]
        torrent_pref = row[11]
        
        src_path = Path(file_location).resolve()
        if not src_path.exists():
            logging.error(f"Source path missing: {src_path}")
            return False
        
        # Find media files
        media_files = find_media_files(src_path)
        
        with _log_lock:
            logging.debug(f"Found {len(media_files)} media files in {src_path}")
        
        if not media_files:
            logging.warning(f"No media files found in {src_path}")
            return False
        
        # Extract and normalize values
        season = extract_number(sea_no)
        episode = extract_number(ep_no)
        series_norm = normalize_name(series)
        
        if not series_norm:
            logging.error(f"Invalid series name for checksum {checksum}")
            return False
        
        # Determine destination based on torrent preference
        if torrent_pref in ['series', 'all']:
            # Keep original structure
            dst_path = def_loc / src_path.name
        elif torrent_pref == 'season':
            # Season folder: "Series - Season ##"
            folder_name = f"{series_norm} - Season {season:02d}"
            dst_path = def_loc / folder_name
        elif torrent_pref == 'episode':
            # Episode folder: "Series - Season ## - Episode ##"
            folder_name = f"{series_norm} - Season {season:02d} - Episode {episode:02d}"
            dst_path = def_loc / folder_name
        else:
            logging.error(f"Unknown torrent preference: {torrent_pref}")
            return False
        
        # Copy files
        copy_media_files(src_path, dst_path, torrent_pref)
        
        # Clean destination if needed
        if torrent_pref in ['series', 'all']:
            clean_folder_recursive(dst_path)
        
        # Verify files were copied
        copied_files = find_media_files(dst_path)
        copied_count = len(copied_files)
        
        with _log_lock:
            logging.debug(f"Copied {copied_count} media files to {dst_path}")
        
        if copied_count == 0:
            logging.error(f"No files were copied to {dst_path}")
            return False
        
        # Record new location
        record_location(checksum, dst_path, torrent_pref)
        
        with _log_lock:
            logging.info(f"Processed {series_norm} S{season:02d}E{episode:02d} -> {dst_path} ({copied_count} files)")
        
        return True
        
    except Exception as e:
        checksum = row[0] if row else 'unknown'
        with _log_lock:
            logging.error(f"Failed processing checksum {checksum}: {e}")
        return False

def main():
    setup_logging()
    
    def_loc = load_config()
    def_loc.mkdir(parents=True, exist_ok=True)
    
    rows = get_import_tuner_data()
    if not rows:
        logging.warning("No data in import_tuner table")
        return
    
    max_workers = min(4, os.cpu_count() or 1)
    successful = 0
    failed = 0
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_row = {
            executor.submit(process_single_row, row, def_loc): i 
            for i, row in enumerate(rows)
        }
        
        for future in as_completed(future_to_row):
            row_index = future_to_row[future]
            try:
                if future.result():
                    successful += 1
                else:
                    failed += 1
            except Exception as e:
                failed += 1
                logging.error(f"Unexpected error processing row {row_index}: {e}")
    
    logging.info(f"Processing complete: {successful} successful, {failed} failed")

if __name__ == "__main__":
    main()