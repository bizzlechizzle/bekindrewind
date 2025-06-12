#!/usr/bin/env python3

import json
import sqlite3
import os
import shutil
import logging
from pathlib import Path
import sys

VIDEO_EXTS = frozenset(['.mkv', '.mp4', '.avi', '.mov', '.wmv', '.flv', '.webm', '.m4v'])
SUBTITLE_EXTS = frozenset(['.srt', '.ass', '.ssa', '.vtt', '.sub', '.idx', '.sup'])

def setup_logging():
    try:
        with open('2jznoshit.json', 'r') as f:
            log_enabled = json.load(f).get('oil_change', {}).get('logs', False)
    except:
        log_enabled = False
    
    if log_enabled:
        logging.basicConfig(
            level=logging.DEBUG,
            format='%(asctime)s - %(levelname)s - %(message)s',
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
        return Path(def_loc)
    except Exception as e:
        logging.error(f"Config load failed: {e}")
        sys.exit(1)

def get_import_tuner_data():
    try:
        with sqlite3.connect('danger2manifold.db') as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM import_tuner")
            return cursor.fetchall()
    except sqlite3.Error as e:
        logging.error(f"Database error: {e}")
        sys.exit(1)

def is_media_file(path):
    return path.suffix.lower() in VIDEO_EXTS or path.suffix.lower() in SUBTITLE_EXTS

def clean_folder(folder_path):
    """Remove non-media files and empty directories"""
    if not folder_path.exists():
        return
    
    # Remove non-media files
    for root, dirs, files in os.walk(folder_path):
        for file in files:
            file_path = Path(root) / file
            if not is_media_file(file_path):
                file_path.unlink(missing_ok=True)
    
    # Remove empty directories (bottom-up)
    for root, dirs, files in os.walk(folder_path, topdown=False):
        for dir_name in dirs:
            dir_path = Path(root) / dir_name
            try:
                if not any(dir_path.iterdir()):
                    dir_path.rmdir()
            except OSError:
                pass

def copy_media_files(src_path, dst_path):
    """Copy all media files from src to dst"""
    dst_path.mkdir(parents=True, exist_ok=True)
    
    for root, dirs, files in os.walk(src_path):
        for file in files:
            src_file = Path(root) / file
            if is_media_file(src_file):
                dst_file = dst_path / file
                shutil.copy2(src_file, dst_file)

def record_location(checksum, file_loc, torrent_type):
    """Record new location in oil_change table"""
    try:
        with sqlite3.connect('danger2manifold.db') as conn:
            cursor = conn.cursor()
            cursor.execute(
                "INSERT OR REPLACE INTO oil_change (it_checksum, file_loc, it_torrent) VALUES (?, ?, ?)",
                (checksum, str(file_loc), torrent_type)
            )
            conn.commit()
    except sqlite3.Error as e:
        logging.error(f"Failed to record location: {e}")

def process_row(row, def_loc):
    """Process single import_tuner row"""
    src_path = Path(row['file_location'])
    if not src_path.exists():
        logging.error(f"Source path missing: {src_path}")
        return
    
    # Clean source folder
    clean_folder(src_path)
    
    torrent_pref = row['it_torrent']
    series = row['it_series']
    season = int(row['it_sea_no']) if row['it_sea_no'] else 1
    episode = int(row['it_ep_no']) if row['it_ep_no'] else 1
    checksum = row['it_checksum']
    
    if torrent_pref in ['series', 'all']:
        # Keep original structure
        dst_path = def_loc / src_path.name
        if dst_path.exists():
            shutil.rmtree(dst_path)
        shutil.copytree(src_path, dst_path)
        
    elif torrent_pref == 'season':
        # Season folder: "Series - Season ##"
        folder_name = f"{series} - Season {season:02d}"
        dst_path = def_loc / folder_name
        copy_media_files(src_path, dst_path)
        
    elif torrent_pref == 'episode':
        # Episode folder: "Series - Season ## - Episode ##"
        folder_name = f"{series} - Season {season:02d} - Episode {episode:02d}"
        dst_path = def_loc / folder_name
        copy_media_files(src_path, dst_path)
        
    else:
        logging.error(f"Unknown torrent preference: {torrent_pref}")
        return
    
    record_location(checksum, dst_path, torrent_pref)
    logging.info(f"Processed {series} S{season:02d}E{episode:02d} -> {dst_path}")

def main():
    setup_logging()
    def_loc = load_config()
    def_loc.mkdir(parents=True, exist_ok=True)
    
    rows = get_import_tuner_data()
    if not rows:
        logging.warning("No data in import_tuner table")
        return
    
    for row in rows:
        try:
            process_row(row, def_loc)
        except Exception as e:
            logging.error(f"Failed processing {row.get('it_checksum', 'unknown')}: {e}")

if __name__ == "__main__":
    main()