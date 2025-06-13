#!/usr/bin/env python3

import sqlite3
import json
import logging
import os
import re
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

def setup_logging():
    try:
        with open('2jznoshit.json', 'r') as f:
            config = json.load(f)
            log_enabled = config.get('fast_sev', {}).get('logs', False)
    except:
        log_enabled = False
    
    if log_enabled:
        logging.basicConfig(
            filename='fast_sev.log',
            level=logging.DEBUG,
            format='%(asctime)s - %(threadName)s - %(levelname)s - %(message)s',
            filemode='a'
        )
        logging.info("Logging enabled for fast_sev")
    else:
        logging.disable(logging.CRITICAL)

def normalize_series_name(series_name):
    """Normalize series name for torrent compatibility - remove problematic punctuation"""
    if not series_name:
        return ""
    
    # Remove apostrophes, quotes, question marks entirely
    normalized = re.sub(r"['\"`?!]", "", series_name)
    
    # Replace other punctuation with dots
    normalized = re.sub(r"[^\w\s\-]", ".", normalized)
    
    # Replace spaces with dots
    normalized = re.sub(r"\s+", ".", normalized)
    
    # Clean up multiple dots
    normalized = re.sub(r"\.+", ".", normalized)
    
    # Remove leading/trailing dots
    normalized = normalized.strip(".")
    
    return normalized

def normalize_season_episode(sea_no, ep_no):
    """Convert season/episode to S##E## format"""
    # Extract numbers from season and episode strings
    season_match = re.search(r'(\d+)', str(sea_no))
    episode_match = re.search(r'(\d+)', str(ep_no))
    
    if season_match and episode_match:
        s_num = int(season_match.group(1))
        e_num = int(episode_match.group(1))
        return f"S{s_num:02d}E{e_num:02d}"
    return None

def build_filename(qm_data, name_type):
    """Build filename based on naming conventions"""
    series = normalize_series_name(qm_data['qm_series'])
    sea_no = qm_data['qm_sea_no']
    ep_no = qm_data['qm_ep_no'] 
    res = qm_data['qm_res']
    hdr = qm_data['qm_hdr']
    vid_bac = qm_data['qm_vid_bac']
    aud_cdc = qm_data['qm_aud_cdc']
    aud_chn = qm_data['qm_aud_chn']
    src_short = qm_data['qm_src_short']
    rga = qm_data['qm_rga']
    
    # Handle HDR - only include if HDR, skip if SDR
    hdr_part = ""
    if hdr and 'HDR' in hdr.upper():
        hdr_part = ".HDR"
    
    # Handle audio channels - only include 5.1 or 7.1
    aud_chn_part = ""
    if aud_chn and ('5.1' in aud_chn or '7.1' in aud_chn):
        aud_chn_part = f".{aud_chn}"
    
    if name_type == 'series':
        # Check for multiple seasons
        season_nums = re.findall(r'(\d+)', sea_no)
        if len(season_nums) > 1:
            sea_part = f"S{season_nums[0]:0>2}-S{season_nums[-1]:0>2}"
        else:
            sea_part = f"S{season_nums[0]:0>2}" if season_nums else "S01"
        
        return f"{series}.{sea_part}.{res}{hdr_part}.{src_short}.{vid_bac}.{aud_cdc}{aud_chn_part}-{rga}"
    
    elif name_type == 'season':
        season_num = re.search(r'(\d+)', sea_no)
        sea_part = f"S{season_num.group(1):0>2}" if season_num else "S01"
        
        return f"{series}.{sea_part}.{res}{hdr_part}.{src_short}.{vid_bac}.{aud_cdc}{aud_chn_part}-{rga}"
    
    elif name_type == 'episode':
        se_format = normalize_season_episode(sea_no, ep_no)
        if not se_format:
            return None
        
        return f"{series}.{se_format}.{res}{hdr_part}.{src_short}.{vid_bac}.{aud_cdc}{aud_chn_part}-{rga}"
    
    return None

def safe_rename(old_path, new_path):
    """Safely rename file/folder with conflict handling"""
    old_path = Path(old_path)
    new_path = Path(new_path)
    
    if not old_path.exists():
        logging.error(f"Source path does not exist: {old_path}")
        return False
    
    if new_path.exists():
        counter = 1
        stem = new_path.stem
        suffix = new_path.suffix
        parent = new_path.parent
        
        while new_path.exists():
            if suffix:
                new_name = f"{stem}_{counter}{suffix}"
            else:
                new_name = f"{stem}_{counter}"
            new_path = parent / new_name
            counter += 1
        
        logging.warning(f"Target exists, using: {new_path}")
    
    try:
        old_path.rename(new_path)
        logging.info(f"Renamed: {old_path} -> {new_path}")
        return str(new_path)
    except Exception as e:
        logging.error(f"Rename failed: {old_path} -> {new_path}: {e}")
        return False

def process_files(checksum, import_data, qm_data):
    """Process renaming for a single checksum"""
    logging.debug(f"Processing checksum: {checksum}")
    
    current_location = Path(import_data['it_def_loc'])
    torrent_type = import_data['it_torrent']
    has_subtitles = bool(import_data.get('it_subtitles'))
    
    # If exact path doesn't exist, try to find similar folder structure
    if not current_location.exists():
        parent_dir = current_location.parent
        filename = current_location.name
        
        # Look for folders with similar names in parent directory
        if parent_dir.exists():
            for folder in parent_dir.iterdir():
                if folder.is_dir():
                    potential_file = folder / filename
                    if potential_file.exists():
                        current_location = potential_file
                        logging.info(f"Found file at: {current_location}")
                        break
        
        # If still not found, search more broadly
        if not current_location.exists():
            base_path = Path("/Volumes/barbossa/upload/tor")
            if base_path.exists():
                for item in base_path.rglob(filename):
                    if item.is_file():
                        current_location = item
                        logging.info(f"Found file via search: {current_location}")
                        break
    
    if not current_location.exists():
        logging.error(f"File not found after search: {current_location}")
        return None
    
    # Determine naming type based on torrent structure
    if torrent_type == 'all' or torrent_type == 'series':
        name_type = 'episode'  # Individual files get episode naming
    elif torrent_type == 'season':
        name_type = 'episode'
    else:  # per episode
        name_type = 'episode'
    
    # Build new filename
    new_filename = build_filename(qm_data, name_type)
    if not new_filename:
        logging.error(f"Could not build filename for {checksum}")
        return None
    
    # Get file extension and build full new path
    file_ext = current_location.suffix
    new_file_path = current_location.parent / f"{new_filename}{file_ext}"
    
    # Rename video file
    renamed_video = safe_rename(current_location, new_file_path)
    if not renamed_video:
        return None
    
    new_location = renamed_video
    
    # Handle .nfo file - search for any .nfo in the same directory
    current_folder = current_location.parent
    for nfo_file in current_folder.glob("*.nfo"):
        if nfo_file.exists():
            new_nfo_name = build_filename(qm_data, 'season' if torrent_type == 'season' else 'series')
            if new_nfo_name:
                new_nfo_path = current_folder / f"{new_nfo_name}.nfo"
                safe_rename(nfo_file, new_nfo_path)
    
    # Handle subtitle files
    if has_subtitles:
        subtitle_extensions = ['.srt', '.sub', '.idx', '.vtt', '.ass']
        for ext in subtitle_extensions:
            sub_path = current_location.with_suffix(ext)
            if sub_path.exists():
                new_sub_path = Path(renamed_video).with_suffix(ext)
                safe_rename(sub_path, new_sub_path)
    
    # Handle folder renaming based on torrent type
    current_folder = current_location.parent
    
    if torrent_type == 'all' or torrent_type == 'series':
        # Rename season folder
        season_name = build_filename(qm_data, 'season')
        if season_name:
            new_season_folder = current_folder.parent / season_name
            if current_folder.name != season_name:
                renamed_season = safe_rename(current_folder, new_season_folder)
                if renamed_season:
                    current_folder = Path(renamed_season)
                    new_location = current_folder / Path(renamed_video).name
        
        # Rename series folder (parent of season)
        series_folder = current_folder.parent
        series_name = build_filename(qm_data, 'series')
        if series_name:
            new_series_folder = series_folder.parent / series_name
            if series_folder.name != series_name:
                renamed_series = safe_rename(series_folder, new_series_folder)
                if renamed_series:
                    new_location = Path(renamed_series) / current_folder.name / Path(renamed_video).name
    
    elif torrent_type == 'season':
        # Rename season folder
        season_name = build_filename(qm_data, 'season')
        if season_name and current_folder.name != season_name:
            new_season_folder = current_folder.parent / season_name
            renamed_season = safe_rename(current_folder, new_season_folder)
            if renamed_season:
                new_location = Path(renamed_season) / Path(renamed_video).name
    
    else:  # per episode
        # Rename episode folder
        episode_name = build_filename(qm_data, 'episode')
        if episode_name and current_folder.name != episode_name:
            new_episode_folder = current_folder.parent / episode_name
            renamed_episode = safe_rename(current_folder, new_episode_folder)
            if renamed_episode:
                new_location = Path(renamed_episode) / Path(renamed_video).name
    
    logging.info(f"Completed processing: {checksum} -> {new_location}")
    return str(new_location)

def update_database(cursor, checksum, new_location):
    """Update it_file_loc in database"""
    try:
        cursor.execute(
            "UPDATE import_tuner SET it_file_loc = ? WHERE it_checksum = ?",
            (new_location, checksum)
        )
        logging.debug(f"Updated database for {checksum}: {new_location}")
    except Exception as e:
        logging.error(f"Database update failed for {checksum}: {e}")

def main():
    setup_logging()
    logging.info("Starting fast_sev rename process")
    
    try:
        # Connect to database
        conn = sqlite3.connect('danger2manifold.db')
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        # Get matching checksums between tables
        query = """
        SELECT it.it_checksum, it.it_def_loc, it.it_torrent, it.it_subtitles,
               qm.qm_series, qm.qm_sea_no, qm.qm_ep_no, qm.qm_res, qm.qm_hdr,
               qm.qm_vid_bac, qm.qm_aud_cdc, qm.qm_aud_chn, qm.qm_src_short, qm.qm_rga
        FROM import_tuner it
        JOIN qtr_mile qm ON it.it_checksum = qm.it_checksum
        WHERE it.it_def_loc IS NOT NULL
        """
        
        cursor.execute(query)
        rows = cursor.fetchall()
        
        if not rows:
            logging.warning("No matching records found")
            return
        
        logging.info(f"Found {len(rows)} files to process")
        
        # Process files with threading for performance
        max_workers = min(16, len(rows))
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all tasks
            future_to_checksum = {}
            for row in rows:
                import_data = {
                    'it_def_loc': row['it_def_loc'],
                    'it_torrent': row['it_torrent'],
                    'it_subtitles': row['it_subtitles']
                }
                qm_data = {
                    'qm_series': row['qm_series'],
                    'qm_sea_no': row['qm_sea_no'],
                    'qm_ep_no': row['qm_ep_no'],
                    'qm_res': row['qm_res'],
                    'qm_hdr': row['qm_hdr'],
                    'qm_vid_bac': row['qm_vid_bac'],
                    'qm_aud_cdc': row['qm_aud_cdc'],
                    'qm_aud_chn': row['qm_aud_chn'],
                    'qm_src_short': row['qm_src_short'],
                    'qm_rga': row['qm_rga']
                }
                
                future = executor.submit(process_files, row['it_checksum'], import_data, qm_data)
                future_to_checksum[future] = row['it_checksum']
            
            # Process completed tasks
            for future in as_completed(future_to_checksum):
                checksum = future_to_checksum[future]
                try:
                    new_location = future.result()
                    if new_location:
                        update_database(cursor, checksum, new_location)
                except Exception as e:
                    logging.error(f"Task failed for {checksum}: {e}")
        
        # Commit all database changes
        conn.commit()
        logging.info("All database updates committed")
        
    except Exception as e:
        logging.error(f"Main process error: {e}")
        raise
    finally:
        if 'conn' in locals():
            conn.close()
        logging.info("fast_sev process completed")

if __name__ == "__main__":
    main()