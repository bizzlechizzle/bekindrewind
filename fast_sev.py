#!/usr/bin/env python3

import sqlite3
import json
import logging
import os
import re
from pathlib import Path
from collections import defaultdict

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
            format='%(asctime)s - %(levelname)s - %(message)s',
            filemode='a'
        )
    else:
        logging.disable(logging.CRITICAL)

def normalize_series_name(series_name):
    if not series_name or not series_name.strip():
        return ""
    
    normalized = series_name.strip()
    normalized = re.sub(r"['\"`?!]", "", normalized)
    normalized = re.sub(r"[^\w\s\-]", ".", normalized)
    normalized = re.sub(r"\s+", ".", normalized)
    normalized = re.sub(r"\.+", ".", normalized)
    normalized = normalized.strip(".")
    
    return normalized

def build_filename(qm_data, name_type):
    series = normalize_series_name(qm_data['qm_series'])
    if not series:
        return None
        
    sea_no = str(qm_data['qm_sea_no'])
    ep_no = str(qm_data['qm_ep_no'])
    res = qm_data['qm_res']
    hdr = qm_data['qm_hdr']
    vid_bac = qm_data['qm_vid_bac'] 
    aud_cdc = qm_data['qm_aud_cdc']
    aud_chn = qm_data['qm_aud_chn']
    src_short = qm_data['qm_src_short']
    rga = qm_data['qm_rga']
    
    if not all([res, vid_bac, aud_cdc, src_short, rga]):
        return None
    
    # HDR part - only if HDR present
    hdr_part = ".HDR" if hdr and 'HDR' in hdr.upper() else ""
    
    # Audio channel part - only for 5.1 or 7.1
    aud_chn_part = f".{aud_chn}" if aud_chn and ('5.1' in aud_chn or '7.1' in aud_chn) else ""
    
    if name_type == 'series':
        # Extract all season numbers for range
        season_nums = [int(x) for x in re.findall(r'(\d+)', sea_no)]
        if len(season_nums) > 1:
            sea_part = f"S{min(season_nums):02d}-S{max(season_nums):02d}"
        else:
            sea_part = f"S{season_nums[0]:02d}" if season_nums else "S01"
        
        return f"{series}.{sea_part}.{res}{hdr_part}.{src_short}.{vid_bac}.{aud_cdc}{aud_chn_part}-{rga}"
    
    elif name_type == 'season':
        season_nums = [int(x) for x in re.findall(r'(\d+)', sea_no)]
        sea_part = f"S{season_nums[0]:02d}" if season_nums else "S01"
        
        return f"{series}.{sea_part}.{res}{hdr_part}.{src_short}.{vid_bac}.{aud_cdc}{aud_chn_part}-{rga}"
    
    elif name_type == 'episode':
        # Build S##E## format
        season_nums = [int(x) for x in re.findall(r'(\d+)', sea_no)]
        episode_nums = [int(x) for x in re.findall(r'(\d+)', ep_no)]
        
        if not season_nums or not episode_nums:
            return None
            
        se_part = f"S{season_nums[0]:02d}E{episode_nums[0]:02d}"
        
        return f"{series}.{se_part}.{res}{hdr_part}.{src_short}.{vid_bac}.{aud_cdc}{aud_chn_part}-{rga}"
    
    return None

def safe_rename(old_path, new_path):
    old_path = Path(old_path)
    new_path = Path(new_path)
    
    if not old_path.exists():
        logging.error(f"Source does not exist: {old_path}")
        return None
    
    if old_path.resolve() == new_path.resolve():
        return old_path
    
    # Handle conflicts
    if new_path.exists():
        counter = 1
        stem = new_path.stem
        suffix = new_path.suffix
        parent = new_path.parent
        
        while new_path.exists():
            new_name = f"{stem}_{counter}{suffix}" if suffix else f"{stem}_{counter}"
            new_path = parent / new_name
            counter += 1
    
    # Safety check - only rename within tor directory
    tor_base = Path("/Volumes/barbossa/upload/tor")
    try:
        if not old_path.is_relative_to(tor_base) or not new_path.is_relative_to(tor_base):
            logging.error(f"SAFETY: Refusing rename outside tor directory")
            return None
            
        old_path.rename(new_path)
        logging.info(f"Renamed: {old_path.name} -> {new_path.name}")
        return new_path
    except Exception as e:
        logging.error(f"Rename failed: {e}")
        return None

def get_torrent_structure(file_path):
    """Determine torrent structure type"""
    tor_base = Path("/Volumes/barbossa/upload/tor")
    file_path = Path(file_path)
    
    try:
        relative_path = file_path.relative_to(tor_base)
        depth = len(relative_path.parts)
        
        if depth >= 3:  # series/season/file
            return 'series'
        elif depth == 2:  # season/file or episode/file  
            parent_name = relative_path.parts[0]
            if re.search(r'season|s\d+', parent_name, re.I):
                return 'season'
            else:
                return 'episode'
        else:
            return 'unknown'
    except ValueError:
        return 'unknown'

def find_and_rename_nfos(directory, qm_data, structure_type):
    """Find and rename all NFO files in directory based on structure"""
    series_name = build_filename(qm_data, 'series')
    season_name = build_filename(qm_data, 'season')
    episode_name = build_filename(qm_data, 'episode')
    
    # Find all NFO files in directory
    nfo_files = list(directory.glob("*.nfo"))
    
    for nfo_file in nfo_files:
        current_name = nfo_file.name.lower()
        
        # Check if it's a generic NFO name first
        if current_name == 'series.nfo' and series_name:
            new_nfo_path = directory / f"{series_name}.nfo"
            safe_rename(nfo_file, new_nfo_path)
        elif current_name == 'season.nfo' and season_name:
            new_nfo_path = directory / f"{season_name}.nfo"
            safe_rename(nfo_file, new_nfo_path)
        else:
            # For structure types, determine appropriate NFO naming
            if structure_type == 'season':
                # Season torrents should have season NFOs
                if season_name:
                    new_nfo_path = directory / f"{season_name}.nfo"
                    safe_rename(nfo_file, new_nfo_path)
            elif structure_type == 'episode':
                # Episode torrents should have episode NFOs
                if episode_name:
                    new_nfo_path = directory / f"{episode_name}.nfo"
                    safe_rename(nfo_file, new_nfo_path)
            elif structure_type == 'series':
                # Series torrents can have both - check content or use episode as fallback
                if episode_name:
                    new_nfo_path = directory / f"{episode_name}.nfo"
                    safe_rename(nfo_file, new_nfo_path)

def process_single_file(file_data, structure_type, cursor):
    """Process single video file and related files"""
    checksum = file_data['it_checksum']
    current_file = Path(file_data['it_def_loc'])
    
    if not current_file.exists():
        logging.error(f"File not found: {current_file}")
        return False, None
    
    # Build new filename
    qm_data = {
        'qm_series': file_data['qm_series'],
        'qm_sea_no': file_data['qm_sea_no'],
        'qm_ep_no': file_data['qm_ep_no'],
        'qm_res': file_data['qm_res'],
        'qm_hdr': file_data['qm_hdr'],
        'qm_vid_bac': file_data['qm_vid_bac'],
        'qm_aud_cdc': file_data['qm_aud_cdc'],
        'qm_aud_chn': file_data['qm_aud_chn'],
        'qm_src_short': file_data['qm_src_short'],
        'qm_rga': file_data['qm_rga']
    }
    
    new_filename = build_filename(qm_data, 'episode')
    if not new_filename:
        logging.error(f"Could not build filename for {checksum}")
        return False, None
    
    # Rename video file
    file_ext = current_file.suffix
    new_video_path = current_file.parent / f"{new_filename}{file_ext}"
    renamed_video = safe_rename(current_file, new_video_path)
    
    if not renamed_video:
        return False, None
    
    # Rename subtitle files if available
    if file_data['it_subtitles']:
        video_stem = current_file.stem
        subtitle_exts = ['.srt', '.sub', '.idx', '.vtt', '.ass', '.ssa']
        for ext in subtitle_exts:
            sub_file = current_file.parent / f"{video_stem}{ext}"
            if sub_file.exists():
                new_sub_path = current_file.parent / f"{new_filename}{ext}"
                safe_rename(sub_file, new_sub_path)
    
    return True, (qm_data, renamed_video.parent, renamed_video, checksum)

def rename_directories(qm_data, directory, structure_type):
    """Rename directories based on structure type"""
    if structure_type == 'series':
        # Rename season folder first
        season_name = build_filename(qm_data, 'season')
        if season_name:
            new_season_path = directory.parent / season_name
            renamed_season = safe_rename(directory, new_season_path)
            
            if renamed_season:
                # Rename series folder
                series_name = build_filename(qm_data, 'series')
                if series_name:
                    new_series_path = renamed_season.parent.parent / series_name
                    return safe_rename(renamed_season.parent, new_series_path)
                return renamed_season.parent
            return directory
    
    elif structure_type == 'season':
        season_name = build_filename(qm_data, 'season')
        if season_name:
            new_season_path = directory.parent / season_name
            return safe_rename(directory, new_season_path)
        return directory
    
    elif structure_type == 'episode':
        episode_name = build_filename(qm_data, 'episode')
        if episode_name:
            new_episode_path = directory.parent / episode_name
            return safe_rename(directory, new_episode_path)
        return directory
    
    return directory

def process_files(torrent_files, cursor):
    """Process and rename files"""
    if not torrent_files:
        return 0, 0
    
    successful = 0
    failed = 0
    
    # Group by directory for efficient processing
    dir_groups = defaultdict(list)
    for file_data in torrent_files:
        file_path = Path(file_data['it_def_loc'])
        dir_groups[file_path.parent].append(file_data)
    
    processed_dirs = {}
    db_updates = []
    
    for directory, files in dir_groups.items():
        structure_type = get_torrent_structure(files[0]['it_def_loc'])
        
        if structure_type == 'unknown':
            failed += len(files)
            continue
        
        # Process each file in directory
        qm_data = None
        for file_data in files:
            success, result = process_single_file(file_data, structure_type, cursor)
            if success:
                successful += 1
                qm_data, current_dir, renamed_video, checksum = result
                db_updates.append((renamed_video, checksum))
            else:
                failed += 1
        
        # Rename NFOs after all video files are processed
        if qm_data:
            find_and_rename_nfos(directory, qm_data, structure_type)
            processed_dirs[directory] = (qm_data, structure_type)
    
    # Rename directories last (deepest first)
    sorted_dirs = sorted(processed_dirs.items(), key=lambda x: len(x[0].parts), reverse=True)
    final_updates = []
    
    for directory, (qm_data, structure_type) in sorted_dirs:
        if directory.exists():
            old_dir = directory
            new_dir = rename_directories(qm_data, directory, structure_type)
            
            # Update paths for files that were in renamed directories
            if new_dir and new_dir != old_dir:
                for i, (video_path, checksum) in enumerate(db_updates):
                    if old_dir in video_path.parents or video_path.parent == old_dir:
                        # Calculate new path after directory rename
                        relative_path = video_path.relative_to(old_dir)
                        new_video_path = new_dir / relative_path
                        db_updates[i] = (new_video_path, checksum)
    
    # Update database with final paths
    for video_path, checksum in db_updates:
        try:
            cursor.execute(
                "UPDATE import_tuner SET it_file_loc = ? WHERE it_checksum = ?",
                (str(video_path), checksum)
            )
        except Exception as e:
            logging.error(f"Database update failed for {checksum}: {e}")
    
    return successful, failed

def main():
    setup_logging()
    logging.info("Starting fast_sev")
    
    try:
        conn = sqlite3.connect('danger2manifold.db')
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        query = """
        SELECT it.it_checksum, it.it_def_loc, it.it_torrent, it.it_subtitles,
               qm.qm_series, qm.qm_sea_no, qm.qm_ep_no, qm.qm_res, qm.qm_hdr,
               qm.qm_vid_bac, qm.qm_aud_cdc, qm.qm_aud_chn, qm.qm_src_short, qm.qm_rga
        FROM import_tuner it
        JOIN qtr_mile qm ON it.it_checksum = qm.it_checksum
        WHERE it.it_def_loc IS NOT NULL AND it.it_def_loc != ''
        ORDER BY it.it_torrent, qm.qm_series, qm.qm_sea_no, qm.qm_ep_no
        """
        
        cursor.execute(query)
        rows = cursor.fetchall()
        
        if not rows:
            print("No files to process")
            return
        
        print(f"Processing {len(rows)} files")
        
        # Convert to list of dicts
        files = [dict(row) for row in rows]
        
        # Group by torrent type
        torrent_groups = defaultdict(list)
        for file_data in files:
            key = (file_data['it_torrent'], file_data['qm_series'])
            torrent_groups[key].append(file_data)
        
        total_successful = 0
        total_failed = 0
        
        for (torrent_type, series), torrent_files in torrent_groups.items():
            print(f"Processing {series} ({torrent_type}): {len(torrent_files)} files")
            successful, failed = process_files(torrent_files, cursor)
            total_successful += successful
            total_failed += failed
        
        conn.commit()
        print(f"Completed: {total_successful} successful, {total_failed} failed")
        
    except Exception as e:
        logging.error(f"Error: {e}")
        print(f"Error: {e}")
        raise
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    main()