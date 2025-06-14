#!/usr/bin/env python3

import sqlite3
import json
import logging
import os
import sys
import subprocess
import fcntl
import errno
from pathlib import Path
from collections import defaultdict

def setup_logging():
    try:
        with open('2jznoshit.json', 'r') as f:
            log_enabled = json.load(f).get('f8_fr', {}).get('logs', False)
    except:
        log_enabled = False
    
    if log_enabled:
        logging.basicConfig(
            filename='f8_fr.log',
            level=logging.DEBUG,
            format='%(asctime)s - %(levelname)s - %(message)s',
            filemode='a'
        )
    else:
        logging.disable(logging.CRITICAL)

def load_config():
    try:
        with open('2jznoshit.json', 'r') as f:
            return json.load(f)
    except Exception as e:
        logging.error(f"Config load failed: {e}")
        sys.exit(1)

def get_db_data():
    if not os.path.exists("danger2manifold.db"):
        logging.error("Database not found")
        sys.exit(1)
    
    try:
        with sqlite3.connect("danger2manifold.db") as conn:
            conn.row_factory = sqlite3.Row
            return [dict(row) for row in conn.execute("""
                SELECT it_file_loc, it_torrent, it_series, it_sea_no, it_ep_no, 
                       it_ep_title, it_checksum, file_name
                FROM import_tuner 
                WHERE it_file_loc IS NOT NULL AND it_file_loc != ''
                ORDER BY it_series, it_sea_no, it_ep_no
            """)]
    except sqlite3.Error as e:
        logging.error(f"Database error: {e}")
        sys.exit(1)

def check_file_unlocked(path):
    """Check if file is locked by trying to open with exclusive lock"""
    try:
        with open(path, 'rb') as f:
            fcntl.flock(f.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            fcntl.flock(f.fileno(), fcntl.LOCK_UN)
        return True
    except (OSError, IOError) as e:
        if e.errno in (errno.EAGAIN, errno.EACCES):
            return False
        return True

def wait_for_file_unlock(path, max_wait=30):
    """Wait for file to become unlocked"""
    import time
    for _ in range(max_wait):
        if check_file_unlocked(path):
            return True
        time.sleep(1)
    return False

def validate_path_access(source_path):
    """Validate all files in path are accessible"""
    if source_path.is_file():
        return wait_for_file_unlock(source_path)
    
    for file_path in source_path.rglob('*'):
        if file_path.is_file() and not wait_for_file_unlock(file_path, 5):
            logging.warning(f"File locked: {file_path}")
            return False
    return True

def create_nfo(path, series, season=None, episode=None, episodes=None):
    """Create NFO file"""
    content = [f"Series: {series}"]
    
    if episode:
        content.extend([
            f"Season: {episode.get('it_sea_no', 'Unknown')}",
            f"Episode: {episode.get('it_ep_no', 'Unknown')} - {episode.get('it_ep_title', 'Unknown')}",
            f"File: {episode.get('file_name', 'Unknown')}",
            f"Checksum: {episode.get('it_checksum', 'Unknown')}"
        ])
    elif season:
        content.extend([
            f"Season: {season}",
            f"Episodes: {len(episodes) if episodes else 'Unknown'}"
        ])
    else:
        content.append("Complete Series Pack")
    
    try:
        with open(path, 'w', encoding='utf-8') as f:
            f.write('\n'.join(content) + '\n')
        return True
    except OSError:
        return False

def create_torrent(source_path, torrent_path, tracker_url):
    """Create torrent file with file lock checking"""
    source_path = Path(source_path)
    torrent_path = Path(torrent_path)
    
    if not source_path.exists():
        logging.error(f"Source not found: {source_path}")
        return False
    
    if not validate_path_access(source_path):
        logging.error(f"Files locked or inaccessible: {source_path}")
        return False
    
    # Ensure torrent directory exists
    torrent_path.parent.mkdir(parents=True, exist_ok=True)
    
    cmd = [
        'py3createtorrent',
        '--threads', '1',  # Single thread to avoid resource conflicts
        '-o', str(torrent_path),
        str(source_path)
    ]
    
    if tracker_url:
        cmd.extend(['-t', tracker_url])
    
    try:
        # Run with reduced priority and timeout
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=300,
            preexec_fn=lambda: os.nice(10) if hasattr(os, 'nice') else None
        )
        
        if result.returncode == 0:
            logging.info(f"Created: {torrent_path}")
            return True
        else:
            logging.error(f"Torrent failed: {result.stderr}")
            return False
            
    except subprocess.TimeoutExpired:
        logging.error(f"Timeout: {source_path}")
        return False
    except Exception as e:
        logging.error(f"Error: {e}")
        return False

def process_torrents(data, config):
    """Process all torrent creation tasks"""
    tracker_url = config['user_input']['default']['tracker'].split(';')[1].strip()
    tor_loc = Path(config['user_input']['default']['tor_loc'])
    
    # Group data by series and torrent type
    groups = defaultdict(lambda: defaultdict(list))
    for item in data:
        if Path(item['it_file_loc']).exists():
            groups[item['it_torrent']][item['it_series']].append(item)
    
    results = []
    total_tasks = 0
    
    for torrent_type, series_data in groups.items():
        for series, episodes in series_data.items():
            
            if torrent_type == 'episode':
                # Individual episode torrents
                for episode in episodes:
                    ep_path = Path(episode['it_file_loc'])
                    ep_dir = ep_path.parent
                    torrent_path = tor_loc / f"{ep_dir.name}.torrent"
                    
                    logging.info(f"Processing episode: {ep_dir.name}")
                    result = create_torrent(ep_dir, torrent_path, tracker_url)
                    results.append(result)
                    total_tasks += 1
                    
            elif torrent_type == 'season':
                # Group episodes by season
                seasons = defaultdict(list)
                for ep in episodes:
                    seasons[ep['it_sea_no']].append(ep)
                
                for season_num, season_eps in seasons.items():
                    if len(season_eps) > 1:
                        # Multi-episode season torrent
                        season_path = Path(season_eps[0]['it_file_loc']).parent
                        torrent_path = tor_loc / f"{season_path.name}.torrent"
                        
                        logging.info(f"Processing season: {season_path.name}")
                        result = create_torrent(season_path, torrent_path, tracker_url)
                        results.append(result)
                        total_tasks += 1
                    else:
                        # Single episode in season
                        episode = season_eps[0]
                        ep_path = Path(episode['it_file_loc'])
                        ep_dir = ep_path.parent
                        torrent_path = tor_loc / f"{ep_dir.name}.torrent"
                        
                        logging.info(f"Processing single episode: {ep_dir.name}")
                        result = create_torrent(ep_dir, torrent_path, tracker_url)
                        results.append(result)
                        total_tasks += 1
                
                # Series torrent if multiple seasons
                if len(seasons) > 1:
                    series_path = Path(episodes[0]['it_file_loc']).parent.parent
                    torrent_path = tor_loc / f"{series_path.name}.torrent"
                    
                    logging.info(f"Processing series: {series_path.name}")
                    result = create_torrent(series_path, torrent_path, tracker_url)
                    results.append(result)
                    total_tasks += 1
                    
            else:
                # Series torrent
                series_path = Path(episodes[0]['it_file_loc']).parent.parent
                torrent_path = tor_loc / f"{series_path.name}.torrent"
                
                logging.info(f"Processing series: {series_path.name}")
                result = create_torrent(series_path, torrent_path, tracker_url)
                results.append(result)
                total_tasks += 1
    
    return results, total_tasks

def main():
    setup_logging()
    logging.info("Starting f8_fr")
    
    # Validate py3createtorrent
    try:
        subprocess.run(['py3createtorrent', '--version'], 
                      capture_output=True, timeout=10, check=True)
    except:
        logging.error("py3createtorrent not available")
        sys.exit(1)
    
    config = load_config()
    data = get_db_data()
    
    if not data:
        logging.error("No data found")
        sys.exit(1)
    
    logging.info(f"Processing {len(data)} entries")
    
    results, total_tasks = process_torrents(data, config)
    successful = sum(results)
    
    logging.info(f"Complete: {successful}/{total_tasks} successful")
    print(f"Complete: {successful}/{total_tasks} torrents created")

if __name__ == "__main__":
    main()