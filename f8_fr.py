#!/usr/bin/env python3

import sqlite3
import json
import logging
import os
import sys
import subprocess
import shutil
from pathlib import Path
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
import multiprocessing

def setup_logging():
    try:
        with open('2jznoshit.json', 'r') as f:
            config = json.load(f)
            log_enabled = config.get('f8_fr', {}).get('logs', False)
    except (FileNotFoundError, json.JSONDecodeError, KeyError):
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
    except (FileNotFoundError, json.JSONDecodeError) as e:
        logging.error(f"Config load failed: {e}")
        sys.exit(1)

def get_db_data():
    db_path = "danger2manifold.db"
    if not os.path.exists(db_path):
        logging.error(f"Database not found: {db_path}")
        sys.exit(1)
    
    try:
        with sqlite3.connect(db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute("""
                SELECT it_file_loc, it_torrent, it_series, it_sea_no, it_ep_no, 
                       it_ep_title, it_checksum, file_name
                FROM import_tuner 
                WHERE it_file_loc IS NOT NULL AND it_file_loc != ''
                ORDER BY it_series, it_sea_no, it_ep_no
            """)
            return [dict(row) for row in cursor.fetchall()]
    except sqlite3.Error as e:
        logging.error(f"Database error: {e}")
        sys.exit(1)

def validate_py3createtorrent():
    try:
        result = subprocess.run(['py3createtorrent', '--version'], 
                              capture_output=True, text=True, timeout=10)
        return result.returncode == 0
    except (subprocess.TimeoutExpired, subprocess.SubprocessError, FileNotFoundError):
        logging.error("py3createtorrent not available")
        sys.exit(1)

def safe_copy_file(src, dst):
    """Copy file with fallback from hardlink to copy"""
    dst.parent.mkdir(parents=True, exist_ok=True)
    
    if dst.exists():
        return True
        
    try:
        os.link(src, dst)
        return True
    except OSError:
        try:
            shutil.copy2(src, dst)
            return True
        except OSError as e:
            logging.error(f"Copy failed {src} -> {dst}: {e}")
            return False

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

def create_torrent(source_path, torrent_path, tracker_url=None):
    """Create torrent file"""
    cmd = ['py3createtorrent', '-o', str(torrent_path), str(source_path)]
    if tracker_url:
        cmd.extend(['-t', tracker_url])
    
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
        success = result.returncode == 0
        if not success:
            logging.error(f"Torrent creation failed: {result.stderr}")
        return success
    except subprocess.TimeoutExpired:
        logging.error(f"Torrent timeout: {source_path}")
        return False
    except subprocess.SubprocessError as e:
        logging.error(f"Subprocess error: {e}")
        return False

def process_series(series, episodes, tor_loc):
    """Create series torrent"""
    logging.info(f"Processing series: {series}")
    
    # Use the existing series folder from the first episode
    first_ep_path = Path(episodes[0]['it_file_loc'])
    
    # Find the series folder (should be 2 levels up from episode file)
    series_dir = first_ep_path.parent.parent
    
    if series_dir.exists():
        torrent_path = tor_loc / f"{series_dir.name}.torrent"
        return create_torrent(series_dir, torrent_path)
    return False

def process_season(series, season, episodes, tor_loc):
    """Create season torrent"""
    logging.info(f"Processing season: {series} {season}")
    
    # Use the existing season folder from the first episode
    first_ep_path = Path(episodes[0]['it_file_loc'])
    season_dir = first_ep_path.parent
    
    if season_dir.exists():
        torrent_path = tor_loc / f"{season_dir.name}.torrent"
        return create_torrent(season_dir, torrent_path)
    return False

def process_episode(episode, tor_loc):
    """Create episode torrent"""
    logging.info(f"Processing episode: {episode['it_series']}")
    
    # Use the existing episode folder or file parent
    ep_path = Path(episode['it_file_loc'])
    
    # If it's in an episode folder, use that folder, otherwise use the file's parent
    if ep_path.parent.name != ep_path.parent.parent.name:
        ep_dir = ep_path.parent
    else:
        ep_dir = ep_path.parent
    
    if ep_dir.exists():
        torrent_path = tor_loc / f"{ep_dir.name}.torrent"
        return create_torrent(ep_dir, torrent_path)
    return False

def get_torrent_tasks(data, tor_loc):
    """Generate torrent creation tasks"""
    structure = defaultdict(lambda: defaultdict(list))
    
    # Group data
    for item in data:
        if not Path(item['it_file_loc']).exists():
            logging.warning(f"File not found: {item['it_file_loc']}")
            continue
        
        series = item['it_series'].strip()
        structure[item['it_torrent']][series].append(item)
    
    tasks = []
    
    for torrent_type, series_data in structure.items():
        for series, episodes in series_data.items():
            if torrent_type == 'season':
                # Group by season
                seasons = defaultdict(list)
                for ep in episodes:
                    seasons[ep['it_sea_no']].append(ep)
                
                for season, season_eps in seasons.items():
                    if len(season_eps) > 1:
                        tasks.append(('season', series, season, season_eps))
                    else:
                        tasks.append(('episode', season_eps[0], None, None))
                
                # Series torrent if multiple seasons
                if len(seasons) > 1:
                    tasks.append(('series', series, None, episodes))
                    
            elif torrent_type == 'episode':
                for episode in episodes:
                    tasks.append(('episode', episode, None, None))
            else:
                tasks.append(('series', series, None, episodes))
    
    return tasks

def execute_tasks(tasks, tor_loc):
    """Execute torrent creation tasks in parallel"""
    max_workers = min(multiprocessing.cpu_count(), len(tasks), 8)
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = []
        
        for task in tasks:
            task_type, arg1, arg2, arg3 = task
            
            if task_type == 'series':
                future = executor.submit(process_series, arg1, arg3, tor_loc)
            elif task_type == 'season':
                future = executor.submit(process_season, arg1, arg2, arg3, tor_loc)
            elif task_type == 'episode':
                future = executor.submit(process_episode, arg1, tor_loc)
            
            futures.append((future, task))
        
        results = []
        for future, task in futures:
            try:
                result = future.result()
                results.append(result)
                status = "SUCCESS" if result else "FAILED"
                logging.info(f"{status}: {task[0]} - {task[1] if isinstance(task[1], str) else 'episode'}")
            except Exception as e:
                logging.error(f"Task error {task}: {e}")
                results.append(False)
    
    return results

def main():
    setup_logging()
    logging.info("Starting f8_fr")
    
    validate_py3createtorrent()
    config = load_config()
    data = get_db_data()
    
    if not data:
        logging.error("No data found")
        sys.exit(1)
    
    logging.info(f"Processing {len(data)} entries")
    
    tor_loc = Path(config['user_input']['default']['tor_loc'])
    tor_loc.mkdir(parents=True, exist_ok=True)
    
    tasks = get_torrent_tasks(data, tor_loc)
    results = execute_tasks(tasks, tor_loc)
    
    successful = sum(results)
    total = len(results)
    
    logging.info(f"Complete: {successful}/{total} successful")
    print(f"Complete: {successful}/{total} torrents created")

if __name__ == "__main__":
    main()