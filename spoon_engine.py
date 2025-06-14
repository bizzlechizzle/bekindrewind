#!/usr/bin/env python3

import json
import sqlite3
import subprocess
import random
import logging
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor
import multiprocessing

def load_config():
    with open('2jznoshit.json', 'r') as f:
        return json.load(f)

def get_files_from_db(db_path):
    with sqlite3.connect(db_path) as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT it_checksum, it_file_loc, file_name
            FROM import_tuner
            WHERE it_file_loc IS NOT NULL AND it_file_loc != ''
        """)
        return cur.fetchall()

def get_duration(file_path, checksum, db_path):
    """Get video duration with fallback to ffprobe."""
    with sqlite3.connect(db_path) as conn:
        cur = conn.cursor()
        cur.execute("SELECT ff_ep_dur FROM ford_probe WHERE it_checksum = ?", (checksum,))
        row = cur.fetchone()
    
    if row and row[0]:
        dur = row[0]
        if isinstance(dur, (int, float)):
            return max(float(dur), 60.0)  # Minimum 1 minute
        if isinstance(dur, str) and ':' in dur:
            parts = dur.split(':')
            if len(parts) == 3:
                try:
                    h, m, s = map(float, parts)
                    return max(h * 3600 + m * 60 + s, 60.0)
                except ValueError:
                    pass
    
    # Fallback to ffprobe
    try:
        result = subprocess.run(
            ['ffprobe', '-v', 'error', '-show_entries', 'format=duration',
             '-of', 'default=noprint_wrappers=1:nokey=1', str(file_path)],
            capture_output=True, text=True, check=True, timeout=30
        )
        return max(float(result.stdout.strip()), 60.0)
    except (subprocess.CalledProcessError, ValueError, subprocess.TimeoutExpired):
        return 2700.0  # 45min default

def build_screenshot_dir(src_path, scn_loc):
    """Build screenshot directory maintaining folder structure."""
    parts = src_path.parts
    out_dir = Path(scn_loc)
    
    # Find last directory before filename (always use parent)
    if len(parts) > 1:
        out_dir = out_dir / parts[-2]  # Use parent directory name
    
    out_dir.mkdir(parents=True, exist_ok=True)
    return out_dir

def calculate_screenshot_count(duration, total_files):
    """Calculate screenshots based on duration and file count."""
    if total_files <= 8:
        return 7  # Short seasons get more screenshots
    return max(1, min(int(duration // 600), 5))  # 1 per 10min, max 5

def generate_screenshots(src_path, file_name, duration, num_screenshots, out_dir, logger):
    """Generate screenshots at random intervals."""
    if duration < 120:  # Less than 2 minutes
        timestamps = [duration * 0.5]  # Single middle screenshot
    else:
        # Generate unique random timestamps in 10-90% range
        start_time = duration * 0.1
        end_time = duration * 0.9
        timestamps = sorted(random.uniform(start_time, end_time) 
                          for _ in range(num_screenshots))
    
    base_name = Path(file_name).stem  # Use database file_name
    
    for i, ts in enumerate(timestamps, 1):
        dest = out_dir / f"{base_name}_{i}.jpg"
        
        try:
            subprocess.run([
                'ffmpeg', '-ss', f'{ts:.2f}', '-i', str(src_path),
                '-vframes', '1', '-q:v', '2', '-y', str(dest)
            ], capture_output=True, check=True, timeout=60)
            
            logger.debug(f"Screenshot {i}/{len(timestamps)} for {file_name}")
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as e:
            logger.error(f"Screenshot failed for {file_name} at {ts:.2f}s")
            raise

def process_video(args):
    checksum, file_loc, file_name, scn_loc, total_files, log_enabled, db_path = args
    
    # Setup logging with unique logger per process
    logger = logging.getLogger(f'spoon_engine_{checksum[:8]}')
    logger.setLevel(logging.DEBUG if log_enabled else logging.WARNING)
    
    if log_enabled and not any(isinstance(h, logging.FileHandler) for h in logger.handlers):
        h = logging.FileHandler('spoon_engine.log')
        h.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        logger.addHandler(h)

    try:
        src = Path(file_loc)
        if not src.exists():
            return checksum, False, f"File not found: {file_loc}"

        # Fail-fast playback test
        subprocess.run([
            'ffmpeg', '-v', 'error', '-i', str(src), 
            '-frames:v', '1', '-f', 'null', '-'
        ], capture_output=True, check=True, timeout=30)

        # Get duration and calculate screenshots
        duration = get_duration(str(src), checksum, db_path)
        num_screenshots = calculate_screenshot_count(duration, total_files)

        # Build output directory
        out_dir = build_screenshot_dir(src, scn_loc)

        # Set random seed for reproducible results
        random.seed(hash(checksum) & 0x7FFFFFFF)
        
        # Generate screenshots
        generate_screenshots(src, file_name, duration, num_screenshots, out_dir, logger)

        # Strip metadata
        temp = src.with_suffix('.temp.mkv')
        subprocess.run([
            'ffmpeg', '-i', str(src), '-map_metadata', '-1',
            '-c', 'copy', '-bitexact', '-y', str(temp)
        ], capture_output=True, check=True)
        temp.replace(src)

        logger.info(f"Processed: {file_name}")
        return checksum, True, "Success"

    except subprocess.CalledProcessError as e:
        msg = (e.stderr or b'').decode().strip()
        logger.error(f"Failed {file_name}: {msg}")
        return checksum, False, f"FFmpeg error: {msg}"
    except Exception as e:
        logger.error(f"Failed {file_name}: {e}")
        return checksum, False, f"Error: {e}"

def main():
    cfg = load_config()
    log_enabled = cfg.get('spoon_engine', {}).get('logs', False)
    scn_loc = cfg['user_input']['default']['scn_loc']

    logger = logging.getLogger('spoon_engine')
    logger.setLevel(logging.DEBUG if log_enabled else logging.WARNING)
    if log_enabled:
        h = logging.FileHandler('spoon_engine.log')
        h.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        logger.addHandler(h)

    logger.info("Starting spoon_engine")

    db_path = 'danger2manifold.db'
    if not Path(db_path).exists():
        print(f"Error: Database not found: {db_path}")
        return

    files = get_files_from_db(db_path)
    if not files:
        print("No files found in database")
        return

    total = len(files)
    logger.info(f"Processing {total} files with dynamic screenshot counts")

    # Updated args to pass total_files instead of fixed num
    args = [(c, loc, name, scn_loc, total, log_enabled, db_path) for c, loc, name in files]
    max_workers = min(max(multiprocessing.cpu_count() // 2, 1), 4)

    success = fail = 0
    with ProcessPoolExecutor(max_workers=max_workers) as pool:
        for checksum, ok, msg in pool.map(process_video, args):
            if ok:
                success += 1
            else:
                fail += 1
                print(f"Failed: {checksum} - {msg}")

    logger.info(f"Processing complete. Success: {success}, Failed: {fail}")
    print(f"\nProcessing complete. Success: {success}, Failed: {fail}")

if __name__ == "__main__":
    main()