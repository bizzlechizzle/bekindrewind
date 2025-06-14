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
    with sqlite3.connect(db_path) as conn:
        cur = conn.cursor()
        cur.execute("SELECT ff_ep_dur FROM ford_probe WHERE it_checksum = ?", (checksum,))
        row = cur.fetchone()
    if row and row[0]:
        dur = row[0]
        if isinstance(dur, (int, float)):
            return float(dur)
        if isinstance(dur, str) and ':' in dur:
            parts = dur.split(':')
            if len(parts) == 3:
                h, m, s = map(float, parts)
                return h * 3600 + m * 60 + s
    # fallback to ffprobe
    cmd = [
        'ffprobe', '-v', 'error',
        '-show_entries', 'format=duration',
        '-of', 'default=noprint_wrappers=1:nokey=1',
        str(file_path)
    ]
    try:
        out = subprocess.run(cmd, capture_output=True, text=True, check=True)
        return float(out.stdout.strip())
    except subprocess.CalledProcessError:
        return 2700.0

def process_video(args):
    checksum, file_loc, file_name, scn_loc, num_screenshots, log_enabled, db_path = args
    logger = logging.getLogger('spoon_engine')
    logger.setLevel(logging.DEBUG if log_enabled else logging.WARNING)
    if log_enabled and not any(isinstance(h, logging.FileHandler) for h in logger.handlers):
        h = logging.FileHandler('spoon_engine.log')
        h.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        logger.addHandler(h)

    try:
        src = Path(file_loc)
        if not src.exists():
            return checksum, False, f"File not found: {file_loc}"

        # fail-fast playback test
        subprocess.run(
            ['ffmpeg', '-v', 'error', '-i', file_loc, '-frames:v', '1', '-f', 'null', '-'],
            capture_output=True, check=True
        )

        duration = get_duration(file_loc, checksum, db_path)

        # build screenshot path
        parts = src.parts
        out_dir = Path(scn_loc)
        if 'tor' in parts:
            idx = parts.index('tor')
            for p in parts[idx + 1 : -1]:
                out_dir /= p
        else:
            out_dir /= src.parent.name
        out_dir.mkdir(parents=True, exist_ok=True)

        base = src.stem
        for i in range(1, num_screenshots + 1):
            ts = random.uniform(duration * 0.1, duration * 0.9)
            dest = out_dir / f"{base}_{i}.jpg"
            subprocess.run(
                ['ffmpeg', '-ss', str(ts), '-i', file_loc,
                 '-vframes', '1', '-q:v', '2', '-y', str(dest)],
                capture_output=True, check=True
            )
            logger.debug(f"Screenshot {i}/{num_screenshots} for {file_name}")

        # strip metadata
        temp = src.with_suffix('.temp.mkv')
        subprocess.run(
            ['ffmpeg', '-i', file_loc, '-map_metadata', '-1',
             '-c', 'copy', '-bitexact', '-y', str(temp)],
            capture_output=True, check=True
        )
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
    num = 7 if total <= 8 else 1
    logger.info(f"Processing {total} files with {num} screenshots each")

    args = [(c, loc, name, scn_loc, num, log_enabled, db_path) for c, loc, name in files]
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
