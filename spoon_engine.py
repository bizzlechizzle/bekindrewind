#!/usr/bin/env python3

import json
import sqlite3
import subprocess
import random
import logging
import sys
from pathlib import Path
import time

def load_config():
   with open('2jznoshit.json', 'r') as f:
       return json.load(f)

def get_files_from_db(db_path):
   with sqlite3.connect(db_path) as conn:
       conn.execute("PRAGMA journal_mode=WAL")  # Better for concurrent reads
       cur = conn.cursor()
       cur.execute("""
           SELECT it_checksum, it_file_loc, file_name
           FROM import_tuner
           WHERE it_file_loc IS NOT NULL AND it_file_loc != ''
           ORDER BY it_checksum
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
           return max(float(dur), 60.0)
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
       return 2700.0

def build_screenshot_dir(src_path, scn_loc):
   """Build screenshot directory maintaining folder structure."""
   parts = src_path.parts
   out_dir = Path(scn_loc)
   
   if len(parts) > 1:
       out_dir = out_dir / parts[-2]
   
   out_dir.mkdir(parents=True, exist_ok=True)
   return out_dir

def calculate_screenshot_count(duration, total_files):
   """Calculate screenshots based on duration and file count."""
   if total_files <= 8:
       return 7
   return max(1, min(int(duration // 600), 5))

def process_video(args):
   checksum, file_loc, file_name, scn_loc, total_files, log_enabled, db_path, file_idx, total = args
   
   logger = logging.getLogger(f'spoon_engine_{checksum[:8]}')
   logger.setLevel(logging.DEBUG if log_enabled else logging.WARNING)
   
   if log_enabled and not any(isinstance(h, logging.FileHandler) for h in logger.handlers):
       h = logging.FileHandler('spoon_engine.log')
       h.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
       logger.addHandler(h)

   # Terminal progress output
   print(f"[{file_idx}/{total}] Processing: {file_name}", flush=True)

   try:
       src = Path(file_loc)
       if not src.exists():
           return checksum, False, f"File not found: {file_loc}", file_name
       
       # Add size check to prevent processing corrupted files
       if src.stat().st_size < 1024:  # Less than 1KB
           return checksum, False, f"File too small: {file_loc}", file_name

       if log_enabled:
           print(f"  → Testing playback: {file_name}", flush=True)
       
       # Fail-fast playback test
       subprocess.run([
           'ffmpeg', '-v', 'error', '-i', str(src), 
           '-frames:v', '1', '-f', 'null', '-'
       ], capture_output=True, check=True, timeout=30)

       # Get duration and calculate screenshots
       duration = get_duration(str(src), checksum, db_path)
       num_screenshots = calculate_screenshot_count(duration, total_files)
       
       if log_enabled:
           print(f"  → Duration: {duration:.1f}s, Screenshots: {num_screenshots}", flush=True)

       # Build output directory
       out_dir = build_screenshot_dir(src, scn_loc)

       # Set random seed for reproducible results
       random.seed(hash(checksum) & 0x7FFFFFFF)
       
       # Generate screenshots
       if duration < 120:
           timestamps = [duration * 0.5]
       else:
           start_time = duration * 0.1
           end_time = duration * 0.9
           timestamps = sorted(random.uniform(start_time, end_time) 
                             for _ in range(num_screenshots))
       
       base_name = Path(file_name).stem
       
       if log_enabled:
           print(f"  → Taking {len(timestamps)} screenshots", flush=True)
       
       for i, ts in enumerate(timestamps, 1):
           dest = out_dir / f"{base_name}_{i}.jpg"
           
           subprocess.run([
               'ffmpeg', '-ss', f'{ts:.2f}', '-i', str(src),
               '-vframes', '1', '-q:v', '2', '-y', str(dest)
           ], capture_output=True, check=True, timeout=60)
           
           if log_enabled:
               print(f"    • Screenshot {i}/{len(timestamps)} saved", flush=True)

       if log_enabled:
           print(f"  → Stripping metadata", flush=True)

       # Strip metadata with better error handling
       temp = src.with_suffix('.temp.mkv')
       try:
           subprocess.run([
               'ffmpeg', '-i', str(src), '-map_metadata', '-1',
               '-c', 'copy', '-bitexact', '-y', str(temp)
           ], capture_output=True, check=True, timeout=120)
           
           # Atomic replacement
           if temp.exists() and temp.stat().st_size > 0:
               temp.replace(src)
           else:
               if temp.exists():
                   temp.unlink()
               raise subprocess.CalledProcessError(1, 'ffmpeg', b'Output file invalid')
               
       except subprocess.CalledProcessError:
           if temp.exists():
               temp.unlink()
           # Continue without metadata stripping rather than failing
           logger.warning(f"Metadata stripping failed for {file_name}, continuing...")

       print(f"  ✓ Completed: {file_name}", flush=True)
       logger.info(f"Processed: {file_name}")
       return checksum, True, "Success", file_name

   except subprocess.CalledProcessError as e:
       msg = (e.stderr or b'').decode().strip()
       error_msg = f"FFmpeg error: {msg}"
       print(f"  ✗ Failed: {file_name} - {error_msg}", flush=True)
       logger.error(f"Failed {file_name}: {msg}")
       return checksum, False, error_msg, file_name
   except Exception as e:
       error_msg = f"Error: {e}"
       print(f"  ✗ Failed: {file_name} - {error_msg}", flush=True)
       logger.error(f"Failed {file_name}: {e}")
       return checksum, False, error_msg, file_name

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

   print("Starting spoon_engine...", flush=True)
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
   print(f"Found {total} files to process", flush=True)
   
   if log_enabled:
       print("Verbose logging enabled - detailed progress will be shown", flush=True)
   
   logger.info(f"Processing {total} files with dynamic screenshot counts")

   success = fail = 0
   start_time = time.time()
   
   # Process one at a time to avoid network saturation
   for i, (checksum, file_loc, file_name) in enumerate(files, 1):
       args = (checksum, file_loc, file_name, scn_loc, total, log_enabled, db_path, i, total)
       checksum, ok, msg, file_name = process_video(args)
       
       if ok:
           success += 1
       else:
           fail += 1
           print(f"  Error: {msg}", flush=True)

   elapsed = time.time() - start_time
   print(f"\nProcessing complete in {elapsed:.1f}s")
   print(f"Success: {success}, Failed: {fail}")
   
   logger.info(f"Processing complete. Success: {success}, Failed: {fail}")

if __name__ == "__main__":
   main()