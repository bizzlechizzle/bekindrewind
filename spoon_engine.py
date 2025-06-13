#!/usr/bin/env python3

import json
import sqlite3
import subprocess
import os
import sys
import random
import logging
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

class SpoonEngine:
    def __init__(self):
        self.config = self._load_config()
        self.logger = self._setup_logging()
        
    def _load_config(self):
        with open('2jznoshit.json', 'r') as f:
            return json.load(f)
    
    def _setup_logging(self):
        log_enabled = self.config.get('spoon_engine', {}).get('logs', False)
        
        logger = logging.getLogger('spoon_engine')
        logger.handlers.clear()
        
        if log_enabled:
            logger.setLevel(logging.DEBUG)
            handler = logging.FileHandler('spoon_engine.log', mode='w')
            formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        else:
            logger.addHandler(logging.NullHandler())
            logger.setLevel(logging.CRITICAL)
        
        return logger
    
    def _get_file_data(self):
        conn = sqlite3.connect('danger2manifold.db')
        cursor = conn.cursor()
        cursor.execute("SELECT it_checksum, it_file_loc, file_name FROM import_tuner")
        data = cursor.fetchall()
        conn.close()
        return data
    
    def _get_duration(self, checksum):
        conn = sqlite3.connect('danger2manifold.db')
        cursor = conn.cursor()
        cursor.execute("SELECT ff_ep_dur FROM ford_probe WHERE it_checksum = ?", (checksum,))
        result = cursor.fetchone()
        conn.close()
        return int(result[0].split()[0]) if result else None
    
    def _test_playback(self, file_path):
        cmd = ['ffmpeg', '-v', 'error', '-xerror', '-i', str(file_path), '-f', 'null', '-']
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=600)
            if result.returncode == 0:
                self.logger.debug(f"Playback test passed: {file_path}")
                return True
            else:
                self.logger.error(f"Playback test failed: {file_path} - {result.stderr}")
                return False
        except subprocess.TimeoutExpired:
            self.logger.error(f"Playback test timeout: {file_path}")
            return False
        except Exception as e:
            self.logger.error(f"Playback test error: {file_path} - {e}")
            return False
    
    def _create_screenshot_dir(self, file_path):
        scn_loc = Path(self.config['user_input']['default']['scn_loc'])
        def_loc = Path(self.config['user_input']['default']['def_loc'])
        file_path = Path(file_path)
        
        # Get relative path from def_loc
        try:
            rel_path = file_path.relative_to(def_loc)
            screenshot_dir = scn_loc / rel_path.parent
        except ValueError:
            # Fallback if file not under def_loc
            screenshot_dir = scn_loc / file_path.parent.name
        
        screenshot_dir.mkdir(parents=True, exist_ok=True)
        return screenshot_dir
    
    def _generate_screenshots(self, file_path, checksum, file_name):
        duration = self._get_duration(checksum)
        if not duration:
            self.logger.error(f"No duration found for checksum: {checksum}")
            return False
        
        screenshot_dir = self._create_screenshot_dir(file_path)
        base_name = Path(file_name).stem
        
        # Always minimum 7 screenshots
        num_screenshots = max(7, min(duration // 300, 20))
        
        success_count = 0
        for i in range(num_screenshots):
            timestamp = random.randint(int(duration * 0.1), int(duration * 0.9))
            screenshot_path = screenshot_dir / f"{base_name}_{i+1}.jpeg"
            
            cmd = [
                'ffmpeg', '-v', 'error', '-ss', str(timestamp), '-i', str(file_path),
                '-vframes', '1', '-q:v', '2', '-y', str(screenshot_path)
            ]
            
            try:
                result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
                if result.returncode == 0:
                    success_count += 1
                    self.logger.debug(f"Screenshot created: {screenshot_path}")
                else:
                    self.logger.error(f"Screenshot failed: {screenshot_path} - {result.stderr}")
            except subprocess.TimeoutExpired:
                self.logger.error(f"Screenshot timeout: {screenshot_path}")
            except Exception as e:
                self.logger.error(f"Screenshot error: {screenshot_path} - {e}")
        
        return success_count >= 7
    
    def _clean_metadata(self, file_path):
        file_path = Path(file_path)
        temp_path = file_path.with_suffix(file_path.suffix + '.tmp')
        
        cmd = [
            'ffmpeg', '-v', 'error', '-i', str(file_path),
            '-map_metadata', '-1',
            '-map_chapters', '-1',
            '-c', 'copy',
            '-f', 'matroska',
            '-y', str(temp_path)
        ]
        
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
            if result.returncode == 0:
                os.replace(temp_path, file_path)
                self.logger.debug(f"Metadata cleaned: {file_path}")
                return True
            else:
                self.logger.error(f"Metadata cleaning failed: {file_path} - {result.stderr}")
                if temp_path.exists():
                    temp_path.unlink()
                return False
        except subprocess.TimeoutExpired:
            self.logger.error(f"Metadata cleaning timeout: {file_path}")
            if temp_path.exists():
                temp_path.unlink()
            return False
        except Exception as e:
            self.logger.error(f"Metadata cleaning error: {file_path} - {e}")
            if temp_path.exists():
                temp_path.unlink()
            return False
    
    def _process_file(self, file_data):
        checksum, file_loc, file_name = file_data
        file_path = Path(file_loc)
        
        self.logger.info(f"Processing: {file_name}")
        
        # Step 1: Test playback with fail-fast
        if not self._test_playback(file_path):
            return False
        
        # Step 2: Generate screenshots
        if not self._generate_screenshots(file_path, checksum, file_name):
            return False
        
        # Step 3: Clean metadata without remuxing
        if not self._clean_metadata(file_path):
            return False
        
        self.logger.info(f"Completed: {file_name}")
        return True
    
    def run(self):
        self.logger.info("Starting spoon_engine")
        
        files = self._get_file_data()
        if not files:
            print("No files found in database")
            return
        
        print(f"Processing {len(files)} files...")
        
        max_workers = min(os.cpu_count() or 4, 16)
        success_count = 0
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_file = {executor.submit(self._process_file, f): f for f in files}
            
            for future in as_completed(future_to_file):
                try:
                    if future.result():
                        success_count += 1
                except Exception as e:
                    file_data = future_to_file[future]
                    self.logger.error(f"Processing failed for {file_data[2]}: {e}")
        
        print(f"Completed: {success_count}/{len(files)} files processed successfully")
        self.logger.info(f"Finished: {success_count}/{len(files)} successful")

if __name__ == "__main__":
    try:
        engine = SpoonEngine()
        engine.run()
    except FileNotFoundError:
        print("Config file 2jznoshit.json not found")
        sys.exit(1)
    except Exception as e:
        print(f"Fatal error: {e}")
        sys.exit(1)