#!/usr/bin/env python3
"""
galo12.py - Media Library Cleanup Tool
Cleans media folders to maintain proper structure with only video files and subtitles.
"""

import os
import json
import sqlite3
import shutil
import logging
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, as_completed
from typing import Dict, List, Tuple, Set
import sys


class MediaCleaner:
    def __init__(self, config_path: str = "2jznoshit.json"):
        self.config = self._load_config(config_path)
        self.db_path = "danger2manifold.db"
        self.video_extensions = {'.mkv', '.mp4', '.avi', '.mov', '.wmv', '.flv', '.webm', '.m4v'}
        self.subtitle_extensions = {'.srt', '.vtt', '.ass', '.ssa', '.sub', '.idx'}
        self.logger = self._setup_logging()
        
    def _load_config(self, config_path: str) -> Dict:
        """Load configuration from JSON file."""
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except (FileNotFoundError, json.JSONDecodeError) as e:
            print(f"Error loading config: {e}")
            sys.exit(1)
    
    def _setup_logging(self) -> logging.Logger:
        """Setup logging based on config."""
        logger = logging.getLogger('galo12')
        logger.handlers.clear()
        
        if self.config.get('galo12', {}).get('logs', False):
            logging.basicConfig(
                level=logging.DEBUG,
                format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                handlers=[
                    logging.FileHandler('galo12.log'),
                    logging.StreamHandler()
                ]
            )
        else:
            logging.basicConfig(level=logging.ERROR)
        
        return logger
    
    def _get_media_records(self) -> List[Tuple[str, str, str]]:
        """Get media records from database."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT it_checksum, it_subtitles, file_location 
                    FROM import_tuner
                """)
                return cursor.fetchall()
        except sqlite3.Error as e:
            self.logger.error(f"Database error: {e}")
            sys.exit(1)
    
    def _get_series_folders(self, records: List[Tuple[str, str, str]]) -> Set[str]:
        """Extract unique series folder paths from records."""
        series_folders = set()
        for _, _, file_location in records:
            path = Path(file_location)
            # Find series folder (2 levels up from video file)
            if len(path.parents) >= 2:
                series_folder = path.parents[1]
                series_folders.add(str(series_folder))
        return series_folders
    
    def _is_keeper_file(self, file_path: Path) -> bool:
        """Check if file should be kept (video or subtitle)."""
        suffix = file_path.suffix.lower()
        return suffix in self.video_extensions or suffix in self.subtitle_extensions
    
    def _clean_folder(self, folder_path: str) -> Tuple[str, int, int]:
        """Clean a single folder, removing non-media files."""
        folder = Path(folder_path)
        if not folder.exists():
            return folder_path, 0, 0
        
        removed_files = 0
        removed_dirs = 0
        
        try:
            # Remove unwanted files
            for item in folder.rglob('*'):
                if item.is_file() and not self._is_keeper_file(item):
                    try:
                        item.unlink()
                        removed_files += 1
                        self.logger.debug(f"Removed file: {item}")
                    except OSError as e:
                        self.logger.error(f"Failed to remove file {item}: {e}")
            
            # Remove empty directories (bottom-up)
            for item in sorted(folder.rglob('*'), key=lambda p: len(p.parts), reverse=True):
                if item.is_dir() and not any(item.iterdir()):
                    try:
                        item.rmdir()
                        removed_dirs += 1
                        self.logger.debug(f"Removed empty dir: {item}")
                    except OSError as e:
                        self.logger.error(f"Failed to remove dir {item}: {e}")
        
        except Exception as e:
            self.logger.error(f"Error cleaning folder {folder_path}: {e}")
        
        return folder_path, removed_files, removed_dirs
    
    def run(self):
        """Main execution method."""
        self.logger.info("Starting media library cleanup")
        
        # Get media records from database
        records = self._get_media_records()
        if not records:
            self.logger.warning("No records found in database")
            return
        
        # Get unique series folders to clean
        series_folders = self._get_series_folders(records)
        if not series_folders:
            self.logger.warning("No series folders found")
            return
        
        self.logger.info(f"Found {len(series_folders)} series folders to clean")
        
        # Process folders in parallel
        max_workers = min(os.cpu_count() or 1, 8)  # Cap at 8 workers
        total_removed_files = 0
        total_removed_dirs = 0
        
        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            future_to_folder = {
                executor.submit(self._clean_folder, folder): folder 
                for folder in series_folders
            }
            
            for future in as_completed(future_to_folder):
                folder_path, removed_files, removed_dirs = future.result()
                total_removed_files += removed_files
                total_removed_dirs += removed_dirs
                
                if removed_files > 0 or removed_dirs > 0:
                    self.logger.info(
                        f"Cleaned {folder_path}: {removed_files} files, {removed_dirs} dirs"
                    )
        
        self.logger.info(
            f"Cleanup complete: {total_removed_files} files, {total_removed_dirs} dirs removed"
        )


def main():
    """Entry point."""
    try:
        cleaner = MediaCleaner()
        cleaner.run()
    except KeyboardInterrupt:
        print("\nOperation cancelled by user")
        sys.exit(1)
    except Exception as e:
        print(f"Unexpected error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()