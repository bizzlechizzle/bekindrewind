#!/usr/bin/env python3
"""
fast_six.py - NFO file generator for media torrents
Production-ready, cross-platform, optimized for performance
Updated with proper NFO formatting and qm_ep_tit support
"""

import json
import sqlite3
import os
import sys
import re
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import logging


class NFOGenerator:
    __slots__ = ('config_path', 'config', 'db_path', 'template_dir', 'logger', '_template_cache')
    
    # Pre-compiled patterns for performance
    DATE_YMD = re.compile(r'^\d{4}-\d{2}-\d{2}$')
    DATE_MDY = re.compile(r'^\d{2}/\d{2}/\d{4}$')
    NUMERIC = re.compile(r'(\d+(?:\.\d+)?)')
    INVALID_CHARS = re.compile(r'[<>:"/\\|?*]')
    MULTI_UNDERSCORE = re.compile(r'_+')
    
    # Static mappings for O(1) lookups
    LANGUAGE_MAP = {
        'en': 'English', 'eng': 'English', 'es': 'Spanish', 'esp': 'Spanish',
        'fr': 'French', 'fre': 'French', 'de': 'German', 'ger': 'German',
        'it': 'Italian', 'ita': 'Italian', 'pt': 'Portuguese', 'por': 'Portuguese',
        'ru': 'Russian', 'rus': 'Russian', 'ja': 'Japanese', 'jpn': 'Japanese',
        'ko': 'Korean', 'kor': 'Korean', 'zh': 'Chinese', 'chi': 'Chinese'
    }
    
    AUDIO_CODEC_MAP = {
        'E-AC-3': 'EAC3', 'AC-3': 'AC3', 'DTS-HD': 'DTSHD', 'DTS-X': 'DTSX'
    }
    
    def __init__(self, config_path: str = "2jznoshit.json"):
        self.config_path = Path(config_path)
        self.config = self._load_config()
        self.db_path = Path("danger2manifold.db")
        self.template_dir = Path("template")
        self.logger = self._setup_logging()
        self._template_cache = {}
        
    def _load_config(self) -> Dict:
        try:
            with open(self.config_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except (FileNotFoundError, json.JSONDecodeError) as e:
            print(f"Config error: {e}")
            sys.exit(1)
    
    def _setup_logging(self) -> logging.Logger:
        logger = logging.getLogger('fast_six')
        logger.handlers.clear()
        
        if self.config.get('fast_six', {}).get('logs', False):
            logging.basicConfig(
                level=logging.DEBUG,
                format='%(asctime)s - %(levelname)s - %(message)s',
                handlers=[
                    logging.FileHandler('fast_six.log'),
                    logging.StreamHandler()
                ]
            )
        else:
            logging.basicConfig(level=logging.WARNING, handlers=[logging.NullHandler()])
        return logger
    
    def _get_db_connection(self) -> sqlite3.Connection:
        try:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            conn.execute("PRAGMA cache_size=10000")
            conn.execute("PRAGMA temp_store=MEMORY")
            return conn
        except sqlite3.Error as e:
            self.logger.error(f"Database connection failed: {e}")
            sys.exit(1)
    
    def _safe_get(self, row: sqlite3.Row, key: str, default=None):
        """Safely get value from sqlite3.Row"""
        try:
            value = row[key]
            return value if value is not None else default
        except (KeyError, IndexError):
            return default
    
    def _get_import_data(self) -> List[sqlite3.Row]:
        with self._get_db_connection() as conn:
            cursor = conn.execute("""
                SELECT it_checksum, it_torrent, it_sea_no, it_def_loc, it_ep_no, it_series
                FROM import_tuner
                WHERE it_def_loc IS NOT NULL AND it_torrent IS NOT NULL
                ORDER BY it_checksum
            """)
            return cursor.fetchall()
    
    def _load_template(self, template_type: str) -> Dict:
        if template_type in self._template_cache:
            return self._template_cache[template_type]
            
        template_path = self.template_dir / f"{template_type}.json"
        try:
            with open(template_path, 'r', encoding='utf-8') as f:
                template = json.load(f)
                self._template_cache[template_type] = template
                return template
        except (FileNotFoundError, json.JSONDecodeError) as e:
            self.logger.error(f"Template error: {e}")
            raise
    
    def _get_qtr_mile_data(self, checksum: str) -> Optional[sqlite3.Row]:
        with self._get_db_connection() as conn:
            cursor = conn.execute("SELECT * FROM qtr_mile WHERE it_checksum = ?", (checksum,))
            return cursor.fetchone()
    
    def _normalize_date(self, date_str: str) -> str:
        if not date_str:
            return ""
        try:
            if self.DATE_YMD.match(date_str):
                dt = datetime.strptime(date_str, '%Y-%m-%d')
            elif self.DATE_MDY.match(date_str):
                dt = datetime.strptime(date_str, '%m/%d/%Y')
            else:
                return date_str
            return dt.strftime('%B %d, %Y')
        except ValueError:
            return date_str
    
    def _normalize_resolution(self, res_str: str) -> str:
        if not res_str:
            return "SD"
        res_lower = str(res_str).lower()
        if '2160' in res_lower or '4k' in res_lower:
            return "2160p"
        elif '1080' in res_lower:
            return "1080p"
        elif '720' in res_lower:
            return "720p"
        elif '576' in res_lower:
            return "576p"
        elif '480' in res_lower:
            return "480p"
        return "SD"
    
    def _normalize_bitrate(self, bitrate_str: str) -> str:
        if not bitrate_str:
            return ""
        
        match = self.NUMERIC.search(str(bitrate_str))
        if not match:
            return str(bitrate_str)
        
        try:
            value = float(match.group(1))
            bitrate_lower = str(bitrate_str).lower()
            if 'mbps' in bitrate_lower:
                return f"{value} Mbps"
            elif value > 2500:
                return f"{value / 1000:.2f} Mbps"
            else:
                return f"{value} kbps"
        except ValueError:
            return str(bitrate_str)
    
    def _normalize_audio_codec(self, codec_str: str) -> str:
        if not codec_str:
            return ""
        codec = str(codec_str).upper()
        for old, new in self.AUDIO_CODEC_MAP.items():
            codec = codec.replace(old, new)
        return codec
    
    def _normalize_language(self, lang_str: str) -> str:
        if not lang_str:
            return "English"
        return self.LANGUAGE_MAP.get(str(lang_str).lower(), str(lang_str).title())
    
    def _normalize_file_size(self, size_str: str) -> str:
        if not size_str:
            return ""
        
        match = self.NUMERIC.search(str(size_str))
        if not match:
            return str(size_str)
        
        try:
            value = float(match.group(1))
            size_lower = str(size_str).lower()
            if 'gb' in size_lower:
                return f"{value} GB"
            elif 'mb' in size_lower:
                return f"{value / 1024:.2f} GB" if value > 2048 else f"{value} MB"
            else:
                return f"{value / 1024:.2f} GB" if value > 2048 else f"{value} MB"
        except ValueError:
            return str(size_str)
    
    def _normalize_source(self, src_str: str) -> str:
        if not src_str:
            return ""
        src = str(src_str).title()
        src_lower = src.lower()
        if 'web' not in src_lower and 'download' not in src_lower:
            src += " Web Download"
        return src
    
    def _normalize_subtitles(self, sub_str: str) -> str:
        if not sub_str:
            return "Not Included"
        sub_lower = str(sub_str).lower()
        if 'internal' in sub_lower and 'external' in sub_lower:
            return "Internal & External"
        elif 'internal' in sub_lower:
            return "Internal"
        elif 'external' in sub_lower:
            return "External"
        return "Not Included"
    
    def _sanitize_filename(self, filename: str) -> str:
        if not filename:
            return "Unknown"
        sanitized = self.INVALID_CHARS.sub('_', str(filename))
        sanitized = self.MULTI_UNDERSCORE.sub('_', sanitized)
        return sanitized.strip('_').strip()
    
    def _format_season_episode(self, season: str, episode: str) -> str:
        """Format season and episode as S##E##"""
        try:
            season_num = int(re.search(r'\d+', str(season)).group()) if season else 1
            episode_num = int(re.search(r'\d+', str(episode)).group()) if episode else 1
            return f"S{season_num:02d}E{episode_num:02d}"
        except (AttributeError, ValueError):
            return f"S01E{episode}" if episode else "S01E01"
    
    def _build_episode_list(self, import_row: sqlite3.Row) -> Tuple[str, str, str, int]:
        torrent_type = self._safe_get(import_row, 'it_torrent')
        if torrent_type not in ['season', 'series']:
            return "", "", "", 0
        
        try:
            with self._get_db_connection() as conn:
                if torrent_type == 'series':
                    query = """
                        SELECT i.it_ep_no, i.it_sea_no, q.qm_ep_desc, q.qm_ep_tit, q.qm_air, q.qm_dur, q.qm_size
                        FROM import_tuner i 
                        LEFT JOIN qtr_mile q ON i.it_checksum = q.it_checksum 
                        WHERE i.it_series = ? 
                        ORDER BY CAST(SUBSTR(i.it_sea_no, -2) AS INTEGER), 
                                CAST(SUBSTR(i.it_ep_no, -2) AS INTEGER)
                    """
                    params = (self._safe_get(import_row, 'it_series'),)
                else:
                    query = """
                        SELECT i.it_ep_no, i.it_sea_no, q.qm_ep_desc, q.qm_ep_tit, q.qm_air, q.qm_dur, q.qm_size
                        FROM import_tuner i 
                        LEFT JOIN qtr_mile q ON i.it_checksum = q.it_checksum 
                        WHERE i.it_series = ? AND i.it_sea_no = ? 
                        ORDER BY CAST(SUBSTR(i.it_ep_no, -2) AS INTEGER)
                    """
                    params = (self._safe_get(import_row, 'it_series'), self._safe_get(import_row, 'it_sea_no'))
                
                episodes = conn.execute(query, params).fetchall()
                episode_count = len(episodes)
                
                episode_list = []
                episode_details = []
                
                for ep in episodes:
                    # Use qm_ep_tit if available, fallback to qm_ep_desc, then default
                    ep_title = (self._safe_get(ep, 'qm_ep_tit') or 
                               self._safe_get(ep, 'qm_ep_desc') or 
                               'Episode Title')
                    ep_desc = self._safe_get(ep, 'qm_ep_desc') or ep_title
                    ep_air = self._normalize_date(self._safe_get(ep, 'qm_air') or '')
                    ep_num = self._safe_get(ep, 'it_ep_no') or '01'
                    season_num = self._safe_get(ep, 'it_sea_no') or '01'
                    ep_duration = self._safe_get(ep, 'qm_dur') or ''
                    ep_size = self._normalize_file_size(self._safe_get(ep, 'qm_size') or '')
                    
                    # Format as S##E## for episode list
                    formatted_ep = self._format_season_episode(season_num, ep_num)
                    episode_list.append(f'"{formatted_ep}" - {ep_title}')
                    
                    # Format episode details
                    episode_details.append(
                        f"{formatted_ep} - {ep_title}\n"
                        f"Air Date: {ep_air}\n"
                        f"Duration: {ep_duration}\n"
                        f"Size: {ep_size}\n"
                        f"Description: {ep_desc}\n"
                    )
                
                return '\n'.join(episode_list), '\n'.join(episode_details), episode_count
                
        except Exception as e:
            self.logger.warning(f"Error building episode list: {e}")
            return "", "", "", 0
    
    def _get_container_format(self, file_path: str) -> str:
        """Extract container format from file extension"""
        if not file_path:
            return "MKV"
        
        ext = Path(file_path).suffix.lower()
        container_map = {
            '.mkv': 'MKV',
            '.mp4': 'MP4',
            '.avi': 'AVI',
            '.mov': 'MOV',
            '.wmv': 'WMV',
            '.m4v': 'M4V'
        }
        return container_map.get(ext, 'MKV')
    
    def _format_template_data(self, import_row: sqlite3.Row, qtr_row: sqlite3.Row) -> Dict:
        qtr_data = dict(qtr_row) if qtr_row else {}
        episode_list, episode_details, episode_count = self._build_episode_list(import_row)
        
        file_path = self._safe_get(import_row, 'it_def_loc') or ''
        
        # Build data dict with all required mappings
        data = {
            'qm_series': qtr_data.get('qm_series', ''),
            'qm_sea_no': self._safe_get(import_row, 'it_sea_no') or '01',
            'qm_ep_no': qtr_data.get('qm_ep_no', ''),
            'qm_ser_desc': qtr_data.get('qm_ser_desc', ''),
            'qm_sea_desc': qtr_data.get('qm_sea_desc', ''),
            'qm_ep_desc': qtr_data.get('qm_ep_desc', ''),
            'qm_ep_tit': qtr_data.get('qm_ep_tit', ''),
            'qm_sea_yr': qtr_data.get('qm_sea_yr', ''),
            'qm_air': self._normalize_date(qtr_data.get('qm_air', '')),
            'qm_src': self._normalize_source(qtr_data.get('qm_src', '')),
            'qm_res': self._normalize_resolution(qtr_data.get('qm_res', '')),
            'qm_hdr': qtr_data.get('qm_hdr', 'SDR (BT.709)'),
            'qm_vid_bac': qtr_data.get('qm_vid_bac', ''),
            'qm_vid_adv': qtr_data.get('qm_vid_adv', ''),
            'qm_vid_br': self._normalize_bitrate(qtr_data.get('qm_vid_br', '')),
            'qm_vid_fr': qtr_data.get('qm_vid_fr', ''),
            'qm_aud_cdc': self._normalize_audio_codec(qtr_data.get('qm_aud_cdc', '')),
            'qm_aud_chn': qtr_data.get('qm_aud_chn', ''),
            'qm_aud_sr': qtr_data.get('qm_aud_sr', ''),
            'qm_aud_br': self._normalize_bitrate(qtr_data.get('qm_aud_br', '')),
            'am_aud_br': self._normalize_bitrate(qtr_data.get('am_aud_br', '')),
            'qm_dur': qtr_data.get('qm_dur', ''),
            'qm_lan': self._normalize_language(qtr_data.get('qm_lan', '')),
            'qm_sub': self._normalize_subtitles(qtr_data.get('qm_sub', '')),
            'qm_size': self._normalize_file_size(qtr_data.get('qm_size', '')),
            'qm_container': self._get_container_format(file_path),
            'qm_release_date': datetime.now().strftime('%B %d, %Y'),
            'qm_net': qtr_data.get('qm_net', ''),
            'qm_genre': qtr_data.get('qm_genre', ''),
            'qm_rat': qtr_data.get('qm_rat', ''),
            'qm_cast': qtr_data.get('qm_cast', ''),
            'qm_imdb': qtr_data.get('qm_imdb', ''),
            'qm_tmdb': qtr_data.get('qm_tmdb', ''),
            'qm_maze': qtr_data.get('qm_maze', ''),
            'qm_tvdb': qtr_data.get('qm_tvdb', ''),
            'qm_src_short': qtr_data.get('qm_src_short', ''),
            'qm_rg': qtr_data.get('qm_rg', ''),
            'qm_rga': qtr_data.get('qm_rga', ''),
            'episode_count': episode_count or 1,
            'episode_list': episode_list,  # Legacy format
            'episode_list_formatted': episode_list,  # New formatted version
            'episode_details': episode_details,  # Legacy format
            'episode_details_formatted': episode_details,  # New formatted version
        }
        
        # Clean null/empty values
        return {k: (v if v is not None and str(v).strip() else '') for k, v in data.items()}
    
    def _generate_nfo_content(self, template_data: Dict, template_type: str) -> str:
        template = self._load_template(template_type)
        nfo_lines = []
        
        for line in template.get('template', []):
            try:
                nfo_lines.append(line.format(**template_data))
            except KeyError as e:
                self.logger.warning(f"Missing template variable {e} in {template_type}")
                nfo_lines.append(line)
        
        return '\n'.join(nfo_lines)
    
    def _get_nfo_path(self, import_row: sqlite3.Row) -> Tuple[Path, Path]:
        def_loc_str = self._safe_get(import_row, 'it_def_loc')
        if not def_loc_str:
            raise ValueError(f"it_def_loc is null for checksum: {self._safe_get(import_row, 'it_checksum', 'Unknown')}")
        
        def_loc = Path(def_loc_str)
        series_name = self._sanitize_filename(self._safe_get(import_row, 'it_series') or "Unknown_Series")
        season_num = str(self._safe_get(import_row, 'it_sea_no') or "01")
        episode_num = str(self._safe_get(import_row, 'it_ep_no') or "01")
        torrent_type = self._safe_get(import_row, 'it_torrent')
        
        folder_path = def_loc.parent
        
        if torrent_type == 'series':
            nfo_path = folder_path / f"{series_name}.nfo"
        elif torrent_type == 'season':
            nfo_path = folder_path / f"{series_name} - Season {season_num}.nfo"
        else:
            nfo_path = folder_path / f"{series_name} - {season_num} - {episode_num}.nfo"
            
        return nfo_path, folder_path
    
    def _nfo_needs_update(self, nfo_path: Path, import_row: sqlite3.Row) -> bool:
        if not nfo_path.exists():
            return True
        
        try:
            video_path_str = self._safe_get(import_row, 'it_def_loc')
            if video_path_str:
                video_path = Path(video_path_str)
                if video_path.exists():
                    return nfo_path.stat().st_mtime < video_path.stat().st_mtime
        except (OSError, TypeError):
            pass
        
        return False
    
    def _process_entry(self, import_row: sqlite3.Row) -> bool:
        checksum = self._safe_get(import_row, 'it_checksum', 'Unknown')
        
        try:
            qtr_data = self._get_qtr_mile_data(checksum)
            if not qtr_data:
                self.logger.warning(f"No qtr_mile data for checksum: {checksum}")
                return False
            
            nfo_path, folder_path = self._get_nfo_path(import_row)
            
            if not self._nfo_needs_update(nfo_path, import_row):
                self.logger.info(f"NFO up to date: {nfo_path}")
                return True
            
            template_type = self._safe_get(import_row, 'it_torrent')
            if not template_type:
                self.logger.error(f"No template type for checksum: {checksum}")
                return False
                
            template_data = self._format_template_data(import_row, qtr_data)
            nfo_content = self._generate_nfo_content(template_data, template_type)
            
            folder_path.mkdir(parents=True, exist_ok=True)
            nfo_path.write_text(nfo_content, encoding='utf-8')
            
            self.logger.info(f"Generated NFO: {nfo_path}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error processing {checksum}: {e}")
            return False
    
    def generate_nfos(self, max_workers: int = None) -> Tuple[int, int]:
        if max_workers is None:
            max_workers = min(os.cpu_count() or 1, 16)
        
        import_data = self._get_import_data()
        
        if not import_data:
            self.logger.warning("No valid entries to process")
            return 0, 0
        
        self.logger.info(f"Processing {len(import_data)} entries with {max_workers} workers")
        
        success_count = 0
        error_count = 0
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_entry = {
                executor.submit(self._process_entry, entry): entry 
                for entry in import_data
            }
            
            for future in as_completed(future_to_entry):
                try:
                    if future.result():
                        success_count += 1
                    else:
                        error_count += 1
                except Exception as e:
                    error_count += 1
                    entry = future_to_entry[future]
                    checksum = self._safe_get(entry, 'it_checksum', 'Unknown')
                    self.logger.error(f"Thread execution failed for {checksum}: {e}")
        
        return success_count, error_count


def main():
    try:
        generator = NFOGenerator()
        success, errors = generator.generate_nfos()
        print(f"NFO generation complete: {success} successful, {errors} errors")
        
        if errors > 0:
            print("Check the logs for details on failed entries")
            sys.exit(1)
            
    except KeyboardInterrupt:
        print("\nOperation cancelled by user")
        sys.exit(1)
    except Exception as e:
        print(f"Fatal error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()