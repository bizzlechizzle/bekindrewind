#!/usr/bin/env python3
import json
import sqlite3
import sys
from pathlib import Path

def main():
    try:
        # Step 1: Load config
        with open('2jznoshit.json', 'r') as f:
            config = json.load(f)
        log_enabled = config.get('fast_sev', {}).get('logs', False)
        
        if log_enabled:
            print("Starting fast_sev.py")
        
        # Step 2: Open database
        conn = sqlite3.connect('danger2manifold.db')
        conn.row_factory = sqlite3.Row
        
        import_data = [dict(row) for row in conn.execute('SELECT * FROM import_tuner')]
        qtr_data = [dict(row) for row in conn.execute('SELECT * FROM qtr_mile')]
        qtr_map = {item['it_checksum']: item for item in qtr_data}
        
        if log_enabled:
            print(f"Loaded {len(import_data)} import records, {len(qtr_data)} qtr records")
        
        # Step 3: Process files
        for item in import_data:
            checksum = item['it_checksum']
            if checksum not in qtr_map:
                if log_enabled:
                    print(f"No metadata for checksum: {checksum}")
                continue
                
            meta = qtr_map[checksum]
            old_path = Path(item['it_def_loc'])
            
            if not old_path.exists():
                if log_enabled:
                    print(f"File not found: {old_path}")
                continue
            
            # Build components
            series = meta['qm_series'].replace(' ', '.')
            season = meta['qm_sea_no'].split()[-1].zfill(2)
            episode = meta['qm_ep_no'].split()[-1].zfill(2)
            se = f"S{season}E{episode}"
            
            # Episode name parts
            ep_parts = [series, se, meta['qm_res']]
            if meta['qm_hdr'] != 'SDR (BT.709)':
                ep_parts.append(meta['qm_hdr'].replace(' ', '.').replace('(', '').replace(')', ''))
            ep_parts.extend([meta['qm_vid_bac'], meta['qm_aud_cdc']])
            if meta['qm_aud_chn'] in ['5.1', '7.1']:
                ep_parts.append(meta['qm_aud_chn'])
            ep_parts.extend([meta['qm_src_short'], meta['qm_rga']])
            
            # Season name parts  
            season_parts = [series, f"S{season}", meta['qm_res']]
            if meta['qm_hdr'] != 'SDR (BT.709)':
                season_parts.append(meta['qm_hdr'].replace(' ', '.').replace('(', '').replace(')', ''))
            season_parts.extend([meta['qm_vid_bac'], meta['qm_aud_cdc']])
            if meta['qm_aud_chn'] in ['5.1', '7.1']:
                season_parts.append(meta['qm_aud_chn'])
            season_parts.extend([meta['qm_src_short'], meta['qm_rga']])
            
            episode_name = '.'.join(ep_parts)
            season_name = '.'.join(season_parts)
            
            # Rename video file
            new_path = old_path.parent / f"{episode_name}{old_path.suffix}"
            if old_path != new_path:
                if log_enabled:
                    print(f"RENAME: {old_path.name} -> {new_path.name}")
                old_path.rename(new_path)
                
                # Update database
                conn.execute('UPDATE import_tuner SET it_def_loc = ? WHERE it_checksum = ?', 
                           (str(new_path), checksum))
            
            # Handle subtitles
            if item.get('it_subtitles') and item['it_subtitles'].strip():
                sub_path = Path(item['it_subtitles'])
                if sub_path.exists():
                    new_sub = new_path.with_suffix('.srt')
                    if log_enabled:
                        print(f"RENAME SUB: {sub_path.name} -> {new_sub.name}")
                    sub_path.rename(new_sub)
            
            # Create .nfo files based on torrent type
            torrent_type = item['it_torrent']
            if torrent_type in ['series', 'all']:
                series_dir = old_path.parent.parent
                season_dir = old_path.parent
                (series_dir / f"{series}.nfo").touch(exist_ok=True)
                (season_dir / f"{season_name}.nfo").touch(exist_ok=True)
            elif torrent_type == 'season':
                (old_path.parent / f"{season_name}.nfo").touch(exist_ok=True)
            elif torrent_type == 'episode':
                (old_path.parent / f"{episode_name}.nfo").touch(exist_ok=True)
        
        conn.commit()
        conn.close()
        
        if log_enabled:
            print("Completed successfully")
            
    except FileNotFoundError as e:
        print(f"ERROR: Missing file - {e}")
        sys.exit(1)
    except sqlite3.Error as e:
        print(f"ERROR: Database error - {e}")
        sys.exit(1)
    except Exception as e:
        print(f"ERROR: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()