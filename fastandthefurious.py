#!/usr/bin/env python3
import json
import sqlite3
import logging
import os
import re
from collections import defaultdict


def setup_logging():
    try:
        with open('2jznoshit.json', 'r') as f:
            log_enabled = json.load(f).get('fastandthefurious', {}).get('logs', False)
    except (FileNotFoundError, json.JSONDecodeError, KeyError):
        log_enabled = False
    
    if log_enabled:
        logging.basicConfig(
            filename='fastandthefurious.log',
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            filemode='a'
        )
    else:
        logging.disable(logging.CRITICAL)


def normalize_episode(ep_str):
    """Extract episode number from string like 'episode 1' or 'S01E01'."""
    if not ep_str:
        return None
    
    # Match patterns like "episode 1", "S01E01", "1", etc.
    match = re.search(r'(?:episode\s+)?(?:S\d+E)?(\d+)', str(ep_str), re.IGNORECASE)
    return int(match.group(1)) if match else None


def delete_file(file_path):
    if file_path and os.path.isfile(file_path):
        try:
            os.remove(file_path)
            logging.info(f"Deleted file: {file_path}")
        except OSError as e:
            logging.error(f"Failed to delete {file_path}: {e}")


def parse_numeric(value):
    if not value or str(value).lower() == 'null':
        return 0
    try:
        # Extract first number from strings like "300 seconds" or "465 MB"
        return float(str(value).split()[0])
    except (ValueError, TypeError, IndexError):
        return 0


def calculate_scores(rows):
    """Calculate scores for all duplicates - shorter/smaller files get penalties."""
    scores = {}
    file_data = {}
    
    # Extract data for each checksum
    for row in rows:
        checksum = row['it_checksum']
        file_data[checksum] = {
            'special': row['it_special'],
            'ff_dur': parse_numeric(row['ff_ep_dur']),
            'ff_size': parse_numeric(row['ff_size']), 
            'mi_dur': parse_numeric(row['mi_ep_dur']),
            'mi_size': parse_numeric(row['mi_size'])
        }
        scores[checksum] = 0
    
    # Special content penalties
    for checksum, data in file_data.items():
        if data['special']:
            special_lower = str(data['special']).lower()
            if any(kw in special_lower for kw in ['sample', 'sneak peek', 'sneak peak', 'featurette', 'teaser', 'trailer']):
                scores[checksum] += 1
                logging.info(f"Special penalty: {checksum} = {data['special']}")
    
    # Find shortest durations and smallest sizes to penalize
    all_ff_durs = [data['ff_dur'] for data in file_data.values() if data['ff_dur'] > 0]
    all_mi_durs = [data['mi_dur'] for data in file_data.values() if data['mi_dur'] > 0]
    all_ff_sizes = [data['ff_size'] for data in file_data.values() if data['ff_size'] > 0]
    all_mi_sizes = [data['mi_size'] for data in file_data.values() if data['mi_size'] > 0]
    
    # Penalize shorter durations
    if all_ff_durs:
        min_ff_dur = min(all_ff_durs)
        for checksum, data in file_data.items():
            if data['ff_dur'] == min_ff_dur and data['ff_dur'] > 0:
                scores[checksum] += 1
    
    if all_mi_durs:
        min_mi_dur = min(all_mi_durs)
        for checksum, data in file_data.items():
            if data['mi_dur'] == min_mi_dur and data['mi_dur'] > 0:
                scores[checksum] += 1
    
    # Penalize smaller sizes
    if all_ff_sizes:
        min_ff_size = min(all_ff_sizes)
        for checksum, data in file_data.items():
            if data['ff_size'] == min_ff_size and data['ff_size'] > 0:
                scores[checksum] += 1
    
    if all_mi_sizes:
        min_mi_size = min(all_mi_sizes)
        for checksum, data in file_data.items():
            if data['mi_size'] == min_mi_size and data['mi_size'] > 0:
                scores[checksum] += 1
    
    for checksum in scores:
        data = file_data[checksum]
        logging.info(f"Score {checksum}: {scores[checksum]} (ff_dur:{data['ff_dur']}, ff_size:{data['ff_size']}, mi_dur:{data['mi_dur']}, mi_size:{data['mi_size']})")
    
    return scores


def prompt_user_decision(checksums, scores):
    print("\nDuplicate files found with similar scores:")
    for checksum in checksums:
        print(f"  {checksum[:16]}...: score {scores[checksum]:.6f}")
    
    while True:
        choice = input("Delete highest score? (y/n): ").lower().strip()
        if choice in ['y', 'yes']:
            return True
        elif choice in ['n', 'no']:
            return False


def main():
    setup_logging()
    
    with sqlite3.connect('danger2manifold.db') as conn:
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        # Get all episodes and normalize episode numbers
        cursor.execute("SELECT it_checksum, it_ep_no, it_series, It_sea_no FROM import_tuner")
        episodes = cursor.fetchall()
        
        if not episodes:
            print("No episodes found in database")
            return
        
        # Group by series/season/normalized episode number
        ep_groups = defaultdict(list)
        for row in episodes:
            ep_num = normalize_episode(row['it_ep_no'])
            if ep_num is not None:
                key = (row['it_series'], row['It_sea_no'], ep_num)
                ep_groups[key].append(row['it_checksum'])
        
        deleted_count = 0
        
        for (series, season, ep_num), checksums in ep_groups.items():
            if len(checksums) <= 1:
                continue
            
            logging.info(f"Processing {len(checksums)} duplicates for {series} S{season}E{ep_num}")
            
            # Get scoring data
            placeholders = ','.join('?' * len(checksums))
            cursor.execute(f"""
                SELECT 
                    it.it_checksum,
                    it.it_special,
                    it.file_location,
                    fp.ff_ep_dur,
                    fp.ff_size,
                    mi.mi_ep_dur,
                    mi.mi_size
                FROM import_tuner it
                LEFT JOIN ford_probe fp ON it.it_checksum = fp.it_checksum
                LEFT JOIN miata_info mi ON it.it_checksum = mi.it_checksum
                WHERE it.it_checksum IN ({placeholders})
            """, checksums)
            
            rows = cursor.fetchall()
            if not rows:
                continue
            
            # Calculate scores
            scores = calculate_scores(rows)
            
            if len(scores) < 2:
                continue
            
            max_score = max(scores.values())
            min_score = min(scores.values())
            score_diff = max_score - min_score
            
            logging.info(f"Score range: {min_score:.6f} to {max_score:.6f} (diff: {score_diff:.6f})")
            
            # Decide whether to delete
            should_delete = False
            if score_diff > 2:
                should_delete = True
            elif score_diff > 0:
                should_delete = prompt_user_decision(checksums, scores)
            
            if should_delete:
                worst_checksum = max(scores, key=scores.get)
                
                # Get file path from the row data
                file_path = None
                for row in rows:
                    if row['it_checksum'] == worst_checksum:
                        file_path = row['file_location']
                        break
                
                logging.info(f"Deleting {worst_checksum} (score: {max_score:.6f})")
                print(f"Deleting duplicate with score {max_score:.6f}")
                
                # Delete from all tables
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
                for (table_name,) in cursor.fetchall():
                    try:
                        cursor.execute(f"DELETE FROM `{table_name}` WHERE it_checksum = ?", (worst_checksum,))
                    except sqlite3.OperationalError:
                        pass
                
                delete_file(file_path)
                deleted_count += 1
        
        # Update episode counts
        cursor.execute("""
            UPDATE import_tuner 
            SET it_ep_avl = (
                SELECT COUNT(*) 
                FROM import_tuner i2 
                WHERE i2.it_series = import_tuner.it_series 
                AND i2.It_sea_no = import_tuner.It_sea_no
            )
        """)
        
        cursor.execute("SELECT COUNT(DISTINCT it_checksum) FROM import_tuner")
        total_checksums = cursor.fetchone()[0]
        
        conn.commit()
        
        logging.info(f"Deleted {deleted_count} duplicates, {total_checksums} total checksums")
        print(f"Deleted {deleted_count} duplicate entries. Total checksums: {total_checksums}")


if __name__ == "__main__":
    main()