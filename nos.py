#!/usr/bin/env python3

import json
import sqlite3
import logging
from datetime import datetime
import sys

def setup_logging(enabled):
    if not enabled:
        logging.disable(logging.CRITICAL)
        return
    
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler("nos.log"),
            logging.StreamHandler(sys.stdout)
        ]
    )

def load_preferences():
    try:
        with open('2jznoshit.json', 'r') as f:
            return json.load(f)
    except Exception as e:
        print(f"Error loading preferences: {e}")
        sys.exit(1)

def parse_bye_monica(bye_monica_data):
    if not bye_monica_data:
        return {k: None for k in ['pw_series', 'pw_sea_no', 'pw_sea_desc', 'pw_sea_yr', 'pw_ep_no', 'pw_ep_title', 'pw_ep_desc', 'pw_air', 'pw_ep_dur', 'pw_genre', 'pw_nw_rat', 'pw_ep_img', 'pw_series_img', 'pw_ep_avl']}
    
    try:
        data = json.loads(bye_monica_data) if isinstance(bye_monica_data, str) else bye_monica_data
        
        # Direct mapping - removed pw_studio, pw_cast, and pw_src_link
        return {
            'pw_series': data.get('pw_series'),
            'pw_sea_no': data.get('pw_sea_no'),
            'pw_sea_desc': data.get('pw_sea_desc'),
            'pw_sea_yr': data.get('pw_sea_yr'),
            'pw_ep_no': data.get('pw_ep_no'),
            'pw_ep_title': data.get('pw_ep_title'),
            'pw_ep_desc': data.get('pw_ep_desc'),
            'pw_air': data.get('pw_air'),
            'pw_ep_dur': data.get('pw_ep_dur'),
            'pw_genre': data.get('pw_genre'),
            'pw_nw_rat': data.get('pw_nw_rat'),
            'pw_ep_img': data.get('pw_ep_img'),
            'pw_series_img': data.get('pw_series_img'),
            'pw_ep_avl': data.get('pw_ep_avl')
        }
    except (json.JSONDecodeError, AttributeError, TypeError):
        return {k: None for k in ['pw_series', 'pw_sea_no', 'pw_sea_desc', 'pw_sea_yr', 'pw_ep_no', 'pw_ep_title', 'pw_ep_desc', 'pw_air', 'pw_ep_dur', 'pw_genre', 'pw_nw_rat', 'pw_ep_img', 'pw_series_img', 'pw_ep_avl']}

def calculate_missing_episodes(current_ep, available_eps):
    if not current_ep or not available_eps:
        return "none"
    
    try:
        current = int(current_ep)
        total = int(available_eps)
        if current <= total:
            return "none"
        else:
            return f"missing_{total - current}"
    except:
        return "none"

def main():
    # Step 1: Load preferences and setup logging
    prefs = load_preferences()
    logs_enabled = prefs.get('nos', {}).get('logs', False)
    setup_logging(logs_enabled)
    
    logging.info("Starting nos.py execution")
    
    # Step 2: Open database and connect to tables
    try:
        conn = sqlite3.connect('danger2manifold.db')
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        logging.info("Connected to danger2manifold.db")
    except Exception as e:
        logging.error(f"Database connection failed: {e}")
        sys.exit(1)
    
    # Step 3: Read checksums and find matches
    try:
        # Get all checksums from monica table
        cursor.execute("SELECT it_checksum, bye_monica FROM monica WHERE it_checksum IS NOT NULL")
        monica_rows = cursor.fetchall()
        logging.info(f"Found {len(monica_rows)} rows in monica table")
        
        # Get all checksums from officer_oconner table
        cursor.execute("SELECT it_checksum, it_ep_no FROM officer_oconner WHERE it_checksum IS NOT NULL")
        officer_rows = {row['it_checksum']: row for row in cursor.fetchall()}
        logging.info(f"Found {len(officer_rows)} rows in officer_oconner table")
        
    except Exception as e:
        logging.error(f"Error reading tables: {e}")
        conn.close()
        sys.exit(1)
    
    # Step 4: Parse bye_monica and update officer_oconner
    updates = 0
    
    for monica_row in monica_rows:
        checksum = monica_row['it_checksum']
        
        if checksum not in officer_rows:
            logging.warning(f"Checksum {checksum} not found in officer_oconner table")
            continue
        
        # Parse bye_monica data
        parsed_data = parse_bye_monica(monica_row['bye_monica'])
        
        # Calculate pw_ep_dif
        current_ep = officer_rows[checksum]['it_ep_no']
        available_eps = parsed_data.get('pw_ep_avl')
        parsed_data['pw_ep_dif'] = calculate_missing_episodes(current_ep, available_eps)
        
        # Build update query
        update_fields = []
        update_values = []
        
        for field, value in parsed_data.items():
            update_fields.append(f"{field} = ?")
            update_values.append(value)
        
        if update_fields:
            update_values.append(checksum)
            update_query = f"UPDATE officer_oconner SET {', '.join(update_fields)} WHERE it_checksum = ?"
            
            try:
                cursor.execute(update_query, update_values)
                updates += 1
                logging.debug(f"Updated checksum {checksum}")
            except Exception as e:
                logging.error(f"Error updating checksum {checksum}: {e}")
    
    # Commit changes and close
    try:
        conn.commit()
        logging.info(f"Successfully updated {updates} rows")
    except Exception as e:
        logging.error(f"Error committing changes: {e}")
    finally:
        conn.close()
        logging.info("Database connection closed")

if __name__ == "__main__":
    main()