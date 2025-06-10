#!/usr/bin/env python3

import sqlite3
import json
import logging
import os
from pathlib import Path

def setup_logging():
    try:
        with open('2jznoshit.json', 'r') as f:
            config = json.load(f)
        log_enabled = config.get('danger2manifold', {}).get('logs', False)
    except (FileNotFoundError, json.JSONDecodeError):
        log_enabled = False
    
    if not log_enabled:
        logging.disable(logging.CRITICAL)
        return
    
    logging.basicConfig(
        filename='danger2manifold.log',
        level=logging.INFO,
        format='%(asctime)s - %(message)s',
        filemode='a'
    )

def create_database():
    db_name = "danger2manifold.db"
    
    if Path(db_name).exists():
        os.remove(db_name)
    
    tables = [
        ("import_tuner", """
            it_checksum TEXT,
            file_name TEXT,
            file_location TEXT,
            it_series TEXT,
            it_sea_no TEXT,
            it_ep_no TEXT,
            it_ep_title TEXT,
            it_special TEXT,
            it_subtitles TEXT,
            it_src TEXT,
            it_src_link TEXT,
            it_torrent TEXT,
            it_ep_avl INTEGER DEFAULT 0
        """),
        ("ford_probe", """
            it_checksum TEXT,
            it_ep_no TEXT,
            ff_codec_basic TEXT,
            ff_resolution TEXT,
            ff_codec_adv TEXT,
            ff_hdr TEXT,
            ff_vid_br TEXT,
            ff_aud_codec TEXT,
            ff_aud_chan TEXT,
            ff_aud_sr TEXT,
            ff_aud_br TEXT,
            ff_ep_dur TEXT,
            ff_size TEXT,
            ff_subtitles TEXT,
            ff_language TEXT
        """),
        ("miata_info", """
            it_checksum TEXT,
            it_ep_no TEXT,
            mi_codec_basic TEXT,
            mi_resolution TEXT,
            mi_codec_adv TEXT,
            mi_hdr TEXT,
            mi_vid_br TEXT,
            mi_aud_codec TEXT,
            mi_aud_chan TEXT,
            mi_aud_sr TEXT,
            mi_aud_br TEXT,
            mi_ep_dur TEXT,
            mi_size TEXT,
            mi_subtitles TEXT,
            mi_language TEXT
        """),
        ("monica", """
            it_checksum TEXT,
            it_series TEXT,
            it_sea_no TEXT,
            it_src TEXT,
            it_src_link TEXT,
            no_monica TEXT,
            y_monica TEXT
        """),
        ("officer_oconner", """
            it_checksum TEXT,
            it_series TEXT,
            it_sea_no TEXT,
            it_ep_no TEXT,
            it_src TEXT,
            it_src_link TEXT,
            pw_series TEXT,
            pw_sea_no TEXT,
            pw_sea_desc TEXT,
            pw_sea_yr TEXT,
            pw_ep_no TEXT,
            pw_ep_title TEXT,
            pw_ep_desc TEXT,
            pw_air TEXT,
            pw_ep_dur TEXT,
            pw_studio TEXT,
            pw_genre TEXT,
            pw_nw_rat TEXT,
            pw_cast TEXT,
            pw_src_link TEXT,
            pw_ep_img TEXT,
            pw_series_img TEXT,
            pw_ep_avl TEXT,
            pw_ep_dif TEXT
        """),
        ("honda_s2000", """
            it_checksum TEXT,
            it_series TEXT,
            it_sea_no TEXT,
            it_ep_no TEXT,
            tvm_id TEXT,
            tvm_series TEXT,
            tvm_series_desc TEXT,
            tvm_sea_no TEXT,
            tvm_sea_desc TEXT,
            tvm_sea_yr TEXT,
            tvm_ep_no TEXT,
            tvm_ep_title TEXT,
            tvm_ep_desc TEXT,
            tvm_air TEXT,
            tvm_ep_dur TEXT,
            tvm_studio TEXT,
            tvm_genre TEXT,
            tvm_nw_rat TEXT,
            tvm_cast TEXT,
            tvm_src_link TEXT,
            tvm_ep_img TEXT,
            tvm_sea_img TEXT,
            tvm_series_img TEXT,
            tvm_sea_avl TEXT,
            tvm_ep_avl TEXT,
            tvm_mis_ep TEXT
        """),
        ("hector", """
            it_checksum TEXT,
            it_series TEXT,
            it_sea_no TEXT,
            it_ep_no TEXT,
            imdb_id TEXT,
            imdb_series TEXT,
            imdb_series_desc TEXT,
            imdb_sea_no TEXT,
            imdb_sea_desc TEXT,
            imdb_sea_yr TEXT,
            imdb_ep_no TEXT,
            imdb_ep_title TEXT,
            imdb_ep_desc TEXT,
            imdb_air TEXT,
            imdb_ep_dur TEXT,
            imdb_studio TEXT,
            imdb_genre TEXT,
            imdb_nw_rat TEXT,
            imdb_cast TEXT,
            imdb_src_link TEXT,
            imdb_sea_avl TEXT,
            imdb_ep_avl TEXT
        """),
        ("any_flavor", """
            it_checksum TEXT,
            it_series TEXT,
            it_sea_no TEXT,
            it_ep_no TEXT,
            tmdb_id TEXT,
            tmdb_series TEXT,
            tmdb_series_desc TEXT,
            tmdb_sea_no TEXT,
            tmdb_sea_desc TEXT,
            tmdb_sea_yr TEXT,
            tmdb_ep_no TEXT,
            tmdb_ep_title TEXT,
            tmdb_ep_desc TEXT,
            tmdb_air TEXT,
            tmdb_ep_dur TEXT,
            tmdb_studio TEXT,
            tmdb_genre TEXT,
            tmdb_nw_rat TEXT,
            tmdb_cast TEXT,
            tmdb_src_link TEXT,
            tmdb_ep_img TEXT,
            tmdb_sea_img TEXT,
            tmdb_series_img TEXT,
            tmdb_sea_avl TEXT,
            tmdb_ep_avl TEXT
        """),
        ("doms_charger", """
            it_checksum TEXT,
            it_series TEXT,
            it_sea_no TEXT,
            it_ep_no TEXT,
            tvdb_id TEXT,
            tvdb_series TEXT,
            tvdb_series_desc TEXT,
            tvdb_sea_no TEXT,
            tvdb_sea_desc TEXT,
            tvdb_sea_yr TEXT,
            tvdb_ep_no TEXT,
            tvdb_ep_title TEXT,
            tvdb_ep_desc TEXT,
            tvdb_air TEXT,
            tvdb_ep_dur TEXT,
            tvdb_studio TEXT,
            tvdb_genre TEXT,
            tvdb_src_link TEXT,
            tvdb_sea_avl TEXT,
            tvdb_ep_avl TEXT
        """),
        ("140mph", """
            it_checksum TEXT,
            it_series TEXT,
            it_sea_no TEXT,
            it_ep_no TEXT
        """)
    ]
    
    conn = sqlite3.connect(db_name)
    try:
        cursor = conn.cursor()
        
        for table_name, columns in tables:
            cursor.execute(f"CREATE TABLE `{table_name}` ({columns})")
            logging.info(f"Created table: {table_name}")
        
        # Create trigger to update it_ep_avl count
        cursor.execute("""
            CREATE TRIGGER update_ep_count 
            AFTER INSERT ON import_tuner
            BEGIN
                UPDATE import_tuner 
                SET it_ep_avl = (
                    SELECT COUNT(*) 
                    FROM import_tuner 
                    WHERE it_series = NEW.it_series 
                    AND it_sea_no = NEW.it_sea_no
                )
                WHERE it_series = NEW.it_series 
                AND it_sea_no = NEW.it_sea_no;
            END;
        """)
        
        conn.commit()
        logging.info("Database creation complete")
    except sqlite3.Error as e:
        logging.error(f"Database error: {e}")
        raise
    finally:
        conn.close()

def main():
    setup_logging()
    create_database()
    print("Complete")

if __name__ == "__main__":
    main()