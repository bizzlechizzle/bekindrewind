#!/usr/bin/env python3

import sqlite3
import json
import logging
import os

def setup_logging():
    try:
        with open('2jznoshit.json', 'r') as f:
            log_enabled = json.load(f).get('danger2manifold', {}).get('logs', False)
    except:
        log_enabled = False
    
    if log_enabled:
        logging.basicConfig(
            filename='danger2manifold.log',
            level=logging.INFO,
            format='%(asctime)s - %(message)s',
            filemode='a'
        )
    else:
        logging.disable(logging.CRITICAL)

def create_database():
    db_name = "danger2manifold.db"
    
    if os.path.exists(db_name):
        os.remove(db_name)
    
    with sqlite3.connect(db_name) as conn:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        
        conn.executescript("""
            CREATE TABLE import_tuner (
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
                it_ep_avl INTEGER DEFAULT 0,
                it_def_loc TEXT
            );
            
            CREATE TABLE ford_probe (
                it_checksum TEXT,
                it_ep_no TEXT,
                ff_codec_basic TEXT,
                ff_resolution TEXT,
                ff_codec_adv TEXT,
                ff_hdr TEXT,
                ff_vid_br TEXT,
                ff_fr TEXT,
                ff_aud_codec TEXT,
                ff_aud_chan TEXT,
                ff_aud_sr TEXT,
                ff_aud_br TEXT,
                ff_ep_dur TEXT,
                ff_size TEXT,
                ff_subtitles TEXT,
                ff_language TEXT
            );
            
            CREATE TABLE miata_info (
                it_checksum TEXT,
                it_ep_no TEXT,
                mi_codec_basic TEXT,
                mi_resolution TEXT,
                mi_codec_adv TEXT,
                mi_hdr TEXT,
                mi_vid_br TEXT,
                mi_fr TEXT,
                mi_aud_codec TEXT,
                mi_aud_chan TEXT,
                mi_aud_sr TEXT,
                mi_aud_br TEXT,
                mi_ep_dur TEXT,
                mi_size TEXT,
                mi_subtitles TEXT,
                mi_language TEXT
            );
            
            CREATE TABLE monica (
                it_checksum TEXT,
                it_series TEXT,
                it_sea_no TEXT,
                it_ep_no TEXT,
                it_src TEXT,
                it_src_link TEXT,
                no_monica TEXT,
                y_monica TEXT,
                bye_monica TEXT
            );
            
            CREATE TABLE officer_oconner (
                it_checksum TEXT,
                it_sea_no TEXT,
                it_ep_no TEXT,
                pw_series TEXT,
                pw_sea_no TEXT,
                pw_sea_desc TEXT,
                pw_sea_yr TEXT,
                pw_ep_no TEXT,
                pw_ep_title TEXT,
                pw_ep_desc TEXT,
                pw_air TEXT,
                pw_ep_dur TEXT,
                pw_nw_rat TEXT,
                pw_genre TEXT,
                pw_ep_img TEXT,
                pw_series_img TEXT,
                pw_ep_avl TEXT,
                pw_ep_dif TEXT
            );
            
            CREATE TABLE honda_s2000 (
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
            );
            
            CREATE TABLE hector (
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
            );
            
            CREATE TABLE any_flavor (
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
            );
            
            CREATE TABLE doms_charger (
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
            );
            
            CREATE TABLE qtr_mile (
                it_checksum TEXT,
                qm_series TEXT,
                qm_sea_no TEXT,
                qm_ep_no TEXT,
                qm_ser_desc TEXT,
                qm_sea_desc TEXT,
                qm_ep_desc TEXT,
                qm_sea_yr TEXT,
                qm_air TEXT,
                qm_res TEXT,
                qm_hdr TEXT,
                qm_vid_bac TEXT,
                qm_vid_adv TEXT,
                qm_vid_br TEXT,
                qm_vid_fr TEXT,
                qm_aud_cdc TEXT,
                qm_aud_chn TEXT,
                qm_aud_sr TEXT,
                qm_aud_br TEXT,
                qm_dur TEXT,
                qm_lan TEXT,
                qm_sub TEXT,
                qm_size TEXT,
                qm_net TEXT,
                qm_genre TEXT,
                qm_rat TEXT,
                qm_cast TEXT,
                qm_imdb TEXT,
                qm_tmdb TEXT,
                qm_maze TEXT,
                qm_tvdb TEXT,
                qm_src TEXT,
                qm_src_short TEXT,
                qm_rg TEXT,
                qm_rga TEXT
            );
            
            CREATE TRIGGER update_ep_count 
            AFTER INSERT ON import_tuner
            FOR EACH ROW
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
        
        logging.info("Database creation complete")

if __name__ == "__main__":
    setup_logging()
    create_database()
    print("Complete")