#!/usr/bin/env python3

import sqlite3
import json
import logging
import re
from collections import Counter

def setup_logging():
    """Setup logging based on 2jznoshit.json preferences"""
    try:
        with open('2jznoshit.json', 'r') as f:
            log_enabled = json.load(f).get('fast_five', {}).get('logs', False)
        
        if log_enabled:
            logging.basicConfig(
                filename='fast_five.log',
                level=logging.DEBUG,
                format='%(asctime)s - %(levelname)s - %(message)s',
                filemode='a'
            )
            logging.info("Fast Five initialized - full debug and verbose logging enabled")
        else:
            logging.disable(logging.CRITICAL)
    except:
        logging.disable(logging.CRITICAL)

def load_config():
    """Load user preferences and settings"""
    try:
        with open('2jznoshit.json', 'r') as f:
            return json.load(f)
    except Exception as e:
        logging.error(f"Failed to load config: {e}")
        return {}

def normalize_season_episode(value, prefix):
    """Normalize season/episode numbers - FIX 1: Proper season capitalization"""
    if not value:
        return None
    
    # Convert to string and remove duplicate words (Season Season -> Season)
    value_str = str(value)
    value_str = re.sub(r'\b(Season|season)\s+\1\b', r'\1', value_str, flags=re.IGNORECASE)
    
    # Capitalize 'Season' or 'Episode' properly
    value_str = re.sub(r'\bseason\b', 'Season', value_str, flags=re.IGNORECASE)
    value_str = re.sub(r'\bepisode\b', 'Episode', value_str, flags=re.IGNORECASE)
    
    # Extract number and format
    match = re.search(r'(\d+)', value_str)
    if match:
        num = int(match.group(1))
        return f"{prefix} {num:02d}"
    
    return value_str

def normalize_airdate(date_str):
    """Normalize airdate to 'Month Day, Year' format"""
    if not date_str:
        return None
    
    months = {
        '01': 'January', '02': 'February', '03': 'March', '04': 'April',
        '05': 'May', '06': 'June', '07': 'July', '08': 'August',
        '09': 'September', '10': 'October', '11': 'November', '12': 'December'
    }
    
    # YYYY-MM-DD format
    match = re.search(r'(\d{4})-(\d{1,2})-(\d{1,2})', str(date_str))
    if match:
        year, month, day = match.groups()
        month_name = months.get(month.zfill(2))
        if month_name:
            return f"{month_name} {int(day)}, {year}"
    
    return str(date_str).strip()

def normalize_resolution(res_str):
    """Normalize resolution to standard format"""
    if not res_str:
        return None
    
    res_lower = str(res_str).lower()
    if '2160' in res_lower or '4k' in res_lower or 'uhd' in res_lower:
        return '2160p'
    elif '1080' in res_lower:
        return '1080p'
    elif '720' in res_lower:
        return '720p'
    elif '576' in res_lower:
        return '576p'
    elif '480' in res_lower:
        return '480p'
    else:
        return 'SD'

def normalize_audio_codec(codec):
    """Normalize audio codec - caps case, clean dashes"""
    if not codec:
        return None
    
    codec_str = str(codec).upper().replace('-', '')
    if 'EAC3' in codec_str or 'E-AC-3' in str(codec).upper():
        return 'EAC3'
    elif 'AC3' in codec_str:
        return 'AC3'
    elif 'DTS' in codec_str:
        return 'DTS'
    elif 'TRUEHD' in codec_str:
        return 'TrueHD'
    
    return str(codec).upper()

def normalize_video_bitrate(bitrate):
    """Normalize video bitrate - over 2500kbps post in Mbps"""
    if not bitrate:
        return None
    
    match = re.search(r'([\d.]+)', str(bitrate))
    if not match:
        return str(bitrate)
    
    value = float(match.group(1))
    if value > 2500:
        return f"{value / 1000:.2f} Mbps"
    
    return str(bitrate)

def normalize_audio_bitrate(bitrate):
    """Normalize audio bitrate format - FIX 2: Ensure proper extraction"""
    if not bitrate or not str(bitrate).strip():
        return None
    
    match = re.search(r'(\d+)', str(bitrate))
    if match:
        return f"{match.group(1)} kbps"
    
    return str(bitrate)

def format_duration_in_minutes(seconds):
    """Convert seconds to minutes format - FIX 3"""
    if not seconds:
        return seconds
    
    try:
        total_seconds = int(float(seconds))
        minutes = total_seconds / 60
        return f"{minutes:.1f} minutes"
    except (ValueError, TypeError):
        return seconds

def normalize_file_size(size_str):
    """Normalize file size - over 2048 MB post in GB"""
    if not size_str:
        return None
    
    match = re.search(r'([\d.]+)', str(size_str))
    if not match:
        return str(size_str)
    
    value = float(match.group(1))
    if 'gb' in str(size_str).lower():
        return f"{value:.1f} GB"
    elif value > 2048:
        return f"{value / 1024:.1f} GB"
    else:
        return f"{value:.1f} MB"

def normalize_language(lang_code):
    """Normalize 2-3 letter codes to full names"""
    if not lang_code:
        return None
    
    lang_map = {
        'en': 'English', 'eng': 'English',
        'es': 'Spanish', 'spa': 'Spanish', 
        'fr': 'French', 'fra': 'French',
        'de': 'German', 'ger': 'German'
    }
    
    return lang_map.get(str(lang_code).lower(), str(lang_code).title())

def normalize_subtitles(ff_subs, mi_subs, it_subs):
    """Normalize subtitle info: Internal/External/Internal & External/Not Included"""
    has_internal = bool((ff_subs and str(ff_subs).strip()) or (mi_subs and str(mi_subs).strip()))
    has_external = bool(it_subs and str(it_subs).strip())
    
    if has_internal and has_external:
        return "Internal & External"
    elif has_internal:
        return "Internal"
    elif has_external:
        return "External"
    else:
        return "Not Included"

def normalize_source(src, config):
    """Normalize source - caps case, add Web Download"""
    if not src:
        return None
    
    library = config.get('library', {})
    src_lower = str(src).lower()
    
    if src_lower in library:
        src_name = library[src_lower]['it_src'].title()
        return f"{src_name} Web Download"
    
    return f"{str(src).title()} Web Download"

def get_source_short(src, config):
    """Get abbreviated source"""
    if not src:
        return None
    
    library = config.get('library', {})
    src_lower = str(src).lower()
    
    if src_lower in library:
        return library[src_lower]['it_src_short']
    
    user_default = config.get('user_input', {}).get('default', {})
    return user_default.get('it_src_short')

def get_longest_description(*descriptions):
    """Get longest non-empty description"""
    valid = [d for d in descriptions if d and str(d).strip()]
    return max(valid, key=len) if valid else None

def get_shortest_description(*descriptions):
    """Get shortest non-empty description"""
    valid = [d for d in descriptions if d and str(d).strip()]
    return min(valid, key=len) if valid else None

def get_majority_answer(*values):
    """Get most common non-empty value"""
    valid = [v for v in values if v and str(v).strip()]
    if not valid:
        return None
    counter = Counter(valid)
    return counter.most_common(1)[0][0]

def get_prioritized_network(*networks):
    """Get trusted airing network with priority: TMDB > TVDb > IMDb > TV Maze"""
    for network in networks:
        if network is not None:
            cleaned = str(network).strip()
            if cleaned and cleaned.lower() not in ("null", "none", "critical content"):
                return cleaned
    return None

def combine_genres(*genre_lists):
    """Combine genres from multiple sources"""
    all_genres = []
    for genre_list in genre_lists:
        if genre_list:
            genres = re.split(r'[,;|]+', str(genre_list))
            all_genres.extend([g.strip().title() for g in genres if g.strip()])
    
    unique = []
    seen = set()
    for genre in all_genres:
        if genre.lower() not in seen:
            unique.append(genre)
            seen.add(genre.lower())
    
    return ", ".join(unique) if unique else None

def process_qtr_mile_data(conn, config):
    """Main data processing function"""
    logging.info("Starting quarter mile data collection and parsing")
    
    try:
        cursor = conn.cursor()
        
        # Single query to get all data joined by checksum
        query = """
        SELECT DISTINCT i.it_checksum,
               i.it_sea_no, i.it_ep_no, i.it_subtitles, i.it_series,
               fp.ff_resolution, fp.ff_hdr, fp.ff_codec_basic, fp.ff_codec_adv, 
               fp.ff_vid_br, fp.ff_fr, fp.ff_aud_codec, fp.ff_aud_chan, 
               fp.ff_aud_sr, fp.ff_aud_br, fp.ff_ep_dur, fp.ff_size, 
               fp.ff_subtitles, fp.ff_language,
               mi.mi_resolution, mi.mi_hdr, mi.mi_codec_basic, mi.mi_codec_adv,
               mi.mi_vid_br, mi.mi_fr, mi.mi_aud_codec, mi.mi_aud_chan,
               mi.mi_aud_sr, mi.mi_aud_br, mi.mi_ep_dur, mi.mi_size,
               mi.mi_subtitles, mi.mi_language,
               oc.pw_series, oc.pw_sea_desc, oc.pw_sea_yr, oc.pw_ep_desc, oc.pw_ep_title,
               oc.pw_air, oc.pw_nw_rat, oc.pw_genre,
               hs.tvm_id, hs.tvm_series_desc, hs.tvm_sea_yr, hs.tvm_ep_desc, hs.tvm_ep_title,
               hs.tvm_air, hs.tvm_studio, hs.tvm_genre, hs.tvm_nw_rat, hs.tvm_cast,
               h.imdb_id, h.imdb_series_desc, h.imdb_sea_yr, h.imdb_ep_desc, h.imdb_ep_title,
               h.imdb_air, h.imdb_studio, h.imdb_genre, h.imdb_nw_rat, h.imdb_cast,
               af.tmdb_id, af.tmdb_series_desc, af.tmdb_sea_yr, af.tmdb_ep_desc, af.tmdb_ep_title,
               af.tmdb_air, af.tmdb_studio, af.tmdb_genre, af.tmdb_nw_rat, af.tmdb_cast,
               dc.tvdb_id, dc.tvdb_series_desc, dc.tvdb_sea_yr, dc.tvdb_ep_desc, dc.tvdb_ep_title,
               dc.tvdb_air, dc.tvdb_studio, dc.tvdb_genre
        FROM import_tuner i
        LEFT JOIN ford_probe fp ON i.it_checksum = fp.it_checksum
        LEFT JOIN miata_info mi ON i.it_checksum = mi.it_checksum  
        LEFT JOIN officer_oconner oc ON i.it_checksum = oc.it_checksum
        LEFT JOIN honda_s2000 hs ON i.it_checksum = hs.it_checksum
        LEFT JOIN hector h ON i.it_checksum = h.it_checksum
        LEFT JOIN any_flavor af ON i.it_checksum = af.it_checksum
        LEFT JOIN doms_charger dc ON i.it_checksum = dc.it_checksum
        WHERE i.it_checksum IS NOT NULL
        """
        
        cursor.execute(query)
        rows = cursor.fetchall()
        
        logging.info(f"Processing {len(rows)} video files")
        
        release_group = config.get('release_group', {})
        user_input = config.get('user_input', {}).get('default', {})
        
        # Process in batches for memory efficiency
        batch_size = 100
        for i in range(0, len(rows), batch_size):
            batch = rows[i:i + batch_size]
            
            conn.execute("BEGIN TRANSACTION")
            
            for row in batch:
                qm_data = process_row(row, config, release_group, user_input)
                insert_or_update_qtr_mile(cursor, qm_data)
            
            conn.commit()
            logging.debug(f"Processed batch {i//batch_size + 1}")
        
        logging.info("Quarter mile data processing complete")
        
    except Exception as e:
        conn.rollback()
        logging.error(f"Processing failed: {e}")
        raise

def process_row(row, config, release_group, user_input):
    """Process single row of joined data"""
    (checksum, it_sea_no, it_ep_no, it_subtitles, it_series,
     ff_resolution, ff_hdr, ff_codec_basic, ff_codec_adv, ff_vid_br, ff_fr,
     ff_aud_codec, ff_aud_chan, ff_aud_sr, ff_aud_br, ff_ep_dur, ff_size,
     ff_subtitles, ff_language,
     mi_resolution, mi_hdr, mi_codec_basic, mi_codec_adv, mi_vid_br, mi_fr,
     mi_aud_codec, mi_aud_chan, mi_aud_sr, mi_aud_br, mi_ep_dur, mi_size,
     mi_subtitles, mi_language,
     pw_series, pw_sea_desc, pw_sea_yr, pw_ep_desc, pw_ep_title,
     pw_air, pw_nw_rat, pw_genre,
     tvm_id, tvm_series_desc, tvm_sea_yr, tvm_ep_desc, tvm_ep_title,
     tvm_air, tvm_studio, tvm_genre, tvm_nw_rat, tvm_cast,
     imdb_id, imdb_series_desc, imdb_sea_yr, imdb_ep_desc, imdb_ep_title,
     imdb_air, imdb_studio, imdb_genre, imdb_nw_rat, imdb_cast,
     tmdb_id, tmdb_series_desc, tmdb_sea_yr, tmdb_ep_desc, tmdb_ep_title,
     tmdb_air, tmdb_studio, tmdb_genre, tmdb_nw_rat, tmdb_cast,
     tvdb_id, tvdb_series_desc, tvdb_sea_yr, tvdb_ep_desc, tvdb_ep_title,
     tvdb_air, tvdb_studio, tvdb_genre) = row
    
    return {
        'it_checksum': checksum,
        'qm_series': pw_series or it_series,
        'qm_sea_no': normalize_season_episode(it_sea_no, "Season"),
        'qm_ep_no': normalize_season_episode(it_ep_no, "Episode"),
        'qm_ser_desc': get_longest_description(tmdb_series_desc, tvdb_series_desc, imdb_series_desc, tvm_series_desc),
        'qm_sea_desc': pw_sea_desc or get_shortest_description(tmdb_series_desc, tvdb_series_desc, imdb_series_desc, tvm_series_desc),
        'qm_ep_desc': pw_ep_desc or get_longest_description(tmdb_ep_desc, tvdb_ep_desc, imdb_ep_desc, tvm_ep_desc),
        'qm_ep_tit': pw_ep_title or get_longest_description(tmdb_ep_title, tvdb_ep_title, imdb_ep_title, tvm_ep_title),
        'qm_sea_yr': pw_sea_yr or get_majority_answer(tmdb_sea_yr, tvdb_sea_yr, imdb_sea_yr, tvm_sea_yr),
        'qm_air': normalize_airdate(pw_air or get_majority_answer(tmdb_air, tvdb_air, imdb_air, tvm_air)),
        'qm_res': normalize_resolution(ff_resolution or mi_resolution),
        'qm_hdr': ff_hdr or mi_hdr,
        'qm_vid_bac': ff_codec_basic or mi_codec_basic,
        'qm_vid_adv': ff_codec_adv or mi_codec_adv,
        'qm_vid_br': normalize_video_bitrate(mi_vid_br or ff_vid_br),
        'qm_vid_fr': ff_fr or mi_fr,
        'qm_aud_cdc': normalize_audio_codec(ff_aud_codec or mi_aud_codec),
        'qm_aud_chn': ff_aud_chan or mi_aud_chan,
        'qm_aud_sr': ff_aud_sr or mi_aud_sr,
        'qm_aud_br': normalize_audio_bitrate(ff_aud_br or mi_aud_br),
        'qm_dur': format_duration_in_minutes(ff_ep_dur or mi_ep_dur),
        'qm_lan': normalize_language(ff_language or mi_language),
        'qm_sub': normalize_subtitles(ff_subtitles, mi_subtitles, it_subtitles),
        'qm_size': normalize_file_size(ff_size or mi_size),
        'qm_net': get_prioritized_network(tmdb_studio, tvdb_studio, imdb_studio, tvm_studio),
        'qm_genre': combine_genres(tmdb_genre, tvdb_genre, imdb_genre, tvm_genre, pw_genre),
        'qm_rat': pw_nw_rat or get_majority_answer(tmdb_nw_rat, imdb_nw_rat, tvm_nw_rat),
        'qm_cast': get_majority_answer(tmdb_cast, imdb_cast, tvm_cast),
        'qm_imdb': imdb_id,
        'qm_tmdb': tmdb_id,
        'qm_maze': tvm_id,
        'qm_tvdb': tvdb_id,
        'qm_src': normalize_source(user_input.get('it_src'), config),
        'qm_src_short': get_source_short(user_input.get('it_src'), config),
        'qm_rg': release_group.get('full'),
        'qm_rga': release_group.get('short')
    }

def insert_or_update_qtr_mile(cursor, qm_data):
    """Insert or update qtr_mile record"""
    cursor.execute("SELECT COUNT(*) FROM qtr_mile WHERE it_checksum = ?", (qm_data['it_checksum'],))
    exists = cursor.fetchone()[0] > 0
    
    if exists:
        # Update existing
        set_pairs = [f"{k} = ?" for k in qm_data.keys() if k != 'it_checksum']
        values = [v for k, v in qm_data.items() if k != 'it_checksum'] + [qm_data['it_checksum']]
        cursor.execute(f"UPDATE qtr_mile SET {', '.join(set_pairs)} WHERE it_checksum = ?", values)
    else:
        # Insert new
        columns = list(qm_data.keys())
        placeholders = ['?' for _ in columns]
        values = list(qm_data.values())
        cursor.execute(f"INSERT INTO qtr_mile ({', '.join(columns)}) VALUES ({', '.join(placeholders)})", values)

def main():
    """Main execution function"""
    setup_logging()
    
    try:
        config = load_config()
        logging.info("Configuration loaded")
        
        with sqlite3.connect('danger2manifold.db') as conn:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            process_qtr_mile_data(conn, config)
        
        logging.info("Fast Five execution complete")
        print("Complete")
        
    except Exception as e:
        logging.error(f"Fast Five execution failed: {e}")
        print(f"Error: {e}")
        raise

if __name__ == "__main__":
    main()