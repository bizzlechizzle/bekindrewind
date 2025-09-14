#!/usr/bin/env python3

import argparse
import json
import re
import requests
import sqlite3
from pathlib import Path

def get_config():
    config_path = Path(__file__).parent.parent / "user.json"
    with open(config_path, 'r') as f:
        config = json.load(f)
    return config.get('API_KEYS', {})

def get_records():
    db_path = Path(__file__).parent.parent / "tapedeck.db"
    conn = sqlite3.connect(str(db_path))
    cursor = conn.cursor()

    # Check import table schema
    cursor.execute("PRAGMA table_info(import)")
    cols = {row[1] for row in cursor.fetchall()}

    select_fields = ["checksum"]
    if 'movie' in cols: select_fields.append("movie")
    if 'series' in cols: select_fields.extend(["series", "season", "episode"])

    if len(select_fields) == 1:  # Only checksum
        conn.close()
        return [], cols

    cursor.execute(f"SELECT {', '.join(select_fields)} FROM import")
    data = cursor.fetchall()
    conn.close()
    return data, cols

def call_api(url):
    try:
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            return response.json()
    except:
        pass
    return None

def search_tvmaze(series_name):
    if not series_name:
        return None
    clean_name = re.sub(r'[^\w\s]', '', series_name).strip()
    url = f"https://api.tvmaze.com/search/shows?q={clean_name}"
    result = call_api(url)
    if result and len(result) > 0:
        return result[0].get('show')
    return None

def search_tmdb(title, api_key, is_tv=True):
    if not api_key or not title:
        return None
    clean_title = re.sub(r'[^\w\s]', '', title).strip()
    endpoint = "tv" if is_tv else "movie"
    url = f"https://api.themoviedb.org/3/search/{endpoint}?api_key={api_key}&query={clean_title}"
    result = call_api(url)
    if result and result.get('results'):
        return result['results'][0]
    return None

def search_tvdb(series_name, api_key):
    if not api_key or not series_name:
        return None

    # Get token first
    token_url = "https://api4.thetvdb.com/v4/login"
    try:
        response = requests.post(token_url, json={"apikey": api_key}, timeout=10)
        if response.status_code != 200:
            return None
        token = response.json().get('data', {}).get('token')
        if not token:
            return None

        clean_name = re.sub(r'[^\w\s]', '', series_name).strip()
        search_url = f"https://api4.thetvdb.com/v4/search?query={clean_name}"
        headers = {"Authorization": f"Bearer {token}"}
        search_response = requests.get(search_url, headers=headers, timeout=10)
        if search_response.status_code == 200:
            search_result = search_response.json()
            if search_result.get('data'):
                return search_result['data'][0]
    except:
        pass
    return None

def extract_field(field, tvdb_data, tvmaze_data, imdb_data, tmdb_data):
    # Field-specific priority from instructions (lines 76-94)

    if field == 'dmovie':
        # Priority: online.py > ffprobe > TMDB > IMBD
        if tmdb_data.get('overview'):
            return tmdb_data['overview']
        if imdb_data.get('Plot') and imdb_data['Plot'] != 'N/A':
            return imdb_data['Plot']

    elif field == 'dseries':
        # Priority: online.py > TVDB > TVMAZE > IMBD > TMDB
        if tvdb_data.get('overview'):
            return tvdb_data['overview']
        if tvmaze_data.get('summary'):
            return re.sub(r'<[^>]+>', '', tvmaze_data['summary']).strip()
        if imdb_data.get('Plot') and imdb_data['Plot'] != 'N/A':
            return imdb_data['Plot']
        if tmdb_data.get('overview'):
            return tmdb_data['overview']

    elif field == 'dseason':
        # Priority: online.py > TVDB > TVMAZE > IMBD > TMDB
        if tvdb_data.get('overview'):
            return tvdb_data['overview']
        if tvmaze_data.get('summary'):
            return re.sub(r'<[^>]+>', '', tvmaze_data['summary']).strip()

    elif field == 'depisode':
        # Priority: online.py > ffprobe > TVDB > TVMAZE > IMBD > TMDB
        if tvdb_data.get('overview'):
            return tvdb_data['overview']
        if tvmaze_data.get('summary'):
            return re.sub(r'<[^>]+>', '', tvmaze_data['summary']).strip()

    elif field == 'airdate':
        # Priority: online.py > TVDB > TVMAZE > IMBD > TMDB
        if tvdb_data.get('firstAired'):
            return tvdb_data['firstAired']
        if tvmaze_data.get('premiered'):
            return tvmaze_data['premiered']
        if tmdb_data.get('first_air_date'):
            return tmdb_data['first_air_date']

    elif field == 'network':
        # Priority: online.py > TVDB > TVMAZE > IMBD > TMDB
        if tvdb_data.get('primaryNetwork', {}).get('name'):
            return tvdb_data['primaryNetwork']['name']
        if tvmaze_data.get('network', {}).get('name'):
            return tvmaze_data['network']['name']
        elif tvmaze_data.get('webChannel', {}).get('name'):
            return tvmaze_data['webChannel']['name']
        if tmdb_data.get('networks') and tmdb_data['networks']:
            return tmdb_data['networks'][0]['name']

    elif field == 'genre':
        # Priority: TVDB > TVMAZE > IMBD > TMDB > online.py
        if tvdb_data.get('genres') and tvdb_data['genres']:
            return ', '.join([g['name'] for g in tvdb_data['genres']])
        if tvmaze_data.get('genres'):
            return ', '.join(tvmaze_data['genres'])
        if imdb_data.get('Genre') and imdb_data['Genre'] != 'N/A':
            return imdb_data['Genre']
        if tmdb_data.get('genres'):
            return ', '.join([g['name'] for g in tmdb_data['genres']])

    elif field == 'rating':
        # Priority: TVDB > TVMAZE > IMBD > TMDB > online.py
        if tvdb_data.get('rating'):
            return tvdb_data['rating']
        if imdb_data.get('Rated') and imdb_data['Rated'] != 'N/A':
            return imdb_data['Rated']

    elif field == 'cast':
        # Priority: TVDB > TVMAZE > IMBD > TMDB > online.py (limit to top 5)
        if imdb_data.get('Actors') and imdb_data['Actors'] != 'N/A':
            actors = imdb_data['Actors'].split(', ')
            return ', '.join(actors[:5])

    elif field == 'release':
        # Priority: online.py > TMDB > IMBD
        if tmdb_data.get('release_date'):
            return tmdb_data['release_date']
        if imdb_data.get('Released') and imdb_data['Released'] != 'N/A':
            return imdb_data['Released']

    elif field == 'studio':
        # Priority: online.py > TMDB > IMBD
        if tmdb_data.get('production_companies') and tmdb_data['production_companies']:
            return tmdb_data['production_companies'][0]['name']

    elif field == 'imovie':
        # Priority: online.py > TMDB > IMBD
        if tmdb_data.get('poster_path'):
            return f"https://image.tmdb.org/t/p/w500{tmdb_data['poster_path']}"

    elif field == 'iseries':
        # Priority: online.py > TVDB > TVMAZE > IMBD > TMDB
        if tvdb_data.get('image'):
            return tvdb_data['image']
        if tvmaze_data.get('image', {}).get('original'):
            return tvmaze_data['image']['original']
        if tmdb_data.get('poster_path'):
            return f"https://image.tmdb.org/t/p/w500{tmdb_data['poster_path']}"

    return None

def get_api_ids(checksum):
    """Check if online table already has API IDs for this checksum."""
    db_path = Path(__file__).parent.parent / "tapedeck.db"
    try:
        conn = sqlite3.connect(str(db_path))
        cursor = conn.cursor()
        cursor.execute("SELECT imdb, tmdb, tvmaze, tvdb FROM online WHERE checksum = ?", (checksum,))
        result = cursor.fetchone()
        conn.close()
        if result:
            return {'imdb': result[0], 'tmdb': result[1], 'tvmaze': result[2], 'tvdb': result[3]}
    except:
        pass
    return {'imdb': None, 'tmdb': None, 'tvmaze': None, 'tvdb': None}

def update_online_table(checksum, updates, api_ids):
    """Update online table with metadata and API IDs."""
    if not updates and not any(api_ids.values()):
        return 0

    db_path = Path(__file__).parent.parent / "tapedeck.db"
    conn = sqlite3.connect(str(db_path))
    cursor = conn.cursor()

    # Build update query
    all_updates = {}
    all_updates.update(updates)
    all_updates.update({k: v for k, v in api_ids.items() if v is not None})

    if all_updates:
        fields = ', '.join(f"{k} = ?" for k in all_updates.keys())
        values = list(all_updates.values()) + [checksum]
        cursor.execute(f"UPDATE online SET {fields} WHERE checksum = ?", values)

    conn.commit()
    conn.close()
    return len(updates)

def update_import_table(checksum, api_ids):
    """Write API IDs back to import table (instructions lines 103-107)."""
    if not any(api_ids.values()):
        return

    db_path = Path(__file__).parent.parent / "tapedeck.db"
    conn = sqlite3.connect(str(db_path))
    cursor = conn.cursor()

    # Check what API ID columns exist in import table
    cursor.execute("PRAGMA table_info(import)")
    cols = {row[1] for row in cursor.fetchall()}

    import_updates = {}
    if 'imdb' in cols and api_ids['imdb']: import_updates['imdb'] = api_ids['imdb']
    if 'tmdb' in cols and api_ids['tmdb']: import_updates['tmdb'] = api_ids['tmdb']
    if 'tvmaze' in cols and api_ids['tvmaze']: import_updates['tvmaze'] = api_ids['tvmaze']
    if 'tvdb' in cols and api_ids['tvdb']: import_updates['tvdb'] = api_ids['tvdb']

    if import_updates:
        fields = ', '.join(f"{k} = ?" for k in import_updates.keys())
        values = list(import_updates.values()) + [checksum]
        cursor.execute(f"UPDATE import SET {fields} WHERE checksum = ?", values)

    conn.commit()
    conn.close()

def main():
    parser = argparse.ArgumentParser(description="API metadata import")
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()

    api_keys = get_config()
    records, import_cols = get_records()

    if not records:
        print("No records found in import table")
        return

    if args.verbose:
        print(f"Processing {len(records)} records from import table")

    total_updated = 0

    for record in records:
        checksum = record[0]

        # Parse record based on actual column order from select_fields
        movie = None
        series = None

        if 'movie' in import_cols and len(record) > 1:
            movie = record[1]
        elif 'series' in import_cols and len(record) > 1:
            series = record[1]

        # Check existing API IDs in online table
        existing_api_ids = get_api_ids(checksum)

        # Search APIs to get new API IDs and data
        new_api_ids = {'imdb': None, 'tmdb': None, 'tvmaze': None, 'tvdb': None}
        tvdb_data = {}
        tvmaze_data = {}
        tmdb_data = {}
        imdb_data = {}

        # Use existing API IDs or search for new ones
        final_api_ids = {}
        for key in new_api_ids:
            final_api_ids[key] = existing_api_ids[key] or new_api_ids[key]

        # Search for missing API IDs only
        if series:
            if not final_api_ids['tvmaze']:
                tvmaze_result = search_tvmaze(series)
                if tvmaze_result:
                    final_api_ids['tvmaze'] = tvmaze_result.get('id')
                    tvmaze_data = tvmaze_result

            if not final_api_ids['tmdb'] and api_keys.get('TMDB'):
                tmdb_result = search_tmdb(series, api_keys['TMDB'], is_tv=True)
                if tmdb_result:
                    final_api_ids['tmdb'] = tmdb_result.get('id')
                    tmdb_data = tmdb_result

            if not final_api_ids['tvdb'] and api_keys.get('theTVDB'):
                tvdb_result = search_tvdb(series, api_keys['theTVDB'])
                if tvdb_result:
                    final_api_ids['tvdb'] = tvdb_result.get('id')

        elif movie:
            if not final_api_ids['tmdb'] and api_keys.get('TMDB'):
                tmdb_result = search_tmdb(movie, api_keys['TMDB'], is_tv=False)
                if tmdb_result:
                    final_api_ids['tmdb'] = tmdb_result.get('id')
                    tmdb_data = tmdb_result

        # Get metadata from APIs using existing or new IDs
        if final_api_ids['tvmaze']:
            if not tvmaze_data:
                tvmaze_data = call_api(f"https://api.tvmaze.com/shows/{final_api_ids['tvmaze']}")

        if final_api_ids['tmdb'] and api_keys.get('TMDB'):
            if not tmdb_data:
                endpoint = "tv" if series else "movie"
                tmdb_data = call_api(f"https://api.themoviedb.org/3/{endpoint}/{final_api_ids['tmdb']}?api_key={api_keys['TMDB']}")

        if final_api_ids['imdb'] and api_keys.get('OMDB'):
            imdb_data = call_api(f"http://www.omdbapi.com/?apikey={api_keys['OMDB']}&i={final_api_ids['imdb']}")

        # Extract metadata using field priorities
        updates = {}

        # Check current online table values to only fill empty columns
        db_path = Path(__file__).parent.parent / "tapedeck.db"
        try:
            conn = sqlite3.connect(str(db_path))
            cursor = conn.cursor()

            all_fields = ['dmovie', 'release', 'studio', 'dseries', 'dseason', 'depisode',
                         'airdate', 'network', 'genre', 'rating', 'cast', 'imovie', 'iseries']
            cursor.execute("PRAGMA table_info(online)")
            existing_cols = {row[1] for row in cursor.fetchall()}
            available_fields = [f for f in all_fields if f in existing_cols]

            if available_fields:
                # Escape reserved words like 'cast'
                escaped_fields = [f'"{field}"' for field in available_fields]
                query = f"SELECT {', '.join(escaped_fields)} FROM online WHERE checksum = ?"
                cursor.execute(query, (checksum,))
                current = cursor.fetchone()

                if current:
                    for i, field in enumerate(available_fields):
                        if current[i] is None or current[i] == '':
                            value = extract_field(field, tvdb_data, tvmaze_data, imdb_data, tmdb_data)
                            if value:
                                updates[field] = value
            conn.close()
        except Exception as e:
            pass

        # Update tables
        if updates or any(new_api_ids.values()):
            updated_count = update_online_table(checksum, updates, final_api_ids)
            update_import_table(checksum, final_api_ids)
            total_updated += updated_count

            if args.verbose:
                title = movie or series or checksum
                print(f"Updated {updated_count} fields for {title}")

    print(f"Updated {total_updated} total fields")

if __name__ == "__main__":
    main()