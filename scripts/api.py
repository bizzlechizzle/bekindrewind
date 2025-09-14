#!/usr/bin/env python3
"""
API BOI - KISS data extraction from APIs
Data priority: TVMaze > TMDb > TVDB > OMDb

Usage: python api.py [-v]
"""

import argparse
import json
import re
import requests
import sqlite3
import sys
from pathlib import Path


def get_script_dir():
    """Get directory where script is located."""
    return Path(__file__).parent


def load_api_keys():
    """Load API keys from user.json."""
    config_path = get_script_dir().parent / "user.json"
    try:
        with open(config_path, 'r') as f:
            config = json.load(f)
        return config.get("API_KEYS", {})
    except Exception:
        return {}


def get_records():
    """Get records from api table."""
    db_path = get_script_dir().parent / "tapedeck.db"
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT checksum, movie, series, season, episode FROM api")
        records = cursor.fetchall()
        conn.close()
        return records
    except Exception:
        return []


def get_online_ids(checksum):
    """Get API IDs from online table."""
    db_path = get_script_dir().parent / "tapedeck.db"
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT imdb, tmdb, tvmaze, tvdb FROM online WHERE checksum = ?", (checksum,))
        result = cursor.fetchone()
        conn.close()
        if result:
            return {'imdb': result[0], 'tmdb': result[1], 'tvmaze': result[2], 'tvdb': result[3]}
    except Exception:
        pass
    return {}


def call_api(url):
    """Simple API call."""
    try:
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            return response.json()
    except Exception:
        pass
    return None


def search_tvmaze(series_name):
    """Search TVMaze API."""
    if not series_name:
        return None
    clean_name = re.sub(r'[^\w\s]', '', series_name).strip()
    url = f"https://api.tvmaze.com/search/shows?q={clean_name}"
    result = call_api(url)
    if result and len(result) > 0:
        return result[0].get('show')
    return None


def search_tmdb(series_name, movie_name, api_key):
    """Search TMDb API."""
    if not api_key:
        return None

    if series_name:
        clean_name = re.sub(r'[^\w\s]', '', series_name).strip()
        url = f"https://api.themoviedb.org/3/search/tv?api_key={api_key}&query={clean_name}"
    elif movie_name:
        clean_name = re.sub(r'[^\w\s]', '', movie_name).strip()
        url = f"https://api.themoviedb.org/3/search/movie?api_key={api_key}&query={clean_name}"
    else:
        return None

    result = call_api(url)
    if result and result.get('results'):
        return result['results'][0]
    return None


def search_tvdb(series_name, api_key):
    """Search TVDB API."""
    if not api_key or not series_name:
        return None

    # Get token first
    token_url = "https://api4.thetvdb.com/v4/login"
    token_data = {"apikey": api_key}
    try:
        response = requests.post(token_url, json=token_data, timeout=10)
        if response.status_code == 200:
            token = response.json().get('data', {}).get('token')
            if token:
                clean_name = re.sub(r'[^\w\s]', '', series_name).strip()
                search_url = f"https://api4.thetvdb.com/v4/search?query={clean_name}"
                headers = {"Authorization": f"Bearer {token}"}
                search_response = requests.get(search_url, headers=headers, timeout=10)
                if search_response.status_code == 200:
                    search_result = search_response.json()
                    if search_result.get('data'):
                        return search_result['data'][0]
    except Exception:
        pass
    return None


def search_omdb(imdb_id, api_key):
    """Search OMDb API by IMDB ID."""
    if not api_key or not imdb_id:
        return None
    url = f"http://www.omdbapi.com/?apikey={api_key}&i={imdb_id}"
    return call_api(url)


def extract_data(api_data, source):
    """Extract data from API response."""
    data = {}

    if source == "tvmaze" and api_data:
        if api_data.get('premiered'):
            data['year'] = api_data['premiered'][:4]
        if api_data.get('summary'):
            data['dseries'] = re.sub(r'<[^>]+>', '', api_data['summary']).strip()
        if api_data.get('network', {}).get('name'):
            data['network'] = api_data['network']['name']
        elif api_data.get('webChannel', {}).get('name'):
            data['network'] = api_data['webChannel']['name']
        if api_data.get('genres'):
            data['genre'] = ', '.join(api_data['genres'])

    elif source == "tmdb" and api_data:
        if api_data.get('first_air_date'):
            data['year'] = api_data['first_air_date'][:4]
            data['airdate'] = api_data['first_air_date']
        elif api_data.get('release_date'):
            data['year'] = api_data['release_date'][:4]
            data['release'] = api_data['release_date']
        if api_data.get('overview'):
            if api_data.get('first_air_date'):
                data['dseries'] = api_data['overview']
            else:
                data['dmovie'] = api_data['overview']
        if api_data.get('networks') and len(api_data['networks']) > 0:
            data['network'] = api_data['networks'][0]['name']
        if api_data.get('genres'):
            data['genre'] = ', '.join([g['name'] for g in api_data['genres']])
        if api_data.get('poster_path'):
            if api_data.get('first_air_date'):
                data['iseries'] = f"https://image.tmdb.org/t/p/w500{api_data['poster_path']}"
            else:
                data['imovie'] = f"https://image.tmdb.org/t/p/w500{api_data['poster_path']}"

    elif source == "omdb" and api_data:
        if api_data.get('Rated') and api_data['Rated'] != 'N/A':
            data['rating'] = api_data['Rated']
        if api_data.get('Actors'):
            data['cast'] = api_data['Actors']
        if api_data.get('Genre'):
            data['genre'] = api_data['Genre']

    return data


def update_record(checksum, data):
    """Update api table with extracted data."""
    if not data:
        return False

    db_path = get_script_dir().parent / "tapedeck.db"
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # Build update query
        fields = [f"{k} = ?" for k in data.keys()]
        values = list(data.values()) + [checksum]

        cursor.execute(f"UPDATE api SET {', '.join(fields)} WHERE checksum = ?", values)
        success = cursor.rowcount > 0

        conn.commit()
        conn.close()
        return success
    except Exception:
        return False


def process_record(checksum, movie, series, season, episode, api_keys, verbose=False):
    """Process single record."""
    online_ids = get_online_ids(checksum)
    all_data = {}

    # TVMaze (Priority 1) - TV shows only
    if series and online_ids.get('tvmaze'):
        tvmaze_data = call_api(f"https://api.tvmaze.com/shows/{online_ids['tvmaze']}")
        data = extract_data(tvmaze_data, "tvmaze")
        all_data.update(data)

    # TMDb (Priority 2)
    if online_ids.get('tmdb'):
        if series:
            tmdb_data = call_api(f"https://api.themoviedb.org/3/tv/{online_ids['tmdb']}?api_key={api_keys.get('TMDB', '')}")
        elif movie:
            tmdb_data = call_api(f"https://api.themoviedb.org/3/movie/{online_ids['tmdb']}?api_key={api_keys.get('TMDB', '')}")
        else:
            tmdb_data = None

        data = extract_data(tmdb_data, "tmdb")
        # Only update if field doesn't exist (priority system)
        for k, v in data.items():
            if k not in all_data:
                all_data[k] = v

    # OMDb (Priority 4) - Only if we have IMDB ID
    if online_ids.get('imdb') and api_keys.get('OMDB'):
        omdb_data = search_omdb(online_ids['imdb'], api_keys['OMDB'])
        data = extract_data(omdb_data, "omdb")
        # Only update if field doesn't exist (priority system)
        for k, v in data.items():
            if k not in all_data:
                all_data[k] = v

    if all_data and update_record(checksum, all_data):
        if verbose:
            print(f"Updated: {movie or series} ({len(all_data)} fields)")
        return len(all_data)
    return 0


def main():
    """Main function."""
    parser = argparse.ArgumentParser(description="API BOI - KISS data extraction")
    parser.add_argument("-v", "--verbose", action="store_true", help="Verbose mode")
    args = parser.parse_args()

    api_keys = load_api_keys()
    if not api_keys:
        print("No API keys found")
        return

    records = get_records()
    if not records:
        print("No records found")
        return

    print(f"Processing {len(records)} records with MAXIMUM DATA EXTRACTION...")

    processed = 0
    failed = 0
    total_fields = 0

    for checksum, movie, series, season, episode in records:
        result = process_record(checksum, movie, series, season, episode, api_keys, args.verbose)
        if result:
            processed += 1
            total_fields += result  # process_record should return field count
        else:
            failed += 1

    print("MAXIMUM EXTRACTION COMPLETE:")
    print(f"  Processed: {processed}/{len(records)}")
    print(f"  Failed: {failed}")
    print(f"  Total Fields: {total_fields}")


if __name__ == "__main__":
    main()