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
    with sqlite3.connect(str(db_path)) as conn:
        cursor = conn.cursor()

        cursor.execute("PRAGMA table_info(import)")
        cols = {row[1] for row in cursor.fetchall()}

        select_fields = ["checksum"]
        if 'movie' in cols: select_fields.append("movie")
        if 'series' in cols: select_fields.extend(["series", "season", "episode"])
        if 'dlsource' in cols: select_fields.append("dlsource")

        if len(select_fields) == 1:
            return [], cols

        cursor.execute(f"SELECT {', '.join(select_fields)} FROM import")
        data = cursor.fetchall()
    return data, cols

def call_api(url):
    try:
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            return response.json()
    except:
        pass
    return None

def smart_pick(results, target):
    if not results or len(results) <= 1:
        return results[0] if results else None

    clean_target = re.sub(r'[^\w\s]', '', target.lower())
    best = results[0]
    best_score = 0

    for r in results:
        score = 0
        name = r.get('name') or r.get('title') or r.get('original_name') or r.get('original_title') or ''
        clean_name = re.sub(r'[^\w\s]', '', name.lower())

        if clean_name == clean_target:
            score += 100
        elif clean_target in clean_name:
            score += 50
        elif any(word in clean_name for word in clean_target.split()):
            score += 20

        if r.get('vote_count', 0) > best.get('vote_count', 0):
            score += 10
        if r.get('popularity', 0) > best.get('popularity', 0):
            score += 5

        if score > best_score:
            best_score = score
            best = r

    return best

def search_tvmaze(series_name):
    if not series_name:
        return None
    clean_name = re.sub(r'[^\w\s]', '', series_name).strip()
    url = f"https://api.tvmaze.com/search/shows?q={clean_name}"
    result = call_api(url)
    if result and len(result) > 0:
        shows = [r.get('show') for r in result if r.get('show')]
        return smart_pick(shows, series_name)
    return None

def search_tmdb(title, api_key, is_tv=True):
    if not api_key or not title:
        return None
    clean_title = re.sub(r'[^\w\s]', '', title).strip()
    endpoint = "tv" if is_tv else "movie"
    url = f"https://api.themoviedb.org/3/search/{endpoint}?api_key={api_key}&query={clean_title}"
    result = call_api(url)
    if result and result.get('results'):
        return smart_pick(result['results'], title)
    return None

def search_tvdb(series_name, api_key):
    if not api_key or not series_name:
        return None

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

def best_choice(options):
    if not options:
        return None
    if len(options) == 1:
        return list(options.values())[0]

    scored = []
    for source, value in options.items():
        if not value:
            continue
        score = len(str(value))
        if value.count(',') > 0:
            score += value.count(',') * 10
        if 'http' in value:
            score += 20
        scored.append((score, value))

    return max(scored, key=lambda x: x[0])[1] if scored else None

def is_origin_source_image(image_url, source):
    if not image_url or not source:
        return False

    source_domains = {
        'amazon': ['amazon.com', 'primevideo.com', 'images-amazon.com'],
        'hbo': ['hbo.com', 'hbomax.com', 'max.com'],
        'max': ['hbo.com', 'hbomax.com', 'max.com'],
        'hbo max': ['hbo.com', 'hbomax.com', 'max.com'],
        'youtube': ['youtube.com', 'ytimg.com']
    }

    domains = source_domains.get(source.lower(), [])
    return any(domain in image_url.lower() for domain in domains)

def extract_field(field, tvdb_data, tvmaze_data, imdb_data, tmdb_data, existing_value=None, source=None):
    if field == 'dmovie':
        return best_choice({
            'tmdb': tmdb_data.get('overview'),
            'imdb': imdb_data.get('Plot') if imdb_data.get('Plot') != 'N/A' else None
        })

    elif field == 'dseries':
        return best_choice({
            'tvdb': tvdb_data.get('overview'),
            'tvmaze': re.sub(r'<[^>]+>', '', tvmaze_data.get('summary', '')).strip() or None,
            'imdb': imdb_data.get('Plot') if imdb_data.get('Plot') != 'N/A' else None,
            'tmdb': tmdb_data.get('overview')
        })

    elif field in ['dseason', 'depisode']:
        return best_choice({
            'tvdb': tvdb_data.get('overview'),
            'tvmaze': re.sub(r'<[^>]+>', '', tvmaze_data.get('summary', '')).strip() or None
        })

    elif field == 'airdate':
        return best_choice({
            'tvdb': tvdb_data.get('firstAired'),
            'tvmaze': tvmaze_data.get('premiered'),
            'tmdb': tmdb_data.get('first_air_date')
        })

    elif field == 'network':
        return best_choice({
            'tvdb': tvdb_data.get('primaryNetwork', {}).get('name'),
            'tvmaze': tvmaze_data.get('network', {}).get('name') or tvmaze_data.get('webChannel', {}).get('name'),
            'tmdb': tmdb_data.get('networks', [{}])[0].get('name') if tmdb_data.get('networks') else None
        })

    elif field == 'genre':
        all_genres = []
        if tvdb_data.get('genres'):
            all_genres.extend([g['name'] for g in tvdb_data['genres']])
        if tvmaze_data.get('genres'):
            all_genres.extend(tvmaze_data['genres'])
        if imdb_data.get('Genre') and imdb_data['Genre'] != 'N/A':
            all_genres.extend([g.strip() for g in imdb_data['Genre'].split(',')])
        if tmdb_data.get('genres'):
            all_genres.extend([g['name'] for g in tmdb_data['genres']])

        if all_genres:
            unique = []
            seen = set()
            for g in all_genres:
                g_clean = g.strip().lower()
                if g_clean not in seen and g.strip():
                    unique.append(g.strip())
                    seen.add(g_clean)
            return ', '.join(unique[:5])

    elif field == 'rating':
        return best_choice({
            'tvdb': tvdb_data.get('rating'),
            'imdb': imdb_data.get('Rated') if imdb_data.get('Rated') != 'N/A' else None
        })

    elif field == 'cast':
        actors = imdb_data.get('Actors')
        if actors and actors != 'N/A':
            return ', '.join(actors.split(', ')[:5])

    elif field == 'release':
        return best_choice({
            'tmdb': tmdb_data.get('release_date'),
            'imdb': imdb_data.get('Released') if imdb_data.get('Released') != 'N/A' else None
        })

    elif field == 'studio':
        companies = tmdb_data.get('production_companies')
        return companies[0]['name'] if companies else None

    elif field == 'imovie':
        if existing_value and source and is_origin_source_image(existing_value, source):
            return existing_value
        path = tmdb_data.get('poster_path')
        return f"https://image.tmdb.org/t/p/w500{path}" if path else None

    elif field in ['iseries', 'iseason', 'iepisode']:
        if existing_value and source and is_origin_source_image(existing_value, source):
            return existing_value
        return best_choice({
            'tvdb': tvdb_data.get('image'),
            'tvmaze': tvmaze_data.get('image', {}).get('original'),
            'tmdb': f"https://image.tmdb.org/t/p/w500{tmdb_data.get('poster_path' if field != 'iepisode' else 'still_path')}" if tmdb_data.get('poster_path' if field != 'iepisode' else 'still_path') else None
        })

    return None

def get_api_ids(checksum):
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
    if not updates and not any(api_ids.values()):
        return 0

    db_path = Path(__file__).parent.parent / "tapedeck.db"
    with sqlite3.connect(str(db_path)) as conn:
        cursor = conn.cursor()

        all_updates = {}
        all_updates.update(updates)
        all_updates.update({k: v for k, v in api_ids.items() if v is not None})

        if all_updates:
            fields = ', '.join(f"{k} = ?" for k in all_updates.keys())
            values = list(all_updates.values()) + [checksum]
            cursor.execute(f"UPDATE online SET {fields} WHERE checksum = ?", values)
    return len(updates)

def update_import_table(checksum, api_ids):
    if not any(api_ids.values()):
        return

    db_path = Path(__file__).parent.parent / "tapedeck.db"
    with sqlite3.connect(str(db_path)) as conn:
        cursor = conn.cursor()

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

def main():
    parser = argparse.ArgumentParser()
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

        movie = None
        series = None
        source = None

        if 'movie' in import_cols and len(record) > 1:
            movie = record[1]
        elif 'series' in import_cols and len(record) > 1:
            series = record[1]

        if 'dlsource' in import_cols:
            source_idx = 1 + ('movie' in import_cols) + ('series' in import_cols) * 3
            if len(record) > source_idx:
                source = record[source_idx]

        existing_api_ids = get_api_ids(checksum)

        new_api_ids = {'imdb': None, 'tmdb': None, 'tvmaze': None, 'tvdb': None}
        tvdb_data = {}
        tvmaze_data = {}
        tmdb_data = {}
        imdb_data = {}

        final_api_ids = {}
        for key in new_api_ids:
            final_api_ids[key] = existing_api_ids[key] or new_api_ids[key]

        if series:
            if not final_api_ids['tvmaze']:
                tvmaze_result = search_tvmaze(series)
                if tvmaze_result:
                    final_api_ids['tvmaze'] = tvmaze_result.get('id')
                    tvmaze_data = tvmaze_result
                    if tvmaze_result.get('externals', {}).get('imdb') and not final_api_ids['imdb']:
                        final_api_ids['imdb'] = tvmaze_result['externals']['imdb']

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
                    if tmdb_result.get('imdb_id') and not final_api_ids['imdb']:
                        final_api_ids['imdb'] = tmdb_result['imdb_id']

        if final_api_ids['tvmaze']:
            if not tvmaze_data:
                tvmaze_data = call_api(f"https://api.tvmaze.com/shows/{final_api_ids['tvmaze']}")
                if tvmaze_data and tvmaze_data.get('externals', {}).get('imdb') and not final_api_ids['imdb']:
                    final_api_ids['imdb'] = tvmaze_data['externals']['imdb']

        if final_api_ids['tmdb'] and api_keys.get('TMDB'):
            if not tmdb_data:
                endpoint = "tv" if series else "movie"
                tmdb_data = call_api(f"https://api.themoviedb.org/3/{endpoint}/{final_api_ids['tmdb']}?api_key={api_keys['TMDB']}")

            if not final_api_ids['imdb'] and final_api_ids['tmdb']:
                endpoint = "tv" if series else "movie"
                external_ids = call_api(f"https://api.themoviedb.org/3/{endpoint}/{final_api_ids['tmdb']}/external_ids?api_key={api_keys['TMDB']}")
                if external_ids and external_ids.get('imdb_id'):
                    final_api_ids['imdb'] = external_ids['imdb_id']

        if final_api_ids['imdb'] and api_keys.get('OMDB'):
            imdb_data = call_api(f"http://www.omdbapi.com/?apikey={api_keys['OMDB']}&i={final_api_ids['imdb']}")

        updates = {}

        db_path = Path(__file__).parent.parent / "tapedeck.db"
        try:
            conn = sqlite3.connect(str(db_path))
            cursor = conn.cursor()

            all_fields = ['dmovie', 'release', 'studio', 'dseries', 'dseason', 'depisode',
                         'airdate', 'network', 'genre', 'rating', 'cast', 'imovie', 'iseries', 'iseason', 'iepisode']
            cursor.execute("PRAGMA table_info(online)")
            existing_cols = {row[1] for row in cursor.fetchall()}
            available_fields = [f for f in all_fields if f in existing_cols]

            if available_fields:
                escaped_fields = [f'"{field}"' for field in available_fields]
                query = f"SELECT {', '.join(escaped_fields)} FROM online WHERE checksum = ?"
                cursor.execute(query, (checksum,))
                current = cursor.fetchone()

                if current:
                    for i, field in enumerate(available_fields):
                        existing_value = current[i] if current[i] not in [None, ''] else None
                        value = extract_field(field, tvdb_data, tvmaze_data, imdb_data, tmdb_data, existing_value, source)
                        if value and (current[i] is None or current[i] == '' or value != current[i]):
                            updates[field] = value
            conn.close()
        except Exception as e:
            pass

        new_api_ids_to_update = {}
        for key, value in final_api_ids.items():
            if value and value != existing_api_ids[key]:
                new_api_ids_to_update[key] = value

        if updates or new_api_ids_to_update:
            updated_count = update_online_table(checksum, updates, new_api_ids_to_update)
            update_import_table(checksum, new_api_ids_to_update)
            total_updated += updated_count

            if args.verbose:
                title = movie or series or checksum
                print(f"Updated {updated_count} fields for {title}")

    print(f"Updated {total_updated} total fields")

if __name__ == "__main__":
    main()