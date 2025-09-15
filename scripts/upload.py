#!/usr/bin/env python3

import argparse
import json
import os
import requests
import shutil
import sqlite3
import subprocess
import sys
from pathlib import Path

def get_config():
    config_path = Path(__file__).parent.parent / "user.json"
    with open(config_path, 'r') as f:
        return json.load(f)

def get_torrent_sites():
    sites_path = Path(__file__).parent.parent / "preferences" / "torrentsites.json"
    with open(sites_path, 'r') as f:
        return json.load(f)

def get_uploads():
    db_path = Path(__file__).parent.parent / "tapedeck.db"
    conn = sqlite3.connect(str(db_path))
    cursor = conn.cursor()

    cursor.execute("PRAGMA table_info(import)")
    import_cols = [row[1] for row in cursor.fetchall()]

    if 'uploaded' not in import_cols:
        cursor.execute("ALTER TABLE import ADD COLUMN uploaded INTEGER")
        conn.commit()

    cursor.execute("PRAGMA table_info(online)")
    online_cols = [row[1] for row in cursor.fetchall()]

    cursor.execute("SELECT * FROM import WHERE newloc IS NOT NULL AND newloc != '' AND (uploaded IS NULL OR uploaded = 0)")
    records = cursor.fetchall()

    folders_processed = set()
    uploads = []

    is_movie = 'movie' in import_cols
    content_type = 'movies' if is_movie else 'tv_shows'

    for record in records:
        data = dict(zip(import_cols, record))
        folder_path = Path(data['newloc']).parent

        if folder_path in folders_processed:
            continue

        folders_processed.add(folder_path)

        cursor.execute("SELECT * FROM online WHERE checksum = ?", (data['checksum'],))
        online_row = cursor.fetchone()
        online_data = dict(zip(online_cols, online_row)) if online_row else {}

        uploads.append({
            'folder': folder_path,
            'folder_name': folder_path.name,
            'content_type': content_type,
            'torrenttype': data.get('torrenttype', 'season'),
            'torrentsite': data.get('torrentsite', 'torrentleech'),
            'imdb': online_data.get('imdb', ''),
            'tvmaze': online_data.get('tvmaze', ''),
            'checksums': [data['checksum']],
            'is_movie': is_movie
        })

    for upload in uploads[:]:
        for record in records:
            data = dict(zip(import_cols, record))
            if Path(data['newloc']).parent == upload['folder'] and data['checksum'] not in upload['checksums']:
                upload['checksums'].append(data['checksum'])

    conn.close()

    uploads.sort(key=lambda x: x['folder_name'])

    return uploads

def create_torrent(folder_path, announce_url, output_path):
    cmd = ['mktorrent', '-a', announce_url, '-o', str(output_path), str(folder_path)]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            print(f"mktorrent error: {result.stderr}")
        return result.returncode == 0
    except FileNotFoundError:
        print("mktorrent not found - install mktorrent")
        return False

def mark_uploaded(checksums):
    db_path = Path(__file__).parent.parent / "tapedeck.db"
    conn = sqlite3.connect(str(db_path))
    cursor = conn.cursor()
    for checksum in checksums:
        cursor.execute("UPDATE import SET uploaded = 1 WHERE checksum = ?", (checksum,))
    conn.commit()
    conn.close()

def get_category_from_config(torrent_type, is_movie, site_config):
    category_mapping = site_config.get('category_mapping', {})
    categories = site_config.get('categories', {})

    if is_movie:
        category_key = category_mapping.get('movie', 'movies_webRip')
    else:
        category_key = category_mapping.get(torrent_type.lower(), 'tv_boxsets')

    return categories.get(category_key, 37)

def upload_torrent(torrent_path, nfo_path, config, site_config, upload_data, verbose=False):
    announce_key = config['torrent_sites']['torrentleech']['announcekey']

    data = {
        'announcekey': announce_key,
        'category': get_category_from_config(upload_data['torrenttype'], upload_data['is_movie'], site_config)
    }

    if upload_data.get('imdb'):
        data['imdb'] = upload_data['imdb']

    if upload_data.get('tvmaze') and not upload_data['is_movie']:
        data['tvmaze'] = upload_data['tvmaze']
        data['tvmazetype'] = '2' if upload_data['torrenttype'] == 'episode' else '1'

    files = {'torrent': open(torrent_path, 'rb')}
    if nfo_path and nfo_path.exists():
        files['nfo'] = open(nfo_path, 'rb')

    try:
        response = requests.post(site_config['upload_url'], files=files, data=data)
        if verbose:
            print(f"Upload response: {response.status_code}")
            if response.status_code != 200:
                print(f"Response: {response.text}")
        return response.status_code == 200
    except Exception as e:
        print(f"Upload error: {e}")
        return False
    finally:
        for f in files.values():
            f.close()

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-v", "--verbose", action="store_true")
    parser.add_argument("-t", "--test", action="store_true")
    args = parser.parse_args()

    config = get_config()
    torrent_sites = get_torrent_sites()
    uploads = get_uploads()

    if not uploads:
        print("No uploads found")
        return

    if args.verbose:
        print(f"Found {len(uploads)} folders to process")

    for upload in uploads:
        folder_path = upload['folder']
        content_type = upload['content_type']
        site_name = upload['torrentsite']

        if site_name not in torrent_sites:
            print(f"Unknown torrent site: {site_name}")
            continue

        site_config = torrent_sites[site_name]
        announce_key = config['torrent_sites'][site_name]['announcekey']
        announce_url = site_config['announce_url'].format(announcekey=announce_key)

        temp_dir = Path(config['locations']['temp_torrent_upload'][content_type])
        monitored_dir = Path(config['locations']['monitored_upload'][content_type])

        try:
            temp_dir.mkdir(parents=True, exist_ok=True)
        except (OSError, IOError) as e:
            print(f"Error creating temp directory {temp_dir}: {e}")
            continue

        try:
            monitored_dir.mkdir(parents=True, exist_ok=True)
        except (OSError, IOError) as e:
            if args.verbose:
                print(f"Warning: Could not create monitored directory {monitored_dir}: {e}")

        torrent_name = f"{folder_path.name}.torrent"
        temp_torrent = temp_dir / torrent_name
        final_torrent = monitored_dir / torrent_name

        nfo_path = folder_path / f"{folder_path.name}.nfo"

        if args.verbose:
            print(f"Processing {folder_path.name}")

        if create_torrent(folder_path, announce_url, temp_torrent):
            if args.test:
                if args.verbose:
                    print(f"TEST MODE: Created torrent {torrent_name}")
                    print(f"TEST MODE: Would upload to {site_name}")
                    print(f"TEST MODE: Would move to monitored folder")
                print(f"TEST MODE: Torrent ready for upload: {torrent_name}")
            else:
                if upload_torrent(temp_torrent, nfo_path, config, site_config, upload, args.verbose):
                    shutil.move(str(temp_torrent), str(final_torrent))
                    mark_uploaded(upload['checksums'])
                    if args.verbose:
                        print(f"Successfully uploaded: {torrent_name}")
                else:
                    print(f"Failed to upload: {torrent_name}")
                    if temp_torrent.exists():
                        temp_torrent.unlink()
        else:
            print(f"Failed to create torrent: {folder_path.name}")

    if args.verbose:
        print("Upload process complete")

if __name__ == "__main__":
    main()