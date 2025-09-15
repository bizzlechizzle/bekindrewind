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
    cols = [row[1] for row in cursor.fetchall()]

    # Check if uploaded column exists, if not add it
    cursor.execute("PRAGMA table_info(import)")
    columns = [row[1] for row in cursor.fetchall()]
    if 'uploaded' not in columns:
        cursor.execute("ALTER TABLE import ADD COLUMN uploaded INTEGER")
        conn.commit()

    cursor.execute("SELECT * FROM import WHERE newloc IS NOT NULL AND newloc != '' AND (uploaded IS NULL OR uploaded = 0)")
    records = cursor.fetchall()
    conn.close()

    folders_processed = set()
    uploads = []

    for record in records:
        data = dict(zip(cols, record))
        folder_path = Path(data['newloc']).parent

        if folder_path in folders_processed or not folder_path.exists():
            continue

        folders_processed.add(folder_path)

        is_movie = 'movie' in cols and data.get('movie')
        content_type = 'movies' if is_movie else 'tv_shows'

        uploads.append({
            'folder': folder_path,
            'content_type': content_type,
            'torrenttype': data.get('torrenttype', 'season'),
            'torrentsite': data.get('torrentsite', 'torrentleech'),
            'imdb': data.get('imdb', ''),
            'tvmaze': data.get('tvmaze', ''),
            'data': data
        })

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

def mark_uploaded(data):
    db_path = Path(__file__).parent.parent / "tapedeck.db"
    conn = sqlite3.connect(str(db_path))
    cursor = conn.cursor()
    cursor.execute("UPDATE import SET uploaded = 1 WHERE checksum = ?", (data['checksum'],))
    conn.commit()
    conn.close()

def get_category(torrent_type, is_hd=True):
    if 'movie' in torrent_type.lower():
        return 37  # Movies :: WEBRip
    elif torrent_type.lower() == 'episode':
        return 32 if is_hd else 26  # TV :: Episodes HD/SD
    else:
        return 27  # TV :: BoxSets

def upload_torrent(torrent_path, nfo_path, config, site_config, upload_data, verbose=False):
    announce_key = config['torrent_sites']['torrentleech']['announcekey']

    data = {
        'announcekey': announce_key,
        'category': get_category(upload_data.get('torrenttype', 'season'))
    }

    if upload_data.get('imdb'):
        data['imdb'] = upload_data['imdb']

    if upload_data.get('tvmaze'):
        data['tvmaze'] = upload_data['tvmaze']
        data['tvmazetype'] = '1'

    files = {'torrent': open(torrent_path, 'rb')}
    if nfo_path and nfo_path.exists():
        files['nfo'] = open(nfo_path, 'rb')

    try:
        response = requests.post(site_config['upload_url'], files=files, data=data)
        if verbose:
            print(f"Upload response: {response.status_code}")
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
        print(f"Found {len(uploads)} uploads to process")

    uploads.sort(key=lambda x: x['folder'].name)

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

        temp_dir.mkdir(parents=True, exist_ok=True)
        monitored_dir.mkdir(parents=True, exist_ok=True)

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
                    mark_uploaded(upload['data'])
                    if args.verbose:
                        print(f"Successfully uploaded: {torrent_name}")
                else:
                    print(f"Failed to upload: {torrent_name}")
                    if temp_torrent.exists():
                        temp_torrent.unlink()
        else:
            print(f"Failed to create torrent: {folder_path.name}")

if __name__ == "__main__":
    main()