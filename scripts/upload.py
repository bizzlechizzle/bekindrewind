#!/usr/bin/env python3

import argparse
import json
import requests
import shutil
import sqlite3
import subprocess
from pathlib import Path

def load_config():
    config_path = Path(__file__).parent.parent / "user.json"
    with open(config_path, 'r') as f:
        return json.load(f)

def load_torrent_sites():
    sites_path = Path(__file__).parent.parent / "preferences" / "torrentsites.json"
    with open(sites_path, 'r') as f:
        return json.load(f)

def get_db_connection():
    db_path = Path(__file__).parent.parent / "tapedeck.db"
    return sqlite3.connect(str(db_path))

def ensure_uploaded_column():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("PRAGMA table_info(import)")
    columns = [row[1] for row in cursor.fetchall()]
    if 'uploaded' not in columns:
        cursor.execute("ALTER TABLE import ADD COLUMN uploaded INTEGER")
        conn.commit()
    conn.close()

def get_upload_folders():
    config = load_config()
    conn = get_db_connection()
    cursor = conn.cursor()

    folders = []

    for content_type in ['tv_shows', 'movies']:
        upload_dir = Path(config['locations']['file_upload'][content_type])
        if not upload_dir.exists():
            continue

        for folder_path in upload_dir.iterdir():
            if not folder_path.is_dir():
                continue

            query = """
            SELECT i.checksum, i.torrenttype, i.torrentsite, o.imdb, o.tvmaze
            FROM import i
            LEFT JOIN online o ON i.checksum = o.checksum
            WHERE i.filename LIKE ? AND (i.uploaded IS NULL OR i.uploaded = 0)
            """

            show_name = folder_path.name.split('.')[0:2]
            if len(show_name) >= 2:
                search_name = f"%{show_name[0]}%{show_name[1]}%"
            else:
                search_name = f"%{folder_path.name.split('.')[0]}%"
            cursor.execute(query, (search_name,))
            rows = cursor.fetchall()

            if rows:
                first_row = rows[0]
                folders.append({
                    'path': folder_path,
                    'name': folder_path.name,
                    'torrenttype': first_row[1] or 'season',
                    'torrentsite': first_row[2] or 'torrentleech',
                    'imdb': first_row[3] or '',
                    'tvmaze': first_row[4] or '',
                    'checksums': [row[0] for row in rows]
                })

    conn.close()
    return sorted(folders, key=lambda x: x['name'])

def create_torrent(folder_path, announce_url, output_path):
    cmd = ['mktorrent', '-a', announce_url, '-o', str(output_path), str(folder_path)]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            print(f"mktorrent failed: {result.stderr}")
        return result.returncode == 0
    except FileNotFoundError:
        print("mktorrent not found - install mktorrent")
        return False

def get_category(torrenttype, is_movie):
    if is_movie:
        return 37
    return 26 if torrenttype == 'episode' else 27

def upload_torrent(torrent_path, folder_data, config, site_config, verbose):
    announce_key = config['torrent_sites'][folder_data['torrentsite']]['announcekey']
    is_movie = 'movie' in str(folder_data['path']).lower()

    data = {
        'announcekey': announce_key,
        'category': get_category(folder_data['torrenttype'], is_movie)
    }

    if folder_data['imdb']:
        data['imdb'] = folder_data['imdb']

    if folder_data['tvmaze'] and not is_movie:
        data['tvmaze'] = folder_data['tvmaze']
        data['tvmazetype'] = '2' if folder_data['torrenttype'] == 'episode' else '1'

    nfo_path = folder_data['path'] / f"{folder_data['name']}.nfo"
    files = {'torrent': open(torrent_path, 'rb')}
    if nfo_path.exists():
        files['nfo'] = open(nfo_path, 'rb')

    try:
        response = requests.post(site_config['upload_url'], files=files, data=data)
        if verbose:
            print(f"Upload response: {response.status_code}")
            if response.status_code != 200:
                print(f"Response: {response.text}")
        return response.status_code == 200
    except Exception as e:
        print(f"Upload failed: {e}")
        return False
    finally:
        for f in files.values():
            f.close()

def mark_uploaded(checksums):
    conn = get_db_connection()
    cursor = conn.cursor()
    for checksum in checksums:
        cursor.execute("UPDATE import SET uploaded = 1 WHERE checksum = ?", (checksum,))
    conn.commit()
    conn.close()

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-v", "--verbose", action="store_true")
    parser.add_argument("-t", "--test", action="store_true")
    args = parser.parse_args()

    ensure_uploaded_column()

    config = load_config()
    torrent_sites = load_torrent_sites()
    folders = get_upload_folders()

    if not folders:
        print("No uploads found")
        return

    if args.verbose:
        print(f"Found {len(folders)} folders to process")

    for folder_data in folders:
        site_name = folder_data['torrentsite']

        if site_name not in torrent_sites:
            print(f"Unknown torrent site: {site_name}")
            continue

        site_config = torrent_sites[site_name]
        announce_key = config['torrent_sites'][site_name]['announcekey']
        announce_url = site_config['announce_url'].format(announcekey=announce_key)

        is_movie = 'movie' in str(folder_data['path']).lower()
        content_type = 'movies' if is_movie else 'tv_shows'

        temp_dir = Path(config['locations']['temp_torrent_upload'][content_type])
        monitored_dir = Path(config['locations']['monitored_upload'][content_type])

        temp_dir.mkdir(parents=True, exist_ok=True)
        monitored_dir.mkdir(parents=True, exist_ok=True)

        torrent_name = f"{folder_data['name']}.torrent"
        temp_torrent = temp_dir / torrent_name
        final_torrent = monitored_dir / torrent_name

        if args.verbose:
            print(f"Processing {folder_data['name']}")

        if create_torrent(folder_data['path'], announce_url, temp_torrent):
            if args.test:
                print(f"TEST MODE: Torrent ready: {torrent_name}")
                if args.verbose:
                    print(f"TEST MODE: Would upload to {site_name}")
            else:
                if upload_torrent(temp_torrent, folder_data, config, site_config, args.verbose):
                    shutil.move(str(temp_torrent), str(final_torrent))
                    mark_uploaded(folder_data['checksums'])
                    if args.verbose:
                        print(f"Successfully uploaded: {torrent_name}")
                else:
                    print(f"Failed to upload: {torrent_name}")
                    temp_torrent.unlink(missing_ok=True)
        else:
            print(f"Failed to create torrent: {folder_data['name']}")

    if args.verbose:
        print("Upload process complete")

if __name__ == "__main__":
    main()