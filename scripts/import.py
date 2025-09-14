#!/usr/bin/env python3

import argparse
import hashlib
import json
import sqlite3
import subprocess
import sys
from pathlib import Path

try:
    from guessit import guessit
except ImportError:
    print("Error: guessit library not found. Install with: pip install guessit")
    sys.exit(1)


def get_checksum(file_path):
    """Generate 256 SHA checksum."""
    sha256_hash = hashlib.sha256()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            sha256_hash.update(chunk)
    return sha256_hash.hexdigest()


def extract_filesource(file_path):
    """Extract online file source from path."""
    parts = Path(file_path).parts
    for i, part in enumerate(parts):
        if part.lower() in ['videos', 'downloads'] and i + 1 < len(parts):
            return parts[i + 1]
    return "unknown"




def scan_videos(directory):
    """Find video files."""
    video_exts = {'.mkv', '.mp4', '.avi', '.mov', '.wmv', '.flv', '.webm', '.m4v'}
    return [p for p in Path(directory).rglob('*')
            if p.is_file() and p.suffix.lower() in video_exts]


def process_files(files, torrent_site, torrent_type, verbose):
    """Process video files and extract data per import.md instructions."""
    data = []

    for file_path in files:
        if verbose:
            print(f"Processing: {file_path.name}")

        try:
            checksum = get_checksum(file_path)
            guess = guessit(str(file_path.name))
            filesource = extract_filesource(file_path)

            entry = {
                'checksum': checksum,
                'filename': file_path.name,
                'fileloc': str(file_path),
                'filesource': filesource,
                'torrentsite': torrent_site,
                'torrenttype': torrent_type
            }

            # Set movie or TV data per instructions
            if guess.get('type') == 'movie':
                entry['movie'] = guess.get('title', 'Unknown')
                entry['stitle'] = guess.get('edition')
                entry['is_movie'] = True
            else:
                entry['series'] = guess.get('title', 'Unknown')
                entry['season'] = guess.get('season')
                entry['episode'] = guess.get('episode')
                entry['title'] = guess.get('episode_title')
                entry['stitle'] = guess.get('edition')
                entry['is_movie'] = False

            data.append(entry)

        except Exception as e:
            print(f"Error processing {file_path.name}: {e}")
            continue

    return data


def create_database(has_movies, has_tv, verbose):
    """Run database.py to create clean database."""
    script_dir = Path(__file__).parent
    cmd = [sys.executable, str(script_dir / "database.py")]

    if verbose:
        cmd.append("-v")
    if has_movies:
        cmd.append("-movie")
    if has_tv:
        cmd.append("-tv")

    subprocess.run(cmd, check=True)


def insert_data(data, verbose):
    """Insert data into import table and copy checksums to other tables."""
    script_dir = Path(__file__).parent
    db_path = script_dir.parent / "tapedeck.db"

    conn = sqlite3.connect(str(db_path))
    cursor = conn.cursor()

    # Check import table schema
    cursor.execute("PRAGMA table_info(import)")
    import_cols = {row[1] for row in cursor.fetchall()}

    for entry in data:
        # Build insert for import table per import.md instructions
        cols = ['checksum', 'filename', 'fileloc', 'torrentsite', 'torrenttype']
        vals = [entry['checksum'], entry['filename'], entry['fileloc'],
                entry['torrentsite'], entry['torrenttype']]

        # Add conditional columns per instructions
        if 'movie' in import_cols and entry['is_movie']:
            cols.append('movie')
            vals.append(entry['movie'])
        if 'series' in import_cols and not entry['is_movie']:
            cols.extend(['series', 'season', 'episode', 'title'])
            vals.extend([entry['series'], entry['season'], entry['episode'], entry['title']])
        if 'stitle' in import_cols and entry.get('stitle'):
            cols.append('stitle')
            vals.append(entry['stitle'])
        if 'dlsource' in import_cols:
            cols.append('dlsource')
            vals.append(entry['filesource'])

        # Filter out columns that don't exist in schema
        valid_cols = []
        valid_vals = []
        for col, val in zip(cols, vals):
            if col in import_cols:
                valid_cols.append(col)
                valid_vals.append(val)

        placeholders = ', '.join(['?'] * len(valid_vals))
        cursor.execute(f"INSERT OR REPLACE INTO import ({', '.join(valid_cols)}) VALUES ({placeholders})", valid_vals)

        # Copy checksum to other tables per instructions - ONLY CHECKSUM
        cursor.execute("INSERT OR REPLACE INTO online (checksum) VALUES (?)", (entry['checksum'],))

        if verbose:
            print(f"Imported: {entry['filename']}")

    conn.commit()
    conn.close()


def main():
    parser = argparse.ArgumentParser(description="Import media files")
    parser.add_argument("-v", "--verbose", action="store_true")
    parser.add_argument("-loc", help="File location")
    parser.add_argument("-site", help="Torrent site")
    parser.add_argument("-movie", action="store_true", help="Movie torrent type")
    parser.add_argument("-series", action="store_true", help="Series torrent type")
    parser.add_argument("-season", action="store_true", help="Season torrent type")
    parser.add_argument("-episode", action="store_true", help="Episode torrent type")

    args = parser.parse_args()

    # Load user.json config with error handling
    config_path = Path(__file__).parent.parent / "user.json"
    try:
        with open(config_path, 'r') as f:
            config = json.load(f)
    except FileNotFoundError:
        print(f"Error: {config_path} not found")
        sys.exit(1)
    except json.JSONDecodeError:
        print(f"Error: Invalid JSON in {config_path}")
        sys.exit(1)

    defaults = config.get('default', {})

    # Determine values from args or defaults
    location = args.loc or defaults.get('filelocation', '.')
    torrent_site = args.site or defaults.get('torrentsite', '')

    # Validate location exists
    if not Path(location).exists():
        print(f"Error: Location does not exist: {location}")
        sys.exit(1)

    # Determine torrent type per instructions
    if args.movie:
        torrent_type = "movie"
    elif args.series:
        torrent_type = "series"
    elif args.season:
        torrent_type = "season"
    elif args.episode:
        torrent_type = "episode"
    else:
        torrent_type = defaults.get('torrenttype', 'season')

    if args.verbose:
        print(f"Scanning: {location}")

    # Scan and process files
    video_files = scan_videos(location)
    if not video_files:
        print(f"No video files found in: {location}")
        return

    data = process_files(video_files, torrent_site, torrent_type, args.verbose)

    # Determine database type needed
    has_movies = any(entry['is_movie'] for entry in data)
    has_tv = any(not entry['is_movie'] for entry in data)

    # Create database with correct schema
    try:
        create_database(has_movies, has_tv, args.verbose)
    except subprocess.CalledProcessError as e:
        print(f"Error creating database: {e}")
        sys.exit(1)

    # Insert data
    try:
        insert_data(data, args.verbose)
    except Exception as e:
        print(f"Error inserting data: {e}")
        sys.exit(1)

    print(f"Successfully imported {len(data)} files")


if __name__ == "__main__":
    main()