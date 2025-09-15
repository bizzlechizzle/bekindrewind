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
    """Extract online file source from path per import.md: the folder the files were found in if applicable (Amazon, Youtube, HBO, etc)."""
    parts = Path(file_path).parts
    streaming_sources = ['amazon', 'youtube', 'hbo', 'max', 'netflix', 'hulu', 'disney', 'paramount', 'peacock', 'apple']

    for part in parts:
        part_lower = part.lower()
        for source in streaming_sources:
            if source in part_lower:
                return part
    return "unknown"




def scan_videos(directory):
    """Find video files."""
    video_exts = {'.mkv', '.mp4', '.avi', '.mov', '.wmv', '.flv', '.webm', '.m4v'}
    return [p for p in Path(directory).rglob('*')
            if p.is_file() and p.suffix.lower() in video_exts]


def extract_media_info(guess):
    """KISS: Extract media info from guessit result."""
    if guess.get('type') == 'movie':
        return {
            'movie': guess.get('title', 'Unknown'),
            'stitle': guess.get('edition'),
            'is_movie': True
        }
    else:
        return {
            'series': guess.get('title', 'Unknown'),
            'season': guess.get('season'),
            'episode': guess.get('episode'),
            'title': guess.get('episode_title'),
            'stitle': guess.get('edition'),
            'is_movie': False
        }

def process_single_file(file_path, torrent_site, torrent_type, verbose):
    """KISS: Process single video file."""
    if verbose:
        print(f"Processing: {file_path.name}")

    checksum = get_checksum(file_path)
    guess = guessit(str(file_path.name))
    dlsource = extract_filesource(file_path)

    entry = {
        'checksum': checksum,
        'filename': file_path.name,
        'fileloc': str(file_path),
        'dlsource': dlsource,
        'torrentsite': torrent_site,
        'torrenttype': torrent_type
    }

    # Add media-specific info
    entry.update(extract_media_info(guess))
    return entry

def process_files(files, torrent_site, torrent_type, verbose):
    """Process video files and extract data per import.md instructions."""
    data = []

    for file_path in files:
        try:
            entry = process_single_file(file_path, torrent_site, torrent_type, verbose)
            data.append(entry)
        except Exception as e:
            print(f"Error processing {file_path.name}: {e}")
            continue

    return data


def create_database(verbose):
    """Run database.py to create unified schema database."""
    script_dir = Path(__file__).parent
    cmd = [sys.executable, str(script_dir / "database.py"), "-tv"]  # -tv creates unified schema

    if verbose:
        cmd.append("-v")

    subprocess.run(cmd, check=True)


def insert_data(data, verbose):
    """Insert data into import table and copy checksums to online table."""
    script_dir = Path(__file__).parent
    db_path = script_dir.parent / "tapedeck.db"

    conn = sqlite3.connect(str(db_path))
    cursor = conn.cursor()

    for entry in data:
        # KISS: Simplified insert with unified schema - all columns always exist
        if entry['is_movie']:
            cursor.execute("""INSERT OR REPLACE INTO import
                             (checksum, movie, filename, fileloc, torrentsite, torrenttype, dlsource, stitle)
                             VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                          (entry['checksum'], entry.get('movie'), entry['filename'],
                           entry['fileloc'], entry['torrentsite'], entry['torrenttype'],
                           entry.get('dlsource'), entry.get('stitle')))
        else:
            cursor.execute("""INSERT OR REPLACE INTO import
                             (checksum, series, season, episode, title, filename, fileloc,
                              torrentsite, torrenttype, dlsource, stitle)
                             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                          (entry['checksum'], entry.get('series'), entry.get('season'),
                           entry.get('episode'), entry.get('title'), entry['filename'],
                           entry['fileloc'], entry['torrentsite'], entry['torrenttype'],
                           entry.get('dlsource'), entry.get('stitle')))

        # Copy checksum to online table per instructions - ONLY CHECKSUM
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

    # KISS: Simplified torrent type mapping
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

    # Create unified schema database (KISS: no conditional logic needed)
    try:
        create_database(args.verbose)
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