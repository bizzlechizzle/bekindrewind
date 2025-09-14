#!/usr/bin/env python3
"""
KISS media file import script using guessit.
Bulletproof, works on any system, handles smart naming scenarios.

Usage:
    python import.py [-v] [-movie] [-tv] [-loc PATH] [-site SITE] [-series] [-season] [-episode]

Arguments:
    -v        : Verbose mode
    -movie    : Force movie database
    -tv       : Force TV database
    -loc PATH : Custom file location
    -site SITE: Custom torrent site
    -series   : Set torrent type to series
    -season   : Set torrent type to season
    -episode  : Set torrent type to episode
"""

import argparse
import hashlib
import json
import os
import sqlite3
import subprocess
import sys
from pathlib import Path

try:
    from guessit import guessit
except ImportError:
    print("Error: guessit library not found. Install with: pip install guessit")
    sys.exit(1)


def load_user_config():
    """Load user.json configuration."""
    script_dir = Path(__file__).parent
    config_path = script_dir.parent / "user.json"
    try:
        with open(config_path, 'r') as f:
            return json.load(f)
    except Exception as e:
        print(f"Error reading user.json: {e}")
        sys.exit(1)


def load_sources():
    """Load source mappings from preferences/sources.json."""
    script_dir = Path(__file__).parent
    sources_path = script_dir.parent / "preferences" / "sources.json"
    try:
        with open(sources_path, 'r') as f:
            return json.load(f)
    except Exception:
        return {}


def create_database(force_movie=False, force_tv=False, verbose=False):
    """Create database using database.py."""
    script_dir = Path(__file__).parent
    cmd = [sys.executable, str(script_dir / "database.py")]

    if verbose:
        cmd.append("-v")
    if force_movie:
        cmd.append("-movie")
    elif force_tv:
        cmd.append("-tv")

    try:
        subprocess.run(cmd, check=True)
        return True
    except subprocess.CalledProcessError:
        return False


def generate_checksum(file_path):
    """Generate SHA256 checksum for file."""
    sha256_hash = hashlib.sha256()
    try:
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(65536), b""):
                sha256_hash.update(chunk)
        return sha256_hash.hexdigest()
    except Exception as e:
        print(f"Error generating checksum for {file_path}: {e}")
        return None


def extract_source_from_path(file_path):
    """Extract source from streamfab path pattern."""
    parts = Path(file_path).parts
    # Find 'videos' folder and return next part as source
    for i, part in enumerate(parts):
        if part.lower() == 'videos' and i + 1 < len(parts):
            return parts[i + 1]
    return "unknown"


def is_video_file(file_path):
    """Check if file is a video file."""
    video_extensions = {'.mkv', '.mp4', '.avi', '.mov', '.wmv', '.flv', '.webm', '.m4v'}
    return Path(file_path).suffix.lower() in video_extensions


def scan_directory(directory, verbose=False):
    """Scan directory for video files."""
    directory = Path(directory).expanduser().resolve()

    if not directory.exists():
        print(f"Error: Directory does not exist: {directory}")
        return []

    media_files = []
    try:
        for file_path in directory.rglob('*'):
            if file_path.is_file() and is_video_file(file_path):
                media_files.append(str(file_path))
    except Exception as e:
        print(f"Error scanning directory: {e}")
        return []

    if verbose:
        print(f"Found {len(media_files)} media files")

    return media_files


def process_file(file_path, sources, verbose=False):
    """Process single media file and extract metadata."""
    if verbose:
        print(f"Processing: {Path(file_path).name}")

    checksum = generate_checksum(file_path)
    if not checksum:
        return None

    filename = Path(file_path).name
    guess = guessit(filename)
    filesource = extract_source_from_path(file_path)
    filesrc = sources.get(filesource.lower(), "")

    # Build data - NO filesize per instructions
    data = {
        'checksum': checksum,
        'filename': filename,
        'fileloc': str(file_path),
        'newloc': None,  # Not populated yet
        'filesource': filesource,
        'url': None,  # Not populated yet
        'movie': None,
        'series': None,
        'season': None,
        'episode': None,
        'title': None,
        'resolution': guess.get('screen_size'),
        'hdr': None,  # Not populated yet
        'vcodec': guess.get('video_codec'),
        'vlevel': None,  # Not populated yet
        'vbitrate': None,  # Not populated yet
        'acodec': guess.get('audio_codec'),
        'abitrate': None,  # Not populated yet
        'achannels': None,  # Not populated yet
        'asample': None,  # Not populated yet
        'duration': None,  # Not populated yet
        'language': str(guess.get('language', [])) if guess.get('language') else None,
        'subtitles': None,  # Not populated yet
        'sptitle': str(guess.get('edition')) if guess.get('edition') else None,
        'is_movie': False,
        'filesrc': filesrc
    }

    # Set movie or TV data
    if guess.get('type') == 'movie':
        data['movie'] = guess.get('title', 'Unknown')
        data['is_movie'] = True
    else:
        data['series'] = guess.get('title', 'Unknown')
        season = guess.get('season')
        episode = guess.get('episode')
        data['season'] = int(season) if season and str(season).isdigit() else season
        data['episode'] = int(episode) if episode and str(episode).isdigit() else episode
        data['title'] = guess.get('episode_title')

    if verbose:
        print(f"  Type: {'Movie' if data['is_movie'] else 'TV Show'}")
        print(f"  Checksum: {checksum[:12]}...")

    return data


def insert_into_database(data_list, config, torrent_type_override, custom_site, verbose=False):
    """Insert data into database - ONLY import and user tables."""
    script_dir = Path(__file__).parent
    db_path = script_dir.parent / "tapedeck.db"
    defaults = config.get('default', {})

    try:
        conn = sqlite3.connect(str(db_path))
        cursor = conn.cursor()

        # Check what columns exist in import table
        cursor.execute("PRAGMA table_info(import)")
        columns = {row[1]: row for row in cursor.fetchall()}

        has_movie = 'movie' in columns
        has_series = 'series' in columns

        for data in data_list:
            # Determine torrent type
            if torrent_type_override:
                torrent_type = torrent_type_override
            else:
                torrent_type = "movie" if data['is_movie'] else "season"

            # Insert into import table ONLY
            if has_movie and has_series:
                # Both columns exist - insert everything
                cursor.execute("""
                    INSERT OR REPLACE INTO import (
                        checksum, movie, series, season, episode, title, resolution, hdr,
                        vcodec, vlevel, vbitrate, acodec, abitrate, achannels, asample,
                        filesize, duration, language, subtitles, sptitle, filename,
                        fileloc, newloc, filesource, url
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    data['checksum'], data['movie'], data['series'], data['season'],
                    data['episode'], data['title'], data['resolution'], data['hdr'],
                    data['vcodec'], data['vlevel'], data['vbitrate'], data['acodec'],
                    data['abitrate'], data['achannels'], data['asample'], None,  # filesize = None
                    data['duration'], data['language'], data['subtitles'], data['sptitle'],
                    data['filename'], data['fileloc'], data['newloc'], data['filesource'],
                    data['url']
                ))

            elif has_movie:
                # Movie-only database
                cursor.execute("""
                    INSERT OR REPLACE INTO import (
                        checksum, movie, resolution, hdr, vcodec, vlevel, vbitrate, acodec,
                        abitrate, achannels, asample, filesize, duration, language,
                        subtitles, sptitle, filename, fileloc, newloc, filesource, url
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    data['checksum'], data['movie'], data['resolution'], data['hdr'],
                    data['vcodec'], data['vlevel'], data['vbitrate'], data['acodec'],
                    data['abitrate'], data['achannels'], data['asample'], None,  # filesize = None
                    data['duration'], data['language'], data['subtitles'], data['sptitle'],
                    data['filename'], data['fileloc'], data['newloc'], data['filesource'],
                    data['url']
                ))

            elif has_series:
                # TV-only database
                cursor.execute("""
                    INSERT OR REPLACE INTO import (
                        checksum, series, season, episode, title, resolution, hdr, vcodec,
                        vlevel, vbitrate, acodec, abitrate, achannels, asample, filesize,
                        duration, language, subtitles, sptitle, filename, fileloc, newloc,
                        filesource, url
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    data['checksum'], data['series'], data['season'], data['episode'],
                    data['title'], data['resolution'], data['hdr'], data['vcodec'],
                    data['vlevel'], data['vbitrate'], data['acodec'], data['abitrate'],
                    data['achannels'], data['asample'], None,  # filesize = None
                    data['duration'], data['language'], data['subtitles'], data['sptitle'],
                    data['filename'], data['fileloc'], data['newloc'], data['filesource'],
                    data['url']
                ))

            # Insert into user table with defaults from user.json
            cursor.execute("""
                INSERT OR REPLACE INTO user (
                    checksum, releasegroup, filereleasegroup, torrentsite, torrenttype,
                    url, filesource, filesrc, type
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                data['checksum'],
                defaults.get('releasegroup', ''),
                defaults.get('filereleasegroup', ''),
                custom_site or defaults.get('torrentsite', ''),
                torrent_type,
                data['url'],  # This will be None initially
                data['filesource'],
                data['filesrc'],
                "movie" if data['is_movie'] else "tv show"
            ))

            if verbose:
                print(f"Imported: {data['filename']}")

        conn.commit()
        conn.close()
        return True

    except Exception as e:
        print(f"Database error: {e}")
        return False


def main():
    """Main function."""
    parser = argparse.ArgumentParser(description="KISS media file importer")
    parser.add_argument("-v", "--verbose", action="store_true", help="Verbose mode")
    parser.add_argument("-movie", action="store_true", help="Force movie database")
    parser.add_argument("-tv", action="store_true", help="Force TV database")
    parser.add_argument("-loc", help="Custom file location")
    parser.add_argument("-site", help="Custom torrent site")
    parser.add_argument("-series", action="store_true", help="Set torrent type to series")
    parser.add_argument("-season", action="store_true", help="Set torrent type to season")
    parser.add_argument("-episode", action="store_true", help="Set torrent type to episode")

    args = parser.parse_args()

    # Load configuration
    config = load_user_config()
    sources = load_sources()

    # Determine torrent type override
    torrent_type_override = None
    if args.series:
        torrent_type_override = "series"
    elif args.season:
        torrent_type_override = "season"
    elif args.episode:
        torrent_type_override = "episode"

    # Get scan directory
    scan_dir = args.loc or config.get('default', {}).get('filelocation', '.')

    if args.verbose:
        print(f"Scanning: {scan_dir}")

    # Scan for files
    media_files = scan_directory(scan_dir, args.verbose)
    if not media_files:
        print(f"No media files found in: {scan_dir}")
        return

    # Create database
    if not create_database(args.movie, args.tv, args.verbose):
        print("Failed to create database")
        sys.exit(1)

    # Process files
    processed_data = []
    for file_path in media_files:
        data = process_file(file_path, sources, args.verbose)
        if data:
            processed_data.append(data)

    if not processed_data:
        print("No files processed successfully")
        return

    # Insert into database
    if insert_into_database(processed_data, config, torrent_type_override, args.site, args.verbose):
        print(f"Successfully imported {len(processed_data)} files")
    else:
        print("Failed to import files")
        sys.exit(1)


if __name__ == "__main__":
    main()