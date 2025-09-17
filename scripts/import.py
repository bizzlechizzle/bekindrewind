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


STREAMING_SOURCES = (
    "amazon",
    "youtube",
    "hbo",
    "max",
    "netflix",
    "hulu",
    "disney",
    "paramount",
    "peacock",
    "apple",
)


def extract_filesource(file_path):
    """Return folder name representing the online source if present, else parent folder."""
    path = Path(file_path)
    for part in path.parts:
        lowered = part.lower()
        if any(source in lowered for source in STREAMING_SOURCES):
            return part
    parent_name = path.parent.name
    return parent_name or "unknown"

def scan_videos(directory):
    """Find video files."""
    video_exts = {
        ".mkv",
        ".mp4",
        ".avi",
        ".mov",
        ".wmv",
        ".flv",
        ".webm",
        ".m4v",
    }
    return sorted(
        p for p in Path(directory).rglob("*") if p.is_file() and p.suffix.lower() in video_exts
    )


def _first_or_none(value):
    if isinstance(value, (list, tuple)):
        return value[0] if value else None
    return value


def _normalize_number(value):
    candidate = _first_or_none(value)
    if candidate is None:
        return None
    try:
        return int(candidate)
    except (TypeError, ValueError):
        return None


def extract_media_info(guess):
    """Extract core media metadata from guessit result."""
    media_type = str(guess.get("type", "")).lower()
    stitle = guess.get("edition")

    if media_type == "movie":
        return {
            "movie": guess.get("title") or "Unknown",
            "stitle": stitle,
            "is_movie": True,
        }

    series_name = guess.get("title") or guess.get("series") or "Unknown"
    return {
        "series": series_name,
        "season": _normalize_number(guess.get("season")),
        "episode": _normalize_number(guess.get("episode")),
        "title": guess.get("episode_title"),
        "stitle": stitle,
        "is_movie": False,
    }

def process_single_file(file_path, torrent_site, torrent_type, verbose):
    """Process one video file into an import table entry."""
    if verbose:
        print(f"Processing: {file_path.name}")

    checksum = get_checksum(file_path)
    guess = guessit(str(file_path))
    entry = {
        "checksum": checksum,
        "filename": file_path.name,
        "fileloc": str(file_path),
        "dlsource": extract_filesource(file_path),
        "torrentsite": torrent_site,
        "torrenttype": torrent_type,
    }
    entry.update(extract_media_info(guess))
    return entry

def process_files(files, torrent_site, torrent_type, verbose):
    """Process video files and extract data per import.md instructions."""
    entries = []
    for file_path in files:
        try:
            entries.append(process_single_file(file_path, torrent_site, torrent_type, verbose))
        except KeyboardInterrupt:
            raise
        except Exception as exc:
            print(f"Error processing {file_path.name}: {exc}")
    return entries


def create_database(verbose):
    """Run database.py to create unified schema database."""
    script_dir = Path(__file__).parent
    cmd = [sys.executable, str(script_dir / "database.py"), "-tv"]
    if verbose:
        cmd.append("-v")
    subprocess.run(cmd, check=True)


MOVIE_COLUMNS = (
    "checksum",
    "movie",
    "filename",
    "fileloc",
    "torrentsite",
    "torrenttype",
    "dlsource",
    "stitle",
)

EPISODE_COLUMNS = (
    "checksum",
    "series",
    "season",
    "episode",
    "title",
    "filename",
    "fileloc",
    "torrentsite",
    "torrenttype",
    "dlsource",
    "stitle",
)


def _insert_import(cursor, entry):
    if entry["is_movie"]:
        columns = MOVIE_COLUMNS
    else:
        columns = EPISODE_COLUMNS
    placeholders = ", ".join("?" for _ in columns)
    column_list = ", ".join(columns)
    values = tuple(entry.get(column) for column in columns)
    cursor.execute(
        f"INSERT OR REPLACE INTO import ({column_list}) VALUES ({placeholders})",
        values,
    )


def insert_data(data, verbose):
    """Insert data into import table and copy checksums to online table."""
    db_path = Path(__file__).parent.parent / "tapedeck.db"
    with sqlite3.connect(str(db_path)) as conn:
        cursor = conn.cursor()
        for entry in data:
            _insert_import(cursor, entry)
            cursor.execute(
                "INSERT OR REPLACE INTO online (checksum) VALUES (?)",
                (entry["checksum"],),
            )
            if verbose:
                print(f"Imported: {entry['filename']}")


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

    config_path = Path(__file__).parent.parent / "user.json"
    try:
        with open(config_path, "r", encoding="utf-8") as config_file:
            config = json.load(config_file)
    except FileNotFoundError:
        print(f"Error: {config_path} not found")
        sys.exit(1)
    except json.JSONDecodeError as exc:
        print(f"Error: Invalid JSON in {config_path}: {exc}")
        sys.exit(1)

    defaults = config.get("default", {})

    location_value = args.loc or defaults.get("filelocation")
    if not location_value:
        print("Error: File location is not configured")
        sys.exit(1)
    location_path = Path(location_value).expanduser()

    if not location_path.exists():
        print(f"Error: Location does not exist: {location_path}")
        sys.exit(1)
    if not location_path.is_dir():
        print(f"Error: Location is not a directory: {location_path}")
        sys.exit(1)

    torrent_site = args.site or defaults.get("torrentsite")
    if not torrent_site:
        print("Error: Torrent site is not configured")
        sys.exit(1)

    torrent_flags = [args.movie, args.series, args.season, args.episode]
    if sum(bool(flag) for flag in torrent_flags) > 1:
        print("Error: Specify only one torrent type flag")
        sys.exit(1)

    if args.movie:
        torrent_type = "movie"
    elif args.series:
        torrent_type = "series"
    elif args.season:
        torrent_type = "season"
    elif args.episode:
        torrent_type = "episode"
    else:
        torrent_type = defaults.get("torrenttype", "season")

    valid_torrent_types = {"movie", "series", "season", "episode"}
    if torrent_type not in valid_torrent_types:
        print(f"Error: Invalid torrent type: {torrent_type}")
        sys.exit(1)

    if args.verbose:
        print(f"Scanning: {location_path}")

    video_files = scan_videos(location_path)
    if not video_files:
        print(f"No video files found in: {location_path}")
        return

    try:
        create_database(args.verbose)
    except subprocess.CalledProcessError as exc:
        print(f"Error creating database: {exc}")
        sys.exit(1)

    data = process_files(video_files, torrent_site, torrent_type, args.verbose)
    if not data:
        print("No files imported.")
        return

    try:
        insert_data(data, args.verbose)
    except sqlite3.Error as exc:
        print(f"Error inserting data: {exc}")
        sys.exit(1)

    print(f"Successfully imported {len(data)} files")


if __name__ == "__main__":
    main()
