#!/usr/bin/env python3

import argparse
import sqlite3
import sys
from pathlib import Path


def create_tables(cursor, is_movie, is_tv):
    # KISS: Always create unified schema with ALL columns to prevent script failures
    import_schema = """checksum TEXT PRIMARY KEY,
    movie TEXT,
    series TEXT,
    season INTEGER,
    episode INTEGER,
    title TEXT,
    stitle TEXT,
    resolution TEXT,
    hdr TEXT,
    vcodec TEXT,
    vacodec TEXT,
    vbitrate REAL,
    acodec TEXT,
    abitrate REAL,
    achannels TEXT,
    asample REAL,
    filesize REAL,
    duration TEXT,
    language TEXT,
    subtitles TEXT,
    filename TEXT,
    fileloc TEXT,
    newname TEXT,
    newloc TEXT,
    dlsource TEXT,
    torrentsite TEXT,
    torrenttype TEXT,
    url TEXT,
    uploaded INTEGER"""

    online_schema = """checksum TEXT PRIMARY KEY,
    dmovie TEXT,
    release TEXT,
    studio TEXT,
    dseries TEXT,
    dseason TEXT,
    depisode TEXT,
    airdate TEXT,
    network TEXT,
    genre TEXT,
    rating TEXT,
    cast TEXT,
    imovie TEXT,
    iseries TEXT,
    iseason TEXT,
    iepisode TEXT,
    imdb TEXT,
    tmdb TEXT,
    tvmaze TEXT,
    tvdb TEXT"""

    cursor.execute(f"CREATE TABLE import ({import_schema})")
    cursor.execute(f"CREATE TABLE online ({online_schema})")

    # Validate schema was created correctly
    cursor.execute("PRAGMA table_info(import)")
    import_cols = [row[1] for row in cursor.fetchall()]
    cursor.execute("PRAGMA table_info(online)")
    online_cols = [row[1] for row in cursor.fetchall()]

    expected_import = ['checksum', 'movie', 'series', 'season', 'episode', 'title', 'stitle', 'resolution', 'hdr', 'vcodec', 'vacodec', 'vbitrate', 'acodec', 'abitrate', 'achannels', 'asample', 'filesize', 'duration', 'language', 'subtitles', 'filename', 'fileloc', 'newname', 'newloc', 'dlsource', 'torrentsite', 'torrenttype', 'url', 'uploaded']
    expected_online = ['checksum', 'dmovie', 'release', 'studio', 'dseries', 'dseason', 'depisode', 'airdate', 'network', 'genre', 'rating', 'cast', 'imovie', 'iseries', 'iseason', 'iepisode', 'imdb', 'tmdb', 'tvmaze', 'tvdb']

    if import_cols != expected_import:
        raise Exception(f"Import table schema mismatch: got {import_cols}")
    if online_cols != expected_online:
        raise Exception(f"Online table schema mismatch: got {online_cols}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-v", "--verbose", action="store_true")
    parser.add_argument("-movie", action="store_true", help="Legacy flag - now creates unified schema")
    parser.add_argument("-tv", action="store_true", help="Legacy flag - now creates unified schema")
    args = parser.parse_args()

    # KISS: Always create unified schema supporting both movies and TV
    if not args.movie and not args.tv:
        print("Error: Must specify either -movie or -tv (creates unified schema)")
        sys.exit(1)

    db_path = Path(__file__).parent.parent / "tapedeck.db"

    if not db_path.parent.exists():
        print(f"Error: Parent directory does not exist: {db_path.parent}")
        sys.exit(1)

    if db_path.exists():
        db_path.unlink()
        if args.verbose:
            print(f"Deleted existing database: {db_path}")

    try:
        conn = sqlite3.connect(str(db_path))
        cursor = conn.cursor()

        if args.verbose:
            print(f"Creating database: {db_path}")
            print("Creating unified schema with all columns for both movies and TV")

        create_tables(cursor, args.movie, args.tv)

        if args.verbose:
            print("Created tables: import, online with unified schema")

        conn.commit()
        conn.close()
        print(f"Database created: {db_path}")

    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()