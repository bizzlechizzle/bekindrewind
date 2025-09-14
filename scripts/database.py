#!/usr/bin/env python3
"""
KISS SQLite database creation script.
Creates tapedeck.db in parent directory with conditional schemas.
Universal, works on any OS, bulletproof.

Usage:
    python database.py [-v] [-movie] [-tv]

Arguments:
    -v      : Verbose mode
    -movie  : Movie mode (creates movie-only columns)
    -tv     : TV mode (creates TV-only columns)
"""

import argparse
import os
import sqlite3
import sys
from pathlib import Path


def main():
    """Main function - KISS approach."""
    parser = argparse.ArgumentParser(description="Create SQLite database")
    parser.add_argument("-v", "--verbose", action="store_true", help="Verbose mode")
    parser.add_argument("-movie", action="store_true", help="Movie mode")
    parser.add_argument("-tv", action="store_true", help="TV mode")

    args = parser.parse_args()

    # Get database path - always in parent directory
    script_dir = Path(__file__).parent
    db_path = script_dir.parent / "tapedeck.db"

    # Delete old database
    if db_path.exists():
        db_path.unlink()
        if args.verbose:
            print(f"Deleted existing database: {db_path}")

    try:
        # Create database
        conn = sqlite3.connect(str(db_path))
        cursor = conn.cursor()

        if args.verbose:
            print(f"Creating database: {db_path}")

        # USER TABLE - always the same
        cursor.execute("""
            CREATE TABLE user (
                checksum TEXT PRIMARY KEY,
                releasegroup TEXT,
                filereleasegroup TEXT,
                torrentsite TEXT,
                torrenttype TEXT,
                url TEXT,
                filesource TEXT,
                filesrc TEXT,
                type TEXT
            )
        """)

        if args.verbose:
            print("Created table: user")

        # IMPORT TABLE - conditional
        if args.movie:
            # Movie only
            cursor.execute("""
                CREATE TABLE import (
                    movie TEXT,
                    resolution TEXT,
                    hdr TEXT,
                    vcodec TEXT,
                    vlevel TEXT,
                    vbitrate REAL,
                    acodec TEXT,
                    abitrate REAL,
                    achannels TEXT,
                    asample REAL,
                    filesize REAL,
                    duration TEXT,
                    language TEXT,
                    subtitles TEXT,
                    sptitle TEXT,
                    filename TEXT,
                    fileloc TEXT,
                    newloc TEXT,
                    filesource TEXT,
                    url TEXT,
                    checksum TEXT PRIMARY KEY
                )
            """)
        elif args.tv:
            # TV only
            cursor.execute("""
                CREATE TABLE import (
                    series TEXT,
                    season INTEGER,
                    episode INTEGER,
                    title TEXT,
                    resolution TEXT,
                    hdr TEXT,
                    vcodec TEXT,
                    vlevel TEXT,
                    vbitrate REAL,
                    acodec TEXT,
                    abitrate REAL,
                    achannels TEXT,
                    asample REAL,
                    filesize REAL,
                    duration TEXT,
                    language TEXT,
                    subtitles TEXT,
                    sptitle TEXT,
                    filename TEXT,
                    fileloc TEXT,
                    newloc TEXT,
                    filesource TEXT,
                    url TEXT,
                    checksum TEXT PRIMARY KEY
                )
            """)
        else:
            # Both
            cursor.execute("""
                CREATE TABLE import (
                    movie TEXT,
                    series TEXT,
                    season INTEGER,
                    episode INTEGER,
                    title TEXT,
                    resolution TEXT,
                    hdr TEXT,
                    vcodec TEXT,
                    vlevel TEXT,
                    vbitrate REAL,
                    acodec TEXT,
                    abitrate REAL,
                    achannels TEXT,
                    asample REAL,
                    filesize REAL,
                    duration TEXT,
                    language TEXT,
                    subtitles TEXT,
                    sptitle TEXT,
                    filename TEXT,
                    fileloc TEXT,
                    newloc TEXT,
                    filesource TEXT,
                    url TEXT,
                    checksum TEXT PRIMARY KEY
                )
            """)

        if args.verbose:
            print("Created table: import")

        # ONLINE TABLE - conditional (HAS API ID COLUMNS)
        if args.movie:
            # Movie only
            cursor.execute("""
                CREATE TABLE online (
                    checksum TEXT PRIMARY KEY,
                    movie TEXT,
                    sptitle TEXT,
                    dmovie TEXT,
                    year INTEGER,
                    release TEXT,
                    genre TEXT,
                    rating TEXT,
                    cast TEXT,
                    imdb TEXT,
                    tmdb TEXT,
                    imovie TEXT
                )
            """)
        elif args.tv:
            # TV only
            cursor.execute("""
                CREATE TABLE online (
                    checksum TEXT PRIMARY KEY,
                    series TEXT,
                    season INTEGER,
                    episode INTEGER,
                    title TEXT,
                    sptitle TEXT,
                    dseries TEXT,
                    dseason TEXT,
                    depisode TEXT,
                    year INTEGER,
                    airdate TEXT,
                    network TEXT,
                    genre TEXT,
                    rating TEXT,
                    cast TEXT,
                    imdb TEXT,
                    tmdb TEXT,
                    tvmaze TEXT,
                    tvdb TEXT,
                    iseries TEXT,
                    iseason TEXT,
                    iepisode TEXT
                )
            """)
        else:
            # Both
            cursor.execute("""
                CREATE TABLE online (
                    checksum TEXT PRIMARY KEY,
                    movie TEXT,
                    series TEXT,
                    season INTEGER,
                    episode INTEGER,
                    title TEXT,
                    sptitle TEXT,
                    dmovie TEXT,
                    dseries TEXT,
                    dseason TEXT,
                    depisode TEXT,
                    year INTEGER,
                    airdate TEXT,
                    release TEXT,
                    network TEXT,
                    genre TEXT,
                    rating TEXT,
                    cast TEXT,
                    imdb TEXT,
                    tmdb TEXT,
                    tvmaze TEXT,
                    tvdb TEXT,
                    iseries TEXT,
                    iseason TEXT,
                    iepisode TEXT,
                    imovie TEXT
                )
            """)

        if args.verbose:
            print("Created table: online")

        # API TABLE - conditional (NO API ID COLUMNS, HAS IMOVIE)
        if args.movie:
            # Movie only
            cursor.execute("""
                CREATE TABLE api (
                    checksum TEXT PRIMARY KEY,
                    movie TEXT,
                    sptitle TEXT,
                    dmovie TEXT,
                    year INTEGER,
                    release TEXT,
                    genre TEXT,
                    rating TEXT,
                    cast TEXT,
                    imovie TEXT
                )
            """)
        elif args.tv:
            # TV only
            cursor.execute("""
                CREATE TABLE api (
                    checksum TEXT PRIMARY KEY,
                    series TEXT,
                    season INTEGER,
                    episode INTEGER,
                    title TEXT,
                    sptitle TEXT,
                    dseries TEXT,
                    dseason TEXT,
                    depisode TEXT,
                    year INTEGER,
                    airdate TEXT,
                    network TEXT,
                    genre TEXT,
                    rating TEXT,
                    cast TEXT,
                    iseries TEXT,
                    iseason TEXT,
                    iepisode TEXT
                )
            """)
        else:
            # Both
            cursor.execute("""
                CREATE TABLE api (
                    checksum TEXT PRIMARY KEY,
                    movie TEXT,
                    series TEXT,
                    season INTEGER,
                    episode INTEGER,
                    title TEXT,
                    sptitle TEXT,
                    dmovie TEXT,
                    dseries TEXT,
                    dseason TEXT,
                    depisode TEXT,
                    year INTEGER,
                    airdate TEXT,
                    release TEXT,
                    network TEXT,
                    genre TEXT,
                    rating TEXT,
                    cast TEXT,
                    iseries TEXT,
                    iseason TEXT,
                    iepisode TEXT,
                    imovie TEXT
                )
            """)

        if args.verbose:
            print("Created table: api")

        conn.commit()
        conn.close()

        print(f"Database created: {db_path}")
        if args.verbose:
            mode = "Movie" if args.movie else "TV" if args.tv else "Both"
            print(f"Mode: {mode}")

    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()