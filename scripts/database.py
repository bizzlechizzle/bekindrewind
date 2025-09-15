#!/usr/bin/env python3

import argparse
import sqlite3
import sys
from pathlib import Path


def create_tables(cursor, is_movie, is_tv):
    i = "checksum TEXT PRIMARY KEY"
    if is_movie:
        i += ",movie TEXT"
    if is_tv:
        i += ",series TEXT,season INTEGER,episode INTEGER,title TEXT"
    i += ",stitle TEXT,resolution TEXT,hdr TEXT,vcodec TEXT,vacodec TEXT,vbitrate REAL,acodec TEXT,abitrate REAL,achannels TEXT,asample REAL,filesize REAL,duration TEXT,language TEXT,subtitles TEXT,filename TEXT,fileloc TEXT,newloc TEXT,dlsource TEXT,torrentsite TEXT,torrenttype TEXT,url TEXT,uploaded INTEGER"

    cursor.execute(f"CREATE TABLE import ({i})")

    o = "checksum TEXT PRIMARY KEY"
    if is_movie:
        o += ",dmovie TEXT,release TEXT,studio TEXT"
    if is_tv:
        o += ",dseries TEXT,dseason TEXT,depisode TEXT,airdate TEXT,network TEXT"
    o += ",genre TEXT,rating TEXT,cast TEXT"
    if is_movie:
        o += ",imovie TEXT"
    if is_tv:
        o += ",iseries TEXT,iseason TEXT,iepisode TEXT"
    o += ",imdb TEXT,tmdb TEXT"
    if is_tv:
        o += ",tvmaze TEXT,tvdb TEXT"

    cursor.execute(f"CREATE TABLE online ({o})")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-v", "--verbose", action="store_true")
    parser.add_argument("-movie", action="store_true")
    parser.add_argument("-tv", action="store_true")
    args = parser.parse_args()

    if not args.movie and not args.tv:
        print("Error: Must specify either -movie or -tv")
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

        create_tables(cursor, args.movie, args.tv)

        if args.verbose:
            print("Created tables: import, online")

        conn.commit()
        conn.close()
        print(f"Database created: {db_path}")

    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()