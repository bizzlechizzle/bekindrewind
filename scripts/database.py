#!/usr/bin/env python3

import argparse
import sqlite3
import sys
from pathlib import Path


IMPORT_COLUMNS = [
    ("checksum", "TEXT PRIMARY KEY"),
    ("movie", "TEXT"),
    ("series", "TEXT"),
    ("season", "INTEGER"),
    ("episode", "INTEGER"),
    ("title", "TEXT"),
    ("stitle", "TEXT"),
    ("resolution", "TEXT"),
    ("hdr", "TEXT"),
    ("vcodec", "TEXT"),
    ("vacodec", "TEXT"),
    ("vbitrate", "TEXT"),
    ("acodec", "TEXT"),
    ("abitrate", "TEXT"),
    ("achannels", "TEXT"),
    ("asample", "TEXT"),
    ("filesize", "TEXT"),
    ("duration", "TEXT"),
    ("language", "TEXT"),
    ("subtitles", "TEXT"),
    ("filename", "TEXT"),
    ("fileloc", "TEXT"),
    ("newname", "TEXT"),
    ("newloc", "TEXT"),
    ("dlsource", "TEXT"),
    ("torrentsite", "TEXT"),
    ("torrenttype", "TEXT"),
    ("url", "TEXT"),
    ("uploaded", "INTEGER"),
]

ONLINE_COLUMNS = [
    ("checksum", "TEXT PRIMARY KEY"),
    ("dmovie", "TEXT"),
    ("release", "TEXT"),
    ("studio", "TEXT"),
    ("dseries", "TEXT"),
    ("dseason", "TEXT"),
    ("depisode", "TEXT"),
    ("airdate", "TEXT"),
    ("network", "TEXT"),
    ("genre", "TEXT"),
    ("rating", "TEXT"),
    ("cast", "TEXT"),
    ("imovie", "TEXT"),
    ("iseries", "TEXT"),
    ("iseason", "TEXT"),
    ("iepisode", "TEXT"),
    ("imdb", "TEXT"),
    ("tmdb", "TEXT"),
    ("tvmaze", "TEXT"),
    ("tvdb", "TEXT"),
]


def create_tables(cursor):
    import_definition = ", ".join(f"{col} {col_type}" for col, col_type in IMPORT_COLUMNS)
    online_definition = ", ".join(f"{col} {col_type}" for col, col_type in ONLINE_COLUMNS)

    cursor.execute(f"CREATE TABLE import ({import_definition})")
    cursor.execute(f"CREATE TABLE online ({online_definition})")

    cursor.execute("PRAGMA table_info(import)")
    import_columns = [row[1] for row in cursor.fetchall()]
    cursor.execute("PRAGMA table_info(online)")
    online_columns = [row[1] for row in cursor.fetchall()]

    expected_import = [col for col, _ in IMPORT_COLUMNS]
    expected_online = [col for col, _ in ONLINE_COLUMNS]

    if import_columns != expected_import:
        raise RuntimeError(f"Import table schema mismatch: {import_columns}")
    if online_columns != expected_online:
        raise RuntimeError(f"Online table schema mismatch: {online_columns}")


def parse_args():
    parser = argparse.ArgumentParser(description="Create blank tapedeck database")
    parser.add_argument("-v", "--verbose", action="store_true", help="Verbose output")
    parser.add_argument("-movie", action="store_true", help="Legacy flag (ignored)")
    parser.add_argument("-tv", action="store_true", help="Legacy flag (ignored)")
    return parser.parse_args()


def main():
    args = parse_args()

    db_path = Path(__file__).parent.parent / "tapedeck.db"

    if not db_path.parent.exists():
        print(f"Error: Parent directory does not exist: {db_path.parent}")
        sys.exit(1)

    if db_path.exists():
        try:
            db_path.unlink()
            if args.verbose:
                print(f"Deleted existing database: {db_path}")
        except OSError as exc:
            print(f"Error deleting database: {exc}")
            sys.exit(1)

    if args.verbose:
        print(f"Creating database: {db_path}")

    try:
        with sqlite3.connect(str(db_path)) as conn:
            cursor = conn.cursor()
            create_tables(cursor)
            if args.verbose:
                print("Created tables: import, online")
    except sqlite3.Error as exc:
        print(f"Database error: {exc}")
        sys.exit(1)
    except RuntimeError as exc:
        print(f"Schema validation failed: {exc}")
        sys.exit(1)

    print(f"Database created: {db_path}")


if __name__ == "__main__":
    main()
