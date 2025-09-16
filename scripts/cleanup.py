#!/usr/bin/env python3

import argparse
import json
import sqlite3
import sys
from pathlib import Path

def get_config():
    config_path = Path(__file__).parent.parent / "user.json"
    try:
        with open(config_path, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        print(
            "Error: Configuration file not found at "
            f"{config_path}. Create user.json in the project root "
            "or update the path in the script."
        )
        sys.exit(1)
    except json.JSONDecodeError as exc:
        print(
            "Error: Configuration file contains invalid JSON. "
            f"Check {config_path} and fix the syntax (details: {exc})."
        )
        sys.exit(1)

def get_completed_uploads():
    db_path = Path(__file__).parent.parent / "tapedeck.db"
    conn = sqlite3.connect(str(db_path))
    cursor = conn.cursor()

    cursor.execute("PRAGMA table_info(import)")
    cols = [row[1] for row in cursor.fetchall()]

    cursor.execute("SELECT * FROM import WHERE uploaded = 1 AND fileloc IS NOT NULL")
    records = cursor.fetchall()
    conn.close()

    return [dict(zip(cols, record)) for record in records]

def cleanup_source_files(config, completed_uploads, verbose=False):
    source_location = config['default']['filelocation']
    cleaned_count = 0

    for upload in completed_uploads:
        file_path = Path(upload['fileloc'])

        if not file_path.exists():
            continue

        if not str(file_path).startswith(source_location):
            continue

        source = upload.get('dlsource', '').lower()
        if source not in ['amazon', 'youtube', 'hbo', 'max', 'netflix', 'hulu']:
            continue

        try:
            file_path.unlink()
            cleaned_count += 1
            if verbose:
                print(f"Deleted: {file_path.name}")
        except Exception as e:
            if verbose:
                print(f"Failed to delete {file_path.name}: {e}")

        parent_dir = file_path.parent
        try:
            if parent_dir.exists() and not any(parent_dir.iterdir()):
                parent_dir.rmdir()
                if verbose:
                    print(f"Deleted empty directory: {parent_dir.name}")
        except:
            pass

    return cleaned_count

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()

    config = get_config()
    completed_uploads = get_completed_uploads()

    if not completed_uploads:
        print("No completed uploads found")
        return

    cleaned_count = cleanup_source_files(config, completed_uploads, args.verbose)
    print(f"Cleaned up {cleaned_count} source files")

if __name__ == "__main__":
    main()
