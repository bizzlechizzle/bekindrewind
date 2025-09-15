#!/usr/bin/env python3

import sqlite3
from pathlib import Path

def migrate_database():
    db_path = Path(__file__).parent.parent / "tapedeck.db"

    if not db_path.exists():
        print("No database found - will be created fresh")
        return

    conn = sqlite3.connect(str(db_path))
    cursor = conn.cursor()

    # Check if uploaded column exists
    cursor.execute("PRAGMA table_info(import)")
    columns = [row[1] for row in cursor.fetchall()]

    if 'uploaded' not in columns:
        print("Adding uploaded column to import table...")
        cursor.execute("ALTER TABLE import ADD COLUMN uploaded INTEGER")
        conn.commit()
        print("Migration complete")
    else:
        print("Database already up to date")

    conn.close()

if __name__ == "__main__":
    migrate_database()