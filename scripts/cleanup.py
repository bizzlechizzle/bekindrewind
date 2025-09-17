#!/usr/bin/env python3
"""Remove source files for uploads that have finished seeding."""

import argparse
import json
import sqlite3
from pathlib import Path
from typing import Iterable, Set

CONFIG_PATH = Path(__file__).parent.parent / "user.json"
DB_PATH = Path(__file__).parent.parent / "tapedeck.db"
ALLOWED_SOURCES = {"amazon", "youtube", "netflix", "hulu", "hbomax", "hbo", "max"}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Delete uploaded source files")
    parser.add_argument("-v", "--verbose", action="store_true", help="Verbose output")
    return parser.parse_args()


def load_config() -> dict:
    try:
        with CONFIG_PATH.open("r", encoding="utf-8") as handle:
            config = json.load(handle)
    except FileNotFoundError as exc:
        raise RuntimeError(f"Error: Missing configuration file: {CONFIG_PATH}") from exc
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Error: Failed to parse configuration: {exc}") from exc

    location = config.get("default", {}).get("filelocation")
    if not location or not str(location).strip():
        raise RuntimeError("Error: default.filelocation is not configured")
    return config


def resolve_root(config: dict) -> Path:
    root = Path(str(config["default"]["filelocation"]))
    root = root.expanduser()
    if not root.exists():
        raise RuntimeError(f"Error: Source location not found: {root}")
    return root


def normalize_tokens(text: str) -> Set[str]:
    cleaned = text.lower()
    for separator in "-_.()[]{}":
        cleaned = cleaned.replace(separator, " ")
    return {token for token in cleaned.split() if token}


def allowed_source(name: str) -> bool:
    tokens = normalize_tokens(name)
    return bool(tokens & ALLOWED_SOURCES)


def inside(path: Path, root: Path) -> bool:
    try:
        path.expanduser().resolve().relative_to(root.resolve())
        return True
    except (ValueError, FileNotFoundError):
        return False


def same_file(a: Path, b: Path) -> bool:
    try:
        return a.exists() and b.exists() and a.is_file() and b.is_file() and a.samefile(b)
    except OSError:
        return False


def remove_empty_directories(path: Path, root: Path, verbose: bool) -> None:
    current = path
    while current != root:
        if not inside(current, root):
            break
        try:
            current.rmdir()
            if verbose:
                print(f"Deleted empty directory: {current}")
        except OSError:
            break
        current = current.parent


def delete_sources(records: Iterable[sqlite3.Row], root: Path, verbose: bool) -> int:
    removed = 0
    processed: Set[Path] = set()

    for row in records:
        source = Path(row["fileloc"])
        if source in processed:
            continue
        processed.add(source)

        if not source.exists():
            if verbose:
                print(f"Skipping missing file: {source}")
            continue
        if not inside(source, root):
            if verbose:
                print(f"Skipping outside source root: {source}")
            continue

        dlsource = str(row["dlsource"] or "")
        if dlsource and not allowed_source(dlsource):
            continue

        release_path_text = row["newloc"]
        if not release_path_text:
            if verbose:
                print(f"Skipping without release path: {source}")
            continue
        release_path = Path(release_path_text)
        if inside(release_path, root):
            if verbose:
                print(f"Skipping release inside source root: {release_path}")
            continue
        if not same_file(source, release_path):
            if verbose:
                print(f"Skipping without matching hardlink: {source}")
            continue

        try:
            source.unlink()
            removed += 1
            if verbose:
                print(f"Deleted: {source}")
        except OSError as exc:
            if verbose:
                print(f"Failed to delete {source}: {exc}")
            continue

        remove_empty_directories(source.parent, root, verbose)

    return removed


def fetch_records(conn: sqlite3.Connection):
    query = """
        SELECT fileloc, newloc, dlsource
        FROM import
        WHERE uploaded = 1
          AND fileloc IS NOT NULL
          AND TRIM(fileloc) != ''
    """
    return conn.execute(query)


def main() -> None:
    args = parse_args()

    try:
        config = load_config()
        root = resolve_root(config)
    except RuntimeError as exc:
        print(exc)
        return

    if not DB_PATH.exists():
        print(f"Error: Database not found at {DB_PATH}")
        return

    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        records = list(fetch_records(conn))

    if not records:
        print("No completed uploads found")
        return

    removed = delete_sources(records, root, args.verbose)
    print(f"Cleaned up {removed} source files")


if __name__ == "__main__":
    main()
