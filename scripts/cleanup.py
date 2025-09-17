#!/usr/bin/env python3
"""Cleanup uploaded source files per cleanup.md instructions."""

from __future__ import annotations

import argparse
import json
import sqlite3
from pathlib import Path
from typing import Dict, Iterable, Set

CONFIG_PATH = Path(__file__).parent.parent / "user.json"
DB_PATH = Path(__file__).parent.parent / "tapedeck.db"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Delete uploaded source files safely")
    parser.add_argument("-v", "--verbose", action="store_true", help="Verbose output")
    return parser.parse_args()


def load_config(path: Path = CONFIG_PATH) -> Dict[str, object]:
    try:
        with path.open("r", encoding="utf-8") as handle:
            config = json.load(handle)
    except FileNotFoundError as exc:
        raise RuntimeError(f"Error: Missing configuration file: {path}") from exc
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Error: Failed to parse configuration: {exc}") from exc

    default = config.get("default")
    if not isinstance(default, dict):
        raise RuntimeError("Error: Configuration missing 'default' section")

    location = default.get("filelocation")
    if not isinstance(location, str) or not location.strip():
        raise RuntimeError("Error: Configuration missing default.filelocation")

    return config


def safe_resolve(path: Path) -> Path:
    try:
        return path.expanduser().resolve()
    except FileNotFoundError:
        return path.expanduser().absolute()


def is_relative_to(path: Path, root: Path) -> bool:
    try:
        safe_resolve(path).relative_to(safe_resolve(root))
        return True
    except ValueError:
        return False


def tokenize(source: str) -> Set[str]:
    cleaned = source.lower()
    for separator in "-_.()[]{}":
        cleaned = cleaned.replace(separator, " ")
    return {token for token in cleaned.split() if token}


def matches_allowed_source(source: str) -> bool:
    tokens = tokenize(source)
    if tokens & {"amazon", "youtube", "netflix", "hulu"}:
        return True
    if {"hbo", "hbomax"} & tokens:
        return True
    if "max" in tokens:
        return True
    return False


def get_source_root(config: Dict[str, object]) -> Path:
    root = Path(str(config["default"]["filelocation"]).strip()).expanduser()
    if not root.exists():
        raise RuntimeError(f"Error: Source location not found: {root}")
    return root


def fetch_completed_uploads(conn: sqlite3.Connection) -> Iterable[sqlite3.Row]:
    conn.row_factory = sqlite3.Row
    query = """
        SELECT checksum, fileloc, newloc, dlsource
        FROM import
        WHERE uploaded = 1
          AND fileloc IS NOT NULL
          AND TRIM(fileloc) != ''
    """
    return conn.execute(query)


def ensure_hardlink(source_path: Path, release_path: Path) -> bool:
    try:
        if not release_path.exists() or not source_path.exists():
            return False
        if not release_path.is_file() or not source_path.is_file():
            return False
        if not release_path.samefile(source_path):
            return False
        if source_path.stat().st_nlink < 2:
            return False
    except OSError:
        return False
    return True


def remove_empty_directories(start: Path, root: Path, verbose: bool) -> None:
    for directory in [start, *start.parents]:
        if directory == root:
            break
        if not is_relative_to(directory, root):
            break
        try:
            directory.rmdir()
            if verbose:
                print(f"Deleted empty directory: {directory}")
        except OSError:
            break


def cleanup_uploaded_sources(records: Iterable[sqlite3.Row], source_root: Path, verbose: bool) -> int:
    cleaned = 0
    processed: Set[Path] = set()

    for row in records:
        fileloc = row["fileloc"]
        if not fileloc:
            continue
        source_path = Path(fileloc)
        resolved_source = safe_resolve(source_path)
        if resolved_source in processed:
            continue
        processed.add(resolved_source)

        if not source_path.exists():
            if verbose:
                print(f"Skipping missing file: {source_path}")
            continue
        if not is_relative_to(source_path, source_root):
            if verbose:
                print(f"Skipping outside source root: {source_path}")
            continue

        dlsource = str(row["dlsource"] or "")
        if not matches_allowed_source(dlsource):
            continue

        newloc = row["newloc"]
        if not newloc:
            if verbose:
                print(f"Skipping missing release path for: {source_path}")
            continue
        release_path = Path(newloc)
        if is_relative_to(release_path, source_root):
            if verbose:
                print(f"Skipping release within source root: {release_path}")
            continue
        if not ensure_hardlink(source_path, release_path):
            if verbose:
                print(f"Skipping without hardlink: {source_path}")
            continue

        try:
            source_path.unlink()
            cleaned += 1
            if verbose:
                print(f"Deleted: {source_path}")
        except OSError as exc:
            if verbose:
                print(f"Failed to delete {source_path}: {exc}")
            continue

        remove_empty_directories(source_path.parent, source_root, verbose)

    return cleaned


def main() -> None:
    args = parse_args()

    try:
        config = load_config()
        source_root = get_source_root(config)
    except RuntimeError as exc:
        print(exc)
        return

    if not DB_PATH.exists():
        print(f"Error: Database not found at {DB_PATH}")
        return

    with sqlite3.connect(str(DB_PATH)) as conn:
        records = list(fetch_completed_uploads(conn))

    if not records:
        print("No completed uploads found")
        return

    cleaned = cleanup_uploaded_sources(records, source_root, args.verbose)
    print(f"Cleaned up {cleaned} source files")


if __name__ == "__main__":
    main()
