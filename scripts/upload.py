#!/usr/bin/env python3
"""Create torrents from prepared releases and upload them."""

import argparse
import json
import shutil
import sqlite3
import subprocess
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

import requests

CONFIG_PATH = Path(__file__).parent.parent / "user.json"
PREFERENCES_PATH = Path(__file__).parent.parent / "preferences" / "torrentsites.json"
DB_PATH = Path(__file__).parent.parent / "tapedeck.db"

DISALLOWED_NAMES = {".ds_store", "tests"}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Create torrents and upload them")
    parser.add_argument("-v", "--verbose", action="store_true", help="Verbose output")
    parser.add_argument("-t", "--test", action="store_true", help="Create torrents without uploading")
    return parser.parse_args()


def load_json(path: Path, description: str) -> Dict:
    try:
        with path.open("r", encoding="utf-8") as handle:
            return json.load(handle)
    except FileNotFoundError as exc:
        raise RuntimeError(f"Error: Missing {description}: {path}") from exc
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Error: Failed to parse {description}: {exc}") from exc


def ensure_directory(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def normalize_type(value: Optional[str], default: str) -> str:
    if not value:
        return default
    text = str(value).strip().lower()
    return text or default


def detect_library(path: Path, file_roots: Dict[str, Path]) -> Optional[str]:
    for key, root in file_roots.items():
        try:
            path.resolve().relative_to(root.resolve())
            return key
        except (ValueError, FileNotFoundError):
            continue
    return None


def release_key(directory: Path) -> Tuple[float, str]:
    try:
        mtime = directory.stat().st_mtime
    except OSError:
        mtime = 0.0
    return (mtime, directory.as_posix())


def collect_releases(conn: sqlite3.Connection, config: Dict, verbose: bool) -> List[Dict]:
    default_type = normalize_type(config.get("default", {}).get("torrenttype"), "season")
    file_roots = {
        key: Path(path).expanduser()
        for key, path in config.get("locations", {}).get("file_upload", {}).items()
    }
    if {"movies", "tv_shows"} - set(file_roots):
        missing = ", ".join(sorted({"movies", "tv_shows"} - set(file_roots)))
        raise RuntimeError(f"Error: Missing file_upload locations for {missing}")

    query = """
        SELECT i.checksum, i.newloc, i.torrenttype, i.torrentsite,
               o.imdb, o.tvmaze
        FROM import AS i
        LEFT JOIN online AS o USING (checksum)
        WHERE i.newloc IS NOT NULL
          AND TRIM(i.newloc) != ''
          AND (i.uploaded IS NULL OR i.uploaded = 0)
    """
    rows = conn.execute(query).fetchall()
    releases: Dict[Path, Dict] = {}

    for checksum, newloc, torrenttype, torrentsite, imdb, tvmaze in rows:
        release_path = Path(newloc)
        if not release_path.exists():
            if verbose:
                print(f"Skipping missing release path: {release_path}")
            continue
        if release_path.is_file():
            release_path = release_path.parent

        library_key = detect_library(release_path, file_roots)
        if library_key is None:
            if verbose:
                print(f"Skipping release outside upload roots: {release_path}")
            continue

        key = release_path
        info = releases.setdefault(
            key,
            {
                "directory": release_path,
                "library": library_key,
                "checksums": [],
                "type": normalize_type(torrenttype, default_type),
                "site": normalize_type(torrentsite, config.get("default", {}).get("torrentsite", "torrentleech")),
                "imdb": None,
                "tvmaze": None,
            },
        )
        info["checksums"].append(checksum)
        if not info["imdb"] and imdb:
            info["imdb"] = str(imdb).strip()
        if not info["tvmaze"] and tvmaze:
            info["tvmaze"] = str(tvmaze).strip()
        if torrenttype:
            info["type"] = normalize_type(torrenttype, info["type"])
        if torrentsite:
            info["site"] = normalize_type(torrentsite, info["site"])

    ordered = sorted(releases.values(), key=lambda item: release_key(item["directory"]))
    return ordered


def find_disallowed(directory: Path) -> List[Path]:
    matches: List[Path] = []
    for entry in directory.rglob("*"):
        if entry.name.lower() in DISALLOWED_NAMES:
            matches.append(entry)
    return sorted(matches)


def build_payload(
    release: Dict,
    announce_key: str,
    category_mapping: Dict[str, str],
    categories: Dict[str, int],
) -> Dict[str, str]:
    category_key = category_mapping[release["type"]]
    payload = {
        "announcekey": announce_key,
        "category": str(categories[category_key]),
    }
    if release.get("imdb"):
        payload["imdb"] = release["imdb"]
    if release.get("tvmaze") and release["type"] != "movie":
        payload["tvmaze"] = release["tvmaze"]
        payload["tvmazetype"] = "2" if release["type"] == "episode" else "1"
    return payload


def create_torrent(source: Path, announce_url: str, output_path: Path, verbose: bool) -> bool:
    if verbose:
        print(f"Creating torrent for {source}")
    command = [
        "mktorrent",
        "-a",
        announce_url,
        "-o",
        str(output_path),
        str(source),
    ]
    result = subprocess.run(command, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"Error: mktorrent failed for {source}")
        if result.stderr:
            print(result.stderr.strip())
        return False
    return True


def upload_torrent(release: Dict, torrent_path: Path, payload: Dict[str, str], site_prefs: Dict, verbose: bool) -> bool:
    upload_url = site_prefs.get("upload_url")
    if not upload_url:
        print("Error: upload_url missing in torrentsites.json")
        return False

    files = {
        "torrent": (torrent_path.name, torrent_path.open("rb"), "application/x-bittorrent"),
    }
    nfo_path = release["directory"] / f"{release['directory'].name}.nfo"
    if nfo_path.exists():
        files["nfo"] = (nfo_path.name, nfo_path.open("rb"), "text/plain")
    else:
        payload["description"] = f"Release: {release['directory'].name}\nGenerated by upload.py"

    try:
        response = requests.post(upload_url, data=payload, files=files, timeout=60)
    except requests.RequestException as exc:
        print(f"Error: Upload failed for {release['directory'].name}: {exc}")
        return False
    finally:
        for _, handle, _ in files.values():
            handle.close()

    if verbose:
        print(f"Upload response: {response.status_code}")
    if not response.ok:
        print(f"Error: Upload rejected for {release['directory'].name}")
        if response.text:
            print(response.text.strip())
        return False
    if verbose and response.text:
        print(response.text.strip())
    return True


def move_torrent(temp_torrent: Path, final_torrent: Path, verbose: bool) -> bool:
    ensure_directory(final_torrent.parent)
    try:
        shutil.move(str(temp_torrent), str(final_torrent))
    except OSError as exc:
        print(f"Error: Unable to move torrent to monitored folder: {exc}")
        return False
    if verbose:
        print(f"Moved torrent to {final_torrent}")
    return True


def mark_uploaded(conn: sqlite3.Connection, checksums: Iterable[str]) -> None:
    conn.executemany("UPDATE import SET uploaded = 1 WHERE checksum = ?", [(checksum,) for checksum in checksums])
    conn.commit()


def main() -> None:
    args = parse_args()

    try:
        config = load_json(CONFIG_PATH, "user.json")
        site_preferences = load_json(PREFERENCES_PATH, "torrentsites.json")
    except RuntimeError as exc:
        print(exc)
        return

    if not DB_PATH.exists():
        print(f"Error: Database not found at {DB_PATH}")
        return

    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        releases = collect_releases(conn, config, args.verbose)

        if not releases:
            print("No uploads found")
            return

        if args.verbose:
            print(f"Found {len(releases)} releases to process")

        for release in releases:
            site_name = release["site"]
            site_config = config.get("torrent_sites", {}).get(site_name)
            prefs = site_preferences.get(site_name)
            if not site_config or "announcekey" not in site_config:
                print(f"Error: Missing announce key for {site_name}")
                continue
            if not prefs or "announce_url" not in prefs:
                print(f"Error: Missing announce_url for {site_name}")
                continue
            try:
                category_map = prefs["categories"]
                mapping = prefs["category_mapping"]
            except KeyError:
                print(f"Error: categories for {site_name} are not configured correctly")
                continue
            if release["type"] not in mapping:
                print(f"Error: category_mapping missing entry for {release['type']}")
                continue
            disallowed = find_disallowed(release["directory"])
            if disallowed:
                print(
                    f"Error: Skipping {release['directory'].name} because it contains disallowed files: "
                    + ", ".join(path.name for path in disallowed)
                )
                continue

            temp_root = Path(config["locations"]["temp_torrent_upload"][release["library"]]).expanduser()
            monitored_root = Path(config["locations"]["monitored_upload"][release["library"]]).expanduser()
            ensure_directory(temp_root)
            ensure_directory(monitored_root)

            torrent_name = f"{release['directory'].name}.torrent"
            temp_torrent = temp_root / torrent_name
            final_torrent = monitored_root / torrent_name

            announce_key = site_config["announcekey"]
            announce_url = prefs["announce_url"].format(announcekey=announce_key)
            if temp_torrent.exists():
                temp_torrent.unlink()
            if not create_torrent(release["directory"], announce_url, temp_torrent, args.verbose):
                continue

            if args.test:
                print(f"TEST MODE: Created torrent {torrent_name}")
                continue

            payload = build_payload(release, announce_key, mapping, category_map)
            if not upload_torrent(release, temp_torrent, payload, prefs, args.verbose):
                temp_torrent.unlink(missing_ok=True)
                continue

            if not move_torrent(temp_torrent, final_torrent, args.verbose):
                temp_torrent.unlink(missing_ok=True)
                continue

            mark_uploaded(conn, release["checksums"])
            if args.verbose:
                print(f"Uploaded {torrent_name}")


if __name__ == "__main__":
    main()
