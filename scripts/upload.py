#!/usr/bin/env python3
"""Create and upload torrents per upload.md instructions."""

from __future__ import annotations

import argparse
import json
import os
import shutil
import sqlite3
import subprocess
from collections import Counter, defaultdict
from contextlib import ExitStack
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

import requests

CONFIG_PATH = Path(__file__).parent.parent / "user.json"
TORRENT_SITES_PATH = Path(__file__).parent.parent / "preferences" / "torrentsites.json"
DB_PATH = Path(__file__).parent.parent / "tapedeck.db"

ALLOWED_TORRENT_TYPES = {"movie", "series", "season", "episode"}
DEFAULT_CATEGORY_FALLBACKS = {
    "movie": "movies_webRip",
    "series": "tv_boxsets",
    "season": "tv_boxsets",
    "episode": "tv_episodes_hd",
}
DISALLOWED_UPLOAD_NAMES = (".ds_store", "tests")


class UploadError(Exception):
    """Raised when an unrecoverable error occurs during upload."""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Create torrents and upload them")
    parser.add_argument("-v", "--verbose", action="store_true", help="Verbose output")
    parser.add_argument(
        "-t",
        "--test",
        action="store_true",
        help="Test mode. Create torrents but do not upload.",
    )
    return parser.parse_args()


def load_json(path: Path, description: str) -> Dict:
    try:
        with path.open("r", encoding="utf-8") as handle:
            return json.load(handle)
    except FileNotFoundError:
        raise UploadError(f"Error: Missing {description}: {path}")
    except json.JSONDecodeError as exc:
        raise UploadError(f"Error: Failed to parse {description}: {exc}")


def safe_resolve(path: Path) -> Path:
    try:
        return path.resolve()
    except FileNotFoundError:
        return Path(os.path.abspath(path))


def is_relative_to(path: Path, root: Path) -> bool:
    try:
        safe_resolve(path).relative_to(safe_resolve(root))
        return True
    except ValueError:
        return False


def first_non_empty(values: Iterable[Optional[str]]) -> Optional[str]:
    for value in values:
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    return None


def most_common_non_empty(values: Iterable[Optional[str]]) -> Optional[str]:
    filtered = [str(value).strip().upper() for value in values if value]
    if not filtered:
        return None
    return Counter(filtered).most_common(1)[0][0]


def build_upload_roots(config: Dict) -> Dict[str, Path]:
    locations = config.get("locations", {})
    file_upload = locations.get("file_upload", {})
    required = {"movies", "tv_shows"}
    if not required.issubset(file_upload):
        missing = ", ".join(sorted(required - set(file_upload)))
        raise UploadError(f"Error: Missing file_upload locations for: {missing}")
    roots = {key: Path(path).expanduser() for key, path in file_upload.items()}
    for root in roots.values():
        root.mkdir(parents=True, exist_ok=True)
    return roots


def determine_content_type(directory: Path, roots: Dict[str, Path]) -> Tuple[Optional[str], Optional[Path]]:
    for content_type, root in roots.items():
        if is_relative_to(directory, root):
            return content_type, root
    return None, None


def locate_release_directory(file_path: Path, roots: Dict[str, Path]) -> Tuple[Optional[Path], Optional[str]]:
    candidate = file_path if file_path.is_dir() else file_path.parent
    content_type, boundary = determine_content_type(candidate, roots)
    if boundary is None:
        return None, None

    for directory in [candidate, *candidate.parents]:
        if not is_relative_to(directory, boundary):
            break
        nfo_candidate = directory / f"{directory.name}.nfo"
        if nfo_candidate.exists():
            return directory, content_type
    return (candidate if candidate.exists() else None), content_type


def determine_release_type(rows: List[sqlite3.Row], default_type: str) -> str:
    values = [str(row["torrenttype"]).lower() for row in rows if row["torrenttype"]]
    if not values:
        return default_type
    release_type = Counter(values).most_common(1)[0][0]
    if release_type not in ALLOWED_TORRENT_TYPES:
        return default_type
    return release_type


def collect_releases(
    conn: sqlite3.Connection,
    config: Dict,
    verbose: bool,
) -> List[Dict]:
    default_type = config.get("default", {}).get("torrenttype", "season").lower()
    upload_roots = build_upload_roots(config)

    query = """
        SELECT i.*, o.imdb, o.tvmaze, o.tmdb, o.tvdb
        FROM import AS i
        LEFT JOIN online AS o USING (checksum)
        WHERE i.newloc IS NOT NULL
          AND i.newloc != ''
          AND (i.uploaded IS NULL OR i.uploaded = 0)
    """
    rows = conn.execute(query).fetchall()
    if not rows:
        return []

    grouped: Dict[Path, List[sqlite3.Row]] = defaultdict(list)
    content_types: Dict[Path, str] = {}

    for row in rows:
        newloc = row["newloc"]
        if not newloc:
            continue
        release_dir, content_type = locate_release_directory(Path(newloc), upload_roots)
        if not release_dir or not content_type:
            if verbose:
                print(f"Skipping unmatched release path: {newloc}")
            continue
        if not release_dir.exists():
            if verbose:
                print(f"Skipping missing release folder: {release_dir}")
            continue
        grouped[release_dir].append(row)
        content_types[release_dir] = content_type

    releases: List[Dict] = []
    for release_dir, group in grouped.items():
        release_type = determine_release_type(group, default_type)
        torrentsite = first_non_empty(row["torrentsite"] for row in group)
        torrentsite = (torrentsite or config.get("default", {}).get("torrentsite", "torrentleech")).lower()
        imdb = first_non_empty(row["imdb"] for row in group)
        tvmaze = first_non_empty(row["tvmaze"] for row in group)
        resolution = most_common_non_empty(row["resolution"] for row in group)
        releases.append(
            {
                "directory": release_dir,
                "content_type": content_types[release_dir],
                "release_type": release_type,
                "torrentsite": torrentsite,
                "checksums": [row["checksum"] for row in group],
                "imdb": imdb,
                "tvmaze": tvmaze,
                "resolution": resolution,
            }
        )

    return sorted(releases, key=release_sort_key)


def release_sort_key(release: Dict) -> Tuple[float, str]:
    directory: Path = release["directory"]
    try:
        mtime = directory.stat().st_mtime
    except OSError:
        mtime = 0.0
    return (mtime, str(directory))


def find_disallowed_entries(directory: Path) -> List[Path]:
    matches: List[Path] = []
    for candidate in directory.rglob("*"):
        name = candidate.name.lower()
        if name not in DISALLOWED_UPLOAD_NAMES:
            continue
        try:
            candidate.relative_to(directory)
        except ValueError:
            continue
        matches.append(candidate)
    return sorted(matches, key=lambda path: str(path))


def ensure_directories(paths: Iterable[Path]) -> None:
    for path in paths:
        path.mkdir(parents=True, exist_ok=True)


def build_announce_key(config: Dict, site_name: str) -> str:
    site_config = config.get("torrent_sites", {}).get(site_name)
    if not site_config:
        raise UploadError(f"Error: Missing announce key for site '{site_name}' in user.json")
    announce_key = site_config.get("announcekey")
    if not announce_key:
        raise UploadError(f"Error: Announce key missing for site '{site_name}'")
    return announce_key


def build_announce_url(site_prefs: Dict, announce_key: str) -> str:
    template = site_prefs.get("announce_url")
    if not template:
        raise UploadError("Error: Missing announce_url in torrentsites.json")
    return template.format(announcekey=announce_key)


def resolve_category(release: Dict, site_prefs: Dict) -> int:
    categories = site_prefs.get("categories", {})
    mapping = site_prefs.get("category_mapping", {})
    release_type = release["release_type"]

    category_key = mapping.get(release_type)
    if release_type == "movie" and not category_key:
        if release.get("resolution") == "2160P":
            category_key = "movies_4k"
    if not category_key:
        category_key = DEFAULT_CATEGORY_FALLBACKS.get(release_type, "tv_boxsets")

    category_value = categories.get(category_key)
    if category_value is None:
        available = ", ".join(sorted(categories))
        raise UploadError(
            f"Error: Category '{category_key}' missing for release type '{release_type}'. Available: {available}"
        )
    return int(category_value)


def build_upload_payload(
    release: Dict,
    announce_key: str,
    site_prefs: Dict,
) -> Dict[str, str]:
    payload = {
        "announcekey": announce_key,
        "category": str(resolve_category(release, site_prefs)),
    }

    if release.get("imdb"):
        payload["imdb"] = release["imdb"]

    if release.get("tvmaze") and release["release_type"] != "movie":
        payload["tvmaze"] = release["tvmaze"]
        payload["tvmazetype"] = "2" if release["release_type"] == "episode" else "1"

    return payload


def textual_nfo_fallback(release: Dict) -> str:
    lines = [
        f"Release: {release['directory'].name}",
        f"Type: {release['release_type']}",
        "Source: upload.py",
    ]
    if release.get("imdb"):
        lines.append(f"IMDB: {release['imdb']}")
    if release.get("tvmaze"):
        lines.append(f"TVMaze: {release['tvmaze']}")
    return "\n".join(lines)


def create_torrent(source: Path, announce_url: str, output_path: Path, verbose: bool) -> bool:
    if verbose:
        print(f"Creating torrent for: {source}")
    cmd = ["mktorrent", "-a", announce_url, "-o", str(output_path), str(source)]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"Error: mktorrent failed for {source}")
        if verbose and result.stderr:
            print(result.stderr.strip())
        return False
    return True


def upload_release(
    release: Dict,
    torrent_path: Path,
    site_prefs: Dict,
    payload: Dict[str, str],
    session: requests.Session,
    verbose: bool,
) -> bool:
    upload_url = site_prefs.get("upload_url")
    if not upload_url:
        print("Error: upload_url missing in torrentsites.json")
        return False

    files: Dict[str, Tuple[str, object, str]] = {}
    with ExitStack() as stack:
        torrent_file = stack.enter_context(torrent_path.open("rb"))
        files["torrent"] = (torrent_path.name, torrent_file, "application/x-bittorrent")
        nfo_path = release["directory"] / f"{release['directory'].name}.nfo"
        if nfo_path.exists():
            nfo_file = stack.enter_context(nfo_path.open("rb"))
            files["nfo"] = (nfo_path.name, nfo_file, "text/plain")
        else:
            payload["description"] = textual_nfo_fallback(release)

        try:
            response = session.post(upload_url, data=payload, files=files, timeout=60)
        except requests.RequestException as exc:
            print(f"Error: Upload failed for {release['directory'].name}: {exc}")
            return False

    if verbose:
        print(f"Upload response: {response.status_code}")
    if not response.ok:
        print(f"Error: Upload rejected for {release['directory'].name}")
        if verbose and response.text:
            print(response.text.strip())
        return False

    if verbose and response.text:
        print(response.text.strip())
    return True


def move_torrent(temp_torrent: Path, final_torrent: Path, verbose: bool) -> bool:
    if verbose:
        print(f"Moving torrent to monitored folder: {final_torrent}")
    ensure_directories([final_torrent.parent])
    try:
        shutil.move(str(temp_torrent), str(final_torrent))
    except OSError as exc:
        print(f"Error: Unable to move torrent to monitored folder: {exc}")
        return False
    return True


def mark_uploaded(conn: sqlite3.Connection, checksums: Iterable[str]) -> None:
    conn.executemany(
        "UPDATE import SET uploaded = 1 WHERE checksum = ?",
        [(checksum,) for checksum in checksums],
    )
    conn.commit()


def process_release(
    release: Dict,
    config: Dict,
    torrent_sites: Dict,
    session: requests.Session,
    args: argparse.Namespace,
    conn: sqlite3.Connection,
) -> bool:
    site_name = release["torrentsite"]
    site_prefs = torrent_sites.get(site_name)
    if not site_prefs:
        print(f"Error: Unknown torrent site '{site_name}' for {release['directory'].name}")
        return False

    announce_key = build_announce_key(config, site_name)
    announce_url = build_announce_url(site_prefs, announce_key)

    locations = config.get("locations", {})
    temp_config = locations.get("temp_torrent_upload", {})
    monitored_config = locations.get("monitored_upload", {})
    temp_path = temp_config.get(release["content_type"])
    monitored_path = monitored_config.get(release["content_type"])
    if not temp_path:
        print(f"Error: temp_torrent_upload path missing for {release['content_type']}")
        return False
    if not monitored_path:
        print(f"Error: monitored_upload path missing for {release['content_type']}")
        return False

    temp_base = Path(temp_path).expanduser()
    monitored_base = Path(monitored_path).expanduser()
    ensure_directories([temp_base, monitored_base])

    torrent_name = f"{release['directory'].name}.torrent"
    temp_torrent = temp_base / torrent_name
    final_torrent = monitored_base / torrent_name

    if temp_torrent.exists():
        try:
            temp_torrent.unlink()
        except OSError as exc:
            print(f"Error: Unable to remove existing torrent {temp_torrent}: {exc}")
            return False

    disallowed = find_disallowed_entries(release["directory"])
    if disallowed:
        relative = ", ".join(str(path.relative_to(release["directory"])) for path in disallowed)
        print(
            "Error: Skipping {name} because it contains disallowed files: {items}".format(
                name=release["directory"].name,
                items=relative,
            )
        )
        return False

    if not create_torrent(release["directory"], announce_url, temp_torrent, args.verbose):
        return False

    if args.test:
        print(f"TEST MODE: Created torrent {torrent_name}")
        return True

    payload = build_upload_payload(release, announce_key, site_prefs)
    if not upload_release(release, temp_torrent, site_prefs, payload, session, args.verbose):
        try:
            temp_torrent.unlink()
        except FileNotFoundError:
            pass
        except OSError as exc:
            print(f"Warning: Failed to remove temporary torrent {temp_torrent}: {exc}")
        return False

    if not move_torrent(temp_torrent, final_torrent, args.verbose):
        return False
    mark_uploaded(conn, release["checksums"])
    if args.verbose:
        print(f"Uploaded: {torrent_name}")
    return True


def main() -> None:
    args = parse_args()

    try:
        config = load_json(CONFIG_PATH, "user.json")
        torrent_sites = load_json(TORRENT_SITES_PATH, "torrentsites.json")
    except UploadError as exc:
        print(exc)
        return

    if not DB_PATH.exists():
        print(f"Error: Database not found at {DB_PATH}")
        return

    session = requests.Session()
    try:
        with sqlite3.connect(str(DB_PATH)) as conn:
            conn.row_factory = sqlite3.Row
            releases = collect_releases(conn, config, args.verbose)
            if not releases:
                print("No uploads found")
                return

            if args.verbose:
                print(f"Found {len(releases)} releases to process")

            success = 0
            for release in releases:
                try:
                    if process_release(release, config, torrent_sites, session, args, conn):
                        success += 1
                except UploadError as exc:
                    print(exc)
                except KeyboardInterrupt:
                    raise
                except Exception as exc:
                    print(f"Error processing {release['directory'].name}: {exc}")
            if args.verbose:
                print(f"Completed uploads: {success}/{len(releases)}")
    finally:
        session.close()


if __name__ == "__main__":
    main()
