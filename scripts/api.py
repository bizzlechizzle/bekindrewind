#!/usr/bin/env python3
"""Fetch metadata for imported media and update the online table."""

import argparse
import json
import re
import sqlite3
from html import unescape
from pathlib import Path
from typing import Dict, Iterable, Optional, Tuple

import requests

CONFIG_PATH = Path(__file__).parent.parent / "user.json"
DB_PATH = Path(__file__).parent.parent / "tapedeck.db"
TMDB_IMAGE_BASE = "https://image.tmdb.org/t/p/original"
REQUEST_TIMEOUT = 20
SUMMARY_RE = re.compile(r"<[^>]+>")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Populate metadata for imported releases")
    parser.add_argument("-v", "--verbose", action="store_true", help="Verbose output")
    return parser.parse_args()


def load_config() -> dict:
    try:
        with CONFIG_PATH.open("r", encoding="utf-8") as handle:
            return json.load(handle)
    except FileNotFoundError as exc:
        raise RuntimeError(f"Error: Missing configuration file: {CONFIG_PATH}") from exc
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Error: Failed to parse configuration: {exc}") from exc


def clean_value(value: Optional[str]) -> str:
    if value is None:
        return ""
    return str(value).strip()


def set_if_missing(updates: Dict[str, str], column: str, value: Optional[str], existing: Optional[str]) -> None:
    text = clean_value(value)
    current = clean_value(existing)
    if text and not current:
        updates[column] = text


def fetch_json(session: requests.Session, url: str, params: Optional[Dict[str, str]] = None) -> Optional[dict]:
    try:
        response = session.get(url, params=params, timeout=REQUEST_TIMEOUT)
    except requests.RequestException:
        return None
    if response.status_code != 200:
        return None
    try:
        return response.json()
    except ValueError:
        return None


def clean_summary(text: Optional[str]) -> str:
    if not text:
        return ""
    stripped = SUMMARY_RE.sub("", text)
    return unescape(stripped).strip()


def join_list(values: Iterable[str]) -> str:
    filtered = [clean_value(value) for value in values if clean_value(value)]
    return ", ".join(filtered)


def movie_needs_metadata(row: sqlite3.Row) -> bool:
    required = [row["current_dmovie"], row["current_imdb"], row["current_tmdb"]]
    return any(not clean_value(value) for value in required)


def tv_needs_metadata(row: sqlite3.Row) -> bool:
    required = [row["current_dseries"], row["current_tvmaze"], row["current_tmdb"]]
    return any(not clean_value(value) for value in required)


def tmdb_movie_details(session: requests.Session, api_key: Optional[str], title: str) -> Optional[dict]:
    if not api_key:
        return None
    params = {"api_key": api_key, "query": title}
    search = fetch_json(session, "https://api.themoviedb.org/3/search/movie", params=params)
    if not search or not search.get("results"):
        return None
    movie_id = search["results"][0].get("id")
    if not movie_id:
        return None
    detail_params = {"api_key": api_key, "append_to_response": "credits"}
    return fetch_json(session, f"https://api.themoviedb.org/3/movie/{movie_id}", params=detail_params)


def tmdb_tv_details(session: requests.Session, api_key: Optional[str], title: str) -> Optional[dict]:
    if not api_key:
        return None
    params = {"api_key": api_key, "query": title}
    search = fetch_json(session, "https://api.themoviedb.org/3/search/tv", params=params)
    if not search or not search.get("results"):
        return None
    show_id = search["results"][0].get("id")
    if not show_id:
        return None
    detail_params = {"api_key": api_key, "append_to_response": "credits,external_ids"}
    return fetch_json(session, f"https://api.themoviedb.org/3/tv/{show_id}", params=detail_params)


def omdb_lookup(session: requests.Session, api_key: Optional[str], imdb_id: Optional[str], title: str) -> Optional[dict]:
    if not api_key:
        return None
    params = {"apikey": api_key, "plot": "full"}
    if imdb_id:
        params["i"] = imdb_id
    else:
        params["t"] = title
    data = fetch_json(session, "https://www.omdbapi.com/", params=params)
    if not data or str(data.get("Response", "False")).lower() != "true":
        return None
    return data


class TvMazeCache:
    """Simple cache for TVMaze resources."""

    def __init__(self) -> None:
        self.by_title: Dict[str, dict] = {}
        self.by_id: Dict[int, dict] = {}
        self.seasons: Dict[int, Dict[int, dict]] = {}
        self.episodes: Dict[Tuple[int, int, int], Optional[dict]] = {}

    def store_show(self, show: Optional[dict]) -> None:
        if not show:
            return
        show_id = show.get("id")
        if isinstance(show_id, int):
            self.by_id[show_id] = show


def tvmaze_show(session: requests.Session, cache: TvMazeCache, title: str, tvmaze_id: Optional[str]) -> Optional[dict]:
    if tvmaze_id:
        try:
            show_id = int(tvmaze_id)
        except (TypeError, ValueError):
            show_id = None
        if show_id is not None:
            if show_id not in cache.by_id:
                show = fetch_json(
                    session,
                    f"https://api.tvmaze.com/shows/{show_id}",
                    params={"embed": "cast"},
                )
                cache.store_show(show)
            return cache.by_id.get(show_id)

    key = title.lower()
    if key not in cache.by_title:
        show = fetch_json(
            session,
            "https://api.tvmaze.com/singlesearch/shows",
            params={"q": title, "embed": "cast"},
        )
        cache.by_title[key] = show or {}
        cache.store_show(show)
    show = cache.by_title.get(key)
    return show if show else None


def tvmaze_season(session: requests.Session, cache: TvMazeCache, show_id: int, number: int) -> Optional[dict]:
    seasons = cache.seasons.setdefault(show_id, {})
    if number not in seasons:
        data = fetch_json(session, f"https://api.tvmaze.com/shows/{show_id}/seasons")
        if data:
            for season in data:
                idx = season.get("number")
                if isinstance(idx, int):
                    seasons[idx] = season
    return seasons.get(number)


def tvmaze_episode(
    session: requests.Session,
    cache: TvMazeCache,
    show_id: int,
    season: int,
    episode: int,
) -> Optional[dict]:
    key = (show_id, season, episode)
    if key not in cache.episodes:
        cache.episodes[key] = fetch_json(
            session,
            f"https://api.tvmaze.com/shows/{show_id}/episodebynumber",
            params={"season": season, "number": episode},
        )
    return cache.episodes.get(key)


def update_movie(row: sqlite3.Row, session: requests.Session, api_keys: Dict[str, str]) -> Dict[str, str]:
    title = clean_value(row["movie"]) or clean_value(row["title"]) or ""
    if not title:
        return {}

    updates: Dict[str, str] = {}
    tmdb_data = tmdb_movie_details(session, api_keys.get("TMDB"), title)
    if tmdb_data:
        set_if_missing(updates, "tmdb", str(tmdb_data.get("id")), row["current_tmdb"])
        set_if_missing(updates, "dmovie", tmdb_data.get("overview"), row["current_dmovie"])
        set_if_missing(updates, "release", tmdb_data.get("release_date"), row["current_release"])
        companies = tmdb_data.get("production_companies") or []
        if companies:
            set_if_missing(updates, "studio", companies[0].get("name"), row["current_studio"])
        genres = tmdb_data.get("genres") or []
        if genres:
            set_if_missing(updates, "genre", join_list(g.get("name") for g in genres), row["current_genre"])
        credits = tmdb_data.get("credits", {}).get("cast") or []
        if credits:
            names = [member.get("name") for member in credits if member.get("name")][:5]
            if names:
                set_if_missing(updates, "cast", ", ".join(names), row["current_cast"])
        poster = tmdb_data.get("poster_path")
        if poster:
            set_if_missing(updates, "imovie", f"{TMDB_IMAGE_BASE}{poster}", row["current_imovie"])
        imdb_id = clean_value(tmdb_data.get("imdb_id"))
        if imdb_id:
            set_if_missing(updates, "imdb", imdb_id, row["current_imdb"])
    else:
        imdb_id = ""

    imdb_id = imdb_id or clean_value(row["current_imdb"])
    omdb_data = omdb_lookup(session, api_keys.get("OMDB"), imdb_id, title)
    if omdb_data:
        set_if_missing(updates, "imdb", omdb_data.get("imdbID"), row["current_imdb"])
        set_if_missing(updates, "dmovie", omdb_data.get("Plot"), row["current_dmovie"])
        set_if_missing(updates, "release", omdb_data.get("Released"), row["current_release"])
        set_if_missing(updates, "studio", omdb_data.get("Production"), row["current_studio"])
        set_if_missing(updates, "genre", omdb_data.get("Genre"), row["current_genre"])
        set_if_missing(updates, "rating", omdb_data.get("Rated"), row["current_rating"])
        poster = clean_value(omdb_data.get("Poster"))
        if poster and poster.upper() != "N/A":
            set_if_missing(updates, "imovie", poster, row["current_imovie"])

    return updates


def update_tv(row: sqlite3.Row, session: requests.Session, api_keys: Dict[str, str], cache: TvMazeCache) -> Dict[str, str]:
    title = clean_value(row["series"]) or clean_value(row["movie"]) or clean_value(row["title"]) or ""
    if not title:
        return {}

    updates: Dict[str, str] = {}
    tmdb_data = tmdb_tv_details(session, api_keys.get("TMDB"), title)
    if tmdb_data:
        set_if_missing(updates, "tmdb", str(tmdb_data.get("id")), row["current_tmdb"])
        set_if_missing(updates, "dseries", tmdb_data.get("overview"), row["current_dseries"])
        networks = tmdb_data.get("networks") or []
        if networks:
            set_if_missing(updates, "network", networks[0].get("name"), row["current_network"])
        genres = tmdb_data.get("genres") or []
        if genres:
            set_if_missing(updates, "genre", join_list(g.get("name") for g in genres), row["current_genre"])
        credits = tmdb_data.get("credits", {}).get("cast") or []
        if credits:
            names = [member.get("name") for member in credits if member.get("name")][:5]
            if names:
                set_if_missing(updates, "cast", ", ".join(names), row["current_cast"])
        poster = tmdb_data.get("poster_path")
        if poster:
            set_if_missing(updates, "iseries", f"{TMDB_IMAGE_BASE}{poster}", row["current_iseries"])
        externals = tmdb_data.get("external_ids", {})
        set_if_missing(updates, "imdb", externals.get("imdb_id"), row["current_imdb"])
        set_if_missing(updates, "tvdb", externals.get("tvdb_id"), row["current_tvdb"])

    show = tvmaze_show(session, cache, title, clean_value(row["current_tvmaze"]))
    if show:
        show_id = show.get("id")
        set_if_missing(updates, "tvmaze", str(show_id), row["current_tvmaze"])
        set_if_missing(updates, "dseries", clean_summary(show.get("summary")), row["current_dseries"])
        genres = show.get("genres") or []
        if genres:
            set_if_missing(updates, "genre", join_list(genres), row["current_genre"])
        network = show.get("network") or show.get("webChannel") or {}
        set_if_missing(updates, "network", network.get("name"), row["current_network"])
        externals = show.get("externals", {})
        set_if_missing(updates, "imdb", externals.get("imdb"), row["current_imdb"])
        set_if_missing(updates, "tvdb", externals.get("thetvdb"), row["current_tvdb"])
        image = show.get("image") or {}
        poster = clean_value(image.get("original")) or clean_value(image.get("medium"))
        set_if_missing(updates, "iseries", poster, row["current_iseries"])
        cast = show.get("_embedded", {}).get("cast") or []
        names = [member.get("person", {}).get("name") for member in cast if member.get("person")]
        names = [name for name in names if name]
        if names:
            set_if_missing(updates, "cast", ", ".join(names[:5]), row["current_cast"])

        season_number = row["season"]
        if isinstance(season_number, int) and show_id is not None:
            season = tvmaze_season(session, cache, show_id, season_number)
            if season:
                set_if_missing(updates, "dseason", clean_summary(season.get("summary")), row["current_dseason"])
                season_image = season.get("image") or {}
                art = clean_value(season_image.get("original")) or clean_value(season_image.get("medium"))
                set_if_missing(updates, "iseason", art, row["current_iseason"])

            episode_number = row["episode"]
            if isinstance(episode_number, int):
                episode = tvmaze_episode(session, cache, show_id, season_number, episode_number)
                if episode:
                    set_if_missing(updates, "depisode", clean_summary(episode.get("summary")), row["current_depisode"])
                    set_if_missing(updates, "airdate", episode.get("airdate"), row["current_airdate"])
                    episode_image = episode.get("image") or {}
                    art = clean_value(episode_image.get("original")) or clean_value(episode_image.get("medium"))
                    set_if_missing(updates, "iepisode", art, row["current_iepisode"])

    imdb_id = clean_value(updates.get("imdb")) or clean_value(row["current_imdb"])
    omdb_data = omdb_lookup(session, api_keys.get("OMDB"), imdb_id, title)
    if omdb_data:
        set_if_missing(updates, "dseries", omdb_data.get("Plot"), row["current_dseries"])
        set_if_missing(updates, "rating", omdb_data.get("Rated"), row["current_rating"])
        poster = clean_value(omdb_data.get("Poster"))
        if poster and poster.upper() != "N/A":
            set_if_missing(updates, "iseries", poster, row["current_iseries"])
        genres = omdb_data.get("Genre")
        set_if_missing(updates, "genre", genres, row["current_genre"])

    return updates


def apply_updates(conn: sqlite3.Connection, checksum: str, updates: Dict[str, str]) -> None:
    if not updates:
        return
    assignments = ", ".join(f"{column} = ?" for column in updates)
    values = list(updates.values()) + [checksum]
    conn.execute(f"UPDATE online SET {assignments} WHERE checksum = ?", values)


def process_rows(conn: sqlite3.Connection, session: requests.Session, api_keys: Dict[str, str], verbose: bool) -> None:
    cache = TvMazeCache()
    query = """
        SELECT i.checksum, i.movie, i.series, i.season, i.episode, i.title, i.torrenttype,
               o.dmovie AS current_dmovie,
               o.release AS current_release,
               o.studio AS current_studio,
               o.dseries AS current_dseries,
               o.dseason AS current_dseason,
               o.depisode AS current_depisode,
               o.airdate AS current_airdate,
               o.network AS current_network,
               o.genre AS current_genre,
               o.rating AS current_rating,
               o.cast AS current_cast,
               o.imovie AS current_imovie,
               o.iseries AS current_iseries,
               o.iseason AS current_iseason,
               o.iepisode AS current_iepisode,
               o.imdb AS current_imdb,
               o.tmdb AS current_tmdb,
               o.tvmaze AS current_tvmaze,
               o.tvdb AS current_tvdb
        FROM import AS i
        LEFT JOIN online AS o ON o.checksum = i.checksum
    """
    for row in conn.execute(query):
        checksum = row["checksum"]
        torrent_type = clean_value(row["torrenttype"]).lower()
        if torrent_type == "movie":
            if not movie_needs_metadata(row):
                if verbose:
                    print(f"Skipping movie {checksum}: already populated")
                continue
            updates = update_movie(row, session, api_keys)
        else:
            if not tv_needs_metadata(row):
                if verbose:
                    print(f"Skipping TV {checksum}: already populated")
                continue
            updates = update_tv(row, session, api_keys, cache)

        if updates:
            if verbose:
                print(f"Updating {checksum}: {', '.join(sorted(updates))}")
            apply_updates(conn, checksum, updates)
            conn.commit()
        elif verbose:
            print(f"No updates for {checksum}")


def main() -> None:
    args = parse_args()

    try:
        config = load_config()
    except RuntimeError as exc:
        print(exc)
        return

    api_keys = {key.upper(): value for key, value in config.get("API_KEYS", {}).items() if value}
    if not api_keys:
        print("Error: No API keys configured in user.json")
        return

    if not DB_PATH.exists():
        print(f"Error: Database not found at {DB_PATH}")
        return

    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        session = requests.Session()
        try:
            process_rows(conn, session, api_keys, args.verbose)
        finally:
            session.close()


if __name__ == "__main__":
    main()
