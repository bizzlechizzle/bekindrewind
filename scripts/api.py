#!/usr/bin/env python3
"""Fetch metadata for imported media and update the online table."""

import argparse
import json
import re
import sqlite3
import time
from html import unescape
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

import requests

CONFIG_PATH = Path(__file__).parent.parent / "user.json"
DB_PATH = Path(__file__).parent.parent / "tapedeck.db"
TMDB_IMAGE_BASE = "https://image.tmdb.org/t/p/original"
REQUEST_TIMEOUT = 20
SUMMARY_RE = re.compile(r"<[^>]+>")
TVDB_API = "https://api4.thetvdb.com/v4"

ORIGIN_DOMAINS = {
    "amazon": ("amazon.com", "primevideo.com", "images-amazon.com"),
    "amazon prime": ("amazon.com", "primevideo.com", "images-amazon.com"),
    "primevideo": ("amazon.com", "primevideo.com", "images-amazon.com"),
    "hbo": ("hbo.com", "hbomax.com", "max.com"),
    "max": ("hbo.com", "hbomax.com", "max.com"),
    "hbo max": ("hbo.com", "hbomax.com", "max.com"),
    "youtube": ("youtube.com", "ytimg.com"),
}


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


def clean_summary(text: Optional[str]) -> str:
    if not text:
        return ""
    stripped = SUMMARY_RE.sub("", text)
    return unescape(stripped).strip()


def call_json(
    session: requests.Session,
    method: str,
    url: str,
    *,
    params: Optional[Dict[str, str]] = None,
    json_body: Optional[dict] = None,
    headers: Optional[Dict[str, str]] = None,
) -> Optional[dict]:
    try:
        response = session.request(
            method,
            url,
            params=params,
            json=json_body,
            headers=headers,
            timeout=REQUEST_TIMEOUT,
        )
    except requests.RequestException:
        return None
    if response.status_code != 200:
        return None
    try:
        return response.json()
    except ValueError:
        return None


def choose_result(results: List[dict], target: str) -> Optional[dict]:
    if not results:
        return None
    if len(results) == 1:
        return results[0]

    normalized_target = re.sub(r"[^\w\s]", "", target.lower())
    best: Optional[dict] = None
    best_score = -1

    for item in results:
        name = item.get("name") or item.get("title") or item.get("original_name") or item.get("original_title") or ""
        normalized_name = re.sub(r"[^\w\s]", "", name.lower())
        score = 0

        if normalized_name == normalized_target:
            score += 100
        elif normalized_target and normalized_target in normalized_name:
            score += 60
        else:
            matches = sum(1 for word in normalized_target.split() if word and word in normalized_name)
            score += matches * 15

        score += int(item.get("vote_count", 0))
        score += int(item.get("popularity", 0))

        if score > best_score:
            best = item
            best_score = score

    return best


def join_list(values: Iterable[str]) -> str:
    unique: List[str] = []
    seen = set()
    for value in values:
        text = clean_value(value)
        if not text:
            continue
        lowered = text.lower()
        if lowered in seen:
            continue
        seen.add(lowered)
        unique.append(text)
    return ", ".join(unique)


def should_preserve_image(existing: str, source: str) -> bool:
    if not existing or not source:
        return False
    domains = ORIGIN_DOMAINS.get(source.lower())
    if not domains:
        return False
    lowered = existing.lower()
    return any(domain in lowered for domain in domains)


class TvMazeCache:
    """Cache TVMaze lookups so we do minimal HTTP requests."""

    def __init__(self) -> None:
        self.show_by_title: Dict[str, dict] = {}
        self.show_by_id: Dict[int, dict] = {}
        self.seasons: Dict[int, Dict[int, dict]] = {}
        self.episodes: Dict[Tuple[int, int, int], Optional[dict]] = {}

    def store_show(self, show: Optional[dict]) -> None:
        if not show:
            return
        identifier = show.get("id")
        if isinstance(identifier, int):
            self.show_by_id[identifier] = show


def tvmaze_show(
    session: requests.Session,
    cache: TvMazeCache,
    title: str,
    tvmaze_id: Optional[str],
) -> Optional[dict]:
    if tvmaze_id:
        try:
            show_id = int(tvmaze_id)
        except (TypeError, ValueError):
            show_id = None
        if show_id is not None:
            if show_id not in cache.show_by_id:
                data = call_json(session, "get", f"https://api.tvmaze.com/shows/{show_id}", params={"embed": "cast"})
                cache.store_show(data)
            return cache.show_by_id.get(show_id)

    key = title.lower()
    if key not in cache.show_by_title:
        data = call_json(
            session,
            "get",
            "https://api.tvmaze.com/singlesearch/shows",
            params={"q": title, "embed": "cast"},
        )
        cache.show_by_title[key] = data or {}
        cache.store_show(data)
    show = cache.show_by_title.get(key)
    return show if show else None


def tvmaze_season(
    session: requests.Session,
    cache: TvMazeCache,
    show_id: int,
    number: int,
) -> Optional[dict]:
    seasons = cache.seasons.setdefault(show_id, {})
    if number not in seasons:
        data = call_json(session, "get", f"https://api.tvmaze.com/shows/{show_id}/seasons") or []
        for entry in data:
            try:
                idx = int(entry.get("number"))
            except (TypeError, ValueError):
                continue
            seasons[idx] = entry
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
        cache.episodes[key] = call_json(
            session,
            "get",
            f"https://api.tvmaze.com/shows/{show_id}/episodebynumber",
            params={"season": season, "number": episode},
        )
    return cache.episodes.get(key)


class TvdbClient:
    """Minimal TVDB v4 client; silently fails when the API is unreachable."""

    def __init__(self, api_key: Optional[str], session: requests.Session) -> None:
        self.api_key = clean_value(api_key)
        self.session = session
        self._token: Optional[str] = None
        self._token_timestamp: float = 0.0

    def _ensure_token(self) -> Optional[str]:
        if not self.api_key:
            return None
        now = time.time()
        if self._token and now - self._token_timestamp < 3600:
            return self._token
        data = call_json(self.session, "post", f"{TVDB_API}/login", json_body={"apikey": self.api_key})
        if not data:
            return None
        token = data.get("data", {}).get("token")
        if not token:
            return None
        self._token = token
        self._token_timestamp = now
        return token

    def _authorized_headers(self) -> Optional[Dict[str, str]]:
        token = self._ensure_token()
        if not token:
            return None
        return {"Authorization": f"Bearer {token}"}

    def search_series(self, title: str) -> Optional[dict]:
        headers = self._authorized_headers()
        if not headers or not title:
            return None
        data = call_json(
            self.session,
            "get",
            f"{TVDB_API}/search",
            params={"query": title, "type": "series"},
            headers=headers,
        )
        if not data:
            return None
        results = data.get("data") or []
        if not results:
            return None
        best = choose_result(results, title)
        return best or results[0]

    def series_details(self, series_id: Optional[int]) -> Optional[dict]:
        headers = self._authorized_headers()
        if not headers or series_id is None:
            return None
        data = call_json(
            self.session,
            "get",
            f"{TVDB_API}/series/{series_id}",
            headers=headers,
        )
        if not data:
            return None
        return data.get("data")


def tmdb_movie_details(
    session: requests.Session,
    api_key: Optional[str],
    title: str,
    tmdb_id: Optional[str],
) -> Tuple[Optional[str], Optional[dict]]:
    api_key = clean_value(api_key)
    if not api_key:
        return None, None

    if tmdb_id:
        params = {"api_key": api_key, "append_to_response": "credits,external_ids"}
        data = call_json(session, "get", f"https://api.themoviedb.org/3/movie/{tmdb_id}", params=params)
        if data:
            return str(data.get("id")), data

    if not title:
        return None, None

    params = {"api_key": api_key, "query": title}
    search = call_json(session, "get", "https://api.themoviedb.org/3/search/movie", params=params)
    if not search:
        return None, None
    best = choose_result(search.get("results") or [], title)
    if not best:
        return None, None
    movie_id = best.get("id")
    if not movie_id:
        return None, None
    params = {"api_key": api_key, "append_to_response": "credits,external_ids"}
    detail = call_json(session, "get", f"https://api.themoviedb.org/3/movie/{movie_id}", params=params)
    if not detail:
        return None, None
    return str(detail.get("id")), detail


def tmdb_tv_details(
    session: requests.Session,
    api_key: Optional[str],
    title: str,
    tmdb_id: Optional[str],
) -> Tuple[Optional[str], Optional[dict]]:
    api_key = clean_value(api_key)
    if not api_key:
        return None, None

    if tmdb_id:
        params = {"api_key": api_key, "append_to_response": "credits,external_ids"}
        data = call_json(session, "get", f"https://api.themoviedb.org/3/tv/{tmdb_id}", params=params)
        if data:
            return str(data.get("id")), data

    if not title:
        return None, None

    params = {"api_key": api_key, "query": title}
    search = call_json(session, "get", "https://api.themoviedb.org/3/search/tv", params=params)
    if not search:
        return None, None
    best = choose_result(search.get("results") or [], title)
    if not best:
        return None, None
    show_id = best.get("id")
    if not show_id:
        return None, None
    params = {"api_key": api_key, "append_to_response": "credits,external_ids"}
    detail = call_json(session, "get", f"https://api.themoviedb.org/3/tv/{show_id}", params=params)
    if not detail:
        return None, None
    return str(detail.get("id")), detail


def omdb_lookup(
    session: requests.Session,
    api_key: Optional[str],
    imdb_id: Optional[str],
    title: str,
) -> Optional[dict]:
    api_key = clean_value(api_key)
    if not api_key:
        return None
    params = {"apikey": api_key, "plot": "full"}
    if imdb_id:
        params["i"] = imdb_id
    elif title:
        params["t"] = title
    else:
        return None
    data = call_json(session, "get", "https://www.omdbapi.com/", params=params)
    if not data or str(data.get("Response", "False")).lower() != "true":
        return None
    return data


def prefer_text(existing: str, candidate: str, *, min_gain: int = 25) -> Optional[str]:
    candidate_clean = clean_value(candidate)
    if not candidate_clean:
        return None
    existing_clean = clean_value(existing)
    if not existing_clean:
        return candidate_clean
    if candidate_clean == existing_clean:
        return None
    if len(candidate_clean) >= len(existing_clean) + min_gain:
        return candidate_clean
    return None


def prefer_simple(existing: str, candidate: str) -> Optional[str]:
    candidate_clean = clean_value(candidate)
    if not candidate_clean:
        return None
    existing_clean = clean_value(existing)
    if not existing_clean or candidate_clean != existing_clean:
        return candidate_clean
    return None


def prefer_list(existing: str, values: Iterable[str]) -> Optional[str]:
    cleaned: List[str] = []
    for value in values:
        text = clean_value(value)
        if text:
            cleaned.append(text)
    candidate = join_list(cleaned)
    if not candidate:
        return None
    existing_clean = clean_value(existing)
    if not existing_clean:
        return candidate
    existing_set = {item.strip().lower() for item in existing_clean.split(",") if item.strip()}
    candidate_set = {item.strip().lower() for item in candidate.split(",") if item.strip()}
    if candidate_set.difference(existing_set):
        return candidate
    if len(candidate_set) > len(existing_set):
        return candidate
    return None


def prefer_image(existing: str, candidate: str, source: str) -> Optional[str]:
    candidate_clean = clean_value(candidate)
    if not candidate_clean or not candidate_clean.lower().startswith("http"):
        return None
    existing_clean = clean_value(existing)
    if existing_clean:
        if existing_clean == candidate_clean:
            return None
        if should_preserve_image(existing_clean, source):
            return None
    return candidate_clean


def prioritized_summary(*values: Optional[str]) -> str:
    for value in values:
        text = clean_summary(value)
        if text:
            return text
    return ""


def build_column_query(import_cols: set, online_cols: set) -> Tuple[str, List[str]]:
    select_fields = ["i.checksum"]
    selected_aliases: List[str] = ["checksum"]

    def add_import(column: str, alias: Optional[str] = None) -> None:
        name = alias or column
        if column in import_cols:
            select_fields.append(f"i.{column} AS {name}")
            selected_aliases.append(name)

    def add_online(column: str) -> None:
        alias = f"current_{column}"
        if column in online_cols:
            select_fields.append(f"o.{column} AS {alias}")
            selected_aliases.append(alias)

    for column in ("movie", "series", "season", "episode", "title", "torrenttype", "dlsource", "imdb", "tmdb", "tvmaze", "tvdb"):
        add_import(column, f"import_{column}")

    for column in (
        "dmovie",
        "release",
        "studio",
        "dseries",
        "dseason",
        "depisode",
        "airdate",
        "network",
        "genre",
        "rating",
        "cast",
        "imovie",
        "iseries",
        "iseason",
        "iepisode",
        "imdb",
        "tmdb",
        "tvmaze",
        "tvdb",
    ):
        add_online(column)

    query = " "
    query = "SELECT " + ", ".join(select_fields) + " FROM import AS i LEFT JOIN online AS o ON o.checksum = i.checksum"
    return query, selected_aliases


def table_columns(conn: sqlite3.Connection, table: str) -> set:
    columns = set()
    for row in conn.execute(f"PRAGMA table_info({table})"):
        columns.add(row[1])
    return columns


def update_tables(
    conn: sqlite3.Connection,
    checksum: str,
    updates: Dict[str, str],
    id_updates: Dict[str, str],
    import_cols: set,
) -> None:
    if updates:
        assignments = ", ".join(f"{column} = ?" for column in updates)
        values = list(updates.values()) + [checksum]
        conn.execute(f"UPDATE online SET {assignments} WHERE checksum = ?", values)
    if id_updates:
        id_assignments = {key: value for key, value in id_updates.items() if key in {"imdb", "tmdb", "tvmaze", "tvdb"}}
        if id_assignments:
            assignments = ", ".join(f"{column} = ?" for column in id_assignments)
            values = list(id_assignments.values()) + [checksum]
            conn.execute(f"UPDATE online SET {assignments} WHERE checksum = ?", values)
        import_updates = {key: value for key, value in id_updates.items() if key in import_cols}
        if import_updates:
            assignments = ", ".join(f"{column} = ?" for column in import_updates)
            values = list(import_updates.values()) + [checksum]
            conn.execute(f"UPDATE import SET {assignments} WHERE checksum = ?", values)


def gather_ids(row: Dict[str, str]) -> Dict[str, str]:
    ids: Dict[str, str] = {}
    for key in ("imdb", "tmdb", "tvmaze", "tvdb"):
        ids[key] = clean_value(row.get(f"current_{key}")) or clean_value(row.get(f"import_{key}"))
    return ids


def update_movie_metadata(
    row: Dict[str, str],
    session: requests.Session,
    api_keys: Dict[str, str],
) -> Tuple[Dict[str, str], Dict[str, str]]:
    title = clean_value(row.get("import_movie")) or clean_value(row.get("import_title"))
    if not title:
        return {}, {}

    ids = gather_ids(row)
    updates: Dict[str, str] = {}
    id_updates: Dict[str, str] = {}

    tmdb_id, tmdb_data = tmdb_movie_details(session, api_keys.get("TMDB"), title, ids.get("tmdb"))
    if tmdb_id and tmdb_id != ids.get("tmdb"):
        ids["tmdb"] = tmdb_id
        id_updates["tmdb"] = tmdb_id
    if tmdb_data:
        imdb_from_tmdb = clean_value(tmdb_data.get("imdb_id"))
        if imdb_from_tmdb and imdb_from_tmdb != ids.get("imdb"):
            ids["imdb"] = imdb_from_tmdb
            id_updates["imdb"] = imdb_from_tmdb

    omdb_data = omdb_lookup(session, api_keys.get("OMDB"), ids.get("imdb"), title)

    existing = row.get
    candidate = prefer_text(existing("current_dmovie", ""), (tmdb_data or {}).get("overview"))
    if not candidate and omdb_data:
        candidate = prefer_text(existing("current_dmovie", ""), omdb_data.get("Plot"), min_gain=15)
    if candidate:
        updates["dmovie"] = candidate

    candidate = prefer_simple(existing("current_release", ""), (tmdb_data or {}).get("release_date"))
    if not candidate and omdb_data:
        candidate = prefer_simple(existing("current_release", ""), omdb_data.get("Released"))
    if candidate:
        updates["release"] = candidate

    companies = (tmdb_data or {}).get("production_companies") or []
    studio = companies[0].get("name") if companies else ""
    candidate = prefer_simple(existing("current_studio", ""), studio)
    if candidate:
        updates["studio"] = candidate

    genres: List[str] = []
    genres.extend([genre.get("name", "") for genre in (tmdb_data or {}).get("genres") or []])
    if omdb_data and clean_value(omdb_data.get("Genre")) and omdb_data.get("Genre") != "N/A":
        genres.extend(name.strip() for name in omdb_data["Genre"].split(","))
    candidate = prefer_list(existing("current_genre", ""), genres)
    if candidate:
        updates["genre"] = candidate

    if omdb_data and clean_value(omdb_data.get("Rated")) and omdb_data.get("Rated") != "N/A":
        candidate = prefer_simple(existing("current_rating", ""), omdb_data.get("Rated"))
        if candidate:
            updates["rating"] = candidate

    tmdb_cast = []
    for member in (tmdb_data or {}).get("credits", {}).get("cast", [])[:5]:
        name = member.get("name")
        if name:
            tmdb_cast.append(name)
    if omdb_data and clean_value(omdb_data.get("Actors")) and omdb_data.get("Actors") != "N/A":
        tmdb_cast.extend(name.strip() for name in omdb_data.get("Actors", "").split(","))
    candidate = prefer_list(existing("current_cast", ""), tmdb_cast)
    if candidate:
        updates["cast"] = candidate

    poster_path = clean_value((tmdb_data or {}).get("poster_path"))
    poster = f"{TMDB_IMAGE_BASE}{poster_path}" if poster_path else ""
    source = clean_value(row.get("import_dlsource"))
    candidate = prefer_image(existing("current_imovie", ""), poster, source)
    if not candidate and omdb_data:
        poster = clean_value(omdb_data.get("Poster"))
        if poster.upper() != "N/A":
            candidate = prefer_image(existing("current_imovie", ""), poster, source)
    if candidate:
        updates["imovie"] = candidate

    if ids.get("imdb"):
        id_updates.setdefault("imdb", ids["imdb"])

    return updates, id_updates


def update_tv_metadata(
    row: Dict[str, str],
    session: requests.Session,
    api_keys: Dict[str, str],
    cache: TvMazeCache,
    tvdb_client: Optional[TvdbClient],
) -> Tuple[Dict[str, str], Dict[str, str]]:
    title = clean_value(row.get("import_series")) or clean_value(row.get("import_movie")) or clean_value(row.get("import_title"))
    if not title:
        return {}, {}

    ids = gather_ids(row)
    updates: Dict[str, str] = {}
    id_updates: Dict[str, str] = {}

    show = tvmaze_show(session, cache, title, ids.get("tvmaze"))
    if show:
        show_id = show.get("id")
        if show_id and str(show_id) != ids.get("tvmaze"):
            ids["tvmaze"] = str(show_id)
            id_updates["tvmaze"] = str(show_id)
    else:
        show_id = None

    tmdb_id, tmdb_data = tmdb_tv_details(session, api_keys.get("TMDB"), title, ids.get("tmdb"))
    if tmdb_id and tmdb_id != ids.get("tmdb"):
        ids["tmdb"] = tmdb_id
        id_updates["tmdb"] = tmdb_id

    tvdb_data = None
    tvdb_series_id = None
    if tvdb_client:
        if ids.get("tvdb"):
            try:
                tvdb_series_id = int(ids["tvdb"])
            except ValueError:
                tvdb_series_id = None
        if tvdb_series_id is None:
            series = tvdb_client.search_series(title)
            if series:
                tvdb_series_id = series.get("id")
                if tvdb_series_id:
                    ids["tvdb"] = str(tvdb_series_id)
                    id_updates["tvdb"] = str(tvdb_series_id)
        if tvdb_series_id is not None:
            tvdb_data = tvdb_client.series_details(tvdb_series_id)

    imdb_id = ids.get("imdb")
    externals = (tmdb_data or {}).get("external_ids", {})
    if not imdb_id:
        imdb_id = clean_value(externals.get("imdb_id"))
    if not imdb_id and show:
        imdb_id = clean_value(show.get("externals", {}).get("imdb"))
    if imdb_id and imdb_id != ids.get("imdb"):
        ids["imdb"] = imdb_id
        id_updates["imdb"] = imdb_id

    omdb_data = omdb_lookup(session, api_keys.get("OMDB"), imdb_id, title)

    current = row.get
    series_summary = prioritized_summary(
        (tvdb_data or {}).get("overview") if tvdb_data else None,
        show.get("summary") if show else None,
        (omdb_data or {}).get("Plot") if omdb_data else None,
        (tmdb_data or {}).get("overview") if tmdb_data else None,
    )
    candidate = prefer_text(current("current_dseries", ""), series_summary, min_gain=10)
    if candidate:
        updates["dseries"] = candidate

    season_number = row.get("import_season")
    try:
        season_number_int = int(season_number) if season_number is not None else None
    except (TypeError, ValueError):
        season_number_int = None

    episode_number = row.get("import_episode")
    try:
        episode_number_int = int(episode_number) if episode_number is not None else None
    except (TypeError, ValueError):
        episode_number_int = None

    source = clean_value(row.get("import_dlsource"))

    if show_id and season_number_int is not None:
        season = tvmaze_season(session, cache, show_id, season_number_int)
    else:
        season = None
    if season:
        season_summary = prioritized_summary(season.get("summary"))
        candidate = prefer_text(current("current_dseason", ""), season_summary, min_gain=10)
        if candidate:
            updates["dseason"] = candidate
        season_image = season.get("image") or {}
        art = clean_value(season_image.get("original")) or clean_value(season_image.get("medium"))
        candidate = prefer_image(current("current_iseason", ""), art, source)
        if candidate:
            updates["iseason"] = candidate

    if show_id and season_number_int is not None and episode_number_int is not None:
        episode = tvmaze_episode(session, cache, show_id, season_number_int, episode_number_int)
    else:
        episode = None
    if episode:
        episode_summary = prioritized_summary(episode.get("summary"))
        candidate = prefer_text(current("current_depisode", ""), episode_summary, min_gain=10)
        if candidate:
            updates["depisode"] = candidate
        candidate = prefer_simple(current("current_airdate", ""), episode.get("airdate"))
        if candidate:
            updates["airdate"] = candidate
        ep_image = episode.get("image") or {}
        art = clean_value(ep_image.get("original")) or clean_value(ep_image.get("medium"))
        candidate = prefer_image(current("current_iepisode", ""), art, source)
        if candidate:
            updates["iepisode"] = candidate

    networks: List[str] = []
    if tvdb_data:
        network = tvdb_data.get("primaryNetwork") or {}
        networks.append(clean_value(network.get("name")))
    if show:
        net = show.get("network") or show.get("webChannel") or {}
        networks.append(clean_value(net.get("name")))
    if tmdb_data:
        tmdb_networks = tmdb_data.get("networks") or []
        networks.extend(clean_value(net.get("name")) for net in tmdb_networks)
    candidate = prefer_simple(current("current_network", ""), join_list(networks))
    if candidate:
        updates["network"] = candidate

    genres: List[str] = []
    if tvdb_data:
        genres.extend(clean_value(genre.get("name")) for genre in tvdb_data.get("genres") or [])
    if show:
        genres.extend(show.get("genres") or [])
    if omdb_data and clean_value(omdb_data.get("Genre")) and omdb_data.get("Genre") != "N/A":
        genres.extend(name.strip() for name in omdb_data.get("Genre", "").split(","))
    if tmdb_data:
        genres.extend(clean_value(genre.get("name")) for genre in tmdb_data.get("genres") or [])
    candidate = prefer_list(current("current_genre", ""), genres)
    if candidate:
        updates["genre"] = candidate

    if tvdb_data and clean_value(tvdb_data.get("rating")):
        candidate = prefer_simple(current("current_rating", ""), tvdb_data.get("rating"))
        if candidate:
            updates["rating"] = candidate
    elif omdb_data and clean_value(omdb_data.get("Rated")) and omdb_data.get("Rated") != "N/A":
        candidate = prefer_simple(current("current_rating", ""), omdb_data.get("Rated"))
        if candidate:
            updates["rating"] = candidate

    cast_names: List[str] = []
    if show:
        cast = show.get("_embedded", {}).get("cast") or []
        for member in cast:
            person = member.get("person") or {}
            name = clean_value(person.get("name"))
            if name:
                cast_names.append(name)
    credits = (tmdb_data or {}).get("credits", {}).get("cast") or []
    for member in credits[:5]:
        name = clean_value(member.get("name"))
        if name:
            cast_names.append(name)
    candidate = prefer_list(current("current_cast", ""), cast_names[:5])
    if candidate:
        updates["cast"] = candidate

    show_image = ""
    if show:
        image_data = show.get("image") or {}
        show_image = clean_value(image_data.get("original")) or clean_value(image_data.get("medium"))
    if not show_image and tmdb_data:
        poster_path = clean_value(tmdb_data.get("poster_path"))
        if poster_path:
            show_image = f"{TMDB_IMAGE_BASE}{poster_path}"
    candidate = prefer_image(current("current_iseries", ""), show_image, source)
    if candidate:
        updates["iseries"] = candidate

    if ids.get("imdb"):
        id_updates.setdefault("imdb", ids["imdb"])
    if ids.get("tvdb"):
        id_updates.setdefault("tvdb", ids["tvdb"])
    if ids.get("tvmaze"):
        id_updates.setdefault("tvmaze", ids["tvmaze"])
    if ids.get("tmdb"):
        id_updates.setdefault("tmdb", ids["tmdb"])

    return updates, id_updates


def process_rows(
    conn: sqlite3.Connection,
    session: requests.Session,
    api_keys: Dict[str, str],
    verbose: bool,
) -> None:
    import_cols = table_columns(conn, "import")
    online_cols = table_columns(conn, "online")
    if "checksum" not in import_cols:
        print("Error: import table missing checksum column")
        return
    query, aliases = build_column_query(import_cols, online_cols)

    cache = TvMazeCache()
    tvdb_client = TvdbClient(api_keys.get("theTVDB"), session) if clean_value(api_keys.get("theTVDB")) else None

    total_updates = 0
    for row in conn.execute(query):
        data = dict(zip(aliases, row)) if not isinstance(row, sqlite3.Row) else dict(row)
        checksum = clean_value(data.get("checksum"))
        if not checksum:
            continue
        torrent_type = clean_value(
            data.get("import_torrenttype") or data.get("current_torrenttype") or data.get("torrenttype")
        ).lower()
        if torrent_type not in {"movie", "tv", "series"}:
            torrent_type = "tv" if clean_value(data.get("import_series")) else "movie"

        if torrent_type == "movie":
            updates, id_updates = update_movie_metadata(data, session, api_keys)
        else:
            updates, id_updates = update_tv_metadata(data, session, api_keys, cache, tvdb_client)

        if updates or id_updates:
            update_tables(conn, checksum, updates, id_updates, import_cols)
            conn.commit()
            total_updates += len(updates)
            if verbose:
                changed = ", ".join(sorted(updates)) or ", ".join(sorted(id_updates))
                title = clean_value(data.get("import_movie")) or clean_value(data.get("import_series")) or checksum
                print(f"Updated {title}: {changed}")
        elif verbose:
            title = clean_value(data.get("import_movie")) or clean_value(data.get("import_series")) or checksum
            print(f"No updates for {title}")

    if verbose:
        print(f"Total metadata fields updated: {total_updates}")


def main() -> None:
    args = parse_args()

    try:
        config = load_config()
    except RuntimeError as exc:
        print(exc)
        return

    api_keys = config.get("API_KEYS", {})
    if not any(clean_value(value) for value in api_keys.values()):
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
