#!/usr/bin/env python3

import argparse
import json
import re
import sqlite3
from collections import OrderedDict
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests


API_ID_FIELDS: Tuple[str, ...] = ("imdb", "tmdb", "tvmaze", "tvdb")
METADATA_FIELDS: Tuple[str, ...] = (
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
)
MOVIE_ONLY_FIELDS = {"dmovie", "release", "studio", "imovie"}
TV_ONLY_FIELDS = {"dseries", "dseason", "depisode", "airdate", "network", "iseries", "iseason", "iepisode"}
USER_AGENT = "bekindrewind-api/1.0"


def load_api_keys() -> Dict[str, str]:
    config_path = Path(__file__).parent.parent / "user.json"
    try:
        with config_path.open("r", encoding="utf-8") as handle:
            config = json.load(handle)
    except FileNotFoundError:
        return {}
    except json.JSONDecodeError:
        return {}
    api_keys = config.get("API_KEYS", {})
    return {key: value for key, value in api_keys.items() if isinstance(value, str) and value.strip()}


def connect_database() -> sqlite3.Connection:
    db_path = Path(__file__).parent.parent / "tapedeck.db"
    conn = sqlite3.connect(str(db_path))
    return conn


def fetch_table_columns(cursor: sqlite3.Cursor, table: str) -> List[str]:
    cursor.execute(f"PRAGMA table_info({table})")
    return [row[1] for row in cursor.fetchall()]


def parse_int(value: Any) -> Optional[int]:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def clean_text(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    text = re.sub(r"\s+", " ", str(value)).strip()
    return text or None


def fetch_import_records(conn: sqlite3.Connection) -> Tuple[List[Dict[str, Any]], List[str]]:
    cursor = conn.cursor()
    columns = fetch_table_columns(cursor, "import")
    select_fields: List[str] = ["checksum"]
    for field in ["movie", "series", "season", "episode", "dlsource"]:
        if field in columns:
            select_fields.append(field)

    quoted_fields = ", ".join(f'"{field}"' for field in select_fields)
    cursor.execute(f"SELECT {quoted_fields} FROM import")
    rows = cursor.fetchall()

    records: List[Dict[str, Any]] = []
    for row in rows:
        record = dict(zip(select_fields, row))
        record["checksum"] = str(record.get("checksum"))
        record["movie"] = clean_text(record.get("movie"))
        record["series"] = clean_text(record.get("series"))
        record["season"] = parse_int(record.get("season"))
        record["episode"] = parse_int(record.get("episode"))
        record["dlsource"] = clean_text(record.get("dlsource"))
        records.append(record)

    return records, columns


def normalize_cache_key(value: str) -> str:
    return re.sub(r"\s+", " ", value.strip().lower())


def normalize_for_compare(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", value.lower())


def request_json(
    session: requests.Session,
    method: str,
    url: str,
    *,
    params: Optional[Dict[str, Any]] = None,
    json_body: Optional[Dict[str, Any]] = None,
    headers: Optional[Dict[str, str]] = None,
) -> Any:
    try:
        response = session.request(
            method,
            url,
            params=params,
            json=json_body,
            headers=headers,
            timeout=15,
        )
        response.raise_for_status()
        if not response.content:
            return None
        return response.json()
    except (requests.RequestException, ValueError):
        return None


def smart_pick(results: Iterable[Dict[str, Any]], target: Optional[str]) -> Optional[Dict[str, Any]]:
    results_list = [r for r in results if isinstance(r, dict)]
    if not results_list:
        return None
    if not target:
        return results_list[0]

    normalized_target = normalize_for_compare(target)
    if not normalized_target:
        normalized_target = target.strip().lower()

    best: Optional[Dict[str, Any]] = None
    best_score = float("-inf")

    for candidate in results_list:
        name = (
            candidate.get("name")
            or candidate.get("title")
            or candidate.get("original_name")
            or candidate.get("original_title")
        )
        if not name:
            continue

        normalized_name = normalize_for_compare(name)
        score = 0.0

        if normalized_name and normalized_name == normalized_target:
            score += 1000.0
        else:
            ratio = SequenceMatcher(None, normalized_target, normalized_name).ratio()
            score += ratio * 400
            target_words = set(re.findall(r"[a-z0-9]+", normalized_target))
            name_words = set(re.findall(r"[a-z0-9]+", normalized_name))
            if target_words:
                score += len(target_words & name_words) * 15

        vote_count = candidate.get("vote_count")
        if isinstance(vote_count, (int, float)):
            score += min(vote_count, 5000) / 40

        popularity = candidate.get("popularity")
        if isinstance(popularity, (int, float)):
            score += min(popularity, 10000) / 80

        if score > best_score:
            best_score = score
            best = candidate

    return best or results_list[0]


def score_candidate(value: Any, priority_rank: int) -> float:
    if value is None:
        return float("-inf")
    if isinstance(value, (list, tuple)):
        text = ", ".join(str(v) for v in value)
    else:
        text = str(value)
    stripped = text.strip()
    if not stripped:
        return float("-inf")
    score = priority_rank * 200
    score += min(len(stripped), 400)
    score += stripped.count(",") * 5
    if "http" in stripped.lower():
        score += 80
    return score


def best_choice(options: OrderedDict[str, Any], existing_value: Optional[Any] = None) -> Optional[Any]:
    if not options:
        return existing_value

    best_value = existing_value
    best_score = score_candidate(existing_value, 0) if existing_value else float("-inf")
    total = len(options)

    for index, value in enumerate(options.values()):
        score = score_candidate(value, total - index)
        if score > best_score:
            best_score = score
            best_value = value

    return best_value


def strip_html(summary: Optional[str]) -> Optional[str]:
    if not summary:
        return None
    cleaned = re.sub(r"<[^>]+>", "", summary)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned or None


def tmdb_image_url(path: Optional[str]) -> Optional[str]:
    if not path:
        return None
    return f"https://image.tmdb.org/t/p/w500{path}"


def is_origin_source_image(image_url: Optional[str], source: Optional[str]) -> bool:
    if not image_url or not source:
        return False

    source_domains = {
        "amazon": ["amazon.com", "primevideo.com", "images-amazon.com"],
        "hbo": ["hbo.com", "hbomax.com", "max.com"],
        "max": ["hbo.com", "hbomax.com", "max.com"],
        "hbo max": ["hbo.com", "hbomax.com", "max.com"],
        "youtube": ["youtube.com", "ytimg.com"],
    }

    domains = source_domains.get(source.lower(), [])
    image_url_lower = image_url.lower()
    return any(domain in image_url_lower for domain in domains)


class APICache:
    def __init__(self) -> None:
        self.tvmaze_search: Dict[str, Optional[int]] = {}
        self.tvmaze_show: Dict[int, Dict[str, Any]] = {}
        self.tvmaze_seasons: Dict[int, List[Dict[str, Any]]] = {}
        self.tvmaze_episodes: Dict[int, List[Dict[str, Any]]] = {}
        self.tvmaze_cast: Dict[int, List[str]] = {}
        self.tmdb_search: Dict[Tuple[str, bool], Optional[Dict[str, Any]]] = {}
        self.tmdb_details: Dict[Tuple[int, bool], Dict[str, Any]] = {}
        self.tmdb_external: Dict[Tuple[int, bool], Dict[str, Any]] = {}
        self.tmdb_episode: Dict[Tuple[int, int, int], Dict[str, Any]] = {}
        self.omdb: Dict[str, Dict[str, Any]] = {}
        self.tvdb_token: Optional[str] = None
        self.tvdb_search: Dict[str, Optional[Dict[str, Any]]] = {}
        self.tvdb_series: Dict[int, Dict[str, Any]] = {}


def get_tvmaze_show(session: requests.Session, title: Optional[str], cache: APICache) -> Optional[Dict[str, Any]]:
    if not title:
        return None
    key = normalize_cache_key(title)
    show_id = cache.tvmaze_search.get(key)

    if show_id is None:
        data = request_json(
            session,
            "get",
            "https://api.tvmaze.com/search/shows",
            params={"q": title},
        )
        if not data:
            cache.tvmaze_search[key] = None
            return None
        shows = [entry.get("show") for entry in data if isinstance(entry, dict) and entry.get("show")]
        match = smart_pick(shows, title)
        show_id = match.get("id") if match else None
        cache.tvmaze_search[key] = show_id
        if show_id and match:
            cache.tvmaze_show[show_id] = match

    if not show_id:
        return None

    show = cache.tvmaze_show.get(show_id)
    if not show or "genres" not in show:
        details = request_json(session, "get", f"https://api.tvmaze.com/shows/{show_id}")
        if details:
            cache.tvmaze_show[show_id] = details
            show = details

    return show


def get_tvmaze_season(
    session: requests.Session, show_id: Optional[int], season_number: Optional[int], cache: APICache
) -> Dict[str, Any]:
    if not show_id or season_number is None:
        return {}
    seasons = cache.tvmaze_seasons.get(show_id)
    if seasons is None:
        data = request_json(session, "get", f"https://api.tvmaze.com/shows/{show_id}/seasons")
        seasons = data if isinstance(data, list) else []
        cache.tvmaze_seasons[show_id] = seasons
    for season in seasons:
        if season.get("number") == season_number:
            return season
    return {}


def get_tvmaze_episode(
    session: requests.Session,
    show_id: Optional[int],
    season_number: Optional[int],
    episode_number: Optional[int],
    cache: APICache,
) -> Dict[str, Any]:
    if not show_id or season_number is None or episode_number is None:
        return {}
    episodes = cache.tvmaze_episodes.get(show_id)
    if episodes is None:
        data = request_json(session, "get", f"https://api.tvmaze.com/shows/{show_id}/episodes")
        episodes = data if isinstance(data, list) else []
        cache.tvmaze_episodes[show_id] = episodes
    for episode in episodes:
        if episode.get("season") == season_number and episode.get("number") == episode_number:
            return episode
    return {}


def get_tvmaze_cast(session: requests.Session, show_id: Optional[int], cache: APICache) -> List[str]:
    if not show_id:
        return []
    cast = cache.tvmaze_cast.get(show_id)
    if cast is None:
        data = request_json(session, "get", f"https://api.tvmaze.com/shows/{show_id}/cast")
        cast = []
        if isinstance(data, list):
            for item in data:
                if not isinstance(item, dict):
                    continue
                person = item.get("person") or {}
                name = person.get("name")
                if name:
                    cast.append(name)
        cache.tvmaze_cast[show_id] = cast
    return cast


def search_tmdb(
    session: requests.Session,
    title: Optional[str],
    api_key: Optional[str],
    is_tv: bool,
    cache: APICache,
) -> Optional[Dict[str, Any]]:
    if not api_key or not title:
        return None
    key = (normalize_cache_key(title), is_tv)
    if key in cache.tmdb_search:
        return cache.tmdb_search[key]

    endpoint = "tv" if is_tv else "movie"
    data = request_json(
        session,
        "get",
        f"https://api.themoviedb.org/3/search/{endpoint}",
        params={"api_key": api_key, "query": title, "include_adult": "false"},
    )

    result = None
    if isinstance(data, dict) and data.get("results"):
        result = smart_pick(data["results"], title)

    cache.tmdb_search[key] = result
    return result


def get_tmdb_details(
    session: requests.Session,
    tmdb_id: Optional[int],
    api_key: Optional[str],
    is_tv: bool,
    cache: APICache,
) -> Dict[str, Any]:
    if not api_key or not tmdb_id:
        return {}
    key = (tmdb_id, is_tv)
    details = cache.tmdb_details.get(key)
    if details:
        return details

    endpoint = "tv" if is_tv else "movie"
    data = request_json(
        session,
        "get",
        f"https://api.themoviedb.org/3/{endpoint}/{tmdb_id}",
        params={"api_key": api_key},
    )
    if isinstance(data, dict):
        cache.tmdb_details[key] = data
        return data
    return {}


def get_tmdb_external_ids(
    session: requests.Session,
    tmdb_id: Optional[int],
    api_key: Optional[str],
    is_tv: bool,
    cache: APICache,
) -> Dict[str, Any]:
    if not api_key or not tmdb_id:
        return {}
    key = (tmdb_id, is_tv)
    external = cache.tmdb_external.get(key)
    if external is not None:
        return external

    endpoint = "tv" if is_tv else "movie"
    data = request_json(
        session,
        "get",
        f"https://api.themoviedb.org/3/{endpoint}/{tmdb_id}/external_ids",
        params={"api_key": api_key},
    )
    cache.tmdb_external[key] = data if isinstance(data, dict) else {}
    return cache.tmdb_external[key]


def get_tmdb_episode(
    session: requests.Session,
    tmdb_id: Optional[int],
    season_number: Optional[int],
    episode_number: Optional[int],
    api_key: Optional[str],
    cache: APICache,
) -> Dict[str, Any]:
    if not api_key or not tmdb_id or season_number is None or episode_number is None:
        return {}
    key = (tmdb_id, season_number, episode_number)
    episode = cache.tmdb_episode.get(key)
    if episode:
        return episode

    data = request_json(
        session,
        "get",
        f"https://api.themoviedb.org/3/tv/{tmdb_id}/season/{season_number}/episode/{episode_number}",
        params={"api_key": api_key},
    )
    if isinstance(data, dict):
        cache.tmdb_episode[key] = data
        return data
    return {}


def get_omdb_data(
    session: requests.Session, imdb_id: Optional[str], api_key: Optional[str], cache: APICache
) -> Dict[str, Any]:
    if not api_key or not imdb_id:
        return {}
    imdb_id = imdb_id.strip()
    if imdb_id in cache.omdb:
        return cache.omdb[imdb_id]

    data = request_json(
        session,
        "get",
        "http://www.omdbapi.com/",
        params={"apikey": api_key, "i": imdb_id},
    )
    if not isinstance(data, dict) or data.get("Response") == "False":
        cache.omdb[imdb_id] = {}
    else:
        cache.omdb[imdb_id] = data
    return cache.omdb[imdb_id]


def ensure_tvdb_token(session: requests.Session, api_key: Optional[str], cache: APICache) -> Optional[str]:
    if not api_key:
        return None
    if cache.tvdb_token:
        return cache.tvdb_token
    data = request_json(
        session,
        "post",
        "https://api4.thetvdb.com/v4/login",
        json_body={"apikey": api_key},
    )
    token = None
    if isinstance(data, dict):
        token = data.get("data", {}).get("token")
    cache.tvdb_token = token
    return token


def search_tvdb(
    session: requests.Session, series_name: Optional[str], api_key: Optional[str], cache: APICache
) -> Optional[Dict[str, Any]]:
    if not api_key or not series_name:
        return None
    key = normalize_cache_key(series_name)
    if key in cache.tvdb_search:
        return cache.tvdb_search[key]

    token = ensure_tvdb_token(session, api_key, cache)
    if not token:
        cache.tvdb_search[key] = None
        return None

    headers = {"Authorization": f"Bearer {token}"}
    data = request_json(
        session,
        "get",
        "https://api4.thetvdb.com/v4/search",
        params={"query": series_name, "type": "series"},
        headers=headers,
    )

    result = None
    if isinstance(data, dict) and data.get("data"):
        result = smart_pick(data["data"], series_name)

    if result and result.get("id"):
        cache.tvdb_series[result["id"]] = result

    cache.tvdb_search[key] = result
    return result


def get_tvdb_series(
    session: requests.Session, series_id: Optional[int], api_key: Optional[str], cache: APICache
) -> Dict[str, Any]:
    if not api_key or not series_id:
        return {}
    if series_id in cache.tvdb_series and cache.tvdb_series[series_id].get("overview"):
        return cache.tvdb_series[series_id]

    token = ensure_tvdb_token(session, api_key, cache)
    if not token:
        return cache.tvdb_series.get(series_id, {})

    headers = {"Authorization": f"Bearer {token}"}
    data = request_json(session, "get", f"https://api4.thetvdb.com/v4/series/{series_id}", headers=headers)
    if isinstance(data, dict) and data.get("data"):
        cache.tvdb_series[series_id] = data["data"]
        return data["data"]
    return cache.tvdb_series.get(series_id, {})


def assemble_context(
    *,
    record: Dict[str, Any],
    api_keys: Dict[str, str],
    session: requests.Session,
    cache: APICache,
    existing_ids: Dict[str, Optional[str]],
) -> Tuple[Dict[str, Any], Dict[str, Optional[str]]]:
    movie_title = record.get("movie")
    series_title = record.get("series")
    season_number = record.get("season")
    episode_number = record.get("episode")

    context: Dict[str, Any] = {
        "type": "movie" if movie_title and not series_title else "series" if series_title else None,
        "movie_title": movie_title,
        "series_title": series_title,
        "season_number": season_number,
        "episode_number": episode_number,
    }

    final_ids: Dict[str, Optional[str]] = {key: existing_ids.get(key) for key in API_ID_FIELDS}

    if context["type"] == "movie":
        if not final_ids.get("tmdb") and api_keys.get("TMDB"):
            tmdb_match = search_tmdb(session, movie_title, api_keys["TMDB"], False, cache)
            if tmdb_match and tmdb_match.get("id"):
                final_ids["tmdb"] = str(tmdb_match["id"])

        tmdb_numeric_id = parse_int(final_ids.get("tmdb"))
        tmdb_data = get_tmdb_details(
            session,
            tmdb_numeric_id,
            api_keys.get("TMDB"),
            False,
            cache,
        )
        if tmdb_data:
            context["tmdb"] = tmdb_data
            if not final_ids.get("imdb"):
                imdb_id = tmdb_data.get("imdb_id")
                if imdb_id:
                    final_ids["imdb"] = imdb_id

        if not final_ids.get("imdb") and tmdb_numeric_id is not None and api_keys.get("TMDB"):
            external = get_tmdb_external_ids(
                session,
                tmdb_numeric_id,
                api_keys.get("TMDB"),
                False,
                cache,
            )
            if external and external.get("imdb_id"):
                final_ids["imdb"] = external["imdb_id"]

        imdb_data = get_omdb_data(session, final_ids.get("imdb"), api_keys.get("OMDB"), cache)
        if imdb_data:
            context["imdb"] = imdb_data

    elif context["type"] == "series":
        tvmaze_show = get_tvmaze_show(session, series_title, cache)
        if tvmaze_show:
            context["tvmaze_show"] = tvmaze_show
            if not final_ids.get("tvmaze"):
                final_ids["tvmaze"] = str(tvmaze_show.get("id")) if tvmaze_show.get("id") is not None else None
            externals = tvmaze_show.get("externals", {})
            if externals.get("imdb") and not final_ids.get("imdb"):
                final_ids["imdb"] = externals.get("imdb")

            show_id = tvmaze_show.get("id")
            context["tvmaze_season"] = get_tvmaze_season(session, show_id, season_number, cache)
            context["tvmaze_episode"] = get_tvmaze_episode(
                session,
                show_id,
                season_number,
                episode_number,
                cache,
            )
            context["tvmaze_cast"] = get_tvmaze_cast(session, show_id, cache)

        if not final_ids.get("tmdb") and api_keys.get("TMDB"):
            tmdb_match = search_tmdb(session, series_title, api_keys["TMDB"], True, cache)
            if tmdb_match and tmdb_match.get("id"):
                final_ids["tmdb"] = str(tmdb_match["id"])

        tmdb_numeric_id = parse_int(final_ids.get("tmdb"))
        tmdb_data = get_tmdb_details(
            session,
            tmdb_numeric_id,
            api_keys.get("TMDB"),
            True,
            cache,
        )
        if tmdb_data:
            context["tmdb"] = tmdb_data

        if tmdb_numeric_id is not None and api_keys.get("TMDB") and not final_ids.get("imdb"):
            external = get_tmdb_external_ids(
                session,
                tmdb_numeric_id,
                api_keys.get("TMDB"),
                True,
                cache,
            )
            if external and external.get("imdb_id"):
                final_ids["imdb"] = external["imdb_id"]

        if tmdb_numeric_id is not None and api_keys.get("TMDB"):
            context["tmdb_episode"] = get_tmdb_episode(
                session,
                tmdb_numeric_id,
                season_number,
                episode_number,
                api_keys.get("TMDB"),
                cache,
            )

        if not final_ids.get("tvdb") and api_keys.get("theTVDB"):
            tvdb_match = search_tvdb(session, series_title, api_keys.get("theTVDB"), cache)
            if tvdb_match and tvdb_match.get("id"):
                final_ids["tvdb"] = str(tvdb_match.get("id"))

        tvdb_numeric_id = parse_int(final_ids.get("tvdb"))
        tvdb_series = get_tvdb_series(
            session,
            tvdb_numeric_id,
            api_keys.get("theTVDB"),
            cache,
        )
        if tvdb_series:
            context["tvdb_series"] = tvdb_series

        imdb_data = get_omdb_data(session, final_ids.get("imdb"), api_keys.get("OMDB"), cache)
        if imdb_data:
            context["imdb"] = imdb_data

    final_ids = {key: (str(value) if value not in (None, "") else None) for key, value in final_ids.items()}
    return context, final_ids


def extract_field(
    field: str,
    context: Dict[str, Any],
    *,
    existing_value: Optional[str],
    source: Optional[str],
) -> Optional[str]:
    tvdb_series = context.get("tvdb_series", {})
    tvmaze_show = context.get("tvmaze_show", {})
    tvmaze_season = context.get("tvmaze_season", {})
    tvmaze_episode = context.get("tvmaze_episode", {})
    tvmaze_cast = context.get("tvmaze_cast", [])
    tmdb_data = context.get("tmdb", {})
    tmdb_episode = context.get("tmdb_episode", {})
    imdb_data = context.get("imdb", {})

    options: OrderedDict[str, Any] = OrderedDict()

    if field == "dmovie":
        options["tmdb"] = tmdb_data.get("overview")
        plot = imdb_data.get("Plot") if imdb_data.get("Plot") != "N/A" else None
        options["imdb"] = plot
        return best_choice(options, existing_value)

    if field == "dseries":
        options["tvdb"] = tvdb_series.get("overview")
        options["tvmaze"] = strip_html(tvmaze_show.get("summary"))
        plot = imdb_data.get("Plot") if imdb_data.get("Plot") != "N/A" else None
        options["imdb"] = plot
        options["tmdb"] = tmdb_data.get("overview")
        return best_choice(options, existing_value)

    if field == "dseason":
        options["tvmaze"] = strip_html(tvmaze_season.get("summary"))
        return best_choice(options, existing_value)

    if field == "depisode":
        options["tvmaze"] = strip_html(tvmaze_episode.get("summary"))
        plot = imdb_data.get("Plot") if imdb_data.get("Plot") != "N/A" else None
        options["imdb"] = plot
        options["tmdb"] = strip_html(tmdb_episode.get("overview"))
        return best_choice(options, existing_value)

    if field == "airdate":
        options["tvmaze"] = tvmaze_episode.get("airdate") or tvmaze_episode.get("airstamp")
        options["tmdb"] = tmdb_episode.get("air_date") or tmdb_data.get("first_air_date")
        return best_choice(options, existing_value)

    if field == "network":
        tvdb_network = tvdb_series.get("network") or {}
        if isinstance(tvdb_network, dict):
            options["tvdb"] = tvdb_network.get("name")
        elif isinstance(tvdb_network, str):
            options["tvdb"] = tvdb_network
        tvmaze_network = tvmaze_show.get("network") or {}
        tvmaze_web = tvmaze_show.get("webChannel") or {}
        if isinstance(tvmaze_network, dict):
            options["tvmaze"] = tvmaze_network.get("name")
        if isinstance(tvmaze_web, dict) and not options.get("tvmaze"):
            options["tvmaze"] = tvmaze_web.get("name")
        networks = tmdb_data.get("networks")
        if isinstance(networks, list) and networks:
            options["tmdb"] = networks[0].get("name")
        return best_choice(options, existing_value)

    if field == "genre":
        all_genres: List[str] = []
        tvdb_genres = tvdb_series.get("genres")
        if isinstance(tvdb_genres, list):
            for genre in tvdb_genres:
                if isinstance(genre, dict) and genre.get("name"):
                    all_genres.append(genre["name"])
                elif isinstance(genre, str):
                    all_genres.append(genre)
        if isinstance(tvmaze_show.get("genres"), list):
            all_genres.extend(tvmaze_show["genres"])
        imdb_genre = imdb_data.get("Genre")
        if isinstance(imdb_genre, str) and imdb_genre != "N/A":
            all_genres.extend([g.strip() for g in imdb_genre.split(",")])
        tmdb_genres = tmdb_data.get("genres")
        if isinstance(tmdb_genres, list):
            for genre in tmdb_genres:
                if isinstance(genre, dict) and genre.get("name"):
                    all_genres.append(genre["name"])

        unique: List[str] = []
        seen = set()
        for genre in all_genres:
            cleaned = genre.strip()
            if not cleaned:
                continue
            lowered = cleaned.lower()
            if lowered not in seen:
                seen.add(lowered)
                unique.append(cleaned)
        if unique:
            combined = ", ".join(unique[:5])
            options["combined"] = combined
        return best_choice(options, existing_value)

    if field == "rating":
        tvdb_ratings = tvdb_series.get("contentRatings")
        if isinstance(tvdb_ratings, list) and tvdb_ratings:
            rating = tvdb_ratings[0].get("name")
            options["tvdb"] = rating
        imdb_rated = imdb_data.get("Rated")
        if isinstance(imdb_rated, str) and imdb_rated != "N/A":
            options["imdb"] = imdb_rated
        tmdb_rating = tmdb_data.get("content_rating")
        if tmdb_rating:
            options["tmdb"] = tmdb_rating
        return best_choice(options, existing_value)

    if field == "cast":
        if tvmaze_cast:
            options["tvmaze"] = ", ".join(tvmaze_cast[:5])
        actors = imdb_data.get("Actors")
        if isinstance(actors, str) and actors != "N/A":
            options["imdb"] = ", ".join([name.strip() for name in actors.split(",")][:5])
        return best_choice(options, existing_value)

    if field == "release":
        options["tmdb"] = tmdb_data.get("release_date")
        released = imdb_data.get("Released")
        if isinstance(released, str) and released != "N/A":
            options["imdb"] = released
        return best_choice(options, existing_value)

    if field == "studio":
        companies = tmdb_data.get("production_companies")
        if isinstance(companies, list) and companies:
            for company in companies:
                if isinstance(company, dict) and company.get("name"):
                    options["tmdb"] = company.get("name")
                    break
        return best_choice(options, existing_value)

    if field == "imovie":
        if existing_value and source and is_origin_source_image(existing_value, source):
            return existing_value
        poster_path = tmdb_data.get("poster_path")
        options["tmdb"] = tmdb_image_url(poster_path)
        return best_choice(options, existing_value)

    if field == "iseries":
        if existing_value and source and is_origin_source_image(existing_value, source):
            return existing_value
        options["tvdb"] = tvdb_series.get("image", {}).get("url") if isinstance(tvdb_series.get("image"), dict) else None
        tvmaze_image = tvmaze_show.get("image") if isinstance(tvmaze_show.get("image"), dict) else {}
        options["tvmaze"] = tvmaze_image.get("original") or tvmaze_image.get("medium")
        options["tmdb"] = tmdb_image_url(tmdb_data.get("poster_path"))
        return best_choice(options, existing_value)

    if field == "iseason":
        if existing_value and source and is_origin_source_image(existing_value, source):
            return existing_value
        if isinstance(tvmaze_season.get("image"), dict):
            options["tvmaze"] = tvmaze_season["image"].get("original") or tvmaze_season["image"].get("medium")
        options["tmdb"] = tmdb_image_url(tmdb_episode.get("still_path"))
        return best_choice(options, existing_value)

    if field == "iepisode":
        if existing_value and source and is_origin_source_image(existing_value, source):
            return existing_value
        if isinstance(tvmaze_episode.get("image"), dict):
            options["tvmaze"] = tvmaze_episode["image"].get("original") or tvmaze_episode["image"].get("medium")
        options["tmdb"] = tmdb_image_url(tmdb_episode.get("still_path"))
        return best_choice(options, existing_value)

    return existing_value


def fetch_online_row(
    cursor: sqlite3.Cursor, checksum: str, fields: List[str]
) -> Dict[str, Optional[str]]:
    if not fields:
        return {}
    quoted_fields = ", ".join(f'"{field}"' for field in fields)
    cursor.execute(f"SELECT {quoted_fields} FROM online WHERE checksum = ?", (checksum,))
    row = cursor.fetchone()
    if not row:
        return {}
    return dict(zip(fields, row))


def update_row(cursor: sqlite3.Cursor, table: str, checksum: str, updates: Dict[str, Any]) -> int:
    if not updates:
        return 0
    assignments = ", ".join(f'"{field}" = ?' for field in updates.keys())
    values = list(updates.values()) + [checksum]
    cursor.execute(f"UPDATE {table} SET {assignments} WHERE checksum = ?", values)
    return len(updates)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("-v", "--verbose", action="store_true", help="enable verbose output")
    args = parser.parse_args()

    api_keys = load_api_keys()

    with connect_database() as conn:
        records, import_columns = fetch_import_records(conn)
        if not records:
            print("No records found in import table")
            return

        cursor = conn.cursor()
        online_columns = fetch_table_columns(cursor, "online")
        metadata_fields = [field for field in METADATA_FIELDS if field in online_columns]
        id_fields_online = [field for field in API_ID_FIELDS if field in online_columns]
        id_fields_import = [field for field in API_ID_FIELDS if field in import_columns]

        session = requests.Session()
        session.headers.update({"User-Agent": USER_AGENT})
        cache = APICache()

        if args.verbose:
            print(f"Processing {len(records)} records from import table")

        total_metadata_updates = 0

        for record in records:
            checksum = record["checksum"]
            source = record.get("dlsource")

            existing_row = fetch_online_row(cursor, checksum, metadata_fields + id_fields_online)
            existing_metadata = {field: existing_row.get(field) for field in metadata_fields}
            existing_ids = {
                field: (str(existing_row.get(field)) if existing_row.get(field) not in (None, "") else None)
                for field in API_ID_FIELDS
            }

            context, final_ids = assemble_context(
                record=record,
                api_keys=api_keys,
                session=session,
                cache=cache,
                existing_ids=existing_ids,
            )

            if not context.get("type"):
                continue

            updates: Dict[str, Any] = {}
            for field in metadata_fields:
                if context["type"] == "movie" and field in TV_ONLY_FIELDS:
                    continue
                if context["type"] == "series" and field in MOVIE_ONLY_FIELDS:
                    continue
                value = extract_field(
                    field,
                    context,
                    existing_value=existing_metadata.get(field),
                    source=source,
                )
                current_value = existing_metadata.get(field)
                if value is None:
                    continue
                if isinstance(value, str) and not value.strip():
                    continue
                if current_value is None or current_value == "":
                    updates[field] = value
                elif value != current_value:
                    updates[field] = value

            id_updates = {}
            for key, value in final_ids.items():
                if not value:
                    continue
                if existing_ids.get(key) != value:
                    id_updates[key] = value

            online_updates = {k: v for k, v in {**updates, **id_updates}.items() if k in online_columns}
            import_updates = {k: v for k, v in id_updates.items() if k in import_columns}

            if online_updates:
                update_row(cursor, "online", checksum, online_updates)
            if import_updates:
                update_row(cursor, "import", checksum, import_updates)

            if updates:
                total_metadata_updates += len(updates)
                if args.verbose:
                    title = record.get("movie") or record.get("series") or checksum
                    print(f"Updated {len(updates)} fields for {title}")

        conn.commit()

    print(f"Updated {total_metadata_updates} total fields")


if __name__ == "__main__":
    main()
