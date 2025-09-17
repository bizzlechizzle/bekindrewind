#!/usr/bin/env python3
"""Prepare release folders, filenames, and NFO metadata per prep.md instructions."""

from __future__ import annotations

import argparse
import json
import os
import shutil
import sqlite3
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

CONFIG_PATH = Path(__file__).parent.parent / "user.json"
PREFERENCES_DIR = Path(__file__).parent.parent / "preferences"
SOURCES_PATH = PREFERENCES_DIR / "sources.json"
DB_PATH = Path(__file__).parent.parent / "tapedeck.db"

TEMPLATE_FILENAMES = {
    "series": "series.json",
    "season": "season.json",
    "episode": "episode.json",
    "movie": "movie.json",
}

RESOLUTION_ORDER = {
    "2160P": 5,
    "1080P": 4,
    "720P": 3,
    "576P": 2,
    "480P": 1,
    "SD": 0,
}

VIDEO_CODEC_ORDER = {
    "AV1": 5,
    "H265": 4,
    "HEVC": 4,
    "H264": 3,
    "AVC": 3,
    "VC1": 2,
    "XVID": 1,
}

AUDIO_CODEC_ORDER = {
    "TRUEHD": 6,
    "DTS-HD": 5,
    "DTSX": 5,
    "DTS": 4,
    "EAC3": 3,
    "AC3": 2,
    "AAC": 1,
    "OPUS": 1,
}

CHANNEL_ORDER = {"7.1": 3, "5.1": 2, "STEREO": 1, "MONO": 0}
SUBTITLE_EXTENSIONS = {".srt", ".ass", ".vtt", ".sub"}

LANGUAGE_MAP = {
    "en": "English",
    "eng": "English",
    "es": "Spanish",
    "spa": "Spanish",
    "fr": "French",
    "fre": "French",
    "de": "German",
    "ger": "German",
    "it": "Italian",
    "ita": "Italian",
    "pt": "Portuguese",
    "por": "Portuguese",
    "ru": "Russian",
    "rus": "Russian",
    "ja": "Japanese",
    "jpn": "Japanese",
    "ko": "Korean",
    "kor": "Korean",
    "zh": "Chinese",
    "zho": "Chinese",
}


DEFAULT_TEMPLATES = {
    "series": {
        "lines": [
            "----------------------------------------------------------------",
            "                    {series} - {season_headline}",
            "----------------------------------------------------------------",
            "",
            "{series_overview}",
            "{series_image}",
            "",
            "Series Name : {series}",
            "Seasons : {season_span}",
            "Episodes : {episode_count}",
            "Year : {release_year}",
            "Source : {source_name} Web Download",
            "Resolution : {resolution}",
            "HDR : {hdr}",
            "Video Codec : {video_codec}",
            "Audio Codec : {audio_codec}",
            "Audio Channels : {audio_channels}",
            "Release Date : {release_dates}",
            "Release Group : {release_group}",
            "",
            "Network: {network}",
            "Genre: {genre}",
            "Rating: {rating}",
            "Cast: {cast}",
            "",
            "IMDB: {imdb}",
            "TMDB: {tmdb}",
            "TVMAZE: {tvmaze}",
            "THETVDB: {tvdb}",
            "",
            "----------------------------------------------------------------",
            "                    Episodes Included",
            "----------------------------------------------------------------",
            "",
            "{season_overview}",
            "{episodes_list}",
            "",
            "----------------------------------------------------------------",
            "                    Technical Info",
            "----------------------------------------------------------------",
            "",
            "Video",
            "Codec : {video_profile}",
            "Bitrate : {video_bitrate}",
            "",
            "Audio",
            "Sampling Rate : {audio_sample}",
            "Bitrate : {audio_bitrate}",
            "",
            "Duration : {duration_average}",
            "Container : {container}",
            "Language : {language}",
            "Subtitles : {subtitles}",
            "Total Size: {total_size}",
            "",
            "----------------------------------------------------------------",
            "                    Episode Details",
            "----------------------------------------------------------------",
            "",
            "{episode_details}",
            "----------------------------------------------------------------",
            "                    Be Kind, Rewind",
            "----------------------------------------------------------------",
        ]
    },
    "season": {
        "lines": [
            "----------------------------------------------------------------",
            "                    {series} - Season {season_number_display}",
            "----------------------------------------------------------------",
            "",
            "{series_overview}",
            "{series_image}",
            "{season_image}",
            "",
            "Series Name : {series}",
            "Season: Season {season_number_display}",
            "Episodes : {episode_count}",
            "Year : {release_year}",
            "Source : {source_name} Web Download",
            "Resolution : {resolution}",
            "HDR : {hdr}",
            "Video Codec : {video_codec}",
            "Audio Codec : {audio_codec}",
            "Audio Channels : {audio_channels}",
            "Release Date : {release_dates}",
            "Release Group : {release_group}",
            "",
            "Network: {network}",
            "Genre: {genre}",
            "Rating: {rating}",
            "Cast: {cast}",
            "",
            "IMDB: {imdb}",
            "TMDB: {tmdb}",
            "TVMAZE: {tvmaze}",
            "THETVDB: {tvdb}",
            "",
            "----------------------------------------------------------------",
            "                    Episodes Included",
            "----------------------------------------------------------------",
            "",
            "{season_overview}",
            "{episodes_list}",
            "",
            "----------------------------------------------------------------",
            "                    Technical Info",
            "----------------------------------------------------------------",
            "",
            "Video",
            "Codec : {video_profile}",
            "Bitrate : {video_bitrate}",
            "",
            "Audio",
            "Sampling Rate : {audio_sample}",
            "Bitrate : {audio_bitrate}",
            "",
            "Duration : {duration_average}",
            "Container : {container}",
            "Language : {language}",
            "Subtitles : {subtitles}",
            "Total Size: {total_size}",
            "",
            "----------------------------------------------------------------",
            "                    Episode Details",
            "----------------------------------------------------------------",
            "",
            "{episode_details}",
            "----------------------------------------------------------------",
            "                    Be Kind, Rewind",
            "----------------------------------------------------------------",
        ]
    },
    "episode": {
        "lines": [
            "----------------------------------------------------------------",
            "                    {series} - {episode_code}",
            "----------------------------------------------------------------",
            "",
            "{series_overview}",
            "{series_image}",
            "{episode_image}",
            "",
            "Series Name : {series}",
            "Season: Season {season_number_display}",
            "Episode : {episode_number_display}",
            "Year : {release_year}",
            "Source : {source_name} Web Download",
            "Resolution : {resolution}",
            "HDR : {hdr}",
            "Video Codec : {video_codec}",
            "Audio Codec : {audio_codec}",
            "Audio Channels : {audio_channels}",
            "Release Date : {release_dates}",
            "Release Group : {release_group}",
            "",
            "Network: {network}",
            "Genre: {genre}",
            "Rating: {rating}",
            "Cast: {cast}",
            "",
            "IMDB: {imdb}",
            "TMDB: {tmdb}",
            "TVMAZE: {tvmaze}",
            "THETVDB: {tvdb}",
            "",
            "----------------------------------------------------------------",
            "                    Technical Info",
            "----------------------------------------------------------------",
            "",
            "Video",
            "Codec : {video_profile}",
            "Bitrate : {video_bitrate}",
            "",
            "Audio",
            "Sampling Rate : {audio_sample}",
            "Bitrate : {audio_bitrate}",
            "",
            "Duration : {duration_average}",
            "Container : {container}",
            "Language : {language}",
            "Subtitles : {subtitles}",
            "File Size: {total_size}",
            "",
            "Description: {episode_description}",
            "",
            "----------------------------------------------------------------",
            "                    Be Kind, Rewind",
            "----------------------------------------------------------------",
        ]
    },
    "movie": {
        "lines": [
            "----------------------------------------------------------------",
            "                    {movie}",
            "----------------------------------------------------------------",
            "",
            "{movie_overview}",
            "{movie_image}",
            "",
            "Movie Name : {movie}",
            "Year : {release_year}",
            "Source : {source_name} Web Download",
            "Resolution : {resolution}",
            "HDR : {hdr}",
            "Video Codec : {video_codec}",
            "Audio Codec : {audio_codec}",
            "Audio Channels : {audio_channels}",
            "Release Date : {release_dates}",
            "Release Group : {release_group}",
            "",
            "Studio: {studio}",
            "Genre: {genre}",
            "Rating: {rating}",
            "Cast: {cast}",
            "",
            "IMDB: {imdb}",
            "TMDB: {tmdb}",
            "THETVDB: {tvdb}",
            "",
            "----------------------------------------------------------------",
            "                    Technical Info",
            "----------------------------------------------------------------",
            "",
            "Video",
            "Codec : {video_profile}",
            "Bitrate : {video_bitrate}",
            "",
            "Audio",
            "Sampling Rate : {audio_sample}",
            "Bitrate : {audio_bitrate}",
            "",
            "Duration : {duration_average}",
            "Container : {container}",
            "Language : {language}",
            "Subtitles : {subtitles}",
            "File Size: {total_size}",
            "",
            "Description: {movie_description}",
            "",
            "----------------------------------------------------------------",
            "                    Be Kind, Rewind",
            "----------------------------------------------------------------",
        ]
    },
}


class SafeDict(dict):
    """Return an empty string when the template requests a missing key."""

    def __missing__(self, key: str) -> str:  # pragma: no cover - simple helper
        return ""

def load_json_file(path: Path, description: str) -> Dict:
    try:
        with open(path, "r", encoding="utf-8") as handle:
            return json.load(handle)
    except FileNotFoundError:
        raise SystemExit(f"Error: {description} not found at {path}")
    except json.JSONDecodeError as exc:
        raise SystemExit(f"Error: Invalid JSON in {path}: {exc}")


def load_config() -> Dict:
    config = load_json_file(CONFIG_PATH, "user configuration")
    default = config.get("default") or {}
    locations = config.get("locations") or {}

    required_default = {"filereleasegroup", "releasegroup", "torrenttype"}
    if not required_default.issubset(default):
        missing = ", ".join(sorted(required_default - set(default)))
        raise SystemExit(f"Error: Missing default keys in user.json: {missing}")

    file_upload = (locations.get("file_upload") or {}).keys()
    fileflows = (locations.get("fileflows") or {}).keys()
    for key in ("movies", "tv_shows"):
        if key not in file_upload or key not in fileflows:
            raise SystemExit(
                f"Error: user.json.locations requires file_upload and fileflows entries for '{key}'"
            )

    return config


def load_sources() -> Dict[str, str]:
    sources_raw = load_json_file(SOURCES_PATH, "sources configuration")
    return {key.lower(): value for key, value in sources_raw.items()}


def load_template(name: str) -> List[str]:
    filename = TEMPLATE_FILENAMES[name]
    path = PREFERENCES_DIR / filename
    if path.exists():
        try:
            template = load_json_file(path, f"{name} template")
            lines = template.get("lines")
            if isinstance(lines, list) and all(isinstance(line, str) for line in lines):
                return lines
            print(f"Warning: Invalid template format in {path}, using built-in default")
        except SystemExit:
            raise
        except Exception as exc:  # pragma: no cover - defensive fallback
            print(f"Warning: Could not load template {path}: {exc}")
    return DEFAULT_TEMPLATES[name]["lines"]


def sanitize_piece(text: Optional[str]) -> str:
    if not text:
        return "Unknown"
    cleaned = "".join(
        ch if ch.isalnum() or ch in {".", "-", "&", "'"} else " " for ch in text
    )
    collapsed = " ".join(cleaned.split())
    slug = collapsed.replace(" ", ".")
    while ".." in slug:
        slug = slug.replace("..", ".")
    return slug.strip(".") or "Unknown"


def normalize_resolution(value: Optional[str]) -> str:
    if not value:
        return "1080p"
    text = str(value).strip().lower()
    canonical = {
        "2160p": "2160p",
        "1080p": "1080p",
        "720p": "720p",
        "576p": "576p",
        "480p": "480p",
        "sd": "sd",
    }
    if text in canonical:
        return canonical[text]
    if text.endswith("p") and text[:-1].isdigit():
        return text
    return text


def normalize_hdr(value: Optional[str]) -> str:
    if not value:
        return "SDR"
    return "HDR" if str(value).strip().upper() == "HDR" else "SDR"


def normalize_video_codec(value: Optional[str]) -> str:
    if not value:
        return "H264"
    upper = str(value).strip().upper().replace("-", "")
    if upper in {"HEVC", "H265", "X265"}:
        return "H265"
    if upper in {"H264", "X264"}:
        return "H264"
    if upper == "AV1":
        return "AV1"
    return upper


def normalize_audio_codec(value: Optional[str]) -> str:
    if not value:
        return "AAC"
    upper = str(value).strip().upper().replace("-", "")
    if upper == "EAC3":
        return "EAC3"
    if upper == "AC3":
        return "AC3"
    if upper == "TRUEHD":
        return "TRUEHD"
    if upper == "DTSHD":
        return "DTS-HD"
    if upper == "DTSX":
        return "DTSX"
    if upper == "OPUS":
        return "OPUS"
    if upper == "AAC":
        return "AAC"
    if upper == "FLAC":
        return "FLAC"
    return upper


def normalize_channels(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    lower = str(value).strip().lower()
    if lower in {"7.1", "7_1", "7ch", "7 channels"}:
        return "7.1"
    if lower in {"5.1", "5_1", "5ch", "5 channels"}:
        return "5.1"
    if lower in {"stereo", "2ch", "2 channels"}:
        return "stereo"
    if lower in {"mono", "1ch", "1 channel"}:
        return "mono"
    if lower.isdigit():
        return f"{lower} channels"
    return str(value).strip()


def parse_duration_minutes(value: Optional[str]) -> Optional[int]:
    if not value:
        return None
    text = str(value).strip().lower()
    digits = "".join(ch for ch in text if ch.isdigit())
    if not digits:
        return None
    try:
        return int(digits)
    except ValueError:
        return None


def parse_size_mb(value: Optional[str]) -> float:
    if not value:
        return 0.0
    text = str(value).strip().upper().replace(",", "")
    number = "".join(ch for ch in text if ch.isdigit() or ch == ".")
    if not number:
        return 0.0
    try:
        magnitude = float(number)
    except ValueError:
        return 0.0
    if "GB" in text:
        return magnitude * 1024
    if "KB" in text:
        return magnitude / 1024
    return magnitude


def parse_bitrate(value: Optional[str]) -> float:
    if not value:
        return 0.0
    text = str(value).lower().replace(",", "")
    number = "".join(ch for ch in text if ch.isdigit() or ch == ".")
    if not number:
        return 0.0
    try:
        magnitude = float(number)
    except ValueError:
        return 0.0
    if "mb" in text:
        return magnitude * 1000
    return magnitude


def parse_sample_rate(value: Optional[str]) -> float:
    if not value:
        return 0.0
    text = str(value).lower()
    number = "".join(ch for ch in text if ch.isdigit() or ch == ".")
    if not number:
        return 0.0
    try:
        magnitude = float(number)
    except ValueError:
        return 0.0
    if "khz" in text:
        return magnitude
    if "hz" in text:
        return magnitude / 1000
    return magnitude


def pick_best(values: Iterable[str], order_map: Dict[str, int], default: str) -> str:
    best_value = None
    best_rank = -1
    for raw in values:
        if not raw:
            continue
        candidate = str(raw).strip().upper()
        rank = order_map.get(candidate, -1)
        if rank > best_rank:
            best_rank = rank
            best_value = str(raw).strip()
    return best_value or default


def pick_most_common(values: Iterable[str], default: str) -> str:
    filtered = [val for val in values if val]
    if not filtered:
        return default
    counter = Counter(filtered)
    return counter.most_common(1)[0][0]


def format_language(code: Optional[str]) -> str:
    if not code:
        return "English"
    key = code.strip().lower()
    if key in LANGUAGE_MAP:
        return LANGUAGE_MAP[key]
    if len(key) == 3:
        return key.upper()
    return key.title()


def format_subtitles(value: Optional[str]) -> str:
    if not value:
        return "None"
    lower = value.strip().lower()
    if lower == "both":
        return "Both"
    if lower == "internal":
        return "Internal"
    if lower == "external":
        return "External"
    if lower in {"none", "no"}:
        return "None"
    return value.title()


def format_size(total_mb: float) -> str:
    if not total_mb:
        return "Unknown"
    return f"{total_mb:.0f} MB"


def format_average_duration(minutes: Optional[int]) -> str:
    if not minutes:
        return "Unknown"
    return f"{minutes} minutes (average)"


def format_date_range(dates: Sequence[str]) -> Tuple[str, Optional[str]]:
    valid_dates: List[datetime] = []
    for date_str in dates:
        if not date_str:
            continue
        for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%Y-%m", "%Y"):
            try:
                parsed = datetime.strptime(date_str[: len(fmt)], fmt)
                valid_dates.append(parsed)
                break
            except ValueError:
                continue
    if not valid_dates:
        return "Unknown", None
    start = min(valid_dates)
    end = max(valid_dates)
    if start == end:
        return start.strftime("%Y-%m-%d"), start.strftime("%Y")
    return f"{start.strftime('%Y-%m-%d')} to {end.strftime('%Y-%m-%d')}", start.strftime("%Y")


def format_list(value: Optional[str]) -> str:
    if not value:
        return "Unknown"
    text = str(value).replace("|", ",")
    parts = [part.strip() for part in text.split(",") if part.strip()]
    return ", ".join(dict.fromkeys(parts)) or "Unknown"


def format_image(url: Optional[str], max_width: int) -> str:
    if not url:
        return ""
    return (
        f"<div style=\"text-align:center;\"><img src=\"{url}\" "
        f"style=\"max-width:{max_width}px;\"></div>"
    )

def infer_extension(record: Dict) -> str:
    filename = record.get("filename")
    if filename:
        suffix = Path(filename).suffix
        if suffix:
            return suffix
    fileloc = record.get("fileloc")
    if fileloc:
        suffix = Path(fileloc).suffix
        if suffix:
            return suffix
    return ".mkv"


def aggregate_metadata(records: Sequence[Dict]) -> Dict:
    resolutions = [normalize_resolution(rec.get("resolution")) for rec in records if rec.get("resolution")]
    hdr_values = [normalize_hdr(rec.get("hdr")) for rec in records if rec.get("hdr")]
    video_codecs = [normalize_video_codec(rec.get("vcodec")) for rec in records if rec.get("vcodec")]
    audio_codecs = [normalize_audio_codec(rec.get("acodec")) for rec in records if rec.get("acodec")]
    channel_values = [normalize_channels(rec.get("achannels")) for rec in records if rec.get("achannels")]
    vacodecs = [rec.get("vacodec") for rec in records if rec.get("vacodec")]
    vbitrates = [rec.get("vbitrate") for rec in records if rec.get("vbitrate")]
    asamples = [rec.get("asample") for rec in records if rec.get("asample")]
    abitrates = [rec.get("abitrate") for rec in records if rec.get("abitrate")]
    durations = [parse_duration_minutes(rec.get("duration")) for rec in records]
    sizes = [parse_size_mb(rec.get("filesize")) for rec in records]
    languages = [rec.get("language") for rec in records if rec.get("language")]
    subtitles = [rec.get("subtitles") for rec in records if rec.get("subtitles")]

    best_resolution = pick_best(resolutions, RESOLUTION_ORDER, "1080p")
    best_hdr = "HDR" if "HDR" in hdr_values else "SDR"
    best_vcodec = pick_best(video_codecs, VIDEO_CODEC_ORDER, "H264")
    best_acodec = pick_best(audio_codecs, AUDIO_CODEC_ORDER, "AAC")
    best_channels = pick_best([val for val in channel_values if val], CHANNEL_ORDER, "stereo")
    best_vacodec = vacodecs[0] if vacodecs else None

    best_vbitrate_value = 0.0
    best_vbitrate_text = None
    for bitrate in vbitrates:
        parsed = parse_bitrate(bitrate)
        if parsed > best_vbitrate_value:
            best_vbitrate_value = parsed
            best_vbitrate_text = bitrate

    best_asample_value = 0.0
    best_asample_text = None
    for sample in asamples:
        parsed = parse_sample_rate(sample)
        if parsed > best_asample_value:
            best_asample_value = parsed
            best_asample_text = sample

    best_abitrate_value = 0.0
    best_abitrate_text = None
    for bitrate in abitrates:
        parsed = parse_bitrate(bitrate)
        if parsed > best_abitrate_value:
            best_abitrate_value = parsed
            best_abitrate_text = bitrate

    duration_values = [val for val in durations if val]
    average_duration = int(round(sum(duration_values) / len(duration_values))) if duration_values else None

    total_size = sum(sizes)

    language_choice = pick_most_common(languages, "eng")

    subtitle_priority = {"both": 3, "internal": 2, "external": 1, "none": 0}
    subtitle_choice = None
    subtitle_score = -1
    for sub in subtitles:
        score = subtitle_priority.get(str(sub).lower(), 0)
        if score > subtitle_score:
            subtitle_score = score
            subtitle_choice = sub

    container = infer_extension(records[0])[1:].upper() if records else "MKV"

    return {
        "resolution": best_resolution,
        "hdr": best_hdr,
        "vcodec": best_vcodec,
        "acodec": best_acodec,
        "achannels": best_channels,
        "vacodec": best_vacodec,
        "vbitrate": best_vbitrate_text,
        "asample": best_asample_text,
        "abitrate": best_abitrate_text,
        "duration_avg": average_duration,
        "total_size": total_size,
        "language": language_choice,
        "subtitles": subtitle_choice,
        "container": container,
    }


def gather_online_info(records: Sequence[Dict]) -> Dict:
    info: Dict[str, object] = {
        "series_description": None,
        "season_description": defaultdict(str),
        "series_image": None,
        "season_images": defaultdict(str),
        "network": None,
        "genre": None,
        "rating": None,
        "cast": None,
        "imdb": None,
        "tmdb": None,
        "tvmaze": None,
        "tvdb": None,
        "airdates": [],
        "episodes": {},
    }

    for record in records:
        online = record.get("online") or {}
        season = record.get("season")
        checksum = record.get("checksum")

        if not info["series_description"] and online.get("dseries"):
            info["series_description"] = online["dseries"].strip()
        if season is not None and not info["season_description"][season] and online.get("dseason"):
            info["season_description"][season] = online["dseason"].strip()
        if not info["series_image"] and online.get("iseries"):
            info["series_image"] = online["iseries"]
        if season is not None and not info["season_images"][season] and online.get("iseason"):
            info["season_images"][season] = online["iseason"]

        for field in ("network", "genre", "rating", "cast", "imdb", "tmdb", "tvmaze", "tvdb"):
            if not info[field] and online.get(field):
                info[field] = online[field]

        if online.get("airdate"):
            info["airdates"].append(online["airdate"])

        info["episodes"][checksum] = {
            "description": online.get("depisode"),
            "image": online.get("iepisode"),
            "airdate": online.get("airdate"),
        }

    return info


def gather_movie_online_info(record: Dict) -> Dict:
    online = record.get("online") or {}
    return {
        "description": online.get("dmovie"),
        "image": online.get("imovie"),
        "studio": online.get("studio"),
        "genre": online.get("genre"),
        "rating": online.get("rating"),
        "cast": online.get("cast"),
        "imdb": online.get("imdb"),
        "tmdb": online.get("tmdb"),
        "tvdb": online.get("tvdb"),
        "release": online.get("release"),
    }

def build_episode_listing(records: Sequence[Dict]) -> str:
    lines: List[str] = []
    for record in sorted(records, key=lambda rec: ((rec.get("season") or 0), (rec.get("episode") or 0))):
        season = record.get("season")
        episode = record.get("episode")
        if season is not None and episode is not None:
            code = f"S{int(season):02d}E{int(episode):02d}"
        elif episode is not None:
            code = f"Episode {int(episode)}"
        else:
            code = record.get("filename") or record.get("title") or "Unknown"
        title = record.get("title") or (record.get("online") or {}).get("depisode") or "Unknown"
        lines.append(f"{code} - {title}")
    return "\n".join(lines)


def build_episode_details(records: Sequence[Dict], online_info: Dict) -> str:
    blocks: List[str] = []
    for record in sorted(records, key=lambda rec: ((rec.get("season") or 0), (rec.get("episode") or 0))):
        online = online_info.get("episodes", {}).get(record.get("checksum"), {})
        season = record.get("season")
        episode = record.get("episode")
        title = record.get("title") or "Unknown"
        if episode is not None:
            header = f"Episode {int(episode)}"
        else:
            header = "Episode"
        if season is not None and episode is not None:
            header = f"Episode {int(episode)} ({f'S{int(season):02d}E{int(episode):02d}'})"
        block_lines = [f"{header} - {title}"]

        episode_image = online.get("image")
        if episode_image:
            block_lines.append(format_image(episode_image, 400))

        if online.get("airdate"):
            block_lines.append(f"Air Date: {online['airdate']}")

        if record.get("duration"):
            block_lines.append(f"Duration: {record['duration']}")

        if record.get("filesize"):
            block_lines.append(f"Size: {record['filesize']}")

        description = online.get("description") or online.get("depisode")
        if description:
            block_lines.append(f"Description: {description}")

        blocks.append("\n".join(block_lines))
    return "\n\n".join(blocks)


def build_season_overview_text(online_info: Dict, seasons: Sequence[int]) -> str:
    pieces: List[str] = []
    for season in sorted(set(seasons)):
        description = online_info.get("season_description", {}).get(season)
        if description:
            pieces.append(f"Season {int(season)}: {description}")
    return "\n".join(pieces)


def build_context_for_season(
    series_name: str,
    season: int,
    records: Sequence[Dict],
    metadata: Dict,
    online_info: Dict,
    source_name: str,
    config: Dict,
) -> Dict:
    date_range, year = format_date_range(online_info.get("airdates", []))
    context = {
        "series": series_name,
        "season_number_display": f"{int(season):02d}" if season is not None else "",
        "episode_count": len(records),
        "release_year": year or "Unknown",
        "source_name": source_name,
        "resolution": metadata.get("resolution", "1080p"),
        "hdr": metadata.get("hdr", "SDR"),
        "video_codec": metadata.get("vcodec", "H264"),
        "audio_codec": metadata.get("acodec", "AAC"),
        "audio_channels": metadata.get("achannels", "stereo"),
        "release_dates": date_range,
        "release_group": config["default"].get("releasegroup", ""),
        "network": format_list(online_info.get("network")),
        "genre": format_list(online_info.get("genre")),
        "rating": online_info.get("rating") or "Unknown",
        "cast": format_list(online_info.get("cast")),
        "imdb": online_info.get("imdb") or "N/A",
        "tmdb": online_info.get("tmdb") or "N/A",
        "tvmaze": online_info.get("tvmaze") or "N/A",
        "tvdb": online_info.get("tvdb") or "N/A",
        "series_overview": online_info.get("series_description") or "",
        "series_image": format_image(online_info.get("series_image"), 500),
        "season_image": format_image(online_info.get("season_images", {}).get(season), 500),
        "season_overview": online_info.get("season_description", {}).get(season, ""),
        "episodes_list": build_episode_listing(records),
        "video_profile": metadata.get("vacodec") or metadata.get("vcodec", "H264"),
        "video_bitrate": metadata.get("vbitrate") or "Unknown",
        "audio_sample": metadata.get("asample") or "Unknown",
        "audio_bitrate": metadata.get("abitrate") or "Unknown",
        "duration_average": format_average_duration(metadata.get("duration_avg")),
        "container": metadata.get("container", "MKV"),
        "language": format_language(metadata.get("language")),
        "subtitles": format_subtitles(metadata.get("subtitles")),
        "total_size": format_size(metadata.get("total_size") or 0),
        "episode_details": build_episode_details(records, online_info),
    }
    return context


def build_context_for_series(
    series_name: str,
    seasons: Sequence[int],
    records: Sequence[Dict],
    metadata: Dict,
    online_info: Dict,
    source_name: str,
    config: Dict,
) -> Dict:
    date_range, year = format_date_range(online_info.get("airdates", []))
    if seasons:
        if len(set(seasons)) > 1:
            span = f"S{min(seasons):02d}-S{max(seasons):02d}"
        else:
            span = f"S{seasons[0]:02d}"
    else:
        span = ""
    season_overview = build_season_overview_text(online_info, seasons)
    context = {
        "series": series_name,
        "season_headline": f"Season {span}" if span else "Series",
        "season_span": span or "Unknown",
        "episode_count": len(records),
        "release_year": year or "Unknown",
        "source_name": source_name,
        "resolution": metadata.get("resolution", "1080p"),
        "hdr": metadata.get("hdr", "SDR"),
        "video_codec": metadata.get("vcodec", "H264"),
        "audio_codec": metadata.get("acodec", "AAC"),
        "audio_channels": metadata.get("achannels", "stereo"),
        "release_dates": date_range,
        "release_group": config["default"].get("releasegroup", ""),
        "network": format_list(online_info.get("network")),
        "genre": format_list(online_info.get("genre")),
        "rating": online_info.get("rating") or "Unknown",
        "cast": format_list(online_info.get("cast")),
        "imdb": online_info.get("imdb") or "N/A",
        "tmdb": online_info.get("tmdb") or "N/A",
        "tvmaze": online_info.get("tvmaze") or "N/A",
        "tvdb": online_info.get("tvdb") or "N/A",
        "series_overview": online_info.get("series_description") or "",
        "series_image": format_image(online_info.get("series_image"), 500),
        "season_overview": season_overview,
        "episodes_list": build_episode_listing(records),
        "video_profile": metadata.get("vacodec") or metadata.get("vcodec", "H264"),
        "video_bitrate": metadata.get("vbitrate") or "Unknown",
        "audio_sample": metadata.get("asample") or "Unknown",
        "audio_bitrate": metadata.get("abitrate") or "Unknown",
        "duration_average": format_average_duration(metadata.get("duration_avg")),
        "container": metadata.get("container", "MKV"),
        "language": format_language(metadata.get("language")),
        "subtitles": format_subtitles(metadata.get("subtitles")),
        "total_size": format_size(metadata.get("total_size") or 0),
        "episode_details": build_episode_details(records, online_info),
    }
    return context


def build_context_for_episode(
    record: Dict,
    metadata: Dict,
    online_info: Dict,
    source_name: str,
    config: Dict,
) -> Dict:
    season = record.get("season")
    episode = record.get("episode")
    airdates = online_info.get("airdates", [])
    date_range, year = format_date_range(airdates)
    episode_info = online_info.get("episodes", {}).get(record.get("checksum"), {})
    if date_range == "Unknown" and episode_info.get("airdate"):
        date_range = episode_info["airdate"]
        year = year or episode_info["airdate"][:4]
    episode_code = ""
    if season is not None and episode is not None:
        episode_code = f"S{int(season):02d}E{int(episode):02d}"
    context = {
        "series": record.get("series") or "Unknown",
        "episode_code": episode_code,
        "season_number_display": f"{int(season):02d}" if season is not None else "",
        "episode_number_display": f"{int(episode):02d}" if episode is not None else "",
        "release_year": year or "Unknown",
        "source_name": source_name,
        "resolution": metadata.get("resolution", "1080p"),
        "hdr": metadata.get("hdr", "SDR"),
        "video_codec": metadata.get("vcodec", "H264"),
        "audio_codec": metadata.get("acodec", "AAC"),
        "audio_channels": metadata.get("achannels", "stereo"),
        "release_dates": date_range,
        "release_group": config["default"].get("releasegroup", ""),
        "network": format_list(online_info.get("network")),
        "genre": format_list(online_info.get("genre")),
        "rating": online_info.get("rating") or "Unknown",
        "cast": format_list(online_info.get("cast")),
        "imdb": online_info.get("imdb") or "N/A",
        "tmdb": online_info.get("tmdb") or "N/A",
        "tvmaze": online_info.get("tvmaze") or "N/A",
        "tvdb": online_info.get("tvdb") or "N/A",
        "series_overview": online_info.get("series_description") or "",
        "series_image": format_image(online_info.get("series_image"), 500),
        "episode_image": format_image(online_info.get("episodes", {}).get(record.get("checksum"), {}).get("image"), 450),
        "video_profile": metadata.get("vacodec") or metadata.get("vcodec", "H264"),
        "video_bitrate": metadata.get("vbitrate") or "Unknown",
        "audio_sample": metadata.get("asample") or "Unknown",
        "audio_bitrate": metadata.get("abitrate") or "Unknown",
        "duration_average": record.get("duration") or format_average_duration(metadata.get("duration_avg")),
        "container": metadata.get("container", "MKV"),
        "language": format_language(metadata.get("language")),
        "subtitles": format_subtitles(metadata.get("subtitles")),
        "total_size": record.get("filesize") or format_size(metadata.get("total_size") or 0),
        "episode_description": episode_info.get("description") or record.get("title") or "",
    }
    return context


def build_context_for_movie(
    record: Dict,
    metadata: Dict,
    online_info: Dict,
    source_name: str,
    config: Dict,
) -> Dict:
    release = online_info.get("release")
    date_range, year = format_date_range([release] if release else [])
    context = {
        "movie": record.get("movie") or "Unknown",
        "movie_overview": online_info.get("description") or "",
        "movie_image": format_image(online_info.get("image"), 500),
        "release_year": year or "Unknown",
        "source_name": source_name,
        "resolution": metadata.get("resolution", "1080p"),
        "hdr": metadata.get("hdr", "SDR"),
        "video_codec": metadata.get("vcodec", "H264"),
        "audio_codec": metadata.get("acodec", "AAC"),
        "audio_channels": metadata.get("achannels", "stereo"),
        "release_dates": date_range,
        "release_group": config["default"].get("releasegroup", ""),
        "studio": online_info.get("studio") or "Unknown",
        "genre": format_list(online_info.get("genre")),
        "rating": online_info.get("rating") or "Unknown",
        "cast": format_list(online_info.get("cast")),
        "imdb": online_info.get("imdb") or "N/A",
        "tmdb": online_info.get("tmdb") or "N/A",
        "tvdb": online_info.get("tvdb") or "N/A",
        "video_profile": metadata.get("vacodec") or metadata.get("vcodec", "H264"),
        "video_bitrate": metadata.get("vbitrate") or "Unknown",
        "audio_sample": metadata.get("asample") or "Unknown",
        "audio_bitrate": metadata.get("abitrate") or "Unknown",
        "duration_average": record.get("duration") or format_average_duration(metadata.get("duration_avg")),
        "container": metadata.get("container", "MKV"),
        "language": format_language(metadata.get("language")),
        "subtitles": format_subtitles(metadata.get("subtitles")),
        "total_size": record.get("filesize") or format_size(metadata.get("total_size") or 0),
        "movie_description": online_info.get("description") or "",
    }
    return context


def render_nfo(template_lines: Sequence[str], context: Dict) -> str:
    safe_context = SafeDict(context)
    rendered = [line.format_map(safe_context).rstrip() for line in template_lines]
    while rendered and not rendered[-1]:
        rendered.pop()
    return "\n".join(rendered) + "\n"

def choose_source_name(dlsource: Optional[str], sources: Dict[str, str]) -> str:
    if not dlsource:
        return "UNKNOWN"
    mapped = sources.get(dlsource.lower()) if isinstance(dlsource, str) else None
    if mapped:
        return mapped
    return sanitize_piece(dlsource)


def build_series_folder_name(
    series_name: str,
    seasons: Sequence[int],
    metadata: Dict,
    source_name: str,
    release_tag: str,
) -> str:
    slug = sanitize_piece(series_name)
    if seasons:
        if len(set(seasons)) > 1:
            season_part = f"S{min(seasons):02d}-S{max(seasons):02d}"
        else:
            season_part = f"S{seasons[0]:02d}"
    else:
        season_part = "S00"
    parts = [slug, season_part, metadata.get("resolution", "1080p").lower()]
    if metadata.get("hdr") == "HDR":
        parts.append("HDR")
    parts.append(metadata.get("vcodec", "H264").upper())
    parts.append(source_name)
    parts.append(metadata.get("acodec", "AAC").upper())
    channels = metadata.get("achannels")
    if channels in {"5.1", "7.1"}:
        parts.append(channels)
    base = ".".join(filter(None, parts))
    return f"{base}-{release_tag}"


def build_season_folder_name(
    series_name: str,
    season: int,
    metadata: Dict,
    source_name: str,
    release_tag: str,
) -> str:
    slug = sanitize_piece(series_name)
    season_part = f"S{int(season):02d}"
    parts = [slug, season_part, metadata.get("resolution", "1080p").lower()]
    if metadata.get("hdr") == "HDR":
        parts.append("HDR")
    parts.append(metadata.get("vcodec", "H264").upper())
    parts.append(source_name)
    parts.append(metadata.get("acodec", "AAC").upper())
    channels = metadata.get("achannels")
    if channels in {"5.1", "7.1"}:
        parts.append(channels)
    base = ".".join(filter(None, parts))
    return f"{base}-{release_tag}"


def build_episode_base_name(
    record: Dict,
    metadata: Dict,
    source_name: str,
    release_tag: str,
) -> str:
    slug = sanitize_piece(record.get("series") or "Episode")
    season = record.get("season") or 0
    episode = record.get("episode") or 0
    parts = [slug, f"S{int(season):02d}E{int(episode):02d}"]
    resolution = record.get("resolution") or metadata.get("resolution", "1080p")
    parts.append(str(resolution).lower())
    hdr_value = record.get("hdr") or metadata.get("hdr")
    if normalize_hdr(hdr_value) == "HDR":
        parts.append("HDR")
    parts.append(normalize_video_codec(record.get("vcodec") or metadata.get("vcodec")).upper())
    parts.append(source_name)
    parts.append(normalize_audio_codec(record.get("acodec") or metadata.get("acodec")).upper())
    channels = normalize_channels(record.get("achannels") or metadata.get("achannels"))
    if channels in {"5.1", "7.1"}:
        parts.append(channels)
    base = ".".join(filter(None, parts))
    return f"{base}-{release_tag}"


def build_movie_base_name(
    record: Dict,
    metadata: Dict,
    source_name: str,
    release_tag: str,
) -> str:
    slug = sanitize_piece(record.get("movie") or "Movie")
    parts = [slug, str(metadata.get("resolution", "1080p")).lower()]
    if metadata.get("hdr") == "HDR":
        parts.append("HDR")
    parts.append(metadata.get("vcodec", "H264").upper())
    parts.append(source_name)
    parts.append(metadata.get("acodec", "AAC").upper())
    channels = metadata.get("achannels")
    if channels in {"5.1", "7.1"}:
        parts.append(channels)
    base = ".".join(filter(None, parts))
    return f"{base}-{release_tag}"

def ensure_directory(path: Path, verbose: bool = False) -> bool:
    try:
        path.mkdir(parents=True, exist_ok=True)
        return True
    except OSError as exc:
        print(f"Error: Could not create directory {path}: {exc}")
        return False


def link_or_copy(src: Path, dest: Path, verbose: bool = False) -> bool:
    if dest.exists():
        return True
    try:
        os.link(src, dest)
        return True
    except OSError:
        try:
            shutil.copy2(src, dest)
            return True
        except OSError as exc:
            if verbose:
                print(f"Error: Failed to copy {src} to {dest}: {exc}")
            return False


def copy_subtitles(
    video_path: Path,
    base_name: str,
    destinations: Sequence[Path],
    verbose: bool = False,
) -> None:
    if not video_path.exists():
        return
    stem = video_path.stem
    for sub_file in video_path.parent.glob(f"{stem}.*"):
        if sub_file.suffix.lower() not in SUBTITLE_EXTENSIONS:
            continue
        suffix_part = sub_file.name[len(stem) :]
        new_name = f"{base_name}{suffix_part}"
        for dest_dir in destinations:
            if ensure_directory(dest_dir, verbose):
                dest_file = dest_dir / new_name
                if dest_file.exists():
                    continue
                if not link_or_copy(sub_file, dest_file, verbose):
                    continue


def write_nfo(content: str, destinations: Sequence[Path], filename: str, verbose: bool = False) -> None:
    for dest_dir in destinations:
        if not ensure_directory(dest_dir, verbose):
            continue
        try:
            (dest_dir / filename).write_text(content, encoding="utf-8")
        except OSError as exc:
            if verbose:
                print(f"Error: Failed to write NFO {dest_dir / filename}: {exc}")


def process_movie(
    record: Dict,
    config: Dict,
    sources: Dict[str, str],
    templates: Dict[str, Sequence[str]],
    upload_base: Path,
    fileflows_base: Path,
    updates: List[Tuple[str, str, str]],
    verbose: bool,
) -> int:
    fileloc = record.get("fileloc")
    if not fileloc:
        if verbose:
            print("Skipping movie with missing file location")
        return 0
    src_path = Path(fileloc)
    if not src_path.exists():
        if verbose:
            print(f"Skipping missing file: {src_path}")
        return 0

    metadata = aggregate_metadata([record])
    online_info = gather_movie_online_info(record)
    release_tag = config["default"].get("filereleasegroup", "REPACK")
    source_name = choose_source_name(record.get("dlsource"), sources)
    base_name = build_movie_base_name(record, metadata, source_name, release_tag)
    folder_name = base_name
    filename = f"{base_name}{infer_extension(record)}"

    upload_dir = upload_base / folder_name
    fileflows_dir = fileflows_base / folder_name

    if not ensure_directory(upload_dir, verbose):
        return 0
    ensure_directory(fileflows_dir, verbose)

    upload_path = upload_dir / filename
    if not link_or_copy(src_path, upload_path, verbose):
        return 0
    link_or_copy(src_path, fileflows_dir / filename, verbose)

    copy_subtitles(src_path, base_name, [upload_dir, fileflows_dir], verbose)

    nfo_content = render_nfo(
        templates["movie"],
        build_context_for_movie(record, metadata, online_info, source_name, config),
    )
    write_nfo(nfo_content, [upload_dir, fileflows_dir], f"{base_name}.nfo", verbose)

    updates.append((str(upload_path), filename, record["checksum"]))
    if verbose:
        print(f"Processed movie: {base_name}")
    return 1


def process_episode(
    record: Dict,
    config: Dict,
    sources: Dict[str, str],
    templates: Dict[str, Sequence[str]],
    upload_base: Path,
    fileflows_base: Path,
    updates: List[Tuple[str, str, str]],
    verbose: bool,
) -> int:
    fileloc = record.get("fileloc")
    if not fileloc:
        if verbose:
            print("Skipping episode with missing file location")
        return 0
    src_path = Path(fileloc)
    if not src_path.exists():
        if verbose:
            print(f"Skipping missing file: {src_path}")
        return 0

    metadata = aggregate_metadata([record])
    online_info = gather_online_info([record])
    release_tag = config["default"].get("filereleasegroup", "REPACK")
    source_name = choose_source_name(record.get("dlsource"), sources)
    base_name = build_episode_base_name(record, metadata, source_name, release_tag)
    folder_name = base_name
    filename = f"{base_name}{infer_extension(record)}"

    upload_dir = upload_base / folder_name
    fileflows_dir = fileflows_base / folder_name

    if not ensure_directory(upload_dir, verbose):
        return 0
    ensure_directory(fileflows_dir, verbose)

    upload_path = upload_dir / filename
    if not link_or_copy(src_path, upload_path, verbose):
        return 0
    link_or_copy(src_path, fileflows_dir / filename, verbose)

    copy_subtitles(src_path, base_name, [upload_dir, fileflows_dir], verbose)

    nfo_content = render_nfo(
        templates["episode"],
        build_context_for_episode(record, metadata, online_info, source_name, config),
    )
    write_nfo(nfo_content, [upload_dir, fileflows_dir], f"{base_name}.nfo", verbose)

    updates.append((str(upload_path), filename, record["checksum"]))
    if verbose:
        print(f"Processed episode: {base_name}")
    return 1


def process_season_group(
    series_name: str,
    season: int,
    records: Sequence[Dict],
    config: Dict,
    sources: Dict[str, str],
    templates: Dict[str, Sequence[str]],
    upload_base: Path,
    fileflows_base: Path,
    updates: List[Tuple[str, str, str]],
    verbose: bool,
) -> int:
    existing_records = [record for record in records if record.get("fileloc") and Path(record["fileloc"]).exists()]
    if not existing_records:
        if verbose:
            print(f"No files found for season {series_name} S{int(season):02d}")
        return 0

    metadata = aggregate_metadata(existing_records)
    online_info = gather_online_info(existing_records)
    release_tag = config["default"].get("filereleasegroup", "REPACK")
    source_name = choose_source_name(existing_records[0].get("dlsource"), sources)
    folder_name = build_season_folder_name(series_name, season, metadata, source_name, release_tag)
    upload_dir = upload_base / folder_name
    fileflows_dir = fileflows_base / folder_name

    if not ensure_directory(upload_dir, verbose):
        return 0
    ensure_directory(fileflows_dir, verbose)

    context = build_context_for_season(
        series_name,
        season,
        existing_records,
        metadata,
        online_info,
        source_name,
        config,
    )
    nfo_content = render_nfo(templates["season"], context)
    write_nfo(nfo_content, [upload_dir, fileflows_dir], f"{folder_name}.nfo", verbose)

    processed = 0
    for record in existing_records:
        src_path = Path(record["fileloc"])
        base_name = build_episode_base_name(record, metadata, source_name, release_tag)
        filename = f"{base_name}{infer_extension(record)}"
        upload_path = upload_dir / filename
        if not link_or_copy(src_path, upload_path, verbose):
            continue
        link_or_copy(src_path, fileflows_dir / filename, verbose)
        copy_subtitles(src_path, base_name, [upload_dir, fileflows_dir], verbose)
        updates.append((str(upload_path), filename, record["checksum"]))
        processed += 1

    if processed and verbose:
        print(f"Processed season: {folder_name} ({processed} files)")
    return processed


def process_series_group(
    series_name: str,
    records: Sequence[Dict],
    config: Dict,
    sources: Dict[str, str],
    templates: Dict[str, Sequence[str]],
    upload_base: Path,
    fileflows_base: Path,
    updates: List[Tuple[str, str, str]],
    verbose: bool,
) -> int:
    existing_records = [record for record in records if record.get("fileloc") and Path(record["fileloc"]).exists()]
    if not existing_records:
        if verbose:
            print(f"No files found for series {series_name}")
        return 0

    metadata = aggregate_metadata(existing_records)
    online_info = gather_online_info(existing_records)
    seasons = [record.get("season") or 0 for record in existing_records]
    release_tag = config["default"].get("filereleasegroup", "REPACK")
    source_name = choose_source_name(existing_records[0].get("dlsource"), sources)

    series_folder = build_series_folder_name(series_name, seasons, metadata, source_name, release_tag)
    upload_series_dir = upload_base / series_folder
    fileflows_series_dir = fileflows_base / series_folder

    if not ensure_directory(upload_series_dir, verbose):
        return 0
    ensure_directory(fileflows_series_dir, verbose)

    context = build_context_for_series(
        series_name,
        seasons,
        existing_records,
        metadata,
        online_info,
        source_name,
        config,
    )
    nfo_content = render_nfo(templates["series"], context)
    write_nfo(nfo_content, [upload_series_dir, fileflows_series_dir], f"{series_folder}.nfo", verbose)

    processed = 0
    season_map: Dict[int, List[Dict]] = defaultdict(list)
    for record in existing_records:
        season_value = record.get("season") or 0
        season_map[int(season_value)].append(record)

    for season, season_records in sorted(season_map.items()):
        season_metadata = aggregate_metadata(season_records)
        season_folder = build_season_folder_name(series_name, season, season_metadata, source_name, release_tag)
        upload_dir = upload_series_dir / season_folder
        fileflows_dir = fileflows_series_dir / season_folder
        ensure_directory(upload_dir, verbose)
        ensure_directory(fileflows_dir, verbose)
        for record in season_records:
            src_path = Path(record["fileloc"])
            base_name = build_episode_base_name(record, season_metadata, source_name, release_tag)
            filename = f"{base_name}{infer_extension(record)}"
            upload_path = upload_dir / filename
            if not link_or_copy(src_path, upload_path, verbose):
                continue
            link_or_copy(src_path, fileflows_dir / filename, verbose)
            copy_subtitles(src_path, base_name, [upload_dir, fileflows_dir], verbose)
            updates.append((str(upload_path), filename, record["checksum"]))
            processed += 1

    if processed and verbose:
        print(f"Processed series: {series_folder} ({processed} files)")
    return processed

def to_int(value: Optional[object]) -> Optional[int]:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def fetch_records(conn: sqlite3.Connection, default_type: str) -> List[Dict]:
    conn.row_factory = sqlite3.Row
    import_rows = conn.execute("SELECT * FROM import").fetchall()
    online_rows = conn.execute("SELECT * FROM online").fetchall()
    online_map = {row["checksum"]: dict(row) for row in online_rows}

    records: List[Dict] = []
    for row in import_rows:
        record = dict(row)
        record["season"] = to_int(record.get("season"))
        record["episode"] = to_int(record.get("episode"))
        torrent_type = (record.get("torrenttype") or default_type or "season").lower()
        record["torrenttype"] = torrent_type
        record["online"] = online_map.get(record["checksum"], {})
        records.append(record)
    return records

def process_all_records(
    records: Sequence[Dict],
    config: Dict,
    sources: Dict[str, str],
    templates: Dict[str, Sequence[str]],
    verbose: bool,
) -> Tuple[List[Tuple[str, str, str]], int]:
    updates: List[Tuple[str, str, str]] = []
    processed = 0

    if not records:
        return updates, processed

    upload_locations = config["locations"]["file_upload"]
    fileflows_locations = config["locations"]["fileflows"]
    upload_movies = Path(upload_locations["movies"]).expanduser()
    upload_tv = Path(upload_locations["tv_shows"]).expanduser()
    fileflows_movies = Path(fileflows_locations["movies"]).expanduser()
    fileflows_tv = Path(fileflows_locations["tv_shows"]).expanduser()

    movies: List[Dict] = []
    episodes: List[Dict] = []
    season_groups: Dict[Tuple[str, int], List[Dict]] = defaultdict(list)
    series_groups: Dict[str, List[Dict]] = defaultdict(list)

    for record in records:
        torrent_type = record.get("torrenttype", "season").lower()
        if torrent_type == "movie":
            movies.append(record)
        elif torrent_type == "episode":
            episodes.append(record)
        elif torrent_type == "series":
            series_name = record.get("series") or "Unknown Series"
            series_groups[series_name].append(record)
        else:
            series_name = record.get("series") or "Unknown Series"
            season_value = record.get("season") or 0
            season_groups[(series_name, int(season_value))].append(record)

    for record in movies:
        processed += process_movie(
            record,
            config,
            sources,
            templates,
            upload_movies,
            fileflows_movies,
            updates,
            verbose,
        )

    for series_name, series_records in series_groups.items():
        processed += process_series_group(
            series_name,
            series_records,
            config,
            sources,
            templates,
            upload_tv,
            fileflows_tv,
            updates,
            verbose,
        )

    for (series_name, season), group_records in season_groups.items():
        processed += process_season_group(
            series_name,
            season,
            group_records,
            config,
            sources,
            templates,
            upload_tv,
            fileflows_tv,
            updates,
            verbose,
        )

    for record in episodes:
        processed += process_episode(
            record,
            config,
            sources,
            templates,
            upload_tv,
            fileflows_tv,
            updates,
            verbose,
        )

    return updates, processed

def load_templates_map() -> Dict[str, List[str]]:
    return {name: load_template(name) for name in TEMPLATE_FILENAMES}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Prepare release folders and metadata")
    parser.add_argument("-v", "--verbose", action="store_true", help="Enable verbose output")
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    config = load_config()
    sources = load_sources()
    templates = load_templates_map()

    if not DB_PATH.exists():
        print(f"Error: Database not found at {DB_PATH}")
        return

    default_type = config["default"].get("torrenttype", "season")

    with sqlite3.connect(str(DB_PATH)) as conn:
        records = fetch_records(conn, default_type)
        if not records:
            print("No records found")
            return

        updates, processed = process_all_records(records, config, sources, templates, args.verbose)

        if updates:
            cursor = conn.cursor()
            cursor.executemany(
                "UPDATE import SET newloc = ?, newname = ? WHERE checksum = ?",
                updates,
            )
            conn.commit()

    if processed:
        print(f"Processed {processed} files")
    else:
        print("No files processed")


if __name__ == "__main__":
    main()
