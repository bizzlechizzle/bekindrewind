#!/usr/bin/env python3

import argparse
import json
import os
import shutil
import sqlite3
from pathlib import Path

def get_data():
    config_path = Path(__file__).parent.parent / "user.json"
    if not config_path.exists():
        print(f"Error: {config_path} not found")
        return None, None, [], [], {}

    with open(config_path, 'r') as f:
        config = json.load(f)

    sources_path = Path(__file__).parent.parent / "preferences" / "sources.json"
    if not sources_path.exists():
        print(f"Error: {sources_path} not found")
        return None, None, [], [], {}

    with open(sources_path, 'r') as f:
        sources = {k.lower(): v for k, v in json.load(f).items()}

    db_path = Path(__file__).parent.parent / "tapedeck.db"
    if not db_path.exists():
        print(f"Error: {db_path} not found")
        return None, None, [], [], {}

    conn = sqlite3.connect(str(db_path))
    cursor = conn.cursor()

    cursor.execute("PRAGMA table_info(import)")
    import_cols = [row[1] for row in cursor.fetchall()]

    cursor.execute("PRAGMA table_info(online)")
    online_cols = [row[1] for row in cursor.fetchall()]

    cursor.execute("SELECT * FROM import")
    records = cursor.fetchall()

    online_data = {}
    for record in records:
        checksum = record[0]
        cursor.execute("SELECT * FROM online WHERE checksum = ?", (checksum,))
        online_row = cursor.fetchone()
        if online_row:
            online_data[checksum] = dict(zip(online_cols, online_row))

    conn.close()
    return config, sources, records, import_cols, online_data

def make_name(record, import_cols, sources, config):
    data = dict(zip(import_cols, record))
    source = sources.get(data.get('dlsource', '').lower(), data.get('dlsource', 'UNKNOWN'))
    group = config['default']['filereleasegroup']
    torrent_type = config['default']['torrenttype']
    ext = Path(data.get('filename', '')).suffix or '.mkv'

    resolution = data.get('resolution') or '1080p'
    vcodec = (data.get('vcodec') or 'H264').upper()
    acodec = (data.get('acodec') or 'EAC3').upper()
    hdr = data.get('hdr') or 'SDR'
    achannels = data.get('achannels') or 'stereo'

    if 'movie' in import_cols:
        title = (data.get('movie') or 'Unknown').replace(' ', '.').replace('_', '.')
        parts = [title, resolution]
        if hdr.upper() != 'SDR':
            parts.append(hdr.upper())
        parts.extend([vcodec, source, acodec])
        if achannels in ['5.1', '7.1']:
            parts.append(achannels)
        base = '.'.join(parts)
        return f"{base}-{group}{ext}", f"{base}-{group}"

    else:
        series = (data.get('series') or 'Unknown').replace(' ', '.').replace('_', '.')
        season = f"S{data.get('season', 1):02d}"

        if torrent_type == "episode":
            episode = f"E{data.get('episode', 1):02d}"
            parts = [series, f"{season}{episode}", resolution]
            if hdr.upper() != 'SDR':
                parts.append(hdr.upper())
            parts.extend([vcodec, source, acodec])
            if achannels in ['5.1', '7.1']:
                parts.append(achannels)
            folder_base = '.'.join(parts)
            return f"{folder_base}-{group}{ext}", f"{folder_base}-{group}"
        else:
            folder_parts = [series, season, resolution]
            if hdr.upper() != 'SDR':
                folder_parts.append(hdr.upper())
            folder_parts.extend([vcodec, source, acodec])
            if achannels in ['5.1', '7.1']:
                folder_parts.append(achannels)
            folder_base = '.'.join(folder_parts)

            episode = f"E{data.get('episode', 1):02d}"
            file_parts = [series, f"{season}{episode}", resolution]
            if hdr.upper() != 'SDR':
                file_parts.append(hdr.upper())
            file_parts.extend([vcodec, source, acodec])
            if achannels in ['5.1', '7.1']:
                file_parts.append(achannels)
            file_base = '.'.join(file_parts)

            return f"{file_base}-{group}{ext}", f"{folder_base}-{group}"

def create_nfo(records, import_cols, online_data, config):
    if not records:
        return None

    first_record = dict(zip(import_cols, records[0]))
    online = online_data.get(first_record['checksum'], {})

    series = first_record.get('series', 'Unknown')
    season = first_record.get('season', 1)

    nfo = []

    nfo.append('----------------------------------------------------------------')
    nfo.append(f'                    {series} - Season {season}')
    nfo.append('----------------------------------------------------------------')
    nfo.append('')

    if online.get('dseries'):
        nfo.append(online['dseries'])
        nfo.append('')

    if online.get('iseries'):
        nfo.append(f'<div style="text-align:center;"><img src="{online["iseries"]}" style="max-width:500px;"></div>')
        nfo.append('')

    nfo.append(f'Series Name : {series}')
    nfo.append(f'Season: Season {season}')
    nfo.append(f'Episodes : {len(records)}')

    if online.get('airdate'):
        year = online['airdate'][:4] if online['airdate'] else 'Unknown'
        nfo.append(f'Year : {year}')

    nfo.append(f'Source : {first_record.get("dlsource", "Unknown")} Web Download')
    nfo.append(f'Resolution : {first_record.get("resolution", "Unknown")}')
    nfo.append(f'Video Codec : {(first_record.get("vcodec") or "Unknown").upper()}')
    nfo.append(f'Audio Codec : {(first_record.get("acodec") or "Unknown").upper()}')
    nfo.append(f'Audio Channels : {(first_record.get("achannels") or "Unknown").title()}')

    if online.get('airdate'):
        nfo.append(f'Release Date : {online["airdate"]}')

    nfo.append(f'Release Group : [pleaserewind]')
    nfo.append('')

    if online.get('network'):
        nfo.append(f'Network: {online["network"]}')
    if online.get('genre'):
        nfo.append(f'Genre: {online["genre"]}')
    if online.get('rating'):
        nfo.append(f'Rating: {online["rating"]}')
    if online.get('cast'):
        nfo.append(f'Cast: {online["cast"]}')
    nfo.append('')

    if online.get('imdb'):
        nfo.append(f'IMDB: {online["imdb"]}')
    if online.get('tmdb'):
        nfo.append(f'TMDB: {online["tmdb"]}')
    if online.get('tvmaze'):
        nfo.append(f'TVMAZE: {online["tvmaze"]}')
    if online.get('tvdb'):
        nfo.append(f'THETVDB: {online["tvdb"]}')
    nfo.append('')

    nfo.append('----------------------------------------------------------------')
    nfo.append('                    Episodes Included')
    nfo.append('----------------------------------------------------------------')
    nfo.append('')

    if online.get('dseason'):
        nfo.append(online['dseason'])
        nfo.append('')

    for record in sorted(records, key=lambda x: x[import_cols.index('episode')]):
        data = dict(zip(import_cols, record))
        ep_num = f'S{data["season"]:02d}E{data["episode"]:02d}'
        title = data.get('title', 'Unknown')
        nfo.append(f'{ep_num} - {title}')
    nfo.append('')

    nfo.append('----------------------------------------------------------------')
    nfo.append('                    Technical Info')
    nfo.append('----------------------------------------------------------------')
    nfo.append('')

    nfo.append('Video')
    nfo.append(f'Codec : {first_record.get("vacodec", first_record.get("vcodec", "Unknown"))}')
    nfo.append(f'Bitrate : {first_record.get("vbitrate", "Unknown")}')
    nfo.append('')

    nfo.append('Audio')
    nfo.append(f'Sampling Rate : {first_record.get("asample", "Unknown")}')
    nfo.append(f'Bit Rate : {first_record.get("abitrate", "Unknown")}')
    nfo.append('')

    if first_record.get('duration'):
        nfo.append(f'Duration : {first_record["duration"]}')
    if first_record.get('language'):
        nfo.append(f'Language : {(first_record.get("language") or "English").title()}')
    if first_record.get('subtitles'):
        nfo.append(f'Subtitles : {(first_record.get("subtitles") or "None").title()}')

    total_size = 0
    for r in records:
        data = dict(zip(import_cols, r))
        size_str = data.get('filesize')
        if size_str:
            if 'MB' in size_str:
                total_size += float(size_str.replace(' MB', ''))
            elif 'GB' in size_str:
                total_size += float(size_str.replace(' GB', '')) * 1024
            elif isinstance(size_str, str) and size_str.replace('.', '').isdigit():
                total_size += float(size_str)

    if total_size > 0:
        nfo.append(f'Total Size: {total_size:.0f} MB')
    nfo.append('')

    nfo.append('----------------------------------------------------------------')
    nfo.append('                    Episode Details')
    nfo.append('----------------------------------------------------------------')
    nfo.append('')

    sorted_records = sorted(records, key=lambda x: x[import_cols.index('episode')])
    for record in sorted_records:
        data = dict(zip(import_cols, record))
        ep_online = online_data.get(data['checksum'], {})

        nfo.append(f'Episode {data["episode"]} - {data.get("title", "Unknown")}')

        if ep_online.get('iepisode'):
            nfo.append(f'<div style="text-align:center;"><img src="{ep_online["iepisode"]}" style="max-width:400px;"></div>')

        if ep_online.get('airdate'):
            nfo.append(f'Air Date: {ep_online["airdate"]}')

        if data.get('duration'):
            nfo.append(f'Duration: {data["duration"]}')

        if data.get('filesize'):
            size_str = data['filesize']
            if size_str:
                if 'MB' in size_str:
                    nfo.append(f'Size: {size_str}')
                elif isinstance(size_str, str) and size_str.replace('.', '').isdigit():
                    size = float(size_str)
                    nfo.append(f'Size: {size:.0f} MB')

        if ep_online.get('depisode'):
            nfo.append(f'Description: {ep_online["depisode"]}')

        nfo.append('')

    nfo.append('----------------------------------------------------------------')
    nfo.append('                    Be Kind, Rewind')
    nfo.append('----------------------------------------------------------------')

    return '\n'.join(nfo)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()

    config, sources, records, import_cols, online_data = get_data()

    if config is None:
        return

    if not records:
        print("No records found")
        return

    is_movie = 'movie' in import_cols
    upload_dir = config['locations']['file_upload']['movies' if is_movie else 'tv_shows']
    fileflows_dir = config['locations']['fileflows']['movies' if is_movie else 'tv_shows']

    if is_movie:
        for record in records:
            checksum = record[0]
            data = dict(zip(import_cols, record))

            if not Path(data['fileloc']).exists():
                if args.verbose:
                    print(f"Skipping missing file: {data['fileloc']}")
                continue

            file_name, folder_name = make_name(record, import_cols, sources, config)
            folder_path = Path(upload_dir) / folder_name
            try:
                folder_path.mkdir(parents=True, exist_ok=True)
            except (OSError, IOError) as e:
                print(f"Error creating directory {folder_path}: {e}")
                continue

            fileflows_folder_path = Path(fileflows_dir) / folder_name
            try:
                fileflows_folder_path.mkdir(parents=True, exist_ok=True)
            except (OSError, IOError) as e:
                if args.verbose:
                    print(f"Warning: Could not create fileflows directory {fileflows_folder_path}: {e}")

            new_file_path = folder_path / file_name
            fileflows_file_path = fileflows_folder_path / file_name

            if not new_file_path.exists():
                try:
                    os.link(data['fileloc'], new_file_path)
                except (OSError, IOError):
                    shutil.copy2(data['fileloc'], new_file_path)
            if not fileflows_file_path.exists():
                try:
                    os.link(data['fileloc'], fileflows_file_path)
                except (OSError, IOError):
                    shutil.copy2(data['fileloc'], fileflows_file_path)

            db_path = Path(__file__).parent.parent / "tapedeck.db"
            conn = sqlite3.connect(str(db_path))
            cursor = conn.cursor()
            cursor.execute("UPDATE import SET newloc = ? WHERE checksum = ?", (str(new_file_path), checksum))
            conn.commit()
            conn.close()

            if args.verbose:
                print(f"Processed: {folder_name}")

    else:
        season_groups = {}
        for record in records:
            data = dict(zip(import_cols, record))
            key = (data['series'], data['season'])
            if key not in season_groups:
                season_groups[key] = []
            season_groups[key].append(record)

        for (series, season), group_records in season_groups.items():
            first_record = group_records[0]
            _, folder_name = make_name(first_record, import_cols, sources, config)

            folder_path = Path(upload_dir) / folder_name
            try:
                folder_path.mkdir(parents=True, exist_ok=True)
            except (OSError, IOError) as e:
                print(f"Error creating directory {folder_path}: {e}")
                continue

            fileflows_folder_path = Path(fileflows_dir) / folder_name
            try:
                fileflows_folder_path.mkdir(parents=True, exist_ok=True)
            except (OSError, IOError) as e:
                if args.verbose:
                    print(f"Warning: Could not create fileflows directory {fileflows_folder_path}: {e}")

            nfo_content = create_nfo(group_records, import_cols, online_data, config)
            if nfo_content:
                nfo_file = folder_path / f"{folder_name}.nfo"
                fileflows_nfo_file = fileflows_folder_path / f"{folder_name}.nfo"
                with open(nfo_file, 'w') as f:
                    f.write(nfo_content)
                with open(fileflows_nfo_file, 'w') as f:
                    f.write(nfo_content)

            db_path = Path(__file__).parent.parent / "tapedeck.db"
            conn = sqlite3.connect(str(db_path))
            cursor = conn.cursor()

            for record in group_records:
                checksum = record[0]
                data = dict(zip(import_cols, record))

                if not Path(data['fileloc']).exists():
                    if args.verbose:
                        print(f"Skipping missing file: {data['fileloc']}")
                    continue

                file_name, _ = make_name(record, import_cols, sources, config)
                new_file_path = folder_path / file_name
                fileflows_file_path = fileflows_folder_path / file_name

                if not new_file_path.exists():
                    try:
                        os.link(data['fileloc'], new_file_path)
                    except (OSError, IOError):
                        shutil.copy2(data['fileloc'], new_file_path)
                if not fileflows_file_path.exists():
                    try:
                        os.link(data['fileloc'], fileflows_file_path)
                    except (OSError, IOError):
                        shutil.copy2(data['fileloc'], fileflows_file_path)

                video_path = Path(data['fileloc'])
                for sub_file in video_path.parent.glob(f"{video_path.stem}.*"):
                    if sub_file.suffix.lower() in ['.srt', '.vtt', '.ass']:
                        ep_data = dict(zip(import_cols, record))
                        ep_season = f"S{ep_data['season']:02d}"
                        ep_episode = f"E{ep_data['episode']:02d}"
                        sub_name = f"{folder_name}.{ep_season}{ep_episode}{sub_file.suffix}"
                        sub_dst = folder_path / sub_name
                        fileflows_sub_dst = fileflows_folder_path / sub_name
                        if not sub_dst.exists():
                            try:
                                os.link(sub_file, sub_dst)
                            except (OSError, IOError):
                                shutil.copy2(sub_file, sub_dst)
                        if not fileflows_sub_dst.exists():
                            try:
                                os.link(sub_file, fileflows_sub_dst)
                            except (OSError, IOError):
                                shutil.copy2(sub_file, fileflows_sub_dst)

                cursor.execute("UPDATE import SET newloc = ? WHERE checksum = ?", (str(new_file_path), checksum))

            conn.commit()
            conn.close()

            if args.verbose:
                print(f"Processed: {folder_name} ({len(group_records)} files)")

    print(f"Processed {len(records)} files")

if __name__ == "__main__":
    main()