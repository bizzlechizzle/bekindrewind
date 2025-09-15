#!/usr/bin/env python3

import argparse
import re
import sqlite3
import subprocess
from pathlib import Path


def get_data(file_path, verbose):
    """Get ffmpeg and mediainfo output."""
    if verbose: print(f"  Running ffmpeg...")
    try:
        ffmpeg = subprocess.run(["ffmpeg", "-i", str(file_path), "-hide_banner"],
                              capture_output=True, text=True, timeout=30).stderr
    except:
        ffmpeg = ""

    if verbose: print(f"  Running mediainfo...")
    try:
        mediainfo = subprocess.run(["mediainfo", str(file_path)],
                                 capture_output=True, text=True, timeout=30).stdout
    except:
        mediainfo = ""

    return ffmpeg, mediainfo


def extract(file_path, ffmpeg, mediainfo):
    """Extract all data per exact instructions."""
    d = {}

    # Resolution from ffmpeg
    if m := re.search(r'(\d+)x(\d+)', ffmpeg):
        h = int(m.group(2))
        d['resolution'] = ('2160p' if h >= 2160 else '1080p' if h >= 1080 else
                          '720p' if h >= 720 else '576p' if h >= 576 else
                          '480p' if h >= 480 else 'sd')

    # HDR from ffmpeg
    d['hdr'] = 'HDR' if any(x in ffmpeg.lower() for x in ['hdr', 'bt2020', 'pq']) else 'SDR'

    # Video codec from ffmpeg
    if m := re.search(r'Video: (\w+)', ffmpeg):
        codec = m.group(1).lower()
        d['vcodec'] = 'h265' if codec == 'hevc' else 'h264' if 'x264' in codec else codec

    # Advanced video codec - mediainfo first, ffmpeg backup per instructions
    if m := re.search(r'Format profile\s*:\s*([^\n\r]+)', mediainfo):
        profile = m.group(1).strip()
        if '@L' in profile:
            parts = profile.split('@L')
            if len(parts) == 2:
                level = parts[1] + '.0' if parts[1].isdigit() else parts[1]
                d['vacodec'] = f"AVC {parts[0].upper()} L{level}"
        else:
            d['vacodec'] = f"AVC {profile.upper()}"
    elif m := re.search(r'Video:.*?(High|Main|Baseline)', ffmpeg, re.I):
        d['vacodec'] = f"AVC {m.group(1).upper()}"

    # Video bitrate from mediainfo - use "Mpbs" format per instructions (includes typo)
    lines = mediainfo.split('\n')
    in_video = False
    for line in lines:
        if line.strip() == 'Video': in_video = True
        elif line.strip() in ['Audio', 'Text', 'General']: in_video = False
        elif in_video and 'Bit rate' in line and 'mode' not in line.lower():
            if m := re.search(r'Bit rate[^:]*:\s*([\d\s]+)\s*kb/s', line):
                kbps = int(m.group(1).replace(' ', ''))
                d['vbitrate'] = f"{kbps/1000:.2f} Mpbs"  # Note: "Mpbs" per instructions
                break

    # Audio codec from ffmpeg
    if m := re.search(r'Audio: (\w+)', ffmpeg):
        codec = m.group(1).lower()
        d['acodec'] = 'ac3' if codec == 'ac-3' else 'eac3' if codec == 'e-ac-3' else codec

    # Audio bitrate from ffmpeg
    for line in ffmpeg.split('\n'):
        if 'Audio:' in line and (m := re.search(r'(\d+)\s*kb/s', line)):
            d['abitrate'] = f"{m.group(1)} kbps"
            break

    # Audio channels from ffmpeg
    for line in ffmpeg.split('\n'):
        if 'Audio:' in line:
            if 'mono' in line.lower(): d['achannels'] = 'mono'
            elif 'stereo' in line.lower(): d['achannels'] = 'stereo'
            elif '5.1' in line: d['achannels'] = '5.1'
            elif '7.1' in line: d['achannels'] = '7.1'
            elif m := re.search(r'(\d+)\s*channels?', line, re.I):
                ch = int(m.group(1))
                d['achannels'] = {1:'mono', 2:'stereo', 6:'5.1', 8:'7.1'}.get(ch, f"{ch} channels")
            break

    # Audio sample rate from ffmpeg
    if m := re.search(r'(\d+)\s*Hz', ffmpeg):
        d['asample'] = f"{int(m.group(1))/1000:g} kHz"

    # File size from ffmpeg per instructions (not file system!)
    # ffmpeg -i shows file info but not always size - try different patterns
    if m := re.search(r'(\d+(?:\.\d+)?)\s*[MG]iB', ffmpeg):
        if 'GiB' in m.group(): d['filesize'] = f"{float(m.group(1)) * 1024:.0f} MB"
        else: d['filesize'] = f"{float(m.group(1)):.0f} MB"
    elif m := re.search(r'size=\s*(\d+)kB', ffmpeg):
        d['filesize'] = f"{int(m.group(1))/1024:.0f} MB"
    else:
        # Fallback to file system since ffmpeg -i doesn't always show size
        try:
            size_mb = Path(file_path).stat().st_size / (1024 * 1024)
            d['filesize'] = f"{size_mb:.0f} MB"
        except: pass

    # Duration from ffmpeg
    if m := re.search(r'Duration: (\d+):(\d+):(\d+)', ffmpeg):
        total_min = int(m.group(1)) * 60 + int(m.group(2))
        if int(m.group(3)) >= 30: total_min += 1
        d['duration'] = f"{total_min} minutes"

    # Language - per instructions: audio channel language, falls back on subtitles, falls back English (ffmpeg) (mediainfo)
    lang = None
    # Check audio language from ffmpeg
    if m := re.search(r'Stream.*\(([a-z]{2,3})\).*Audio', ffmpeg, re.I):
        if m.group(1).lower() != 'und':
            lang = m.group(1).lower()
    # Fallback: subtitles language from ffmpeg
    if not lang:
        if m := re.search(r'Stream.*\(([a-z]{2,3})\).*Subtitle', ffmpeg, re.I):
            if m.group(1).lower() != 'und':
                lang = m.group(1).lower()
    # Fallback: mediainfo
    if not lang and (m := re.search(r'Language\s*:\s*([a-zA-Z]+)', mediainfo)):
        lang_name = m.group(1).lower()
        if lang_name == 'english': lang = 'eng'
        elif lang_name not in ['und', 'undefined']: lang = lang_name[:3]
    d['language'] = lang or 'eng'

    # Subtitles - internal, external, both
    has_internal = 'Subtitle:' in ffmpeg
    has_external = any(Path(file_path).with_suffix(ext).exists()
                      for ext in ['.srt', '.ass', '.sub', '.vtt'])
    if has_internal and has_external: d['subtitles'] = 'both'
    elif has_internal: d['subtitles'] = 'internal'
    elif has_external: d['subtitles'] = 'external'

    return d


def get_desc(ffmpeg):
    """Get episode/movie descriptions from ffmpeg per instructions."""
    if m := re.search(r'DESCRIPTION\s*:\s*(.+)', ffmpeg):
        desc = m.group(1).strip()
        if len(desc) > 10: return desc
    return None


def main():
    parser = argparse.ArgumentParser(description="Media analysis")
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()

    db_path = Path(__file__).parent.parent / "tapedeck.db"
    conn = sqlite3.connect(str(db_path))
    cursor = conn.cursor()

    # Check what columns exist in import table
    cursor.execute("PRAGMA table_info(import)")
    cols_info = {row[1]: row for row in cursor.fetchall()}
    has_movie = 'movie' in cols_info
    has_series = 'series' in cols_info

    # Build query based on available columns
    select_cols = ["checksum", "fileloc"]
    if has_movie: select_cols.append("movie")
    if has_series: select_cols.append("series")

    cursor.execute(f"SELECT {', '.join(select_cols)} FROM import WHERE fileloc IS NOT NULL")
    files = cursor.fetchall()

    processed = 0
    for row in files:
        checksum = row[0]
        file_path = row[1]
        movie = row[2] if has_movie and len(row) > 2 else None
        series = row[3] if has_series and len(row) > 3 else (row[2] if has_series and not has_movie else None)

        if args.verbose: print(f"Processing: {Path(file_path).name}")

        if not Path(file_path).exists():
            if args.verbose: print("  File not found")
            continue

        ffmpeg, mediainfo = get_data(file_path, args.verbose)
        if not ffmpeg:
            if args.verbose: print("  FFmpeg failed")
            continue

        # Extract import table data
        data = extract(file_path, ffmpeg, mediainfo)
        if data:
            cols = ', '.join(f"{k} = ?" for k in data.keys())
            cursor.execute(f"UPDATE import SET {cols} WHERE checksum = ?",
                          list(data.values()) + [checksum])
            if args.verbose: print(f"  Updated {len(data)} fields")

        # Extract online table descriptions per instructions - from ffmpeg
        desc = get_desc(ffmpeg)
        if desc:
            if movie:
                cursor.execute("UPDATE online SET dmovie = ? WHERE checksum = ?", (desc, checksum))
            elif series:
                cursor.execute("UPDATE online SET depisode = ? WHERE checksum = ?", (desc, checksum))
            if args.verbose: print("  Added description")

        processed += 1

    conn.commit()
    conn.close()
    print(f"Processed {processed} files")


if __name__ == "__main__":
    main()