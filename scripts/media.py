#!/usr/bin/env python3
"""
KISS Media Analysis Script - BULLETPROOF ffmpeg + mediainfo extraction.
FFmpeg first, MediaInfo for vbitrate. Zero bullshit, maximum accuracy.

Usage: python media.py [-v]
"""

import argparse
import re
import sqlite3
import subprocess
import sys
from pathlib import Path


def get_script_dir():
    """Get directory where script is located for bulletproof path resolution."""
    return Path(__file__).parent


def get_files():
    """Get files from database."""
    db_path = get_script_dir().parent / "tapedeck.db"
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT checksum, fileloc FROM import WHERE fileloc IS NOT NULL")
        files = cursor.fetchall()
        conn.close()
        return files
    except Exception as e:
        print(f"Database error: {e}")
        return []


def run_ffmpeg(file_path):
    """Run ffmpeg and return output."""
    try:
        result = subprocess.run(
            ["ffmpeg", "-i", str(file_path), "-hide_banner"],
            capture_output=True, text=True, timeout=60
        )
        return result.stderr
    except Exception:
        return ""


def run_mediainfo(file_path):
    """Run mediainfo and return output."""
    try:
        result = subprocess.run(
            ["mediainfo", str(file_path)],
            capture_output=True, text=True, timeout=60
        )
        return result.stdout
    except Exception:
        return ""


def extract_resolution(ffmpeg_output):
    """Extract resolution from ffmpeg - KISS."""
    res_match = re.search(r'(\d+)x(\d+)', ffmpeg_output)
    if not res_match:
        return None

    height = int(res_match.group(2))
    if height >= 2160: return '2160p'
    elif height >= 1080: return '1080p'
    elif height >= 720: return '720p'
    elif height >= 576: return '576p'
    elif height >= 480: return '480p'
    else: return 'sd'


def extract_hdr(ffmpeg_output):
    """Extract HDR info from ffmpeg - KISS."""
    hdr_indicators = ['hdr', 'bt2020', 'pq', 'hlg']
    ffmpeg_lower = ffmpeg_output.lower()

    for indicator in hdr_indicators:
        if indicator in ffmpeg_lower:
            return 'HDR'
    return 'SDR'


def extract_vcodec(ffmpeg_output):
    """Extract video codec from ffmpeg - KISS."""
    vcodec_match = re.search(r'Video: (\w+)', ffmpeg_output)
    if not vcodec_match:
        return None

    codec = vcodec_match.group(1).lower()
    # Normalize common codec names
    if codec == 'hevc': return 'h265'
    if codec == 'libx264': return 'h264'
    if codec == 'libx265': return 'h265'
    return codec


def extract_vlevel(mediainfo_output):
    """Extract video level from mediainfo - AVC HIGH L4.0 format."""
    if not mediainfo_output:
        return None

    # Look for Format profile in mediainfo (e.g., "High@L4")
    vlevel_match = re.search(r'Format profile\s*:\s*([^\n\r]+)', mediainfo_output)
    if vlevel_match:
        profile = vlevel_match.group(1).strip()

        # Convert to AVC format: "High@L4" -> "AVC HIGH L4.0"
        if '@L' in profile:
            parts = profile.split('@L')
            if len(parts) == 2:
                level_part = parts[1]
                profile_part = parts[0].upper()

                # Add .0 if level is just a number
                if level_part.isdigit():
                    level_part += '.0'

                return f"AVC {profile_part} L{level_part}"

        # Fallback for other formats
        return f"AVC {profile.upper()}"

    return None


def extract_acodec(ffmpeg_output):
    """Extract audio codec from ffmpeg - KISS."""
    acodec_match = re.search(r'Audio: (\w+)', ffmpeg_output)
    if not acodec_match:
        return None

    codec = acodec_match.group(1).lower()
    # Normalize codec names
    if codec == 'ac-3': return 'ac3'
    if codec == 'e-ac-3': return 'eac3'
    return codec


def extract_abitrate(ffmpeg_output):
    """Extract audio bitrate from ffmpeg - FIXED."""
    # Look for audio bitrate specifically in Audio stream line
    audio_lines = [line for line in ffmpeg_output.split('\n') if 'Audio:' in line]

    for line in audio_lines:
        # Match patterns like "224 kb/s", "640 kb/s"
        abitrate_match = re.search(r'(\d+)\s*kb/s', line)
        if abitrate_match:
            return f"{abitrate_match.group(1)} kbps"

    return None


def extract_achannels(ffmpeg_output):
    """Extract audio channels from ffmpeg - FIXED."""
    # Look for channel info in Audio stream line
    audio_lines = [line for line in ffmpeg_output.split('\n') if 'Audio:' in line]

    for line in audio_lines:
        # Direct channel name matching
        if 'mono' in line.lower(): return 'mono'
        if 'stereo' in line.lower(): return 'stereo'
        if '5.1' in line: return '5.1'
        if '7.1' in line: return '7.1'

        # Fallback: numeric channels
        channels_match = re.search(r'(\d+)\s*channels?', line, re.IGNORECASE)
        if channels_match:
            ch = int(channels_match.group(1))
            if ch == 1: return 'mono'
            elif ch == 2: return 'stereo'
            elif ch == 6: return '5.1'
            elif ch == 8: return '7.1'
            else: return f"{ch} channels"

    return None


def extract_asample(ffmpeg_output):
    """Extract audio sample rate from ffmpeg - KISS."""
    asample_match = re.search(r'(\d+)\s*Hz', ffmpeg_output)
    if not asample_match:
        return None

    hz = int(asample_match.group(1))
    khz = hz / 1000
    return f"{khz:g} kHz"  # Use :g to avoid unnecessary decimals


def extract_filesize(file_path):
    """Extract file size - KISS."""
    try:
        size_mb = Path(file_path).stat().st_size / (1024 * 1024)
        return f"{size_mb:.0f} MB"
    except:
        return None


def extract_duration(ffmpeg_output):
    """Extract duration from ffmpeg - KISS."""
    duration_match = re.search(r'Duration: (\d+):(\d+):(\d+)', ffmpeg_output)
    if not duration_match:
        return None

    hours = int(duration_match.group(1))
    minutes = int(duration_match.group(2))
    seconds = int(duration_match.group(3))

    total_minutes = hours * 60 + minutes
    if seconds >= 30:  # Round up if seconds >= 30
        total_minutes += 1

    return f"{total_minutes} minutes"


def extract_language(ffmpeg_output):
    """Extract language from ffmpeg - IMPROVED."""
    # Look for language codes in audio streams
    lang_patterns = [
        r'Audio:[^(]*\(([a-z]{2,3})\)',  # Audio: eac3 (eng)
        r'Stream.*Audio.*\(([a-z]{2,3})\)',  # Stream info with lang
        r'language\s*:\s*([a-z]{2,3})',    # metadata language
    ]

    for pattern in lang_patterns:
        lang_match = re.search(pattern, ffmpeg_output, re.IGNORECASE)
        if lang_match:
            lang = lang_match.group(1).lower()
            if lang != 'und':  # Skip undefined
                return lang

    return 'eng'  # Default fallback


def extract_subtitles(ffmpeg_output, file_path):
    """Extract subtitle info - COMPREHENSIVE."""
    has_internal = 'Subtitle:' in ffmpeg_output

    # Check for external subtitle files
    subtitle_exts = ['.srt', '.ass', '.sub', '.vtt', '.idx', '.sup']
    file_path = Path(file_path)

    has_external = any(
        file_path.with_suffix(ext).exists() or
        file_path.with_suffix(ext.upper()).exists()
        for ext in subtitle_exts
    )

    if has_internal and has_external:
        return 'both'
    elif has_internal:
        return 'internal'
    elif has_external:
        return 'external'
    else:
        return None


def extract_vbitrate(mediainfo_output):
    """Extract video bitrate from mediainfo - FIXED."""
    # Look for video-specific bitrate (skip Overall and Audio bitrates)
    lines = mediainfo_output.split('\n')
    in_video_section = False

    for line in lines:
        line = line.strip()

        # Track when we're in Video section
        if line == 'Video':
            in_video_section = True
            continue
        elif line in ['Audio', 'Text', 'Menu', 'General', '']:
            in_video_section = False
            continue

        # Look for bitrate in video section
        if in_video_section and 'Bit rate' in line and 'mode' not in line.lower():
            # Match patterns like "Bit rate : 7 001 kb/s"
            vbitrate_match = re.search(r'Bit rate[^:]*:\s*([\d\s]+)\s*kb/s', line)
            if vbitrate_match:
                kbps_str = vbitrate_match.group(1).replace(' ', '')  # Remove spaces
                kbps = int(kbps_str)
                mbps = kbps / 1000
                return f"{mbps:.2f} Mbps"

    return None


def extract_metadata(file_path, verbose=False):
    """Extract all metadata using FFmpeg and MediaInfo."""
    if verbose:
        print(f"  Analyzing: {Path(file_path).name}")

    # Get tool outputs
    ffmpeg_output = run_ffmpeg(file_path)
    if not ffmpeg_output:
        if verbose:
            print("  ERROR: FFmpeg failed")
        return {}

    mediainfo_output = run_mediainfo(file_path)

    # Extract all data
    metadata = {}

    # From FFmpeg
    if res := extract_resolution(ffmpeg_output): metadata['resolution'] = res
    if hdr := extract_hdr(ffmpeg_output): metadata['hdr'] = hdr
    if vcodec := extract_vcodec(ffmpeg_output): metadata['vcodec'] = vcodec
    if vlevel := extract_vlevel(mediainfo_output): metadata['vlevel'] = vlevel
    if acodec := extract_acodec(ffmpeg_output): metadata['acodec'] = acodec
    if abitrate := extract_abitrate(ffmpeg_output): metadata['abitrate'] = abitrate
    if achannels := extract_achannels(ffmpeg_output): metadata['achannels'] = achannels
    if asample := extract_asample(ffmpeg_output): metadata['asample'] = asample
    if duration := extract_duration(ffmpeg_output): metadata['duration'] = duration
    if language := extract_language(ffmpeg_output): metadata['language'] = language
    if subtitles := extract_subtitles(ffmpeg_output, file_path): metadata['subtitles'] = subtitles

    # File system
    if filesize := extract_filesize(file_path): metadata['filesize'] = filesize

    # From MediaInfo
    if mediainfo_output:
        if vbitrate := extract_vbitrate(mediainfo_output): metadata['vbitrate'] = vbitrate

    if verbose:
        print(f"  Extracted {len(metadata)} fields: {list(metadata.keys())}")

    return metadata


def update_database(checksum, metadata, verbose=False):
    """Update database with metadata."""
    if not metadata:
        return False

    try:
        db_path = get_script_dir().parent / "tapedeck.db"
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # Build update query
        fields = [f"{k} = ?" for k in metadata.keys()]
        values = list(metadata.values()) + [checksum]

        cursor.execute(f"UPDATE import SET {', '.join(fields)} WHERE checksum = ?", values)

        if cursor.rowcount > 0:
            conn.commit()
            conn.close()
            return True
        else:
            conn.close()
            if verbose:
                print(f"  WARNING: No rows updated for checksum {checksum[:12]}...")
            return False
    except Exception as e:
        if verbose:
            print(f"  ERROR: Database update failed: {e}")
        return False


def main():
    """Main function."""
    parser = argparse.ArgumentParser(description="KISS Media Analysis - FFmpeg + MediaInfo")
    parser.add_argument("-v", "--verbose", action="store_true", help="Verbose mode")
    args = parser.parse_args()

    # Get files to process
    files = get_files()
    if not files:
        print("No files found in database")
        return

    print(f"Processing {len(files)} files...")

    processed = 0
    failed = 0

    for i, (checksum, file_path) in enumerate(files):
        if args.verbose:
            print(f"[{i+1}/{len(files)}] {Path(file_path).name}")

        # Check file exists
        if not Path(file_path).exists():
            if args.verbose:
                print("  ERROR: File not found")
            failed += 1
            continue

        # Extract metadata
        metadata = extract_metadata(file_path, args.verbose)
        if not metadata:
            if args.verbose:
                print("  ERROR: No metadata extracted")
            failed += 1
            continue

        # Update database
        if update_database(checksum, metadata, args.verbose):
            processed += 1
        else:
            failed += 1

    print(f"Results: {processed} processed, {failed} failed")


if __name__ == "__main__":
    main()