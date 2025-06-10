#!/usr/bin/env python3

import json
import sqlite3
import subprocess
import os
import sys
from concurrent.futures import ThreadPoolExecutor

def load_config():
    """Load user preferences from 2jznoshit.json"""
    try:
        with open('2jznoshit.json', 'r') as f:
            return json.load(f).get('miata_info', {}).get('json', False)
    except (FileNotFoundError, json.JSONDecodeError):
        return False

def get_file_path(checksum, conn):
    """Get file path from import_tuner table"""
    cursor = conn.execute("SELECT file_location FROM import_tuner WHERE it_checksum = ?", (checksum,))
    result = cursor.fetchone()
    return result[0] if result and os.path.exists(result[0]) else None

def run_mediainfo(file_path):
    """Run mediainfo and return JSON data"""
    try:
        result = subprocess.run([
            'mediainfo', '--Output=JSON', file_path
        ], capture_output=True, text=True, timeout=30)
        
        return json.loads(result.stdout) if result.returncode == 0 else None
    except (subprocess.TimeoutExpired, json.JSONDecodeError, FileNotFoundError):
        return None

def extract_video_data(media_data, file_path):
    """Extract all required video metadata from mediainfo"""
    if not media_data or 'media' not in media_data:
        return {}
    
    tracks = media_data['media'].get('track', [])
    video_track = audio_track = general_track = None
    subtitle_count = 0
    
    # Single pass to find all tracks
    for track in tracks:
        track_type = track.get('@type', '').lower()
        if track_type == 'video' and not video_track:
            video_track = track
        elif track_type == 'audio' and not audio_track:
            audio_track = track
        elif track_type == 'general' and not general_track:
            general_track = track
        elif track_type == 'text':
            subtitle_count += 1
    
    data = {}
    
    # Video data
    if video_track:
        # Basic codec
        codec = video_track.get('Format', '').upper()
        data['mi_codec_basic'] = 'H264' if codec == 'AVC' else 'H265' if codec == 'HEVC' else codec
        
        # Resolution
        width = video_track.get('Width')
        height = video_track.get('Height')
        if width and height:
            width = ''.join(filter(str.isdigit, str(width)))
            height = ''.join(filter(str.isdigit, str(height)))
            if width and height:
                data['mi_resolution'] = f"{width}x{height}"
        
        # Advanced codec
        parts = []
        if codec == 'AVC':
            parts.append('AVC')
        elif codec == 'HEVC':
            parts.append('HEVC')
        elif codec:
            parts.append(codec)
        
        format_profile = video_track.get('Format_Profile', '')
        format_level = video_track.get('Format_Level', '')
        
        if format_profile:
            parts.append(format_profile.replace('@', ''))
        if format_level:
            level_str = str(format_level)
            parts.append(f"L{level_str}" if level_str.replace('.', '').isdigit() else level_str)
        
        if parts:
            data['mi_codec_adv'] = ' '.join(parts)
        
        # HDR detection
        hdr_format = video_track.get('HDR_Format', '').lower()
        color_primaries = video_track.get('colour_primaries', '').lower()
        transfer_characteristics = video_track.get('transfer_characteristics', '').lower()
        
        if 'dolby vision' in hdr_format or 'dv' in hdr_format:
            data['mi_hdr'] = 'HDR (DV)'
        elif 'hdr10+' in hdr_format:
            data['mi_hdr'] = 'HDR (HDR10+)'
        elif 'hdr10' in hdr_format or 'smpte st 2084' in transfer_characteristics:
            data['mi_hdr'] = 'HDR (HDR10)'
        elif 'hlg' in hdr_format or 'arib std-b67' in transfer_characteristics:
            data['mi_hdr'] = 'HDR (HLG)'
        elif 'bt.2020' in color_primaries or 'rec.2020' in color_primaries:
            data['mi_hdr'] = 'HDR'
        elif 'bt.709' in color_primaries or 'rec.709' in color_primaries:
            data['mi_hdr'] = 'SDR (BT.709)'
        else:
            data['mi_hdr'] = 'SDR'
        
        # Video bitrate
        bit_rate = video_track.get('BitRate')
        if bit_rate:
            try:
                data['mi_vid_br'] = f"{int(bit_rate) // 1000} kbps"
            except (ValueError, TypeError):
                pass
    
    # Audio data
    if audio_track:
        data['mi_aud_codec'] = audio_track.get('Format', '').upper()
        
        # Audio channels
        channels = audio_track.get('Channels')
        if channels:
            try:
                ch_count = int(channels)
                channel_map = {1: 'Mono', 2: 'Stereo', 6: '5.1', 8: '7.1'}
                data['mi_aud_chan'] = channel_map.get(ch_count, f"{ch_count}ch")
            except (ValueError, TypeError):
                pass
        
        # Audio sample rate
        sample_rate = audio_track.get('SamplingRate')
        if sample_rate:
            try:
                data['mi_aud_sr'] = f"{int(sample_rate)} Hz"
            except (ValueError, TypeError):
                pass
        
        # Audio bitrate
        audio_bitrate = audio_track.get('BitRate')
        if audio_bitrate:
            try:
                data['mi_aud_br'] = f"{int(audio_bitrate) // 1000} kbps"
            except (ValueError, TypeError):
                pass
        
        # Language
        data['mi_language'] = audio_track.get('Language', 'eng') or 'eng'
    else:
        data['mi_language'] = 'eng'
    
    # Duration - check multiple sources and formats
    duration = None
    if general_track:
        # Try multiple duration fields
        for field in ['Duration', 'Duration/String3', 'Duration/String']:
            duration_val = general_track.get(field)
            if duration_val:
                try:
                    # Handle different formats
                    if isinstance(duration_val, str):
                        # Remove any non-numeric characters except decimal point
                        clean_val = ''.join(c for c in duration_val if c.isdigit() or c == '.')
                        if clean_val:
                            duration = float(clean_val)
                            # If value is very large, assume it's in milliseconds
                            if duration > 10000:
                                duration = duration / 1000
                            break
                    else:
                        duration = float(duration_val)
                        if duration > 10000:
                            duration = duration / 1000
                        break
                except (ValueError, TypeError):
                    continue
    
    if duration:
        data['mi_ep_dur'] = f"{int(duration)} seconds"
    
    # File size
    try:
        data['mi_size'] = f"{os.path.getsize(file_path) // (1024 * 1024)} MB"
    except OSError:
        pass
    
    # Subtitles
    data['mi_subtitles'] = 'internal' if subtitle_count > 0 else 'null'
    
    return data

def update_database(checksum, data, conn, json_output):
    """Update database with extracted data"""
    if not data:
        return
    
    columns = ', '.join(f"{k} = ?" for k in data.keys())
    values = list(data.values()) + [checksum]
    
    conn.execute(f"UPDATE miata_info SET {columns} WHERE it_checksum = ?", values)
    
    if json_output:
        print(json.dumps({checksum: data}))
    else:
        print(f"Updated {checksum}: {len(data)} fields")

def process_checksum(args):
    """Process single checksum"""
    checksum, json_output = args
    
    conn = sqlite3.connect('danger2manifold.db')
    try:
        file_path = get_file_path(checksum, conn)
        if not file_path:
            return
        
        media_data = run_mediainfo(file_path)
        if not media_data:
            return
        
        data = extract_video_data(media_data, file_path)
        update_database(checksum, data, conn, json_output)
        conn.commit()
    except Exception as e:
        if not json_output:
            print(f"Error processing {checksum}: {e}")
    finally:
        conn.close()

def main():
    json_output = load_config()
    
    if not os.path.exists('danger2manifold.db'):
        print("Database danger2manifold.db not found")
        sys.exit(1)
    
    # Get checksums
    conn = sqlite3.connect('danger2manifold.db')
    try:
        cursor = conn.execute("SELECT it_checksum FROM miata_info WHERE it_checksum IS NOT NULL")
        checksums = [row[0] for row in cursor.fetchall()]
    finally:
        conn.close()
    
    if not checksums:
        print("No checksums found")
        return
    
    # Process in parallel
    max_workers = min(os.cpu_count() or 4, len(checksums))
    args = [(checksum, json_output) for checksum in checksums]
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        list(executor.map(process_checksum, args))

if __name__ == "__main__":
    main()