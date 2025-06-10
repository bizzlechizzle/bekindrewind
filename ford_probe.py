#!/usr/bin/env python3

import json
import sqlite3
import subprocess
import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed

def load_config():
    """Load user preferences from 2jznoshit.json"""
    try:
        with open('2jznoshit.json', 'r') as f:
            config = json.load(f)
        return config.get('ford_probe', {}).get('json', False)
    except (FileNotFoundError, json.JSONDecodeError):
        return False

def get_file_path(checksum, conn):
    """Get file path from import_tuner table"""
    cursor = conn.execute("SELECT file_location FROM import_tuner WHERE it_checksum = ?", (checksum,))
    result = cursor.fetchone()
    return result[0] if result and os.path.exists(result[0]) else None

def run_ffprobe(file_path):
    """Run ffprobe and return JSON data"""
    try:
        result = subprocess.run([
            'ffprobe', '-v', 'quiet', '-print_format', 'json', 
            '-show_format', '-show_streams', file_path
        ], capture_output=True, text=True, timeout=30)
        
        return json.loads(result.stdout) if result.returncode == 0 else None
    except (subprocess.TimeoutExpired, json.JSONDecodeError, FileNotFoundError):
        return None

def extract_video_data(probe_data, file_path):
    """Extract all required video metadata"""
    if not probe_data or 'streams' not in probe_data:
        return {}
    
    video_stream = audio_stream = None
    subtitle_count = 0
    
    for stream in probe_data['streams']:
        codec_type = stream.get('codec_type')
        if codec_type == 'video' and not video_stream:
            video_stream = stream
        elif codec_type == 'audio' and not audio_stream:
            audio_stream = stream
        elif codec_type == 'subtitle':
            subtitle_count += 1
    
    data = {}
    
    # Video codec basic
    if video_stream:
        codec = video_stream.get('codec_name', '').upper()
        data['ff_codec_basic'] = 'H265' if codec == 'HEVC' else codec
        
        # Resolution
        if 'width' in video_stream and 'height' in video_stream:
            data['ff_resolution'] = f"{video_stream['width']}x{video_stream['height']}"
        
        # Advanced codec - use codec name as wrapper + profile + level
        wrapper = codec.replace('H264', 'AVC').replace('H265', 'HEVC').replace('HEVC', 'HEVC')
        profile = video_stream.get('profile', '')
        level = video_stream.get('level', '')
        
        parts = [wrapper] if wrapper else []
        if profile:
            parts.append(profile)
        if level:
            level_str = str(level)
            if level_str.replace('.', '').isdigit():
                parts.append(f"L{level_str}")
            else:
                parts.append(level_str)
        
        if parts:
            data['ff_codec_adv'] = ' '.join(parts)
        
        # HDR detection
        color_space = video_stream.get('color_space', '').lower()
        color_transfer = video_stream.get('color_transfer', '').lower()
        
        if 'bt2020' in color_space or 'rec2020' in color_space:
            if 'smpte2084' in color_transfer or 'pq' in color_transfer:
                data['ff_hdr'] = 'HDR (HDR10)'
            elif 'hlg' in color_transfer or 'arib-std-b67' in color_transfer:
                data['ff_hdr'] = 'HDR (HLG)'
            else:
                data['ff_hdr'] = 'HDR'
        elif 'bt709' in color_space:
            data['ff_hdr'] = 'SDR (BT.709)'
        else:
            data['ff_hdr'] = 'SDR'
        
        # Video bitrate
        if 'bit_rate' in video_stream:
            data['ff_vid_br'] = f"{int(video_stream['bit_rate']) // 1000} kbps"
        elif probe_data.get('format', {}).get('bit_rate'):
            total_br = int(probe_data['format']['bit_rate'])
            audio_br = int(audio_stream.get('bit_rate', 128000)) if audio_stream else 128000
            data['ff_vid_br'] = f"{max(0, total_br - audio_br) // 1000} kbps"
    
    # Audio data
    if audio_stream:
        data['ff_aud_codec'] = audio_stream.get('codec_name', '').upper()
        
        # Audio channels
        channels = audio_stream.get('channels', 0)
        if channels == 1:
            data['ff_aud_chan'] = 'Mono'
        elif channels == 2:
            data['ff_aud_chan'] = 'Stereo'
        elif channels == 6:
            data['ff_aud_chan'] = '5.1'
        elif channels == 8:
            data['ff_aud_chan'] = '7.1'
        elif channels > 0:
            data['ff_aud_chan'] = f"{channels}ch"
        
        # Audio sample rate and bitrate
        if 'sample_rate' in audio_stream:
            data['ff_aud_sr'] = f"{audio_stream['sample_rate']} Hz"
        if 'bit_rate' in audio_stream:
            data['ff_aud_br'] = f"{int(audio_stream['bit_rate']) // 1000} kbps"
        
        # Language
        tags = audio_stream.get('tags', {})
        data['ff_language'] = tags.get('language', 'eng')
    else:
        data['ff_language'] = 'eng'
    
    # Duration and file size
    format_info = probe_data.get('format', {})
    if 'duration' in format_info:
        data['ff_ep_dur'] = f"{int(float(format_info['duration']))} seconds"
    
    try:
        size_mb = os.path.getsize(file_path) // (1024 * 1024)
        data['ff_size'] = f"{size_mb} MB"
    except OSError:
        pass
    
    # Subtitles
    data['ff_subtitles'] = 'internal' if subtitle_count > 0 else 'null'
    
    return data

def update_database(checksum, data, conn, json_output):
    """Update database with extracted data"""
    if not data:
        return
    
    columns = ', '.join(f"{k} = ?" for k in data.keys())
    values = list(data.values()) + [checksum]
    
    conn.execute(f"UPDATE ford_probe SET {columns} WHERE it_checksum = ?", values)
    
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
        
        probe_data = run_ffprobe(file_path)
        if not probe_data:
            return
        
        data = extract_video_data(probe_data, file_path)
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
        cursor = conn.execute("SELECT it_checksum FROM ford_probe WHERE it_checksum IS NOT NULL")
        checksums = [row[0] for row in cursor.fetchall()]
    except sqlite3.Error as e:
        print(f"Database error: {e}")
        sys.exit(1)
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