media.py

ffmpeg and media info dump.

First we run ffmpeg, and then media info unless specified (bitrate). If ffmpeg did not pick it up then check media dump. We run this for each checksum. Open tapedeck.db.

Folder Layout
bekindrewind.py
tapedeck.db
user.json
scripts (folder)
- import.py
- database.py
- media.py
- online.py
- api.py
- prep.py
preferences(folder)
- torrentsites.json
- sources.json
- series.json
- season.json
- episode.json
- 
table - import

rows  - checksums (prefilled)

columns -
resolution: - resolution (ffmpeg) (2160p1080p,720p,576p,480p or sd)
hdr: - hdr or sdr (ffmpeg) (SDR/HDR, HDR 10) 
vcodec: basic video codec (ffmpeg) (h264,h265,av1,vp9,etc)
vlevel: advanced video codec (avc, high low, l4,  etc) 
vbitrate: video bitrate in Mpbs (mediainfo) (76.12 Mpbs,7.84 Mpbs,etc)
acodec: audio codec (ffmpeg) (eac3,opus,atomos,flac,truehd,etc)
abitrate: audio bitrate in kbps (ffmpeg) (640 kbps, 320kbps, 224 kpbs, etc)
achannels: audio channels (ffmpeg) (mono,stereo,5.1,7.1, etc)
asample: audio sample rate in kHz (ffmpeg) (44.1 kHz, 48 kHz, etc)
filesize: file size in MB (ffmpeg) (4294 MB,etc)
duration: duration in full minutes (ffmpeg) (22 minutes)
language: audio channel language, falls back on subtitles language, falls back English (ffmpeg) (mediainfo)
subtitles: internal, external, both (would be with file in .srt, etc or in embedded in file) (ffmpeg) 

