media.py

Rules

1. We are working with Python Scripts
2. Scripts are universal and run on any OS.
3. We always value code that is KISS, yet bulletproof, and verified with ULTRATHINK.
4. We respect the text case of the original folders, online lookups, and API lookups.
5. We follow common normalization for TORRENTING.
6. We dont use emojis or leave un-needed comments.
7. The terminal interface is KISS, nothing extra needed.
8. the database is based on either movies or tv shows, check database.py for more information
9. when troubleshootig check other scripts or .md to understand how they work ONLY REFERENCE 


Overview

We do an ffmpeg dump, fill in the database, then do a media info dump for other/missing data for each 256 Checksum on the import table. We make sure all data is clean and normalized. 

Database Name : tapedeck.db

Folder Layout (for this and other scripts)
autorewind.py
tapedeck.db
user.json
scripts (folder)
- import.py
- database.py 
- media.py (this script)
- online.py
- api.py
- prep.py
preferences(folder)
- torrentsites.json
- sources.json
- series.json
- season.json
- episode.json


arguments
-v = verbose mode

table - import

rows  - checksums (prefilled)

columns -
resolution: - resolution (ffmpeg) (2160p,1080p,720p,576p,480p or sd)
hdr: - hdr or sdr (ffmpeg) (SDR/HDR, HDR 10) 
vcodec: basic video codec (ffmpeg) (h264,h265,av1,vp9,etc)
vacodec: advanced video codec (media info) (ffmpeg (backup)(avc, mpeg, high, low, l4,  etc) 
vbitrate: video bitrate in Mpbs (mediainfo) (76.12 Mpbs,7.84 Mpbs,etc)
acodec: audio codec (ffmpeg) (eac3,opus,atomos,flac,truehd,etc)
abitrate: audio bitrate in kbps (ffmpeg) (640 kbps, 320kbps, 224 kpbs, etc)
achannels: audio channels (ffmpeg) (mono,stereo,5.1,7.1, etc)
asample: audio sample rate in kHz (ffmpeg) (44.1 kHz, 48 kHz, etc)
filesize: file size in MB (ffmpeg) (4294 MB,etc)
duration: duration in full minutes (ffmpeg) (22 minutes)
language: audio channel language, falls back on subtitles language, falls back English (ffmpeg) (mediainfo)
subtitles: internal, external, both (would be with file in .srt, etc or in embedded in file) (ffmpeg) (check same folder as videos for external)



table - online

rows  - checksums (prefilled)

columns - 
depisode - episode description (only if exists) (from ffmpeg, ITS THE LONG DESCRIPTION)
dmovie - movie description (only if exists) (from ffmpeg, ITS THE LONG DESCRIPTION)

