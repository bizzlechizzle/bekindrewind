prep.py

Rules

1. We are working with Python Scripts
2. Scripts are universal and run on any OS.
3. We always value code that is KISS, yet bulletproof, and verified with ULTRATHINK.
4. We respect the text case of the original folders, online lookups, and API lookups.
5. We follow common normalization for TORRENTING.
6. We dont use emojis or leave un-needed comments.
7. The terminal interface is KISS, nothing extra needed.
8. the database is based on either movies or tv shows, check database.py for more information
9. when troubleshootig check other scripts or .md to understand how they work

Overview

We figure out the new torrent friend name of each file, and then make a hardlink of it, to the "file_upload" location in the user.json create and "fileflows" also located there.
 
Once we have the new folders, names, and hard links update the new file location for each checksum (import - newloc)

We check import - dlsource, then sources.json for correct term, Amazon = AMZN.WEB-DL for example

We create .nfo files - i have laid out a seriessample.json as a sample in the preferences folder, make one fully compliant with this script and save is as series.json . it needs to be fully updated. the images should show embeded online so i dont have to manully do screenshots. DO THE FULL BLOWN .NFO DONT BE LAZY. EMBEDED IMAGES CLEAN CLEAN CLEAN. for vbitrate use highest quality file. 



Folder Layout 
autorewind.py 
tapedeck.db
user.json
scripts (folder)
- import.py
- database.py 
- media.py
- online.py
- api.py
- prep.py
- upload.py 
preferences(folder)
- torrentsites.json
- sources.json
- series.json
- season.json
- episode.json


New Series Layout (from user - torrenttype) Folder:

Series:
Series Folder
- Season Folder(s)
    - Videofiles
    - Subtitles

New Season Folder Layout (from user - torrenttype) Folder:
Season Folder(s)
- Video Files
- Subtitles

New Episode Folder Layout (from user - torrenttype) Folder:
Episode Folder(s)
- Video File
- Subtitle

New Movie Folder Layout (from user - torrenttype) Folder:
Movie Folder
- Video File
- Subtitle



NAMING GUIDE

Modifiers/Notes for naming:(import - season) - (import - season) if multiple seasons are present in series [S01-S03]
(import - hdr) only if result returns hdr, always skip if sdr, goes after (import - resolution) 
(import - achannel) only if 5.1 or 7.1 always skip if mono or stereo, goes after “it_aud_c”, keep period in-between “5.1” and “7.1”

Season Folder Example
Fringe.S02.1080p.H264.AMZN.WEB-DL.EAC3-PLZRWD

(import - series).(import - season).(import - resolution).(import - vcodec).(sources.json).(import - acodec)-(user.json - filereleasegroup)


Series Folder Example
The.Bear.S01-S03.2160p.HDR.H265.AMZN.WEB-DL.ATOMOS.5.1-PLZRWD

(import - series).(import - season)-(import - season).(import - resolution).(import - hdr).(import - vcodec).(sources.json).(import - acodec).(import - achannel)-(user.json - filereleasegroup)

Episode Folder Example

Silo.S02E03.2160p.AV1.AMZN.WEB-DL.OPUS.5.1-PLZRWD

(import - series).(import - season import - episode).(import - resolution).(import - vcodec).(sources.json).(import - acodec).(import - achannel)-(user.json - filereleasegroup)


Episode Folder Example

Silo.S02E03.2160p.AV1.AMZN.WEB-DL.OPUS.5.1-PLZRWD

(import - series).(import - season import - episode).(import - resolution).(import - vcodec).(sources.json).(import - acodec).(import - achannel)-(user.json - filereleasegroup)


Movie Example Name
The.Prestige.1080p.HBO.WEB-DL.H265.EAC3-PLZRWD.mkv
(import - movie).(import - resolution).(import - vcodec).(sources.json).(import - acodec)-(user.json - filereleasegroup)


Subtitle/.nfo Example

This.Old.House.S23E12.1080p.AMZN.WEB-DL.H264.EAC3.5.1-PLZRWD.srt

(import - series).(import - season import - episode).(import - resolution).(import - vcodec).(sources.json).(import - acodec).(import - achannel)-(user.json - filereleasegroup).file extention



.NFO - Ive made a sample series file. It should pull the data in and make clean .nfo files. We make it for the torrenttype, one for each season or the series. etc.

update - lets not call them screen shots, lets have the images about each episode. 
