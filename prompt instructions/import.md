import.py

Python script based on guess-it that works on any system. Is KISS but bulletproof. Can handle smart naming scenarios. 

File lay out example:

/mnt/projects/downloads/streamfab/videos/‘source’/series/season/video files
/mnt/projects/downloads/streamfab/videos/‘source’/movie/video files

i dont want the unused movie generated when you -tv, i dont want the tv
   only when you -movie

Do not do url extraction or pull from streamfab

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

Opens database.py to create tapedeck.db fills out information based on file names, folders, orginaztion, etc. ONLY. Import from user.json

Does a checksum on each file it imports. tapedeck.db is created is prior step and is completely blank. Copys the checksums to every table for future proofing everything stays organized. 

This script arguments:arguments
database.py arguments
-v = verbose mode
-movie = force movie database (and “torrent type”)
-tv = force tv show

not default arguments:
-loc = different file location than default (new follows command)
-site = different torrent site (new site follows command)
-series = season torrent type
-season = season torrent type
-episode = season torrent type

I dont want the file size imported yet.

Movies “torrenttypes” are always movieswe import StreamFab.log  from 

Sample Training Names NOT SOURCE NAMES:

Breaking.Bad.S05E14.1080p.BluRay.x264-ROVERS.mkv
Stranger.Things.S01E01.Chapter.One.720p.NF.WEB-DL.DDP5.1.x264-NTb.mkv
Game.of.Thrones.S03E09.The.Rains.of.Castamere.1080p.BluRay.x265.HEVC-MZABI.mkv
The.Office.US.S02E12.720p.AMZN.WEBRip.DDP5.1.x264-NTb.mkv
The.Mandalorian.S02E05.2160p.DSNP.WEB-DL.DDP5.1.Atmos.HDR.HEVC-TOMMY.mkv
Better.Call.Saul.S06E07.720p.HDTV.x264-SYNCOPY.mkv
Rick.and.Morty.S04E10.1080p.HMAX.WEB-DL.DDP5.1.x264-PHOENiX.mkv
Chernobyl.S01E03.720p.BluRay.x264-DEMAND.mkv
The.Crown.S04E04.Favourites.1080p.NF.WEB-DL.DDP5.1.x264-NTG.mkv
Peaky.Blinders.S05E06.1080p.BluRay.x265.10bit.HDR.DDP5.1-MiNX.mkv
the.witcher.season2.episode3.1080p.NF.WEB-DL.x264-NTb.mkv
Succession.s03e02.720p.HMAX.WEBRip.x264-GalaxyTV.mkv
Yellowstone.Season.Four.Episode.Five.1080p.AMZN.WEB-DL.DDP5.1.x264-NTG.mkv
Ozark.s2e8.720p.NF.WEBRip.x265-ION10.mkv
Fargo.Season01.Episode07.720p.BluRay.x264-KILLERS.mkv
Black.Mirror.season3.episode1.1080p.NF.WEB-DL.x264-STRiFE.mkv
Loki.S01E01.Sneak.Peek.1080p.DSNP.WEB-DL.x264-GLHF.mkv
Breaking.Bad.S05E14.1080p.BluRay.x264-ROVERS.copy.mkv
Westworld.S02.E04.720p.HMAX.WEBRip.x265-ION10.mkv 
Narcos.Mexico.Season3.Episode10.1080p.NF.WEBRip.x264-NTb.mkv
restored_s01e01_restoring-a-craftsman-bungalow.mkvStone House Revival_S01E01_1700s Master Bed and Library_1.mkvCheaters - Extended Edition_S01E01_Episode 1.mkvCheap Old Houses_S01E01_Sneak Peek_ New York Time Warp.mkv
Stone House Revival_S01E01_1700s Master Bed and Library_1.mkv
restored_s01e04_1901-victorian-bungalow-restoration.mkv

Can handle paths like /Volumes/The\ Iron\ Giant/Archive/Business/Wedding\ Films/2026/5\:26\ -\ Colby\ \&\ Parker/Save\ The\ Date/Highres.mov 





When done importing copy ‘movie’ data or ’series’ ‘season’ episode’ from import table to the matching checksum on online, and api table.


Then import from defaults user .json

table - user

columns - 

checksum: prefilled 
releasegroup: 
filereleasegroup:
torrentsite: 
torrenttype: (series, season, episode, movie)
url:  url from import table
filesource: from import table
filesrc: from sources.json
type: “movie” = movie, tv show = tv show.


we dont fill the title on api - title