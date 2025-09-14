database.py

give me a python script that creates a new sq3lite database with definitions.

the script is universal and can run on any OS. 

if theres an old database delete it and and make a new one.

New database is called tapedeck.db

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

arguments
-v = verbose mode
-movie = movie
-tv = tv show

i dont want the unused movie generated when you -tv, i dont want the tv
   only when you -movie
table - user

columns - 

checksum: 
releasegroup: 
filereleasegroup:
torrentsite: 
torrenttype: (series, season, episode, movie)
url: url from import table
filesource: from import table 
filesrc: from sources.json
type: determine from import table “movie” = movie, series = series.


table - import

rows  - empty

columns -

movie: - movie title (create only if movie)
series: - series name (create only if tv show)
season: - season number (create only if tv show)
episode: - episode number (create only if tv show)
title: - episode title (create only if tv show)
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
sptitle: special title (if available)(example extended edition, uncut, etc)
filename: - file name
fileloc: - file location
newloc: - new file location after hardlink
filesource: - download source in folder name/layout (amazon, hbo, YouTube, tik Tok, peacock, etc)
url: - download url 
checksum: - sha256 checksum




table - online

rows  - empty

columns - 

checksum - imported from import table sha256 checksum
movie: - movie title (create only if movie)
series: - series name (create only if tv show)
season: - season number (create only if tv show)
episode: - episode number (create only if tv show)
title: - episode title (create only if tv show)
sptitle: - special title (if available)(example extended edition, uncut, etc)
dmovie: - movie description (create only if movie)
dseries: - series descriptione (create only if tv show)
dseason: - season description (create only if tv show)
depisode: - episode description (create only if tv show)
year: - release year
airdate: - (create only if tv show)
release: - (create only if movie)
network: - airing network (create only if tv show)
genre: - genre
rating: - tv or movie audience rating
cast: - top 7 cart
imdb: - imdb number 
tmdb: - the movie database
tvmaze: - tvmaze (create only if tv show)
tvdb: - the tvdb number (create only if tv show)
iseries: - image series (create only if tv show)
iseason: - image season (create only if tv show)
iepisode: - image episode (create only if tv show)
imovie: - image movie poster (create movie) only


table - api

rows  - empty

columns - 

checksum - imported from import table sha256 checksum
movie: - movie title (create only if movie)
series: - series name (create only if tv show)
season: - season number (create only if tv show)
episode: - episode number (create only if tv show)
title: - episode title (create only if tv show)
sptitle: - special title (if available)(example extended edition, uncut, etc)
dmovie: - movie description (create only if movie)
dseries: - series description (create only if tv show)
dseason: - season description (create only if tv show)
depisode: - episode description (create only if tv show)
year: - release year
airdate: - (create only if tv show)
release: - (create only if movie)
network: - airing network (create only if tv show)
genre: - genre
rating: - tv or movie audience rating (tv-g, tvma, rated r, p13, etc)
cast: - top 7 cart
iseries: - image series (create only if tv show)
iseason: - image season (create only if tv show)
iepisode: - image episode (create only if tv show)

