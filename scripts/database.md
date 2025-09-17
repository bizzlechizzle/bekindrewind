database.py

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

This script builds an empty SQLite DB. If there's an old database, FUCKING delete it FOREVER. The database now creates a unified schema with ALL columns for both movies and TV shows, regardless of flags. This prevents script failures when columns don't exist. The descriptions are for reference only and do not go into the database. We do not expect any specific value types, this is a blank clean, simple database. 


Database Name : tapedeck.db (generated locally, not tracked in git)


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


arguments
-v = verbose mode
-movie = legacy flag (now creates unified schema with all columns)
-tv = legacy flag (now creates unified schema with all columns)


Database

table - import

rows  - empty

columns - (ALL columns always created for unified schema)

checksum
movie
series
season
episode
title
stitle
resolution
hdr
vcodec
vacodec
vbitrate
acodec
abitrate
achannels
asample
filesize
duration
language
subtitles
filename
fileloc
newname
newloc
dlsource
torrentsite
torrenttype
url
uploaded


table - online

rows  - empty

columns - (ALL columns always created for unified schema)

checksum
dmovie
release
studio
dseries
dseason
depisode
airdate
network
genre
rating
cast
imovie
iseries
iseason
iepisode
imdb
tmdb
tvmaze
tvdb