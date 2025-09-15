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

This script builds an empty SQLite DB. If there’s an old database, FUCKING delete it FOREVER. The databases are conditional in generation and ran with an argument on if it is a tvshow or a movie. The descriptions are for reference only and do not go into the database. We do not expect any specific value types, this is a blank clean, simple database. 


Database Name : tapedeck.db


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
-movie = movie
-tv = tv show


Database

table - import

rows  - empty

columns -

checksum
movie (create only if -movie)
series (create only if -tv)
season (create only if -tv)
episode (create only if -tv)
title (create only if -tv)
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


table - online

rows  - empty

columns - 

checksum
dmovie (create only if -movie)
release (create only if -movie)
studio (create only if -movie)
dseries (create only if -tv)
dseason (create only if -tv)
depisode (create only if -tv)
airdate (create only if -tv)
network (create only if -tv)
genre
rating
cast
imovie (create only if -movie)
iseries (create only if -tv)
iseason (create only if -tv)
iepisode (create only if -tv)
imdb
tmdb
tvmaze (create only if -tv)
tvdb (create only if -tv)