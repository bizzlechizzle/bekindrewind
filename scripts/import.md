import.py

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

We use guess-it to determine if we are working with a tv show or a movie. Once determined we run database.py with the correct arguments (“-movie = movie or -tv = tv show). Database.py always generates a clean SQ3Lite database (tapedeck.db). Once the database is created we run a 256 checksum on each video file and log that in the import table. Log in the Movie Name (and stitle if applicable) or Tv Show name (series, season, episode (title,stitle if applicable) and then the file name (import -filename), file location (import - fileloc), and online file source (the folder the files were found in if applicable (Amazon, Youtube, HBO, etc) (import - filesource). Once complete we copy all of the checksums to the corresponding checksum in each other table with a checksum column. 

THIS SCRIPT DOES NOT VERIFY ANYTHING WITH database.py SO WE CAN UPDATE THAT INDEPENDATNLY. WE ONLY LAUNCH database.py

Folder Layout (for this and other scripts)
autorewind.py
tapedeck.db
user.json
scripts (folder)
- import.py (this script)
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


database.py arguments
-v = verbose mode
-movie = force movie database (and “torrent type”)
-tv = force tv show


import.py arguments
-loc = different file location than default (new location follows command)
-site = different torrent site (new site follows command)
-movie = movie torrenttype
-series = season torrenttype
-season = season torrenttype
-episode = season torrenttype


We open user.json to check for default file location (default “filelocation”) unless argument -loc is ran.
We open user.json to check for default torrent site (default “torrentsite”) unless argument -site is ran. record torrent site to (import - torrentsite)
We open user.json to check for default torrent type (default “torrenttype”) unless argument -movie,-series,-season,-episode is ran. record torrent type to (import - torrenttype)


Can handle paths like /Volumes/The\ Iron\ Giant/Archive/Business/Wedding\ Films/2026/5\:26\ -\ Colby\ \&\ Parker/Save\ The\ Date/Highres.mov 

import.py DOES NOT FILL anything othen than the checksum on online table

