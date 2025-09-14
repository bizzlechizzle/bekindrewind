api.py

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

We do API calls to get more metadata for tv shos or movies

Database Name : tapedeck.db

Folder Layout (for this and other scripts)
autorewind.py
tapedeck.db
user.json
scripts (folder)
- import.py
- database.py 
- media.py 
- online.py
- api.py (this script)
- prep.py
preferences(folder)
- torrentsites.json
- sources.json
- series.json
- season.json
- episode.json


arguments
-v = verbose mode

We call API’s for more data.

We search using import table data only (movie or series/season, etc)

IMBD uses OMBD api keys in user.json

Checks user.json for API keys

Folder Layout
bekindrewind.py
tapedeck.db
user.json
scripts (folder)
- import.py
- database.py
- media.py
- online.py
- api.py (this script)
- prep.py
- upload.py 
preferences(folder)
- torrentsites.json
- sources.json
- series.json
- season.json
- episode.json

We only fill empty columns, or overwite existing values for prefered sources. We can use smart tools to weigh between catagories, such as text length for descriptions, amount of genres (shows usually have 1+), or size of photos for posters. KISS but SMART.


table - online

rows  - checksums (prefilled)

columns - 

dmovie (create only if -movie) (movie description) (info priority: online.py (source lookup)>ffprobe (prefilled when script stats)>TMDB>IMBD)
release (create only if -movie) (movie release date) (info priority nline.py (source lookup)>TMDB>IMBD)
studio (create only if -movie) (movie studio) (info priority: online.py (source lookup)>TMDB>IMBD)
dseries (create only if -tv) (series description) (info priority: online.py (source lookup)>TVDB>TVMAZE>IMBD>TMDB)
dseason (create only if -tv) (season description) (info priority: online.py (source lookup)>TVDB>TVMAZE>IMBD>TMDB)
depisode (create only if -tv) (episode description) (info priority: online.py (source lookup)>ffprobe (prefilled when script stats)>TVDB>TVMAZE>IMBD>TMDB)
airdate (create only if -tv) (original episode air date) (info priority: online.py (source lookup)>TVDB>TVMAZE>IMBD>TMDB)
network (create only if -tv) (original airing network) (info priority: online.py (source lookup)>TVDB>TVMAZE>IMBD>TMDB)
genre (genre(s) of movie or tv show) (TVDB>TVMAZE>IMBD>TMDB>online.py (source lookup))
rating (network/audince rating, tvma, tv-g, rated r, pg-13, unrated, etc) (TVDB>TVMAZE>IMBD>TMDB>online.py (source lookup))
cast (limit to top 5 cast members) (TVDB>TVMAZE>IMBD>TMDB>online.py (source lookup))
imovie (create only if -movie) (movie poster image) (info priority: online.py (source lookup)>TMDB>IMBD)
iseries (create only if -tv) (tv series poster/image) (info priority: online.py (source lookup)>TVDB>TVMAZE>IMBD>TMDB)
iseason (create only if -tv) (tv series season poster/image) (info priority: online.py (source lookup)>TVDB>TVMAZE>IMBD>TMDB)
iepisode (create only if -tv) (tv episode poster/image/screenshot) (info priority: online.py (source lookup)>TVDB>TVMAZE>IMBD>TMDB)
imdb (imbd number for tv show or movie)
tmdb (the movie database  number for tv show or movie)
tvmaze (create only if -tv) (tvmaze identifier to tv show)
tvdb (create only if -tv) (tvdb identifier to tv show)

api - 
1. IMBD - (technically ombd) If found record in table online under column imbd
2. tmdb - the movie data base If found record in table online under column tmdb
3. tvmaze: - tvmaze (create only if tv show)  If found record in table online under column tvmaze
4. tvdb: - the tvdb number (create only if tv show) If found record in table online under column tvdb
Data priority:TV Maze > TMDb > TVDB > OMDb

Write the corresponding numbers like imbd to tt to import table:
imdb: - imdb number 
tmdb: - the movie database
tvmaze: - tvmaze (create only if tv show)
tvdb: - the tvdb number (create only if tv show)


