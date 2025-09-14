api.py

We call API’s for more data.

We search using api table data only

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
- api.py
- prep.py
preferences(folder)
- torrentsites.json
- sources.json
- series.json
- season.json
- episode.json


table - api

rows  - checksum

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
imovie: - image movie poster (create only if movie)

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


The terminal is clean without t any extra junk and matches the others:(venv) bryant@BryantsacStudio scripts % ./online.py    
Successfully processed 9 items
(venv) bryant@BryantsacStudio scripts % ./media.py     
Processing 9 files...
Results: 9 processed, 0 failed
(venv) bryant@BryantsacStudio scripts % ./api.py   
Processing 9 records with MAXIMUM DATA EXTRACTION...
MAXIMUM EXTRACTION COMPLETE:
  Processed: 9/9
  Failed: 0
  Total Fields: 99
(venv) bryant@BryantsacStudio scripts % ./online.py
Successfully processed 9 items
(venv) bryant@BryantsacStudio scripts % 
