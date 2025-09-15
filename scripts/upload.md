upload.py

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

Arguments:
-v : runs terminal in verbose mode
-t : runs test mode, does not upload torrent

Test mode creates the torrent but does not upload it to the site. 

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
- upload.py (this script)
preferences(folder)
- torrentsites.json
- sources.json
- series.json
- season.json
- episode.json


Step One:
load tapedeck.py (import - newloc) (local - torrenttype) (local - torrentsite)
load user.json (temp_torrent_upload) this is where the temporary torrent goes
load user.json montiroed_upload) this is where the new downloaded torrent goes that goes into a monitored folder for the torrent program to pick up


Now we have the files, and how they should be in torrents. Make the torrent files. 

Create a torrentsites.json and put all of the SPECIFIC information for each torrent site, for now we are only working with torrent leech. 





Once torrent file is created upload torrent. Name the torrent file to match the series/season/episode folder name.
user prefecnes also includes the correct category depending on torrent presence “it_tor”. check table “tape_data” and columns rt_imdb (imdb) and (rt_maze) for torrentleech to add to upload info. 
torrent leech API info (currently only one system is trained on):We have introduced an API to upload/download from Torrentleech which is only
available to our uploaders. An API is automated in scripts, if you do not use any
then this if not for you. Be aware that trial uploaders can’t use the API until they
are promoted to full uploader.
Type: POST HTTP
Upload API:
Upload URL: https://www.torrentleech.org/torrents/upload/apiupload
64
Mandatory fields for uploading: announcekey, textual nfo OR nfo file, category
Optional field: imdb, tvmaze (AND tvmazetype), animeid, igdburl, tags
tvmazetype: 1 for the series in general, 2 is used for single episodes
Jan 2021
1 / 22
Jan 2021
MESSAGES
Inbox
CHANNELS
▼ Example textual NFO:
curl -X POST -F 'announcekey=xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'
Feb 6
-F 'category=13' -F 'description=Textual NFO goes here because
DMS
apparently you don't have an NFO file' -F 'tags=rar,Comedy' -F
'torrent=@/path/torrentname.torrent'
https://www.torrentleech.org/torrents/upload/apiupload
▼ Example file NFO including IMDB
curl -X POST -F 'announcekey=xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'
-F 'category=13' -F 'nfo=@/path/nfopath.nfo' -F 'imdb=ttxxxxxx'
-F 'tags=rar,Comedy' -F 'torrent=@/path/torrentname.torrent'
https://www.torrentleech.org/torrents/upload/apiupload
▼ Example textual NFO including TVMaze for a season pack
curl -X POST -F 'announcekey=xxxxxxxxxxxxxxxxxx' -F
'category=32' -F 'description=Textual NFO goes here because
apparently you dont have an NFO file' -F 'tvmazeid=1000' -F
'tvmazetype=1' -F 'torrent=@torrent_name.torrent'
https://www.torrentleech.org/torrents/upload/apiupload
▼ Example textual NFO including AnimeID
curl -X POST -F 'announcekey=xxxxxxxxxxxx' -F 'category=34' -F
'description=Textual NFO goes here because apparently you dont
have an NFO file' -F 'animeid=10000' -F
'torrent=@torrent_name.torrent'
https://www.torrentleech.org/torrents/upload/apiupload
▼ Example textual NFO including IGDB
curl -X POST -F 'announcekey=xxxxxxxxxxxx' -F 'category=17' -F
'description=Textual NFO goes here because apparently you dont
have an NFO file' -F
'igdburl=https://www.igdb.com/games/xxxxxxxxxx' -F
'torrent=@torrent_name.torrent'
https://www.torrentleech.org/torrents/upload/apiupload
Successful Upload returns torrent ID, which in turn can be used to redownload
and start seeding (if applicable).
Download API
Download URL: https://www.torrentleech.org/torrents/upload/apidownload
Mandatory fields for downloading: announcekey, torrentID
29
▼ Example to get the *.torrent file contents:
curl -X POST -F 'announcekey=xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'
-F 'torrentID=xxxxxxx'
https://www.torrentleech.org/torrents/upload/apidownload
▼ Example to download the actual *.torrent file:
curl -X POST -F 'announcekey=xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'
-F 'torrentID=xxxxxxx'
https://www.torrentleech.org/torrents/upload/apidownload -o
'filename.torrent'
The following table shows the corresponding category value to the site
categories:
▼ Click to show table
Cat
# Description
Cat
# Description
8 Movies :: Cam 17 Games :: PC
9 Movies :: TS/TC 18 Games :: XBOX
11
Movies ::
DVDRip/DVDScreener
19 Games :: XBOX360
12 Movies :: DVD-R 20 Games :: PS2
13 Movies :: Bluray 21 Games :: PS3
14 Movies :: BlurayRip 22 Games :: PSP
15 Movies :: Boxsets 28 Games :: Wii
29 Movies :: Documentaries 30 Games :: Nintendo DS
36 Movies :: Foreign 39 Games :: PS4
37 Movies :: WEBRip 40 Games :: XBOXONE
43 Movies :: HDRip 42 Games :: Mac
47 Movies :: 4K 48
Games :: Nintendo
Switch
49 Games :: PS5
26 TV :: Episodes
27 TV :: BoxSets 16 Music :: Music Videos
32 TV :: Episodes HD 31 Music :: Audio
44 TV :: Foreign
45 Books :: EBooks
23 Applications :: PC-ISO 46 Books :: Comics
24 Applications :: Mac 34 Animation :: Anime
25 Applications :: Mobile 35 Animation :: Cartoons
33 Applications :: 0-day 38 Education
Search API
Search URL: https://www.torrentleech.org/api/torrentsearch
3
Mandatory fields for uploading: announcekey, query
Optional field: exact
query: Make sure that the parameter is enclosed in ‘single quotation marks’,
within the “regular quotationmarks”, otherwise the result will always be 0.
A succesful search query returns either a 0 (no hit) or a 1 (hit). Use “exact=1” if
you want to search for an exact match, rather than fuzzy.
► Example search
9 Reply






 lets always create the torrents in order, so they get uploaded in order, so
  s01 would always be first or whatever the oldest one is. 
