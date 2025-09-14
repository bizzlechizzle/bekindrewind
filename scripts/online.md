online.py

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

We do an ffmpeg dump, fill in the database, then do a media info dump for other/missing data for each 256 Checksum on the import table. We make sure all data is clean and normalized. 

Database Name : tapedeck.db

Folder Layout (for this and other scripts)
autorewind.py
tapedeck.db
user.json
scripts (folder)
- import.py
- database.py 
- media.py 
- online.py Rules

1. We are working with Python Scripts
2. Scripts are universal and run on any OS.
3. We always value code that is KISS, yet bulletproof, and verified with ULTRATHINK.
4. We respect the text case of the original folders, online lookups, and API lookups.
5. We follow common normalization for TORRENTING.
6. We dont use emojis or leave un-needed comments.
7. The terminal interface is KISS, nothing extra needed.

Overview

We do a search for URLS on a log file, use playwright to check for rich media info on source url. log into database.

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


We use 3 different browers, with 3 different machine tops, for 9 possible browsers, plus standard anti-blocking meausres to download webpages.

We scroll to the bottom of the web page. We click on any in-link mores, drop downs, etc on this page, looking for more information.


Imports loglocation from user.json 
loglocation: :/mnt/projects/downloads/streamfab/logs

Opens streamfab.log

We can limit or verify our url search since we know where it came from - filesource in the import tab in the database.

Pulls Urls (If we have 1 movie, 2 tv shows seasons = we need 3 urls - check most recent url and work backwards. - limit of 2x what we are looking for. If we have 5 movies and 0 tv shows, we’d check upto the last 10 different urls for a match. If we have 2 separate shows, with 4 seasons each we’d be looking for 8 links or the last 16 different urls max. Episodes dont have their own URL. If no urls can be found at this point double the limit and try one last time. 

Example (https://www.amazon.com/gp/video/detail/B0DL5RVPTW/)

The Url will be the same for each episode in a season


We match import tab in tapedeck.db:

Movies by Movie Name 

We match Tv Shows:
Verify Series, Season Number

If we match tv show our total episode numbers for that season vs total episodes found on source url. If numbers match move on, if they dont then figure out what episode(s) is missing. If missing episodes print error missing episodes and then make a missingepisodes.txt that shows what episodes were found locally, what episode is missing, and keep it easy and give me the web URL again in the file. This STOPS the entire process.

Write the URL to import table - url - for each checksum


We then open online tab in tapedeck.db - and fill in as much as we can:




One A. Playright Online SearchOne B. Dumping HTML File
One C. Parsing Per Episode


table - online

rows  - checksums (prefilled)

columns - 

dmovie (create only if -movie) (movie description)
release (create only if -movie) (movie release date)
studio (create only if -movie) (movie studio)
dseries (create only if -tv) (series description)
dseason (create only if -tv) (season description)
depisode (create only if -tv) (episode description)
airdate (create only if -tv) (original episode air date)
network (create only if -tv) (original airing network)
genre (genre(s) of move or tv show)
rating (network/audince rating, tvma, tv-g, rated r, pg-13, unrated, etc)
cast (limit to top 5 cast members)
imovie (create only if -movie) (movie poster image)
iseries (create only if -tv) (tv series poster/image)
iseason (create only if -tv) (tv series season poster/image)
iepisode (create only if -tv) (tv episode poster/image/screenshot)
imdb (imbd number for tv show or movie)
tmdb (the movie database  number for tv show or movie)
tvmaze (create only if -tv) (tvmaze identifier to tv show)
tvdb (create only if -tv) (tvdb identifier to tv show)













checksum - imported from import table sha256 checksum
movie: - movie title (create only if movie)
series: - series name (create only if tv show)
season: - season number (create only if tv show)
episode: - episode number (create only if tv show)
title: - episode title (create only if tv show)
stitle: - special title (if available)(example extended edition, uncut, etc)
dmovie: - movie description (create only if movie)
dseries: - series descriptione (create only if tv show)
dseason: - season description (create only if tv show)
depisode: - episode description (create only if tv show)
year: - release year
airdate: - (create only if tv show)
release: - (create only if movie)
network: - airing network (create only if tv show)
genre: - genre
rating: - tv or movie audience rating (tvma tv-7, pg 13, r, etc)
cast: - top 7 cart
imdb: - imdb number 
tmdb: - the movie database
tvmaze: - tvmaze (create only if tv show)
tvdb: - the tvdb number (create only if tv show)
iseries: - image series (create only if tv show)
iseason: - image season (create only if tv show)
iepisode: - image episode (create only if tv show)
imovie: - image movie poster (create movie) only


OLD NOTES FROM OLD PROGRAM FOR OLD DATABASE IT WAS TRAINED EXCELENET AT PARSGING FROM AMAZON.COM WE DONT NEED ALL THE SEARCH OR API AS WE ALREADY HAVE THE EXACT LINK HECK YEAH. THIS SCRIPT IS REWIND_TAPE.PY YOU CAN LOOK BUT DO NOT VEER ON THE NEW TASK FROM MY INSTRUCTIONS
If there is a full match url then proceeded to the matched url and do a jfull dom html Use dump of the data and enter it into the database. If it is a partial match proceed to the page and then check for correct season number and series name on page, if the season number and series matches then do the dump. If the partial match was not correct proceed to next search engine. Use LXML to Parse the data. 

If no full match results can be found after checking each search engine prompt user for download source url. 


Table :

Rows: each “it_checksum”

Columns:
it_checksum - prefilled from import_tape.py
rt_ser - series name filled from Step 1
rt_sea - season number filled from Step 1
rt_ep - episode number filled from Step 1
rt_tit - episode title filled from Step 1
rt_d_ser - series description filled from Step 2
rt_d_sea - season description filled from Step 1
rt_d_ep - episode description filled from Step 1
rt_ep_d -episode duration filled from Step 1
rt_y - season/series year filled from Step 1
rt_air - episode air date filled from Step 1
rt_net - airing network filled from Step 2
rt_gen - genre filled from Step 1 and Step 2
rt_tr - television rating - filled from Step 1 or Step 2
rt_cst - cast - filled from Step 2
rt_imdb - imdb number (filled by tvmaze or omdb) filled from Step 2
rt_tmdb - tmdb number number - filled from Step 2
rt_maze - tvmaze number - filled from Step 2
rt_tvdb - thetvdb - filled from Step 2
rt_src - the source URL for each season that was dumped
rt_ep_img - the source image URL for each episode thumbnail
rt_ser_img- the source image URL for the series/season
rt_ep_n - lists the total number of episodes found on source page
rt_ep_a -  lists the all of the episodes found on source page for that season. “S02E01” Format 




HTML PARSING TRAINING:rt_series: (same for all “it_checksum” in that series)
<h1 class="p-jAFk Qo+b2C" data-automation-id="title" elementtiming="dv-web-timing-atfVisible">Cheap Old Houses</h1>
rt_sea (same for all “it_checksum” in that season)

<div class="dv-node-dp-seasons"><span class="enCoYt"><span class="_36qUej">Season 1</span></span></div>

and 

<div data-automation-id="ep-title-episode-0" class="dCocJw"><div class="wfjdyJ"><label class="lzsdji" for="selector-B09C5NPSQN"></label><div><h3 class="izvPPq"><span class="_36qUej izvPPq"><span>S1 E1</span><span class="Z7ThIH"> - </span><span class="P1uAb6">Time Warp in Upstate New York</span></span></h3><div class="Pol9sO"></div></div></div></div>

rt_d_sea (same for all “it_checksum” in that season)
<span class="_1H6ABQ" style="--expanded-max-height:unset">Property enthusiasts Elizabeth and Ethan Finkelstein hunt for their next batch of cheap old houses to feature on their wildly successful social media site. Along the way, they check out houses that have been lovingly restored by historic home devotees.</span>

rt_y (same for all “it_checksum” in that season)

<span role="img" aria-label="Released 2021" data-automation-id="release-year-badge" class="_3F76dX _23dw7w">2021</span>

rt_ep  (per “it_checksum” / “rt_ep” )

<div data-automation-id="ep-title-episode-0" class="dCocJw"><div class="wfjdyJ"><label class="lzsdji" for="selector-B09C5NPSQN"></label><div><h3 class="izvPPq"><span class="_36qUej izvPPq"><span>S1 E1</span><span class="Z7ThIH"> - </span><span class="P1uAb6">Time Warp in Upstate New York</span></span></h3><div class="Pol9sO"></div></div></div></div>

rt_tit    (per “it_checksum” / “rt_ep” )

<div data-automation-id="ep-title-episode-0" class="dCocJw"><div class="wfjdyJ"><label class="lzsdji" for="selector-B09C5NPSQN"></label><div><h3 class="izvPPq"><span class="_36qUej izvPPq"><span>S1 E1</span><span class="Z7ThIH"> - </span><span class="P1uAb6">Time Warp in Upstate New York</span></span></h3><div class="Pol9sO"></div></div></div></div>

rt_d_ep (per “it_checksum” / “rt_ep” )

<div class="_1+KXv2 ci7S35"><div class="p-jAFk _1zr6Jb" data-automation-id="synopsis-B09C5NPSQN"><div class="mlmPSf -12Ln6 hjuloM"><input id="synopsis-B09C5NPSQN" class="wgGFDs" type="checkbox" disabled="" checked=""><label for="synopsis-B09C5NPSQN" class="_1W5VSv"></label><div class="_3qsVvm e8yjMf"><div dir="auto">While in New York, Ethan and Elizabeth visit a 1850 Italianate with a jaw-dropping midcentury interior and a 1900 Victorian with its original details. Then, they tour a 1850 schoolhouse restored by a father-daughter team with a connection to the building.</div></div></div></div></div>

rt_air (per “it_checksum” / “rt_ep” )

<div class="_1wFEYz ci7S35" data-testid="episode-metadata"><div class="riRKnh"><div data-testid="episode-release-date">August 9, 2021</div><div data-testid="episode-runtime">25min</div><span aria-label="G (General Audience)" class="_3U8JX- _2zZuYJ _1TNG7J fbl-maturity-rating _1UHwej fbl-label-badge" role="img" data-testid="rating-badge" data-automation-id="rating-badge" dir="ltr">TV-G</span></div></div>

rt_ep_d (per “it_checksum” / “rt_ep” )

<div class="_1wFEYz ci7S35" data-testid="episode-metadata"><div class="riRKnh"><div data-testid="episode-release-date">August 9, 2021</div><div data-testid="episode-runtime">25min</div><span aria-label="G (General Audience)" class="_3U8JX- _2zZuYJ _1TNG7J fbl-maturity-rating _1UHwej fbl-label-badge" role="img" data-testid="rating-badge" data-automation-id="rating-badge" dir="ltr">TV-G</span></div></div>

rt_net  (same for all “it_checksum” in that series) (may return null)

<dl class="-Zstym" data-testid="metadata-row"><dt class="_5HWLFr"><h3><span class="_36qUej">Studio</span></h3></dt><dd class="_3k277F">HGTV</dd></dl>

rt_gen (same for all “it_checksum” in that series) (may return null)

<div class="I0iH2G" data-testid="genresMetadata"><span data-testid="genre-texts" class="_3F76dX _23dw7w"><a href="/gp/video/browse/ref=atv_dp_pd_gen?serviceToken=v0_EgVxdWVyeRgBKgdkZWZhdWx0MgZjZW50ZXI6BnNlYXJjaHoAggGGARpsbm9kZT0yODU4Nzc4MDExJmZpZWxkLXRoZW1lX2Jyb3dzZS1iaW49JmdlbnJlLWJpbj1hdl9nZW5yZV9zcGVjaWFsX2ludGVyZXN0JnNlYXJjaC1hbGlhcz1wcmltZS1pbnN0YW50LXZpZGVvIhBTcGVjaWFsIEludGVyZXN0MABQAHAA" class="_1NNx6V">Special Interest</a></span></div>

rt_tr   ( parsed from first “it_checksum” / “it_ep_num” and applied to all it_checksums)

<div class="_1wFEYz ci7S35" data-testid="episode-metadata"><div class="riRKnh"><div data-testid="episode-release-date">August 9, 2021</div><div data-testid="episode-runtime">25min</div><span aria-label="G (General Audience)" class="_3U8JX- _2zZuYJ _1TNG7J fbl-maturity-rating _1UHwej fbl-label-badge" role="img" data-testid="rating-badge" data-automation-id="rating-badge" dir="ltr">TV-G</span></div></div>

rt_cst 

<dl class="-Zstym" data-testid="metadata-row"><dt class="_5HWLFr"><h3><span class="_36qUej">Cast</span></h3></dt><dd class="_3k277F"><a href="/gp/video/search/ref=atv_dp_pd_actors?phrase=Elizabeth%20Finkelstein&amp;ie=UTF8" class="_1NNx6V">Elizabeth Finkelstein</a><span aria-hidden="true"><span class="_36qUej">, </span></span><a href="/prime-video/actor/Ethan-Finkelstein/amzn1.dv.gti.88a5160f-62f8-4c0f-9099-0c10494c710f/ref=atv_dp_md_pp" class="_1NNx6V">Ethan Finkelstein</a></dd></dl>

rt_ep_img

<img alt="" class="FHb5CR Ah1hNY" style="aspect-ratio:16/9" src="https://m.media-amazon.com/images/S/pv-target-images/7b7ef0f6698d47bbd48c846db25cb9f48135fc0a858bf937597b891ccbdaeb44._BR-6_AC_SX720_FMjpg_.jpg" data-testid="base-image" loading="lazy">

rt_ser_img

<div class="BNTHjF" data-automation-id="hero-background"><div class="om7nme" style="aspect-ratio:16/9"><picture><source type="image/webp" srcset="https://m.media-amazon.com/images/S/pv-target-images/44a5794d8358911a1412a6c560708c3a9d342356efb29daf9a1f1dccc3a28684._SX360_FMwebp_.jpg 360w, https://m.media-amazon.com/images/S/pv-target-images/44a5794d8358911a1412a6c560708c3a9d342356efb29daf9a1f1dccc3a28684._SX480_FMwebp_.jpg 480w, https://m.media-amazon.com/images/S/pv-target-images/44a5794d8358911a1412a6c560708c3a9d342356efb29daf9a1f1dccc3a28684._SX720_FMwebp_.jpg 720w, https://m.media-amazon.com/images/S/pv-target-images/44a5794d8358911a1412a6c560708c3a9d342356efb29daf9a1f1dccc3a28684._SX1080_FMwebp_.jpg 1080w, https://m.media-amazon.com/images/S/pv-target-images/44a5794d8358911a1412a6c560708c3a9d342356efb29daf9a1f1dccc3a28684._SX1440_FMwebp_.jpg 1440w, https://m.media-amazon.com/images/S/pv-target-images/44a5794d8358911a1412a6c560708c3a9d342356efb29daf9a1f1dccc3a28684._SX1920_FMwebp_.jpg 1920w" sizes="(max-width: 28em) 450px, (max-width: 55em) 900px, (max-width: 80em) 1300px, (max-width: 100em) 1600px, (max-width: 150em) 2400px, 1920px"><source type="image/jpeg" srcset="https://m.media-amazon.com/images/S/pv-target-images/44a5794d8358911a1412a6c560708c3a9d342356efb29daf9a1f1dccc3a28684._SX360_FMjpg_.jpg 360w, https://m.media-amazon.com/images/S/pv-target-images/44a5794d8358911a1412a6c560708c3a9d342356efb29daf9a1f1dccc3a28684._SX480_FMjpg_.jpg 480w, https://m.media-amazon.com/images/S/pv-target-images/44a5794d8358911a1412a6c560708c3a9d342356efb29daf9a1f1dccc3a28684._SX720_FMjpg_.jpg 720w, https://m.media-amazon.com/images/S/pv-target-images/44a5794d8358911a1412a6c560708c3a9d342356efb29daf9a1f1dccc3a28684._SX1080_FMjpg_.jpg 1080w, https://m.media-amazon.com/images/S/pv-target-images/44a5794d8358911a1412a6c560708c3a9d342356efb29daf9a1f1dccc3a28684._SX1440_FMjpg_.jpg 1440w, https://m.media-amazon.com/images/S/pv-target-images/44a5794d8358911a1412a6c560708c3a9d342356efb29daf9a1f1dccc3a28684._SX1920_FMjpg_.jpg 1920w" sizes="(max-width: 28em) 450px, (max-width: 55em) 900px, (max-width: 80em) 1300px, (max-width: 100em) 1600px, (max-width: 150em) 2400px, 1920px"><img alt="Cheap Old Houses" class="Ah1hNY" style="aspect-ratio:16/9" src="https://m.media-amazon.com/images/S/pv-target-images/44a5794d8358911a1412a6c560708c3a9d342356efb29daf9a1f1dccc3a28684._SX1080_FMjpg_.jpg" data-testid="base-image" loading="eager" elementtiming="dv-web-timing-atfVisible"></picture></div><div class="YMr7XB GBUIzn"></div></div>Playwright Source Page:

jfull html dom dumped 
Scroll, wait, dump HTML 
await page.goto(target_url, timeout=60000)

# Scroll to bottom to trigger lazy loading
prev_height = 0
while True:
    await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
    await asyncio.sleep(2)  # simulate human scroll pause
    curr_height = await page.evaluate("document.body.scrollHeight")
    if curr_height == prev_height:
        break
    prev_height = curr_height

# Wait a bit more for any final AJAX
await asyncio.sleep(2)

# Dump full rendered HTML
html_dump = await page.content()

# KISS sanity check
if len(html_dump) < 100_000:
    raise Exception("HTML too short — probably missing content")
