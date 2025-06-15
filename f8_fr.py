#!/usr/bin/env python3
import json
import sqlite3
import math
import subprocess
import logging
import requests
from pathlib import Path

# 1. Load config & set up file logging
with open('2jznoshit.json') as f:
    cfg = json.load(f)
LOG_FILE = 'f8_fr.log'
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.DEBUG if cfg['f8_fr']['logs'] else logging.INFO,
    format='%(asctime)s %(levelname)s: %(message)s'
)

# 2. Paths & tracker info
loc = cfg['user_input']['default']
TOR_DIR = Path(loc['tor_loc'])
UTOR_DIR = Path(loc['utor_loc'])
announce_url = loc['tracker'].split()[-1]
announce_key = announce_url.rstrip('/').split('/')[-1]

# 3. API endpoints and categories
UPLOAD_URL   = 'https://www.torrentleech.org/torrents/upload/apiupload'
DOWNLOAD_URL = 'https://www.torrentleech.org/torrents/upload/apidownload'
CATEGORY     = {'series':'27','all':'27','season':'27','episode':'26'}  # :contentReference[oaicite:3]{index=3}

# 4. Fetch targets from DB
conn = sqlite3.connect('danger2manifold.db')
cur  = conn.cursor()
cur.execute("SELECT it_file_loc, it_torrent FROM import_tuner")
rows = cur.fetchall()
conn.close()

# 5. Group into (kind, path)
targets = set()
for path_str, kind in rows:
    p = Path(path_str)
    if kind in ('series','all'):
        targets.add(('series', p.parent.parent))
    elif kind == 'season':
        targets.add(('season', p.parent))
    else:
        targets.add(('episode', p))

# 6. Process each
for kind, target in targets:
    name = target.name
    tmp_file = TOR_DIR / f"{name}.tmp.torrent"

    # 6a. Calc piece-size exponent
    total = sum(f.stat().st_size for f in target.rglob('*') if f.is_file())
    exp = math.ceil(math.log2(total)/2) if total>0 else 18
    exp = max(18, min(exp, 25))

    # 6b. Create private V1 torrent
    try:
        subprocess.run([
            'mktorrent','-p','-l',str(exp),
            '-a',announce_url,
            '-o',str(tmp_file),
            str(target)
        ], check=True)
    except subprocess.CalledProcessError as e:
        logging.error(f"mktorrent failed ({e.returncode}): {' '.join(e.cmd)}")
        continue

    # 6c. Read .nfo if exists
    nfo = next(target.glob('*.nfo'), None)
    desc = nfo.read_text() if nfo else ''
    if not nfo:
        logging.warning(f"No .nfo in {target}")

    # 6d. Upload
    try:
        with open(tmp_file,'rb') as tf:
            resp = requests.post(
                UPLOAD_URL,
                data={'announcekey':announce_key,'category':CATEGORY[kind],'description':desc},
                files={'torrent':tf}
            )
        resp.raise_for_status()
        tid = resp.text.strip()
        logging.info(f"Upload OK: {name} → ID {tid}")
    except Exception as e:
        logging.error(f"Upload error for {name}: {e}")
        tmp_file.unlink(missing_ok=True)
        continue

    # 6e. Download final .torrent
    try:
        dl = requests.post(
            DOWNLOAD_URL,
            data={'announcekey':announce_key,'torrentID':tid}
        )
        dl.raise_for_status()
        final = TOR_DIR / f"{name}.torrent"
        final.write_bytes(dl.content)
        logging.info(f"Downloaded final torrent for {name}")
    except Exception as e:
        logging.error(f"Download error for {name}: {e}")
        tmp_file.unlink(missing_ok=True)
        continue
    finally:
        tmp_file.unlink(missing_ok=True)

    # 6f. Mirror to utor_dir
    try:
        (UTOR_DIR / final.name).write_bytes(final.read_bytes())
    except Exception as e:
        logging.error(f"Mirror error for {name}: {e}")
