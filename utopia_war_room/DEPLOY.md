# Deploy to Render (Live Updating)

Use a **Web Service** on Render.

## Canonical Settings

- `Root Directory`: `utopia_war_room`
- `Build Command`: `pip install -r requirements.txt`
- `Start Command`: `gunicorn app:app --bind 0.0.0.0:$PORT --workers 1 --threads 4 --timeout 120 --access-logfile -`
- `Health Check Path`: `/healthz`

## Environment Variables

Set these in Render:

- `UTOPIA_ENABLE_INGEST` = `1`
- `UTOPIA_DB_PATH` = `/data/utopia.db`
- `UTOPIA_SESSIONID` = `<your current utopia session cookie>`

Recommended defaults:

- `UTOPIA_SESSION_COOKIE_NAME` = `sessionid`
- `UTOPIA_BASE_URL` = `https://utopia-game.com`
- `UTOPIA_WORLD` = `wol`
- `UTOPIA_KINGDOM_NEWS_PATH` = `/wol/game/kingdom_news`
- `UTOPIA_CRAWL` = `true`
- `UTOPIA_MAX_PAGES` = `12`
- `UTOPIA_POLL_SECONDS` = `300`

## Persistent Storage

Add a disk:

- `Mount Path`: `/data`
- `Size`: `1 GB` (or higher)

Without a persistent disk, DB data will reset on restart.

## Plan

Use **Starter** (or higher) for reliable always-on ingest and persistent disk.
Free plan spins down and does not support persistent disks.

## What “Good” Looks Like in Logs

You should see:

- `[app] WSGI ingest thread started ...`
- `[collector] ... status=200 ...`
- `[parser] fetches=... extracted=...`

## If You Leave Root Directory Blank

Use this alternate pair instead:

- Build: `pip install -r utopia_war_room/requirements.txt`
- Start: `gunicorn app:app --chdir utopia_war_room --bind 0.0.0.0:$PORT --workers 1 --threads 4 --timeout 120 --access-logfile -`

Do not mix both patterns.
