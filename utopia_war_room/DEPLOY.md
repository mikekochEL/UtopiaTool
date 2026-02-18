# Public Deployment Guide

This project is now configured for production serving with `gunicorn`.

## Included deploy files

- `requirements.txt` includes `gunicorn`
- `Procfile` for Railway/Heroku-style runtimes
- `railway.toml` for Railway deploy defaults
- `render.yaml` for Render Blueprint deploy
- `.gitignore` excludes `config.json`, `utopia.db`, and local secrets

## 1) Fastest public URL (no hosting account)

Run locally and tunnel it:

```powershell
python app.py
cloudflared tunnel --url http://127.0.0.1:5055
```

Share the generated `trycloudflare.com` URL.

## 2) Render deploy (managed hosting)

1. Push this folder to GitHub.
2. In Render, create from Blueprint (`render.yaml` is auto-detected).
3. Render will run:

```bash
gunicorn app:app --bind 0.0.0.0:$PORT --workers 2 --threads 4 --timeout 120 --access-logfile -
```

4. Health check endpoint:

```text
/healthz
```

Notes:
- `UTOPIA_DB_PATH` defaults to `/data/utopia.db` in `render.yaml`.
- The hosted app under `gunicorn` is dashboard-only (no collector loop in web workers).

## 3) Railway deploy

1. Push to GitHub.
2. Create a Railway project from repo.
3. Railway will use `railway.toml` start command (or fallback `Procfile`).
4. Add a persistent volume and set:

```text
UTOPIA_DB_PATH=/data/utopia.db
```

## Data strategy (important)

- Keep ingestion private (your cookie stays local in `config.json`).
- Keep the public app read-only and publish DB snapshots.

Recommended workflow:
1. Local machine runs ingestion (`python app.py` or collector/parser loop).
2. Public host serves dashboard from a copied snapshot DB.
3. Never commit `config.json` or session cookies.
