# Cloudflare Tunnel Deploy

This project now uses a local Flask app + Cloudflare Tunnel for public access.

## 1) Run the app locally

From `utopia_war_room`:

```powershell
$env:UTOPIA_ENABLE_INGEST="1"
$env:UTOPIA_SESSIONID="<your utopia session cookie>"
python app.py --host 127.0.0.1 --port 5055
```

Optional:

```powershell
$env:UTOPIA_CONFIG_PATH="config.json"
$env:UTOPIA_POLL_SECONDS="300"
```

## 2) Install cloudflared (Windows)

Pick one:

```powershell
winget install Cloudflare.cloudflared
```

or

```powershell
choco install cloudflared
```

## 3) Start a quick public tunnel

```powershell
cloudflared tunnel --url http://127.0.0.1:5055
```

Cloudflare prints a public `https://...trycloudflare.com` URL you can share.

## 4) Stable domain (recommended)

Use a named tunnel and DNS route so the URL is permanent:

```powershell
cloudflared tunnel login
cloudflared tunnel create utopia-war-room
cloudflared tunnel route dns utopia-war-room war.yourdomain.com
```

Create `config.yml`:

```yaml
tunnel: <TUNNEL_UUID>
credentials-file: C:\Users\<you>\.cloudflared\<TUNNEL_UUID>.json
ingress:
  - hostname: war.yourdomain.com
    service: http://127.0.0.1:5055
  - service: http_status:404
```

Run:

```powershell
cloudflared tunnel run utopia-war-room
```

## 5) Keep it running

- Use Task Scheduler or NSSM to run both `python app.py` and `cloudflared tunnel run ...` at startup.
- Keep `UTOPIA_SESSIONID` fresh; ingestion stops when the cookie expires.

## Quick checks

- App health: `http://127.0.0.1:5055/healthz`
- Ingest status: `http://127.0.0.1:5055/api/status`
- Public page: your Cloudflare URL
