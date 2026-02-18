import argparse
import hashlib
import json
import os
import time
from collections import deque
from datetime import datetime, timezone
from typing import Any, Deque, Dict, List, Tuple
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

from db import execute, init_db


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def sha256_text(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8", errors="ignore")).hexdigest()


def env_truthy(value: str | None, default: bool = False) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def load_json_file(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8-sig") as f:
        return json.load(f)


def load_config(path: str = "config.json") -> Dict[str, Any]:
    """
    Load config from file when present, then allow env vars to override.

    Render-friendly env vars:
    - UTOPIA_BASE_URL
    - UTOPIA_WORLD
    - UTOPIA_KINGDOM_NEWS_PATH
    - UTOPIA_CRAWL
    - UTOPIA_MAX_PAGES
    - UTOPIA_POLL_SECONDS
    - UTOPIA_COOKIES_JSON
    - UTOPIA_SESSIONID (+ UTOPIA_SESSION_COOKIE_NAME)
    """
    file_cfg: Dict[str, Any] = {}
    if path and os.path.exists(path):
        file_cfg = load_json_file(path)

    base_url = os.getenv("UTOPIA_BASE_URL", file_cfg.get("base_url", "https://utopia-game.com"))
    world = os.getenv("UTOPIA_WORLD", file_cfg.get("world", "wol"))

    file_pages = file_cfg.get("pages", {})
    file_news = file_pages.get("kingdom_news", {})
    if isinstance(file_news, str):
        file_news_path = file_news
        file_news_crawl = True
        file_news_max = 12
    else:
        file_news_path = file_news.get("path", f"/{world}/game/kingdom_news")
        file_news_crawl = bool(file_news.get("crawl", True))
        file_news_max = int(file_news.get("max_pages", 12))

    news_path = os.getenv("UTOPIA_KINGDOM_NEWS_PATH", file_news_path)
    news_crawl = env_truthy(os.getenv("UTOPIA_CRAWL"), default=file_news_crawl)
    news_max_pages = int(os.getenv("UTOPIA_MAX_PAGES", str(file_news_max)))
    poll_seconds = int(os.getenv("UTOPIA_POLL_SECONDS", str(file_cfg.get("poll_seconds", 300))))

    cookies: Dict[str, str] = {}
    cookies.update(file_cfg.get("cookies", {}))

    cookies_json = os.getenv("UTOPIA_COOKIES_JSON")
    if cookies_json:
        parsed = json.loads(cookies_json)
        if isinstance(parsed, dict):
            cookies.update({str(k): str(v) for k, v in parsed.items()})

    sessionid = os.getenv("UTOPIA_SESSIONID")
    if sessionid:
        cookie_name = os.getenv("UTOPIA_SESSION_COOKIE_NAME", "sessionid")
        cookies[cookie_name] = sessionid

    return {
        "base_url": base_url,
        "world": world,
        "pages": {
            "kingdom_news": {
                "path": news_path,
                "crawl": news_crawl,
                "max_pages": max(1, news_max_pages),
            }
        },
        "cookies": cookies,
        "poll_seconds": max(15, poll_seconds),
    }


def build_url(base_url: str, path_or_url: str) -> str:
    return urljoin(base_url.rstrip("/") + "/", path_or_url)


def internal_path(base_url: str, path_or_url: str) -> str | None:
    base = urlparse(base_url)
    absolute = urlparse(build_url(base_url, path_or_url))

    if absolute.netloc and absolute.netloc != base.netloc:
        return None

    path = absolute.path or "/"
    if absolute.query:
        return f"{path}?{absolute.query}"
    return path


def fetch_page(session: requests.Session, url: str) -> requests.Response:
    headers = {"User-Agent": "Mozilla/5.0"}
    return session.get(url, headers=headers, timeout=30)


def store_fetch(page_key: str, url: str, status: int, html: str) -> bool:
    """Store fetch and return True only when a new row is inserted."""
    digest = sha256_text(html)
    inserted = execute(
        """
        INSERT OR IGNORE INTO fetch_log(
          fetched_at_utc, page_key, url, http_status, sha256, raw_html
        )
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (utc_now_iso(), page_key, url, int(status), digest, html),
    )
    return inserted > 0


def parse_page_spec(page_key: str, page_spec: Any) -> Tuple[str, bool, int]:
    if isinstance(page_spec, str):
        crawl = page_key == "kingdom_news"
        max_pages = 12 if crawl else 1
        return page_spec, crawl, max_pages

    if isinstance(page_spec, dict):
        path = str(page_spec["path"])
        crawl_default = page_key == "kingdom_news"
        crawl = bool(page_spec.get("crawl", crawl_default))
        max_default = 12 if crawl else 1
        max_pages = max(1, int(page_spec.get("max_pages", max_default)))
        return path, crawl, max_pages

    raise ValueError(f"Unsupported page spec for '{page_key}': {page_spec!r}")


def extract_related_paths(base_url: str, html: str, seed_path: str) -> List[str]:
    seed_normalized = internal_path(base_url, seed_path)
    if not seed_normalized:
        return []

    prefix = seed_normalized.split("?", 1)[0].rstrip("/")
    if not prefix:
        return []

    soup = BeautifulSoup(html, "html.parser")
    out: List[str] = []
    seen = set()

    for anchor in soup.select("a[href]"):
        candidate = internal_path(base_url, anchor["href"])
        if not candidate:
            continue

        candidate_path = candidate.split("?", 1)[0]
        if candidate_path == prefix or candidate_path.startswith(prefix + "/"):
            if candidate not in seen:
                seen.add(candidate)
                out.append(candidate)

    return out


def collect_page_family(
    session: requests.Session,
    base_url: str,
    page_key: str,
    seed_path: str,
    crawl: bool,
    max_pages: int,
) -> int:
    queue: Deque[str] = deque([seed_path])
    seen = set()
    fetched = 0

    while queue and len(seen) < max_pages:
        path = queue.popleft()
        normalized = internal_path(base_url, path)
        if not normalized or normalized in seen:
            continue

        seen.add(normalized)

        url = build_url(base_url, normalized)
        response = fetch_page(session, url)
        html = response.text or ""
        inserted = store_fetch(page_key, url, response.status_code, html)
        state = "inserted" if inserted else "deduped"
        print(
            f"[collector] {page_key} status={response.status_code} bytes={len(html)} "
            f"state={state} url={url}"
        )

        fetched += 1

        if crawl and response.ok:
            for next_path in extract_related_paths(base_url, html, seed_path):
                if next_path not in seen and next_path not in queue:
                    if len(seen) + len(queue) >= max_pages:
                        break
                    queue.append(next_path)

    return fetched


def run_once(config_path: str = "config.json") -> None:
    cfg = load_config(config_path)
    base_url = cfg["base_url"]
    pages = cfg["pages"]
    cookies = cfg.get("cookies", {})

    if not cookies:
        raise ValueError(
            "No session cookies configured. Set UTOPIA_SESSIONID (or cookies in config.json)."
        )

    init_db()

    with requests.Session() as session:
        for key, value in cookies.items():
            session.cookies.set(key, value)

        for page_key, page_spec in pages.items():
            path, crawl, max_pages = parse_page_spec(page_key, page_spec)
            collect_page_family(
                session=session,
                base_url=base_url,
                page_key=page_key,
                seed_path=path,
                crawl=crawl,
                max_pages=max_pages,
            )


def run_loop(config_path: str = "config.json") -> None:
    cfg = load_config(config_path)
    poll_seconds = int(cfg.get("poll_seconds", 300))

    while True:
        try:
            run_once(config_path)
        except Exception as exc:  # pragma: no cover
            print(f"[collector] ERROR: {exc}")

        time.sleep(poll_seconds)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default="config.json")
    parser.add_argument("--loop", action="store_true")
    args = parser.parse_args()

    if args.loop:
        run_loop(args.config)
    else:
        run_once(args.config)
