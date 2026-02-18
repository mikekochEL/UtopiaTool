import os
import sqlite3
from pathlib import Path
from typing import Any, Iterable

DB_PATH = Path(os.getenv("UTOPIA_DB_PATH", "utopia.db"))

SCHEMA = """
PRAGMA journal_mode=WAL;

CREATE TABLE IF NOT EXISTS fetch_log (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  fetched_at_utc TEXT NOT NULL,
  page_key TEXT NOT NULL,
  url TEXT NOT NULL,
  http_status INTEGER NOT NULL,
  sha256 TEXT NOT NULL,
  raw_html TEXT NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_fetch_dedupe
ON fetch_log(page_key, sha256);

CREATE TABLE IF NOT EXISTS kd_news_events (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  fetched_at_utc TEXT NOT NULL,
  event_time_text TEXT,
  category TEXT,
  actor TEXT,
  target TEXT,
  summary TEXT NOT NULL,
  raw_line TEXT NOT NULL,
  sha256 TEXT NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_news_dedupe
ON kd_news_events(sha256);
"""


def connect() -> sqlite3.Connection:
    cn = sqlite3.connect(DB_PATH)
    cn.row_factory = sqlite3.Row
    return cn


def init_db() -> None:
    cn = connect()
    try:
        cn.executescript(SCHEMA)
        cn.commit()
    finally:
        cn.close()


def execute(sql: str, params: Iterable[Any] = ()) -> int:
    cn = connect()
    try:
        cur = cn.execute(sql, params)
        cn.commit()
        return cur.rowcount
    finally:
        cn.close()


def executemany(sql: str, rows: Iterable[Iterable[Any]]) -> int:
    cn = connect()
    try:
        cur = cn.executemany(sql, rows)
        cn.commit()
        return cur.rowcount
    finally:
        cn.close()


def fetchall(sql: str, params: Iterable[Any] = ()):  # noqa: ANN201
    cn = connect()
    try:
        return cn.execute(sql, params).fetchall()
    finally:
        cn.close()


def fetchone(sql: str, params: Iterable[Any] = ()):  # noqa: ANN201
    cn = connect()
    try:
        return cn.execute(sql, params).fetchone()
    finally:
        cn.close()
