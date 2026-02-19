import os
import sqlite3
import time
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

CREATE TABLE IF NOT EXISTS kd_kingdom_snapshots (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  fetched_at_utc TEXT NOT NULL,
  kingdom_coord TEXT NOT NULL,
  kingdom_name TEXT,
  total_provinces INTEGER,
  stance TEXT,
  total_networth INTEGER,
  avg_networth INTEGER,
  networth_rank INTEGER,
  total_land INTEGER,
  avg_land INTEGER,
  land_rank INTEGER,
  total_honor INTEGER,
  honor_rank INTEGER,
  wars_won INTEGER,
  war_score REAL,
  avg_opp_relative_size_pct INTEGER,
  source_fetch_id INTEGER,
  sha256 TEXT NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_kd_kingdom_snapshot_dedupe
ON kd_kingdom_snapshots(kingdom_coord, fetched_at_utc, sha256);

CREATE INDEX IF NOT EXISTS ix_kd_kingdom_snapshot_coord_time
ON kd_kingdom_snapshots(kingdom_coord, fetched_at_utc);

CREATE TABLE IF NOT EXISTS kd_province_snapshots (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  fetched_at_utc TEXT NOT NULL,
  kingdom_coord TEXT NOT NULL,
  slot INTEGER NOT NULL,
  province_name TEXT NOT NULL,
  race TEXT,
  land INTEGER,
  networth INTEGER,
  nwpa REAL,
  nobility TEXT,
  is_monarch INTEGER NOT NULL DEFAULT 0,
  is_steward INTEGER NOT NULL DEFAULT 0,
  is_you INTEGER NOT NULL DEFAULT 0,
  is_online INTEGER NOT NULL DEFAULT 0,
  source_fetch_id INTEGER,
  sha256 TEXT NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_kd_province_snapshot_unique_slot
ON kd_province_snapshots(kingdom_coord, fetched_at_utc, slot);

CREATE INDEX IF NOT EXISTS ix_kd_province_snapshot_lookup
ON kd_province_snapshots(kingdom_coord, province_name, fetched_at_utc);

CREATE TABLE IF NOT EXISTS kd_doctrine_snapshots (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  fetched_at_utc TEXT NOT NULL,
  kingdom_coord TEXT NOT NULL,
  race TEXT NOT NULL,
  provinces INTEGER,
  doctrine_effect TEXT,
  current_bonus TEXT,
  source_fetch_id INTEGER,
  sha256 TEXT NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_kd_doctrine_snapshot_dedupe
ON kd_doctrine_snapshots(kingdom_coord, fetched_at_utc, race, sha256);

CREATE TABLE IF NOT EXISTS kd_war_history_snapshots (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  fetched_at_utc TEXT NOT NULL,
  kingdom_coord TEXT NOT NULL,
  opponent_name TEXT NOT NULL,
  opponent_coord TEXT,
  status TEXT,
  source_fetch_id INTEGER,
  sha256 TEXT NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_kd_war_history_snapshot_dedupe
ON kd_war_history_snapshots(kingdom_coord, fetched_at_utc, opponent_name, opponent_coord, sha256);

CREATE TABLE IF NOT EXISTS kd_ops_events (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  intel_op_id INTEGER,
  fetched_at_utc TEXT NOT NULL,
  last_updated_utc TEXT NOT NULL,
  event_time_text TEXT,
  server TEXT,
  category TEXT,
  op_type TEXT,
  op_name TEXT,
  result_code INTEGER,
  result_label TEXT,
  actor TEXT,
  actor_kingdom TEXT,
  target TEXT,
  target_kingdom TEXT,
  gain REAL,
  damage REAL,
  duration_ticks INTEGER,
  summary TEXT NOT NULL,
  raw_line TEXT NOT NULL,
  sha256 TEXT NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_kd_ops_events_intel_id
ON kd_ops_events(intel_op_id);

CREATE UNIQUE INDEX IF NOT EXISTS ux_kd_ops_events_sha
ON kd_ops_events(sha256);

CREATE INDEX IF NOT EXISTS ix_kd_ops_events_day
ON kd_ops_events(event_time_text);

CREATE INDEX IF NOT EXISTS ix_kd_ops_events_actor_target
ON kd_ops_events(actor, target);
"""


def connect() -> sqlite3.Connection:
    cn = sqlite3.connect(DB_PATH, timeout=30)
    cn.row_factory = sqlite3.Row
    cn.execute("PRAGMA busy_timeout=30000")
    return cn


def init_db() -> None:
    _with_retry(lambda cn: cn.executescript(SCHEMA))


def execute(sql: str, params: Iterable[Any] = ()) -> int:
    def _run(cn: sqlite3.Connection) -> int:
        cur = cn.execute(sql, params)
        return cur.rowcount

    return _with_retry(_run)


def executemany(sql: str, rows: Iterable[Iterable[Any]]) -> int:
    rows_list = list(rows)
    if not rows_list:
        return 0

    def _run(cn: sqlite3.Connection) -> int:
        cur = cn.executemany(sql, rows_list)
        return cur.rowcount

    return _with_retry(_run)


def fetchall(sql: str, params: Iterable[Any] = ()):  # noqa: ANN201
    return _with_retry(lambda cn: cn.execute(sql, params).fetchall())


def fetchone(sql: str, params: Iterable[Any] = ()):  # noqa: ANN201
    return _with_retry(lambda cn: cn.execute(sql, params).fetchone())


def _with_retry(fn, attempts: int = 4):  # noqa: ANN001, ANN201
    last_error = None
    for index in range(attempts):
        cn = connect()
        try:
            value = fn(cn)
            cn.commit()
            return value
        except sqlite3.OperationalError as exc:
            last_error = exc
            message = str(exc).lower()
            if "locked" not in message and "busy" not in message:
                raise
            if index == attempts - 1:
                raise
            time.sleep(0.15 * (index + 1))
        finally:
            cn.close()
    if last_error:
        raise last_error
