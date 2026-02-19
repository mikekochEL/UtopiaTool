import argparse
import copy
import os
import re
import threading
import time
import traceback
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from urllib.parse import urlencode

from flask import Flask, jsonify, render_template, request
from werkzeug.exceptions import HTTPException

from collector import load_config, run_once as run_collect_once
from db import fetchall, init_db
from parser import parse_and_store_news

app = Flask(__name__)

DEFAULT_POLL_SECONDS = 300
STATE_LOCK = threading.Lock()
STOP_EVENT = threading.Event()
INGEST_STATE: Dict[str, Any] = {
    "enabled": False,
    "running": False,
    "iterations": 0,
    "last_started_utc": None,
    "last_success_utc": None,
    "last_duration_seconds": None,
    "last_parsed_events": None,
    "last_error": None,
}
INGEST_THREAD: Optional[threading.Thread] = None
INGEST_THREAD_LOCK = threading.Lock()
CACHE_LOCK = threading.Lock()
CACHE_TTL_SECONDS = max(2, int(os.getenv("UTOPIA_CACHE_TTL_SECONDS", "15")))
ANALYTICS_CACHE: Dict[str, Dict[str, Any]] = {}

MONTH_INDEX = {
    "January": 1,
    "February": 2,
    "March": 3,
    "April": 4,
    "May": 5,
    "June": 6,
    "July": 7,
    "August": 8,
    "September": 9,
    "October": 10,
    "November": 11,
    "December": 12,
}
MONTH_NAME = {value: key for key, value in MONTH_INDEX.items()}

EVENT_DAY_RE = re.compile(
    r"^(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d+)\s+of\s+YR(\d+)$"
)
KINGDOM_COORD_RE = re.compile(r"\(\s*(\d+:\d+)\s*\)")
LEADING_SLOT_RE = re.compile(r"^\d+\s*-\s*")
AID_RE = re.compile(
    r"^(?P<actor>.+?) has sent an aid shipment to (?P<target>.+?)\.$",
    re.IGNORECASE,
)

ATTACK_SUCCESS_PATTERNS = [
    re.compile(
        r"^(?P<actor>.+?) captured (?P<acres>\d+) acres of land from (?P<target>.+?)\.?$",
        re.IGNORECASE,
    ),
    re.compile(
        r"^(?P<actor>.+?) invaded (?P<target>.+?) and captured (?P<acres>\d+) acres of land\.?$",
        re.IGNORECASE,
    ),
    re.compile(
        r"^(?P<actor>.+?) ambushed armies from (?P<target>.+?) and took (?P<acres>\d+) acres of land\.?$",
        re.IGNORECASE,
    ),
]
ATTACK_FAILED_PATTERNS = [
    re.compile(
        r"^(?P<actor>.+?) attempted an invasion of (?P<target>.+?), but was repelled\.?$",
        re.IGNORECASE,
    ),
    re.compile(
        r"^(?P<actor>.+?) attempted to invade (?P<target>.+?)\.?$",
        re.IGNORECASE,
    ),
]
ATTACK_SUCCESS_NO_ACRES_PATTERNS = [
    re.compile(r"^(?P<actor>.+?) invaded and pillaged (?P<target>.+?)\.?$", re.IGNORECASE),
    re.compile(r"^(?P<actor>.+?) attacked and pillaged the lands of (?P<target>.+?)\.?$", re.IGNORECASE),
    re.compile(r"^(?P<actor>.+?) learned (?P<target>.+?)\.?$", re.IGNORECASE),
]
ATTACK_RECAPTURE_PATTERN = re.compile(
    r"^(?P<actor>.+?) recaptured (?P<acres>\d+) acres of land from (?P<target>.+?)\.?$",
    re.IGNORECASE,
)
ATTACK_RAZE_TARGET_PATTERN = re.compile(
    r"^(?P<actor>.+?) razed (?P<acres>\d+) acres of (?P<target>\d+\s*-\s*.+?)\.?$",
    re.IGNORECASE,
)
ATTACK_INVADE_RAZE_PATTERN = re.compile(
    r"^(?P<actor>.+?) invaded (?P<target>.+?) and razed (?P<acres>\d+) acres of land\.?$",
    re.IGNORECASE,
)

WAR_DECLARE_PATTERNS = [
    re.compile(r"^We have declared WAR on (?P<opponent>.+?)!$", re.IGNORECASE),
    re.compile(r"^(?P<opponent>.+?) has declared WAR on us!?$", re.IGNORECASE),
]
WAR_END_OPPONENT_PATTERNS = [
    re.compile(r"withdrawn from war with (?P<opponent>.+?)(?:\.|!|$)", re.IGNORECASE),
    re.compile(r"won the war with (?P<opponent>.+?)(?:\.|!|$)", re.IGNORECASE),
    re.compile(r"war with (?P<opponent>.+?)(?: has finally ended| has ended|\.|!|$)", re.IGNORECASE),
]
POST_WAR_START_RE = re.compile(
    r"^Our kingdom is now in a post-war period which will expire on (?P<expiry>.+?)\.$",
    re.IGNORECASE,
)
POST_WAR_END_RE = re.compile(r"^Our post-war period has ended!?$", re.IGNORECASE)
ISO_DAY_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
INTEL_RESULT_RE = re.compile(r"Result:\s*(success|failed|partial|unknown)", re.IGNORECASE)
INTEL_OP_NAME_RE = re.compile(
    r"\[IntelSite\]\s+.+?\s+used\s+(?P<op>.+?)\.\s+Result:",
    re.IGNORECASE,
)
INTEL_GAIN_RE = re.compile(r"Gain:\s*(?P<value>[-\d,]+)", re.IGNORECASE)
INTEL_DAMAGE_RE = re.compile(r"Damage:\s*(?P<value>[-\d,]+)", re.IGNORECASE)
INTEL_DURATION_RE = re.compile(r"Duration:\s*(?P<value>\d+)\s*ticks?", re.IGNORECASE)

ATTACKER_RACES = {"orc", "human", "avian", "undead"}
TM_RACES = {"faery", "elf", "halfling", "dark elf"}

OP_IMPACT_WEIGHTS = {
    "night strike": 12.0,
    "propaganda": 12.0,
    "arson": 10.0,
    "greater arson": 12.0,
    "assassinate thieves": 11.0,
    "assassinate wizards": 11.0,
    "kidnap": 8.0,
    "rob the vaults": 8.0,
    "rob the towers": 6.0,
    "incite riots": 7.0,
    "bribe generals": 8.0,
    "bribe thieves": 7.0,
    "fireball": 9.0,
    "meteor showers": 10.0,
    "nightmare": 13.0,
    "land lust": 10.0,
    "tornadoes": 10.0,
    "greed": 7.0,
    "pitfalls": 6.0,
    "vermin": 6.0,
    "wrath": 8.0,
}

OP_SUPPORT_NAMES = {
    "minor protection",
    "greater protection",
    "magic shield",
    "fertile lands",
    "inspire army",
    "fanaticism",
    "bloodlust",
    "patriotism",
    "town watch",
    "quick feet",
    "aggression",
    "war spoils",
    "nature's blessing",
    "builders boon",
    "fountain of knowledge",
    "love and peace",
    "mystic aura",
    "reflect magic",
    "animate dead",
    "ghost workers",
    "miner's mystique",
    "mind focus",
    "mist",
    "revelation",
    "mage's fury",
    "guile",
    "invisibility",
}

OP_INTEL_KEYWORDS = (
    "spy",
    "survey",
    "crystal ball",
    "revelation",
    "shadow light",
    "illuminate shadows",
    "infiltrate",
)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def update_ingest_state(**kwargs: Any) -> None:
    with STATE_LOCK:
        INGEST_STATE.update(kwargs)


def snapshot_ingest_state() -> Dict[str, Any]:
    with STATE_LOCK:
        return dict(INGEST_STATE)


def cache_token() -> str:
    return (snapshot_ingest_state().get("last_success_utc") or "").strip()


def cache_get(key: str):  # noqa: ANN201
    token = cache_token()
    now = time.time()
    with CACHE_LOCK:
        entry = ANALYTICS_CACHE.get(key)
        if not entry:
            return None
        if entry.get("token") != token:
            ANALYTICS_CACHE.pop(key, None)
            return None
        if float(entry.get("expires", 0)) < now:
            ANALYTICS_CACHE.pop(key, None)
            return None
        return copy.deepcopy(entry.get("value"))


def cache_set(key: str, value: Any) -> None:
    token = cache_token()
    with CACHE_LOCK:
        ANALYTICS_CACHE[key] = {
            "token": token,
            "expires": time.time() + CACHE_TTL_SECONDS,
            "value": copy.deepcopy(value),
        }


def env_truthy(value: str | None, default: bool = False) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def default_config_path() -> str:
    return os.getenv("UTOPIA_CONFIG_PATH", "config.json")


def run_ingest_cycle(config_path: str) -> int:
    run_collect_once(config_path)
    return parse_and_store_news()


def parse_event_day(text: Optional[str]):
    if not text:
        return None

    match = EVENT_DAY_RE.match(text.strip())
    if not match:
        return None

    month_name, day_text, year_text = match.groups()
    return (int(year_text), MONTH_INDEX[month_name], int(day_text))


def safe_int_num(value: Any, default: int = 0) -> int:
    if value in (None, ""):
        return default
    try:
        return int(value)
    except Exception:
        text = str(value).strip()
        try:
            return int(float(text))
        except Exception:
            match = re.search(r"-?\d+", text)
            return int(match.group(0)) if match else default


def safe_float_num(value: Any, default: float = 0.0) -> float:
    if value in (None, ""):
        return default
    try:
        return float(value)
    except Exception:
        text = str(value).replace(",", "").strip()
        try:
            return float(text)
        except Exception:
            return default


def format_event_day(key: tuple[int, int, int]) -> str:
    year, month, day = key
    return f"{MONTH_NAME[month]} {day} of YR{year}"


def extract_kingdom(raw_name: Optional[str]) -> Optional[str]:
    if not raw_name:
        return None
    match = KINGDOM_COORD_RE.search(raw_name)
    return match.group(1) if match else None


def normalize_party(raw_name: Optional[str]) -> Optional[str]:
    if not raw_name:
        return None

    cleaned = raw_name.strip().strip(".")
    cleaned = LEADING_SLOT_RE.sub("", cleaned)
    cleaned = KINGDOM_COORD_RE.sub("", cleaned)
    cleaned = " ".join(cleaned.split()).strip(" -")
    return cleaned or None


def parse_aid_summary(summary: str) -> Optional[Dict[str, str]]:
    match = AID_RE.match((summary or "").strip())
    if not match:
        return None
    return {
        "actor_raw": match.group("actor"),
        "target_raw": match.group("target"),
    }


def classify_attack_type(summary: str) -> str:
    text = (summary or "").strip()
    lower = text.lower()

    if "ambushed armies from" in lower or "recaptured" in lower:
        return "Ambush"
    if "massacre" in lower:
        return "Massacre"
    if "learned" in lower or "learn attack" in lower:
        return "Learn"
    if "plundered" in lower or "pillaged" in lower:
        return "Plunder"
    if "razed" in lower:
        return "Raze"
    if "conquest" in lower or "conquered" in lower:
        return "Conquest"
    if "captured" in lower or "invaded" in lower:
        return "Traditional March"
    return "Other"


def effective_land_impact(
    acres_transfer: int,
    target_loss_acres: int,
    attack_type: str,
    *,
    is_war_context: bool,
) -> int:
    # In war, Raze destroys buildings and does not change land acres.
    if is_war_context and attack_type == "Raze":
        return 0
    return acres_transfer if acres_transfer > 0 else target_loss_acres


def parse_attack_summary(summary: str) -> Dict[str, Any]:
    text = (summary or "").strip()
    attack_type = classify_attack_type(text)

    for pattern in ATTACK_SUCCESS_PATTERNS:
        match = pattern.match(text)
        if match:
            return {
                "outcome": "success",
                "acres": int(match.group("acres")),
                "target_loss_acres": int(match.group("acres")),
                "actor_raw": match.group("actor"),
                "target_raw": match.group("target"),
                "attack_type": attack_type,
            }

    recapture_match = ATTACK_RECAPTURE_PATTERN.match(text)
    if recapture_match:
        acres = int(recapture_match.group("acres"))
        return {
            "outcome": "success",
            "acres": acres,
            "target_loss_acres": acres,
            "actor_raw": recapture_match.group("actor"),
            "target_raw": recapture_match.group("target"),
            "attack_type": attack_type,
        }

    raze_target_match = ATTACK_RAZE_TARGET_PATTERN.match(text)
    if raze_target_match:
        acres = int(raze_target_match.group("acres"))
        return {
            "outcome": "success",
            "acres": 0,
            "target_loss_acres": acres,
            "actor_raw": raze_target_match.group("actor"),
            "target_raw": raze_target_match.group("target"),
            "attack_type": attack_type,
        }

    invade_raze_match = ATTACK_INVADE_RAZE_PATTERN.match(text)
    if invade_raze_match:
        acres = int(invade_raze_match.group("acres"))
        return {
            "outcome": "success",
            "acres": 0,
            "target_loss_acres": acres,
            "actor_raw": invade_raze_match.group("actor"),
            "target_raw": invade_raze_match.group("target"),
            "attack_type": attack_type,
        }

    for pattern in ATTACK_FAILED_PATTERNS:
        match = pattern.match(text)
        if match:
            return {
                "outcome": "failed",
                "acres": 0,
                "target_loss_acres": 0,
                "actor_raw": match.group("actor"),
                "target_raw": match.group("target"),
                "attack_type": attack_type,
            }

    for pattern in ATTACK_SUCCESS_NO_ACRES_PATTERNS:
        match = pattern.match(text)
        if match:
            return {
                "outcome": "success",
                "acres": 0,
                "target_loss_acres": 0,
                "actor_raw": match.group("actor"),
                "target_raw": match.group("target"),
                "attack_type": attack_type,
            }

    return {
        "outcome": "unknown",
        "acres": 0,
        "target_loss_acres": 0,
        "actor_raw": None,
        "target_raw": None,
        "attack_type": attack_type,
    }


def extract_war_opponent(summary: str) -> Optional[str]:
    text = (summary or "").strip()

    for pattern in WAR_DECLARE_PATTERNS:
        match = pattern.match(text)
        if match:
            return match.group("opponent").strip()

    for pattern in WAR_END_OPPONENT_PATTERNS:
        match = pattern.search(text)
        if match:
            return match.group("opponent").strip()

    return None


def classify_war_event(summary: str) -> Optional[str]:
    lower = (summary or "").lower()

    if POST_WAR_START_RE.match(summary or ""):
        return "postwar_start"
    if POST_WAR_END_RE.match(summary or ""):
        return "postwar_end"
    if "declared war" in lower:
        return "declare"
    if "war" in lower and (
        "withdrawn from war" in lower
        or "won the war" in lower
        or "war has finally ended" in lower
        or ("war with" in lower and "has ended" in lower)
    ):
        return "end"

    return None


def classify_war_result(summary: str) -> str:
    lower = (summary or "").lower()

    if "failed war" in lower or "unable to achieve victory" in lower or "withdrawn from war" in lower:
        return "failed"
    if "won the war" in lower or "achieved victory" in lower:
        return "victory"
    if "mutual peace" in lower or "ended in peace" in lower:
        return "peace"
    if "war has finally ended" in lower or ("war with" in lower and "ended" in lower):
        return "ended"
    return "ended"


def ensure_party(stats_by_name: Dict[str, Dict[str, Any]], raw_name: Optional[str]) -> Optional[Dict[str, Any]]:
    name = normalize_party(raw_name)
    if not name:
        return None

    row = stats_by_name.setdefault(
        name,
        {
            "name": name,
            "kingdom": None,
            "gains": 0,
            "losses": 0,
            "net": 0,
            "attacks_sent": 0,
            "attacks_received": 0,
            "successful_attacks": 0,
            "failed_attacks": 0,
            "aid_sent": 0,
            "aid_received": 0,
            "activity": 0,
            "success_rate": 0.0,
        },
    )

    kingdom = extract_kingdom(raw_name)
    if kingdom and not row["kingdom"]:
        row["kingdom"] = kingdom

    return row


def fetch_event_rows() -> list[Dict[str, Any]]:
    return fetchall(
        """
        SELECT id, fetched_at_utc, event_time_text, category, actor, target, summary
        FROM kd_news_events
        ORDER BY id ASC
        """
    )


def fetch_ops_rows() -> list[Dict[str, Any]]:
    rows = fetchall(
        """
        SELECT id, intel_op_id, fetched_at_utc, last_updated_utc, event_time_text, server,
               category, op_type, op_name, result_code, result_label,
               actor, actor_kingdom, target, target_kingdom,
               gain, damage, duration_ticks, summary, raw_line
        FROM kd_ops_events
        ORDER BY id ASC
        """
    )
    return [dict(row) for row in rows]


def day_in_range(day_key, start_key, end_key) -> bool:
    if not day_key or not start_key:
        return False
    if day_key < start_key:
        return False
    if end_key and day_key > end_key:
        return False
    return True


def resolve_selected_war(war_rows: list[Dict[str, Any]], war_id: Optional[str]) -> Optional[Dict[str, Any]]:
    if not war_id:
        return None
    needle = str(war_id).strip()
    if not needle:
        return None
    for row in war_rows:
        if str(row.get("war_id", "")).strip() == needle:
            return row
    return None


def filter_rows_for_war(rows: list[Dict[str, Any]], selected_war: Optional[Dict[str, Any]]) -> list[Dict[str, Any]]:
    if not selected_war:
        return rows

    start_key = selected_war.get("start_key")
    end_key = selected_war.get("end_key")
    if not start_key:
        return rows

    filtered: list[Dict[str, Any]] = []
    for row in rows:
        day_key = parse_event_day(row["event_time_text"])
        if day_in_range(day_key, start_key, end_key):
            filtered.append(row)
    return filtered


def normalize_iso_day(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    text = value.strip()
    if not ISO_DAY_RE.match(text):
        return None
    return text


def filter_rows_for_fetched_day(
    rows: list[Dict[str, Any]],
    start_day: Optional[str],
    end_day: Optional[str],
) -> list[Dict[str, Any]]:
    if not start_day and not end_day:
        return rows

    filtered: list[Dict[str, Any]] = []
    for row in rows:
        fetched_day = (row["fetched_at_utc"] or "")[:10]
        if start_day and fetched_day < start_day:
            continue
        if end_day and fetched_day > end_day:
            continue
        filtered.append(row)
    return filtered


def filter_ops_rows_for_war(
    rows: list[Dict[str, Any]],
    selected_war: Optional[Dict[str, Any]],
) -> list[Dict[str, Any]]:
    if not selected_war:
        return rows

    start_key = selected_war.get("start_key")
    end_key = selected_war.get("end_key")
    if not start_key:
        return rows

    filtered: list[Dict[str, Any]] = []
    for row in rows:
        day_key = parse_event_day(row.get("event_time_text"))
        if day_in_range(day_key, start_key, end_key):
            filtered.append(row)
    return filtered


def scoped_ops_rows(
    selected_war: Optional[Dict[str, Any]],
    start_day: Optional[str],
    end_day: Optional[str],
) -> list[Dict[str, Any]]:
    war_key = str(selected_war.get("war_id")) if selected_war else "all"
    key = f"ops_scope:{war_key}:{start_day or '-'}:{end_day or '-'}"
    cached = cache_get(key)
    if cached is not None:
        return [dict(row) for row in cached]

    rows = fetch_ops_rows()
    rows = filter_ops_rows_for_war(rows, selected_war)
    rows = filter_rows_for_fetched_day(rows, start_day, end_day)
    cache_set(key, rows)
    return [dict(row) for row in rows]


def fetch_latest_kingdom_snapshots() -> list[Dict[str, Any]]:
    rows = fetchall(
        """
        SELECT s.*
        FROM kd_kingdom_snapshots s
        JOIN (
          SELECT kingdom_coord, MAX(id) AS max_id
          FROM kd_kingdom_snapshots
          GROUP BY kingdom_coord
        ) latest
          ON latest.max_id = s.id
        ORDER BY s.kingdom_coord ASC
        """
    )
    return [dict(row) for row in rows]


def fetch_previous_kingdom_snapshots() -> Dict[str, Dict[str, Any]]:
    rows = fetchall(
        """
        SELECT s.*
        FROM kd_kingdom_snapshots s
        WHERE s.id = (
          SELECT s2.id
          FROM kd_kingdom_snapshots s2
          WHERE s2.kingdom_coord = s.kingdom_coord
            AND s2.id < (
              SELECT MAX(s3.id)
              FROM kd_kingdom_snapshots s3
              WHERE s3.kingdom_coord = s.kingdom_coord
            )
          ORDER BY s2.id DESC
          LIMIT 1
        )
        """
    )
    return {row["kingdom_coord"]: dict(row) for row in rows}


def fetch_kingdom_trend_rows(kingdom_coord: str, limit: int = 240) -> list[Dict[str, Any]]:
    rows = fetchall(
        """
        SELECT fetched_at_utc, kingdom_coord, kingdom_name, stance, total_provinces,
               total_networth, total_land, total_honor
        FROM kd_kingdom_snapshots
        WHERE kingdom_coord=?
        ORDER BY id ASC
        LIMIT ?
        """,
        (kingdom_coord, limit),
    )
    return [dict(row) for row in rows]


def fetch_province_snapshot_rows(kingdom_coord: str, fetched_at_utc: str) -> list[Dict[str, Any]]:
    rows = fetchall(
        """
        SELECT fetched_at_utc, kingdom_coord, slot, province_name, race, land, networth, nwpa,
               nobility, is_monarch, is_steward, is_you, is_online
        FROM kd_province_snapshots
        WHERE kingdom_coord=? AND fetched_at_utc=?
        ORDER BY slot ASC
        """,
        (kingdom_coord, fetched_at_utc),
    )
    return [dict(row) for row in rows]


def fetch_doctrine_snapshot_rows(kingdom_coord: str, fetched_at_utc: str) -> list[Dict[str, Any]]:
    rows = fetchall(
        """
        SELECT race, provinces, doctrine_effect, current_bonus
        FROM kd_doctrine_snapshots
        WHERE kingdom_coord=? AND fetched_at_utc=?
        ORDER BY race ASC
        """,
        (kingdom_coord, fetched_at_utc),
    )
    return [dict(row) for row in rows]


def fetch_war_history_snapshot_rows(kingdom_coord: str, fetched_at_utc: str) -> list[Dict[str, Any]]:
    rows = fetchall(
        """
        SELECT opponent_name, opponent_coord, status
        FROM kd_war_history_snapshots
        WHERE kingdom_coord=? AND fetched_at_utc=?
        ORDER BY opponent_name ASC
        """,
        (kingdom_coord, fetched_at_utc),
    )
    return [dict(row) for row in rows]


def compress_daily_snapshot_rows(rows: list[Dict[str, Any]]) -> list[Dict[str, Any]]:
    by_day: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        day = (row["fetched_at_utc"] or "")[:10]
        if not day:
            continue
        by_day[day] = row
    return [by_day[day] for day in sorted(by_day)]


def build_home_nw_swing_rows(
    home_kingdom: Optional[str],
    scoped_event_rows: list[Dict[str, Any]],
    limit: int = 720,
) -> list[Dict[str, Any]]:
    if not home_kingdom:
        return []

    trend_rows = fetch_kingdom_trend_rows(home_kingdom, limit=limit)
    if not trend_rows:
        return []

    daily_map: Dict[str, Dict[str, Any]] = {}
    for row in trend_rows:
        day = (row.get("fetched_at_utc") or "")[:10]
        if not day:
            continue
        daily_map[day] = row

    scoped_days = {
        (row.get("fetched_at_utc") or "")[:10]
        for row in scoped_event_rows
        if (row.get("fetched_at_utc") or "")
    }
    day_keys = sorted(daily_map.keys())
    if scoped_days:
        filtered = [day for day in day_keys if day in scoped_days]
        if filtered:
            day_keys = filtered

    out = []
    prev_nw: Optional[int] = None
    prev_land: Optional[int] = None
    for day in day_keys:
        row = daily_map[day]
        nw = safe_int_num(row.get("total_networth"), 0)
        land = safe_int_num(row.get("total_land"), 0)
        delta_nw = nw - prev_nw if prev_nw is not None else 0
        delta_land = land - prev_land if prev_land is not None else 0
        prev_nw = nw
        prev_land = land
        out.append(
            {
                "day": day,
                "total_networth": nw,
                "delta_networth": delta_nw,
                "total_land": land,
                "delta_land": delta_land,
            }
        )

    return out


def build_roster_health(province_rows: list[Dict[str, Any]]) -> Dict[str, Any]:
    active = len(province_rows)
    empty_slots = max(0, 25 - active)
    online = sum(1 for row in province_rows if int(row.get("is_online") or 0) == 1)
    monarchs = sum(1 for row in province_rows if int(row.get("is_monarch") or 0) == 1)
    stewards = sum(1 for row in province_rows if int(row.get("is_steward") or 0) == 1)
    attackers = sum(1 for row in province_rows if str(row.get("race") or "").strip().lower() in ATTACKER_RACES)
    tms = sum(1 for row in province_rows if str(row.get("race") or "").strip().lower() in TM_RACES)
    nwpa_values = [float(row.get("nwpa") or 0.0) for row in province_rows if row.get("nwpa") is not None]
    avg_nwpa = round(sum(nwpa_values) / len(nwpa_values), 1) if nwpa_values else 0.0
    return {
        "active_provinces": active,
        "empty_slots": empty_slots,
        "online": online,
        "monarchs": monarchs,
        "stewards": stewards,
        "attackers": attackers,
        "tms": tms,
        "avg_nwpa": avg_nwpa,
    }


def snapshot_delta(current: Optional[Dict[str, Any]], previous: Optional[Dict[str, Any]], key: str) -> int:
    if not current or not previous:
        return 0
    return int(current.get(key) or 0) - int(previous.get(key) or 0)


def build_snapshot_analytics(
    event_analytics: Dict[str, Any],
    selected_war: Optional[Dict[str, Any]],
    requested_kingdom: Optional[str],
    requested_compare: Optional[str],
) -> Dict[str, Any]:
    latest_rows = fetch_latest_kingdom_snapshots()
    if not latest_rows:
        return {
            "available": False,
            "kingdom_choices": [],
            "focus_kingdom": None,
            "focus_label": "No kingdom snapshots yet",
            "focus_trend_rows": [],
            "focus_latest": None,
            "focus_previous": None,
            "focus_doctrines": [],
            "focus_war_history": [],
            "focus_roster": None,
            "race_mix": [],
            "top_land_rows": [],
            "delta_rows": [],
            "target_board_rows": [],
            "compare": None,
            "alerts": [],
        }

    latest_map = {row["kingdom_coord"]: row for row in latest_rows}
    previous_map = fetch_previous_kingdom_snapshots()

    kingdom_choices = [
        {
            "coord": row["kingdom_coord"],
            "name": row.get("kingdom_name") or "Unnamed kingdom",
            "label": f"{row.get('kingdom_name') or 'Unnamed kingdom'} ({row['kingdom_coord']})",
        }
        for row in latest_rows
    ]

    focus_kingdom = requested_kingdom
    if focus_kingdom not in latest_map:
        home = event_analytics.get("home_kingdom")
        if home in latest_map:
            focus_kingdom = home
        else:
            focus_kingdom = kingdom_choices[0]["coord"]

    compare_kingdom = requested_compare
    if compare_kingdom not in latest_map:
        if selected_war and selected_war.get("opponent_kingdom") in latest_map:
            compare_kingdom = selected_war["opponent_kingdom"]
        elif event_analytics.get("opponent_rows"):
            for row in event_analytics["opponent_rows"]:
                if row["kingdom"] in latest_map:
                    compare_kingdom = row["kingdom"]
                    break
    if compare_kingdom == focus_kingdom:
        compare_kingdom = None

    focus_latest = latest_map.get(focus_kingdom)
    focus_previous = previous_map.get(focus_kingdom)
    focus_trend_rows = fetch_kingdom_trend_rows(focus_kingdom) if focus_kingdom else []
    focus_trend_rows = compress_daily_snapshot_rows(focus_trend_rows)

    focus_provinces = (
        fetch_province_snapshot_rows(focus_kingdom, focus_latest["fetched_at_utc"])
        if focus_latest and focus_kingdom
        else []
    )
    focus_doctrines = (
        fetch_doctrine_snapshot_rows(focus_kingdom, focus_latest["fetched_at_utc"])
        if focus_latest and focus_kingdom
        else []
    )
    focus_war_history = (
        fetch_war_history_snapshot_rows(focus_kingdom, focus_latest["fetched_at_utc"])
        if focus_latest and focus_kingdom
        else []
    )
    focus_roster = build_roster_health(focus_provinces) if focus_provinces else None

    race_counter: Dict[str, int] = defaultdict(int)
    for row in focus_provinces:
        race = (row.get("race") or "Unknown").strip()
        race_counter[race] += 1
    race_mix = sorted(
        [{"race": race, "count": count} for race, count in race_counter.items()],
        key=lambda row: (-row["count"], row["race"]),
    )
    top_land_rows = sorted(
        focus_provinces,
        key=lambda row: (int(row.get("land") or 0), int(row.get("networth") or 0)),
        reverse=True,
    )[:12]

    delta_rows = []
    alerts = []
    for coord, current in latest_map.items():
        previous = previous_map.get(coord)
        delta_land = snapshot_delta(current, previous, "total_land")
        delta_nw = snapshot_delta(current, previous, "total_networth")
        delta_honor = snapshot_delta(current, previous, "total_honor")
        stance = (current.get("stance") or "").strip()
        previous_stance = (previous.get("stance") or "").strip() if previous else ""
        stance_changed = bool(previous and stance != previous_stance)
        delta_rows.append(
            {
                "coord": coord,
                "name": current.get("kingdom_name") or "Unnamed kingdom",
                "fetched_at_utc": current.get("fetched_at_utc"),
                "stance": stance or "-",
                "prev_stance": previous_stance or "-",
                "delta_land": delta_land,
                "delta_nw": delta_nw,
                "delta_honor": delta_honor,
                "delta_provinces": snapshot_delta(current, previous, "total_provinces"),
                "current_land": int(current.get("total_land") or 0),
                "current_nw": int(current.get("total_networth") or 0),
                "current_honor": int(current.get("total_honor") or 0),
                "stance_changed": stance_changed,
            }
        )
        if abs(delta_land) >= 150:
            alerts.append(
                f"{current.get('kingdom_name') or coord} ({coord}) land swing {delta_land:+d} acres since prior snapshot."
            )
        if stance_changed:
            alerts.append(
                f"{current.get('kingdom_name') or coord} ({coord}) stance changed {previous_stance or '-'} -> {stance or '-'}."
            )

    delta_rows.sort(
        key=lambda row: (abs(row["delta_land"]), abs(row["delta_nw"]), abs(row["delta_honor"])),
        reverse=True,
    )
    delta_rows = delta_rows[:24]

    province_metric_map: Dict[tuple[str, str], Dict[str, Any]] = {}
    for row in event_analytics.get("province_rows", []):
        key = (str(row.get("name") or "").lower(), str(row.get("kingdom") or ""))
        province_metric_map[key] = row

    target_kingdoms: List[str] = []
    if compare_kingdom:
        target_kingdoms.append(compare_kingdom)
    for row in event_analytics.get("opponent_rows", []):
        kd = row["kingdom"]
        if kd not in target_kingdoms and kd in latest_map:
            target_kingdoms.append(kd)
        if len(target_kingdoms) >= 4:
            break

    target_board_rows: List[Dict[str, Any]] = []
    for kd in target_kingdoms:
        snap = latest_map.get(kd)
        if not snap:
            continue
        province_rows = fetch_province_snapshot_rows(kd, snap["fetched_at_utc"])
        for row in province_rows:
            name = row.get("province_name") or ""
            metrics = province_metric_map.get((name.lower(), kd), {})
            nwpa = float(row.get("nwpa") or 0.0)
            incoming = int(metrics.get("attacks_received") or 0)
            losses = int(metrics.get("losses") or 0)
            gains = int(metrics.get("gains") or 0)
            activity = int(metrics.get("activity") or 0)
            score = round(max(0.0, 280.0 - nwpa) * 1.15 + incoming * 6.5 + losses * 0.11 - gains * 0.03, 1)
            target_board_rows.append(
                {
                    "kingdom": kd,
                    "province": name,
                    "race": row.get("race") or "-",
                    "land": int(row.get("land") or 0),
                    "nwpa": round(nwpa, 1),
                    "incoming_hits": incoming,
                    "losses": losses,
                    "activity": activity,
                    "score": score,
                }
            )
    target_board_rows.sort(key=lambda row: (row["score"], row["incoming_hits"], -row["nwpa"]), reverse=True)
    target_board_rows = target_board_rows[:36]

    compare = None
    if focus_kingdom and compare_kingdom and focus_kingdom in latest_map and compare_kingdom in latest_map:
        left_latest = latest_map[focus_kingdom]
        right_latest = latest_map[compare_kingdom]
        left_previous = previous_map.get(focus_kingdom)
        right_previous = previous_map.get(compare_kingdom)

        left_trend = compress_daily_snapshot_rows(fetch_kingdom_trend_rows(focus_kingdom))
        right_trend = compress_daily_snapshot_rows(fetch_kingdom_trend_rows(compare_kingdom))
        left_by_day = {(row["fetched_at_utc"] or "")[:10]: row for row in left_trend}
        right_by_day = {(row["fetched_at_utc"] or "")[:10]: row for row in right_trend}
        trend_days = sorted(set(left_by_day) | set(right_by_day))
        compare_trend_rows = []
        for day in trend_days:
            left_row = left_by_day.get(day, {})
            right_row = right_by_day.get(day, {})
            compare_trend_rows.append(
                {
                    "day": day,
                    "left_land": int(left_row.get("total_land") or 0),
                    "right_land": int(right_row.get("total_land") or 0),
                    "left_nw": int(left_row.get("total_networth") or 0),
                    "right_nw": int(right_row.get("total_networth") or 0),
                    "left_honor": int(left_row.get("total_honor") or 0),
                    "right_honor": int(right_row.get("total_honor") or 0),
                }
            )

        left_contrib = sorted(
            [row for row in event_analytics.get("province_rows", []) if row.get("kingdom") == focus_kingdom],
            key=lambda row: (row.get("net", 0), row.get("activity", 0)),
            reverse=True,
        )[:10]
        right_contrib = sorted(
            [row for row in event_analytics.get("province_rows", []) if row.get("kingdom") == compare_kingdom],
            key=lambda row: (row.get("attacks_sent", 0), row.get("activity", 0)),
            reverse=True,
        )[:10]

        compare = {
            "left": {
                "coord": focus_kingdom,
                "name": left_latest.get("kingdom_name") or "Unnamed kingdom",
                "land": int(left_latest.get("total_land") or 0),
                "networth": int(left_latest.get("total_networth") or 0),
                "honor": int(left_latest.get("total_honor") or 0),
                "provinces": int(left_latest.get("total_provinces") or 0),
                "stance": left_latest.get("stance") or "-",
                "d_land": snapshot_delta(left_latest, left_previous, "total_land"),
                "d_nw": snapshot_delta(left_latest, left_previous, "total_networth"),
                "d_honor": snapshot_delta(left_latest, left_previous, "total_honor"),
            },
            "right": {
                "coord": compare_kingdom,
                "name": right_latest.get("kingdom_name") or "Unnamed kingdom",
                "land": int(right_latest.get("total_land") or 0),
                "networth": int(right_latest.get("total_networth") or 0),
                "honor": int(right_latest.get("total_honor") or 0),
                "provinces": int(right_latest.get("total_provinces") or 0),
                "stance": right_latest.get("stance") or "-",
                "d_land": snapshot_delta(right_latest, right_previous, "total_land"),
                "d_nw": snapshot_delta(right_latest, right_previous, "total_networth"),
                "d_honor": snapshot_delta(right_latest, right_previous, "total_honor"),
            },
            "trend_rows": compare_trend_rows[-90:],
            "left_contrib": left_contrib,
            "right_contrib": right_contrib,
        }

    if selected_war and selected_war.get("result") == "active":
        alerts.insert(
            0,
            f"Active war scope: {selected_war.get('opponent_name')} ({selected_war.get('opponent_kingdom') or '?'})",
        )
    alerts = alerts[:10]

    focus_label = "No focus kingdom selected"
    if focus_latest:
        focus_label = f"{focus_latest.get('kingdom_name') or 'Unnamed kingdom'} ({focus_kingdom})"

    return {
        "available": True,
        "kingdom_choices": kingdom_choices,
        "focus_kingdom": focus_kingdom,
        "focus_label": focus_label,
        "focus_trend_rows": focus_trend_rows[-120:],
        "focus_latest": focus_latest,
        "focus_previous": focus_previous,
        "focus_doctrines": focus_doctrines,
        "focus_war_history": focus_war_history,
        "focus_roster": focus_roster,
        "race_mix": race_mix,
        "top_land_rows": top_land_rows,
        "delta_rows": delta_rows,
        "target_board_rows": target_board_rows,
        "compare": compare,
        "alerts": alerts,
    }


def build_momentum_rows(rows: list[Dict[str, Any]]) -> list[Dict[str, Any]]:
    counts_by_day: dict[tuple[int, int, int], dict[str, int]] = defaultdict(lambda: defaultdict(int))
    for row in rows:
        day_key = parse_event_day(row["event_time_text"])
        if not day_key:
            continue
        category = (row["category"] or "other").lower()
        counts_by_day[day_key][category] += 1

    payload: list[Dict[str, Any]] = []
    for day_key in sorted(counts_by_day):
        for category in sorted(counts_by_day[day_key]):
            payload.append(
                {
                    "day": format_event_day(day_key),
                    "category": category,
                    "cnt": counts_by_day[day_key][category],
                }
            )
    return payload


def build_war_rows(
    war_events: list[Dict[str, Any]],
    attack_records: list[Dict[str, Any]],
    home_kingdom: Optional[str],
) -> list[Dict[str, Any]]:
    wars: list[Dict[str, Any]] = []
    open_wars: list[Dict[str, Any]] = []
    war_seq = 0

    def day_in_war(day_key, war_row) -> bool:
        if not day_key or not war_row["start_key"]:
            return False
        if day_key < war_row["start_key"]:
            return False
        if war_row["end_key"] and day_key > war_row["end_key"]:
            return False
        return True

    def find_open_war(opponent_kingdom: Optional[str], opponent_name: Optional[str]):
        for war_row in reversed(open_wars):
            if opponent_kingdom and war_row["opponent_kingdom"] == opponent_kingdom:
                return war_row
            if opponent_name and war_row["opponent_name"].lower() == opponent_name.lower():
                return war_row
        if open_wars:
            return open_wars[-1]
        return None

    ordered_war_events = sorted(
        war_events,
        key=lambda event: (
            event["day_key"] if event["day_key"] else (9999, 99, 99),
            event["row_id"],
        ),
    )

    for event in ordered_war_events:
        summary = event["summary"]
        event_type = classify_war_event(summary)
        if not event_type:
            continue

        day_key = event["day_key"]
        day_text = event["event_time_text"] or ""

        if event_type == "declare":
            war_seq += 1
            opponent_raw = extract_war_opponent(summary)
            opponent_name = normalize_party(opponent_raw) or (opponent_raw or "Unknown Kingdom")
            opponent_kingdom = extract_kingdom(opponent_raw)

            war_row = {
                "war_id": str(war_seq),
                "opponent_name": opponent_name,
                "opponent_kingdom": opponent_kingdom,
                "start_day": day_text,
                "start_key": day_key,
                "end_day": "",
                "end_key": None,
                "result": "active",
                "status": "active",
                "end_summary": "",
                "postwar_expires": "",
                "postwar_end_day": "",
                "hits_for": 0,
                "hits_against": 0,
                "acres_for": 0,
                "acres_against": 0,
                "net_acres": 0,
            }
            wars.append(war_row)
            open_wars.append(war_row)
            continue

        if event_type == "end":
            opponent_raw = extract_war_opponent(summary)
            opponent_name = normalize_party(opponent_raw) if opponent_raw else None
            opponent_kingdom = extract_kingdom(opponent_raw) if opponent_raw else None
            war_row = find_open_war(opponent_kingdom, opponent_name)
            if not war_row:
                continue

            war_row["end_day"] = day_text
            war_row["end_key"] = day_key
            war_row["end_summary"] = summary
            war_row["result"] = classify_war_result(summary)
            war_row["status"] = "closed"
            open_wars = [row for row in open_wars if row is not war_row]
            continue

        if event_type == "postwar_start":
            match = POST_WAR_START_RE.match(summary)
            if not match:
                continue
            expiry_day = match.group("expiry").strip()
            for war_row in reversed(wars):
                if war_row["status"] == "closed" and not war_row["postwar_expires"]:
                    war_row["postwar_expires"] = expiry_day
                    break
            continue

        if event_type == "postwar_end":
            for war_row in reversed(wars):
                if war_row["postwar_expires"] and not war_row["postwar_end_day"]:
                    war_row["postwar_end_day"] = day_text
                    break

    if home_kingdom:
        for war_row in wars:
            opponent_kingdom = war_row["opponent_kingdom"]
            if not opponent_kingdom:
                continue

            for attack in attack_records:
                if not day_in_war(attack["day_key"], war_row):
                    continue

                actor_kingdom = attack["actor_kingdom"]
                target_kingdom = attack["target_kingdom"]
                acres_transfer = attack["acres"]
                acres_lost = attack["target_loss_acres"]
                attack_type = attack.get("attack_type", "Other")
                impact_acres = effective_land_impact(
                    int(acres_transfer),
                    int(acres_lost),
                    str(attack_type),
                    is_war_context=True,
                )

                if actor_kingdom == home_kingdom and target_kingdom == opponent_kingdom:
                    war_row["hits_for"] += 1
                    if attack["outcome"] == "success" and impact_acres > 0:
                        war_row["acres_for"] += impact_acres
                elif actor_kingdom == opponent_kingdom and target_kingdom == home_kingdom:
                    war_row["hits_against"] += 1
                    if attack["outcome"] == "success" and impact_acres > 0:
                        war_row["acres_against"] += impact_acres

            war_row["net_acres"] = war_row["acres_for"] - war_row["acres_against"]

    for war_row in wars:
        if war_row["status"] == "active":
            war_row["result"] = "active"
        if not war_row["end_day"]:
            war_row["end_day"] = "-"
        if not war_row["postwar_expires"]:
            war_row["postwar_expires"] = "-"
        if not war_row["postwar_end_day"]:
            war_row["postwar_end_day"] = "-"
        war_row["war_label"] = (
            f"{war_row['start_day']} -> {war_row['end_day']} vs {war_row['opponent_name']} "
            f"({war_row['opponent_kingdom'] or '?'}) [{war_row['result']}]"
        )

    wars.sort(
        key=lambda row: row["start_key"] if row["start_key"] else (-1, -1, -1),
        reverse=True,
    )
    return wars


def build_dashboard_analytics(
    rows: Optional[list[Dict[str, Any]]] = None,
    forced_home_kingdom: Optional[str] = None,
    include_wars: bool = True,
) -> Dict[str, Any]:
    if rows is None:
        rows = fetch_event_rows()

    category_counts: dict[str, int] = defaultdict(int)
    provinces: Dict[str, Dict[str, Any]] = {}
    kingdom_mentions: dict[str, int] = defaultdict(int)
    attack_records: list[Dict[str, Any]] = []
    war_events: list[Dict[str, Any]] = []

    successful_hits = 0
    failed_hits = 0
    acres_exchanged = 0
    aid_shipments = 0

    for row in rows:
        category = (row["category"] or "other").lower()
        summary = row["summary"] or ""
        event_day_text = row["event_time_text"] or ""
        event_day_key = parse_event_day(event_day_text)
        category_counts[category] += 1

        war_event_type = classify_war_event(summary)
        if war_event_type:
            war_events.append(
                {
                    "row_id": row["id"],
                    "event_type": war_event_type,
                    "summary": summary,
                    "event_time_text": event_day_text,
                    "day_key": event_day_key,
                }
            )

        if category == "aid":
            aid = parse_aid_summary(summary)
            actor_raw = aid["actor_raw"] if aid else row["actor"]
            target_raw = aid["target_raw"] if aid else row["target"]
            actor = ensure_party(provinces, actor_raw)
            target = ensure_party(provinces, target_raw)
            if actor:
                actor["aid_sent"] += 1
                if actor["kingdom"]:
                    kingdom_mentions[actor["kingdom"]] += 1
            if target:
                target["aid_received"] += 1
                if target["kingdom"]:
                    kingdom_mentions[target["kingdom"]] += 1
            if aid:
                aid_shipments += 1
            continue

        if category != "attack":
            continue

        attack = parse_attack_summary(summary)
        actor_raw = attack["actor_raw"] or row["actor"]
        target_raw = attack["target_raw"] or row["target"]
        actor = ensure_party(provinces, actor_raw)
        target = ensure_party(provinces, target_raw)

        actor_kingdom = actor["kingdom"] if actor else extract_kingdom(actor_raw)
        target_kingdom = target["kingdom"] if target else extract_kingdom(target_raw)

        if actor:
            actor["attacks_sent"] += 1
        if target:
            target["attacks_received"] += 1
        if actor_kingdom:
            kingdom_mentions[actor_kingdom] += 1
        if target_kingdom:
            kingdom_mentions[target_kingdom] += 1

        if attack["outcome"] == "failed":
            failed_hits += 1
            if actor:
                actor["failed_attacks"] += 1
        elif attack["outcome"] == "success":
            successful_hits += 1
            if actor:
                actor["successful_attacks"] += 1
            transfer_acres = int(attack["acres"])
            target_loss_acres = int(attack.get("target_loss_acres", transfer_acres))
            if transfer_acres > 0:
                acres_exchanged += transfer_acres
                if actor:
                    actor["gains"] += transfer_acres
            if target_loss_acres > 0 and target:
                target["losses"] += target_loss_acres

        attack_records.append(
            {
                "row_id": row["id"],
                "day_key": event_day_key,
                "actor_kingdom": actor_kingdom,
                "target_kingdom": target_kingdom,
                "actor_name": actor["name"] if actor else (normalize_party(actor_raw) or ""),
                "target_name": target["name"] if target else (normalize_party(target_raw) or ""),
                "outcome": attack["outcome"],
                "attack_type": attack.get("attack_type", "Other"),
                "acres": int(attack["acres"]),
                "target_loss_acres": int(attack.get("target_loss_acres", attack["acres"])),
            }
        )

    home_kingdom = forced_home_kingdom
    if not home_kingdom and kingdom_mentions:
        home_kingdom = sorted(kingdom_mentions.items(), key=lambda item: (-item[1], item[0]))[0][0]

    war_rows = build_war_rows(war_events, attack_records, home_kingdom) if include_wars else []

    def is_raze_in_home_war(rec: Dict[str, Any]) -> bool:
        if rec.get("outcome") != "success":
            return False
        if rec.get("attack_type") != "Raze":
            return False
        if not home_kingdom:
            return False

        day_key = rec.get("day_key")
        if not day_key:
            return False

        actor_kingdom = rec.get("actor_kingdom")
        target_kingdom = rec.get("target_kingdom")
        if actor_kingdom == home_kingdom and target_kingdom and target_kingdom != home_kingdom:
            opponent_kingdom = target_kingdom
        elif target_kingdom == home_kingdom and actor_kingdom and actor_kingdom != home_kingdom:
            opponent_kingdom = actor_kingdom
        else:
            return False

        for war_row in war_rows:
            if war_row.get("opponent_kingdom") != opponent_kingdom:
                continue
            if day_in_range(day_key, war_row.get("start_key"), war_row.get("end_key")):
                return True

        return False

    # Province loss correction: war Raze does building damage only, not land loss.
    for rec in attack_records:
        if not is_raze_in_home_war(rec):
            continue
        target_name = rec.get("target_name")
        if not target_name or target_name not in provinces:
            continue
        old_loss = int(provinces[target_name]["losses"])
        provinces[target_name]["losses"] = max(0, old_loss - int(rec.get("target_loss_acres", 0)))

    opponent_pressure: dict[str, Dict[str, Any]] = defaultdict(
        lambda: {
            "kingdom": None,
            "hits_for": 0,
            "hits_against": 0,
            "acres_for": 0,
            "acres_against": 0,
            "net": 0,
        }
    )
    home_land_by_day: dict[tuple[int, int, int], Dict[str, int]] = defaultdict(lambda: {"gained": 0, "lost": 0})

    if home_kingdom:
        for rec in attack_records:
            actor_kingdom = rec["actor_kingdom"]
            target_kingdom = rec["target_kingdom"]
            acres_transfer = rec["acres"]
            acres_lost = rec["target_loss_acres"]
            impact_acres = effective_land_impact(
                int(acres_transfer),
                int(acres_lost),
                str(rec.get("attack_type", "Other")),
                is_war_context=is_raze_in_home_war(rec),
            )
            day_key = rec["day_key"]

            if actor_kingdom == home_kingdom and target_kingdom and target_kingdom != home_kingdom:
                opp = opponent_pressure[target_kingdom]
                opp["kingdom"] = target_kingdom
                opp["hits_for"] += 1
                if rec["outcome"] == "success" and impact_acres > 0:
                    opp["acres_for"] += impact_acres
                    if day_key and acres_transfer > 0:
                        home_land_by_day[day_key]["gained"] += acres_transfer

            if target_kingdom == home_kingdom and actor_kingdom and actor_kingdom != home_kingdom:
                opp = opponent_pressure[actor_kingdom]
                opp["kingdom"] = actor_kingdom
                opp["hits_against"] += 1
                if rec["outcome"] == "success" and impact_acres > 0:
                    opp["acres_against"] += impact_acres
                    if day_key:
                        home_land_by_day[day_key]["lost"] += impact_acres

    province_rows: list[Dict[str, Any]] = []
    for province in provinces.values():
        row = dict(province)
        row["net"] = row["gains"] - row["losses"]
        row["activity"] = row["attacks_sent"] + row["attacks_received"] + row["aid_sent"] + row["aid_received"]
        row["success_rate"] = (
            round((row["successful_attacks"] / row["attacks_sent"]) * 100.0, 1)
            if row["attacks_sent"] > 0
            else 0.0
        )
        province_rows.append(row)

    friendly_rows = (
        [row for row in province_rows if row["kingdom"] == home_kingdom]
        if home_kingdom
        else list(province_rows)
    )
    active_rows = sorted(
        province_rows,
        key=lambda row: (row["activity"], row["attacks_sent"], row["aid_sent"]),
        reverse=True,
    )[:18]
    target_rows = sorted(
        province_rows,
        key=lambda row: (row["attacks_received"], row["losses"]),
        reverse=True,
    )[:18]
    friendly_leaderboard = sorted(
        friendly_rows,
        key=lambda row: (row["net"], row["gains"], row["activity"]),
        reverse=True,
    )[:18]

    opponent_rows = []
    for row in opponent_pressure.values():
        row["net"] = row["acres_for"] - row["acres_against"]
        opponent_rows.append(row)
    opponent_rows = sorted(
        opponent_rows,
        key=lambda row: (row["hits_for"] + row["hits_against"], abs(row["net"])),
        reverse=True,
    )[:14]

    land_swing_rows = []
    for day_key in sorted(home_land_by_day):
        gained = home_land_by_day[day_key]["gained"]
        lost = home_land_by_day[day_key]["lost"]
        land_swing_rows.append(
            {
                "day": format_event_day(day_key),
                "gained": gained,
                "lost": lost,
                "net": gained - lost,
            }
        )

    home_total_gained = sum(row["gained"] for row in land_swing_rows)
    home_total_lost = sum(row["lost"] for row in land_swing_rows)
    attack_outcome_total = successful_hits + failed_hits
    attack_success_rate = round((successful_hits / attack_outcome_total) * 100.0, 1) if attack_outcome_total else 0.0

    category_totals = sorted(
        [{"category": category, "cnt": count} for category, count in category_counts.items()],
        key=lambda row: row["cnt"],
        reverse=True,
    )
    active_wars = sum(1 for row in war_rows if row["status"] == "active")
    completed_wars = len(war_rows) - active_wars
    war_victories = sum(1 for row in war_rows if row["result"] == "victory")
    war_failures = sum(1 for row in war_rows if row["result"] == "failed")

    return {
        "home_kingdom": home_kingdom,
        "category_totals": category_totals,
        "war_rows": war_rows,
        "province_rows": province_rows,
        "friendly_leaderboard": friendly_leaderboard,
        "active_rows": active_rows,
        "target_rows": target_rows,
        "opponent_rows": opponent_rows,
        "land_swing_rows": land_swing_rows,
        "kpis": {
            "total_events": len(rows),
            "total_attacks": category_counts.get("attack", 0),
            "successful_hits": successful_hits,
            "failed_hits": failed_hits,
            "attack_success_rate": attack_success_rate,
            "acres_exchanged": acres_exchanged,
            "home_gained": home_total_gained,
            "home_lost": home_total_lost,
            "home_net": home_total_gained - home_total_lost,
            "aid_shipments": aid_shipments,
            "dragons": category_counts.get("dragon", 0),
            "diplomacy": category_counts.get("diplomacy", 0),
            "tracked_provinces": len(province_rows),
            "tracked_kingdoms": len(kingdom_mentions),
            "active_wars": active_wars,
            "completed_wars": completed_wars,
            "war_victories": war_victories,
            "war_failures": war_failures,
        },
    }


def build_event_entry(row: Dict[str, Any]) -> Dict[str, Any]:
    category = (row["category"] or "other").lower()
    summary = row["summary"] or ""
    actor_raw = row["actor"]
    target_raw = row["target"]
    actor_kingdom = extract_kingdom(actor_raw)
    target_kingdom = extract_kingdom(target_raw)
    outcome = ""
    acres = None
    acres_transfer = 0
    target_loss_acres = 0
    attack_type = "-"
    op_gain = 0
    op_damage = 0
    op_duration_ticks = 0

    if category == "attack":
        attack = parse_attack_summary(summary)
        actor_raw = attack["actor_raw"] or actor_raw
        target_raw = attack["target_raw"] or target_raw
        actor_kingdom = extract_kingdom(actor_raw)
        target_kingdom = extract_kingdom(target_raw)
        outcome = attack["outcome"]
        attack_type = attack.get("attack_type", "-")
        acres_value = int(attack["acres"])
        acres_transfer = acres_value
        target_loss_value = int(attack.get("target_loss_acres", 0))
        target_loss_acres = target_loss_value
        impact_acres = acres_value if acres_value > 0 else target_loss_value
        acres = impact_acres if impact_acres > 0 else None
    elif category == "aid":
        aid = parse_aid_summary(summary)
        if aid:
            actor_raw = aid["actor_raw"]
            target_raw = aid["target_raw"]
            actor_kingdom = extract_kingdom(actor_raw)
            target_kingdom = extract_kingdom(target_raw)
            outcome = "aid"
    elif category in {"thievery", "magic"}:
        result_match = INTEL_RESULT_RE.search(summary)
        if result_match:
            outcome = result_match.group(1).lower()
        op_name = extract_intel_operation_name(summary, target_raw or "")
        if op_name:
            attack_type = op_name
        gain_match = INTEL_GAIN_RE.search(summary)
        if gain_match:
            op_gain = int(gain_match.group("value").replace(",", ""))
        damage_match = INTEL_DAMAGE_RE.search(summary)
        if damage_match:
            op_damage = int(damage_match.group("value").replace(",", ""))
        duration_match = INTEL_DURATION_RE.search(summary)
        if duration_match:
            op_duration_ticks = int(duration_match.group("value"))

    return {
        "event_id": row["id"],
        "fetched_at_utc": row["fetched_at_utc"],
        "event_time_text": row["event_time_text"] or "",
        "category": category,
        "actor": normalize_party(actor_raw) or "-",
        "target": normalize_party(target_raw) or "-",
        "actor_kingdom": actor_kingdom,
        "target_kingdom": target_kingdom,
        "acres": acres,
        "acres_transfer": acres_transfer,
        "target_loss_acres": target_loss_acres,
        "attack_type": attack_type,
        "op_gain": op_gain,
        "op_damage": op_damage,
        "op_duration_ticks": op_duration_ticks,
        "outcome": outcome,
        "summary": summary,
    }


def normalize_operation_name(event: Dict[str, Any]) -> str:
    category = (event.get("category") or "").lower()
    if category not in {"thievery", "magic"}:
        return "-"

    attack_type = (event.get("attack_type") or "").strip()
    if attack_type and attack_type != "-":
        return attack_type

    summary = event.get("summary") or ""
    op_match = INTEL_OP_NAME_RE.search(summary)
    if op_match:
        return op_match.group("op").strip()

    return "Unknown Op"


def operation_key(name: str) -> str:
    return " ".join((name or "").strip().lower().split())


def extract_intel_operation_name(summary: str, target_raw: str) -> str:
    op_match = INTEL_OP_NAME_RE.search(summary or "")
    if not op_match:
        return ""

    op_text = op_match.group("op").strip()
    target_name = normalize_party(target_raw) or (target_raw or "").strip()
    if not target_name or target_name == "-":
        return op_text

    suffix = f" on {target_name}"
    if op_text.lower().endswith(suffix.lower()):
        return op_text[: -len(suffix)].strip()

    return op_text


def classify_operation_kind(op_name: str, actor: str, target: str) -> str:
    op_key = operation_key(op_name)
    actor_key = operation_key(actor)
    target_key = operation_key(target)

    if actor_key and target_key and actor_key == target_key:
        return "support"
    if op_key in OP_SUPPORT_NAMES:
        return "support"
    if any(token in op_key for token in OP_INTEL_KEYWORDS):
        return "intel"
    return "hostile"


def operation_outcome_multiplier(outcome: str) -> float:
    value = (outcome or "").strip().lower()
    if value == "success":
        return 1.0
    if value == "partial":
        return 0.5
    return 0.0


def operation_impact_points(op_name: str, outcome: str, op_kind: str) -> float:
    if op_kind != "hostile":
        return 0.0
    base = OP_IMPACT_WEIGHTS.get(operation_key(op_name), 5.0)
    return round(base * operation_outcome_multiplier(outcome), 2)


def build_latest_feed(rows: Optional[list[Dict[str, Any]]] = None, limit: int = 80) -> list[Dict[str, Any]]:
    if rows is None:
        rows = fetchall(
            """
            SELECT id, fetched_at_utc, event_time_text, category, actor, target, summary
            FROM kd_news_events
            ORDER BY id DESC
            LIMIT ?
            """,
            (limit,),
        )
    else:
        rows = sorted(rows, key=lambda row: row["id"], reverse=True)[:limit]

    return [build_event_entry(row) for row in rows]


def event_in_home_war(
    day_key,
    actor_kingdom: Optional[str],
    target_kingdom: Optional[str],
    home_kingdom: Optional[str],
    war_rows: list[Dict[str, Any]],
) -> bool:
    if not day_key or not home_kingdom:
        return False

    if actor_kingdom == home_kingdom and target_kingdom and target_kingdom != home_kingdom:
        opponent_kingdom = target_kingdom
    elif target_kingdom == home_kingdom and actor_kingdom and actor_kingdom != home_kingdom:
        opponent_kingdom = actor_kingdom
    else:
        return False

    for war_row in war_rows:
        if war_row.get("opponent_kingdom") != opponent_kingdom:
            continue
        if day_in_range(day_key, war_row.get("start_key"), war_row.get("end_key")):
            return True
    return False


def build_province_history(
    province_name: str,
    province_kingdom: Optional[str],
    rows: list[Dict[str, Any]],
    home_kingdom: Optional[str],
    war_rows: list[Dict[str, Any]],
    limit: int = 120,
) -> Optional[Dict[str, Any]]:
    normalized = normalize_party(province_name)
    if not normalized:
        return None

    target_name = normalized.lower()
    target_kingdom = (province_kingdom or "").strip()

    stats = {
        "attacks_sent": 0,
        "attacks_received": 0,
        "aid_sent": 0,
        "aid_received": 0,
        "ops_sent": 0,
        "ops_received": 0,
        "magic_sent": 0,
        "magic_received": 0,
        "gains": 0,
        "losses": 0,
    }
    entries: list[Dict[str, Any]] = []

    for row in rows:
        event = build_event_entry(row)
        actor_match = event["actor"].lower() == target_name
        target_match = event["target"].lower() == target_name

        if target_kingdom:
            if actor_match and event["actor_kingdom"] and event["actor_kingdom"] != target_kingdom:
                actor_match = False
            if target_match and event["target_kingdom"] and event["target_kingdom"] != target_kingdom:
                target_match = False

        if not actor_match and not target_match:
            continue

        day_key = parse_event_day(event["event_time_text"])
        war_context = event_in_home_war(
            day_key,
            event.get("actor_kingdom"),
            event.get("target_kingdom"),
            home_kingdom,
            war_rows,
        )
        is_war_raze = war_context and event.get("attack_type") == "Raze"

        role = "both" if actor_match and target_match else ("actor" if actor_match else "target")
        category = event["category"]
        outcome = event["outcome"]

        if category == "attack":
            if actor_match:
                stats["attacks_sent"] += 1
                if outcome == "success":
                    gain_acres = int(event.get("acres_transfer", 0))
                    if is_war_raze:
                        gain_acres = 0
                    if gain_acres > 0:
                        stats["gains"] += gain_acres
            if target_match:
                stats["attacks_received"] += 1
                if outcome == "success":
                    loss_acres = int(event["target_loss_acres"]) if int(event["target_loss_acres"]) > 0 else int(event["acres"] or 0)
                    if is_war_raze:
                        loss_acres = 0
                    stats["losses"] += loss_acres
        elif category == "aid":
            if actor_match:
                stats["aid_sent"] += 1
            if target_match:
                stats["aid_received"] += 1
        elif category in {"thievery", "magic"}:
            if actor_match:
                stats["ops_sent"] += 1
                if category == "magic":
                    stats["magic_sent"] += 1
            if target_match:
                stats["ops_received"] += 1
                if category == "magic":
                    stats["magic_received"] += 1

        entries.append(
            {
                "event_id": event["event_id"],
                "fetched_at_utc": event["fetched_at_utc"],
                "event_time_text": event["event_time_text"] or "-",
                "category": category,
                "outcome": outcome or "-",
                "attack_type": event["attack_type"] if event["category"] in {"attack", "thievery", "magic"} else "-",
                "acres": event["acres"] if event["acres"] is not None else "-",
                "role": role,
                "actor": event["actor"],
                "target": event["target"],
                "summary": event["summary"],
            }
        )

    entries.sort(
        key=lambda row: (
            row.get("fetched_at_utc") or "",
            int(row.get("event_id") or 0),
        ),
        reverse=True,
    )
    entries = entries[:limit]
    stats["net"] = stats["gains"] - stats["losses"]

    return {
        "province": normalized,
        "kingdom": target_kingdom or None,
        "stats": stats,
        "events": entries,
    }


def build_province_detail(
    province_name: str,
    province_kingdom: Optional[str],
    rows: list[Dict[str, Any]],
    home_kingdom: Optional[str],
    war_rows: list[Dict[str, Any]],
    limit: int = 240,
    ops_rows: Optional[list[Dict[str, Any]]] = None,
) -> Optional[Dict[str, Any]]:
    normalized = normalize_party(province_name)
    if not normalized:
        return None

    target_name = normalized.lower()
    target_kingdom = (province_kingdom or "").strip()

    stats: Dict[str, Any] = {
        "attacks_sent": 0,
        "attacks_received": 0,
        "aid_sent": 0,
        "aid_received": 0,
        "gains": 0,
        "losses": 0,
        "ops_sent": 0,
        "ops_received": 0,
        "magic_sent": 0,
        "magic_received": 0,
        "thievery_sent": 0,
        "thievery_received": 0,
        "hostile_ops_sent": 0,
        "hostile_ops_received": 0,
        "intel_ops_sent": 0,
        "intel_ops_received": 0,
        "support_ops_sent": 0,
        "support_ops_received": 0,
        "op_success_sent": 0,
        "op_partial_sent": 0,
        "op_failed_sent": 0,
        "op_unknown_sent": 0,
        "op_success_received": 0,
        "op_partial_received": 0,
        "op_failed_received": 0,
        "op_unknown_received": 0,
        "op_damage_done": 0.0,
        "op_damage_taken": 0.0,
        "op_gain_done": 0.0,
        "op_gain_taken": 0.0,
        "war_ops_sent": 0,
        "war_ops_received": 0,
        "war_hostile_ops_sent": 0,
        "war_hostile_ops_received": 0,
        "war_op_success_sent": 0,
        "war_op_partial_sent": 0,
        "war_op_failed_sent": 0,
        "war_op_unknown_sent": 0,
        "war_op_success_received": 0,
        "war_op_partial_received": 0,
        "war_op_failed_received": 0,
        "war_op_unknown_received": 0,
        "war_op_damage_done": 0.0,
        "war_op_damage_taken": 0.0,
        "war_op_gain_done": 0.0,
        "war_op_gain_taken": 0.0,
    }

    op_breakdown: Dict[str, Dict[str, Any]] = {}
    entries: list[Dict[str, Any]] = []
    known_kingdom_by_name: Dict[str, str] = {}
    use_structured_ops = bool(ops_rows)

    # Build a name->kingdom hint map from events that contain explicit coordinates.
    for row in rows:
        event = build_event_entry(row)
        actor_name = (event.get("actor") or "").strip().lower()
        target_name_hint = (event.get("target") or "").strip().lower()
        actor_kingdom = (event.get("actor_kingdom") or "").strip()
        target_kingdom_hint = (event.get("target_kingdom") or "").strip()
        if actor_name and actor_name != "-" and actor_kingdom:
            known_kingdom_by_name.setdefault(actor_name, actor_kingdom)
        if target_name_hint and target_name_hint != "-" and target_kingdom_hint:
            known_kingdom_by_name.setdefault(target_name_hint, target_kingdom_hint)
    if ops_rows:
        for row in ops_rows:
            actor_name_hint = (normalize_party(row.get("actor")) or "").strip().lower()
            target_name_hint = (normalize_party(row.get("target")) or "").strip().lower()
            actor_kingdom_hint = (row.get("actor_kingdom") or "").strip()
            target_kingdom_hint = (row.get("target_kingdom") or "").strip()
            if actor_name_hint and actor_kingdom_hint:
                known_kingdom_by_name.setdefault(actor_name_hint, actor_kingdom_hint)
            if target_name_hint and target_kingdom_hint:
                known_kingdom_by_name.setdefault(target_name_hint, target_kingdom_hint)

    def ensure_breakdown(op_name: str, op_kind: str, category: str) -> Dict[str, Any]:
        row = op_breakdown.setdefault(
            op_name,
            {
                "op_name": op_name,
                "op_kind": op_kind,
                "category": category,
                "sent": 0,
                "received": 0,
                "sent_success": 0,
                "sent_partial": 0,
                "sent_failed": 0,
                "sent_unknown": 0,
                "received_success": 0,
                "received_partial": 0,
                "received_failed": 0,
                "received_unknown": 0,
                "damage_done": 0.0,
                "damage_taken": 0.0,
                "gain_done": 0.0,
                "gain_taken": 0.0,
                "war_sent": 0,
                "war_received": 0,
                "war_damage_done": 0.0,
                "war_damage_taken": 0.0,
                "war_gain_done": 0.0,
                "war_gain_taken": 0.0,
            },
        )
        if row["op_kind"] != "hostile" and op_kind == "hostile":
            row["op_kind"] = "hostile"
        if row["category"] not in {"thievery", "magic"} and category in {"thievery", "magic"}:
            row["category"] = category
        return row

    def bump_outcome(prefix: str, outcome: str) -> None:
        value = (outcome or "").strip().lower()
        if value == "success":
            stats[f"{prefix}_success"] += 1
        elif value == "partial":
            stats[f"{prefix}_partial"] += 1
        else:
            stats[f"{prefix}_failed"] += 1

    for row in rows:
        event = build_event_entry(row)
        actor_hint_key = (event.get("actor") or "").strip().lower()
        target_hint_key = (event.get("target") or "").strip().lower()
        if not event.get("actor_kingdom") and actor_hint_key:
            event["actor_kingdom"] = known_kingdom_by_name.get(actor_hint_key)
        if not event.get("target_kingdom") and target_hint_key:
            event["target_kingdom"] = known_kingdom_by_name.get(target_hint_key)

        actor_match = event["actor"].lower() == target_name
        target_match = event["target"].lower() == target_name

        if target_kingdom:
            if actor_match and event["actor_kingdom"] and event["actor_kingdom"] != target_kingdom:
                actor_match = False
            if target_match and event["target_kingdom"] and event["target_kingdom"] != target_kingdom:
                target_match = False

        if not actor_match and not target_match:
            continue

        day_key = parse_event_day(event["event_time_text"])
        war_context = event_in_home_war(
            day_key,
            event.get("actor_kingdom"),
            event.get("target_kingdom"),
            home_kingdom,
            war_rows,
        )
        is_war_raze = war_context and event.get("attack_type") == "Raze"
        role = "both" if actor_match and target_match else ("actor" if actor_match else "target")
        category = event["category"]
        outcome = (event.get("outcome") or "").strip().lower()
        is_self_target = actor_match and target_match

        if not war_context and category in {"thievery", "magic"} and day_key and not is_self_target:
            if target_kingdom and home_kingdom:
                if target_kingdom == home_kingdom:
                    war_context = any(
                        row.get("opponent_kingdom")
                        and day_in_range(day_key, row.get("start_key"), row.get("end_key"))
                        for row in war_rows
                    )
                else:
                    war_context = any(
                        row.get("opponent_kingdom") == target_kingdom
                        and day_in_range(day_key, row.get("start_key"), row.get("end_key"))
                        for row in war_rows
                    )

        if category == "attack":
            if actor_match:
                stats["attacks_sent"] += 1
                if outcome == "success":
                    gain_acres = int(event.get("acres_transfer", 0))
                    if is_war_raze:
                        gain_acres = 0
                    if gain_acres > 0:
                        stats["gains"] += gain_acres
            if target_match:
                stats["attacks_received"] += 1
                if outcome == "success":
                    loss_acres = int(event["target_loss_acres"]) if int(event["target_loss_acres"]) > 0 else int(event["acres"] or 0)
                    if is_war_raze:
                        loss_acres = 0
                    stats["losses"] += loss_acres

        elif category == "aid":
            if actor_match:
                stats["aid_sent"] += 1
            if target_match and not is_self_target:
                stats["aid_received"] += 1

        elif category in {"thievery", "magic"} and not use_structured_ops:
            op_name = normalize_operation_name(event)
            op_kind = classify_operation_kind(op_name, event["actor"], event["target"])
            damage_points = float(event.get("op_damage") or 0)
            gain_points = float(event.get("op_gain") or 0)
            derived_points = operation_impact_points(op_name, outcome, op_kind)
            points = damage_points if damage_points > 0 else (gain_points if gain_points > 0 else 0.0)
            b = ensure_breakdown(op_name, op_kind, category)

            if actor_match:
                stats["ops_sent"] += 1
                stats[f"{category}_sent"] += 1
                stats[f"{op_kind}_ops_sent"] += 1
                b["sent"] += 1
                if outcome == "success":
                    stats["op_success_sent"] += 1
                    b["sent_success"] += 1
                elif outcome == "partial":
                    stats["op_partial_sent"] += 1
                    b["sent_partial"] += 1
                elif outcome == "failed":
                    stats["op_failed_sent"] += 1
                    b["sent_failed"] += 1
                else:
                    stats["op_unknown_sent"] += 1
                    b["sent_unknown"] += 1

                if op_kind == "hostile" and not is_self_target and points > 0:
                    stats["op_damage_done"] += points
                    b["damage_done"] += points
                if op_kind == "hostile" and not is_self_target and gain_points > 0:
                    stats["op_gain_done"] += gain_points
                    b["gain_done"] += gain_points

                if war_context:
                    stats["war_ops_sent"] += 1
                    if op_kind == "hostile":
                        stats["war_hostile_ops_sent"] += 1
                    if outcome == "success":
                        stats["war_op_success_sent"] += 1
                    elif outcome == "partial":
                        stats["war_op_partial_sent"] += 1
                    elif outcome == "failed":
                        stats["war_op_failed_sent"] += 1
                    else:
                        stats["war_op_unknown_sent"] += 1
                    b["war_sent"] += 1
                    if op_kind == "hostile" and not is_self_target and points > 0:
                        stats["war_op_damage_done"] += points
                        b["war_damage_done"] += points
                    if op_kind == "hostile" and not is_self_target and gain_points > 0:
                        stats["war_op_gain_done"] += gain_points
                        b["war_gain_done"] += gain_points

            if target_match and not is_self_target:
                stats["ops_received"] += 1
                stats[f"{category}_received"] += 1
                stats[f"{op_kind}_ops_received"] += 1
                b["received"] += 1
                if outcome == "success":
                    stats["op_success_received"] += 1
                    b["received_success"] += 1
                elif outcome == "partial":
                    stats["op_partial_received"] += 1
                    b["received_partial"] += 1
                elif outcome == "failed":
                    stats["op_failed_received"] += 1
                    b["received_failed"] += 1
                else:
                    stats["op_unknown_received"] += 1
                    b["received_unknown"] += 1

                if op_kind == "hostile" and points > 0:
                    stats["op_damage_taken"] += points
                    b["damage_taken"] += points
                if op_kind == "hostile" and gain_points > 0:
                    stats["op_gain_taken"] += gain_points
                    b["gain_taken"] += gain_points

                if war_context:
                    stats["war_ops_received"] += 1
                    if op_kind == "hostile":
                        stats["war_hostile_ops_received"] += 1
                    if outcome == "success":
                        stats["war_op_success_received"] += 1
                    elif outcome == "partial":
                        stats["war_op_partial_received"] += 1
                    elif outcome == "failed":
                        stats["war_op_failed_received"] += 1
                    else:
                        stats["war_op_unknown_received"] += 1
                    b["war_received"] += 1
                    if op_kind == "hostile" and points > 0:
                        stats["war_op_damage_taken"] += points
                        b["war_damage_taken"] += points
                    if op_kind == "hostile" and gain_points > 0:
                        stats["war_op_gain_taken"] += gain_points
                        b["war_gain_taken"] += gain_points

            event["operation_name"] = op_name
            event["operation_kind"] = op_kind
            event["operation_points"] = points
            event["operation_estimated_points"] = derived_points

        entries.append(
            {
                "event_id": event["event_id"],
                "fetched_at_utc": event["fetched_at_utc"],
                "event_time_text": event["event_time_text"] or "-",
                "category": category,
                "operation_name": normalize_operation_name(event) if category in {"thievery", "magic"} else "-",
                "operation_kind": classify_operation_kind(
                    normalize_operation_name(event),
                    event["actor"],
                    event["target"],
                )
                if category in {"thievery", "magic"}
                else "-",
                "op_damage": int(event.get("op_damage") or 0),
                "op_gain": int(event.get("op_gain") or 0),
                "op_duration_ticks": int(event.get("op_duration_ticks") or 0),
                "outcome": outcome or "-",
                "attack_type": event["attack_type"] if event["category"] == "attack" else "-",
                "acres": event["acres"] if event["acres"] is not None else "-",
                "role": role,
                "war_context": war_context,
                "actor": event["actor"],
                "target": event["target"],
                "summary": event["summary"],
            }
        )

    if use_structured_ops and ops_rows:
        for row in ops_rows:
            event_actor = normalize_party(row.get("actor")) or "-"
            event_target = normalize_party(row.get("target")) or "-"
            actor_kingdom = (row.get("actor_kingdom") or "").strip() or known_kingdom_by_name.get(event_actor.lower())
            target_kingdom_hint = (row.get("target_kingdom") or "").strip() or known_kingdom_by_name.get(event_target.lower())
            actor_match = event_actor.lower() == target_name
            target_match = event_target.lower() == target_name

            if target_kingdom:
                if actor_match and actor_kingdom and actor_kingdom != target_kingdom:
                    actor_match = False
                if target_match and target_kingdom_hint and target_kingdom_hint != target_kingdom:
                    target_match = False

            if not actor_match and not target_match:
                continue

            category = (row.get("category") or "").strip().lower()
            if category not in {"thievery", "magic"}:
                op_type = operation_key(str(row.get("op_type") or ""))
                category = "thievery" if "spy" in op_type or "thie" in op_type else "magic"

            day_text = (row.get("event_time_text") or "").strip()
            day_key = parse_event_day(day_text)
            outcome = (row.get("result_label") or "unknown").strip().lower()
            op_name = (row.get("op_name") or row.get("op_type") or "Unknown Op")
            op_kind = classify_operation_kind(op_name, event_actor, event_target)
            damage_points = safe_float_num(row.get("damage"))
            gain_points = safe_float_num(row.get("gain"))
            duration_ticks = safe_int_num(row.get("duration_ticks"))
            derived_points = operation_impact_points(op_name, outcome, op_kind)
            points = damage_points if damage_points > 0 else (gain_points if gain_points > 0 else 0.0)
            is_self_target = actor_match and target_match

            war_context = event_in_home_war(
                day_key,
                actor_kingdom,
                target_kingdom_hint,
                home_kingdom,
                war_rows,
            )
            if not war_context and day_key and not is_self_target:
                if target_kingdom and home_kingdom:
                    if target_kingdom == home_kingdom:
                        war_context = any(
                            row.get("opponent_kingdom")
                            and day_in_range(day_key, row.get("start_key"), row.get("end_key"))
                            for row in war_rows
                        )
                    else:
                        war_context = any(
                            row.get("opponent_kingdom") == target_kingdom
                            and day_in_range(day_key, row.get("start_key"), row.get("end_key"))
                            for row in war_rows
                        )

            b = ensure_breakdown(op_name, op_kind, category)
            if actor_match:
                stats["ops_sent"] += 1
                stats[f"{category}_sent"] += 1
                stats[f"{op_kind}_ops_sent"] += 1
                b["sent"] += 1
                if outcome == "success":
                    stats["op_success_sent"] += 1
                    b["sent_success"] += 1
                elif outcome == "partial":
                    stats["op_partial_sent"] += 1
                    b["sent_partial"] += 1
                elif outcome == "failed":
                    stats["op_failed_sent"] += 1
                    b["sent_failed"] += 1
                else:
                    stats["op_unknown_sent"] += 1
                    b["sent_unknown"] += 1

                if op_kind == "hostile" and not is_self_target and points > 0:
                    stats["op_damage_done"] += points
                    b["damage_done"] += points
                if op_kind == "hostile" and not is_self_target and gain_points > 0:
                    stats["op_gain_done"] += gain_points
                    b["gain_done"] += gain_points

                if war_context:
                    stats["war_ops_sent"] += 1
                    if op_kind == "hostile":
                        stats["war_hostile_ops_sent"] += 1
                    if outcome == "success":
                        stats["war_op_success_sent"] += 1
                    elif outcome == "partial":
                        stats["war_op_partial_sent"] += 1
                    elif outcome == "failed":
                        stats["war_op_failed_sent"] += 1
                    else:
                        stats["war_op_unknown_sent"] += 1
                    b["war_sent"] += 1
                    if op_kind == "hostile" and not is_self_target and points > 0:
                        stats["war_op_damage_done"] += points
                        b["war_damage_done"] += points
                    if op_kind == "hostile" and not is_self_target and gain_points > 0:
                        stats["war_op_gain_done"] += gain_points
                        b["war_gain_done"] += gain_points

            if target_match and not is_self_target:
                stats["ops_received"] += 1
                stats[f"{category}_received"] += 1
                stats[f"{op_kind}_ops_received"] += 1
                b["received"] += 1
                if outcome == "success":
                    stats["op_success_received"] += 1
                    b["received_success"] += 1
                elif outcome == "partial":
                    stats["op_partial_received"] += 1
                    b["received_partial"] += 1
                elif outcome == "failed":
                    stats["op_failed_received"] += 1
                    b["received_failed"] += 1
                else:
                    stats["op_unknown_received"] += 1
                    b["received_unknown"] += 1

                if op_kind == "hostile" and points > 0:
                    stats["op_damage_taken"] += points
                    b["damage_taken"] += points
                if op_kind == "hostile" and gain_points > 0:
                    stats["op_gain_taken"] += gain_points
                    b["gain_taken"] += gain_points

                if war_context:
                    stats["war_ops_received"] += 1
                    if op_kind == "hostile":
                        stats["war_hostile_ops_received"] += 1
                    if outcome == "success":
                        stats["war_op_success_received"] += 1
                    elif outcome == "partial":
                        stats["war_op_partial_received"] += 1
                    elif outcome == "failed":
                        stats["war_op_failed_received"] += 1
                    else:
                        stats["war_op_unknown_received"] += 1
                    b["war_received"] += 1
                    if op_kind == "hostile" and points > 0:
                        stats["war_op_damage_taken"] += points
                        b["war_damage_taken"] += points
                    if op_kind == "hostile" and gain_points > 0:
                        stats["war_op_gain_taken"] += gain_points
                        b["war_gain_taken"] += gain_points

            role = "both" if actor_match and target_match else ("actor" if actor_match else "target")
            entries.append(
                {
                    "event_id": int(row.get("id") or row.get("intel_op_id") or 0),
                    "fetched_at_utc": row.get("last_updated_utc") or row.get("fetched_at_utc") or "-",
                    "event_time_text": day_text or "-",
                    "category": category,
                    "operation_name": op_name,
                    "operation_kind": op_kind,
                    "op_damage": safe_int_num(damage_points),
                    "op_gain": safe_int_num(gain_points),
                    "op_duration_ticks": duration_ticks,
                    "outcome": outcome or "-",
                    "attack_type": "-",
                    "acres": "-",
                    "role": role,
                    "war_context": war_context,
                    "actor": event_actor,
                    "target": event_target,
                    "summary": row.get("summary") or "",
                    "operation_points": points,
                    "operation_estimated_points": derived_points,
                }
            )

    for key in (
        "op_damage_done",
        "op_damage_taken",
        "war_op_damage_done",
        "war_op_damage_taken",
        "op_gain_done",
        "op_gain_taken",
        "war_op_gain_done",
        "war_op_gain_taken",
    ):
        stats[key] = round(float(stats[key]), 2)
    stats["net"] = stats["gains"] - stats["losses"]
    stats["op_net_damage"] = round(stats["op_damage_done"] - stats["op_damage_taken"], 2)
    stats["war_op_net_damage"] = round(stats["war_op_damage_done"] - stats["war_op_damage_taken"], 2)

    entries.sort(key=lambda row: row["event_id"], reverse=True)
    entries = entries[:limit]

    breakdown_rows = list(op_breakdown.values())
    for row in breakdown_rows:
        row["damage_done"] = round(float(row["damage_done"]), 2)
        row["damage_taken"] = round(float(row["damage_taken"]), 2)
        row["war_damage_done"] = round(float(row["war_damage_done"]), 2)
        row["war_damage_taken"] = round(float(row["war_damage_taken"]), 2)
        row["gain_done"] = round(float(row["gain_done"]), 2)
        row["gain_taken"] = round(float(row["gain_taken"]), 2)
        row["war_gain_done"] = round(float(row["war_gain_done"]), 2)
        row["war_gain_taken"] = round(float(row["war_gain_taken"]), 2)
        row["total_events"] = int(row["sent"]) + int(row["received"])
        row["net_damage"] = round(row["damage_done"] - row["damage_taken"], 2)
        row["war_net_damage"] = round(row["war_damage_done"] - row["war_damage_taken"], 2)

    breakdown_rows.sort(
        key=lambda row: (
            row["war_damage_done"] + row["damage_done"] + row["damage_taken"],
            row["total_events"],
            row["op_name"],
        ),
        reverse=True,
    )

    return {
        "province": normalized,
        "kingdom": target_kingdom or None,
        "stats": stats,
        "op_breakdown_rows": breakdown_rows,
        "events": entries,
    }


def build_ops_summary(
    rows: list[Dict[str, Any]],
    home_kingdom: Optional[str],
    war_rows: list[Dict[str, Any]],
    ops_rows: Optional[list[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    row_dicts = [dict(row) if not isinstance(row, dict) else row for row in rows]
    all_events = [build_event_entry(row) for row in row_dicts]
    op_events: list[Dict[str, Any]] = []

    if ops_rows:
        for row in ops_rows:
            actor_name = normalize_party(row.get("actor")) or "-"
            target_name = normalize_party(row.get("target")) or "-"
            category = (row.get("category") or "").strip().lower()
            if category not in {"thievery", "magic"}:
                op_type = operation_key(str(row.get("op_type") or ""))
                category = "thievery" if "spy" in op_type or "thie" in op_type else "magic"
            op_name = (row.get("op_name") or row.get("op_type") or "Unknown Op")
            op_events.append(
                {
                    "event_id": int(row.get("id") or 0),
                    "fetched_at_utc": row.get("last_updated_utc") or row.get("fetched_at_utc") or "",
                    "event_time_text": row.get("event_time_text") or "",
                    "category": category,
                    "actor": actor_name,
                    "target": target_name,
                    "actor_kingdom": (row.get("actor_kingdom") or None),
                    "target_kingdom": (row.get("target_kingdom") or None),
                    "acres": None,
                    "acres_transfer": 0,
                    "target_loss_acres": 0,
                    "attack_type": op_name,
                    "op_gain": float(row.get("gain") or 0),
                    "op_damage": float(row.get("damage") or 0),
                    "op_duration_ticks": safe_int_num(row.get("duration_ticks")),
                    "outcome": (row.get("result_label") or "unknown").strip().lower(),
                    "summary": row.get("summary") or "",
                }
            )

    if not op_events:
        op_events = [event for event in all_events if (event.get("category") or "").lower() in {"thievery", "magic"}]

    known_kingdom_by_name: Dict[str, str] = {}
    for event in all_events:
        actor_name = (event.get("actor") or "").strip().lower()
        target_name = (event.get("target") or "").strip().lower()
        actor_kingdom = (event.get("actor_kingdom") or "").strip()
        target_kingdom = (event.get("target_kingdom") or "").strip()
        if actor_name and actor_name != "-" and actor_kingdom:
            known_kingdom_by_name.setdefault(actor_name, actor_kingdom)
        if target_name and target_name != "-" and target_kingdom:
            known_kingdom_by_name.setdefault(target_name, target_kingdom)
    for event in op_events:
        actor_name = (event.get("actor") or "").strip().lower()
        target_name = (event.get("target") or "").strip().lower()
        actor_kingdom = (event.get("actor_kingdom") or "").strip()
        target_kingdom = (event.get("target_kingdom") or "").strip()
        if actor_name and actor_name != "-" and actor_kingdom:
            known_kingdom_by_name.setdefault(actor_name, actor_kingdom)
        if target_name and target_name != "-" and target_kingdom:
            known_kingdom_by_name.setdefault(target_name, target_kingdom)

    def in_any_war(day_key) -> bool:
        if not day_key:
            return False
        return any(day_in_range(day_key, war_row.get("start_key"), war_row.get("end_key")) for war_row in war_rows)

    type_rows: Dict[str, Dict[str, Any]] = {}
    caster_rows: Dict[str, Dict[str, Any]] = {}
    target_rows: Dict[str, Dict[str, Any]] = {}
    home_victim_rows: Dict[str, Dict[str, Any]] = {}
    enemy_caster_rows: Dict[str, Dict[str, Any]] = {}
    timeline_rows_by_day: Dict[str, Dict[str, Any]] = {}
    recent_damage_events: list[Dict[str, Any]] = []

    kpis: Dict[str, Any] = {
        "total_ops": 0,
        "war_total_ops": 0,
        "hostile_ops": 0,
        "war_hostile_ops": 0,
        "damage_done": 0.0,
        "damage_taken": 0.0,
        "war_damage_done": 0.0,
        "war_damage_taken": 0.0,
        "gain_done": 0.0,
        "gain_taken": 0.0,
        "war_gain_done": 0.0,
        "war_gain_taken": 0.0,
    }

    def ensure_type_row(op_name: str, op_kind: str, category: str) -> Dict[str, Any]:
        row = type_rows.setdefault(
            op_name,
            {
                "op_name": op_name,
                "op_kind": op_kind,
                "category": category,
                "casts_total": 0,
                "casts_home": 0,
                "casts_vs_home": 0,
                "casts_other": 0,
                "war_casts_home": 0,
                "war_casts_vs_home": 0,
                "home_success": 0,
                "home_failed": 0,
                "home_partial": 0,
                "home_unknown": 0,
                "vs_home_success": 0,
                "vs_home_failed": 0,
                "vs_home_partial": 0,
                "vs_home_unknown": 0,
                "damage_done": 0.0,
                "damage_taken": 0.0,
                "war_damage_done": 0.0,
                "war_damage_taken": 0.0,
                "gain_done": 0.0,
                "gain_taken": 0.0,
                "war_gain_done": 0.0,
                "war_gain_taken": 0.0,
                "duration_ticks_home": 0,
            },
        )
        if row["op_kind"] != "hostile" and op_kind == "hostile":
            row["op_kind"] = "hostile"
        if row["category"] not in {"thievery", "magic"} and category in {"thievery", "magic"}:
            row["category"] = category
        return row

    def ensure_party_row(store: Dict[str, Dict[str, Any]], name: str, kingdom: Optional[str]) -> Dict[str, Any]:
        key = f"{name}|{kingdom or ''}"
        return store.setdefault(
            key,
            {
                "province": name,
                "kingdom": kingdom or "-",
                "casts": 0,
                "hostile_casts": 0,
                "damage_done": 0.0,
                "damage_taken": 0.0,
                "gain_done": 0.0,
                "gain_taken": 0.0,
                "war_damage_done": 0.0,
                "war_damage_taken": 0.0,
            },
        )

    for event in op_events:
        actor_hint = (event.get("actor") or "").strip().lower()
        target_hint = (event.get("target") or "").strip().lower()
        if not event.get("actor_kingdom") and actor_hint:
            event["actor_kingdom"] = known_kingdom_by_name.get(actor_hint)
        if not event.get("target_kingdom") and target_hint:
            event["target_kingdom"] = known_kingdom_by_name.get(target_hint)

        op_name = normalize_operation_name(event)
        op_kind = classify_operation_kind(op_name, event["actor"], event["target"])
        category = (event.get("category") or "other").lower()
        outcome = (event.get("outcome") or "").strip().lower()
        actor_name = event.get("actor") or "-"
        target_name = event.get("target") or "-"
        actor_kingdom = event.get("actor_kingdom")
        target_kingdom = event.get("target_kingdom")
        day_key = parse_event_day(event.get("event_time_text"))
        day_text = event.get("event_time_text") or "-"
        damage_value = safe_float_num(event.get("op_damage"))
        gain_value = safe_float_num(event.get("op_gain"))
        duration_ticks = safe_int_num(event.get("op_duration_ticks"))
        is_self_target = actor_name != "-" and target_name != "-" and operation_key(actor_name) == operation_key(target_name)

        war_context = event_in_home_war(day_key, actor_kingdom, target_kingdom, home_kingdom, war_rows)
        if not war_context and day_key and not is_self_target:
            if home_kingdom and actor_kingdom == home_kingdom and target_kingdom and target_kingdom != home_kingdom:
                war_context = any(
                    war_row.get("opponent_kingdom") == target_kingdom
                    and day_in_range(day_key, war_row.get("start_key"), war_row.get("end_key"))
                    for war_row in war_rows
                )
            elif home_kingdom and target_kingdom == home_kingdom and actor_kingdom and actor_kingdom != home_kingdom:
                war_context = any(
                    war_row.get("opponent_kingdom") == actor_kingdom
                    and day_in_range(day_key, war_row.get("start_key"), war_row.get("end_key"))
                    for war_row in war_rows
                )
            elif home_kingdom and (actor_kingdom == home_kingdom or target_kingdom == home_kingdom):
                war_context = in_any_war(day_key)

        actor_home = bool(home_kingdom and actor_kingdom == home_kingdom)
        target_home = bool(home_kingdom and target_kingdom == home_kingdom)
        from_home = actor_home and not target_home
        against_home = target_home and not actor_home
        if not home_kingdom:
            from_home = True
            against_home = False

        kpis["total_ops"] += 1
        if war_context:
            kpis["war_total_ops"] += 1
        if op_kind == "hostile":
            kpis["hostile_ops"] += 1
            if war_context:
                kpis["war_hostile_ops"] += 1

        op_row = ensure_type_row(op_name, op_kind, category)
        op_row["casts_total"] += 1

        timeline = timeline_rows_by_day.setdefault(
            day_text,
            {
                "day": day_text,
                "day_key": day_key,
                "ops_total": 0,
                "hostile_ops": 0,
                "war_ops": 0,
                "damage_done": 0.0,
                "damage_taken": 0.0,
                "war_damage_done": 0.0,
                "war_damage_taken": 0.0,
                "gain_done": 0.0,
                "gain_taken": 0.0,
            },
        )
        timeline["ops_total"] += 1
        if op_kind == "hostile":
            timeline["hostile_ops"] += 1
        if war_context:
            timeline["war_ops"] += 1

        if from_home:
            op_row["casts_home"] += 1
            if war_context:
                op_row["war_casts_home"] += 1
            if outcome == "success":
                op_row["home_success"] += 1
            elif outcome == "partial":
                op_row["home_partial"] += 1
            elif outcome == "failed":
                op_row["home_failed"] += 1
            else:
                op_row["home_unknown"] += 1
            op_row["duration_ticks_home"] += duration_ticks

            caster = ensure_party_row(caster_rows, actor_name, actor_kingdom)
            caster["casts"] += 1
            if op_kind == "hostile":
                caster["hostile_casts"] += 1

            if op_kind == "hostile" and not is_self_target:
                if damage_value > 0:
                    kpis["damage_done"] += damage_value
                    op_row["damage_done"] += damage_value
                    timeline["damage_done"] += damage_value
                    caster["damage_done"] += damage_value
                    if war_context:
                        kpis["war_damage_done"] += damage_value
                        op_row["war_damage_done"] += damage_value
                        timeline["war_damage_done"] += damage_value
                        caster["war_damage_done"] += damage_value
                if gain_value > 0:
                    kpis["gain_done"] += gain_value
                    op_row["gain_done"] += gain_value
                    timeline["gain_done"] += gain_value
                    caster["gain_done"] += gain_value
                    if war_context:
                        kpis["war_gain_done"] += gain_value
                        op_row["war_gain_done"] += gain_value

                if damage_value > 0 or gain_value > 0 or duration_ticks > 0:
                    recent_damage_events.append(
                        {
                            "event_id": event["event_id"],
                            "event_time_text": day_text,
                            "fetched_at_utc": event["fetched_at_utc"],
                            "op_name": op_name,
                            "op_kind": op_kind,
                            "category": category,
                            "actor": actor_name,
                            "actor_kingdom": actor_kingdom or "-",
                            "target": target_name,
                            "target_kingdom": target_kingdom or "-",
                            "outcome": outcome or "-",
                            "damage": round(damage_value, 2),
                            "gain": round(gain_value, 2),
                            "duration_ticks": duration_ticks,
                            "war_context": war_context,
                            "summary": event.get("summary") or "",
                        }
                    )

                if target_name and target_name != "-":
                    target_party = ensure_party_row(target_rows, target_name, target_kingdom)
                    if damage_value > 0:
                        target_party["damage_taken"] += damage_value
                    if gain_value > 0:
                        target_party["gain_taken"] += gain_value
                    if war_context and damage_value > 0:
                        target_party["war_damage_taken"] += damage_value

        elif against_home:
            op_row["casts_vs_home"] += 1
            if war_context:
                op_row["war_casts_vs_home"] += 1
            if outcome == "success":
                op_row["vs_home_success"] += 1
            elif outcome == "partial":
                op_row["vs_home_partial"] += 1
            elif outcome == "failed":
                op_row["vs_home_failed"] += 1
            else:
                op_row["vs_home_unknown"] += 1

            enemy_caster = ensure_party_row(enemy_caster_rows, actor_name, actor_kingdom)
            enemy_caster["casts"] += 1
            if op_kind == "hostile":
                enemy_caster["hostile_casts"] += 1

            home_target = ensure_party_row(home_victim_rows, target_name, target_kingdom)
            home_target["casts"] += 1

            if op_kind == "hostile" and not is_self_target:
                if damage_value > 0:
                    kpis["damage_taken"] += damage_value
                    op_row["damage_taken"] += damage_value
                    timeline["damage_taken"] += damage_value
                    home_target["damage_taken"] += damage_value
                    enemy_caster["damage_done"] += damage_value
                    if war_context:
                        kpis["war_damage_taken"] += damage_value
                        op_row["war_damage_taken"] += damage_value
                        timeline["war_damage_taken"] += damage_value
                        home_target["war_damage_taken"] += damage_value
                        enemy_caster["war_damage_done"] += damage_value
                if gain_value > 0:
                    kpis["gain_taken"] += gain_value
                    op_row["gain_taken"] += gain_value
                    timeline["gain_taken"] += gain_value
                    home_target["gain_taken"] += gain_value
                    enemy_caster["gain_done"] += gain_value
                    if war_context:
                        kpis["war_gain_taken"] += gain_value
                        op_row["war_gain_taken"] += gain_value

        else:
            op_row["casts_other"] += 1

    for key in (
        "damage_done",
        "damage_taken",
        "war_damage_done",
        "war_damage_taken",
        "gain_done",
        "gain_taken",
        "war_gain_done",
        "war_gain_taken",
    ):
        kpis[key] = round(float(kpis[key]), 2)
    kpis["net_damage"] = round(kpis["damage_done"] - kpis["damage_taken"], 2)
    kpis["war_net_damage"] = round(kpis["war_damage_done"] - kpis["war_damage_taken"], 2)

    type_rows_list = list(type_rows.values())
    for row in type_rows_list:
        for key in (
            "damage_done",
            "damage_taken",
            "war_damage_done",
            "war_damage_taken",
            "gain_done",
            "gain_taken",
            "war_gain_done",
            "war_gain_taken",
        ):
            row[key] = round(float(row[key]), 2)
        row["net_damage"] = round(row["damage_done"] - row["damage_taken"], 2)
        row["war_net_damage"] = round(row["war_damage_done"] - row["war_damage_taken"], 2)
    type_rows_list.sort(
        key=lambda row: (
            row["war_damage_done"] + row["damage_done"] + row["damage_taken"],
            row["casts_home"] + row["casts_vs_home"],
            row["op_name"],
        ),
        reverse=True,
    )

    def finalize_party_rows(store: Dict[str, Dict[str, Any]], key_name: str) -> list[Dict[str, Any]]:
        out = list(store.values())
        for row in out:
            for key in ("damage_done", "damage_taken", "gain_done", "gain_taken", "war_damage_done", "war_damage_taken"):
                row[key] = round(float(row[key]), 2)
            row["net_damage"] = round(row["damage_done"] - row["damage_taken"], 2)
            row["label"] = f"{row[key_name]} ({row['kingdom']})"
        return out

    caster_rows_list = finalize_party_rows(caster_rows, "province")
    caster_rows_list.sort(key=lambda row: (row["damage_done"], row["gain_done"], row["hostile_casts"]), reverse=True)

    target_rows_list = finalize_party_rows(target_rows, "province")
    target_rows_list.sort(key=lambda row: (row["damage_taken"], row["gain_taken"], row["casts"]), reverse=True)

    home_victim_rows_list = finalize_party_rows(home_victim_rows, "province")
    home_victim_rows_list.sort(key=lambda row: (row["damage_taken"], row["gain_taken"], row["casts"]), reverse=True)

    enemy_caster_rows_list = finalize_party_rows(enemy_caster_rows, "province")
    enemy_caster_rows_list.sort(key=lambda row: (row["damage_done"], row["hostile_casts"]), reverse=True)

    timeline_rows = list(timeline_rows_by_day.values())
    for row in timeline_rows:
        for key in ("damage_done", "damage_taken", "war_damage_done", "war_damage_taken", "gain_done", "gain_taken"):
            row[key] = round(float(row[key]), 2)
        row["net_damage"] = round(row["damage_done"] - row["damage_taken"], 2)
    timeline_rows.sort(key=lambda row: row["day_key"] or (0, 0, 0))

    recent_damage_events.sort(key=lambda row: row["event_id"], reverse=True)

    return {
        "home_kingdom": home_kingdom,
        "kpis": kpis,
        "type_rows": type_rows_list,
        "caster_rows": caster_rows_list[:40],
        "target_rows": target_rows_list[:40],
        "home_victim_rows": home_victim_rows_list[:40],
        "enemy_caster_rows": enemy_caster_rows_list[:40],
        "timeline_rows": timeline_rows,
        "recent_damage_events": recent_damage_events[:240],
    }


def build_replay_timeline(
    rows: list[Dict[str, Any]],
    ops_rows: list[Dict[str, Any]],
    home_kingdom: Optional[str],
    war_rows: list[Dict[str, Any]],
) -> list[Dict[str, Any]]:
    day_map: Dict[tuple[int, int, int], Dict[str, Any]] = {}
    all_events = [build_event_entry(row) for row in rows]

    def ensure_day(day_key: tuple[int, int, int]) -> Dict[str, Any]:
        return day_map.setdefault(
            day_key,
            {
                "day": format_event_day(day_key),
                "day_key": day_key,
                "attack_events": 0,
                "home_hits": 0,
                "enemy_hits": 0,
                "home_land_gained": 0,
                "home_land_lost": 0,
                "home_land_net": 0,
                "op_damage_done": 0.0,
                "op_damage_taken": 0.0,
                "op_net_damage": 0.0,
                "cumulative_land_net": 0,
                "cumulative_op_net_damage": 0.0,
            },
        )

    for event in all_events:
        if (event.get("category") or "").lower() != "attack":
            continue
        day_key = parse_event_day(event.get("event_time_text"))
        if not day_key:
            continue

        actor_kingdom = event.get("actor_kingdom")
        target_kingdom = event.get("target_kingdom")
        war_context = event_in_home_war(day_key, actor_kingdom, target_kingdom, home_kingdom, war_rows)

        day_row = ensure_day(day_key)
        day_row["attack_events"] += 1

        if not home_kingdom:
            continue

        outcome = (event.get("outcome") or "").strip().lower()
        if outcome != "success":
            continue

        attack_type = event.get("attack_type") or "Other"
        impact = effective_land_impact(
            int(event.get("acres_transfer") or 0),
            int(event.get("target_loss_acres") or 0),
            attack_type,
            is_war_context=war_context,
        )
        if impact <= 0:
            continue

        if actor_kingdom == home_kingdom and target_kingdom != home_kingdom:
            day_row["home_hits"] += 1
            day_row["home_land_gained"] += impact
        elif target_kingdom == home_kingdom and actor_kingdom != home_kingdom:
            day_row["enemy_hits"] += 1
            day_row["home_land_lost"] += impact

    for row in ops_rows:
        day_key = parse_event_day(row.get("event_time_text"))
        if not day_key:
            continue
        day_row = ensure_day(day_key)
        actor = normalize_party(row.get("actor")) or "-"
        target = normalize_party(row.get("target")) or "-"
        actor_kingdom = (row.get("actor_kingdom") or "").strip() or None
        target_kingdom = (row.get("target_kingdom") or "").strip() or None
        op_name = (row.get("op_name") or row.get("op_type") or "Unknown Op")
        op_kind = classify_operation_kind(op_name, actor, target)
        if op_kind != "hostile":
            continue

        damage = float(row.get("damage") or 0)
        gain = float(row.get("gain") or 0)
        points = damage if damage > 0 else (gain if gain > 0 else 0.0)
        if points <= 0:
            continue

        if home_kingdom and actor_kingdom == home_kingdom and target_kingdom != home_kingdom:
            day_row["op_damage_done"] += points
        elif home_kingdom and target_kingdom == home_kingdom and actor_kingdom != home_kingdom:
            day_row["op_damage_taken"] += points

    cumulative_land = 0
    cumulative_op = 0.0
    timeline = []
    for day_key in sorted(day_map):
        row = day_map[day_key]
        row["home_land_net"] = int(row["home_land_gained"]) - int(row["home_land_lost"])
        row["op_net_damage"] = round(float(row["op_damage_done"]) - float(row["op_damage_taken"]), 2)
        row["op_damage_done"] = round(float(row["op_damage_done"]), 2)
        row["op_damage_taken"] = round(float(row["op_damage_taken"]), 2)
        cumulative_land += row["home_land_net"]
        cumulative_op += row["op_net_damage"]
        row["cumulative_land_net"] = cumulative_land
        row["cumulative_op_net_damage"] = round(cumulative_op, 2)
        timeline.append(row)

    return timeline


def build_war_command(
    rows: list[Dict[str, Any]],
    ops_rows: list[Dict[str, Any]],
    home_kingdom: Optional[str],
    war_rows: list[Dict[str, Any]],
) -> Dict[str, Any]:
    attack_events = [build_event_entry(row) for row in rows if (row.get("category") or "").lower() == "attack"]
    chain_map: Dict[str, Dict[str, Any]] = {}
    home_attackers: Dict[str, Dict[str, Any]] = {}
    enemy_attackers: Dict[str, Dict[str, Any]] = {}
    op_uptime: Dict[str, Dict[str, Any]] = {}

    def ensure_attacker(store: Dict[str, Dict[str, Any]], name: str, kingdom: Optional[str]) -> Dict[str, Any]:
        key = f"{name}|{kingdom or ''}"
        return store.setdefault(
            key,
            {
                "province": name,
                "kingdom": kingdom or "-",
                "hits": 0,
                "land": 0,
                "avg_land": 0.0,
            },
        )

    for event in attack_events:
        day_key = parse_event_day(event.get("event_time_text"))
        if not day_key:
            continue
        actor_kingdom = event.get("actor_kingdom")
        target_kingdom = event.get("target_kingdom")
        war_context = event_in_home_war(day_key, actor_kingdom, target_kingdom, home_kingdom, war_rows)
        if not war_context:
            continue

        outcome = (event.get("outcome") or "").strip().lower()
        if outcome != "success":
            continue

        attack_type = event.get("attack_type") or "Other"
        impact = effective_land_impact(
            int(event.get("acres_transfer") or 0),
            int(event.get("target_loss_acres") or 0),
            attack_type,
            is_war_context=True,
        )
        if impact <= 0:
            continue

        actor_name = event.get("actor") or "-"
        target_name = event.get("target") or "-"

        side = ""
        if home_kingdom and actor_kingdom == home_kingdom and target_kingdom != home_kingdom:
            side = "outgoing"
            attacker_row = ensure_attacker(home_attackers, actor_name, actor_kingdom)
            attacker_row["hits"] += 1
            attacker_row["land"] += impact
        elif home_kingdom and target_kingdom == home_kingdom and actor_kingdom != home_kingdom:
            side = "incoming"
            attacker_row = ensure_attacker(enemy_attackers, actor_name, actor_kingdom)
            attacker_row["hits"] += 1
            attacker_row["land"] += impact
        if not side:
            continue

        chain_key = f"{event.get('event_time_text')}|{target_name}|{target_kingdom or '-'}|{side}"
        chain_row = chain_map.setdefault(
            chain_key,
            {
                "day": event.get("event_time_text") or "-",
                "side": side,
                "target": target_name,
                "target_kingdom": target_kingdom or "-",
                "hits": 0,
                "land": 0,
                "attackers": set(),
            },
        )
        chain_row["hits"] += 1
        chain_row["land"] += impact
        chain_row["attackers"].add(actor_name)

    for row in ops_rows:
        day_key = parse_event_day(row.get("event_time_text"))
        if not day_key:
            continue
        actor_name = normalize_party(row.get("actor")) or "-"
        target_name = normalize_party(row.get("target")) or "-"
        actor_kingdom = (row.get("actor_kingdom") or "").strip() or None
        target_kingdom = (row.get("target_kingdom") or "").strip() or None
        war_context = event_in_home_war(day_key, actor_kingdom, target_kingdom, home_kingdom, war_rows)
        if not war_context:
            continue

        duration = safe_int_num(row.get("duration_ticks"))
        if duration <= 0:
            continue

        op_name = (row.get("op_name") or row.get("op_type") or "Unknown Op")
        op_kind = classify_operation_kind(op_name, actor_name, target_name)
        op_key = f"{operation_key(op_name)}|{op_kind}"
        uptime_row = op_uptime.setdefault(
            op_key,
            {
                "op_name": op_name,
                "op_kind": op_kind,
                "casts_home": 0,
                "casts_vs_home": 0,
                "duration_home": 0,
                "duration_vs_home": 0,
            },
        )

        if home_kingdom and actor_kingdom == home_kingdom and target_kingdom != home_kingdom:
            uptime_row["casts_home"] += 1
            uptime_row["duration_home"] += duration
        elif home_kingdom and target_kingdom == home_kingdom and actor_kingdom != home_kingdom:
            uptime_row["casts_vs_home"] += 1
            uptime_row["duration_vs_home"] += duration

    chain_rows = []
    for row in chain_map.values():
        attackers = sorted(row["attackers"])
        chain_rows.append(
            {
                "day": row["day"],
                "side": row["side"],
                "target": row["target"],
                "target_kingdom": row["target_kingdom"],
                "hits": row["hits"],
                "land": row["land"],
                "unique_attackers": len(attackers),
                "attackers": ", ".join(attackers[:6]),
            }
        )
    chain_rows.sort(key=lambda row: (row["hits"], row["land"], row["unique_attackers"]), reverse=True)

    def finalize_attackers(store: Dict[str, Dict[str, Any]]) -> list[Dict[str, Any]]:
        out = list(store.values())
        for row in out:
            row["avg_land"] = round((row["land"] / row["hits"]) if row["hits"] else 0.0, 2)
        out.sort(key=lambda row: (row["land"], row["hits"]), reverse=True)
        return out

    uptime_rows = list(op_uptime.values())
    for row in uptime_rows:
        row["avg_duration_home"] = round((row["duration_home"] / row["casts_home"]) if row["casts_home"] else 0.0, 2)
        row["avg_duration_vs_home"] = round(
            (row["duration_vs_home"] / row["casts_vs_home"]) if row["casts_vs_home"] else 0.0,
            2,
        )
    uptime_rows.sort(
        key=lambda row: (
            row["duration_home"] + row["duration_vs_home"],
            row["casts_home"] + row["casts_vs_home"],
            row["op_name"],
        ),
        reverse=True,
    )

    replay_rows = build_replay_timeline(rows, ops_rows, home_kingdom, war_rows)
    chain_alerts = [row for row in chain_rows if row["hits"] >= 3][:24]

    return {
        "chain_rows": chain_rows[:80],
        "chain_alerts": chain_alerts,
        "home_attackers": finalize_attackers(home_attackers)[:30],
        "enemy_attackers": finalize_attackers(enemy_attackers)[:30],
        "uptime_rows": uptime_rows[:80],
        "replay_rows": replay_rows,
    }


def build_fact_detail(
    fact: str,
    key: Optional[str],
    analytics: Dict[str, Any],
    rows: list[Dict[str, Any]],
) -> Dict[str, Any]:
    fact_key = (fact or "").strip().lower()
    detail = {
        "title": "Fact Detail",
        "summary": "No detail available for this selection.",
        "rows": [],
        "events": [],
    }

    if fact_key == "total_events":
        detail["title"] = "Total Events"
        detail["summary"] = f"Parsed events in scope: {analytics['kpis']['total_events']}."
        detail["rows"] = [{"label": row["category"], "value": row["cnt"]} for row in analytics["category_totals"]]
        return detail

    if fact_key == "attack_success":
        detail["title"] = "Attack Success"
        detail["summary"] = (
            f"Success rate {analytics['kpis']['attack_success_rate']}% "
            f"({analytics['kpis']['successful_hits']} successful / {analytics['kpis']['failed_hits']} failed)."
        )
        detail["rows"] = [
            {"label": "Successful Hits", "value": analytics["kpis"]["successful_hits"]},
            {"label": "Failed Hits", "value": analytics["kpis"]["failed_hits"]},
            {"label": "Total Attack Events", "value": analytics["kpis"]["total_attacks"]},
        ]
        failed_events = [
            event for event in build_latest_feed(rows, limit=300) if event["category"] == "attack" and event["outcome"] == "failed"
        ]
        detail["events"] = failed_events[:25]
        return detail

    if fact_key == "home_net":
        detail["title"] = "Home Net Acres"
        detail["summary"] = (
            f"Home net is {analytics['kpis']['home_net']} "
            f"(gained {analytics['kpis']['home_gained']} / lost {analytics['kpis']['home_lost']})."
        )
        detail["rows"] = [
            {"label": "Gained", "value": analytics["kpis"]["home_gained"]},
            {"label": "Lost", "value": analytics["kpis"]["home_lost"]},
            {"label": "Net", "value": analytics["kpis"]["home_net"]},
        ]
        return detail

    if fact_key == "aid_shipments":
        detail["title"] = "Aid Shipments"
        detail["summary"] = f"Total aid events in scope: {analytics['kpis']['aid_shipments']}."
        top_aid = sorted(analytics["active_rows"], key=lambda row: (row["aid_sent"] + row["aid_received"], row["aid_sent"]), reverse=True)
        detail["rows"] = [
            {
                "label": f"{row['name']} ({row['kingdom'] or '?'})",
                "value": f"out {row['aid_sent']} / in {row['aid_received']}",
            }
            for row in top_aid[:12]
            if row["aid_sent"] > 0 or row["aid_received"] > 0
        ]
        return detail

    if fact_key == "wars":
        detail["title"] = "War Summary"
        detail["summary"] = (
            f"{analytics['kpis']['active_wars']} active / {analytics['kpis']['completed_wars']} completed. "
            f"Victories {analytics['kpis']['war_victories']} / failures {analytics['kpis']['war_failures']}."
        )
        detail["rows"] = [
            {
                "label": row["war_label"],
                "value": f"hits {row['hits_for']}:{row['hits_against']} acres {row['acres_for']}:{row['acres_against']}",
            }
            for row in analytics["war_rows"]
        ]
        return detail

    if fact_key == "war" and key:
        war_row = next((row for row in analytics["war_rows"] if row["war_id"] == str(key)), None)
        if not war_row:
            return detail
        detail["title"] = f"War Detail: {war_row['opponent_name']}"
        detail["summary"] = (
            f"{war_row['start_day']} to {war_row['end_day']} [{war_row['result']}]. "
            f"Hits {war_row['hits_for']}:{war_row['hits_against']} / Acres {war_row['acres_for']}:{war_row['acres_against']}."
        )
        detail["rows"] = [
            {"label": "Opponent Kingdom", "value": war_row["opponent_kingdom"] or "-"},
            {"label": "Post-war Expires", "value": war_row["postwar_expires"]},
            {"label": "Post-war Ended", "value": war_row["postwar_end_day"]},
            {"label": "Net Acres", "value": war_row["net_acres"]},
        ]
        return detail

    if fact_key == "opponent" and key:
        opp_row = next((row for row in analytics["opponent_rows"] if row["kingdom"] == key), None)
        if not opp_row:
            return detail
        detail["title"] = f"Opponent Pressure: {key}"
        detail["summary"] = (
            f"Hits {opp_row['hits_for']} for / {opp_row['hits_against']} against. "
            f"Acres {opp_row['acres_for']} for / {opp_row['acres_against']} against (net {opp_row['net']})."
        )
        detail["rows"] = [
            {"label": "Hits For", "value": opp_row["hits_for"]},
            {"label": "Hits Against", "value": opp_row["hits_against"]},
            {"label": "Acres For", "value": opp_row["acres_for"]},
            {"label": "Acres Against", "value": opp_row["acres_against"]},
            {"label": "Net", "value": opp_row["net"]},
        ]
        detail["events"] = [
            event
            for event in build_latest_feed(rows, limit=300)
            if event["category"] == "attack" and (event["actor_kingdom"] == key or event["target_kingdom"] == key)
        ][:25]
        return detail

    return detail


def ingest_loop(config_path: str, stop_event: threading.Event) -> None:
    try:
        cfg = load_config(config_path)
    except Exception as exc:  # pragma: no cover
        update_ingest_state(enabled=True, running=False, last_error=f"Config error: {exc}")
        print(f"[app] Background ingest config ERROR: {exc}")
        return

    poll_seconds = int(cfg.get("poll_seconds", DEFAULT_POLL_SECONDS))
    update_ingest_state(enabled=True, last_error=None)
    print(f"[app] Background ingest loop started (poll_seconds={poll_seconds}, config={config_path})")

    while not stop_event.is_set():
        started = time.time()
        update_ingest_state(
            running=True,
            last_started_utc=utc_now_iso(),
            last_error=None,
        )

        try:
            parsed_events = run_ingest_cycle(config_path)
            update_ingest_state(
                last_success_utc=utc_now_iso(),
                last_parsed_events=parsed_events,
                last_error=None,
            )
        except Exception as exc:  # pragma: no cover
            update_ingest_state(last_error=str(exc))
            print(f"[app] ingest ERROR: {exc}")
        finally:
            with STATE_LOCK:
                INGEST_STATE["running"] = False
                INGEST_STATE["iterations"] = int(INGEST_STATE["iterations"]) + 1
                INGEST_STATE["last_duration_seconds"] = round(time.time() - started, 3)

        if stop_event.wait(poll_seconds):
            break

    print("[app] Background ingest loop stopped")


def start_ingest_thread(config_path: str) -> bool:
    global INGEST_THREAD

    with INGEST_THREAD_LOCK:
        if INGEST_THREAD and INGEST_THREAD.is_alive():
            return False

        init_db()
        STOP_EVENT.clear()
        INGEST_THREAD = threading.Thread(
            target=ingest_loop,
            args=(config_path, STOP_EVENT),
            name="ingest-worker",
            daemon=True,
        )
        INGEST_THREAD.start()
        return True


def maybe_start_ingest_for_wsgi() -> None:
    enabled = env_truthy(os.getenv("UTOPIA_ENABLE_INGEST"), default=False)
    if not enabled:
        update_ingest_state(enabled=False)
        return

    config_path = default_config_path()
    started = start_ingest_thread(config_path)
    if started:
        print(f"[app] WSGI ingest thread started using config={config_path}")


def requested_war_id() -> Optional[str]:
    raw = (request.args.get("war") or "").strip()
    if not raw or raw.lower() == "all":
        return None
    return raw


def requested_start_day() -> Optional[str]:
    return normalize_iso_day(request.args.get("start"))


def requested_end_day() -> Optional[str]:
    return normalize_iso_day(request.args.get("end"))


def requested_kingdom_coord() -> Optional[str]:
    raw = (request.args.get("kingdom") or "").strip()
    if re.match(r"^\d+:\d+$", raw):
        return raw
    return None


def requested_compare_coord() -> Optional[str]:
    raw = (request.args.get("compare") or "").strip()
    if re.match(r"^\d+:\d+$", raw):
        return raw
    return None


def requested_view_mode() -> str:
    raw = (request.args.get("view") or "").strip().lower()
    return raw if raw in {"overview", "war", "kingdom", "targets"} else "overview"


def war_context():
    cached = cache_get("base_rows_full_analytics")
    if cached is not None:
        all_rows, full_analytics = cached
        all_rows = [dict(row) if not isinstance(row, dict) else row for row in all_rows]
        full_analytics = dict(full_analytics)
    else:
        all_rows = fetch_event_rows()
        full_analytics = build_dashboard_analytics(all_rows)
        cache_set("base_rows_full_analytics", (all_rows, full_analytics))
    selected_war = resolve_selected_war(full_analytics["war_rows"], requested_war_id())
    filtered_rows = filter_rows_for_war(all_rows, selected_war)
    start_day = requested_start_day()
    end_day = requested_end_day()
    filtered_rows = filter_rows_for_fetched_day(filtered_rows, start_day, end_day)
    return all_rows, full_analytics, selected_war, filtered_rows, start_day, end_day


def scope_query_params() -> Dict[str, str]:
    out: Dict[str, str] = {}
    for key in ("war", "start", "end", "view"):
        value = (request.args.get(key) or "").strip()
        if value:
            out[key] = value
    return out


@app.route("/")
def dashboard():
    init_db()
    _, full_analytics, selected_war, filtered_rows, start_day, end_day = war_context()
    ops_rows = scoped_ops_rows(selected_war, start_day, end_day)
    analytics = build_dashboard_analytics(
        filtered_rows,
        forced_home_kingdom=full_analytics["home_kingdom"],
        include_wars=True,
    )
    analytics["war_rows"] = full_analytics["war_rows"]
    analytics["selected_war_id"] = selected_war["war_id"] if selected_war else "all"
    analytics["selected_war_label"] = selected_war["war_label"] if selected_war else "All events"
    analytics["kpis"]["active_wars"] = full_analytics["kpis"]["active_wars"]
    analytics["kpis"]["completed_wars"] = full_analytics["kpis"]["completed_wars"]
    analytics["kpis"]["war_victories"] = full_analytics["kpis"]["war_victories"]
    analytics["kpis"]["war_failures"] = full_analytics["kpis"]["war_failures"]
    analytics["selected_start_day"] = start_day or ""
    analytics["selected_end_day"] = end_day or ""
    analytics["selected_view"] = requested_view_mode()
    ops_summary = build_ops_summary(
        filtered_rows,
        full_analytics["home_kingdom"],
        full_analytics["war_rows"],
        ops_rows=ops_rows,
    )
    analytics["kpis"]["op_damage_done"] = ops_summary["kpis"]["damage_done"]
    analytics["kpis"]["op_damage_taken"] = ops_summary["kpis"]["damage_taken"]
    analytics["kpis"]["op_net_damage"] = ops_summary["kpis"]["net_damage"]
    analytics["kpis"]["war_op_damage_done"] = ops_summary["kpis"]["war_damage_done"]
    analytics["kpis"]["war_op_damage_taken"] = ops_summary["kpis"]["war_damage_taken"]
    war_command = build_war_command(
        filtered_rows,
        ops_rows,
        full_analytics["home_kingdom"],
        full_analytics["war_rows"],
    )

    snapshot = build_snapshot_analytics(
        analytics,
        selected_war,
        requested_kingdom_coord(),
        requested_compare_coord(),
    )

    scope_params = scope_query_params()
    ops_summary_href = "/ops"
    if scope_params:
        ops_summary_href = f"/ops?{urlencode(scope_params)}"

    latest = build_latest_feed(filtered_rows)
    return render_template(
        "dashboard.html",
        analytics=analytics,
        snapshot=snapshot,
        war_command=war_command,
        latest=latest,
        ingest=snapshot_ingest_state(),
        ops_summary_href=ops_summary_href,
    )


@app.route("/province")
def province_detail_page():
    init_db()
    province_name = (request.args.get("name") or "").strip()
    province_kingdom = (request.args.get("kingdom") or "").strip() or None
    if not province_name:
        return jsonify({"error": "missing_name"}), 400

    _, full_analytics, selected_war, filtered_rows, start_day, end_day = war_context()
    ops_rows = scoped_ops_rows(selected_war, start_day, end_day)
    detail = build_province_detail(
        province_name,
        province_kingdom,
        filtered_rows,
        full_analytics["home_kingdom"],
        full_analytics["war_rows"],
        ops_rows=ops_rows,
    )
    if not detail:
        return jsonify({"error": "invalid_name"}), 400

    scope_params = scope_query_params()
    dashboard_href = "/"
    ops_summary_href = "/ops"
    if scope_params:
        dashboard_href = f"/?{urlencode(scope_params)}"
        ops_summary_href = f"/ops?{urlencode(scope_params)}"

    return render_template(
        "province_detail.html",
        detail=detail,
        ingest=snapshot_ingest_state(),
        home_kingdom=full_analytics["home_kingdom"],
        selected_war_label=selected_war["war_label"] if selected_war else "All events",
        selected_start_day=start_day or "",
        selected_end_day=end_day or "",
        dashboard_href=dashboard_href,
        ops_summary_href=ops_summary_href,
    )


@app.route("/api/province_detail")
def api_province_detail():
    init_db()
    province_name = (request.args.get("name") or "").strip()
    province_kingdom = (request.args.get("kingdom") or "").strip() or None
    if not province_name:
        return jsonify({"error": "missing_name"}), 400

    _, full_analytics, selected_war, filtered_rows, start_day, end_day = war_context()
    ops_rows = scoped_ops_rows(selected_war, start_day, end_day)
    detail = build_province_detail(
        province_name,
        province_kingdom,
        filtered_rows,
        full_analytics["home_kingdom"],
        full_analytics["war_rows"],
        ops_rows=ops_rows,
    )
    if not detail:
        return jsonify({"error": "invalid_name"}), 400
    return jsonify(detail)


@app.route("/ops")
def ops_summary_page():
    init_db()
    _, full_analytics, selected_war, filtered_rows, start_day, end_day = war_context()
    ops_rows = scoped_ops_rows(selected_war, start_day, end_day)
    ops = build_ops_summary(
        filtered_rows,
        full_analytics["home_kingdom"],
        full_analytics["war_rows"],
        ops_rows=ops_rows,
    )

    scope_params = scope_query_params()
    dashboard_href = "/"
    if scope_params:
        dashboard_href = f"/?{urlencode(scope_params)}"

    return render_template(
        "ops_summary.html",
        ops=ops,
        ingest=snapshot_ingest_state(),
        home_kingdom=full_analytics["home_kingdom"],
        selected_war_label=selected_war["war_label"] if selected_war else "All events",
        selected_start_day=start_day or "",
        selected_end_day=end_day or "",
        dashboard_href=dashboard_href,
    )


@app.route("/api/ops_summary")
def api_ops_summary():
    init_db()
    _, full_analytics, selected_war, filtered_rows, start_day, end_day = war_context()
    ops_rows = scoped_ops_rows(selected_war, start_day, end_day)
    payload = build_ops_summary(
        filtered_rows,
        full_analytics["home_kingdom"],
        full_analytics["war_rows"],
        ops_rows=ops_rows,
    )
    return jsonify(payload)


@app.route("/api/war_command")
def api_war_command():
    init_db()
    _, full_analytics, selected_war, filtered_rows, start_day, end_day = war_context()
    ops_rows = scoped_ops_rows(selected_war, start_day, end_day)
    payload = build_war_command(
        filtered_rows,
        ops_rows,
        full_analytics["home_kingdom"],
        full_analytics["war_rows"],
    )
    return jsonify(payload)


@app.route("/api/replay_timeline")
def api_replay_timeline():
    init_db()
    _, full_analytics, selected_war, filtered_rows, start_day, end_day = war_context()
    ops_rows = scoped_ops_rows(selected_war, start_day, end_day)
    rows = build_replay_timeline(
        filtered_rows,
        ops_rows,
        full_analytics["home_kingdom"],
        full_analytics["war_rows"],
    )
    return jsonify({"rows": rows})


@app.route("/api/momentum")
def api_momentum():
    init_db()
    _, _, _, filtered_rows, _, _ = war_context()
    return jsonify(build_momentum_rows(filtered_rows))


@app.route("/api/land_swing")
def api_land_swing():
    init_db()
    _, full_analytics, _, filtered_rows, _, _ = war_context()
    analytics = build_dashboard_analytics(
        filtered_rows,
        forced_home_kingdom=full_analytics["home_kingdom"],
        include_wars=True,
    )
    return jsonify(
        {
            "home_kingdom": analytics["home_kingdom"],
            "rows": analytics["land_swing_rows"],
        }
    )


@app.route("/api/nw_swing")
def api_nw_swing():
    init_db()
    _, full_analytics, _, filtered_rows, _, _ = war_context()
    rows = build_home_nw_swing_rows(full_analytics["home_kingdom"], filtered_rows)
    return jsonify(
        {
            "home_kingdom": full_analytics["home_kingdom"],
            "rows": rows,
        }
    )


@app.route("/api/wars")
def api_wars():
    init_db()
    analytics = build_dashboard_analytics()
    return jsonify(
        {
            "home_kingdom": analytics["home_kingdom"],
            "kpis": {
                "active_wars": analytics["kpis"]["active_wars"],
                "completed_wars": analytics["kpis"]["completed_wars"],
                "war_victories": analytics["kpis"]["war_victories"],
                "war_failures": analytics["kpis"]["war_failures"],
            },
            "rows": analytics["war_rows"],
        }
    )


@app.route("/api/province_history")
def api_province_history():
    init_db()
    name = (request.args.get("name") or "").strip()
    kingdom = (request.args.get("kingdom") or "").strip() or None
    if not name:
        return jsonify({"error": "missing_name"}), 400

    _, full_analytics, _, filtered_rows, _, _ = war_context()
    payload = build_province_history(
        name,
        kingdom,
        filtered_rows,
        full_analytics["home_kingdom"],
        full_analytics["war_rows"],
    )
    if not payload:
        return jsonify({"error": "invalid_name"}), 400
    return jsonify(payload)


@app.route("/api/fact_detail")
def api_fact_detail():
    init_db()
    fact = (request.args.get("fact") or "").strip()
    key = (request.args.get("key") or "").strip() or None
    _, full_analytics, selected_war, filtered_rows, _, _ = war_context()
    analytics = build_dashboard_analytics(
        filtered_rows,
        forced_home_kingdom=full_analytics["home_kingdom"],
        include_wars=True,
    )
    analytics["war_rows"] = full_analytics["war_rows"]
    analytics["kpis"]["active_wars"] = full_analytics["kpis"]["active_wars"]
    analytics["kpis"]["completed_wars"] = full_analytics["kpis"]["completed_wars"]
    analytics["kpis"]["war_victories"] = full_analytics["kpis"]["war_victories"]
    analytics["kpis"]["war_failures"] = full_analytics["kpis"]["war_failures"]
    return jsonify(build_fact_detail(fact, key, analytics, filtered_rows))


@app.route("/api/kingdom_trends")
def api_kingdom_trends():
    init_db()
    _, full_analytics, selected_war, filtered_rows, _, _ = war_context()
    analytics = build_dashboard_analytics(
        filtered_rows,
        forced_home_kingdom=full_analytics["home_kingdom"],
        include_wars=True,
    )
    snapshot = build_snapshot_analytics(
        analytics,
        selected_war,
        requested_kingdom_coord(),
        requested_compare_coord(),
    )
    return jsonify(
        {
            "available": snapshot["available"],
            "focus_kingdom": snapshot["focus_kingdom"],
            "focus_label": snapshot["focus_label"],
            "rows": snapshot["focus_trend_rows"],
            "doctrines": snapshot["focus_doctrines"],
            "roster": snapshot["focus_roster"],
            "race_mix": snapshot["race_mix"],
            "alerts": snapshot["alerts"],
        }
    )


@app.route("/api/kingdom_compare")
def api_kingdom_compare():
    init_db()
    _, full_analytics, selected_war, filtered_rows, _, _ = war_context()
    analytics = build_dashboard_analytics(
        filtered_rows,
        forced_home_kingdom=full_analytics["home_kingdom"],
        include_wars=True,
    )
    snapshot = build_snapshot_analytics(
        analytics,
        selected_war,
        requested_kingdom_coord(),
        requested_compare_coord(),
    )
    return jsonify(
        {
            "available": snapshot["available"],
            "compare": snapshot["compare"],
        }
    )


@app.route("/api/targeting_board")
def api_targeting_board():
    init_db()
    _, full_analytics, selected_war, filtered_rows, _, _ = war_context()
    analytics = build_dashboard_analytics(
        filtered_rows,
        forced_home_kingdom=full_analytics["home_kingdom"],
        include_wars=True,
    )
    snapshot = build_snapshot_analytics(
        analytics,
        selected_war,
        requested_kingdom_coord(),
        requested_compare_coord(),
    )
    return jsonify({"rows": snapshot["target_board_rows"]})


@app.route("/api/province_snapshot_timeline")
def api_province_snapshot_timeline():
    init_db()
    kingdom = (request.args.get("kingdom") or "").strip()
    name = (request.args.get("name") or "").strip()
    if not kingdom or not name:
        return jsonify({"error": "missing_params"}), 400

    rows = fetchall(
        """
        SELECT fetched_at_utc, kingdom_coord, slot, province_name, race, land, networth, nwpa, nobility
        FROM kd_province_snapshots
        WHERE kingdom_coord=? AND LOWER(province_name)=LOWER(?)
        ORDER BY fetched_at_utc ASC
        """,
        (kingdom, name),
    )
    out = []
    for row in rows:
        out.append(
            {
                "fetched_at_utc": row["fetched_at_utc"],
                "day": (row["fetched_at_utc"] or "")[:10],
                "slot": row["slot"],
                "province_name": row["province_name"],
                "race": row["race"],
                "land": row["land"],
                "networth": row["networth"],
                "nwpa": row["nwpa"],
                "nobility": row["nobility"],
            }
        )
    return jsonify({"rows": out[-180:]})


@app.route("/api/status")
def api_status():
    return jsonify(snapshot_ingest_state())


@app.route("/healthz")
def healthz():
    return jsonify({"ok": True, "utc": utc_now_iso()})


@app.errorhandler(Exception)
def handle_unexpected_error(error):  # noqa: ANN001
    if isinstance(error, HTTPException):
        return error

    path = request.path if request else "unknown"
    print(f"[app] UNHANDLED ERROR path={path}: {error}")
    print(traceback.format_exc())
    if path.startswith("/api/"):
        return jsonify({"error": "internal_server_error"}), 500
    return (
        "Internal server error. Check server logs for traceback and request path.",
        500,
    )


maybe_start_ingest_for_wsgi()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default=default_config_path())
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=5055)
    parser.add_argument("--debug", action="store_true")
    parser.add_argument("--no-ingest", action="store_true")
    args = parser.parse_args()

    init_db()

    if args.no_ingest:
        update_ingest_state(enabled=False)
        print("[app] Background ingest disabled (--no-ingest)")
    else:
        start_ingest_thread(args.config)

    try:
        app.run(
            host=args.host,
            port=args.port,
            debug=args.debug,
            use_reloader=False,
        )
    finally:
        STOP_EVENT.set()
