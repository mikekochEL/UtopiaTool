import argparse
import os
import re
import threading
import time
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from flask import Flask, jsonify, render_template, request

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


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def update_ingest_state(**kwargs: Any) -> None:
    with STATE_LOCK:
        INGEST_STATE.update(kwargs)


def snapshot_ingest_state() -> Dict[str, Any]:
    with STATE_LOCK:
        return dict(INGEST_STATE)


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


def parse_attack_summary(summary: str) -> Dict[str, Any]:
    text = (summary or "").strip()

    for pattern in ATTACK_SUCCESS_PATTERNS:
        match = pattern.match(text)
        if match:
            return {
                "outcome": "success",
                "acres": int(match.group("acres")),
                "target_loss_acres": int(match.group("acres")),
                "actor_raw": match.group("actor"),
                "target_raw": match.group("target"),
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
            }

    return {
        "outcome": "unknown",
        "acres": 0,
        "target_loss_acres": 0,
        "actor_raw": None,
        "target_raw": None,
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
                impact_acres = acres_transfer if acres_transfer > 0 else acres_lost

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
                "day_key": event_day_key,
                "actor_kingdom": actor_kingdom,
                "target_kingdom": target_kingdom,
                "outcome": attack["outcome"],
                "acres": int(attack["acres"]),
                "target_loss_acres": int(attack.get("target_loss_acres", attack["acres"])),
            }
        )

    home_kingdom = forced_home_kingdom
    if not home_kingdom and kingdom_mentions:
        home_kingdom = sorted(kingdom_mentions.items(), key=lambda item: (-item[1], item[0]))[0][0]

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
            impact_acres = acres_transfer if acres_transfer > 0 else acres_lost
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
    war_rows = build_war_rows(war_events, attack_records, home_kingdom) if include_wars else []
    active_wars = sum(1 for row in war_rows if row["status"] == "active")
    completed_wars = len(war_rows) - active_wars
    war_victories = sum(1 for row in war_rows if row["result"] == "victory")
    war_failures = sum(1 for row in war_rows if row["result"] == "failed")

    return {
        "home_kingdom": home_kingdom,
        "category_totals": category_totals,
        "war_rows": war_rows,
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
    target_loss_acres = 0

    if category == "attack":
        attack = parse_attack_summary(summary)
        actor_raw = attack["actor_raw"] or actor_raw
        target_raw = attack["target_raw"] or target_raw
        actor_kingdom = extract_kingdom(actor_raw)
        target_kingdom = extract_kingdom(target_raw)
        outcome = attack["outcome"]
        acres_value = int(attack["acres"])
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
        "target_loss_acres": target_loss_acres,
        "outcome": outcome,
        "summary": summary,
    }


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


def build_province_history(
    province_name: str,
    province_kingdom: Optional[str],
    rows: list[Dict[str, Any]],
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

        role = "both" if actor_match and target_match else ("actor" if actor_match else "target")
        category = event["category"]
        outcome = event["outcome"]

        if category == "attack":
            if actor_match:
                stats["attacks_sent"] += 1
                if outcome == "success" and event["acres"]:
                    stats["gains"] += int(event["acres"])
            if target_match:
                stats["attacks_received"] += 1
                if outcome == "success":
                    loss_acres = int(event["target_loss_acres"]) if int(event["target_loss_acres"]) > 0 else int(event["acres"] or 0)
                    stats["losses"] += loss_acres
        elif category == "aid":
            if actor_match:
                stats["aid_sent"] += 1
            if target_match:
                stats["aid_received"] += 1

        entries.append(
            {
                "event_id": event["event_id"],
                "fetched_at_utc": event["fetched_at_utc"],
                "event_time_text": event["event_time_text"] or "-",
                "category": category,
                "outcome": outcome or "-",
                "acres": event["acres"] if event["acres"] is not None else "-",
                "role": role,
                "actor": event["actor"],
                "target": event["target"],
                "summary": event["summary"],
            }
        )

    entries.sort(key=lambda row: row["event_id"], reverse=True)
    entries = entries[:limit]
    stats["net"] = stats["gains"] - stats["losses"]

    return {
        "province": normalized,
        "kingdom": target_kingdom or None,
        "stats": stats,
        "events": entries,
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


def war_context():
    all_rows = fetch_event_rows()
    full_analytics = build_dashboard_analytics(all_rows)
    selected_war = resolve_selected_war(full_analytics["war_rows"], requested_war_id())
    filtered_rows = filter_rows_for_war(all_rows, selected_war)
    return all_rows, full_analytics, selected_war, filtered_rows


@app.route("/")
def dashboard():
    init_db()
    _, full_analytics, selected_war, filtered_rows = war_context()
    analytics = build_dashboard_analytics(
        filtered_rows,
        forced_home_kingdom=full_analytics["home_kingdom"],
        include_wars=False,
    )
    analytics["war_rows"] = full_analytics["war_rows"]
    analytics["selected_war_id"] = selected_war["war_id"] if selected_war else "all"
    analytics["selected_war_label"] = selected_war["war_label"] if selected_war else "All events"
    analytics["kpis"]["active_wars"] = full_analytics["kpis"]["active_wars"]
    analytics["kpis"]["completed_wars"] = full_analytics["kpis"]["completed_wars"]
    analytics["kpis"]["war_victories"] = full_analytics["kpis"]["war_victories"]
    analytics["kpis"]["war_failures"] = full_analytics["kpis"]["war_failures"]
    latest = build_latest_feed(filtered_rows)
    return render_template(
        "dashboard.html",
        analytics=analytics,
        latest=latest,
        ingest=snapshot_ingest_state(),
    )


@app.route("/api/momentum")
def api_momentum():
    init_db()
    _, _, _, filtered_rows = war_context()
    return jsonify(build_momentum_rows(filtered_rows))


@app.route("/api/land_swing")
def api_land_swing():
    init_db()
    _, full_analytics, _, filtered_rows = war_context()
    analytics = build_dashboard_analytics(
        filtered_rows,
        forced_home_kingdom=full_analytics["home_kingdom"],
        include_wars=False,
    )
    return jsonify(
        {
            "home_kingdom": analytics["home_kingdom"],
            "rows": analytics["land_swing_rows"],
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

    _, _, _, filtered_rows = war_context()
    payload = build_province_history(name, kingdom, filtered_rows)
    if not payload:
        return jsonify({"error": "invalid_name"}), 400
    return jsonify(payload)


@app.route("/api/fact_detail")
def api_fact_detail():
    init_db()
    fact = (request.args.get("fact") or "").strip()
    key = (request.args.get("key") or "").strip() or None
    _, full_analytics, _, filtered_rows = war_context()
    analytics = build_dashboard_analytics(
        filtered_rows,
        forced_home_kingdom=full_analytics["home_kingdom"],
        include_wars=False,
    )
    analytics["war_rows"] = full_analytics["war_rows"]
    analytics["kpis"]["active_wars"] = full_analytics["kpis"]["active_wars"]
    analytics["kpis"]["completed_wars"] = full_analytics["kpis"]["completed_wars"]
    analytics["kpis"]["war_victories"] = full_analytics["kpis"]["war_victories"]
    analytics["kpis"]["war_failures"] = full_analytics["kpis"]["war_failures"]
    return jsonify(build_fact_detail(fact, key, analytics, filtered_rows))


@app.route("/api/status")
def api_status():
    return jsonify(snapshot_ingest_state())


@app.route("/healthz")
def healthz():
    return jsonify({"ok": True, "utc": utc_now_iso()})


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
