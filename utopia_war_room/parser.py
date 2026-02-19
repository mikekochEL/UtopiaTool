import hashlib
import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests
from bs4 import BeautifulSoup

from db import executemany, fetchall, fetchone, init_db

EVENT_LINE_RE = re.compile(
    r"^(?P<time>(January|February|March|April|May|June|July|August|September|October|November|December)\s+\d+\s+of\s+YR\d+)\s+(?P<rest>.+)$"
)
INTEL_OP_KEYWORDS = [
    "thie",
    "thief",
    "operation",
    "op ",
    "nightstrike",
    "propaganda",
    "rob",
    "stole",
    "steal",
    "kidnap",
    "spy",
    "sabotage",
    "assassinate",
    "wizard",
    "spell",
    "magic",
    "meteor",
    "fireball",
    "failed",
    "succeeded",
]

AID_RE = re.compile(r"^(?P<actor>.+?) has sent an aid shipment to (?P<target>.+?)\.$", re.IGNORECASE)
ATTACK_ACTOR_TARGET_RE = re.compile(
    r"^\d+\s*-\s*(?P<actor>.+?)\s+\(\s*\d+:\d+\s*\).+?(?:from|and)\s+\d+\s*-\s*(?P<target>.+?)\s+\(\s*\d+:\d+\s*\)",
    re.IGNORECASE,
)
KINGDOM_HEADING_RE = re.compile(
    r"The kingdom of\s+(?P<name>.+?)\s*\(\s*(?P<coord>\d+:\d+)\s*\)",
    re.IGNORECASE,
)
COORD_RE = re.compile(r"\(\s*(\d+:\d+)\s*\)")
INLINE_COORD_RE = re.compile(r"\b(\d+:\d+)\b")
NUMBER_RE = re.compile(r"(-?[\d,]+)")
PERCENT_RE = re.compile(r"(-?\d+)\s*%")
NETWORTH_TOTAL_AVG_RE = re.compile(r"([\d,]+)\s*gc(?:\s*\(avg:\s*([\d,]+)\s*gc\))?", re.IGNORECASE)
LAND_TOTAL_AVG_RE = re.compile(r"([\d,]+)\s*acres(?:\s*\(avg:\s*([\d,]+)\s*acres\))?", re.IGNORECASE)
RANK_RE = re.compile(r"(\d+)\s+of\s+\d+", re.IGNORECASE)
WARS_WON_RE = re.compile(r"(\d+)\s*/\s*([0-9.]+)")
INTEL_LOGIN_RE = re.compile(r"https://intel\.utopia\.site/login\?t=([^&\"']+)&s=([0-9]+)", re.IGNORECASE)
INTEL_UTO_DATE_RE = re.compile(
    r"^(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d+),\s*YR(\d+)$"
)

INTEL_THIEVERY_OP_TYPES = {
    "BRIBE_GENERALS",
    "BRIBE_THIEVES",
    "CRYSTAL_BALL",
    "INCITE_RIOTS",
    "INFILTRATE",
    "KIDNAP",
    "NIGHT_STRIKE",
    "ROB_THE_TOWERS",
    "ROB_THE_VAULTS",
    "SPY_ON_DEFENSE",
    "SPY_ON_MILITARY",
    "SPY_ON_THRONE",
    "SURVEY",
}


UPSERT_NEWS_SQL = """
INSERT INTO kd_news_events(
  fetched_at_utc, event_time_text, category, actor, target, summary, raw_line, sha256
)
VALUES (?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(sha256) DO UPDATE SET
  fetched_at_utc=excluded.fetched_at_utc,
  event_time_text=excluded.event_time_text,
  category=excluded.category,
  actor=excluded.actor,
  target=excluded.target,
  summary=excluded.summary,
  raw_line=excluded.raw_line
"""

UPSERT_KINGDOM_SQL = """
INSERT INTO kd_kingdom_snapshots(
  fetched_at_utc, kingdom_coord, kingdom_name, total_provinces, stance,
  total_networth, avg_networth, networth_rank,
  total_land, avg_land, land_rank,
  total_honor, honor_rank, wars_won, war_score, avg_opp_relative_size_pct,
  source_fetch_id, sha256
)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(kingdom_coord, fetched_at_utc, sha256) DO NOTHING
"""

UPSERT_PROVINCE_SQL = """
INSERT INTO kd_province_snapshots(
  fetched_at_utc, kingdom_coord, slot, province_name, race, land, networth, nwpa,
  nobility, is_monarch, is_steward, is_you, is_online, source_fetch_id, sha256
)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(kingdom_coord, fetched_at_utc, slot) DO UPDATE SET
  province_name=excluded.province_name,
  race=excluded.race,
  land=excluded.land,
  networth=excluded.networth,
  nwpa=excluded.nwpa,
  nobility=excluded.nobility,
  is_monarch=excluded.is_monarch,
  is_steward=excluded.is_steward,
  is_you=excluded.is_you,
  is_online=excluded.is_online,
  source_fetch_id=excluded.source_fetch_id,
  sha256=excluded.sha256
"""

UPSERT_DOCTRINE_SQL = """
INSERT INTO kd_doctrine_snapshots(
  fetched_at_utc, kingdom_coord, race, provinces, doctrine_effect, current_bonus, source_fetch_id, sha256
)
VALUES (?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(kingdom_coord, fetched_at_utc, race, sha256) DO NOTHING
"""

UPSERT_WAR_HISTORY_SQL = """
INSERT INTO kd_war_history_snapshots(
  fetched_at_utc, kingdom_coord, opponent_name, opponent_coord, status, source_fetch_id, sha256
)
VALUES (?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(kingdom_coord, fetched_at_utc, opponent_name, opponent_coord, sha256) DO NOTHING
"""

UPSERT_OPS_SQL = """
INSERT INTO kd_ops_events(
  intel_op_id, fetched_at_utc, last_updated_utc, event_time_text, server,
  category, op_type, op_name, result_code, result_label,
  actor, actor_kingdom, target, target_kingdom,
  gain, damage, duration_ticks, summary, raw_line, sha256
)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(intel_op_id) DO UPDATE SET
  fetched_at_utc=excluded.fetched_at_utc,
  last_updated_utc=excluded.last_updated_utc,
  event_time_text=excluded.event_time_text,
  server=excluded.server,
  category=excluded.category,
  op_type=excluded.op_type,
  op_name=excluded.op_name,
  result_code=excluded.result_code,
  result_label=excluded.result_label,
  actor=excluded.actor,
  actor_kingdom=excluded.actor_kingdom,
  target=excluded.target,
  target_kingdom=excluded.target_kingdom,
  gain=excluded.gain,
  damage=excluded.damage,
  duration_ticks=excluded.duration_ticks,
  summary=excluded.summary,
  raw_line=excluded.raw_line,
  sha256=excluded.sha256
"""


def sha256_text(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8", errors="ignore")).hexdigest()


def normalize_text(s: str) -> str:
    return " ".join(s.split())


def parse_int(text: Optional[str]) -> Optional[int]:
    if not text:
        return None
    match = NUMBER_RE.search(str(text))
    if not match:
        return None
    return int(match.group(1).replace(",", ""))


def parse_float(text: Optional[str]) -> Optional[float]:
    if not text:
        return None
    cleaned = str(text).replace(",", "").replace("gc", "").strip()
    try:
        return float(cleaned)
    except ValueError:
        return None


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def normalize_intel_uto_date(text: Optional[str]) -> Optional[str]:
    if not text:
        return None
    cleaned = normalize_text(text)
    match = INTEL_UTO_DATE_RE.match(cleaned)
    if not match:
        return None
    month, day, year = match.groups()
    return f"{month} {int(day)} of YR{int(year)}"


def classify_intel_op(op_type: str, name: str) -> str:
    op = (op_type or "").strip().upper()
    nm = (name or "").strip().lower()
    if op in INTEL_THIEVERY_OP_TYPES:
        return "thievery"
    if any(token in nm for token in ["spy", "survey", "infiltrate", "rob", "kidnap", "bribe", "night strike", "incite"]):
        return "thievery"
    return "magic"


def intel_result_label(value: Any) -> str:
    try:
        num = int(value)
    except Exception:
        return "unknown"
    if num == 1:
        return "success"
    if num == 0:
        return "failed"
    if num == 2:
        return "partial"
    return "unknown"


def safe_float(value: Any) -> Optional[float]:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except Exception:
        cleaned = normalize_text(str(value)).replace(",", "")
        try:
            return float(cleaned)
        except Exception:
            return None


def safe_int(value: Any) -> Optional[int]:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except Exception:
        cleaned = normalize_text(str(value)).replace(",", "")
        if cleaned.isdigit() or (cleaned.startswith("-") and cleaned[1:].isdigit()):
            return int(cleaned)
        return None


def parse_coord_text(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = normalize_text(str(value))
    if not text:
        return None
    match = INLINE_COORD_RE.search(text)
    if not match:
        return None
    return match.group(1)


def infer_payload_coord(item: Dict[str, Any], prefix: str, fallback_text: str = "") -> Optional[str]:
    direct_candidates = [
        item.get(f"{prefix}Coord"),
        item.get(f"{prefix}KingdomCoord"),
        item.get(f"{prefix}coord"),
        item.get(f"{prefix}kingdomCoord"),
    ]
    for candidate in direct_candidates:
        coord = parse_coord_text(candidate)
        if coord:
            return coord

    kingdom_candidates = [
        item.get(f"{prefix}Kingdom"),
        item.get(f"{prefix}kingdom"),
        item.get(f"{prefix}Kd"),
        item.get(f"{prefix}kd"),
    ]
    island_candidates = [
        item.get(f"{prefix}Island"),
        item.get(f"{prefix}island"),
    ]
    for kd_value in kingdom_candidates:
        for island_value in island_candidates:
            if kd_value in (None, "") or island_value in (None, ""):
                continue
            try:
                kd = int(kd_value)
                island = int(island_value)
            except Exception:
                continue
            if kd > 0 and island > 0:
                return f"{kd}:{island}"

    return parse_coord_text(fallback_text)


def find_intel_site_token_and_server() -> Optional[Tuple[str, str]]:
    rows = fetchall(
        """
        SELECT raw_html
        FROM fetch_log
        WHERE page_key='kingdom_details'
        ORDER BY id DESC
        LIMIT 30
        """
    )
    if not rows:
        return None
    for row in rows:
        html = row["raw_html"] or ""
        match = INTEL_LOGIN_RE.search(html)
        if not match:
            continue
        token, server = match.group(1), match.group(2)
        if token and server:
            return token, server
    return None


def parse_rank(text: Optional[str]) -> Optional[int]:
    if not text:
        return None
    match = RANK_RE.search(str(text))
    if not match:
        return None
    return int(match.group(1))


def fetch_html_rows(page_key: str):
    return fetchall(
        "SELECT id, fetched_at_utc, raw_html FROM fetch_log WHERE page_key=? ORDER BY id ASC",
        (page_key,),
    )


def list_page_keys(explicit_page_key: Optional[str] = None) -> List[str]:
    if explicit_page_key:
        return [explicit_page_key]
    rows = fetchall("SELECT DISTINCT page_key FROM fetch_log ORDER BY page_key ASC")
    return [row["page_key"] for row in rows]


def extract_news_lines(html: str) -> List[str]:
    soup = BeautifulSoup(html, "html.parser")
    container = soup.select_one("#content-area") or soup.select_one(".game-content") or soup.body
    if not container:
        return []

    out: List[str] = []
    seen = set()

    for node in container.select("tr, li, p"):
        text = normalize_text(node.get_text(" ", strip=True))
        if text and EVENT_LINE_RE.match(text) and text not in seen:
            seen.add(text)
            out.append(text)

    for raw_line in container.get_text("\n", strip=True).splitlines():
        text = normalize_text(raw_line)
        if text and EVENT_LINE_RE.match(text) and text not in seen:
            seen.add(text)
            out.append(text)

    return out


def extract_intel_ops_lines(html: str) -> List[str]:
    soup = BeautifulSoup(html, "html.parser")
    container = soup.select_one("#content-area") or soup.select_one(".game-content") or soup.body
    if not container:
        return []

    out: List[str] = []
    seen = set()

    def maybe_add(text: str) -> None:
        line = normalize_text(text)
        if len(line) < 18:
            return
        lowered = line.lower()
        if not any(keyword in lowered for keyword in INTEL_OP_KEYWORDS):
            return
        if line in seen:
            return
        seen.add(line)
        out.append(line)

    for node in container.select("tr, li, p, div"):
        maybe_add(node.get_text(" ", strip=True))

    for raw_line in container.get_text("\n", strip=True).splitlines():
        maybe_add(raw_line)

    return out


def split_event_line(line: str) -> Tuple[Optional[str], str]:
    match = EVENT_LINE_RE.match(line)
    if not match:
        return None, line
    return match.group("time"), match.group("rest")


def classify_line(summary: str) -> Tuple[Optional[str], Optional[str], Optional[str], str]:
    line = summary
    lower = line.lower()

    actor = None
    target = None

    aid_match = AID_RE.match(line)
    if aid_match:
        actor = aid_match.group("actor")
        target = aid_match.group("target")

    atk_match = ATTACK_ACTOR_TARGET_RE.match(line)
    if atk_match:
        actor = actor or atk_match.group("actor")
        target = target or atk_match.group("target")

    if "dragon" in lower:
        category = "dragon"
    elif any(token in lower for token in ["stole", "thieves", "robbed", "nightstrike", "propaganda"]):
        category = "thievery"
    elif any(token in lower for token in ["spell", "cast", "magic", "meteor", "fireball", "nightmare"]):
        category = "magic"
    elif "aid shipment" in lower:
        category = "aid"
    elif any(
        token in lower
        for token in [
            "ceasefire",
            "hostile",
            "declared war",
            "at war",
            "withdrawn from war",
            "surrendered",
            "relations changed",
        ]
    ):
        category = "diplomacy"
    elif any(
        token in lower
        for token in [
            "captured",
            "invaded",
            "attempted an invasion",
            "attempted to invade",
            "repelled",
            "ambushed",
            "massacre",
            "attacked",
            "raze",
            "plundered",
            "learned",
        ]
    ):
        category = "attack"
    else:
        category = "other"

    return category, actor, target, line


def parse_war_history_rows(soup: BeautifulSoup) -> List[Dict[str, Optional[str]]]:
    container = soup.select_one("#war_history_content")
    if not container:
        return []

    rows: List[Dict[str, Optional[str]]] = []
    chunks: List[str] = []
    current: List[str] = []

    for node in container.children:
        if getattr(node, "name", None) == "br":
            text = normalize_text(" ".join(current))
            if text:
                chunks.append(text)
            current = []
            continue
        piece = normalize_text(node.get_text(" ", strip=True) if hasattr(node, "get_text") else str(node))
        if piece:
            current.append(piece)

    trailing = normalize_text(" ".join(current))
    if trailing:
        chunks.append(trailing)

    for chunk in chunks:
        coord_match = COORD_RE.search(chunk)
        opponent_coord = coord_match.group(1) if coord_match else None
        before = chunk[: coord_match.start()].strip() if coord_match else chunk
        after = chunk[coord_match.end() :].strip() if coord_match else ""
        opponent_name = normalize_text(before).strip("- ")
        if not opponent_name:
            continue
        status = normalize_text(after) if after else None
        rows.append(
            {
                "opponent_name": opponent_name,
                "opponent_coord": opponent_coord,
                "status": status,
            }
        )

    return rows


def parse_doctrine_rows(soup: BeautifulSoup) -> List[Dict[str, Any]]:
    heading = None
    for h2 in soup.find_all("h2"):
        if "War Doctrines" in normalize_text(h2.get_text(" ", strip=True)):
            heading = h2
            break
    if not heading:
        return []

    table = heading.find_next("table")
    if not table:
        return []

    rows: List[Dict[str, Any]] = []
    for tr in table.select("tbody tr"):
        tds = tr.find_all("td")
        if len(tds) < 4:
            continue
        race = normalize_text(tds[0].get_text(" ", strip=True))
        provinces = parse_int(tds[1].get_text(" ", strip=True))
        effect = normalize_text(tds[2].get_text(" ", strip=True))
        bonus = normalize_text(tds[3].get_text(" ", strip=True))
        if race:
            rows.append(
                {
                    "race": race,
                    "provinces": provinces,
                    "doctrine_effect": effect,
                    "current_bonus": bonus,
                }
            )
    return rows


def extract_kingdom_details_snapshot(html: str, fetched_at_utc: str, fetch_id: int) -> Optional[Dict[str, Any]]:
    soup = BeautifulSoup(html, "html.parser")
    game_content = soup.select_one(".game-content") or soup.body
    if not game_content:
        return None

    heading_node = soup.select_one(".change-kingdom-heading")
    heading_text = normalize_text(heading_node.get_text(" ", strip=True)) if heading_node else ""
    heading_match = KINGDOM_HEADING_RE.search(heading_text)
    if not heading_match:
        heading_match = KINGDOM_HEADING_RE.search(normalize_text(game_content.get_text(" ", strip=True)))
    if not heading_match:
        return None

    kingdom_coord = heading_match.group("coord")
    kingdom_name = normalize_text(heading_match.group("name"))

    stats_map: Dict[str, str] = {}
    stats_table = soup.select_one("table.two-column-stats")
    if stats_table:
        for tr in stats_table.select("tr"):
            cells = [normalize_text(cell.get_text(" ", strip=True)) for cell in tr.find_all(["th", "td"])]
            if len(cells) >= 2 and cells[0] and cells[1]:
                stats_map[cells[0]] = cells[1]
            if len(cells) >= 4 and cells[2] and cells[3]:
                stats_map[cells[2]] = cells[3]

    total_networth = None
    avg_networth = None
    total_land = None
    avg_land = None

    networth_text = stats_map.get("Total Networth", "")
    networth_match = NETWORTH_TOTAL_AVG_RE.search(networth_text)
    if networth_match:
        total_networth = int(networth_match.group(1).replace(",", ""))
        if networth_match.group(2):
            avg_networth = int(networth_match.group(2).replace(",", ""))

    land_text = stats_map.get("Total Land", "")
    land_match = LAND_TOTAL_AVG_RE.search(land_text)
    if land_match:
        total_land = int(land_match.group(1).replace(",", ""))
        if land_match.group(2):
            avg_land = int(land_match.group(2).replace(",", ""))

    wars_won = None
    war_score = None
    wars_won_text = stats_map.get("Wars Won / War Score", "")
    wars_match = WARS_WON_RE.search(wars_won_text)
    if wars_match:
        wars_won = int(wars_match.group(1))
        try:
            war_score = float(wars_match.group(2))
        except ValueError:
            war_score = None

    avg_opp_size = None
    opp_size_match = PERCENT_RE.search(stats_map.get("Average Opponent Relative Size", ""))
    if opp_size_match:
        avg_opp_size = int(opp_size_match.group(1))

    kingdom_snapshot = {
        "fetched_at_utc": fetched_at_utc,
        "kingdom_coord": kingdom_coord,
        "kingdom_name": kingdom_name,
        "total_provinces": parse_int(stats_map.get("Total Provinces")),
        "stance": stats_map.get("Stance"),
        "total_networth": total_networth,
        "avg_networth": avg_networth,
        "networth_rank": parse_rank(stats_map.get("Net Worth Rank")),
        "total_land": total_land,
        "avg_land": avg_land,
        "land_rank": parse_rank(stats_map.get("Land Rank")),
        "total_honor": parse_int(stats_map.get("Total Honor")),
        "honor_rank": parse_rank(stats_map.get("Honor Rank")),
        "wars_won": wars_won,
        "war_score": war_score,
        "avg_opp_relative_size_pct": avg_opp_size,
        "source_fetch_id": fetch_id,
    }

    provinces: List[Dict[str, Any]] = []
    prov_table = soup.select_one("table.tablesorter")
    if prov_table:
        for tr in prov_table.select("tbody tr"):
            classes = tr.get("class", [])
            if "unused-slot" in classes:
                continue
            tds = tr.find_all("td")
            if len(tds) < 7:
                continue

            slot = parse_int(tds[0].get_text(" ", strip=True))
            if slot is None:
                continue

            province_cell = tds[1]
            province_link = province_cell.find("a")
            province_name = normalize_text(
                province_link.get_text(" ", strip=True) if province_link else province_cell.get_text(" ", strip=True)
            )
            if not province_name or province_name == "-":
                continue

            province_cell_text = normalize_text(province_cell.get_text(" ", strip=True))
            provinces.append(
                {
                    "fetched_at_utc": fetched_at_utc,
                    "kingdom_coord": kingdom_coord,
                    "slot": slot,
                    "province_name": province_name,
                    "race": normalize_text(tds[2].get_text(" ", strip=True)),
                    "land": parse_int(tds[3].get_text(" ", strip=True)),
                    "networth": parse_int(tds[4].get_text(" ", strip=True)),
                    "nwpa": parse_float(tds[5].get_text(" ", strip=True)),
                    "nobility": normalize_text(tds[6].get_text(" ", strip=True)),
                    "is_monarch": 1 if ("monarch" in classes or "(M)" in province_cell_text) else 0,
                    "is_steward": 1 if ("chamberlain" in classes or "(S)" in province_cell_text) else 0,
                    "is_you": 1 if "you" in classes else 0,
                    "is_online": 1 if "*" in province_cell_text else 0,
                    "source_fetch_id": fetch_id,
                }
            )

    war_history_rows = []
    for entry in parse_war_history_rows(soup):
        war_history_rows.append(
            {
                "fetched_at_utc": fetched_at_utc,
                "kingdom_coord": kingdom_coord,
                "opponent_name": entry["opponent_name"],
                "opponent_coord": entry.get("opponent_coord"),
                "status": entry.get("status"),
                "source_fetch_id": fetch_id,
            }
        )

    doctrine_rows = []
    for entry in parse_doctrine_rows(soup):
        doctrine_rows.append(
            {
                "fetched_at_utc": fetched_at_utc,
                "kingdom_coord": kingdom_coord,
                "race": entry["race"],
                "provinces": entry.get("provinces"),
                "doctrine_effect": entry.get("doctrine_effect"),
                "current_bonus": entry.get("current_bonus"),
                "source_fetch_id": fetch_id,
            }
        )

    return {
        "kingdom": kingdom_snapshot,
        "provinces": provinces,
        "war_history": war_history_rows,
        "doctrines": doctrine_rows,
    }


def parse_and_store_kingdom_details(page_key: str = "kingdom_details") -> int:
    fetch_rows = fetch_html_rows(page_key)
    if not fetch_rows:
        return 0

    kingdom_rows = []
    province_rows = []
    doctrine_rows = []
    war_history_rows = []
    parsed_fetches = 0

    for fetch_row in fetch_rows:
        payload = extract_kingdom_details_snapshot(
            fetch_row["raw_html"],
            fetch_row["fetched_at_utc"],
            int(fetch_row["id"]),
        )
        if not payload:
            continue
        parsed_fetches += 1

        kingdom = payload["kingdom"]
        kingdom_fingerprint = (
            f"{kingdom['fetched_at_utc']}|{kingdom['kingdom_coord']}|{kingdom['total_land']}|"
            f"{kingdom['total_networth']}|{kingdom['total_honor']}|{kingdom['stance']}"
        )
        kingdom_rows.append(
            (
                kingdom["fetched_at_utc"],
                kingdom["kingdom_coord"],
                kingdom["kingdom_name"],
                kingdom["total_provinces"],
                kingdom["stance"],
                kingdom["total_networth"],
                kingdom["avg_networth"],
                kingdom["networth_rank"],
                kingdom["total_land"],
                kingdom["avg_land"],
                kingdom["land_rank"],
                kingdom["total_honor"],
                kingdom["honor_rank"],
                kingdom["wars_won"],
                kingdom["war_score"],
                kingdom["avg_opp_relative_size_pct"],
                kingdom["source_fetch_id"],
                sha256_text(kingdom_fingerprint),
            )
        )

        for row in payload["provinces"]:
            fingerprint = (
                f"{row['fetched_at_utc']}|{row['kingdom_coord']}|{row['slot']}|{row['province_name']}|"
                f"{row['land']}|{row['networth']}|{row['nwpa']}|{row['nobility']}|"
                f"{row['is_monarch']}|{row['is_steward']}|{row['is_you']}|{row['is_online']}"
            )
            province_rows.append(
                (
                    row["fetched_at_utc"],
                    row["kingdom_coord"],
                    row["slot"],
                    row["province_name"],
                    row["race"],
                    row["land"],
                    row["networth"],
                    row["nwpa"],
                    row["nobility"],
                    row["is_monarch"],
                    row["is_steward"],
                    row["is_you"],
                    row["is_online"],
                    row["source_fetch_id"],
                    sha256_text(fingerprint),
                )
            )

        for row in payload["doctrines"]:
            fingerprint = (
                f"{row['fetched_at_utc']}|{row['kingdom_coord']}|{row['race']}|"
                f"{row['provinces']}|{row['doctrine_effect']}|{row['current_bonus']}"
            )
            doctrine_rows.append(
                (
                    row["fetched_at_utc"],
                    row["kingdom_coord"],
                    row["race"],
                    row["provinces"],
                    row["doctrine_effect"],
                    row["current_bonus"],
                    row["source_fetch_id"],
                    sha256_text(fingerprint),
                )
            )

        for row in payload["war_history"]:
            fingerprint = (
                f"{row['fetched_at_utc']}|{row['kingdom_coord']}|{row['opponent_name']}|"
                f"{row['opponent_coord']}|{row['status']}"
            )
            war_history_rows.append(
                (
                    row["fetched_at_utc"],
                    row["kingdom_coord"],
                    row["opponent_name"],
                    row["opponent_coord"],
                    row["status"],
                    row["source_fetch_id"],
                    sha256_text(fingerprint),
                )
            )

    changed = 0
    if kingdom_rows:
        changed += max(executemany(UPSERT_KINGDOM_SQL, kingdom_rows), 0)
    if province_rows:
        changed += max(executemany(UPSERT_PROVINCE_SQL, province_rows), 0)
    if doctrine_rows:
        changed += max(executemany(UPSERT_DOCTRINE_SQL, doctrine_rows), 0)
    if war_history_rows:
        changed += max(executemany(UPSERT_WAR_HISTORY_SQL, war_history_rows), 0)

    print(
        f"[parser] kingdom_details fetches={len(fetch_rows)} parsed={parsed_fetches} "
        f"kingdom_rows={len(kingdom_rows)} provinces={len(province_rows)} "
        f"doctrines={len(doctrine_rows)} war_history={len(war_history_rows)} upserted={changed}"
    )
    return changed


def parse_and_store_intel_site_ops() -> int:
    creds = find_intel_site_token_and_server()
    if not creds:
        print("[parser] intel_site_ops skipped (no token link found in kingdom_details HTML).")
        return 0

    token, server = creds
    url = f"https://api.intel.utopia.site/Kingdom/v1/KingdomOps?server={server}"
    headers = {"Utopia-Token": token, "User-Agent": "Mozilla/5.0"}

    try:
        response = requests.get(url, headers=headers, timeout=30)
    except Exception as exc:
        print(f"[parser] intel_site_ops request ERROR: {exc}")
        return 0

    if not response.ok:
        print(f"[parser] intel_site_ops HTTP {response.status_code}; body={response.text[:180]}")
        return 0

    try:
        payload = response.json()
    except Exception as exc:
        print(f"[parser] intel_site_ops JSON parse ERROR: {exc}")
        return 0

    if not isinstance(payload, list):
        print("[parser] intel_site_ops unexpected payload type; expected list.")
        return 0

    rows_to_upsert = []
    ops_rows_to_upsert = []
    for item in payload:
        if not isinstance(item, dict):
            continue
        op_id = item.get("id")
        if op_id is None:
            continue

        province = normalize_text(str(item.get("provinceName") or "")).strip()
        target = normalize_text(str(item.get("targetName") or "")).strip()
        op_type = normalize_text(str(item.get("opType") or "")).strip()
        op_name = normalize_text(str(item.get("name") or "")).strip()
        result_num = item.get("result")
        result_label = intel_result_label(result_num)
        uto_date = normalize_intel_uto_date(item.get("utoDate"))
        last_updated = normalize_text(str(item.get("lastUpdated") or "")).strip() or utc_now_iso()
        category = classify_intel_op(op_type, op_name)
        damage_value = item.get("damage")
        gain_value = item.get("gain")
        duration_value = item.get("duration")
        actor_kingdom = infer_payload_coord(item, "province", province)
        target_kingdom = infer_payload_coord(item, "target", target)

        damage_text = ""
        if isinstance(damage_value, (int, float)):
            damage_text = str(int(damage_value))
        elif damage_value not in (None, ""):
            damage_text = normalize_text(str(damage_value))

        gain_text = ""
        if isinstance(gain_value, (int, float)):
            gain_text = str(int(gain_value))
        elif gain_value not in (None, ""):
            gain_text = normalize_text(str(gain_value))

        duration_text = ""
        if isinstance(duration_value, (int, float)):
            duration_text = str(int(duration_value))
        elif duration_value not in (None, ""):
            duration_text = normalize_text(str(duration_value))

        actor = province or None
        target_party = target if target and target.lower() not in {"anything", "self", "-"} else None
        summary_parts = [
            f"[IntelSite] {province or 'Unknown'} used {op_name or op_type or 'Operation'}"
            f"{f' on {target}' if target_party else ''}. Result: {result_label}."
        ]
        if gain_text:
            summary_parts.append(f"Gain: {gain_text}.")
        if damage_text:
            summary_parts.append(f"Damage: {damage_text}.")
        if duration_text:
            summary_parts.append(f"Duration: {duration_text} ticks.")
        summary = " ".join(summary_parts)
        raw_line = (
            f"IntelSite id={op_id} server={server} utoDate={item.get('utoDate') or ''} "
            f"type={op_type} name={op_name} result={result_num} actor={province} target={target} "
            f"gain={gain_text or '-'} damage={damage_text or '-'} duration={duration_text or '-'}"
        )
        digest = sha256_text(f"intel-site-op:{op_id}")

        rows_to_upsert.append(
            (
                last_updated,
                uto_date,
                category,
                actor,
                target_party,
                summary,
                raw_line,
                digest,
            )
        )
        ops_rows_to_upsert.append(
            (
                int(op_id),
                utc_now_iso(),
                last_updated,
                uto_date,
                str(server),
                category,
                op_type or None,
                op_name or None,
                safe_int(result_num),
                result_label,
                actor,
                actor_kingdom,
                target_party,
                target_kingdom,
                safe_float(gain_value),
                safe_float(damage_value),
                safe_int(duration_value),
                summary,
                raw_line,
                digest,
            )
        )

    if not rows_to_upsert:
        print("[parser] intel_site_ops empty payload.")
        return 0

    changed = executemany(UPSERT_NEWS_SQL, rows_to_upsert)
    executemany(UPSERT_OPS_SQL, ops_rows_to_upsert)
    print(
        f"[parser] intel_site_ops fetched={len(payload)} upsert_rows={len(rows_to_upsert)} changed={max(changed, 0)}"
    )
    return len(rows_to_upsert)


def parse_and_store_news(page_key: Optional[str] = None) -> int:
    init_db()

    total_extracted = 0
    unique_in_run = set()
    rows_to_upsert = []
    total_fetches = 0

    page_keys = list_page_keys(page_key)
    if not page_keys:
        print("[parser] No HTML available yet. Run collector first.")
        return 0

    news_like_keys = [key for key in page_keys if key in {"kingdom_news", "intel_ops"}]
    for key in news_like_keys:
        fetch_rows = fetch_html_rows(key)
        total_fetches += len(fetch_rows)
        for fetch_row in fetch_rows:
            if key == "intel_ops":
                lines = extract_intel_ops_lines(fetch_row["raw_html"])
            else:
                lines = extract_news_lines(fetch_row["raw_html"])
            total_extracted += len(lines)

            for line in lines:
                digest = sha256_text(line)
                if digest in unique_in_run:
                    continue

                unique_in_run.add(digest)
                event_time_text, summary = split_event_line(line)
                category, actor, target, summary_text = classify_line(summary)
                rows_to_upsert.append(
                    (
                        fetch_row["fetched_at_utc"],
                        event_time_text,
                        category,
                        actor,
                        target,
                        summary_text,
                        line,
                        digest,
                    )
                )

    changed_news = 0
    if rows_to_upsert:
        changed_news = max(executemany(UPSERT_NEWS_SQL, rows_to_upsert), 0)
        print(
            f"[parser] news fetches={total_fetches} extracted={total_extracted} "
            f"unique={len(rows_to_upsert)} upserted={changed_news}"
        )
    else:
        print("[parser] No news lines extracted from stored fetches.")

    changed_snapshots = 0
    if "kingdom_details" in page_keys:
        changed_snapshots = parse_and_store_kingdom_details("kingdom_details")
    changed_intel_site_ops = parse_and_store_intel_site_ops()

    return len(rows_to_upsert) + changed_snapshots + changed_intel_site_ops


if __name__ == "__main__":
    parse_and_store_news()
