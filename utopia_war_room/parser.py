import hashlib
import re
from typing import List, Optional, Tuple

from bs4 import BeautifulSoup

from db import executemany, fetchall, init_db

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


def sha256_text(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8", errors="ignore")).hexdigest()


def normalize_text(s: str) -> str:
    return " ".join(s.split())


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
    """
    Extract event lines that begin with '<Month> <day> of YR<year>'.
    Uses table/list candidates first, then falls back to text-line scanning.
    """
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
    """
    Return (category, actor, target, summary) with conservative buckets.
    """
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


UPSERT_SQL = """
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

    for key in page_keys:
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

    if not rows_to_upsert:
        print("[parser] No event lines extracted from stored fetches.")
        return 0

    changed = executemany(UPSERT_SQL, rows_to_upsert)
    print(
        f"[parser] fetches={total_fetches} extracted={total_extracted} "
        f"unique={len(rows_to_upsert)} upserted={max(changed, 0)}"
    )
    return len(rows_to_upsert)


if __name__ == "__main__":
    parse_and_store_news()
