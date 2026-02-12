from __future__ import annotations

import html
from html.parser import HTMLParser
import re

import aiohttp

from . import db, utils


_DEFAULT_WIKI_URL = "https://wot-blitz.fandom.com/wiki/Vehicles"
_TIER_RE = re.compile(r"^\s*Tier\s+([IVX]+|\d{1,2})\s*$", re.IGNORECASE)
_ENTRY_RE = re.compile(
    r"^(?P<name>.+?)\s*-\s*(?P<type>Light|Medium|Heavy|Tank Destroyer)\b",
    re.IGNORECASE,
)


def _roman_to_int(s: str) -> int:
    values = {"I": 1, "V": 5, "X": 10}
    total = 0
    prev = 0
    for ch in reversed(s.upper()):
        cur = values.get(ch, 0)
        if cur < prev:
            total -= cur
        else:
            total += cur
            prev = cur
    return total


def _normalize_type_from_wiki(raw: str) -> str | None:
    t = (raw or "").strip().lower()
    if t == "light":
        return "light"
    if t == "medium":
        return "medium"
    if t == "heavy":
        return "heavy"
    if t == "tank destroyer":
        return "td"
    return None


class _ContentTextExtractor(HTMLParser):
    """
    Extracts plain text from the article content block while preserving line breaks.
    """

    def __init__(self):
        super().__init__()
        self._in_target = False
        self._depth = 0
        self._parts: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]):
        attrs_map = {k: v for k, v in attrs}
        if not self._in_target and attrs_map.get("id") == "mw-content-text":
            self._in_target = True
            self._depth = 1
            return
        if not self._in_target:
            return
        self._depth += 1
        if tag in ("br", "p", "div", "li", "h2", "h3", "h4", "tr"):
            self._parts.append("\n")

    def handle_endtag(self, tag: str):
        if not self._in_target:
            return
        if tag in ("p", "div", "li", "h2", "h3", "h4", "tr"):
            self._parts.append("\n")
        self._depth -= 1
        if self._depth <= 0:
            self._in_target = False

    def handle_data(self, data: str):
        if self._in_target and data:
            self._parts.append(data)

    def get_text(self) -> str:
        return "".join(self._parts)


async def fetch_wiki_vehicles(url: str = _DEFAULT_WIKI_URL) -> list[tuple[str, int, str]]:
    timeout = aiohttp.ClientTimeout(total=30)
    headers = {"User-Agent": "tankbot/1.0 (wiki-sync)"}
    async with aiohttp.ClientSession(timeout=timeout, headers=headers) as session:
        async with session.get(url) as resp:
            resp.raise_for_status()
            body = await resp.text(encoding="utf-8", errors="replace")

    parser = _ContentTextExtractor()
    parser.feed(body)
    raw_text = html.unescape(parser.get_text())
    lines = [re.sub(r"\s+", " ", ln).strip() for ln in raw_text.splitlines()]

    current_tier: int | None = None
    out: list[tuple[str, int, str]] = []
    seen: set[tuple[str, int, str]] = set()

    for line in lines:
        if not line:
            continue
        tier_m = _TIER_RE.match(line)
        if tier_m:
            tier_token = tier_m.group(1).strip()
            if tier_token.isdigit():
                current_tier = int(tier_token)
            else:
                current_tier = _roman_to_int(tier_token)
            continue

        if current_tier is None:
            continue

        em = _ENTRY_RE.match(line)
        if not em:
            continue

        ttype = _normalize_type_from_wiki(em.group("type"))
        if ttype is None:
            continue

        # remove bracket-only labels from names if present
        name = re.sub(r"\s*\[[^\]]+\]\s*$", "", em.group("name")).strip()
        if not name:
            continue
        key = (utils.norm_tank_name(name), int(current_tier), str(ttype))
        if key in seen:
            continue
        seen.add(key)
        out.append((name, int(current_tier), str(ttype)))

    return out


async def compare_db_with_wiki(url: str = _DEFAULT_WIKI_URL) -> dict:
    wiki_rows = await fetch_wiki_vehicles(url=url)
    db_rows = await db.list_tanks()

    wiki_map = {utils.norm_tank_name(n): (n, int(tier), str(ttype)) for n, tier, ttype in wiki_rows}
    db_map = {utils.norm_tank_name(n): (n, int(tier), str(ttype)) for n, tier, ttype in db_rows}

    missing_in_db: list[tuple[str, int, str]] = []
    extra_in_db: list[tuple[str, int, str]] = []
    mismatched_bucket: list[tuple[str, tuple[int, str], tuple[int, str]]] = []

    for norm, (wiki_name, wiki_tier, wiki_type) in wiki_map.items():
        d = db_map.get(norm)
        if d is None:
            missing_in_db.append((wiki_name, wiki_tier, wiki_type))
            continue
        _db_name, db_tier, db_type = d
        if db_tier != wiki_tier or db_type != wiki_type:
            mismatched_bucket.append((wiki_name, (db_tier, db_type), (wiki_tier, wiki_type)))

    for norm, (db_name, db_tier, db_type) in db_map.items():
        if norm not in wiki_map:
            extra_in_db.append((db_name, db_tier, db_type))

    missing_in_db.sort(key=lambda r: (r[1], r[2], r[0].casefold()))
    extra_in_db.sort(key=lambda r: (r[1], r[2], r[0].casefold()))
    mismatched_bucket.sort(key=lambda r: (r[2][0], r[2][1], r[0].casefold()))

    return {
        "url": url,
        "wiki_total": len(wiki_rows),
        "db_total": len(db_rows),
        "missing_in_db": missing_in_db,
        "extra_in_db": extra_in_db,
        "mismatched_bucket": mismatched_bucket,
    }


async def sync_db_with_wiki(
    *,
    actor: str,
    url: str = _DEFAULT_WIKI_URL,
    apply_missing: bool = True,
    apply_mismatched: bool = False,
) -> dict:
    cmp = await compare_db_with_wiki(url=url)
    added = 0
    updated = 0

    if apply_missing and cmp["missing_in_db"]:
        added, _skipped = await db.add_tanks_bulk(cmp["missing_in_db"], actor, utils.utc_now_z())

    if apply_mismatched and cmp["mismatched_bucket"]:
        for name, _db_bucket, wiki_bucket in cmp["mismatched_bucket"]:
            wiki_tier, wiki_type = wiki_bucket
            await db.edit_tank(name, int(wiki_tier), str(wiki_type), actor, utils.utc_now_z())
            updated += 1

    cmp["added"] = int(added)
    cmp["updated"] = int(updated)
    return cmp

